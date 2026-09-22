//! The modelled distributed cluster.
//!
//! This is an abstract but faithful model of the fork's distributed protocol,
//! not a wrapper over the real code. It captures the rules that matter for the
//! invariants in issue #134:
//!
//! - a global timestamp oracle that hands out strictly increasing commit
//!   timestamps and tracks a monotonic committed floor;
//! - one single-writer per partition (the committer), so same-partition writes
//!   are totally ordered;
//! - optimistic concurrency control on commit (a write conflicts if a read key
//!   changed after the transaction's snapshot);
//! - asynchronous replication of committed deltas to every other partition over
//!   the unreliable [`Network`] (the broadcast / correctness-critical path);
//! - cross-partition 2PC with a durable decision log and crash recovery;
//! - placement-version fencing on prepare;
//! - subscription invalidation when a committed write intersects a read set.
//!
//! Durability boundary (how "Raft" is modelled in this first iteration): a node
//! crash clears volatile in-progress coordinator state but preserves the
//! committed store, prepared redo log, replication frontiers, and the decision
//! log — exactly the durability guarantee Raft provides. Multi-replica leader
//! election is intentionally out of scope for the first iteration (see README).

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use crate::{
    network::{
        Msg,
        Network,
    },
    rng::Rng,
    types::{
        Key,
        NodeId,
        SimTime,
        Ts,
        TxnId,
        Value,
    },
};

const PREPARE_TIMEOUT: SimTime = 25;

/// A committed transaction recorded in the global history for invariant checks.
#[derive(Clone, Debug)]
pub struct CommittedTxn {
    pub id: u64,
    pub begin_ts: Ts,
    pub commit_ts: Ts,
    /// Key -> value observed at snapshot (`0` means "no committed value yet").
    pub reads: BTreeMap<Key, Value>,
    pub writes: BTreeMap<Key, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    Committed(Ts),
    RolledBack,
}

/// A participant-side prepared transaction (durable redo log entry).
#[derive(Clone, Debug)]
struct Prepared {
    writes: Vec<(Key, Value)>,
}

/// Coordinator-side in-progress 2PC state (volatile: lost on crash).
#[derive(Clone, Debug)]
struct Coordinating {
    begin_ts: Ts,
    prepare_ts: Ts,
    started_at: SimTime,
    participants: BTreeSet<NodeId>,
    replies_ok: BTreeSet<NodeId>,
    replies_bad: BTreeSet<NodeId>,
    reads: BTreeMap<Key, Value>,
    writes: BTreeMap<Key, Value>,
    decided: bool,
}

/// Per-node state.
struct Node {
    up: bool,
    placement_version: u64,
    /// Durable, multi-version committed store this node owns: key -> ascending
    /// `(commit_ts, value)` versions. MVCC so a snapshot read can resolve the
    /// value as of any timestamp.
    store: BTreeMap<Key, Vec<(Ts, Value)>>,
    /// Highest commit timestamp this partition has committed as owner. A
    /// prepare with a timestamp at or below this is fenced: the committer
    /// has already moved past it, so accepting it would insert a commit out
    /// of order.
    commit_frontier: Ts,
    /// Durable replicated values from other partitions: key -> (ts, value).
    replica: BTreeMap<Key, (Ts, Value)>,
    /// Durable per-source replication frontier (monotonic).
    frontier: BTreeMap<NodeId, Ts>,
    /// Durable prepared redo log (survives crash).
    prepared: BTreeMap<TxnId, Prepared>,
    /// Volatile coordinator state (cleared on crash).
    coordinating: BTreeMap<TxnId, Coordinating>,
    /// Durable cursor into the global replication log: the number of entries
    /// this node has consumed. Survives crash, so a restarted node resumes
    /// where it left off rather than missing deltas.
    repl_cursor: usize,
}

/// One entry in the durable replication log (models a NATS JetStream record).
/// Retained and replayed, so a partitioned or crashed node catches up on
/// reconnect rather than losing the delta.
#[derive(Clone, Debug)]
struct ReplEntry {
    source: NodeId,
    ts: Ts,
    writes: Vec<(Key, Value)>,
}

impl Node {
    fn new(placement_version: u64) -> Self {
        Self {
            up: true,
            placement_version,
            store: BTreeMap::new(),
            commit_frontier: 0,
            replica: BTreeMap::new(),
            frontier: BTreeMap::new(),
            prepared: BTreeMap::new(),
            coordinating: BTreeMap::new(),
            repl_cursor: 0,
        }
    }
}

pub struct Subscription {
    pub node: NodeId,
    pub read_keys: BTreeSet<Key>,
    pub registered_ts: Ts,
    pub invalidated: bool,
}

/// A monotonicity violation detected live (recorded rather than panicking so
/// the runner can print the seed and trace).
#[derive(Clone, Debug)]
pub struct LiveViolation(pub String);

pub struct Cluster {
    num_partitions: u32,
    nodes: Vec<Node>,
    // Global timestamp oracle.
    tso_next: Ts,
    committed_floor: Ts,
    /// Highest commit timestamp ever applied to a store.
    max_applied: Ts,
    /// Undecided 2PC transactions: prepare timestamp -> the keys that 2PC will
    /// write. The committed floor is never allowed to advance to or past the
    /// smallest prepare timestamp (the "max repeatable ts must not overtake a
    /// prepared commit" rule), and a commit whose read set intersects one of
    /// these pending write sets at a lower timestamp must not be finalized.
    pending_prepares: BTreeMap<Ts, BTreeSet<Key>>,
    // Authoritative placement version (owned by node 0).
    placement_version: u64,
    // Durable, global 2PC decision log (modelling the NATS KV decision record).
    decisions: BTreeMap<TxnId, Decision>,
    // Durable, global replication log (modelling the NATS JetStream commit
    // stream). Owners append committed slices; replicas replay from a cursor.
    commit_log: Vec<ReplEntry>,
    next_txn: TxnId,
    next_value: Value,
    next_txn_record: u64,
    pub history: Vec<CommittedTxn>,
    pub subs: Vec<Subscription>,
    pub live_violations: Vec<LiveViolation>,
    pub trace: Vec<String>,
    /// Every value the committed floor took, in order, so the invariant checker
    /// can confirm it never decreased.
    pub floor_samples: Vec<Ts>,
    /// Exposed replication-frontier values `(node, source, ts)`, in order.
    pub frontier_samples: Vec<(NodeId, NodeId, Ts)>,
    now: SimTime,
}

impl Cluster {
    pub fn new(num_partitions: u32) -> Self {
        let nodes = (0..num_partitions).map(|_| Node::new(0)).collect();
        Self {
            num_partitions,
            nodes,
            tso_next: 1,
            committed_floor: 0,
            max_applied: 0,
            pending_prepares: BTreeMap::new(),
            placement_version: 0,
            decisions: BTreeMap::new(),
            commit_log: Vec::new(),
            next_txn: 1,
            next_value: 1,
            next_txn_record: 1,
            history: Vec::new(),
            subs: Vec::new(),
            live_violations: Vec::new(),
            trace: Vec::new(),
            floor_samples: Vec::new(),
            frontier_samples: Vec::new(),
            now: 0,
        }
    }

    pub fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    pub fn committed_floor(&self) -> Ts {
        self.committed_floor
    }

    /// Owner partition of a key. Ownership is fixed (`key % P`) in this first
    /// iteration; placement *version* fencing is exercised, ownership remap is
    /// future work (see README).
    fn owner(&self, key: Key) -> NodeId {
        key % self.num_partitions
    }

    fn trace(&mut self, msg: String) {
        self.trace.push(format!("t={:04} {msg}", self.now));
    }

    fn alloc_ts(&mut self) -> Ts {
        let ts = self.tso_next.max(self.committed_floor + 1);
        self.tso_next = ts + 1;
        ts
    }

    /// Recompute the committed floor (a.k.a. max repeatable timestamp): the
    /// largest timestamp that is fully applied and not at or beyond any
    /// undecided prepared transaction. Monotonic by construction.
    fn recompute_floor(&mut self) {
        let cap = self
            .pending_prepares
            .keys()
            .next()
            .map(|p| p - 1)
            .unwrap_or(u64::MAX);
        let candidate = self.max_applied.min(cap);
        self.committed_floor = self.committed_floor.max(candidate);
        self.floor_samples.push(self.committed_floor);
    }

    /// Replication frontiers are monotonic by construction. Out-of-order delta
    /// arrival (the network reorders) must not move a frontier backwards.
    fn advance_frontier(&mut self, node: NodeId, source: NodeId, ts: Ts) {
        let (prev, exposed) = {
            let entry = self.nodes[node as usize]
                .frontier
                .entry(source)
                .or_insert(0);
            let prev = *entry;
            *entry = prev.max(ts);
            (prev, *entry)
        };
        if exposed < prev {
            self.live_violations.push(LiveViolation(format!(
                "node {node} frontier for source {source} regressed {prev} -> {exposed}"
            )));
        }
        self.frontier_samples.push((node, source, exposed));
    }

    fn fresh_value(&mut self) -> Value {
        let v = self.next_value;
        self.next_value += 1;
        v
    }

    /// Latest committed version of `key` on its owner, by timestamp.
    fn latest(&self, key: Key) -> Option<(Ts, Value)> {
        let owner = self.owner(key) as usize;
        self.nodes[owner]
            .store
            .get(&key)
            .and_then(|versions| versions.iter().copied().max_by_key(|(ts, _)| *ts))
    }

    /// Value of `key` as of snapshot `ts`: the greatest version with
    /// `commit_ts <= ts`, or `0` if none. This is the MVCC snapshot read.
    fn value_as_of(&self, key: Key, ts: Ts) -> Value {
        let owner = self.owner(key) as usize;
        self.nodes[owner]
            .store
            .get(&key)
            .and_then(|versions| {
                versions
                    .iter()
                    .filter(|(vts, _)| *vts <= ts)
                    .max_by_key(|(vts, _)| *vts)
                    .map(|(_, v)| *v)
            })
            .unwrap_or(0)
    }

    /// OCC: a read key conflicts if a committed write or a prepared intent
    /// touches it after the snapshot `begin_ts`.
    fn has_conflict(&self, reads: &BTreeMap<Key, Value>, begin_ts: Ts) -> bool {
        for key in reads.keys() {
            if let Some((ts, _)) = self.latest(*key)
                && ts > begin_ts
            {
                return true;
            }
            let owner = self.owner(*key) as usize;
            let prepared_conflict = self.nodes[owner]
                .prepared
                .values()
                .any(|p| p.writes.iter().any(|(k, _)| k == key));
            if prepared_conflict {
                return true;
            }
        }
        false
    }

    /// True if some undecided 2PC sits below `commit_ts` and will write a key
    /// this transaction read. Committing above it would miss that write inside
    /// the read window `(begin_ts, commit_ts)` — a non-serializable read. The
    /// committer must defer (we abort) until the prepared transaction resolves.
    fn pending_blocks_reads(&self, reads: &BTreeMap<Key, Value>, commit_ts: Ts) -> bool {
        self.pending_prepares
            .iter()
            .any(|(prepare_ts, write_keys)| {
                *prepare_ts < commit_ts && reads.keys().any(|k| write_keys.contains(k))
            })
    }

    fn record_commit(
        &mut self,
        begin_ts: Ts,
        commit_ts: Ts,
        reads: BTreeMap<Key, Value>,
        writes: BTreeMap<Key, Value>,
    ) {
        let id = self.next_txn_record;
        self.next_txn_record += 1;
        self.history.push(CommittedTxn {
            id,
            begin_ts,
            commit_ts,
            reads,
            writes,
        });
    }

    /// Apply committed writes to the owning partitions' durable stores, fire
    /// local subscription invalidations, and append the slices to the durable
    /// replication log for replicas to replay. The commit decision is the
    /// atomic visibility point.
    fn commit_writes_and_replicate(&mut self, commit_ts: Ts, writes: &BTreeMap<Key, Value>) {
        let mut by_owner: BTreeMap<NodeId, Vec<(Key, Value)>> = BTreeMap::new();
        for (k, v) in writes {
            by_owner.entry(self.owner(*k)).or_default().push((*k, *v));
        }
        for (owner, kvs) in &by_owner {
            for (k, v) in kvs {
                self.nodes[*owner as usize]
                    .store
                    .entry(*k)
                    .or_default()
                    .push((commit_ts, *v));
            }
            let frontier = &mut self.nodes[*owner as usize].commit_frontier;
            *frontier = (*frontier).max(commit_ts);
            self.invalidate_subs_on(*owner, commit_ts, kvs);
            // Append to the durable replication log (the broadcast,
            // correctness-critical path). Replicas replay this even after a
            // partition or crash, so no delta is lost.
            self.commit_log.push(ReplEntry {
                source: *owner,
                ts: commit_ts,
                writes: kvs.clone(),
            });
        }
        self.max_applied = self.max_applied.max(commit_ts);
        self.recompute_floor();
    }

    /// Replay newly available replication-log entries onto every node that can
    /// currently consume them (up and not network-isolated). A crashed or
    /// isolated node simply does not advance its cursor and catches up later —
    /// the log is durable, so nothing is lost.
    pub fn replicate_tick(&mut self, network: &Network) {
        for node in 0..self.num_partitions {
            if !self.nodes[node as usize].up || network.is_isolated(node) {
                continue;
            }
            let mut cursor = self.nodes[node as usize].repl_cursor;
            while cursor < self.commit_log.len() {
                let entry = self.commit_log[cursor].clone();
                cursor += 1;
                if entry.source == node {
                    continue; // Own writes are already in the local store.
                }
                for (k, v) in &entry.writes {
                    let slot = self.nodes[node as usize]
                        .replica
                        .entry(*k)
                        .or_insert((0, 0));
                    if entry.ts >= slot.0 {
                        *slot = (entry.ts, *v);
                    }
                }
                self.advance_frontier(node, entry.source, entry.ts);
                self.invalidate_subs_on(node, entry.ts, &entry.writes);
            }
            self.nodes[node as usize].repl_cursor = cursor;
        }
    }

    fn invalidate_subs_on(&mut self, node: NodeId, commit_ts: Ts, writes: &[(Key, Value)]) {
        let keys: BTreeSet<Key> = writes.iter().map(|(k, _)| *k).collect();
        for sub in &mut self.subs {
            if sub.node == node
                && commit_ts > sub.registered_ts
                && sub.read_keys.iter().any(|k| keys.contains(k))
            {
                sub.invalidated = true;
            }
        }
    }

    // --- Client operations ------------------------------------------------

    /// Attempt one transaction originating on `origin`. Single-partition writes
    /// commit synchronously at the owner; multi-partition writes go through
    /// 2PC.
    pub fn try_transaction(&mut self, rng: &mut Rng, network: &mut Network, origin: NodeId) {
        if !self.nodes[origin as usize].up {
            return;
        }
        let begin_ts = self.committed_floor;
        let n_reads = rng.int_in(1, 3) as usize;
        let n_writes = rng.int_in(1, 2) as usize;
        let key_space = self.num_partitions * 4;

        let mut reads = BTreeMap::new();
        for _ in 0..n_reads {
            let k = rng.int_in(0, (key_space - 1) as u64) as Key;
            // A key is read through its owning partition's committer. If that
            // owner is down the read cannot be served, so the whole transaction
            // is abandoned (mirrors a node being unavailable for its keys).
            if !self.nodes[self.owner(k) as usize].up {
                return;
            }
            let seen = self.value_as_of(k, begin_ts);
            reads.insert(k, seen);
        }
        let mut write_keys = BTreeSet::new();
        for _ in 0..n_writes {
            write_keys.insert(rng.int_in(0, (key_space - 1) as u64) as Key);
        }

        let owners: BTreeSet<NodeId> = write_keys.iter().map(|k| self.owner(*k)).collect();
        if owners.len() <= 1 {
            self.commit_single_partition(begin_ts, reads, write_keys);
        } else {
            self.begin_two_phase(rng, network, origin, begin_ts, reads, write_keys, owners);
        }
    }

    fn commit_single_partition(
        &mut self,
        begin_ts: Ts,
        reads: BTreeMap<Key, Value>,
        write_keys: BTreeSet<Key>,
    ) {
        let Some(&any_key) = write_keys.iter().next() else {
            return;
        };
        let owner = self.owner(any_key);
        if !self.nodes[owner as usize].up {
            return; // Owner unavailable: the write simply does not happen.
        }
        if self.has_conflict(&reads, begin_ts) {
            return; // OCC abort.
        }
        let commit_ts = self.alloc_ts();
        if self.pending_blocks_reads(&reads, commit_ts) {
            return; // Defer to a lower pending prepare that may write a read
                    // key.
        }
        let writes: BTreeMap<Key, Value> = write_keys
            .iter()
            .map(|k| (*k, self.fresh_value()))
            .collect();
        self.trace(format!(
            "commit 1P owner={owner} begin={begin_ts} commit={commit_ts} keys={:?}",
            writes.keys().collect::<Vec<_>>()
        ));
        self.commit_writes_and_replicate(commit_ts, &writes);
        self.record_commit(begin_ts, commit_ts, reads, writes);
    }

    #[allow(clippy::too_many_arguments)]
    fn begin_two_phase(
        &mut self,
        rng: &mut Rng,
        network: &mut Network,
        coordinator: NodeId,
        begin_ts: Ts,
        reads: BTreeMap<Key, Value>,
        write_keys: BTreeSet<Key>,
        participants: BTreeSet<NodeId>,
    ) {
        if self.has_conflict(&reads, begin_ts) {
            return;
        }
        let txn = self.next_txn;
        self.next_txn += 1;
        let prepare_ts = self.alloc_ts();
        // Hold the committed floor below this prepare timestamp until the 2PC
        // resolves, and remember its write set so concurrent commits that read
        // those keys defer to it.
        self.pending_prepares
            .insert(prepare_ts, write_keys.iter().copied().collect());
        let writes: BTreeMap<Key, Value> = write_keys
            .iter()
            .map(|k| (*k, self.fresh_value()))
            .collect();
        let pv = self.placement_version;
        self.trace(format!(
            "begin 2PC txn={txn} coord={coordinator} parts={participants:?} \
             prepare_ts={prepare_ts}"
        ));
        self.nodes[coordinator as usize].coordinating.insert(
            txn,
            Coordinating {
                begin_ts,
                prepare_ts,
                started_at: self.now,
                participants: participants.clone(),
                replies_ok: BTreeSet::new(),
                replies_bad: BTreeSet::new(),
                reads,
                writes: writes.clone(),
                decided: false,
            },
        );
        for p in participants {
            let slice: Vec<(Key, Value)> = writes
                .iter()
                .filter(|(k, _)| self.owner(**k) == p)
                .map(|(k, v)| (*k, *v))
                .collect();
            network.send(
                rng,
                self.now,
                coordinator,
                p,
                Msg::Prepare {
                    txn,
                    coordinator,
                    prepare_ts,
                    writes: slice,
                    placement_version: pv,
                },
            );
        }
    }

    // --- Message handlers -------------------------------------------------

    pub fn deliver(&mut self, rng: &mut Rng, network: &mut Network, to: NodeId, msg: Msg) {
        if !self.nodes[to as usize].up {
            return; // Crashed node drops delivered messages.
        }
        match msg {
            Msg::Prepare {
                txn,
                coordinator,
                prepare_ts,
                writes,
                placement_version,
            } => {
                // Fence stale prepares: the partition has already committed past
                // this timestamp, so accepting it would insert an out-of-order
                // commit. Also enforce placement-version agreement.
                let ok = self.nodes[to as usize].placement_version == placement_version
                    && prepare_ts > self.nodes[to as usize].commit_frontier;
                if ok {
                    self.nodes[to as usize]
                        .prepared
                        .insert(txn, Prepared { writes });
                }
                network.send(
                    rng,
                    self.now,
                    to,
                    coordinator,
                    Msg::PrepareReply {
                        txn,
                        participant: to,
                        ok,
                    },
                );
            },
            Msg::PrepareReply {
                txn,
                participant,
                ok,
            } => {
                self.handle_prepare_reply(rng, network, to, txn, participant, ok);
            },
            Msg::CommitPrepared { txn, commit_ts: _ } => {
                // Writes were already applied at the commit decision; this just
                // clears the participant's durable prepared redo entry.
                self.nodes[to as usize].prepared.remove(&txn);
            },
            Msg::RollbackPrepared { txn } => {
                self.nodes[to as usize].prepared.remove(&txn);
            },
        }
    }

    fn handle_prepare_reply(
        &mut self,
        rng: &mut Rng,
        network: &mut Network,
        coordinator: NodeId,
        txn: TxnId,
        participant: NodeId,
        ok: bool,
    ) {
        // Decide inside a scoped borrow of the coordinator state, then act on
        // `self` once that borrow has ended (the commit path mutates other
        // fields of `self`).
        enum Outcome {
            None,
            Commit {
                begin_ts: Ts,
                commit_ts: Ts,
                reads: BTreeMap<Key, Value>,
                writes: BTreeMap<Key, Value>,
                participants: BTreeSet<NodeId>,
            },
            Rollback {
                prepare_ts: Ts,
                participants: BTreeSet<NodeId>,
            },
        }

        let outcome = {
            let Some(state) = self.nodes[coordinator as usize].coordinating.get_mut(&txn) else {
                return;
            };
            if ok {
                state.replies_ok.insert(participant);
            } else {
                state.replies_bad.insert(participant);
            }
            if state.decided {
                Outcome::None
            } else if state.replies_ok == state.participants {
                state.decided = true;
                Outcome::Commit {
                    begin_ts: state.begin_ts,
                    commit_ts: state.prepare_ts,
                    reads: state.reads.clone(),
                    writes: state.writes.clone(),
                    participants: state.participants.clone(),
                }
            } else if !state.replies_bad.is_empty() {
                state.decided = true;
                Outcome::Rollback {
                    prepare_ts: state.prepare_ts,
                    participants: state.participants.clone(),
                }
            } else {
                Outcome::None
            }
        };

        match outcome {
            Outcome::None => {},
            Outcome::Commit {
                begin_ts,
                commit_ts,
                reads,
                writes,
                participants,
            } => {
                // Two conditions must hold to finalize a cross-partition commit
                // at `commit_ts` and stay serializable:
                //  1. Re-validate the read set (a write may have landed on a read key while the
                //     2PC was in flight).
                //  2. No other prepared transaction sits below this commit timestamp. Such a
                //     transaction could still commit a write into this one's read window
                //     (begin_ts, commit_ts), so we defer to it by rolling back — mirroring the
                //     rule that the max-repeatable timestamp must not overtake a prepared
                //     commit.
                let blocked_by_earlier_prepare = self.pending_blocks_reads(&reads, commit_ts);
                if self.has_conflict(&reads, begin_ts) || blocked_by_earlier_prepare {
                    self.decisions.insert(txn, Decision::RolledBack);
                    self.pending_prepares.remove(&commit_ts);
                    self.recompute_floor();
                    self.trace(format!("decide ROLLBACK (read conflict) txn={txn}"));
                    for p in participants {
                        network.send(rng, self.now, coordinator, p, Msg::RollbackPrepared { txn });
                    }
                } else {
                    self.decisions.insert(txn, Decision::Committed(commit_ts));
                    self.trace(format!("decide COMMIT txn={txn} commit_ts={commit_ts}"));
                    // The decision is the atomic visibility point. Free the
                    // pending hold first so the floor can advance, then apply.
                    self.pending_prepares.remove(&commit_ts);
                    self.commit_writes_and_replicate(commit_ts, &writes);
                    self.record_commit(begin_ts, commit_ts, reads, writes);
                    for p in participants {
                        network.send(
                            rng,
                            self.now,
                            coordinator,
                            p,
                            Msg::CommitPrepared { txn, commit_ts },
                        );
                    }
                }
            },
            Outcome::Rollback {
                prepare_ts,
                participants,
            } => {
                self.decisions.insert(txn, Decision::RolledBack);
                self.trace(format!("decide ROLLBACK txn={txn}"));
                self.pending_prepares.remove(&prepare_ts);
                self.recompute_floor();
                for p in participants {
                    network.send(rng, self.now, coordinator, p, Msg::RollbackPrepared { txn });
                }
            },
        }
    }

    // --- Background processes --------------------------------------------

    /// Coordinator/watcher timeout handling and prepared-transaction recovery.
    fn run_watchers(&mut self, rng: &mut Rng, network: &mut Network) {
        // Coordinator timeout: a still-undecided 2PC past the prepare timeout
        // rolls back (a participant is unreachable or crashed).
        let mut to_rollback = Vec::new();
        for node in 0..self.num_partitions {
            for (txn, state) in &self.nodes[node as usize].coordinating {
                if !state.decided && self.now.saturating_sub(state.started_at) > PREPARE_TIMEOUT {
                    to_rollback.push((node, *txn, state.prepare_ts, state.participants.clone()));
                }
            }
        }
        for (node, txn, prepare_ts, participants) in to_rollback {
            if let Some(state) = self.nodes[node as usize].coordinating.get_mut(&txn) {
                state.decided = true;
            }
            self.decisions.entry(txn).or_insert(Decision::RolledBack);
            self.pending_prepares.remove(&prepare_ts);
            self.recompute_floor();
            self.trace(format!("watcher ROLLBACK (timeout) txn={txn}"));
            for p in participants {
                network.send(rng, self.now, node, p, Msg::RollbackPrepared { txn });
            }
        }

        // Participant recovery: resolve durable prepared entries against the
        // decision log (covers a crashed/restarted participant or a lost commit
        // message). This is the two_phase_watcher in the real code.
        let mut resolutions: Vec<(NodeId, TxnId, Decision)> = Vec::new();
        for node in 0..self.num_partitions {
            if !self.nodes[node as usize].up {
                continue;
            }
            for txn in self.nodes[node as usize].prepared.keys() {
                if let Some(decision) = self.decisions.get(txn) {
                    resolutions.push((node, *txn, decision.clone()));
                }
            }
        }
        for (node, txn, decision) in resolutions {
            // Application already happened at the commit decision; recovery only
            // needs to clear the durable prepared redo against the decision log
            // (commit or rollback both resolve the prepared entry).
            let _ = decision;
            self.nodes[node as usize].prepared.remove(&txn);
        }
    }

    // --- Faults -----------------------------------------------------------

    pub fn crash(&mut self, node: NodeId) {
        if !self.nodes[node as usize].up {
            return;
        }
        self.nodes[node as usize].up = false;
        // Volatile coordinator state is lost. Any 2PC this node was coordinating
        // but had not yet decided is abandoned: record a rollback decision (a
        // dead coordinator can never commit) and release the floor hold so the
        // cluster is not stuck behind a transaction that will never resolve.
        let abandoned: Vec<(TxnId, Ts)> = self.nodes[node as usize]
            .coordinating
            .iter()
            .filter(|(_, state)| !state.decided)
            .map(|(txn, state)| (*txn, state.prepare_ts))
            .collect();
        self.nodes[node as usize].coordinating.clear();
        for (txn, prepare_ts) in abandoned {
            self.decisions.entry(txn).or_insert(Decision::RolledBack);
            self.pending_prepares.remove(&prepare_ts);
        }
        self.recompute_floor();
        self.trace(format!("crash node={node}"));
    }

    pub fn restart(&mut self, node: NodeId) {
        if !self.nodes[node as usize].up {
            self.nodes[node as usize].up = true;
            self.trace(format!("restart node={node}"));
        }
    }

    pub fn is_up(&self, node: NodeId) -> bool {
        self.nodes[node as usize].up
    }

    /// Bump the authoritative placement version and propagate it to a subset of
    /// nodes, leaving the rest stale (exercises placement-version fencing).
    pub fn bump_placement_on(&mut self, refreshed: &[NodeId]) {
        self.placement_version += 1;
        for node in refreshed {
            self.nodes[*node as usize].placement_version = self.placement_version;
        }
        self.trace(format!(
            "placement bump -> v{} refreshed={:?}",
            self.placement_version, refreshed
        ));
    }

    pub fn register_subscription(&mut self, rng: &mut Rng, node: NodeId) {
        let key_space = self.num_partitions * 4;
        let n = rng.int_in(1, 2) as usize;
        let mut read_keys = BTreeSet::new();
        for _ in 0..n {
            read_keys.insert(rng.int_in(0, (key_space - 1) as u64) as Key);
        }
        let registered_ts = self.committed_floor;
        // Match the read set against writes already in the log above the
        // subscription's snapshot. The floor can be capped below already-committed
        // writes (by a pending prepare), so a brand-new subscription may need to
        // be invalidated immediately — the real SubscriptionWorker scans the
        // write log forward from the snapshot for exactly this reason.
        let already_stale = self.commit_log.iter().any(|entry| {
            entry.ts > registered_ts && entry.writes.iter().any(|(k, _)| read_keys.contains(k))
        });
        self.trace(format!(
            "subscribe sub={} node={node} keys={read_keys:?} registered_ts={registered_ts} \
             stale={already_stale}",
            self.subs.len(),
        ));
        self.subs.push(Subscription {
            node,
            read_keys,
            registered_ts,
            invalidated: already_stale,
        });
    }

    // --- Driver -----------------------------------------------------------

    pub fn set_now(&mut self, now: SimTime) {
        self.now = now;
    }

    pub fn tick_background(&mut self, rng: &mut Rng, network: &mut Network) {
        self.run_watchers(rng, network);
    }

    /// Drain the simulation to a quiescent state: heal the network, restart all
    /// nodes, and run delivery + watchers until nothing is in flight and no
    /// prepared transactions remain (bounded so a bug cannot hang the suite).
    pub fn quiesce(&mut self, rng: &mut Rng, network: &mut Network) {
        network.heal_all();
        for node in 0..self.num_partitions {
            self.restart(node);
        }
        for _ in 0..1000 {
            self.now += 1;
            let due = network.deliver_due(self.now);
            for env in due {
                self.deliver(rng, network, env.to, env.msg);
            }
            self.run_watchers(rng, network);
            self.replicate_tick(network);
            let pending_prepared =
                (0..self.num_partitions).any(|n| !self.nodes[n as usize].prepared.is_empty());
            let replicas_caught_up = (0..self.num_partitions)
                .all(|n| self.nodes[n as usize].repl_cursor == self.commit_log.len());
            if network.in_flight_len() == 0 && !pending_prepared && replicas_caught_up {
                break;
            }
        }
    }

    // --- Read-only accessors for invariant checking -----------------------

    pub fn decision(&self, txn: TxnId) -> Option<&Decision> {
        self.decisions.get(&txn)
    }

    pub fn prepared_count(&self) -> usize {
        (0..self.num_partitions)
            .map(|n| self.nodes[n as usize].prepared.len())
            .sum()
    }

    pub fn owner_value(&self, key: Key) -> Option<(Ts, Value)> {
        self.latest(key)
    }

    pub fn replica_value(&self, node: NodeId, key: Key) -> Option<(Ts, Value)> {
        self.nodes[node as usize].replica.get(&key).copied()
    }

    pub fn owner_of(&self, key: Key) -> NodeId {
        self.owner(key)
    }
}
