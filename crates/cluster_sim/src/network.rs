//! In-simulation message transport with fault injection.
//!
//! Models the NATS replication channel and the 2PC gRPC channel as an
//! unreliable, asynchronous network: messages take a variable number of ticks
//! to arrive, can be dropped or duplicated, and links can be partitioned so no
//! message crosses between two groups of nodes until the partition heals.
//!
//! All faults are decided from the run's seeded [`Rng`], so a delivery schedule
//! is fully reproducible.

use std::collections::BTreeSet;

use crate::{
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

/// A message in flight between nodes.
///
/// Only 2PC control messages travel over this lossy channel (modelling the gRPC
/// link, with the durable decision log as the backstop). Replication rides a
/// separate durable log (see `model::Cluster`), mirroring how the real system
/// replays committed deltas from a NATS JetStream after a reconnect.
#[derive(Clone, Debug)]
pub enum Msg {
    /// 2PC: coordinator asks a participant to prepare its slice. Read-set
    /// validation is done by the coordinator at begin; the participant
    /// validates its write slice against its own committed state.
    Prepare {
        txn: TxnId,
        coordinator: NodeId,
        prepare_ts: Ts,
        writes: Vec<(Key, Value)>,
        placement_version: u64,
    },
    /// 2PC: participant's prepare verdict.
    PrepareReply {
        txn: TxnId,
        participant: NodeId,
        ok: bool,
    },
    /// 2PC: coordinator's commit decision for an already-prepared participant.
    CommitPrepared { txn: TxnId, commit_ts: Ts },
    /// 2PC: coordinator's rollback decision.
    RollbackPrepared { txn: TxnId },
}

/// An envelope: a message addressed to a node, scheduled to arrive at a tick.
#[derive(Clone, Debug)]
pub struct Envelope {
    pub to: NodeId,
    pub deliver_at: SimTime,
    /// Monotonic sequence for deterministic tie-breaking when two envelopes are
    /// due at the same tick.
    pub seq: u64,
    pub msg: Msg,
}

/// Tunable fault rates, all probabilities in `[0, 1]`.
#[derive(Clone, Debug)]
pub struct NetworkConfig {
    pub drop_prob: f64,
    pub duplicate_prob: f64,
    pub min_latency: u64,
    pub max_latency: u64,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            drop_prob: 0.05,
            duplicate_prob: 0.05,
            min_latency: 1,
            max_latency: 5,
        }
    }
}

/// Unreliable message transport.
pub struct Network {
    config: NetworkConfig,
    in_flight: Vec<Envelope>,
    /// Nodes currently isolated from the rest of the cluster. A message is only
    /// delivered if sender and receiver are on the same side of the partition.
    isolated: BTreeSet<NodeId>,
    next_seq: u64,
    /// Count of messages dropped by fault injection, for reporting.
    pub dropped: u64,
    pub duplicated: u64,
}

impl Network {
    pub fn new(config: NetworkConfig) -> Self {
        Self {
            config,
            in_flight: Vec::new(),
            isolated: BTreeSet::new(),
            next_seq: 0,
            dropped: 0,
            duplicated: 0,
        }
    }

    /// True if `a` and `b` can currently exchange messages.
    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        // A node only reaches others on its own side of the partition. With a
        // single isolated set, isolated<->isolated and healthy<->healthy can
        // talk, but isolated<->healthy cannot.
        self.isolated.contains(&a) == self.isolated.contains(&b)
    }

    /// Enqueue `msg` from `from` to `to`, applying drop / duplicate / latency
    /// faults from the seeded RNG. Returns the number of copies enqueued.
    pub fn send(
        &mut self,
        rng: &mut Rng,
        now: SimTime,
        from: NodeId,
        to: NodeId,
        msg: Msg,
    ) -> usize {
        if from != to && !self.connected(from, to) {
            // Partitioned away: treat as a silent drop until the partition heals.
            self.dropped += 1;
            return 0;
        }
        if rng.chance(self.config.drop_prob) {
            self.dropped += 1;
            return 0;
        }
        let copies = if rng.chance(self.config.duplicate_prob) {
            self.duplicated += 1;
            2
        } else {
            1
        };
        for _ in 0..copies {
            let latency = rng.int_in(self.config.min_latency, self.config.max_latency);
            let seq = self.next_seq;
            self.next_seq += 1;
            self.in_flight.push(Envelope {
                to,
                deliver_at: now + latency,
                seq,
                msg: msg.clone(),
            });
        }
        copies
    }

    /// Remove and return every envelope due at or before `now`, sorted for
    /// deterministic delivery order. Envelopes addressed to a now-isolated node
    /// are held (the partition delays them), modelling a stalled link.
    pub fn deliver_due(&mut self, now: SimTime) -> Vec<Envelope> {
        let mut due = Vec::new();
        let mut held = Vec::new();
        for env in std::mem::take(&mut self.in_flight) {
            if env.deliver_at <= now {
                due.push(env);
            } else {
                held.push(env);
            }
        }
        self.in_flight = held;
        due.sort_by_key(|env| (env.deliver_at, env.seq));
        due
    }

    pub fn isolate(&mut self, node: NodeId) {
        self.isolated.insert(node);
    }

    pub fn heal(&mut self, node: NodeId) {
        self.isolated.remove(&node);
    }

    pub fn heal_all(&mut self) {
        self.isolated.clear();
    }

    pub fn is_isolated(&self, node: NodeId) -> bool {
        self.isolated.contains(&node)
    }

    pub fn in_flight_len(&self) -> usize {
        self.in_flight.len()
    }
}
