# Resolver-Style Conflict Checking

Issue #131. Cross-partition writes commit through participant/coordinator 2PC
(`two_phase_coordinator.rs`). That is correct and serializable, but it couples
two separable concerns: **conflict checking** (does this transaction's read set
still hold, and do its writes collide?) and **commit coordination** (the
prepare/decide/apply protocol). This document defines how Convex read/write sets
map to conflict-checking shards and the path away from coordinator-only conflict
checking, without removing 2PC before a replacement is proven (a #131 non-goal).

FoundationDB is the reference point: it shows serializable OCC conflict checking
can be physically distributed across a resolver tier, so a single logical
transaction order does not require one single physical committer. It is not a
benchmark for Convex reactivity, but it justifies separating conflict ownership
from commit coordination.

## Read/write sets → conflict shards

The unit of conflict ownership is the **conflict shard**: the partition that owns
a `PlacementTarget`. `crates/database/src/conflict_resolver.rs` builds a
`ConflictPlan` for a transaction:

- Each read (indexed or search) resolves through its tablet to a table, and the
  table to its owning partition via `routed_partition_for_table` — the same
  placement map (#130) used everywhere else. That partition is the read-conflict
  authority for that table.
- Each write is routed to its owning partition (the 2PC coordinator already does
  this, including the catalog `_tables`/`_index` special cases, so the resolver
  takes routed write indexes as input rather than re-deriving them).

The result is a per-partition `ConflictShard { read_tables, write_count }`. The
plan exposes the read-conflict shards, the write-conflict shards, the fan-out
(`num_shards`), and whether conflict checking is cross-shard.

Today targets are whole tables (table-level ownership). The plan shape is
deliberately target-based so the same routing extends to range / key-prefix
ownership: only `PlacementTarget` and `routed_partition_for_table` change, not
the plan or its callers. Application code never chooses shard keys (a #131
non-goal) — ownership is derived from placement metadata.

## Where conflict checking runs today

- **Local writes / single-partition:** the owning partition's committer runs OCC
  (`commit_has_conflict`: read set vs the write log, in-flight pending writes,
  and 2PC prepared intents).
- **Remote reads:** the committer waits for the read shards' replication
  frontiers to reach the transaction's snapshot (`validate_remote_read_frontiers`
  over `remote_read_conflict_partitions`), so the reader observed a consistent
  snapshot of every partition it read.
- **Cross-partition writes:** each 2PC participant validates its write slice at
  prepare against its own committed/prepared state, with prepare-timestamp
  fencing; the coordinator records the durable decision.

The resolver layer added here is the **single source of truth for routing** those
checks to owners. `remote_read_conflict_partitions` (used by the committer) and
the coordinator's `ConflictPlan` are both derived from it, so read routing cannot
drift between the frontier-wait path and the resolver.

## Correctness under failure

Cross-partition correctness is unchanged — it still rests on 2PC + the durable
decision log:

- **Participant failure:** prepare fails or times out → coordinator rolls back.
- **Coordinator failure:** the durable decision log + the participant watcher
  resolve prepared transactions; absent a decision, they roll back after timeout.
- **Retry / duplicate commit or rollback:** decisions are write-once and
  participant apply is idempotent, so duplicate `CommitPrepared` / `RollbackPrepared`
  messages are safe.

Serializable isolation is not weakened (a #131 non-goal): the resolver only
relocates *where the routing decision is made*, not *whether* a conflict aborts a
commit.

## Observability

`metrics.rs` exposes:
- `database_conflict_check_shard_fanout` — shards per transaction.
- `database_conflict_check_shard_write_load{partition}` — per-shard write load.
- `database_conflict_check_shard_read_load{partition}` — per-shard read load.
- Existing 2PC metrics cover conflict-check retry rate
  (`database_two_phase_prepare_retries_total`) and latency
  (`commit_is_stale` timer).

## Remaining work toward a resolver tier

- Route **read-set conflict checks** to owners as explicit resolver requests
  (rather than the frontier-wait approximation), so a read shard can reject a
  stale read without the reader waiting on full frontier catch-up.
- Separate **timestamp/version assignment** from commit coordination so conflict
  resolution and durable commit logging scale independently.
- Evolve `PlacementTarget` to range / key-prefix so a single hot table's conflict
  load can be split across shards.
- Once the resolver path is proven (including under the #134 simulation's
  fault injection), retire the coordinator-only cross-partition conflict path.
