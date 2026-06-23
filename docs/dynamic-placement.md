# Dynamic Placement Control Plane

Issue #104 is the path from startup-configured partitions to online placement
changes. The current implementation is still static: nodes read
`PARTITION_ID`, `PARTITION_MAP`, `NUM_PARTITIONS`, and `NODE_ADDRESSES` at
startup. This document records the safety contract added before online
rebalancing is introduced.

## Current State

Placement ownership is now represented in two layers in
`crates/database/src/partition.rs`:

- `PlacementMetadata` is the versioned control-plane model. It records where
  the metadata came from, how many partitions exist, and which logical targets
  are owned by which partitions.
- `PartitionMap` is the runtime lookup object used by commit, routing, and 2PC
  paths.
- `PlacementState` is the refreshable in-process holder shared by the committer
  and commit client. Each transaction snapshots the current `PartitionMap`
  before routing so a placement refresh cannot change ownership halfway through
  one commit.

Version `0` means the startup-configured map. Operators may set
`PARTITION_MAP_VERSION` and must bump it whenever table ownership changes.

When NATS is configured, startup initializes or loads the shared
`convex_placement/current` NATS KV record and refreshes `PlacementState` from
that authoritative control-plane record. The static env map remains the
bootstrap fallback so `Database::load` can start before any external placement
source is available.

### Cluster membership in the replicated record (#130)

`PlacementMetadata` now carries cluster *membership* in addition to table
ownership: a `members` map from `PartitionId` to that partition's gRPC peer
addresses. Membership is seeded from the static `NODE_ADDRESSES` env when the
first node initializes the `convex_placement/current` record, and it travels
with every published version.

2PC routing reads addresses from this replicated source first
(`CommitterClient::effective_node_addresses`), falling back to the static
`NODE_ADDRESSES` env when the record carries no membership. This is what lets a
new partition be introduced by publishing a new version rather than by
hand-editing every existing node's env.

### Live refresh without restart (#130)

Each partitioned node runs a `placement_metadata_refresh` background task that
re-reads `convex_placement/current` every `PLACEMENT_REFRESH_INTERVAL_SECS`
(default 5s) and adopts any version strictly newer than the one it is currently
routing with (`refresh_placement_once`). Equal or older versions are ignored, so
an idle cluster does not churn the routing lock and a stale publisher cannot roll
ownership backward. Adoption increments `database_placement_refresh_total` and
sets the `database_placement_version_info` gauge.

Publishing a new version goes through `PlacementMetadataStore::publish`, a
compare-and-set that requires a strictly greater version. Two concurrent
publishers cannot silently clobber each other: the loser observes a stale
revision, fails, and must reload and retry.

Placement metadata is control-plane state, not an application API. Nodes,
routers, and operators may reason about placement versions and ownership, but
queries, mutations, actions, and subscriptions should continue to target one
logical Convex database without shard or partition hints.

Cross-partition 2PC `Prepare` requests carry the coordinator's placement
version. The participant compares it with its local `PartitionMap` version and
rejects mismatches before staging writes. This prevents a stale coordinator from
preparing writes against an old owner while the cluster is rolling to a new
placement map.

`CommitPrepared` and `RollbackPrepared` intentionally do not reject on placement
version. Once a participant has prepared a transaction, finishing or aborting
that exact transaction must remain possible even if placement metadata changes.

## Operator Rule

When changing `PARTITION_MAP`, roll all nodes with the same
`PARTITION_MAP_VERSION`. If one node is stale, cross-partition writes involving
that node fail fast with a placement-version mismatch instead of silently
preparing against the wrong owner.

## Table-level vs range/key-prefix placement

Ownership is modeled as `PlacementTarget`, which is deliberately an enum even
though it has a single `Table(TableName)` variant today. The control-plane model
therefore never assumes table-only ownership:

- **Table-level (today).** Each whole table maps to one partition. Routing,
  conflict checking, and 2PC all key off `partition_for_table`. This is enough
  to scale writes across tables but cannot split a single hot table.
- **Range / key-prefix (future).** A future variant
  (e.g. `Range { table, lo, hi }` or `KeyPrefix { table, prefix }`) would let one
  table's key space span partitions. Routing would resolve a *document's key*
  to a partition rather than just its table. The serialized record, the
  `members` map, the version/CAS publish path, and the live-refresh loop are all
  reusable as-is; only `PlacementTarget` and the `partition_for_*` lookups grow a
  range-aware path. Application code stays unchanged — clients still target one
  logical database with no shard hints.

## Operational flows

All flows publish a new `PlacementMetadata` version through
`PlacementMetadataStore::publish` (strictly increasing, CAS). Running nodes adopt
it within `PLACEMENT_REFRESH_INTERVAL_SECS` with no restart.

### Add a node / partition

1. Start the new node with its own `PARTITION_ID` and a `NODE_ADDRESSES` env that
   includes itself (bootstrap; existing nodes do **not** need their env edited).
2. Publish a new version that (a) raises `numPartitions`, (b) adds the new
   partition's addresses to `members`, and (c) optionally moves table ownership
   onto the new partition.
3. Existing nodes pick up the new membership and route 2PC to the new partition
   automatically.

### Remove a node / partition

1. Publish a new version that moves every table owned by the departing partition
   to a remaining partition. Do not drop it from `members` yet — in-flight 2PC
   may still need to reach it to resolve prepared transactions.
2. Once the moved tables' writes have drained and no prepared transactions
   reference the partition, publish a follow-up version dropping it from
   `members` and lowering `numPartitions`.
3. Stop the node.

### Rebalance (move a table)

1. (Future, gated on online data movement) Freeze writes for the moved table on
   the source partition, copy/catch-up its data to the destination, then publish
   the new owner version and unfreeze. Until online movement lands, rebalancing
   is an operator action performed during a maintenance window.

`CommitPrepared`/`RollbackPrepared` intentionally ignore placement version, so a
transaction already prepared against a partition can always be resolved even
while ownership is being rolled forward.

## Remaining #104 Work

- Add an authority/lease model for *who* may publish a new placement version
  (today any node with NATS access can call `publish`).
- Add an online data-movement workflow (freeze → copy → catch-up → publish →
  unfreeze) so rebalances need no maintenance window.
- Grow `PlacementTarget` to a range/key-prefix variant to split single hot
  tables.
- Decide whether NATS KV remains the placement authority long term or becomes a
  bootstrap/transport layer for a replicated placement system table.

Large distributed databases use the same shape at a larger scale: placement is
metadata with an owner, routers carry or observe a metadata version/epoch, stale
routers refresh, and data movement is separate from request routing.
