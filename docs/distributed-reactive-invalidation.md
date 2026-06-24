# Distributed Reactive Invalidation Ownership

Issue #132. Convex sells automatic reactivity: a query's results update when the
data it read changes. Single-node Convex matches each committed write against the
read sets of active subscriptions (`subscription.rs`'s `SubscriptionWorker`). In
a partitioned cluster, a subscription's read set can span partitions, and the
writes that should invalidate it can land on *any* of them. If horizontal scaling
keeps transactions correct but lets subscriptions go stale, lossy, or
topology-aware, the core product attribute is broken.

This is the biggest Convex-specific distributed gap. FoundationDB does not solve
it (it is about key/value transaction conflicts, not arbitrary query read-set
reactivity); it only justifies sharding the work by ownership.

## Correctness contract

A subscription `S` read at snapshot `S.ts` with read set `RS`. A committed write
`W` (write set `WS`, commit timestamp `T`).

- **Forbidden false negative.** If `RS ∩ WS ≠ ∅` and `T > S.ts`, `S` **must** be
  invalidated. Missing this silently serves a stale result — the one thing the
  system may never do.
- **Allowed false positive.** Invalidating `S` when it did not truly need it is
  acceptable; the query reruns and observes no change. Convex tolerates
  conservative over-invalidation.
- **Conservative matching (this iteration).** Matching is **table-level**: if `W`
  touches any table `S` reads, `S` is invalidated, even if the precise documents,
  indexed ranges, or scan bounds do not overlap. This is a strict
  over-approximation of `RS ∩ WS`, so it can only produce false positives, never
  false negatives. Finer key/range matching is a later optimization that may only
  *narrow* within this safe set.

## Ownership and registration

Ownership reuses the placement map (#130): each table (`PlacementTarget`) is owned
by one partition. `crates/database/src/invalidation.rs`:

- `SubscriptionInterest::from_read_set` derives the set of tables a query reads.
- `owning_shards` maps those tables to the partitions that own them — the
  **invalidation shards** the subscription registers with. A subscription that
  reads tables on partitions `{P1, P2}` registers with both.
- `invalidated_by(written_tables)` is the conservative matcher a shard applies to
  a committed write.

When partition `P` commits `W`, `P` is the authority for the tables it owns: it
matches `W`'s tables against the interests registered with it and invalidates the
matching subscriptions — *wherever they are hosted*. Because a cross-partition
subscription is registered with every shard it reads, a write on any of those
shards invalidates it. This is invalidation sharded the same way conflict
detection is (#131): by table/partition ownership.

Today, node-level table interest already exists (`DeltaInterestTracker`) and feeds
selective delivery. This module gives that interest a correctness model: it is the
*registration* of read-set ownership, and the matcher defines exactly what a
committed write must invalidate.

## Delivery is the correctness-critical broadcast path

Invalidations ride the broadcast replication path, which is the correctness-critical
channel (issue #133): every committed delta reaches every node regardless of
interest, so a subscription cannot miss an invalidation because an interest
registration was stale, missing, or lost across a reconnect. Selective delivery
remains a best-effort fanout reduction layered on top and is never the sole path
until this invalidation model is proven.

## Consistent-snapshot rerun

After `S` is invalidated at `T`, its query reruns at a snapshot `>= T` that is
*consistent across every partition `S` reads*. The rerun uses the same machinery
as cross-partition reads: a repeatable read timestamp plus the remote-read
frontier wait (#130/#131), so each partition `S` reads has caught up to `>= T`
before the rerun observes it. This rules out a rerun that sees `W` on one
partition but a pre-`W` snapshot on another (a torn rerun), and the **stale
follower** case: a follower serving the rerun must wait for its replication
frontier to reach the rerun snapshot, or forward, rather than answer from stale
local state.

## Validation

The #134 simulation (`cluster_sim`) models subscriptions on arbitrary nodes
watching key sets that span partitions, and checks the **no-missed-invalidation**
invariant after fault injection (delay/drop/duplicate/crash/restart/partition):
every subscription whose read set intersects a committed write above its snapshot
is invalidated. It also enforces snapshot consistency and that a newly registered
subscription is checked against the write log from its snapshot forward. That is
the executable form of this contract.

`metrics.rs` exposes invalidation-shard fan-out
(`database_invalidation_subscription_shards_total`), cross-partition subscription
count, and conservative-invalidation count.

## Follower-safe `/sync`

`/sync` is `CoordinatorOwner` today because subscription setup creates global sync
state on the coordinator. Once subscription interest is registered with the owning
shards (rather than centrally), and invalidations are delivered over the broadcast
path with the consistent-snapshot rerun above, a subscription can be hosted on any
follower: the follower registers interest with the owning shards and receives
invalidations like any node. The route can then move from `CoordinatorOwner` to a
follower-safe class. This change is **deferred** until the model is proven
(including under simulation) — a #132 non-goal forbids claiming readiness early,
and forbids optimizing fanout before correctness.

## Remaining work

- Wire `SubscriptionInterest` registration into the live `SubscriptionWorker` /
  `/sync` setup so interest is registered with owning shards on subscribe.
- Implement shard-side matching of committed writes against registered remote
  interests and invalidation delivery to the hosting node over the broadcast path.
- Prove the consistent-snapshot rerun end to end, then reclassify `/sync` as
  follower-safe.
- Evolve to finer (range/key-prefix) matching that narrows within the conservative
  set without introducing false negatives.
