# Selective Delivery Correctness Boundary

Issue #133. Selective delivery (`crates/database/src/selective_delivery.rs`)
reduces NATS fanout by tracking, per node, which tables that node currently has
live subscriptions for. A publisher can then send a user-table delta only to the
nodes that care about it instead of broadcasting to everyone.

This document defines exactly what that mechanism is allowed to do and what it
must never be solely responsible for.

## The two delivery paths

A committed delta travels over two logically distinct channels:

1. **Broadcast partition subject** — `convex.commits.{partition_id}`.
   `NatsDistributedLog::publish` always publishes here, for every delta. A node
   that needs another partition's writes subscribes to that partition's subject
   and therefore receives *every* delta from it, regardless of interest. This is
   the **correctness-critical replication and invalidation path.**

2. **Node-targeted subject** — `convex.commits.node.{node}`. After the broadcast
   publish, the publisher *also* sends a shadow copy to each node whose interest
   registration is fresh and matches the touched tables. This is **best-effort
   fanout reduction only.**

System-table deltas (`convex.commits.system`) and frontier heartbeats
(`convex.commits.frontier_heartbeat`) are always broadcast and are never subject
to interest narrowing.

## What selective delivery may optimize

- It may let a node *avoid receiving* deltas for tables it has no live
  subscription interest in — **but only when that narrowing sits on top of a
  delivery guarantee that does not depend on interest.**

## What selective delivery must never do alone

- It must never be the **only** way a node receives a cross-partition user-table
  delta. Interest registrations are soft state:
  - they **expire** after `INTEREST_MAX_AGE` (90s);
  - they are **absent** until a freshly started node first publishes interest;
  - a publisher's cached view of them can **lag or be lost** across a NATS
    reconnect (the watch restarts and the cache is re-primed).
  Any of these means the publisher will *not* target a node that actually needs
  the delta. If targeting were the only path, that delta is silently dropped and
  the node becomes stale — fast, but wrong. Convex tolerates conservative extra
  invalidations; it cannot tolerate a missed one.

## Fail-safe rule

**Uncertainty over-delivers.** Absence or staleness of an interest record means
"deliver anyway" (via broadcast), never "skip". Concretely:

- The replica delta consumer subscribes to the **broadcast partition subjects**
  by default (`SELECTIVE_DELIVERY_TRUST_INTEREST=false`). Selective node
  targeting is a shadow on top, not a replacement.
- Selective-only consumption (`subscribe_selective_node`, which filters to the
  node subject + system + heartbeat) is gated behind
  `SELECTIVE_DELIVERY_TRUST_INTEREST=true`. That flag is an explicit opt-in into
  reduced fanout and **must stay off until distributed reactive invalidation
  (#132) proves no node can miss an invalidation under stale / missing / lost
  interest.**

This is why the static env / broadcast fallback is *not* removed: it is the
correctness floor that selective delivery rides on.

## Observability

`crates/database/src/metrics.rs` exposes:

- `database_selective_delivery_broadcast_deltas_total` — user-table deltas sent
  on the correctness-critical broadcast path.
- `database_selective_delivery_targeted_deliveries_total` — best-effort
  node-targeted shadow publishes.
- `database_selective_delivery_shadow_receives_total` — node-targeted shadow
  deltas consumed.
- `database_selective_delivery_stale_registrations` (gauge) — interest records
  skipped as stale on the last delivery decision. **Nonzero means selective
  delivery would under-deliver if it were the sole path.**
- `database_selective_delivery_interest_hit_rate` (gauge) — fraction of known
  registrations that matched the last delta's tables.
- `database_selective_delivery_broadcast_fallbacks_total` — deltas where no node
  interest was known at all, so no narrowing was possible.

A persistent per-node delivery backlog is observed through the existing
replication-transport pending-message metrics on each consumer
(`database_replication_transport_pending_messages`); selective delivery does not
introduce a separate backlog because the broadcast path remains authoritative.

## Code separation

- `selective_delivery.rs` owns only best-effort interest tracking. Its module
  docs restate this boundary, and `interested_nodes_with_health` surfaces the
  staleness signal rather than hiding it.
- `NatsDistributedLog::publish` always performs the broadcast publish first, then
  the best-effort targeting.
- `local_backend` chooses the consumer's subscription set: broadcast by default,
  selective-only only under the explicit opt-in knob.

## Non-goals (carried from #133)

- Do not make selective delivery the source of truth for subscription
  correctness.
- Do not reduce fanout where freshness/interest is uncertain.
- Do not remove broad delivery fallback until distributed invalidation (#132)
  has its own correctness proof.
