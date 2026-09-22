# cluster_sim — deterministic simulation for the distributed layer (#134)

A seeded, reproducible simulator for the distributed Convex protocol: partitioned
single-writer commit, a global timestamp oracle, asynchronous replication, 2PC,
placement-version fencing, and subscription invalidation. It injects message
delay / drop / duplication, node crash / restart, network partitions, and
placement changes, then checks safety invariants.

This is the FoundationDB testing lesson applied here: integration tests can pass
while a rare interleaving silently corrupts correctness. Every run is determined
by a `seed`, so failures reproduce exactly.

## Run it

```bash
# Fast suite (runs in CI):
cargo test -p cluster_sim

# Long randomized suite (nightly / manual):
cargo test -p cluster_sim -- --ignored nightly_stress_suite
```

On failure the harness prints the seed, the violated invariants, and the tail of
the event trace. Re-run `cluster_sim::run(seed, &cfg)` with that seed to reproduce.

## Invariants checked

- **Serializability** — OCC never lets a write-read conflict commit.
- **Snapshot consistency** — a read reflects exactly the writes visible at its
  snapshot timestamp.
- **No lost committed writes** — every committed write is durable on its owner.
- **No phantom prepared transactions** — every 2PC prepare is eventually
  committed or rolled back; none linger after quiescing.
- **Monotonic frontiers** — the committed floor and per-source replication
  frontiers never move backwards.
- **No missed invalidations** — every subscription whose read set intersects a
  later committed write is invalidated.

## Modelled vs. real (first-iteration boundary)

Per issue #134's non-goals, this first iteration models the protocol abstractly
rather than driving the real `committer` / Raft / NATS code. In particular:

- **One node per partition.** "Raft" is modelled as the durability guarantee a
  crash preserves committed store + prepared redo + frontiers + decision log;
  multi-replica leader election is **future work**.
- **Fixed key ownership** (`key % num_partitions`). Placement *version* fencing
  is exercised; online ownership remap / data movement is **future work**.
- **The commit decision is the atomic visibility point.** Replication, 2PC
  durability messages, crash/restart, and partitions are still asynchronous and
  fault-injected; only the linearization timestamp is treated as instantaneous.

These simplifications keep the invariants well-defined for a first pass. The
intended evolution is to progressively replace modelled components with the real
code behind a deterministic runtime. Until the invariant set and fidelity are
mature, simulation passes are **not** a production-readiness claim (also a #134
non-goal).
