//! Deterministic simulation testing for the distributed Convex layer (issue
//! #134).
//!
//! FoundationDB's most durable lesson is its testing model: a
//! commit/replication/2PC/placement system can pass integration tests and still
//! be wrong under a rare interleaving of delays, drops, crashes, restarts, and
//! partitions. This crate is a first-iteration, self-contained, deterministic
//! simulator for that failure space.
//!
//! A run is fully determined by its `seed`: the same seed replays the same
//! schedule of operations and faults, so any failure is reproducible. On a
//! violation, [`SimReport`] carries the seed and the event trace needed to
//! reproduce it.
//!
//! This is explicitly *not* a replacement for the unit tests or the Docker
//! integration suite, and it does not yet drive the real committer/Raft code —
//! see `README.md` for the modelled-vs-real boundary and the first-iteration
//! non-goals carried from #134.

mod invariants;
mod model;
mod network;
mod rng;
mod types;

pub use invariants::check_all;
pub use model::Cluster;
pub use network::{
    Network,
    NetworkConfig,
};
pub use rng::Rng;

use crate::types::NodeId;

/// Knobs for a simulation run. All probabilities are per-step in `[0, 1]`.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub num_partitions: u32,
    pub steps: u64,
    pub network: NetworkConfig,
    pub txns_per_step: u64,
    pub crash_prob: f64,
    pub restart_prob: f64,
    pub partition_prob: f64,
    pub heal_prob: f64,
    pub placement_bump_prob: f64,
    pub subscribe_prob: f64,
}

impl SimConfig {
    /// A small, fast configuration suitable for the per-commit CI suite.
    pub fn fast() -> Self {
        Self {
            num_partitions: 3,
            steps: 400,
            network: NetworkConfig::default(),
            txns_per_step: 2,
            crash_prob: 0.02,
            restart_prob: 0.10,
            partition_prob: 0.02,
            heal_prob: 0.15,
            placement_bump_prob: 0.01,
            subscribe_prob: 0.05,
        }
    }

    /// A larger, more adversarial configuration for nightly / manual runs.
    pub fn stress() -> Self {
        Self {
            num_partitions: 5,
            steps: 4000,
            network: NetworkConfig {
                drop_prob: 0.15,
                duplicate_prob: 0.10,
                min_latency: 1,
                max_latency: 12,
            },
            txns_per_step: 3,
            crash_prob: 0.05,
            restart_prob: 0.08,
            partition_prob: 0.05,
            heal_prob: 0.10,
            placement_bump_prob: 0.03,
            subscribe_prob: 0.05,
        }
    }
}

/// The outcome of a single simulation run.
#[derive(Clone, Debug)]
pub struct SimReport {
    pub seed: u64,
    pub steps: u64,
    pub committed: usize,
    pub dropped: u64,
    pub duplicated: u64,
    pub violations: Vec<String>,
    pub trace: Vec<String>,
}

impl SimReport {
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }

    /// A reproduction blob: the seed, the violations, and the tail of the event
    /// trace. Printed by the test harness on failure.
    pub fn failure_report(&self) -> String {
        let mut s = format!(
            "SIMULATION FAILED\n  seed: {}\n  steps: {}\n  committed: {}\n  dropped: {}, \
             duplicated: {}\n  violations:\n",
            self.seed, self.steps, self.committed, self.dropped, self.duplicated
        );
        for v in &self.violations {
            s.push_str(&format!("    - {v}\n"));
        }
        // The last slice of the trace is usually enough to see the offending
        // interleaving; the full run is reproducible from the seed alone.
        let tail = 60.min(self.trace.len());
        s.push_str(&format!("  trace (last {tail} events):\n"));
        for line in &self.trace[self.trace.len() - tail..] {
            s.push_str(&format!("    {line}\n"));
        }
        s.push_str(&format!(
            "  reproduce with: cluster_sim::run({}, cfg)\n",
            self.seed
        ));
        s
    }
}

/// Run one simulation to completion and check every invariant.
pub fn run(seed: u64, config: &SimConfig) -> SimReport {
    let mut rng = Rng::from_seed(seed);
    let mut cluster = Cluster::new(config.num_partitions);
    let mut network = Network::new(config.network.clone());

    for step in 1..=config.steps {
        cluster.set_now(step);

        // 1. Deliver everything the network has made due this tick.
        for env in network.deliver_due(step) {
            cluster.deliver(&mut rng, &mut network, env.to, env.msg);
        }

        // 2. Inject faults.
        inject_faults(&mut rng, &mut cluster, &mut network, config);

        // 3. Background processes: coordinator timeouts and prepared recovery.
        cluster.tick_background(&mut rng, &mut network);

        // 4. New client activity.
        if rng.chance(config.subscribe_prob)
            && let Some(node) = random_up_node(&mut rng, &cluster)
        {
            cluster.register_subscription(&mut rng, node);
        }
        for _ in 0..config.txns_per_step {
            if let Some(node) = random_up_node(&mut rng, &cluster) {
                cluster.try_transaction(&mut rng, &mut network, node);
            }
        }

        // 5. Replicas replay the durable commit log (those that are up and not isolated
        //    catch up; the rest catch up once they recover).
        cluster.replicate_tick(&network);
    }

    // Drain to quiescence so the end-state invariants are meaningful.
    cluster.quiesce(&mut rng, &mut network);

    let violations = check_all(&cluster);
    SimReport {
        seed,
        steps: config.steps,
        committed: cluster.history.len(),
        dropped: network.dropped,
        duplicated: network.duplicated,
        violations,
        trace: cluster.trace.clone(),
    }
}

fn random_up_node(rng: &mut Rng, cluster: &Cluster) -> Option<NodeId> {
    let up: Vec<NodeId> = (0..cluster.num_partitions())
        .filter(|n| cluster.is_up(*n))
        .collect();
    if up.is_empty() {
        None
    } else {
        Some(*rng.choose(&up))
    }
}

fn inject_faults(rng: &mut Rng, cluster: &mut Cluster, network: &mut Network, config: &SimConfig) {
    let p = cluster.num_partitions();
    if rng.chance(config.crash_prob) {
        let node = rng.int_in(0, (p - 1) as u64) as NodeId;
        cluster.crash(node);
    }
    if rng.chance(config.restart_prob) {
        let node = rng.int_in(0, (p - 1) as u64) as NodeId;
        cluster.restart(node);
    }
    if rng.chance(config.partition_prob) {
        let node = rng.int_in(0, (p - 1) as u64) as NodeId;
        network.isolate(node);
    }
    if rng.chance(config.heal_prob) {
        network.heal_all();
    }
    if rng.chance(config.placement_bump_prob) {
        // Usually refresh every node (a clean roll); sometimes leave one node
        // stale to exercise placement-version fencing on prepare.
        let mut refreshed: Vec<NodeId> = (0..p).collect();
        if rng.chance(0.5) && p > 1 {
            let stale = rng.int_in(0, (p - 1) as u64) as NodeId;
            refreshed.retain(|n| *n != stale);
        }
        cluster.bump_placement_on(&refreshed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bounded, fast suite for CI: many seeds, small bounds. Every run must
    /// satisfy all invariants. On failure we print the seed + trace so it is
    /// reproducible.
    #[test]
    fn fast_suite_invariants_hold() {
        let config = SimConfig::fast();
        for seed in 0..200 {
            let report = run(seed, &config);
            assert!(report.passed(), "{}", report.failure_report());
        }
    }

    /// The simulation must be deterministic: same seed => identical outcome.
    #[test]
    fn runs_are_reproducible() {
        let config = SimConfig::fast();
        for seed in [1u64, 42, 12345] {
            let a = run(seed, &config);
            let b = run(seed, &config);
            assert_eq!(
                a.committed, b.committed,
                "committed differs for seed {seed}"
            );
            assert_eq!(
                a.violations, b.violations,
                "violations differ for seed {seed}"
            );
            assert_eq!(a.trace, b.trace, "trace differs for seed {seed}");
        }
    }

    /// Sanity check that the workload actually exercises commits and the
    /// network actually injects faults — otherwise the suite would pass
    /// vacuously.
    #[test]
    fn workload_is_non_trivial() {
        let config = SimConfig::fast();
        let mut total_committed = 0usize;
        let mut total_dropped = 0u64;
        for seed in 0..20 {
            let report = run(seed, &config);
            total_committed += report.committed;
            total_dropped += report.dropped;
        }
        assert!(total_committed > 100, "expected many commits across seeds");
        assert!(total_dropped > 0, "expected the network to drop messages");
    }

    /// Failure reporting includes the seed and a trace tail.
    #[test]
    fn failure_report_mentions_seed() {
        let report = SimReport {
            seed: 777,
            steps: 1,
            committed: 0,
            dropped: 0,
            duplicated: 0,
            violations: vec!["example".to_string()],
            trace: vec!["t=0001 something".to_string()],
        };
        let text = report.failure_report();
        assert!(text.contains("seed: 777"));
        assert!(text.contains("example"));
    }

    /// Longer, more adversarial randomized suite. Ignored by default; run
    /// manually or nightly with `cargo test -p cluster_sim -- --ignored`.
    #[test]
    #[ignore = "long-running randomized suite; run nightly or manually"]
    fn nightly_stress_suite() {
        let config = SimConfig::stress();
        for seed in 0..500 {
            let report = run(seed, &config);
            assert!(report.passed(), "{}", report.failure_report());
        }
    }
}
