//! Invariant checks run after a simulation quiesces.
//!
//! Each function returns a list of human-readable violations (empty = passed).
//! These are the safety properties issue #134 calls out: serializability,
//! snapshot consistency, no lost committed writes, no phantom prepared
//! transactions, monotonic frontiers, and no missed invalidations.

use std::collections::BTreeMap;

use crate::{
    model::Cluster,
    types::{
        Key,
        Ts,
        Value,
    },
};

/// Run every invariant and collect all violations.
pub fn check_all(cluster: &Cluster) -> Vec<String> {
    let mut v = Vec::new();
    v.extend(monotonic_floor(cluster));
    v.extend(monotonic_frontiers(cluster));
    v.extend(live_violations(cluster));
    v.extend(serializable(cluster));
    v.extend(snapshot_consistent(cluster));
    v.extend(no_lost_writes(cluster));
    v.extend(no_phantom_prepared(cluster));
    v.extend(no_missed_invalidations(cluster));
    v
}

fn monotonic_floor(cluster: &Cluster) -> Vec<String> {
    let mut out = Vec::new();
    let mut prev = 0;
    for &ts in &cluster.floor_samples {
        if ts < prev {
            out.push(format!(
                "monotonic-floor: committed floor regressed {prev} -> {ts}"
            ));
        }
        prev = ts;
    }
    out
}

fn monotonic_frontiers(cluster: &Cluster) -> Vec<String> {
    let mut out = Vec::new();
    let mut last: BTreeMap<(u32, u32), Ts> = BTreeMap::new();
    for &(node, source, ts) in &cluster.frontier_samples {
        let prev = last.entry((node, source)).or_insert(0);
        if ts < *prev {
            out.push(format!(
                "monotonic-frontier: node {node} source {source} regressed {prev} -> {ts}"
            ));
        }
        *prev = ts;
    }
    out
}

fn live_violations(cluster: &Cluster) -> Vec<String> {
    cluster
        .live_violations
        .iter()
        .map(|v| format!("live: {}", v.0))
        .collect()
}

/// Build, per key, the sorted timeline of committed `(commit_ts, value)`.
fn timelines(cluster: &Cluster) -> BTreeMap<Key, Vec<(Ts, Value)>> {
    let mut timelines: BTreeMap<Key, Vec<(Ts, Value)>> = BTreeMap::new();
    for txn in &cluster.history {
        for (k, v) in &txn.writes {
            timelines.entry(*k).or_default().push((txn.commit_ts, *v));
        }
    }
    for series in timelines.values_mut() {
        series.sort_unstable();
    }
    timelines
}

fn serializable(cluster: &Cluster) -> Vec<String> {
    let mut out = Vec::new();
    let timelines = timelines(cluster);
    for txn in &cluster.history {
        for k in txn.reads.keys() {
            let Some(series) = timelines.get(k) else {
                continue;
            };
            // A committed write to a read key landing strictly between this
            // transaction's snapshot and its commit means OCC let a write-read
            // conflict through.
            for &(commit_ts, _) in series {
                if commit_ts > txn.begin_ts && commit_ts < txn.commit_ts {
                    out.push(format!(
                        "serializability: txn {} (begin={}, commit={}) read key {k} but it was \
                         overwritten at {commit_ts}",
                        txn.id, txn.begin_ts, txn.commit_ts
                    ));
                }
            }
        }
    }
    out
}

fn snapshot_consistent(cluster: &Cluster) -> Vec<String> {
    let mut out = Vec::new();
    let timelines = timelines(cluster);
    for txn in &cluster.history {
        for (k, seen) in &txn.reads {
            let expected = timelines
                .get(k)
                .and_then(|series| {
                    series
                        .iter()
                        .filter(|(ts, _)| *ts <= txn.begin_ts)
                        .map(|(_, v)| *v)
                        .next_back()
                })
                .unwrap_or(0);
            if expected != *seen {
                out.push(format!(
                    "snapshot-consistency: txn {} at begin={} read key {k} = {seen}, but the \
                     value as of that snapshot was {expected}",
                    txn.id, txn.begin_ts
                ));
            }
        }
    }
    out
}

fn no_lost_writes(cluster: &Cluster) -> Vec<String> {
    let mut out = Vec::new();
    let timelines = timelines(cluster);
    for (key, series) in &timelines {
        let Some(&(latest_ts, latest_val)) = series.last() else {
            continue;
        };
        match cluster.owner_value(*key) {
            Some((ts, val)) if ts == latest_ts && val == latest_val => {},
            other => out.push(format!(
                "no-lost-writes: key {key} latest committed write ({latest_ts}, {latest_val}) is \
                 not durable on its owner (found {other:?})"
            )),
        }
    }
    out
}

fn no_phantom_prepared(cluster: &Cluster) -> Vec<String> {
    let count = cluster.prepared_count();
    if count == 0 {
        Vec::new()
    } else {
        vec![format!(
            "no-phantom-prepared: {count} prepared transaction(s) remained unresolved after \
             quiescing"
        )]
    }
}

fn no_missed_invalidations(cluster: &Cluster) -> Vec<String> {
    let mut out = Vec::new();
    for (i, sub) in cluster.subs.iter().enumerate() {
        let should_invalidate = cluster.history.iter().any(|txn| {
            txn.commit_ts > sub.registered_ts
                && txn.writes.keys().any(|k| sub.read_keys.contains(k))
        });
        if should_invalidate && !sub.invalidated {
            out.push(format!(
                "missed-invalidation: subscription {i} on node {} watching {:?} (registered at \
                 {}) was never invalidated despite an intersecting committed write",
                sub.node, sub.read_keys, sub.registered_ts
            ));
        }
    }
    out
}
