//! Distributed reactive invalidation ownership (issue #132).
//!
//! Convex's headline guarantee is reactivity: a query result updates when the
//! data it read changes. A committed write must invalidate *every* active
//! subscription whose read set it intersects — including subscriptions whose
//! read set spans multiple partitions. This is the biggest Convex-specific
//! distributed gap, because subscription read sets are arbitrary query reads
//! (point reads, indexed ranges, table scans), not just key/value transactions.
//!
//! FoundationDB does not solve this; it only justifies sharding the work by
//! ownership. This module owns the *ownership and matching* half of that:
//!
//! - **Registration:** a subscription's read set decomposes into the partitions
//!   (invalidation shards) that own the tables it reads. The subscription
//!   registers its interest with each owning shard.
//! - **Matching:** when a shard commits a write, it matches the write's tables
//!   against registered interests and invalidates the owning subscriptions —
//!   wherever they are hosted — over the correctness-critical broadcast path
//!   (issue #133). A cross-partition subscription is registered with every
//!   shard it reads, so a write on any of them invalidates it.
//!
//! # Correctness contract
//!
//! - **Forbidden false negative:** a subscription whose read set intersects a
//!   committed write must never be left un-invalidated. Missing an invalidation
//!   silently serves stale results.
//! - **Allowed false positive:** invalidating a subscription that did not truly
//!   need it is fine — the query simply reruns. Convex tolerates conservative
//!   over-invalidation; it cannot tolerate staleness.
//! - **Conservative by default:** matching here is **table-level**. If a write
//!   touches any table a subscription reads, the subscription is invalidated,
//!   even if the precise keys/ranges do not overlap. Finer (range/key) matching
//!   is a later optimization and must only ever *narrow* within this safe
//!   over-approximation (never introduce a false negative).
//!
//! See `docs/distributed-reactive-invalidation.md` for the full contract,
//! consistent-snapshot rerun, and the follower-safe `/sync` path.

use std::collections::BTreeSet;

use common::types::TabletIndexName;
use value::{
    TableMapping,
    TableName,
};

use crate::{
    partition::{
        routed_partition_for_table,
        PartitionId,
        PartitionMap,
    },
    reads::ReadSet,
    write_log::WriteSource,
};

/// The tables a subscription's read set depends on — its invalidation interest.
///
/// Table-granular by design (the conservative over-approximation of the true
/// read set). Stored on the subscription and registered with the owning shards.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SubscriptionInterest {
    tables: BTreeSet<TableName>,
}

impl SubscriptionInterest {
    /// Interest from an explicit table set (e.g. a test or a coarse
    /// registration).
    pub fn from_tables(tables: BTreeSet<TableName>) -> Self {
        Self { tables }
    }

    /// Interest derived from a query's read set: every table it reads (indexed
    /// or search), resolved through the table mapping. This is the
    /// over-approximation we invalidate against.
    pub fn from_read_set(reads: &ReadSet, table_mapping: &TableMapping) -> Self {
        let mut tables = BTreeSet::new();
        let mut visit = |index_name: &TabletIndexName| {
            if let Ok(table_name) = table_mapping.tablet_name(*index_name.table()) {
                tables.insert(table_name);
            }
        };
        for (index_name, _) in reads.iter_indexed() {
            visit(index_name);
        }
        for (index_name, _) in reads.iter_search() {
            visit(index_name);
        }
        Self { tables }
    }

    pub fn tables(&self) -> &BTreeSet<TableName> {
        &self.tables
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// The invalidation shards this subscription must register with: the
    /// partitions that own the tables it reads. A write on any of these
    /// partitions can require invalidating the subscription.
    pub fn owning_shards(
        &self,
        partition_map: &PartitionMap,
        write_source: &WriteSource,
    ) -> BTreeSet<PartitionId> {
        self.tables
            .iter()
            .filter_map(|table| routed_partition_for_table(table, partition_map, write_source))
            .collect()
    }

    /// True if this subscription's interest spans more than one partition. Such
    /// subscriptions must be invalidated by writes on any of their shards,
    /// which is exactly what registering with every owning shard
    /// guarantees.
    pub fn is_cross_partition(
        &self,
        partition_map: &PartitionMap,
        write_source: &WriteSource,
    ) -> bool {
        self.owning_shards(partition_map, write_source).len() > 1
    }

    /// Conservative invalidation test: does a committed write touching
    /// `written_tables` require invalidating this subscription? True if they
    /// share any table. This is the safe over-approximation — never a false
    /// negative. A match records the conservative-invalidation metric.
    pub fn invalidated_by(&self, written_tables: &BTreeSet<TableName>) -> bool {
        let matched = self.tables.iter().any(|t| written_tables.contains(t));
        if matched {
            crate::metrics::log_conservative_invalidation();
        }
        matched
    }

    /// Compute the owning shards and record the registration fan-out metric.
    /// This is the registration step: the subscription's interest is now owned
    /// by each returned shard (issue #132).
    pub fn register(
        &self,
        partition_map: &PartitionMap,
        write_source: &WriteSource,
    ) -> BTreeSet<PartitionId> {
        let shards = self.owning_shards(partition_map, write_source);
        crate::metrics::log_invalidation_registration(shards.len());
        shards
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn interest(tables: &[&str]) -> SubscriptionInterest {
        SubscriptionInterest::from_tables(tables.iter().map(|t| t.parse().unwrap()).collect())
    }

    fn written(tables: &[&str]) -> BTreeSet<TableName> {
        tables.iter().map(|t| t.parse().unwrap()).collect()
    }

    #[test]
    fn owning_shards_span_partitions_for_cross_partition_read_set() {
        // messages -> partition 1, projects -> partition 2, tasks -> default 0.
        let map = PartitionMap::from_config("messages=1,projects=2", PartitionId(0), 3);
        let ws = WriteSource::unknown();
        let sub = interest(&["messages", "projects"]);

        assert_eq!(
            sub.owning_shards(&map, &ws),
            BTreeSet::from([PartitionId(1), PartitionId(2)])
        );
        assert!(sub.is_cross_partition(&map, &ws));
    }

    #[test]
    fn owning_shards_single_partition_is_not_cross_partition() {
        let map = PartitionMap::from_config("messages=1,authors=1", PartitionId(0), 3);
        let ws = WriteSource::unknown();
        let sub = interest(&["messages", "authors"]);

        assert_eq!(
            sub.owning_shards(&map, &ws),
            BTreeSet::from([PartitionId(1)])
        );
        assert!(!sub.is_cross_partition(&map, &ws));
    }

    #[test]
    fn invalidated_by_intersecting_write() {
        let sub = interest(&["messages", "users"]);
        // A write touching `messages` (and an unrelated table) invalidates it.
        assert!(sub.invalidated_by(&written(&["messages", "logs"])));
    }

    #[test]
    fn not_invalidated_by_disjoint_write() {
        let sub = interest(&["messages", "users"]);
        assert!(!sub.invalidated_by(&written(&["logs", "events"])));
    }

    #[test]
    fn cross_partition_sub_invalidated_by_write_on_either_shard() {
        // A subscription reading two tables on two partitions is invalidated by a
        // write to either table — registering with both shards is what makes
        // that hold regardless of which partition commits.
        let sub = interest(&["messages", "projects"]);
        assert!(sub.invalidated_by(&written(&["messages"])));
        assert!(sub.invalidated_by(&written(&["projects"])));
    }

    #[test]
    fn empty_interest_matches_nothing() {
        let sub = SubscriptionInterest::default();
        assert!(sub.is_empty());
        assert!(!sub.invalidated_by(&written(&["messages"])));
    }
}
