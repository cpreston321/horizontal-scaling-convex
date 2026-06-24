//! Resolver-style conflict-check routing (issue #131).
//!
//! Cross-partition writes commit through participant/coordinator 2PC today.
//! That is correct, but it couples *conflict checking* to *commit
//! coordination*. This module factors out the first half — mapping a
//! transaction's read and write sets to the partitions that own the affected
//! tables — into one addressable layer, the seam toward a FoundationDB-style
//! resolver tier.
//!
//! FoundationDB shows that serializable OCC conflict checking can be physically
//! distributed: a single logical transaction order does not require a single
//! physical committer. The unit of ownership here is the **conflict shard** —
//! the partition that owns a [`PlacementTarget`]. Today targets are whole
//! tables (`key % num_partitions` style table ownership); the same plan shape
//! extends to range / key-prefix ownership without changing callers.
//!
//! This layer only decides *where* conflict checks belong. The actual OCC check
//! (read-set staleness against committed/pending/prepared writes) still runs on
//! the owning partition's committer, and 2PC remains the commit protocol until
//! a resolver-based replacement is proven (a #131 non-goal forbids removing it
//! early).

use std::collections::{
    BTreeMap,
    BTreeSet,
};

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

/// The conflict-checking work that belongs to one partition (shard) for a
/// single transaction.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConflictShard {
    /// Tables this transaction reads that this partition owns. The owner is the
    /// authority for read-set conflict checking on these tables.
    pub read_tables: BTreeSet<TableName>,
    /// Number of writes routed to this partition (its write-conflict load).
    pub write_count: usize,
}

impl ConflictShard {
    pub fn is_empty(&self) -> bool {
        self.read_tables.is_empty() && self.write_count == 0
    }
}

/// The per-partition conflict-checking plan for one transaction: which shards
/// must validate which reads, and how much write load each carries.
#[derive(Clone, Debug)]
pub struct ConflictPlan {
    local_partition: PartitionId,
    shards: BTreeMap<PartitionId, ConflictShard>,
}

impl ConflictPlan {
    /// Build the plan from a transaction's read set and its already-routed
    /// write indexes (the 2PC coordinator computes write routing, including
    /// the catalog special cases, so we take it as input rather than
    /// re-deriving it).
    pub fn build(
        reads: &ReadSet,
        table_mapping: &TableMapping,
        partition_map: &PartitionMap,
        write_source: &WriteSource,
        write_indexes: &BTreeMap<PartitionId, Vec<usize>>,
    ) -> Self {
        let mut shards: BTreeMap<PartitionId, ConflictShard> = BTreeMap::new();

        let mut visit_read = |index_name: &TabletIndexName| {
            if let Ok(table_name) = table_mapping.tablet_name(*index_name.table())
                && let Some(partition) =
                    routed_partition_for_table(&table_name, partition_map, write_source)
            {
                shards
                    .entry(partition)
                    .or_default()
                    .read_tables
                    .insert(table_name);
            }
        };
        for (index_name, _) in reads.iter_indexed() {
            visit_read(index_name);
        }
        for (index_name, _) in reads.iter_search() {
            visit_read(index_name);
        }

        for (partition, indexes) in write_indexes {
            if indexes.is_empty() {
                continue;
            }
            shards.entry(*partition).or_default().write_count += indexes.len();
        }

        Self {
            local_partition: partition_map.local_partition(),
            shards,
        }
    }

    pub fn shards(&self) -> &BTreeMap<PartitionId, ConflictShard> {
        &self.shards
    }

    pub fn local_partition(&self) -> PartitionId {
        self.local_partition
    }

    /// Partitions other than the local one that own tables this transaction
    /// reads. These owners are the conflict-check authorities for remote reads.
    pub fn remote_read_partitions(&self) -> BTreeSet<PartitionId> {
        self.shards
            .iter()
            .filter(|(partition, shard)| {
                **partition != self.local_partition && !shard.read_tables.is_empty()
            })
            .map(|(partition, _)| *partition)
            .collect()
    }

    /// Partitions carrying write-conflict load.
    pub fn write_partitions(&self) -> BTreeSet<PartitionId> {
        self.shards
            .iter()
            .filter(|(_, shard)| shard.write_count > 0)
            .map(|(partition, _)| *partition)
            .collect()
    }

    /// True if conflict checking for this transaction spans more than one
    /// shard.
    pub fn is_cross_shard(&self) -> bool {
        self.shards
            .values()
            .filter(|shard| !shard.is_empty())
            .count()
            > 1
    }

    /// Number of non-empty conflict shards (the fan-out of conflict checking).
    pub fn num_shards(&self) -> usize {
        self.shards
            .values()
            .filter(|shard| !shard.is_empty())
            .count()
    }

    /// Emit the resolver's fan-out and per-shard conflict-load metrics.
    pub fn record_metrics(&self) {
        let loads: Vec<(PartitionId, usize, usize)> = self
            .shards
            .iter()
            .filter(|(_, shard)| !shard.is_empty())
            .map(|(partition, shard)| (*partition, shard.read_tables.len(), shard.write_count))
            .collect();
        crate::metrics::log_conflict_plan(self.num_shards(), &loads);
    }
}

/// Remote partitions whose tables this transaction reads, the read-conflict
/// shards a transaction must be consistent with. This is the single source of
/// truth used by the committer's remote-read-frontier wait and by the resolver
/// plan, so read routing cannot drift between the two.
pub fn remote_read_conflict_partitions(
    reads: &ReadSet,
    table_mapping: &TableMapping,
    partition_map: &PartitionMap,
    write_source: &WriteSource,
) -> BTreeSet<PartitionId> {
    let plan = ConflictPlan::build(
        reads,
        table_mapping,
        partition_map,
        write_source,
        &BTreeMap::new(),
    );
    plan.remote_read_partitions()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan_with_writes(write_indexes: BTreeMap<PartitionId, Vec<usize>>) -> ConflictPlan {
        // local partition 0, three partitions; reads empty so only write routing
        // shapes the plan.
        let partition_map = PartitionMap::from_config("a=1,b=2", PartitionId(0), 3);
        ConflictPlan::build(
            &ReadSet::empty(),
            &TableMapping::new(),
            &partition_map,
            &WriteSource::unknown(),
            &write_indexes,
        )
    }

    #[test]
    fn test_empty_plan_has_no_shards() {
        let plan = plan_with_writes(BTreeMap::new());
        assert_eq!(plan.num_shards(), 0);
        assert!(!plan.is_cross_shard());
        assert!(plan.remote_read_partitions().is_empty());
        assert!(plan.write_partitions().is_empty());
        assert_eq!(plan.local_partition(), PartitionId(0));
    }

    #[test]
    fn test_writes_spanning_partitions_are_cross_shard() {
        let plan = plan_with_writes(BTreeMap::from([
            (PartitionId(1), vec![0, 1]),
            (PartitionId(2), vec![2]),
        ]));
        assert_eq!(plan.num_shards(), 2);
        assert!(plan.is_cross_shard());
        assert_eq!(
            plan.write_partitions(),
            BTreeSet::from([PartitionId(1), PartitionId(2)])
        );
        assert_eq!(plan.shards()[&PartitionId(1)].write_count, 2);
        assert_eq!(plan.shards()[&PartitionId(2)].write_count, 1);
        // No reads were recorded, so there are no remote read shards.
        assert!(plan.remote_read_partitions().is_empty());
    }

    #[test]
    fn test_single_write_shard_is_not_cross_shard() {
        let plan = plan_with_writes(BTreeMap::from([(PartitionId(1), vec![0])]));
        assert_eq!(plan.num_shards(), 1);
        assert!(!plan.is_cross_shard());
        assert_eq!(plan.write_partitions(), BTreeSet::from([PartitionId(1)]));
    }

    #[test]
    fn test_empty_write_index_entries_are_ignored() {
        let plan = plan_with_writes(BTreeMap::from([
            (PartitionId(1), vec![]),
            (PartitionId(2), vec![0]),
        ]));
        // Partition 1 has no actual writes, so it is not a conflict shard.
        assert_eq!(plan.num_shards(), 1);
        assert_eq!(plan.write_partitions(), BTreeSet::from([PartitionId(2)]));
    }

    #[test]
    fn test_record_metrics_does_not_panic() {
        let plan = plan_with_writes(BTreeMap::from([
            (PartitionId(1), vec![0, 1]),
            (PartitionId(2), vec![2]),
        ]));
        plan.record_metrics();
    }
}
