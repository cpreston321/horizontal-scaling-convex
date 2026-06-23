//! Partition assignment for table-level write scaling.
//!
//! Maps tables to partitions (nodes). Each node owns a set of tables
//! and is the single Committer for those tables, preserving the
//! single-writer guarantee within each partition.
//!
//! ## Design (inspired by Vitess VSchema)
//!
//! A `PartitionMap` assigns each table name to a `PartitionId`.
//! The partition map is loaded from configuration at startup and
//! shared across all nodes. All nodes must agree on the mapping.
//!
//! Tables not explicitly assigned go to partition 0 (the default
//! partition). System tables (`_tables`, `_index`, `_modules`, etc.)
//! are always on partition 0.
//!
//! ## Usage
//!
//! ```ignore
//! let map = PartitionMap::from_config("messages=1,users=1,projects=2,tasks=2");
//! assert_eq!(map.partition_for_table(&"messages".parse().unwrap()), PartitionId(1));
//! assert_eq!(map.partition_for_table(&"_tables".parse().unwrap()), PartitionId(0));
//! ```

use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc,
        RwLock,
    },
};

use anyhow::Context;
use async_trait::async_trait;
use serde::{
    Deserialize,
    Serialize,
};
use value::TableName;

use crate::write_log::WriteSource;

/// Identifies a partition (node) in the cluster.
/// Partition 0 is the default and owns all system tables.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId(pub u32);

impl PartitionId {
    /// The default partition that owns system tables and any unassigned
    /// user tables.
    pub const DEFAULT: Self = Self(0);
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "partition-{}", self.0)
    }
}

/// Monotonically identifies the placement metadata version used for routing.
///
/// Version 0 is the static, startup-configured map. Dynamic placement updates
/// must bump this value whenever ownership changes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlacementVersion(pub u64);

impl PlacementVersion {
    pub const STATIC: Self = Self(0);

    pub fn new(version: u64) -> Self {
        Self(version)
    }
}

impl From<PlacementVersion> for u64 {
    fn from(version: PlacementVersion) -> Self {
        version.0
    }
}

impl From<u64> for PlacementVersion {
    fn from(version: u64) -> Self {
        Self(version)
    }
}

impl fmt::Display for PlacementVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "placement-version-{}", self.0)
    }
}

/// Where a placement map was loaded from.
///
/// The static config source preserves today's env-var path. The replicated
/// source is the #130 target: placement metadata owned by the cluster rather
/// than by each process's startup environment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlacementMetadataSource {
    StaticConfig,
    Replicated,
}

/// Logical object owned by a partition.
///
/// We only route whole tables today. Keeping the target explicit prevents the
/// control-plane model from baking in table-only ownership when #130 evolves
/// toward range or key-prefix placement.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PlacementTarget {
    Table(TableName),
}

/// One ownership rule in the placement metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementRule {
    pub target: PlacementTarget,
    pub owner: PartitionId,
}

/// Static placement inputs from process config.
pub struct StaticPlacementConfig<'a> {
    pub table_assignments: &'a str,
    pub num_partitions: u32,
    pub placement_version: PlacementVersion,
}

/// Versioned placement metadata used to build runtime routing state.
///
/// This is the seam between the future replicated control-plane record and the
/// existing `PartitionMap` used by commit, routing, and 2PC paths.
///
/// In addition to table ownership, the record carries cluster *membership*: the
/// gRPC peer addresses for each partition. Carrying membership in the
/// replicated record is what lets a new partition be introduced by publishing a
/// new version rather than by hand-editing every node's `NODE_ADDRESSES` env
/// (issue #130).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementMetadata {
    source: PlacementMetadataSource,
    version: PlacementVersion,
    num_partitions: u32,
    rules: BTreeMap<PlacementTarget, PartitionId>,
    /// Partition → gRPC peer addresses. Empty when membership is supplied out
    /// of band by the static `NODE_ADDRESSES` env (the bootstrap/fallback
    /// path).
    members: BTreeMap<PartitionId, Vec<String>>,
}

impl PlacementMetadata {
    pub fn from_static_config(config: StaticPlacementConfig<'_>) -> Self {
        let mut rules = BTreeMap::new();
        if !config.table_assignments.is_empty() {
            for pair in config.table_assignments.split(',') {
                let pair = pair.trim();
                if let Some((table, partition)) = pair.split_once('=') {
                    let table = table.trim();
                    let partition = partition.trim();
                    if let (Ok(table_name), Ok(partition_id)) =
                        (table.parse::<TableName>(), partition.parse::<u32>())
                    {
                        rules.insert(
                            PlacementTarget::Table(table_name),
                            PartitionId(partition_id),
                        );
                    }
                }
            }
        }
        Self {
            source: PlacementMetadataSource::StaticConfig,
            version: config.placement_version,
            num_partitions: config.num_partitions,
            rules,
            members: BTreeMap::new(),
        }
    }

    pub fn from_partition_map(
        partition_map: &PartitionMap,
        source: PlacementMetadataSource,
    ) -> Self {
        Self {
            source,
            version: partition_map.placement_version(),
            num_partitions: partition_map.num_partitions(),
            rules: partition_map
                .assignments()
                .iter()
                .map(|(table, owner)| (PlacementTarget::Table(table.clone()), *owner))
                .collect(),
            members: BTreeMap::new(),
        }
    }

    pub fn from_table_assignments(
        source: PlacementMetadataSource,
        version: PlacementVersion,
        num_partitions: u32,
        assignments: BTreeMap<TableName, PartitionId>,
    ) -> Self {
        Self {
            source,
            version,
            num_partitions,
            rules: assignments
                .into_iter()
                .map(|(table, owner)| (PlacementTarget::Table(table), owner))
                .collect(),
            members: BTreeMap::new(),
        }
    }

    /// Attach cluster membership (partition → gRPC peer addresses) to the
    /// record.
    ///
    /// Blank addresses are dropped and partitions with no usable address are
    /// omitted so a half-filled env string cannot register an unroutable
    /// member.
    pub fn with_members(mut self, members: BTreeMap<PartitionId, Vec<String>>) -> Self {
        self.members = members
            .into_iter()
            .filter_map(|(partition, addresses)| {
                let addresses: Vec<String> = addresses
                    .into_iter()
                    .map(|addr| addr.trim().to_string())
                    .filter(|addr| !addr.is_empty())
                    .collect();
                (!addresses.is_empty()).then_some((partition, addresses))
            })
            .collect();
        self
    }

    pub fn source(&self) -> PlacementMetadataSource {
        self.source
    }

    /// Cluster membership: partition → gRPC peer addresses.
    pub fn members(&self) -> &BTreeMap<PartitionId, Vec<String>> {
        &self.members
    }

    pub fn version(&self) -> PlacementVersion {
        self.version
    }

    pub fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    pub fn rules(&self) -> Vec<PlacementRule> {
        self.rules
            .iter()
            .map(|(target, owner)| PlacementRule {
                target: target.clone(),
                owner: *owner,
            })
            .collect()
    }

    pub fn table_assignments(&self) -> BTreeMap<TableName, PartitionId> {
        self.rules
            .iter()
            .filter_map(|(target, owner)| match target {
                PlacementTarget::Table(table) => Some((table.clone(), *owner)),
            })
            .collect()
    }

    pub fn into_partition_map(&self, local_partition: PartitionId) -> PartitionMap {
        PartitionMap {
            assignments: self.table_assignments(),
            local_partition,
            num_partitions: self.num_partitions,
            placement_version: self.version,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SerializedPlacementMetadata {
    version: u64,
    num_partitions: u32,
    table_assignments: BTreeMap<String, u32>,
    /// Partition id (as string) → gRPC peer addresses. Defaulted so records
    /// written before membership existed still decode.
    #[serde(default)]
    members: BTreeMap<String, Vec<String>>,
}

impl From<&PlacementMetadata> for SerializedPlacementMetadata {
    fn from(metadata: &PlacementMetadata) -> Self {
        Self {
            version: u64::from(metadata.version()),
            num_partitions: metadata.num_partitions(),
            table_assignments: metadata
                .table_assignments()
                .into_iter()
                .map(|(table, partition)| (table.to_string(), partition.0))
                .collect(),
            members: metadata
                .members()
                .iter()
                .map(|(partition, addresses)| (partition.0.to_string(), addresses.clone()))
                .collect(),
        }
    }
}

impl TryFrom<SerializedPlacementMetadata> for PlacementMetadata {
    type Error = anyhow::Error;

    fn try_from(serialized: SerializedPlacementMetadata) -> anyhow::Result<Self> {
        let assignments = serialized
            .table_assignments
            .into_iter()
            .map(|(table, partition)| Ok((table.parse::<TableName>()?, PartitionId(partition))))
            .collect::<anyhow::Result<_>>()?;
        let members = serialized
            .members
            .into_iter()
            .map(|(partition, addresses)| {
                Ok((
                    PartitionId(partition.parse::<u32>().with_context(|| {
                        format!("Placement metadata: invalid partition id {partition:?} in members")
                    })?),
                    addresses,
                ))
            })
            .collect::<anyhow::Result<BTreeMap<PartitionId, Vec<String>>>>()?;
        Ok(PlacementMetadata::from_table_assignments(
            PlacementMetadataSource::Replicated,
            PlacementVersion::new(serialized.version),
            serialized.num_partitions,
            assignments,
        )
        .with_members(members))
    }
}

const PLACEMENT_KV_BUCKET: &str = "convex_placement";
const PLACEMENT_CURRENT_KEY: &str = "current";

#[async_trait]
pub trait PlacementMetadataStore: Send + Sync + 'static {
    async fn load(&self) -> anyhow::Result<Option<PlacementMetadata>>;
    async fn ensure_initialized(
        &self,
        bootstrap_metadata: PlacementMetadata,
    ) -> anyhow::Result<PlacementMetadata>;

    /// Publish a new placement version to the authoritative replicated record.
    ///
    /// This is the control-plane entrypoint for add-node, remove-node, and
    /// rebalance: an operator (or a future automated control plane) builds the
    /// next `PlacementMetadata` and publishes it. The new version must be
    /// strictly greater than the current one; the update is a compare-and-set
    /// so two concurrent publishers cannot silently clobber each other.
    async fn publish(&self, metadata: PlacementMetadata) -> anyhow::Result<PlacementMetadata>;
}

pub struct NatsPlacementMetadataStore {
    kv: async_nats::jetstream::kv::Store,
}

impl NatsPlacementMetadataStore {
    pub async fn connect(nats_url: &str) -> anyhow::Result<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = async_nats::connect(nats_url).await.with_context(|| {
            format!("Placement metadata: failed to connect to NATS at {nats_url}")
        })?;
        let jetstream = async_nats::jetstream::new(client);
        let kv = jetstream
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: PLACEMENT_KV_BUCKET.to_string(),
                history: 8,
                ..Default::default()
            })
            .await
            .context("Placement metadata: failed to create KV bucket")?;
        Ok(Self { kv })
    }

    fn encode(metadata: &PlacementMetadata) -> anyhow::Result<Vec<u8>> {
        serde_json::to_vec(&SerializedPlacementMetadata::from(metadata))
            .context("Placement metadata: failed to serialize metadata")
    }

    fn decode(bytes: &[u8]) -> anyhow::Result<PlacementMetadata> {
        let serialized: SerializedPlacementMetadata = serde_json::from_slice(bytes)
            .context("Placement metadata: failed to parse metadata")?;
        serialized.try_into()
    }
}

#[async_trait]
impl PlacementMetadataStore for NatsPlacementMetadataStore {
    async fn load(&self) -> anyhow::Result<Option<PlacementMetadata>> {
        let Some(entry) = self
            .kv
            .entry(PLACEMENT_CURRENT_KEY)
            .await
            .context("Placement metadata: failed to read current metadata")?
        else {
            return Ok(None);
        };
        Ok(Some(Self::decode(&entry.value)?))
    }

    async fn ensure_initialized(
        &self,
        bootstrap_metadata: PlacementMetadata,
    ) -> anyhow::Result<PlacementMetadata> {
        if let Some(metadata) = self.load().await? {
            return Ok(metadata);
        }

        let payload = Self::encode(&bootstrap_metadata)?;
        match self.kv.create(PLACEMENT_CURRENT_KEY, payload.into()).await {
            Ok(_) => self
                .load()
                .await?
                .context("Placement metadata: current metadata missing after initialize"),
            Err(_) => self
                .load()
                .await?
                .context("Placement metadata: current metadata missing after initialize race"),
        }
    }

    async fn publish(&self, metadata: PlacementMetadata) -> anyhow::Result<PlacementMetadata> {
        let payload = Self::encode(&metadata)?;
        let entry = self
            .kv
            .entry(PLACEMENT_CURRENT_KEY)
            .await
            .context("Placement metadata: failed to read current metadata before publish")?;
        match entry {
            Some(entry) => {
                let current = Self::decode(&entry.value)?;
                anyhow::ensure!(
                    metadata.version() > current.version(),
                    "Refusing to publish placement version {} over current {}; the new version \
                     must be strictly greater",
                    metadata.version(),
                    current.version(),
                );
                // Compare-and-set against the observed revision so a concurrent
                // publisher fails instead of silently clobbering this update.
                self.kv
                    .update(PLACEMENT_CURRENT_KEY, payload.into(), entry.revision)
                    .await
                    .context(
                        "Placement metadata: publish failed; another publisher likely raced this \
                         update — reload and retry",
                    )?;
            },
            None => {
                self.kv
                    .create(PLACEMENT_CURRENT_KEY, payload.into())
                    .await
                    .context("Placement metadata: failed to create initial published metadata")?;
            },
        }
        self.load()
            .await?
            .context("Placement metadata: current metadata missing after publish")
    }
}

/// A component that routes with placement metadata and can adopt a newer
/// version at runtime.
///
/// Implemented by `PlacementState` (used directly in tests) and by the
/// committer client (used in production). The refresh loop is written against
/// this trait so its decision logic — "only install strictly newer versions" —
/// can be exercised without a running committer.
pub trait PlacementRefreshTarget: Send + Sync {
    fn current_placement_version(&self) -> PlacementVersion;
    fn install_placement_metadata(&self, metadata: PlacementMetadata) -> anyhow::Result<()>;
}

impl PlacementRefreshTarget for PlacementState {
    fn current_placement_version(&self) -> PlacementVersion {
        self.placement_version()
    }

    fn install_placement_metadata(&self, metadata: PlacementMetadata) -> anyhow::Result<()> {
        self.refresh(metadata)
    }
}

/// Load the authoritative placement record once and install it if it is newer
/// than what the target is currently routing with.
///
/// Returns the version that was installed, or `None` when nothing changed
/// (no record yet, or the record is not newer). This is the body of the
/// background refresh loop and the unit under test for stale-version detection
/// (issue #130, acceptance criterion 2). Equal versions are intentionally a
/// no-op so an idle cluster does not churn the routing lock.
pub async fn refresh_placement_once(
    store: &dyn PlacementMetadataStore,
    target: &dyn PlacementRefreshTarget,
) -> anyhow::Result<Option<PlacementVersion>> {
    let Some(latest) = store.load().await? else {
        return Ok(None);
    };
    if latest.version() <= target.current_placement_version() {
        return Ok(None);
    }
    let new_version = latest.version();
    target.install_placement_metadata(latest)?;
    crate::metrics::log_placement_metadata_refresh(new_version.into());
    Ok(Some(new_version))
}

/// Refreshable placement state shared by routing components.
///
/// Callers take a `PartitionMap` snapshot before classifying or coordinating a
/// transaction. That keeps a single transaction internally consistent even if a
/// newer placement version is installed concurrently.
#[derive(Clone, Debug)]
pub struct PlacementState {
    local_partition: PartitionId,
    metadata: Arc<RwLock<PlacementMetadata>>,
}

impl PlacementState {
    pub fn new(local_partition: PartitionId, metadata: PlacementMetadata) -> anyhow::Result<Self> {
        Self::validate_local_partition(local_partition, metadata.num_partitions())?;
        Ok(Self {
            local_partition,
            metadata: Arc::new(RwLock::new(metadata)),
        })
    }

    pub fn from_partition_map(partition_map: PartitionMap) -> Self {
        let metadata = PlacementMetadata::from_partition_map(
            &partition_map,
            PlacementMetadataSource::StaticConfig,
        );
        Self {
            local_partition: partition_map.local_partition(),
            metadata: Arc::new(RwLock::new(metadata)),
        }
    }

    pub fn partition_map(&self) -> PartitionMap {
        self.metadata
            .read()
            .expect("placement metadata lock poisoned")
            .into_partition_map(self.local_partition)
    }

    pub fn refresh(&self, metadata: PlacementMetadata) -> anyhow::Result<()> {
        Self::validate_local_partition(self.local_partition, metadata.num_partitions())?;
        let mut current = self
            .metadata
            .write()
            .expect("placement metadata lock poisoned");
        anyhow::ensure!(
            metadata.version() >= current.version(),
            "Refusing to refresh placement metadata from {} down to {}",
            current.version(),
            metadata.version(),
        );
        *current = metadata;
        Ok(())
    }

    pub fn placement_version(&self) -> PlacementVersion {
        self.metadata
            .read()
            .expect("placement metadata lock poisoned")
            .version()
    }

    /// Snapshot the cluster membership as `NodeAddresses` for 2PC routing.
    ///
    /// Returns `None` when the replicated record carries no membership, so the
    /// caller can fall back to the static `NODE_ADDRESSES` env. The snapshot is
    /// taken under the same lock as routing reads, so a concurrent placement
    /// refresh cannot change addresses halfway through routing one transaction.
    pub fn node_addresses(&self) -> Option<crate::two_phase::NodeAddresses> {
        let members = self
            .metadata
            .read()
            .expect("placement metadata lock poisoned")
            .members()
            .clone();
        (!members.is_empty()).then(|| crate::two_phase::NodeAddresses::from_map(members))
    }

    pub fn local_partition(&self) -> PartitionId {
        self.local_partition
    }

    pub fn num_partitions(&self) -> u32 {
        self.metadata
            .read()
            .expect("placement metadata lock poisoned")
            .num_partitions()
    }

    fn validate_local_partition(
        local_partition: PartitionId,
        num_partitions: u32,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            local_partition.0 < num_partitions,
            "Local partition {} is outside placement metadata with {} partitions",
            local_partition,
            num_partitions,
        );
        Ok(())
    }
}

/// Maps table names to partitions.
///
/// System tables (starting with `_`) are always on partition 0.
/// User tables are assigned based on the configuration.
/// Unassigned user tables default to partition 0.
#[derive(Clone, Debug)]
pub struct PartitionMap {
    /// Explicit table → partition assignments.
    assignments: BTreeMap<TableName, PartitionId>,
    /// The partition this node owns.
    local_partition: PartitionId,
    /// Total number of partitions in the cluster.
    num_partitions: u32,
    /// Version of the placement metadata used to build this map.
    placement_version: PlacementVersion,
}

impl PartitionMap {
    /// Create a partition map from a config string.
    ///
    /// Format: `"table1=1,table2=1,table3=2,table4=2"`
    ///
    /// Tables not listed default to partition 0.
    /// System tables (starting with `_`) are always partition 0 regardless.
    pub fn from_config(config: &str, local_partition: PartitionId, num_partitions: u32) -> Self {
        Self::from_config_with_version(
            config,
            local_partition,
            num_partitions,
            PlacementVersion::STATIC,
        )
    }

    /// Create a partition map from a config string and placement version.
    pub fn from_config_with_version(
        config: &str,
        local_partition: PartitionId,
        num_partitions: u32,
        placement_version: PlacementVersion,
    ) -> Self {
        PlacementMetadata::from_static_config(StaticPlacementConfig {
            table_assignments: config,
            num_partitions,
            placement_version,
        })
        .into_partition_map(local_partition)
    }

    /// Create a single-partition map (all tables on partition 0).
    /// Used for single-node deployments and backward compatibility.
    pub fn single_partition() -> Self {
        Self {
            assignments: BTreeMap::new(),
            local_partition: PartitionId::DEFAULT,
            num_partitions: 1,
            placement_version: PlacementVersion::STATIC,
        }
    }

    /// Get the partition that owns the given table.
    ///
    /// System tables (starting with `_`) always return partition 0.
    /// User tables return their configured partition, or partition 0 if
    /// not explicitly assigned.
    pub fn partition_for_table(&self, table: &TableName) -> PartitionId {
        // System tables always on partition 0.
        if table.is_system() {
            return PartitionId::DEFAULT;
        }
        self.assignments
            .get(table)
            .copied()
            .unwrap_or(PartitionId::DEFAULT)
    }

    /// Check if this node owns the given table.
    pub fn is_local(&self, table: &TableName) -> bool {
        self.partition_for_table(table) == self.local_partition
    }

    /// Get all tables assigned to a specific partition.
    pub fn tables_for_partition(&self, partition: PartitionId) -> Vec<TableName> {
        self.assignments
            .iter()
            .filter(|(_, p)| **p == partition)
            .map(|(t, _)| t.clone())
            .collect()
    }

    /// Get this node's partition ID.
    pub fn local_partition(&self) -> PartitionId {
        self.local_partition
    }

    /// Get the total number of partitions.
    pub fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    /// Get the placement metadata version.
    pub fn placement_version(&self) -> PlacementVersion {
        self.placement_version
    }

    /// Get all partition IDs in the cluster.
    pub fn all_partitions(&self) -> Vec<PartitionId> {
        (0..self.num_partitions).map(PartitionId).collect()
    }

    /// Get all explicit assignments.
    pub fn assignments(&self) -> &BTreeMap<TableName, PartitionId> {
        &self.assignments
    }
}

pub fn uses_metadata_owner(write_source: &WriteSource) -> bool {
    matches!(write_source.as_str(), Some("start_push" | "finish_push"))
}

/// Returns the authoritative partition for tables that should be coordinated
/// cluster-wide for the current operation.
///
/// - User tables always have a single partition owner.
/// - Deploy metadata operations (`start_push`, `finish_push`) route system
///   tables to the metadata owner on partition 0 (TiDB DDL owner pattern).
/// - Other system-table writes are node-local operational state and therefore
///   return `None`.
pub fn routed_partition_for_table(
    table: &TableName,
    partition_map: &PartitionMap,
    write_source: &WriteSource,
) -> Option<PartitionId> {
    if !table.is_system() || uses_metadata_owner(write_source) {
        Some(partition_map.partition_for_table(table))
    } else {
        None
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod testing {
    use std::sync::{
        Arc,
        Mutex,
    };

    use async_trait::async_trait;

    use super::{
        PlacementMetadata,
        PlacementMetadataStore,
    };

    /// In-memory `PlacementMetadataStore` for tests.
    ///
    /// Mirrors the NATS store's contract: `ensure_initialized` is
    /// first-write-wins and `publish` requires a strictly greater version, so
    /// the refresh loop and control-plane publish path can be exercised without
    /// NATS.
    #[derive(Clone, Default)]
    pub struct InMemoryPlacementMetadataStore {
        current: Arc<Mutex<Option<PlacementMetadata>>>,
    }

    impl InMemoryPlacementMetadataStore {
        pub fn new() -> Self {
            Self::default()
        }
    }

    #[async_trait]
    impl PlacementMetadataStore for InMemoryPlacementMetadataStore {
        async fn load(&self) -> anyhow::Result<Option<PlacementMetadata>> {
            Ok(self.current.lock().unwrap().clone())
        }

        async fn ensure_initialized(
            &self,
            bootstrap_metadata: PlacementMetadata,
        ) -> anyhow::Result<PlacementMetadata> {
            let mut current = self.current.lock().unwrap();
            Ok(current.get_or_insert(bootstrap_metadata).clone())
        }

        async fn publish(&self, metadata: PlacementMetadata) -> anyhow::Result<PlacementMetadata> {
            let mut current = self.current.lock().unwrap();
            if let Some(existing) = current.as_ref() {
                anyhow::ensure!(
                    metadata.version() > existing.version(),
                    "Refusing to publish placement version {} over current {}",
                    metadata.version(),
                    existing.version(),
                );
            }
            *current = Some(metadata.clone());
            Ok(metadata)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        testing::InMemoryPlacementMetadataStore,
        *,
    };
    use crate::write_log::WriteSource;

    #[test]
    fn test_single_partition() {
        let map = PartitionMap::single_partition();
        assert_eq!(
            map.partition_for_table(&"messages".parse().unwrap()),
            PartitionId::DEFAULT
        );
        assert_eq!(
            map.partition_for_table(&"_tables".parse().unwrap()),
            PartitionId::DEFAULT
        );
        assert!(map.is_local(&"anything".parse().unwrap()));
    }

    #[test]
    fn test_multi_partition() {
        let map =
            PartitionMap::from_config("messages=1,users=1,projects=2,tasks=2", PartitionId(1), 3);

        // Assigned tables.
        assert_eq!(
            map.partition_for_table(&"messages".parse().unwrap()),
            PartitionId(1)
        );
        assert_eq!(
            map.partition_for_table(&"users".parse().unwrap()),
            PartitionId(1)
        );
        assert_eq!(
            map.partition_for_table(&"projects".parse().unwrap()),
            PartitionId(2)
        );
        assert_eq!(
            map.partition_for_table(&"tasks".parse().unwrap()),
            PartitionId(2)
        );

        // Unassigned user table defaults to partition 0.
        assert_eq!(
            map.partition_for_table(&"other".parse().unwrap()),
            PartitionId::DEFAULT
        );

        // System tables always partition 0.
        assert_eq!(
            map.partition_for_table(&"_tables".parse().unwrap()),
            PartitionId::DEFAULT
        );
        assert_eq!(
            map.partition_for_table(&"_modules".parse().unwrap()),
            PartitionId::DEFAULT
        );

        // Local ownership.
        assert!(map.is_local(&"messages".parse().unwrap()));
        assert!(map.is_local(&"users".parse().unwrap()));
        assert!(!map.is_local(&"projects".parse().unwrap()));
        assert!(!map.is_local(&"tasks".parse().unwrap()));
        // System tables are on partition 0, not partition 1.
        assert!(!map.is_local(&"_tables".parse().unwrap()));

        // Tables for partition.
        let p1_tables = map.tables_for_partition(PartitionId(1));
        assert_eq!(p1_tables.len(), 2);
        let p2_tables = map.tables_for_partition(PartitionId(2));
        assert_eq!(p2_tables.len(), 2);
    }

    #[test]
    fn test_empty_config() {
        let map = PartitionMap::from_config("", PartitionId(0), 1);
        assert_eq!(
            map.partition_for_table(&"messages".parse().unwrap()),
            PartitionId::DEFAULT
        );
    }

    #[test]
    fn test_all_partitions() {
        let map = PartitionMap::from_config("a=1,b=2", PartitionId(0), 3);
        let all = map.all_partitions();
        assert_eq!(all, vec![PartitionId(0), PartitionId(1), PartitionId(2)]);
    }

    #[test]
    fn test_placement_version_defaults_to_static() {
        let map = PartitionMap::from_config("a=1", PartitionId(0), 2);
        assert_eq!(map.placement_version(), PlacementVersion::STATIC);
    }

    #[test]
    fn test_placement_version_from_config() {
        let map = PartitionMap::from_config_with_version(
            "a=1",
            PartitionId(0),
            2,
            PlacementVersion::new(42),
        );
        assert_eq!(map.placement_version(), PlacementVersion::new(42));
    }

    #[test]
    fn test_static_placement_metadata_builds_runtime_partition_map() {
        let metadata = PlacementMetadata::from_static_config(StaticPlacementConfig {
            table_assignments: "messages=0,projects=1",
            num_partitions: 2,
            placement_version: PlacementVersion::new(7),
        });

        assert_eq!(metadata.source(), PlacementMetadataSource::StaticConfig);
        assert_eq!(metadata.version(), PlacementVersion::new(7));
        assert_eq!(metadata.num_partitions(), 2);
        assert_eq!(
            metadata
                .table_assignments()
                .get(&"projects".parse().unwrap()),
            Some(&PartitionId(1)),
        );

        let map = metadata.into_partition_map(PartitionId(1));
        assert_eq!(
            map.partition_for_table(&"messages".parse().unwrap()),
            PartitionId(0),
        );
        assert_eq!(
            map.partition_for_table(&"projects".parse().unwrap()),
            PartitionId(1),
        );
        assert!(map.is_local(&"projects".parse().unwrap()));
        assert_eq!(map.placement_version(), PlacementVersion::new(7));
    }

    #[test]
    fn test_static_placement_metadata_keeps_table_targets_explicit() {
        let metadata = PlacementMetadata::from_static_config(StaticPlacementConfig {
            table_assignments: "messages=0,projects=1",
            num_partitions: 2,
            placement_version: PlacementVersion::new(3),
        });
        let rules = metadata.rules();

        assert!(rules.contains(&PlacementRule {
            target: PlacementTarget::Table("messages".parse().unwrap()),
            owner: PartitionId(0),
        }));
        assert!(rules.contains(&PlacementRule {
            target: PlacementTarget::Table("projects".parse().unwrap()),
            owner: PartitionId(1),
        }));
    }

    #[test]
    fn test_placement_state_refreshes_to_newer_metadata() -> anyhow::Result<()> {
        let state = PlacementState::new(
            PartitionId(1),
            PlacementMetadata::from_static_config(StaticPlacementConfig {
                table_assignments: "messages=0,projects=1",
                num_partitions: 2,
                placement_version: PlacementVersion::new(1),
            }),
        )?;

        state.refresh(PlacementMetadata::from_static_config(
            StaticPlacementConfig {
                table_assignments: "messages=1,projects=0",
                num_partitions: 2,
                placement_version: PlacementVersion::new(2),
            },
        ))?;

        let map = state.partition_map();
        assert_eq!(state.placement_version(), PlacementVersion::new(2));
        assert_eq!(
            map.partition_for_table(&"messages".parse().unwrap()),
            PartitionId(1),
        );
        assert_eq!(
            map.partition_for_table(&"projects".parse().unwrap()),
            PartitionId(0),
        );
        Ok(())
    }

    #[test]
    fn test_placement_state_rejects_version_downgrade() -> anyhow::Result<()> {
        let state = PlacementState::new(
            PartitionId(0),
            PlacementMetadata::from_static_config(StaticPlacementConfig {
                table_assignments: "messages=0",
                num_partitions: 2,
                placement_version: PlacementVersion::new(3),
            }),
        )?;

        let err = state
            .refresh(PlacementMetadata::from_static_config(
                StaticPlacementConfig {
                    table_assignments: "messages=1",
                    num_partitions: 2,
                    placement_version: PlacementVersion::new(2),
                },
            ))
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("Refusing to refresh placement metadata"));
        assert_eq!(state.placement_version(), PlacementVersion::new(3));
        Ok(())
    }

    #[test]
    fn test_placement_metadata_serialization_loads_as_replicated() -> anyhow::Result<()> {
        let metadata = PlacementMetadata::from_static_config(StaticPlacementConfig {
            table_assignments: "messages=0,projects=1",
            num_partitions: 2,
            placement_version: PlacementVersion::new(9),
        });

        let bytes = NatsPlacementMetadataStore::encode(&metadata)?;
        let decoded = NatsPlacementMetadataStore::decode(&bytes)?;

        assert_eq!(decoded.source(), PlacementMetadataSource::Replicated);
        assert_eq!(decoded.version(), PlacementVersion::new(9));
        assert_eq!(
            decoded
                .table_assignments()
                .get(&"projects".parse().unwrap()),
            Some(&PartitionId(1)),
        );
        Ok(())
    }

    fn members(entries: &[(u32, &str)]) -> BTreeMap<PartitionId, Vec<String>> {
        entries
            .iter()
            .map(|(partition, addr)| (PartitionId(*partition), vec![addr.to_string()]))
            .collect()
    }

    #[test]
    fn test_with_members_drops_blank_addresses() {
        let metadata = PlacementMetadata::from_static_config(StaticPlacementConfig {
            table_assignments: "messages=1",
            num_partitions: 2,
            placement_version: PlacementVersion::new(1),
        })
        .with_members(BTreeMap::from([
            (PartitionId(0), vec!["  node-a:50051 ".to_string()]),
            (PartitionId(1), vec!["".to_string(), "   ".to_string()]),
        ]));

        // Partition 0's address is trimmed; partition 1 had only blanks and is
        // dropped so it cannot register as an unroutable member.
        assert_eq!(
            metadata.members().get(&PartitionId(0)),
            Some(&vec!["node-a:50051".to_string()])
        );
        assert_eq!(metadata.members().get(&PartitionId(1)), None);
    }

    #[test]
    fn test_placement_metadata_serialization_round_trips_members() -> anyhow::Result<()> {
        let metadata = PlacementMetadata::from_static_config(StaticPlacementConfig {
            table_assignments: "messages=0,projects=1",
            num_partitions: 2,
            placement_version: PlacementVersion::new(4),
        })
        .with_members(members(&[(0, "node-a:50051"), (1, "node-b:50051")]));

        let bytes = NatsPlacementMetadataStore::encode(&metadata)?;
        let decoded = NatsPlacementMetadataStore::decode(&bytes)?;

        assert_eq!(decoded.members(), metadata.members());
        Ok(())
    }

    #[test]
    fn test_placement_metadata_decodes_legacy_record_without_members() -> anyhow::Result<()> {
        // A record written before membership existed has no `members` field.
        let legacy = br#"{"version":2,"numPartitions":2,"tableAssignments":{"messages":1}}"#;
        let decoded = NatsPlacementMetadataStore::decode(legacy)?;

        assert_eq!(decoded.version(), PlacementVersion::new(2));
        assert!(decoded.members().is_empty());
        assert_eq!(
            decoded
                .table_assignments()
                .get(&"messages".parse().unwrap()),
            Some(&PartitionId(1))
        );
        Ok(())
    }

    #[test]
    fn test_placement_state_node_addresses_snapshot() -> anyhow::Result<()> {
        let with_members = PlacementMetadata::from_static_config(StaticPlacementConfig {
            table_assignments: "messages=1",
            num_partitions: 2,
            placement_version: PlacementVersion::new(1),
        })
        .with_members(members(&[(0, "node-a:50051"), (1, "node-b:50051")]));
        let state = PlacementState::new(PartitionId(0), with_members)?;

        let addresses = state
            .node_addresses()
            .expect("membership should yield node addresses");
        assert_eq!(addresses.address_for(PartitionId(1)), Some("node-b:50051"));

        // With no membership, the snapshot is None so callers fall back to env.
        let no_members = PlacementState::new(
            PartitionId(0),
            PlacementMetadata::from_static_config(StaticPlacementConfig {
                table_assignments: "messages=1",
                num_partitions: 2,
                placement_version: PlacementVersion::new(1),
            }),
        )?;
        assert!(no_members.node_addresses().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_refresh_placement_once_installs_newer_version() -> anyhow::Result<()> {
        let store = InMemoryPlacementMetadataStore::new();
        store
            .ensure_initialized(PlacementMetadata::from_static_config(
                StaticPlacementConfig {
                    table_assignments: "messages=0",
                    num_partitions: 2,
                    placement_version: PlacementVersion::new(1),
                },
            ))
            .await?;
        let state = PlacementState::new(
            PartitionId(0),
            PlacementMetadata::from_static_config(StaticPlacementConfig {
                table_assignments: "messages=0",
                num_partitions: 2,
                placement_version: PlacementVersion::new(1),
            }),
        )?;

        // Equal version is a no-op.
        assert_eq!(refresh_placement_once(&store, &state).await?, None);

        // Operator publishes a newer version moving `messages` to partition 1.
        store
            .publish(PlacementMetadata::from_static_config(
                StaticPlacementConfig {
                    table_assignments: "messages=1",
                    num_partitions: 2,
                    placement_version: PlacementVersion::new(2),
                },
            ))
            .await?;

        assert_eq!(
            refresh_placement_once(&store, &state).await?,
            Some(PlacementVersion::new(2))
        );
        assert_eq!(state.placement_version(), PlacementVersion::new(2));
        assert_eq!(
            state
                .partition_map()
                .partition_for_table(&"messages".parse().unwrap()),
            PartitionId(1)
        );

        // A second refresh with nothing new is a no-op.
        assert_eq!(refresh_placement_once(&store, &state).await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn test_refresh_placement_once_no_record_is_noop() -> anyhow::Result<()> {
        let store = InMemoryPlacementMetadataStore::new();
        let state = PlacementState::new(
            PartitionId(0),
            PlacementMetadata::from_static_config(StaticPlacementConfig {
                table_assignments: "messages=0",
                num_partitions: 2,
                placement_version: PlacementVersion::new(1),
            }),
        )?;
        assert_eq!(refresh_placement_once(&store, &state).await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn test_publish_rejects_non_increasing_version() -> anyhow::Result<()> {
        let store = InMemoryPlacementMetadataStore::new();
        store
            .ensure_initialized(PlacementMetadata::from_static_config(
                StaticPlacementConfig {
                    table_assignments: "messages=0",
                    num_partitions: 2,
                    placement_version: PlacementVersion::new(5),
                },
            ))
            .await?;

        let err = store
            .publish(PlacementMetadata::from_static_config(
                StaticPlacementConfig {
                    table_assignments: "messages=1",
                    num_partitions: 2,
                    placement_version: PlacementVersion::new(5),
                },
            ))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Refusing to publish"));
        Ok(())
    }

    #[test]
    fn test_routed_partition_for_table_keeps_non_deploy_system_tables_local() {
        let map = PartitionMap::from_config("messages=1", PartitionId(1), 2);
        assert_eq!(
            routed_partition_for_table(
                &"_modules".parse().unwrap(),
                &map,
                &WriteSource::new("cron_tick"),
            ),
            None
        );
    }

    #[test]
    fn test_routed_partition_for_table_routes_deploy_system_tables_to_owner() {
        let map = PartitionMap::from_config("messages=1", PartitionId(1), 2);
        assert_eq!(
            routed_partition_for_table(
                &"_modules".parse().unwrap(),
                &map,
                &WriteSource::new("finish_push"),
            ),
            Some(PartitionId::DEFAULT)
        );
    }
}
