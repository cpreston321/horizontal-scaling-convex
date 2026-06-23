//! Selective delivery: best-effort fanout reduction for replication deltas.
//!
//! # Correctness boundary (issue #133)
//!
//! Selective delivery tracks, per node, which tables that node currently has
//! live subscriptions for, so a publisher *can* send a user-table delta only to
//! the nodes that care about it instead of broadcasting to everyone.
//!
//! This is strictly an **optimization**. It must never be the sole thing
//! responsible for getting a delta to a node, because interest registrations
//! are soft state: they expire ([`INTEREST_MAX_AGE`]), they are absent until a
//! freshly started node first publishes them, and a publisher's cached view of
//! them can lag or be lost across a NATS reconnect. If selective delivery were
//! the only path, any of those would silently drop a delta a node needed for
//! subscription invalidation or frontier correctness — fast, but wrong.
//!
//! The rules this module is built to uphold:
//!
//! - **Correctness-critical replication/invalidation rides the broadcast
//!   partition subjects**, which deliver every delta regardless of interest.
//!   That path is owned by `NatsDistributedLog`'s partition-subject publish and
//!   the broadcast consumer, not by this registry.
//! - **Uncertainty fails safe by over-delivering.** Absence or staleness of an
//!   interest record means "deliver anyway", never "skip". Convex tolerates
//!   conservative extra invalidations; it cannot tolerate a missed one.
//! - This registry only ever *narrows* a delivery that the broadcast path
//!   already guarantees. It is wired in behind
//!   `SELECTIVE_DELIVERY_TRUST_INTEREST` (default off) so it cannot become a
//!   node's only source of deltas until distributed reactive invalidation
//!   (#132) proves that is safe.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    sync::Arc,
    time::{
        Duration,
        SystemTime,
        UNIX_EPOCH,
    },
};

use anyhow::Context;
use async_nats::jetstream::{
    self,
    kv::{
        Operation,
        Store,
    },
};
use futures::StreamExt;
use parking_lot::RwLock;
use serde::{
    Deserialize,
    Serialize,
};
use value::TableName;

pub const SELECTIVE_DELIVERY_KV_BUCKET: &str = "convex_delta_interest";
pub const NODE_SUBJECT_PREFIX: &str = "convex.commits.node";
const INTEREST_MAX_AGE: Duration = Duration::from_secs(90);

#[derive(Clone)]
pub struct SelectiveDeliveryRegistry {
    node_name: String,
    kv: Store,
    cache: Arc<RwLock<BTreeMap<String, InterestRegistration>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct InterestRegistration {
    tables: Vec<String>,
    updated_at_ms: u64,
}

impl SelectiveDeliveryRegistry {
    pub async fn connect(nats_url: &str, node_name: String) -> anyhow::Result<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = async_nats::connect(nats_url)
            .await
            .with_context(|| format!("Failed to connect to NATS at {nats_url}"))?;
        Self::from_client(client, node_name).await
    }

    pub async fn from_client(
        client: async_nats::Client,
        node_name: String,
    ) -> anyhow::Result<Self> {
        let jetstream = jetstream::new(client);
        let kv = jetstream
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: SELECTIVE_DELIVERY_KV_BUCKET.to_string(),
                history: 1,
                max_age: INTEREST_MAX_AGE.saturating_mul(4),
                ..Default::default()
            })
            .await
            .context("Failed to create selective-delivery KV bucket")?;

        let cache = Arc::new(RwLock::new(BTreeMap::new()));
        Self::prime_cache(&kv, &cache).await?;
        Self::spawn_watcher(kv.clone(), cache.clone());

        Ok(Self {
            node_name,
            kv,
            cache,
        })
    }

    pub async fn publish_local_interest(&self, tables: &BTreeSet<TableName>) -> anyhow::Result<()> {
        let registration = InterestRegistration {
            tables: tables.iter().map(ToString::to_string).collect(),
            updated_at_ms: now_ms(),
        };
        let payload = serde_json::to_vec(&registration)
            .context("Failed to serialize selective-delivery interest registration")?;
        self.kv
            .put(&self.node_name, payload.into())
            .await
            .with_context(|| {
                format!(
                    "Failed to publish selective-delivery interest for {}",
                    self.node_name
                )
            })?;
        self.cache
            .write()
            .insert(self.node_name.clone(), registration);
        Ok(())
    }

    pub fn interested_nodes_for_tables(&self, tables: &BTreeSet<TableName>) -> Vec<String> {
        interested_nodes_for_tables_from_cache(&self.cache.read(), now_ms(), tables)
    }

    /// Like [`Self::interested_nodes_for_tables`], but also reports how many
    /// node registrations were known and how many were skipped as stale,
    /// and emits the selective-delivery health metrics (issue #133).
    ///
    /// The stale count is the signal that selective delivery is at risk of
    /// under-delivering: a stale registration is one the publisher can no
    /// longer trust, so the correctness-critical broadcast path must remain
    /// in place.
    pub fn interested_nodes_with_health(&self, tables: &BTreeSet<TableName>) -> InterestSelection {
        let selection = {
            let cache = self.cache.read();
            interest_selection_from_cache(&cache, now_ms(), tables)
        };
        crate::metrics::log_selective_delivery_stale_registrations(selection.stale_nodes);
        crate::metrics::log_selective_delivery_interest_hit(
            selection.known_nodes,
            selection.interested.len(),
        );
        selection
    }

    pub fn node_subject(node_name: &str) -> String {
        format!("{NODE_SUBJECT_PREFIX}.{node_name}")
    }

    async fn prime_cache(
        kv: &Store,
        cache: &Arc<RwLock<BTreeMap<String, InterestRegistration>>>,
    ) -> anyhow::Result<()> {
        let keys = kv
            .keys()
            .await
            .context("Failed to list selective-delivery keys")?;
        let keys: Vec<_> = keys
            .filter_map(|result| async move { result.ok() })
            .collect()
            .await;
        for key in keys {
            let Some(entry) = kv
                .entry(&key)
                .await
                .with_context(|| format!("Failed to read selective-delivery entry {key}"))?
            else {
                continue;
            };
            if let Ok(registration) = serde_json::from_slice::<InterestRegistration>(&entry.value) {
                cache.write().insert(key, registration);
            }
        }
        Ok(())
    }

    fn spawn_watcher(kv: Store, cache: Arc<RwLock<BTreeMap<String, InterestRegistration>>>) {
        tokio::spawn(async move {
            let mut watch = match kv.watch_all().await {
                Ok(watch) => watch,
                Err(e) => {
                    tracing::warn!("Selective-delivery registry watch failed to start: {e:#}");
                    return;
                },
            };
            while let Some(result) = watch.next().await {
                match result {
                    Ok(entry) => match entry.operation {
                        Operation::Put => {
                            match serde_json::from_slice::<InterestRegistration>(&entry.value) {
                                Ok(registration) => {
                                    cache.write().insert(entry.key, registration);
                                },
                                Err(e) => {
                                    tracing::warn!(
                                        "Failed to decode selective-delivery interest entry {}: \
                                         {e:#}",
                                        entry.key
                                    );
                                },
                            }
                        },
                        Operation::Delete | Operation::Purge => {
                            cache.write().remove(&entry.key);
                        },
                    },
                    Err(e) => {
                        tracing::warn!("Selective-delivery registry watch error: {e:#}");
                    },
                }
            }
        });
    }
}

/// Result of evaluating which nodes a delta should be shadow-delivered to,
/// along with the health counters used to reason about whether selective
/// delivery is safely narrowing or at risk of under-delivering.
#[derive(Clone, Debug)]
pub struct InterestSelection {
    /// Nodes with a fresh interest registration matching the touched tables.
    pub interested: Vec<String>,
    /// Total node registrations known to this publisher (fresh or stale).
    pub known_nodes: usize,
    /// Registrations skipped because they were older than [`INTEREST_MAX_AGE`].
    pub stale_nodes: usize,
}

fn interested_nodes_for_tables_from_cache(
    cache: &BTreeMap<String, InterestRegistration>,
    now_ms: u64,
    tables: &BTreeSet<TableName>,
) -> Vec<String> {
    if tables.is_empty() {
        return Vec::new();
    }
    cache
        .iter()
        .filter(|(_, registration)| registration.is_fresh(now_ms))
        .filter(|(_, registration)| registration.matches(tables))
        .map(|(node, _)| node.clone())
        .collect()
}

fn interest_selection_from_cache(
    cache: &BTreeMap<String, InterestRegistration>,
    now_ms: u64,
    tables: &BTreeSet<TableName>,
) -> InterestSelection {
    InterestSelection {
        interested: interested_nodes_for_tables_from_cache(cache, now_ms, tables),
        known_nodes: cache.len(),
        stale_nodes: cache
            .values()
            .filter(|registration| !registration.is_fresh(now_ms))
            .count(),
    }
}

impl InterestRegistration {
    fn is_fresh(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.updated_at_ms) <= INTEREST_MAX_AGE.as_millis() as u64
    }

    fn matches(&self, tables: &BTreeSet<TableName>) -> bool {
        self.tables.iter().any(|table| {
            table
                .parse::<TableName>()
                .ok()
                .is_some_and(|table_name| tables.contains(&table_name))
        })
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::{
        BTreeMap,
        BTreeSet,
    };

    use value::TableName;

    use crate::selective_delivery::{
        interest_selection_from_cache,
        interested_nodes_for_tables_from_cache,
        InterestRegistration,
        SelectiveDeliveryRegistry,
        INTEREST_MAX_AGE,
    };

    fn registration(tables: &[&str], updated_at_ms: u64) -> InterestRegistration {
        InterestRegistration {
            tables: tables.iter().map(ToString::to_string).collect(),
            updated_at_ms,
        }
    }

    #[test]
    fn interested_nodes_match_tables_and_skip_stale_entries() {
        let now_ms = 1_000_000;
        let messages: TableName = "messages".parse().unwrap();
        let tasks: TableName = "tasks".parse().unwrap();
        let cache = BTreeMap::from([
            (
                "node-a".to_string(),
                InterestRegistration {
                    tables: vec!["messages".to_string()],
                    updated_at_ms: now_ms,
                },
            ),
            (
                "node-b".to_string(),
                InterestRegistration {
                    tables: vec!["tasks".to_string()],
                    updated_at_ms: now_ms.saturating_sub(200_000),
                },
            ),
        ]);
        assert_eq!(
            interested_nodes_for_tables_from_cache(&cache, now_ms, &BTreeSet::from([messages])),
            vec!["node-a".to_string()]
        );
        assert!(
            interested_nodes_for_tables_from_cache(&cache, now_ms, &BTreeSet::from([tasks]))
                .is_empty()
        );
    }

    #[test]
    fn node_subject_names_are_stable() {
        assert_eq!(
            SelectiveDeliveryRegistry::node_subject("node-a"),
            "convex.commits.node.node-a"
        );
    }

    // --- Issue #133 fail-safe behavior ---------------------------------------
    //
    // These assert that a stale, missing, or not-yet-published interest record
    // never causes a node to be *targeted* (the publisher narrows conservatively).
    // The matching guarantee that the node still receives the delta lives on the
    // broadcast path, which `SELECTIVE_DELIVERY_TRUST_INTEREST=false` keeps as
    // the consumer's source of truth.

    #[test]
    fn stale_registration_is_skipped_and_counted() {
        let now_ms = 1_000_000;
        let stale_at = now_ms - (INTEREST_MAX_AGE.as_millis() as u64) - 1;
        let messages: TableName = "messages".parse().unwrap();
        let cache = BTreeMap::from([
            (
                "fresh-node".to_string(),
                registration(&["messages"], now_ms),
            ),
            (
                "stale-node".to_string(),
                registration(&["messages"], stale_at),
            ),
        ]);

        let selection = interest_selection_from_cache(&cache, now_ms, &BTreeSet::from([messages]));

        // The stale node is NOT targeted (it would be missed if targeting were
        // the only path) and is surfaced as a staleness signal.
        assert_eq!(selection.interested, vec!["fresh-node".to_string()]);
        assert_eq!(selection.known_nodes, 2);
        assert_eq!(selection.stale_nodes, 1);
    }

    #[test]
    fn missing_interest_yields_no_targets_and_zero_known() {
        // A freshly started node that has not published interest yet is simply
        // absent. Selecting targets must not invent it; the broadcast path is
        // what actually delivers to it.
        let now_ms = 1_000_000;
        let messages: TableName = "messages".parse().unwrap();
        let cache = BTreeMap::new();

        let selection = interest_selection_from_cache(&cache, now_ms, &BTreeSet::from([messages]));

        assert!(selection.interested.is_empty());
        assert_eq!(selection.known_nodes, 0);
        assert_eq!(selection.stale_nodes, 0);
    }

    #[test]
    fn delayed_registration_appears_once_published() {
        let now_ms = 1_000_000;
        let messages: TableName = "messages".parse().unwrap();
        let mut cache = BTreeMap::new();

        // Before the node registers: not a target.
        assert!(interested_nodes_for_tables_from_cache(
            &cache,
            now_ms,
            &BTreeSet::from([messages.clone()])
        )
        .is_empty());

        // After it publishes interest: it becomes a target.
        cache.insert("late-node".to_string(), registration(&["messages"], now_ms));
        assert_eq!(
            interested_nodes_for_tables_from_cache(&cache, now_ms, &BTreeSet::from([messages])),
            vec!["late-node".to_string()]
        );
    }

    #[test]
    fn table_interest_change_updates_targets() {
        let now_ms = 1_000_000;
        let messages: TableName = "messages".parse().unwrap();
        let tasks: TableName = "tasks".parse().unwrap();
        let mut cache =
            BTreeMap::from([("node-a".to_string(), registration(&["messages"], now_ms))]);

        // Registered for `messages`, so a `tasks` delta does not target it.
        assert!(interested_nodes_for_tables_from_cache(
            &cache,
            now_ms,
            &BTreeSet::from([tasks.clone()])
        )
        .is_empty());

        // Interest changes to `tasks`: now a `tasks` delta targets it and a
        // `messages` delta no longer does.
        cache.insert("node-a".to_string(), registration(&["tasks"], now_ms));
        assert_eq!(
            interested_nodes_for_tables_from_cache(&cache, now_ms, &BTreeSet::from([tasks])),
            vec!["node-a".to_string()]
        );
        assert!(interested_nodes_for_tables_from_cache(
            &cache,
            now_ms,
            &BTreeSet::from([messages])
        )
        .is_empty());
    }
}
