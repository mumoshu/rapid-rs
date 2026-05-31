//! Pattern-B sharded service discovery on top of a Rapid cluster.
//!
//! Every cluster node tags itself at join time with a CSV of shard
//! ids in `pb::Metadata` (default key: `"shards"`). This crate
//! provides [`ShardDirectory`], a cheap-to-clone handle that:
//!
//! 1. Snapshots the current cluster view via
//!    [`rapid::cluster::Cluster::initial_view_event`].
//! 2. Subscribes to `ClusterEvent::ViewChange` and folds each delta
//!    into a per-shard `HashMap<String, Vec<Endpoint>>` index.
//! 3. Exposes `replicas_of(shard)` / `all_shards()` /
//!    `configuration_id()` as synchronous reads (the index is
//!    `parking_lot::RwLock`-guarded).
//!
//! Strong-consistency property — because Rapid's `VIEW_CHANGE` event
//! carries the `configuration_id` it took effect at, every
//! [`ShardDirectory`] across the cluster converges to the same
//! `by_shard` map at the same `configuration_id`. This is exactly
//! the "service-discovery-via-membership-metadata" guarantee you'd
//! get from a Raft-replicated placement table, except the placement
//! moves through Rapid's view-change pipeline rather than a
//! separate consensus.
//!
//! Caveats — see `docs/k-h-l.md` and the Pattern-B / Pattern-C
//! discussion in the README. The main one: `pb::Metadata` is **set
//! at join time and not mutated afterwards** in the upstream Rapid
//! protocol. To move a shard between nodes you either
//! leave-and-rejoin or use Pattern C (separate placement service).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use rapid::cluster::Cluster;
use rapid::events::{ClusterEvent, NodeStatusChange};
use rapid::pb;
use rapid::service::MembershipService;
use rapid::types::ConfigurationId;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

/// Default `pb::Metadata` key for the shard CSV. Override via
/// [`ShardDirectoryBuilder::with_key`] when integrating with an existing
/// metadata schema.
pub const DEFAULT_METADATA_KEY: &str = "shards";

/// Default delimiter inside the metadata value. Override via
/// [`ShardDirectoryBuilder::with_delimiter`].
pub const DEFAULT_DELIMITER: char = ',';

/// Strongly consistent per-shard replica directory built from Rapid
/// view-change events.
///
/// Clone-cheap (single `Arc`). Surviving past the underlying cluster
/// is supported: the background forwarder exits when the cluster's
/// broadcast sender is dropped.
#[derive(Clone)]
pub struct ShardDirectory {
    inner: Arc<Inner>,
}

struct Inner {
    index: RwLock<ShardIndex>,
    forwarder: Mutex<Option<JoinHandle<()>>>,
    metadata_key: String,
    delimiter: char,
}

struct ShardIndex {
    configuration_id: ConfigurationId,
    by_shard: HashMap<String, Vec<pb::Endpoint>>,
    /// Reverse index — what shards a given endpoint was last seen
    /// hosting. Needed so that a `DOWN` delta knows which buckets
    /// to scrub the endpoint from (the `DOWN` event itself doesn't
    /// re-carry the metadata).
    by_endpoint: HashMap<EndpointKey, Vec<String>>,
}

impl Default for ShardIndex {
    fn default() -> Self {
        Self {
            // SENTINEL is `-1`; any real configuration id will replace
            // it on the first `apply_delta` call.
            configuration_id: ConfigurationId::SENTINEL,
            by_shard: HashMap::new(),
            by_endpoint: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EndpointKey {
    hostname: Vec<u8>,
    port: i32,
}

impl From<&pb::Endpoint> for EndpointKey {
    fn from(ep: &pb::Endpoint) -> Self {
        Self {
            hostname: ep.hostname.clone(),
            port: ep.port,
        }
    }
}

/// Builder-ish entry points.
impl ShardDirectory {
    /// Build a directory over `cluster` using the default metadata
    /// key (`"shards"`) and delimiter (`,`).
    ///
    /// # Errors
    ///
    /// Bubbles up [`rapid::error::Error::Shutdown`] if the cluster's
    /// actor has already exited.
    pub async fn new(cluster: &Cluster) -> rapid::error::Result<Self> {
        Self::builder().build(cluster).await
    }

    /// Construct a builder.
    #[must_use]
    pub fn builder() -> ShardDirectoryBuilder {
        ShardDirectoryBuilder::default()
    }

    /// Endpoints currently hosting `shard`. Empty when the shard is
    /// not advertised by any node, or when the directory was just
    /// constructed and the cluster has only the seed node.
    #[must_use]
    pub fn replicas_of(&self, shard: &str) -> Vec<pb::Endpoint> {
        self.inner
            .index
            .read()
            .by_shard
            .get(shard)
            .cloned()
            .unwrap_or_default()
    }

    /// Every shard id currently advertised somewhere in the cluster.
    #[must_use]
    pub fn all_shards(&self) -> Vec<String> {
        let g = self.inner.index.read();
        let mut out: Vec<String> = g.by_shard.keys().cloned().collect();
        out.sort();
        out
    }

    /// Configuration id of the most recent view-change folded into
    /// the index. Two `ShardDirectory` handles across the cluster
    /// reporting the same configuration id are guaranteed to have
    /// identical `by_shard` contents.
    #[must_use]
    pub fn configuration_id(&self) -> ConfigurationId {
        self.inner.index.read().configuration_id
    }

    /// Total replica count across all shards (counts duplicates if a
    /// node hosts multiple shards — i.e., one replica per
    /// `(node, shard)` pair).
    #[must_use]
    pub fn total_replica_slots(&self) -> usize {
        self.inner
            .index
            .read()
            .by_shard
            .values()
            .map(Vec::len)
            .sum()
    }

    /// Cooperative shutdown of the background forwarder task.
    /// Idempotent. Calling this is optional — the forwarder also
    /// exits when the underlying cluster's broadcast sender drops.
    pub async fn shutdown(self) {
        let handle = self.inner.forwarder.lock().take();
        if let Some(h) = handle {
            h.abort();
            let _ = h.await;
        }
    }
}

/// Configurable builder for [`ShardDirectory`].
#[derive(Default)]
pub struct ShardDirectoryBuilder {
    metadata_key: Option<String>,
    delimiter: Option<char>,
}

impl ShardDirectoryBuilder {
    /// Override the metadata key (default `"shards"`).
    #[must_use]
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.metadata_key = Some(key.into());
        self
    }

    /// Override the CSV delimiter (default `,`).
    #[must_use]
    pub fn with_delimiter(mut self, delim: char) -> Self {
        self.delimiter = Some(delim);
        self
    }

    /// Build the directory, snapshotting the current cluster state
    /// and starting the background view-change forwarder.
    ///
    /// # Errors
    ///
    /// Bubbles up [`rapid::error::Error::Shutdown`] from the cluster.
    pub async fn build(self, cluster: &Cluster) -> rapid::error::Result<ShardDirectory> {
        let metadata_key = self
            .metadata_key
            .unwrap_or_else(|| DEFAULT_METADATA_KEY.into());
        let delimiter = self.delimiter.unwrap_or(DEFAULT_DELIMITER);

        let initial = cluster.initial_view_event().await?;
        let mut index = ShardIndex {
            configuration_id: initial.configuration_id,
            ..Default::default()
        };
        apply_delta(&mut index, &metadata_key, delimiter, &initial.delta);

        let inner = Arc::new(Inner {
            index: RwLock::new(index),
            forwarder: Mutex::new(None),
            metadata_key: metadata_key.clone(),
            delimiter,
        });

        let inner_for_task = Arc::clone(&inner);
        let service = cluster.service().clone();
        let mut rx = cluster.subscribe(ClusterEvent::ViewChange);
        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(ev) => {
                        let mut g = inner_for_task.index.write();
                        g.configuration_id = ev.configuration_id;
                        apply_delta(
                            &mut g,
                            &inner_for_task.metadata_key,
                            inner_for_task.delimiter,
                            &ev.delta,
                        );
                    }
                    Err(RecvError::Closed) => {
                        tracing::debug!(target: "rapid_shard", "view-change channel closed");
                        return;
                    }
                    Err(RecvError::Lagged(n)) => {
                        tracing::warn!(
                            target: "rapid_shard",
                            skipped = n,
                            "view-change broadcast lagged; resyncing"
                        );
                        if let Ok((cid, delta)) = full_resync(&service).await {
                            let mut g = inner_for_task.index.write();
                            *g = ShardIndex::default();
                            g.configuration_id = cid;
                            apply_delta(
                                &mut g,
                                &inner_for_task.metadata_key,
                                inner_for_task.delimiter,
                                &delta,
                            );
                        }
                    }
                }
            }
        });
        *inner.forwarder.lock() = Some(handle);

        Ok(ShardDirectory { inner })
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Belt-and-suspenders: even if the caller forgets `.shutdown()`,
        // dropping the last `Arc<Inner>` aborts the forwarder. The
        // broadcast::Receiver inside the task gets its sender side
        // dropped on cluster shutdown anyway, but aborting here keeps
        // the lifecycle deterministic in test settings.
        if let Some(h) = self.forwarder.lock().take() {
            h.abort();
        }
    }
}

fn apply_delta(index: &mut ShardIndex, key: &str, delimiter: char, delta: &[NodeStatusChange]) {
    for nsc in delta {
        let ep_key = EndpointKey::from(&nsc.endpoint);
        match nsc.status {
            pb::EdgeStatus::Up => {
                // First scrub any stale reverse-index entry for this
                // endpoint (e.g., a rejoin under a new shard set).
                drop_endpoint_from_buckets(index, &ep_key, &nsc.endpoint);
                let shards = parse_shards(&nsc.metadata, key, delimiter);
                for shard in &shards {
                    let bucket = index.by_shard.entry(shard.clone()).or_default();
                    if !bucket.iter().any(|e| EndpointKey::from(e) == ep_key) {
                        bucket.push(nsc.endpoint.clone());
                    }
                }
                index.by_endpoint.insert(ep_key, shards);
            }
            pb::EdgeStatus::Down => {
                drop_endpoint_from_buckets(index, &ep_key, &nsc.endpoint);
                index.by_endpoint.remove(&ep_key);
            }
        }
    }
}

fn drop_endpoint_from_buckets(index: &mut ShardIndex, ep_key: &EndpointKey, ep: &pb::Endpoint) {
    let Some(shards) = index.by_endpoint.get(ep_key).cloned() else {
        return;
    };
    for shard in &shards {
        if let Some(eps) = index.by_shard.get_mut(shard) {
            eps.retain(|e| !(e.hostname == ep.hostname && e.port == ep.port));
            if eps.is_empty() {
                index.by_shard.remove(shard);
            }
        }
    }
}

fn parse_shards(meta: &pb::Metadata, key: &str, delimiter: char) -> Vec<String> {
    let Some(raw) = meta.metadata.get(key) else {
        return Vec::new();
    };
    let Ok(s) = std::str::from_utf8(raw) else {
        return Vec::new();
    };
    // Filter empty + dedupe while preserving first-seen order.
    let mut seen: HashSet<&str> = HashSet::new();
    s.split(delimiter)
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .filter(|t| seen.insert(*t))
        .map(String::from)
        .collect()
}

/// Equivalent to [`Cluster::initial_view_event`] but driven from the
/// `MembershipService` handle alone — the cluster reference isn't in
/// scope inside the forwarder task.
async fn full_resync(
    service: &MembershipService,
) -> rapid::error::Result<(ConfigurationId, Vec<NodeStatusChange>)> {
    let configuration_id = service.configuration_id().await?;
    let memberlist = service.memberlist().await?;
    let metadata = service.metadata().await.unwrap_or_default();
    let by_ep: HashMap<EndpointKey, pb::Metadata> = metadata
        .into_iter()
        .map(|(ep, m)| (EndpointKey::from(&ep), m))
        .collect();
    let delta = memberlist
        .into_iter()
        .map(|ep| NodeStatusChange {
            metadata: by_ep
                .get(&EndpointKey::from(&ep))
                .cloned()
                .unwrap_or_default(),
            status: pb::EdgeStatus::Up,
            endpoint: ep,
        })
        .collect();
    Ok((configuration_id, delta))
}
