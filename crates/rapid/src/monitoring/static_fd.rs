//! Test-only failure detector with a shared, mutable blacklist.
//!
//! Java parity: `StaticFailureDetector` in
//! `references/rapid-java/.../test/.../StaticFailureDetector.java`.
//!
//! `ClusterTest` uses this to mark specific nodes as "failed" without
//! shutting them down — the surviving nodes' FDs see them as down
//! and trigger the consensus path while the targets keep running
//! (so subsequent assertions like `verifyNumClusterInstances` still
//! observe the kicked instances).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::clock::Clock;
use crate::monitoring::factory::{
    EdgeFailureDetector, EdgeFailureDetectorFactory, EdgeFailureNotifier,
};
use crate::pb;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct EndpointKey {
    hostname: Vec<u8>,
    port: i32,
}

impl From<&pb::Endpoint> for EndpointKey {
    fn from(e: &pb::Endpoint) -> Self {
        Self {
            hostname: e.hostname.clone(),
            port: e.port,
        }
    }
}

/// Shared, mutable blacklist. Cloning is cheap (Arc-backed).
#[derive(Clone, Default)]
pub struct Blacklist {
    inner: Arc<RwLock<HashSet<EndpointKey>>>,
}

impl Blacklist {
    /// Construct an empty blacklist.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark every endpoint in `nodes` as failed. Already-failed
    /// endpoints stay failed (idempotent).
    pub fn add_failed_nodes<I>(&self, nodes: I)
    where
        I: IntoIterator<Item = pb::Endpoint>,
    {
        let mut g = self.inner.write();
        for ep in nodes {
            g.insert(EndpointKey::from(&ep));
        }
    }

    fn contains(&self, ep: &pb::Endpoint) -> bool {
        self.inner.read().contains(&EndpointKey::from(ep))
    }
}

/// Factory analogue of Java `StaticFailureDetector.Factory`. Hand to
/// `ClusterBuilder::with_failure_detector_factory()`.
pub struct StaticFailureDetectorFactory {
    blacklist: Blacklist,
    clock: Arc<dyn Clock>,
    interval: Duration,
}

impl StaticFailureDetectorFactory {
    /// Construct a factory. `interval` controls how often each
    /// per-subject detector polls the shared blacklist.
    #[must_use]
    pub fn new(blacklist: Blacklist, clock: Arc<dyn Clock>, interval: Duration) -> Self {
        Self {
            blacklist,
            clock,
            interval,
        }
    }
}

impl EdgeFailureDetectorFactory for StaticFailureDetectorFactory {
    fn create(
        &self,
        subject: pb::Endpoint,
        notifier: EdgeFailureNotifier,
    ) -> Arc<dyn EdgeFailureDetector> {
        Arc::new(StaticFailureDetector {
            subject,
            notifier,
            blacklist: self.blacklist.clone(),
            clock: self.clock.clone(),
            interval: self.interval,
        })
    }
}

struct StaticFailureDetector {
    subject: pb::Endpoint,
    notifier: EdgeFailureNotifier,
    blacklist: Blacklist,
    clock: Arc<dyn Clock>,
    interval: Duration,
}

#[async_trait]
impl EdgeFailureDetector for StaticFailureDetector {
    async fn run(self: Arc<Self>) {
        loop {
            self.clock.sleep(self.interval).await;
            if self.blacklist.contains(&self.subject) {
                self.notifier.notify().await;
                return;
            }
        }
    }
}
