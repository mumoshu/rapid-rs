//! Multi-actor in-process test harness used by F2 + F11.
//!
//! Mirrors Java `ClusterTest`'s instance map + helpers
//! (`createCluster`, `verifyCluster`, `waitAndVerifyAgreement`,
//! `extendCluster`, `failSomeNodes`, `dropFirstNAtServer`,
//! `useStaticFd`).

#![allow(clippy::needless_pass_by_value)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rapid::cluster::{Cluster, ClusterBuilder};
use rapid::messaging::fault_injection::EnvelopeFilter;
use rapid::messaging::InProcessNetwork;
use rapid::monitoring::{Blacklist, EdgeFailureDetectorFactory, StaticFailureDetectorFactory};
use rapid::pb;
use rapid::types::ConfigurationId;

/// Synthesise a localhost endpoint.
#[must_use]
pub fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

/// Test harness analogous to Java `ClusterTest`'s instance map + helpers.
pub struct Harness {
    pub network: InProcessNetwork,
    pub instances: HashMap<SocketAddr, Cluster>,
    pub base_port: u16,
    pub next_port: u16,
    /// Optional shared blacklist for `useStaticFd` parity. When set,
    /// every new cluster uses a `StaticFailureDetectorFactory` over
    /// this blacklist; tests call `blacklist().add_failed_nodes(...)`
    /// to mark hosts as failed.
    pub static_fd: Option<Blacklist>,
}

impl Harness {
    /// New harness with port range starting at `base_port`.
    #[must_use]
    pub fn new(base_port: u16) -> Self {
        Self {
            network: InProcessNetwork::new(),
            instances: HashMap::new(),
            base_port,
            next_port: base_port,
            static_fd: None,
        }
    }

    /// Switch to a shared `StaticFailureDetector` for every future
    /// cluster (Java `useStaticFd = true`).
    pub fn use_static_fd(&mut self) {
        if self.static_fd.is_none() {
            self.static_fd = Some(Blacklist::new());
        }
    }

    /// Borrow the shared blacklist. Panics if `use_static_fd()` was
    /// not called first.
    #[must_use]
    pub fn blacklist(&self) -> &Blacklist {
        self.static_fd
            .as_ref()
            .expect("use_static_fd() must precede blacklist()")
    }

    /// Install an [`EnvelopeFilter`] on the shared network.
    pub fn set_interceptor(&self, filter: Arc<dyn EnvelopeFilter>) {
        self.network.set_interceptor(Some(filter));
    }

    /// Allocate a fresh `127.0.0.1:port` and bump the counter.
    pub fn allocate_port(&mut self) -> SocketAddr {
        let p = self.next_port;
        self.next_port += 1;
        addr(p)
    }

    fn build_at(&self, a: SocketAddr) -> ClusterBuilder {
        let mut b = ClusterBuilder::new(a, self.network.clone())
            .with_settings(rapid::settings::Settings::for_tests());
        if let Some(bl) = self.static_fd.as_ref() {
            let factory: Arc<dyn EdgeFailureDetectorFactory> =
                Arc::new(StaticFailureDetectorFactory::new(
                    bl.clone(),
                    Arc::new(rapid::clock::TokioClock),
                    Duration::from_millis(100),
                ));
            b = b.with_failure_detector_factory(factory);
        }
        b
    }

    /// Build the seed (port = `base_port`). Returns its address.
    pub async fn create_seed(&mut self) -> SocketAddr {
        let seed_addr = addr(self.base_port);
        self.next_port = self.base_port + 1;
        let seed = self
            .build_at(seed_addr)
            .start()
            .await
            .expect("seed bootstraps");
        self.instances.insert(seed_addr, seed);
        seed_addr
    }

    /// Bootstrap `n_joiners` sequentially through `seed_addr`.
    /// Java `extendCluster(n, seed)`.
    pub async fn extend(&mut self, seed_addr: SocketAddr, n_joiners: usize) {
        for _ in 0..n_joiners {
            let a = self.allocate_port();
            let cluster = self
                .build_at(a)
                .join(seed_addr)
                .await
                .expect("joiner converges");
            self.instances.insert(a, cluster);
        }
    }

    /// Bootstrap a specific endpoint (Java `extendCluster(endpoint,
    /// seed)`). Used by rejoin tests.
    pub async fn extend_at(&mut self, joiner_addr: SocketAddr, seed_addr: SocketAddr) {
        let cluster = self
            .build_at(joiner_addr)
            .join(seed_addr)
            .await
            .expect("joiner converges");
        self.instances.insert(joiner_addr, cluster);
        self.next_port = self.next_port.max(joiner_addr.port() + 1);
    }

    /// Bootstrap `n_joiners` concurrently through `seed_addr`. Java's
    /// `createCluster(numNodes, seed)` past the seed.
    pub async fn extend_parallel(&mut self, seed_addr: SocketAddr, n_joiners: usize) {
        let mut handles = Vec::with_capacity(n_joiners);
        let mut addrs = Vec::with_capacity(n_joiners);
        for _ in 0..n_joiners {
            let a = self.allocate_port();
            addrs.push(a);
            let builder = self.build_at(a);
            handles.push(tokio::spawn(async move { builder.join(seed_addr).await }));
        }
        for (a, h) in addrs.into_iter().zip(handles) {
            let cluster = h.await.expect("join task").expect("joiner converges");
            self.instances.insert(a, cluster);
        }
    }

    /// `createCluster(N, seed)` — seed + N-1 parallel joiners.
    pub async fn create_cluster(&mut self, n: usize, seed_addr: SocketAddr) {
        assert!(n >= 1);
        let actual_seed = self.create_seed().await;
        assert_eq!(actual_seed, seed_addr, "seed port mismatch");
        if n > 1 {
            self.extend_parallel(seed_addr, n - 1).await;
        }
    }

    /// Wait up to `attempts * interval` for every instance to agree
    /// on `expected_size` members + identical `ConfigurationId`.
    /// Java's `waitAndVerifyAgreement`.
    pub async fn wait_and_verify_agreement(
        &self,
        expected_size: usize,
        attempts: u32,
        interval: Duration,
    ) -> bool {
        for _ in 0..attempts {
            if self.agreed(expected_size).await {
                return true;
            }
            tokio::time::sleep(interval).await;
        }
        self.agreed(expected_size).await
    }

    async fn agreed(&self, expected_size: usize) -> bool {
        let mut config: Option<ConfigurationId> = None;
        for cluster in self.instances.values() {
            let Ok(members) = cluster.memberlist().await else {
                return false;
            };
            if members.len() != expected_size {
                return false;
            }
            let Ok(id) = cluster.configuration_id().await else {
                return false;
            };
            match config {
                None => config = Some(id),
                Some(prev) if prev != id => return false,
                _ => {}
            }
        }
        true
    }

    /// Sample membership-size from every instance (Java
    /// `verifyCluster`). Returns false on disagreement.
    pub async fn verify_cluster(&self, expected_size: usize) -> bool {
        for cluster in self.instances.values() {
            match cluster.memberlist().await {
                Ok(m) if m.len() == expected_size => {}
                _ => return false,
            }
        }
        true
    }

    /// Java `verifyNumClusterInstances`. Returns the number of
    /// `Cluster` objects we still own (not the membership size).
    #[must_use]
    pub fn num_cluster_instances(&self) -> usize {
        self.instances.len()
    }

    /// Shutdown every cluster cooperatively.
    pub async fn shutdown_all(self) {
        for (_, cluster) in self.instances {
            cluster.shutdown().await;
        }
    }

    /// Remove a single instance via `shutdown` (ungraceful — no leave message).
    pub async fn fail_node(&mut self, target: SocketAddr) -> Option<()> {
        let cluster = self.instances.remove(&target)?;
        cluster.shutdown().await;
        Some(())
    }

    /// Remove a single instance via `leave_gracefully`.
    pub async fn leave_node(&mut self, target: SocketAddr) -> Option<()> {
        let cluster = self.instances.remove(&target)?;
        cluster.leave_gracefully().await;
        Some(())
    }

    /// Mark `targets` as failed via the static FD blacklist (Java
    /// parity: `staticFds.addFailedNodes(targets)`). Does NOT
    /// shut down the underlying clusters — they keep running and
    /// observe themselves as kicked.
    pub fn mark_failed_via_static_fd(&self, targets: impl IntoIterator<Item = SocketAddr>) {
        let bl = self.blacklist();
        let endpoints: Vec<pb::Endpoint> = targets
            .into_iter()
            .map(|s| pb::Endpoint {
                hostname: s.ip().to_string().into_bytes(),
                port: i32::from(s.port()),
            })
            .collect();
        bl.add_failed_nodes(endpoints);
    }
}
