//! F2 — multi-actor in-process test harness + 7 `ClusterTest` ports.
//!
//! Mirrors Java's `ClusterTest` helpers (`createCluster`, `verifyCluster`,
//! `waitAndVerifyAgreement`, `extendCluster`, `failSomeNodes`) and ports
//! the eight `ClusterTest` methods listed in PLAN.md.

#![allow(clippy::needless_pass_by_value)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use rapid::cluster::{Cluster, ClusterBuilder};
use rapid::events::ClusterEvent;
use rapid::messaging::InProcessNetwork;
use rapid::pb;
use rapid::types::ConfigurationId;

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

/// Test harness analogous to Java `ClusterTest`'s instance map +
/// helpers.
struct Harness {
    network: InProcessNetwork,
    instances: HashMap<SocketAddr, Cluster>,
    base_port: u16,
    next_port: u16,
}

impl Harness {
    fn new(base_port: u16) -> Self {
        Self {
            network: InProcessNetwork::new(),
            instances: HashMap::new(),
            base_port,
            next_port: base_port,
        }
    }

    fn allocate_port(&mut self) -> SocketAddr {
        let p = self.next_port;
        self.next_port += 1;
        addr(p)
    }

    /// Build the seed (port = `base_port`).
    async fn create_seed(&mut self) -> SocketAddr {
        let seed_addr = addr(self.base_port);
        self.next_port = self.base_port + 1;
        let seed = ClusterBuilder::new(seed_addr, self.network.clone())
            .with_settings(rapid::settings::Settings::for_tests())
            .start()
            .await
            .expect("seed bootstraps");
        self.instances.insert(seed_addr, seed);
        seed_addr
    }

    /// Bootstrap `n_joiners` through `seed_addr`. Java's `extendCluster`.
    async fn extend(&mut self, seed_addr: SocketAddr, n_joiners: usize) {
        for _ in 0..n_joiners {
            let a = self.allocate_port();
            let cluster = ClusterBuilder::new(a, self.network.clone())
                .with_settings(rapid::settings::Settings::for_tests())
                .join(seed_addr)
                .await
                .expect("joiner converges");
            self.instances.insert(a, cluster);
        }
    }

    /// Bootstrap `n_joiners` through `seed_addr` concurrently.
    async fn extend_parallel(&mut self, seed_addr: SocketAddr, n_joiners: usize) {
        let mut handles = Vec::with_capacity(n_joiners);
        let mut addrs = Vec::with_capacity(n_joiners);
        for _ in 0..n_joiners {
            let a = self.allocate_port();
            addrs.push(a);
            let net = self.network.clone();
            handles.push(tokio::spawn(async move {
                ClusterBuilder::new(a, net)
                    .with_settings(rapid::settings::Settings::for_tests())
                    .join(seed_addr)
                    .await
            }));
        }
        for (a, h) in addrs.into_iter().zip(handles) {
            let cluster = h.await.expect("join task").expect("joiner converges");
            self.instances.insert(a, cluster);
        }
    }

    /// Wait up to `attempts * interval` for every instance to agree on
    /// the same `expected_size`-member view with identical
    /// `ConfigurationId`. Java's `waitAndVerifyAgreement`.
    async fn wait_and_verify_agreement(
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

    /// Shut down each cluster cooperatively.
    async fn shutdown_all(self) {
        for (_, cluster) in self.instances {
            cluster.shutdown().await;
        }
    }

    /// Remove a single instance via `shutdown` (ungraceful — no leave message).
    async fn fail_node(&mut self, target: SocketAddr) -> Option<()> {
        let cluster = self.instances.remove(&target)?;
        cluster.shutdown().await;
        Some(())
    }

    /// Remove a single instance via `leave_gracefully`.
    async fn leave_node(&mut self, target: SocketAddr) -> Option<()> {
        let cluster = self.instances.remove(&target)?;
        cluster.leave_gracefully().await;
        Some(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_joins_through_seed_via_harness() {
    let mut h = Harness::new(30_000);
    let seed = h.create_seed().await;
    assert!(
        h.wait_and_verify_agreement(1, 5, Duration::from_millis(100))
            .await
    );
    h.extend(seed, 1).await;
    assert!(
        h.wait_and_verify_agreement(2, 20, Duration::from_millis(200))
            .await,
        "2-node convergence failed"
    );
    h.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ten_nodes_join_sequentially() {
    let mut h = Harness::new(30_100);
    let seed = h.create_seed().await;
    for i in 0..10 {
        h.extend(seed, 1).await;
        let expected = i + 2;
        assert!(
            h.wait_and_verify_agreement(expected, 100, Duration::from_millis(200))
                .await,
            "convergence to {expected} failed at iteration {i}",
        );
    }
    h.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ten_nodes_join_in_parallel() {
    let mut h = Harness::new(30_200);
    let seed = h.create_seed().await;
    h.extend_parallel(seed, 10).await;
    assert!(
        h.wait_and_verify_agreement(11, 60, Duration::from_millis(300))
            .await,
        "11-node convergence (10 parallel joiners) failed",
    );
    h.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_failure_before_bootstrap() {
    // A seed exists; an extra Cluster bootstraps and then immediately
    // fails. The remaining N-1 must still observe a coherent view.
    let mut h = Harness::new(30_300);
    let seed = h.create_seed().await;
    h.extend(seed, 2).await;
    assert!(
        h.wait_and_verify_agreement(3, 30, Duration::from_millis(200))
            .await
    );
    // Pick the third node (highest port we allocated).
    let victim = addr(30_302);
    h.fail_node(victim).await.expect("victim present");
    // After failure the remaining two must converge to a 2-node view.
    assert!(
        h.wait_and_verify_agreement(2, 200, Duration::from_millis(200))
            .await,
        "remaining nodes did not converge after a single failure",
    );
    h.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_node_fails_after_bootstrap() {
    let mut h = Harness::new(30_400);
    let seed = h.create_seed().await;
    h.extend(seed, 4).await;
    assert!(
        h.wait_and_verify_agreement(5, 30, Duration::from_millis(200))
            .await,
        "5-node convergence failed",
    );
    h.fail_node(addr(30_402)).await.expect("victim present");
    assert!(
        h.wait_and_verify_agreement(4, 60, Duration::from_millis(500))
            .await,
        "4-node convergence after one failure failed",
    );
    h.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn multiple_nodes_fail_after_bootstrap() {
    let mut h = Harness::new(30_500);
    let seed = h.create_seed().await;
    h.extend(seed, 7).await;
    assert!(
        h.wait_and_verify_agreement(8, 60, Duration::from_millis(300))
            .await,
        "8-node convergence failed",
    );
    // Fail two non-seed nodes.
    h.fail_node(addr(30_502)).await.expect("v1 present");
    h.fail_node(addr(30_503)).await.expect("v2 present");
    assert!(
        h.wait_and_verify_agreement(6, 90, Duration::from_millis(500))
            .await,
        "6-node convergence after two failures failed",
    );
    h.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_leave() {
    let mut h = Harness::new(30_600);
    let seed = h.create_seed().await;
    h.extend(seed, 2).await;
    assert!(
        h.wait_and_verify_agreement(3, 30, Duration::from_millis(200))
            .await
    );
    h.leave_node(addr(30_602)).await.expect("leaver present");
    assert!(
        h.wait_and_verify_agreement(2, 60, Duration::from_millis(500))
            .await,
        "2-node convergence after leave failed",
    );
    h.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_join_and_failure() {
    let mut h = Harness::new(30_700);
    let seed = h.create_seed().await;
    h.extend(seed, 4).await;
    assert!(
        h.wait_and_verify_agreement(5, 30, Duration::from_millis(200))
            .await
    );
    // Concurrently: kick off a join + fail an existing node.
    let net = h.network.clone();
    let new_addr = addr(30_710);
    let join_task = tokio::spawn(async move {
        ClusterBuilder::new(new_addr, net)
            .with_settings(rapid::settings::Settings::for_tests())
            .join(seed)
            .await
    });
    let victim = addr(30_702);
    h.fail_node(victim).await.expect("victim present");
    let cluster = join_task
        .await
        .expect("join task")
        .expect("joiner converges");
    h.instances.insert(new_addr, cluster);
    // Net delta: −1 + 1 = 5 members.
    assert!(
        h.wait_and_verify_agreement(5, 90, Duration::from_millis(500))
            .await,
        "5-node convergence after concurrent failure + join failed",
    );
    h.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kicked_event_carries_node_metadata() {
    // The KICKED event is fired on a node that is removed from the
    // configuration. We construct a 2-node cluster, kick the seed via
    // direct apply_proposal, and observe the KICKED subscriber.
    let mut h = Harness::new(30_800);
    let seed = h.create_seed().await;
    h.extend(seed, 1).await;
    assert!(
        h.wait_and_verify_agreement(2, 30, Duration::from_millis(200))
            .await
    );
    let seed_cluster = h.instances.get(&seed).expect("seed present");
    let mut kicked_rx = seed_cluster.subscribe(ClusterEvent::Kicked);
    let seed_endpoint = seed_cluster.listen_endpoint().clone();
    seed_cluster
        .service()
        .apply_proposal(vec![seed_endpoint])
        .await
        .expect("apply_proposal succeeds");
    let ev = tokio::time::timeout(Duration::from_millis(500), kicked_rx.recv())
        .await
        .expect("KICKED event arrives")
        .expect("channel open");
    assert!(ev.delta.iter().any(|d| d.status == pb::EdgeStatus::Down));
    h.shutdown_all().await;
}
