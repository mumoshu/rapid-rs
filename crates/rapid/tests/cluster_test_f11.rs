//! F11 (part 1) — `ClusterTest.java` bootstrap/scale/failure method ports.
//!
//! The phase-2 drop, rejoin, and concurrent-contact scenarios live in
//! `cluster_test_f11_rejoin.rs` (split to keep each test binary under the
//! 400-line file cap, RULES.md §File length).
//!
//! Java → Rust mapping (`snake_case`):
//! - `hostAndPortBuilderTests`               → `host_and_port_builder_tests`
//! - `twentyNodesJoinSequentially`           → `twenty_nodes_join_sequentially`
//! - `hundredNodesJoinInParallel`            → `hundred_nodes_join_in_parallel`
//! - `fiftyNodesJoinTwentyNodeCluster`       → `fifty_nodes_join_twenty_node_cluster`
//! - `failRandomQuarterOfNodes`              → `fail_random_quarter_of_nodes`
//! - `failRandomThirdOfNodes`                → `fail_random_third_of_nodes`
//! - `failTenRandomNodes`                    → `fail_ten_random_nodes`
//! - `injectAsymmetricDrops`                 → `inject_asymmetric_drops`

mod common;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rand::seq::IteratorRandom;
use rapid::cluster::ClusterBuilder;
use rapid::messaging::fault_injection::{DropAtDests, MessageKind};
use rapid::messaging::InProcessNetwork;

use common::cluster_harness::{addr, Harness};

// ====================================================================
// 1. host_and_port_builder_tests
// ====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_and_port_builder_tests() {
    let net = InProcessNetwork::new();
    let seed_addr = addr(31_000);
    let joiner_addr = addr(31_001);
    let seed = ClusterBuilder::new(seed_addr, net.clone())
        .with_settings(rapid::settings::Settings::for_tests())
        .start()
        .await
        .expect("seed bootstraps");
    let joiner = ClusterBuilder::new(joiner_addr, net.clone())
        .with_settings(rapid::settings::Settings::for_tests())
        .join(seed_addr)
        .await
        .expect("joiner converges");
    assert_eq!(seed.membership_size().await.unwrap(), 2);
    assert_eq!(joiner.membership_size().await.unwrap(), 2);
    joiner.shutdown().await;
    seed.shutdown().await;
}

// ====================================================================
// 2. twenty_nodes_join_sequentially
// ====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn twenty_nodes_join_sequentially() {
    let mut h = Harness::new(31_100);
    let seed = h.create_seed().await;
    for i in 0..20 {
        h.extend(seed, 1).await;
        let expected = i + 2;
        assert!(
            h.wait_and_verify_agreement(expected, 30, Duration::from_millis(200))
                .await,
            "convergence to {expected} failed at iter {i}",
        );
    }
    h.shutdown_all().await;
}

// ====================================================================
// 3. hundred_nodes_join_in_parallel
// ====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn hundred_nodes_join_in_parallel() {
    // Java test timeout is 150 s for this scenario.
    let mut h = Harness::new(31_200);
    h.create_cluster(100, addr(31_200)).await;
    assert!(
        h.wait_and_verify_agreement(100, 100, Duration::from_millis(500))
            .await,
        "100-node convergence failed",
    );
    h.shutdown_all().await;
}

// ====================================================================
// 4. fifty_nodes_join_twenty_node_cluster
// ====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn fifty_nodes_join_twenty_node_cluster() {
    let mut h = Harness::new(31_400);
    h.create_cluster(20, addr(31_400)).await;
    assert!(
        h.wait_and_verify_agreement(20, 40, Duration::from_millis(200))
            .await,
        "20-node phase failed",
    );
    h.extend_parallel(addr(31_400), 50).await;
    assert!(
        h.wait_and_verify_agreement(70, 80, Duration::from_millis(500))
            .await,
        "70-node phase failed",
    );
    h.shutdown_all().await;
}

// ====================================================================
// 5/6/7. Random-failure trio (static FD)
// ====================================================================

async fn random_failure_scenario(
    base_port: u16,
    n_nodes: usize,
    n_failed: usize,
    shutdown_failed: bool,
) {
    let mut h = Harness::new(base_port);
    h.use_static_fd();
    h.create_cluster(n_nodes, addr(base_port)).await;
    assert!(
        h.wait_and_verify_agreement(n_nodes, 60, Duration::from_millis(300))
            .await,
        "N={n_nodes}: bootstrap convergence failed",
    );
    // Java: getRandomHosts(numFailingNodes), skipping the seed itself
    // when caller's range starts at basePort+1 (asymmetric drops do
    // that). Here we replicate the simpler "any non-seed host" rule.
    let seed_addr = addr(base_port);
    let candidates: Vec<SocketAddr> = h
        .instances
        .keys()
        .copied()
        .filter(|a| *a != seed_addr)
        .collect();
    let mut rng = rand::thread_rng();
    let failed: HashSet<SocketAddr> = candidates
        .into_iter()
        .choose_multiple(&mut rng, n_failed)
        .into_iter()
        .collect();
    h.mark_failed_via_static_fd(failed.iter().copied());

    if shutdown_failed {
        for a in &failed {
            h.fail_node(*a).await;
        }
    }

    let expected = n_nodes - failed.len();
    assert!(
        h.wait_and_verify_agreement(expected, 120, Duration::from_millis(500))
            .await,
        "post-failure convergence to {expected} failed",
    );

    // verifyNumClusterInstances: when failed nodes were shut down we
    // expect n - failed; otherwise we still own every cluster object
    // (Java's "Cluster instances stay alive but report themselves
    // kicked" assertion).
    let expected_instances = if shutdown_failed { expected } else { n_nodes };
    assert_eq!(
        h.num_cluster_instances(),
        expected_instances,
        "instance count after failure"
    );
    h.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn fail_random_quarter_of_nodes() {
    // Java: 50 nodes, 12 failed, instances actually shut down.
    random_failure_scenario(31_500, 50, 12, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn fail_random_third_of_nodes() {
    // Java: 50 nodes, 16 failed, instances actually shut down.
    random_failure_scenario(31_600, 50, 16, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn fail_ten_random_nodes() {
    // Java: 50 nodes, 10 failed via static FD only, instances kept alive
    // — but they still consider themselves kicked from the network.
    random_failure_scenario(31_700, 50, 10, false).await;
}

// ====================================================================
// 8. inject_asymmetric_drops
// ====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn inject_asymmetric_drops() {
    // Java: 50 nodes; 10 randomly chosen non-seed hosts have their
    // first 100 *probe* messages dropped at the server. Cluster must
    // still converge — other observers compensate.
    let mut h = Harness::new(31_800);
    // Pre-pick the addresses of the would-be failed hosts before any
    // cluster is up — the interceptor needs to match on `dst`. Java
    // does the same: picks addrs in `[basePort+1, basePort+numNodes)`.
    let mut failed_dests: HashSet<SocketAddr> = HashSet::new();
    let mut rng = rand::thread_rng();
    let candidates: Vec<u16> = (31_801..=31_849).collect();
    for &p in candidates.iter().choose_multiple(&mut rng, 10) {
        failed_dests.insert(addr(p));
    }
    h.set_interceptor(Arc::new(DropAtDests::new(
        100,
        Some(MessageKind::Probe),
        failed_dests.clone(),
    )));
    h.create_cluster(50, addr(31_800)).await;
    // The cluster must converge to (numNodes - failed) because the
    // PingPong FD on healthy peers will eventually mark the dropping
    // hosts as DOWN and a view-change kicks them.
    let expected = 50 - failed_dests.len();
    assert!(
        h.wait_and_verify_agreement(expected, 120, Duration::from_millis(500))
            .await,
        "post-asymmetric-drop convergence to {expected} failed",
    );
    // Cluster instances are NOT shut down — they keep running.
    assert_eq!(h.num_cluster_instances(), 50);
    h.shutdown_all().await;
}
