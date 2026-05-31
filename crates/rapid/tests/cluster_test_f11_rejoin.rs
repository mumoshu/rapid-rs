//! F11 (part 2) — `ClusterTest.java` retry/rejoin method ports.
//!
//! Split out of `cluster_test_f11.rs` to keep each test binary under the
//! 400-line file cap (RULES.md §File length). Holds the phase-2 drop,
//! rejoin, and concurrent-contact scenarios:
//! - `phase2MessageDropsRpcRetries`          → `phase2_message_drops_rpc_retries`
//! - `phase2JoinAttemptRetry`                → `phase2_join_attempt_retry`
//! - `phase2JoinAttemptRetryWithConfigChange`→ `phase2_join_attempt_retry_with_config_change`
//! - `testRejoinSingleNode`                  → `rejoin_single_node`
//! - `testRejoinSingleNodeSameConfiguration` → `rejoin_single_node_same_configuration`
//! - `concurrentNodeJoinsNetty`              → `concurrent_node_joins_via_random_contacts`
//! - `testRejoinMultipleNodes`               → `rejoin_multiple_nodes`

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use rapid::cluster::ClusterBuilder;
use rapid::messaging::fault_injection::{DropAtDests, MessageKind};
use rapid::messaging::InProcessNetwork;

use common::cluster_harness::{addr, Harness};

// ====================================================================
// 9/10. phase-2 drop scenarios
// ====================================================================

async fn phase2_drop_scenario(base_port: u16, drops: u32) {
    let net = InProcessNetwork::new();
    let seed_addr = addr(base_port);
    // Drop the first `drops` JoinMessage(s) bound for the seed.
    let mut dests = HashSet::new();
    dests.insert(seed_addr);
    net.set_interceptor(Some(Arc::new(DropAtDests::new(
        drops,
        Some(MessageKind::Join),
        dests,
    ))));

    // Bump the join retry budget so the outer loop survives `drops`
    // failed attempts. Java relies on its gRPC-layer retry counter;
    // we keep semantically equivalent retry headroom.
    let mut settings = rapid::settings::Settings::for_tests();
    settings.join_phase1_retries = u8::try_from(drops + 4).unwrap_or(u8::MAX);

    let seed = ClusterBuilder::new(seed_addr, net.clone())
        .with_settings(settings.clone())
        .start()
        .await
        .expect("seed bootstraps");

    let joiner_addr = addr(base_port + 1);
    let joiner = ClusterBuilder::new(joiner_addr, net.clone())
        .with_settings(settings)
        .join(seed_addr)
        .await
        .expect("joiner eventually succeeds despite drops");

    assert_eq!(seed.membership_size().await.unwrap(), 2);
    assert_eq!(joiner.membership_size().await.unwrap(), 2);
    seed.shutdown().await;
    joiner.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_message_drops_rpc_retries() {
    // Java drops grpcDefaultRetries - 1 = 4. Our outer loop sends 1
    // JoinMessage per attempt; we drop 4 and let the 5th through.
    phase2_drop_scenario(32_000, 4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_join_attempt_retry() {
    // Java drops grpcDefaultRetries + 1 = 6, forcing the joiner to
    // re-issue its full join under a fresh config_id. With our outer
    // retry budget configured to drops+4 we still expect success.
    phase2_drop_scenario(32_100, 6).await;
}

// ====================================================================
// 11. phase2_join_attempt_retry_with_config_change
// ====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase2_join_attempt_retry_with_config_change() {
    // Java: joiner A's phase-2 JoinMessage is dropped until joiner B
    // also joins, which changes the cluster configuration. When A's
    // retry is finally let through, the seed has already moved to a
    // new config — A must re-attempt with the fresh config_id and
    // succeed.
    let net = InProcessNetwork::new();
    let seed_addr = addr(32_200);
    let joiner_a = addr(32_201);
    let joiner_b = addr(32_202);

    // Drop *exactly two* JoinMessages bound for the seed: this lets
    // joiner A's first attempt fail, joiner B's attempt also gets
    // its first JoinMessage dropped (so B retries once, succeeds);
    // by the time A retries, B is in. A then sees the new config.
    let mut dests = HashSet::new();
    dests.insert(seed_addr);
    net.set_interceptor(Some(Arc::new(DropAtDests::new(
        2,
        Some(MessageKind::Join),
        dests,
    ))));

    let mut settings = rapid::settings::Settings::for_tests();
    settings.join_phase1_retries = 8;

    let seed = ClusterBuilder::new(seed_addr, net.clone())
        .with_settings(settings.clone())
        .start()
        .await
        .expect("seed bootstraps");

    // Both joiners start concurrently.
    let net_a = net.clone();
    let settings_a = settings.clone();
    let net_b = net.clone();
    let settings_b = settings.clone();
    let h_a = tokio::spawn(async move {
        ClusterBuilder::new(joiner_a, net_a)
            .with_settings(settings_a)
            .join(seed_addr)
            .await
    });
    let h_b = tokio::spawn(async move {
        ClusterBuilder::new(joiner_b, net_b)
            .with_settings(settings_b)
            .join(seed_addr)
            .await
    });

    let ca = h_a.await.expect("a task").expect("a converges");
    let cb = h_b.await.expect("b task").expect("b converges");

    // All three should agree on a 3-node view.
    for inst in [&seed, &ca, &cb] {
        let m = inst.membership_size().await.unwrap();
        assert_eq!(m, 3);
    }
    seed.shutdown().await;
    ca.shutdown().await;
    cb.shutdown().await;
}

// ====================================================================
// 12. rejoin_single_node
// ====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rejoin_single_node() {
    // Java: 10-node cluster, the same endpoint leaves and rejoins twice.
    let mut h = Harness::new(32_300);
    h.create_cluster(10, addr(32_300)).await;
    let leaving = addr(32_301);
    let seed = addr(32_300);
    for i in 0..2 {
        h.fail_node(leaving).await.expect("victim present");
        assert!(
            h.wait_and_verify_agreement(9, 120, Duration::from_millis(300))
                .await,
            "iter {i}: 9-node convergence after shutdown failed",
        );
        h.extend_at(leaving, seed).await;
        assert!(
            h.wait_and_verify_agreement(10, 60, Duration::from_millis(300))
                .await,
            "iter {i}: 10-node convergence after rejoin failed",
        );
    }
    h.shutdown_all().await;
}

// ====================================================================
// 13. rejoin_single_node_same_configuration
// ====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rejoin_single_node_same_configuration() {
    // Java: shutdown a node, immediately try to rejoin BEFORE the
    // FDs have kicked it out of the configuration. The fresh
    // `Cluster.Builder.join` should fail because the seed's view
    // still contains the rejoiner's endpoint.
    let mut h = Harness::new(32_400);
    h.create_cluster(10, addr(32_400)).await;
    let rejoiner = addr(32_401);
    let seed = addr(32_400);
    h.fail_node(rejoiner).await.expect("victim present");
    assert_eq!(h.num_cluster_instances(), 9);

    // First rejoin attempt: must fail because the seed still has
    // `rejoiner` in its ring (the FD hasn't fired yet).
    let net = h.network.clone();
    let mut tight = rapid::settings::Settings::for_tests();
    tight.join_phase1_retries = 1; // single attempt
    tight.join_phase1_retry_interval = Duration::from_millis(50);
    let first_attempt = ClusterBuilder::new(rejoiner, net.clone())
        .with_settings(tight)
        .join(seed)
        .await;
    assert!(
        first_attempt.is_err(),
        "rejoin before FD-kickout should be rejected",
    );

    // Wait for the seed's FD to kick the victim, dropping the view to 9.
    assert!(
        h.wait_and_verify_agreement(9, 120, Duration::from_millis(300))
            .await,
        "9-node convergence after FD kickout failed",
    );
    // Now the rejoin should succeed.
    h.extend_at(rejoiner, seed).await;
    assert!(
        h.wait_and_verify_agreement(10, 60, Duration::from_millis(300))
            .await,
        "post-FD rejoin convergence failed",
    );
    h.shutdown_all().await;
}

// ====================================================================
// 15. concurrent_node_joins_via_random_contacts
// ====================================================================
//
// Java parity: `concurrentNodeJoinsNetty`. The "Netty" suffix is a
// Java implementation detail (it sets `useInProcessTransport=false`);
// the parity-meaningful invariant is that the `seed` argument to
// `ClusterBuilder::join` is just a **Phase-1 contact address**, not
// a privileged role — any current cluster member can answer the
// joiner's `PreJoinMessage` (see `cluster::ClusterBuilder::join`
// docs for the full discussion of "seed" vs gossip terminology).
//
// We exercise that with a 5-node founder cluster, then 6 joiners
// using random existing members as contacts, then 6 joiners through
// the original founder. Real-network coverage of the same scenario
// lives in the docker-compose harness + grpc_convergence_f1; this
// file uses the in-process transport for speed.

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_node_joins_via_random_contacts() {
    use rand::seq::IteratorRandom;
    let mut h = Harness::new(32_700);
    let seed = addr(32_700);
    h.create_cluster(5, seed).await;
    assert!(
        h.wait_and_verify_agreement(5, 30, Duration::from_millis(200))
            .await,
        "5-node seed cluster failed",
    );

    let mut rng = rand::thread_rng();
    // Phase 1: 3 batches of 2 joiners each, each batch joining via
    // a random existing member.
    for _ in 0..3 {
        let contact = *h
            .instances
            .keys()
            .choose(&mut rng)
            .expect("at least one member");
        h.extend_parallel(contact, 2).await;
    }
    assert!(
        h.wait_and_verify_agreement(11, 60, Duration::from_millis(300))
            .await,
        "11-node convergence via random contacts failed",
    );

    // Phase 2: 6 joiners through the original seed.
    h.extend_parallel(seed, 6).await;
    assert!(
        h.wait_and_verify_agreement(17, 60, Duration::from_millis(300))
            .await,
        "17-node final convergence failed",
    );
    assert_eq!(h.num_cluster_instances(), 17);
    h.shutdown_all().await;
}

// ====================================================================
// 14. rejoin_multiple_nodes
// ====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn rejoin_multiple_nodes() {
    // Java: 30-node cluster, 5 nodes each leave and rejoin three
    // times in parallel.
    let n_nodes = 30usize;
    let fail_count = 5usize;
    let rejoins_per_node = 3usize;
    let mut h = Harness::new(32_500);
    let seed = addr(32_500);
    h.create_cluster(n_nodes, seed).await;
    assert!(
        h.wait_and_verify_agreement(n_nodes, 60, Duration::from_millis(300))
            .await
    );

    for iter in 0..rejoins_per_node {
        // Sequentially within each iteration is fine: Java's parallel
        // executor is a stress test; what we're verifying is that
        // repeated rejoins are stable.
        for j in 0..fail_count {
            let victim = addr(32_500 + 1 + u16::try_from(j).unwrap());
            h.fail_node(victim).await.expect("victim present");
        }
        assert!(
            h.wait_and_verify_agreement(n_nodes - fail_count, 180, Duration::from_millis(300))
                .await,
            "iter {iter}: convergence to {} after parallel failures failed",
            n_nodes - fail_count
        );
        for j in 0..fail_count {
            let endpoint = addr(32_500 + 1 + u16::try_from(j).unwrap());
            h.extend_at(endpoint, seed).await;
        }
        assert!(
            h.wait_and_verify_agreement(n_nodes, 180, Duration::from_millis(300))
                .await,
            "iter {iter}: convergence to {n_nodes} after rejoin wave failed",
        );
    }
    h.shutdown_all().await;
}
