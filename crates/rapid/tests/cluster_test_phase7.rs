//! Phase 7 — `ClusterTest` ports over the in-process transport.
//!
//! Currently ported:
//! - `singleNodeJoinsThroughSeed`: one seed + one joiner converge to a
//!   2-node view with identical `ConfigurationId`.

use std::net::SocketAddr;
use std::time::Duration;

use rapid::cluster::ClusterBuilder;
use rapid::messaging::InProcessNetwork;

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_joins_through_seed() {
    let net = InProcessNetwork::new();
    let seed_addr = addr(10100);
    let joiner_addr = addr(10101);

    let seed = ClusterBuilder::new(seed_addr, net.clone())
        .start()
        .await
        .expect("seed bootstraps");

    let joiner = tokio::time::timeout(
        Duration::from_secs(10),
        ClusterBuilder::new(joiner_addr, net.clone()).join(seed_addr),
    )
    .await
    .expect("join timed out")
    .expect("joiner joins seed");

    // After the join protocol completes both nodes must agree on a
    // 2-node configuration. The seed needs an additional tick for the
    // consensus → view-change pipeline to apply locally; allow a small
    // poll window.
    let mut converged = false;
    for _ in 0..50 {
        let seed_members = seed.memberlist().await.unwrap();
        let joiner_members = joiner.memberlist().await.unwrap();
        if seed_members.len() == 2 && joiner_members.len() == 2 {
            let seed_id = seed.configuration_id().await.unwrap();
            let joiner_id = joiner.configuration_id().await.unwrap();
            assert_eq!(seed_id, joiner_id, "config IDs must match");
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        converged,
        "seed and joiner failed to converge to 2-node view"
    );

    joiner.shutdown().await;
    seed.shutdown().await;
}
