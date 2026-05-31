//! F1 follow-up gate: a 2-node cluster bootstraps over real gRPC and
//! converges to a 2-node view with identical `ConfigurationId`.

use std::net::SocketAddr;
use std::time::Duration;

use rapid::cluster::ClusterBuilder;

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_node_grpc_bootstrap_converges() {
    // Pick high ports unlikely to collide with anything else.
    let seed_addr = addr(28_400);
    let seed = ClusterBuilder::with_grpc(seed_addr)
        .start()
        .await
        .expect("seed bootstraps over gRPC");

    let joiner_addr = addr(28_401);
    let joiner = tokio::time::timeout(
        Duration::from_secs(15),
        ClusterBuilder::with_grpc(joiner_addr).join(seed_addr),
    )
    .await
    .expect("join completed within 15s")
    .expect("joiner converges via gRPC");

    let mut converged = false;
    for _ in 0..100 {
        let seed_members = seed.memberlist().await.unwrap();
        let joiner_members = joiner.memberlist().await.unwrap();
        if seed_members.len() == 2 && joiner_members.len() == 2 {
            let seed_id = seed.configuration_id().await.unwrap();
            let joiner_id = joiner.configuration_id().await.unwrap();
            assert_eq!(seed_id, joiner_id);
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(converged, "2-node gRPC convergence failed within 10s");

    joiner.shutdown().await;
    seed.shutdown().await;
}
