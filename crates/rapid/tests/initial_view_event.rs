//! Initial-view `VIEW_CHANGE` event — F5 gate.
//!
//! Java parity: `MembershipService` ctor fires a synthetic
//! `VIEW_CHANGE` with every member's status = UP. We expose the same
//! payload via `Cluster::initial_view_event()` so that subscribers
//! starting up post-bootstrap can prepend it to their stream without
//! racing the actor task.

use std::net::SocketAddr;

use rapid::cluster::ClusterBuilder;
use rapid::messaging::InProcessNetwork;
use rapid::pb;

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_view_event_includes_seed_endpoint_with_up_status() {
    let net = InProcessNetwork::new();
    let seed = ClusterBuilder::new(addr(35_000), net.clone())
        .with_settings(rapid::settings::Settings::for_tests())
        .start()
        .await
        .expect("seed bootstraps");
    let initial = seed.initial_view_event().await.expect("initial event");
    assert_eq!(initial.membership.len(), 1);
    assert_eq!(initial.delta.len(), 1);
    assert_eq!(initial.delta[0].status, pb::EdgeStatus::Up);
    assert_eq!(initial.delta[0].endpoint.port, 35_000);
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initial_view_event_after_join_lists_all_members() {
    let net = InProcessNetwork::new();
    let seed = ClusterBuilder::new(addr(35_100), net.clone())
        .with_settings(rapid::settings::Settings::for_tests())
        .start()
        .await
        .expect("seed bootstraps");
    let n1 = ClusterBuilder::new(addr(35_101), net.clone())
        .with_settings(rapid::settings::Settings::for_tests())
        .join(addr(35_100))
        .await
        .expect("n1 joins");
    let initial = n1
        .initial_view_event()
        .await
        .expect("initial event after join");
    assert_eq!(initial.membership.len(), 2);
    let ports: Vec<i32> = initial.delta.iter().map(|d| d.endpoint.port).collect();
    assert!(ports.contains(&35_100));
    assert!(ports.contains(&35_101));
    for d in &initial.delta {
        assert_eq!(d.status, pb::EdgeStatus::Up);
    }
    seed.shutdown().await;
    n1.shutdown().await;
}
