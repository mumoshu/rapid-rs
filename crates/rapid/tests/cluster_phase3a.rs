//! Phase 3a integration tests:
//! - 1-node seed bootstrap exposes a `MembershipView` containing itself
//!   and a non-sentinel `ConfigurationId`.
//! - `PreJoinMessage` from a fresh node receives `SAFE_TO_JOIN` and the
//!   expected K observers.
//! - `PreJoinMessage` from a hostname already present is rejected with
//!   `HOSTNAME_ALREADY_IN_RING`.
//! - `JoinMessage` with stale `configurationId` is rejected with
//!   `CONFIG_CHANGED`.

use std::net::SocketAddr;
use std::time::Duration;

use rapid::cluster::ClusterBuilder;
use rapid::messaging::traits::MessagingClient;
use rapid::messaging::InProcessNetwork;
use rapid::pb;
use rapid::types::ConfigurationId;

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}")
        .parse()
        .expect("literal address parses")
}

fn ep(host: &str, port: i32) -> pb::Endpoint {
    pb::Endpoint {
        hostname: host.as_bytes().to_vec(),
        port,
    }
}

fn nid(high: i64, low: i64) -> pb::NodeId {
    pb::NodeId { high, low }
}

fn pre_join(sender: pb::Endpoint, node_id: pb::NodeId) -> pb::RapidRequest {
    pb::RapidRequest {
        content: Some(pb::rapid_request::Content::PreJoinMessage(
            pb::PreJoinMessage {
                sender: Some(sender),
                node_id: Some(node_id),
                ring_number: Vec::new(),
                configuration_id: -1,
            },
        )),
    }
}

fn join(
    sender: pb::Endpoint,
    node_id: pb::NodeId,
    configuration_id: i64,
    rings: Vec<i32>,
) -> pb::RapidRequest {
    pb::RapidRequest {
        content: Some(pb::rapid_request::Content::JoinMessage(pb::JoinMessage {
            sender: Some(sender),
            node_id: Some(node_id),
            ring_number: rings,
            configuration_id,
            metadata: Some(pb::Metadata::default()),
        })),
    }
}

#[tokio::test]
async fn one_node_seed_bootstrap() {
    let net = InProcessNetwork::new();
    let seed_addr = addr(9100);
    let cluster = ClusterBuilder::new(seed_addr, net)
        .start()
        .await
        .expect("seed bootstraps");
    let members = cluster.memberlist().await.expect("memberlist");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].port, i32::from(seed_addr.port()));
    let config_id = cluster.configuration_id().await.expect("config id");
    assert_ne!(config_id, ConfigurationId::SENTINEL);
    cluster.shutdown().await;
}

#[tokio::test]
async fn pre_join_returns_observers_and_safe_to_join() {
    let net = InProcessNetwork::new();
    let seed_addr = addr(9110);
    let cluster = ClusterBuilder::new(seed_addr, net.clone())
        .start()
        .await
        .expect("seed bootstraps");

    let client = net.client();
    let joiner_addr = addr(9111);
    let resp = client
        .send(
            seed_addr,
            pre_join(ep("127.0.0.1", i32::from(joiner_addr.port())), nid(1, 1)),
        )
        .await
        .expect("seed responds");
    let Some(pb::rapid_response::Content::JoinResponse(jr)) = resp.content else {
        panic!("expected JoinResponse");
    };
    assert_eq!(jr.status_code, pb::JoinStatusCode::SafeToJoin as i32);
    // K=10 endpoints — the single seed repeated.
    assert_eq!(jr.endpoints.len(), 10);
    assert!(jr
        .endpoints
        .iter()
        .all(|e| e.port == i32::from(seed_addr.port())));
    cluster.shutdown().await;
}

#[tokio::test]
async fn pre_join_from_existing_host_is_rejected() {
    let net = InProcessNetwork::new();
    let seed_addr = addr(9120);
    let cluster = ClusterBuilder::new(seed_addr, net.clone())
        .start()
        .await
        .expect("seed bootstraps");
    let client = net.client();
    let resp = client
        .send(
            seed_addr,
            pre_join(ep("127.0.0.1", i32::from(seed_addr.port())), nid(99, 99)),
        )
        .await
        .expect("seed responds");
    let Some(pb::rapid_response::Content::JoinResponse(jr)) = resp.content else {
        panic!("expected JoinResponse");
    };
    assert_eq!(
        jr.status_code,
        pb::JoinStatusCode::HostnameAlreadyInRing as i32
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn join_with_stale_configuration_returns_config_changed() {
    let net = InProcessNetwork::new();
    let seed_addr = addr(9130);
    let cluster = ClusterBuilder::new(seed_addr, net.clone())
        .start()
        .await
        .expect("seed bootstraps");
    let client = net.client();
    let joiner_endpoint = ep("127.0.0.1", 9131);
    // Send a JoinMessage with a deliberately-wrong configuration_id. The
    // seed has just bootstrapped so its config-id is non-zero and won't
    // match `42`.
    let resp = client
        .send(
            seed_addr,
            join(joiner_endpoint.clone(), nid(42, 42), 42, vec![0]),
        )
        .await
        .expect("seed responds");
    let Some(pb::rapid_response::Content::JoinResponse(jr)) = resp.content else {
        panic!("expected JoinResponse");
    };
    assert_eq!(jr.status_code, pb::JoinStatusCode::ConfigChanged as i32);
    cluster.shutdown().await;
}

#[tokio::test]
async fn join_with_correct_configuration_parks_reply() {
    // The actor stashes the reply oneshot in `joinersToRespondTo` and
    // doesn't fire it until consensus (Phase 4). Verify the dispatch
    // doesn't immediately resolve.
    let net = InProcessNetwork::new();
    let seed_addr = addr(9140);
    let cluster = ClusterBuilder::new(seed_addr, net.clone())
        .start()
        .await
        .expect("seed bootstraps");
    let config_id = cluster.configuration_id().await.unwrap().as_i64();
    let client = net.client();
    let joiner = ep("127.0.0.1", 9141);
    let fut = client.send(seed_addr, join(joiner, nid(7, 7), config_id, vec![0, 1, 2]));
    // Phase 3a parks the reply; assert we timeout rather than receive.
    let timed_out = tokio::time::timeout(Duration::from_millis(200), fut).await;
    assert!(
        timed_out.is_err(),
        "join reply should be parked, not delivered"
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn probe_message_returns_ok() {
    let net = InProcessNetwork::new();
    let seed_addr = addr(9150);
    let cluster = ClusterBuilder::new(seed_addr, net.clone())
        .start()
        .await
        .expect("seed bootstraps");
    let client = net.client();
    let req = pb::RapidRequest {
        content: Some(pb::rapid_request::Content::ProbeMessage(
            pb::ProbeMessage::default(),
        )),
    };
    let resp = client.send(seed_addr, req).await.expect("seed responds");
    let Some(pb::rapid_response::Content::ProbeResponse(p)) = resp.content else {
        panic!("expected ProbeResponse");
    };
    assert_eq!(p.status(), pb::NodeStatus::Ok);
    cluster.shutdown().await;
}
