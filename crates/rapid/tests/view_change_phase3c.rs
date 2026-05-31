//! Phase 3c integration tests:
//! - Direct `apply_proposal` injection adds a joiner, fires a `ViewChange`
//!   event with the new membership + delta, and advances `ConfigurationId`.
//! - `apply_proposal` that removes a node fires a `ViewChange` and rebuilds
//!   the FD task set.
//! - `LeaveMessage` enqueues a DOWN alert against the sender.
//! - `Cluster::leave_gracefully` sends `LeaveMessage`s best-effort to current
//!   observers and returns within `leave_message_timeout`.
//! - Subscriptions: a parked subscriber receives the event when applied.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use rapid::cluster::{leave_gracefully, ClusterBuilder};
use rapid::cut_detector::MultiNodeCutDetector;
use rapid::events::ClusterEvent;
use rapid::messaging::handler::ProbeOnlyHandler;
use rapid::messaging::InProcessNetwork;
use rapid::metadata::MetadataManager;
use rapid::monitoring::NoOpFactory;
use rapid::pb;
use rapid::service::state::EndpointKey;
use rapid::service::{MembershipService, ServiceState};
use rapid::settings::Settings;
use rapid::view::MembershipView;
use rapid::TokioClock;

fn ep(host: &str, port: i32) -> pb::Endpoint {
    pb::Endpoint {
        hostname: host.as_bytes().to_vec(),
        port,
    }
}

fn nid(high: i64, low: i64) -> pb::NodeId {
    pb::NodeId { high, low }
}

fn build_view(self_port: i32, other_ports: &[i32]) -> MembershipView {
    let endpoints: Vec<pb::Endpoint> = std::iter::once(self_port)
        .chain(other_ports.iter().copied())
        .map(|p| ep("127.0.0.1", p))
        .collect();
    let ids: Vec<pb::NodeId> = (0..endpoints.len())
        .map(|i| nid(0, i64::try_from(i).unwrap()))
        .collect();
    MembershipView::bootstrap(10, ids, endpoints).expect("view bootstraps")
}

#[tokio::test]
async fn apply_proposal_adds_joiner_and_fires_view_change() {
    let self_addr = ep("127.0.0.1", 9600);
    let joiner = ep("127.0.0.1", 9601);
    let view = build_view(9600, &[]);
    let cd = MultiNodeCutDetector::new(10, 9, 4).unwrap();
    let metadata = MetadataManager::new();
    let settings = Settings::default();

    let mut state = ServiceState::new(self_addr.clone(), view, cd, metadata, settings);
    // Pre-stage joiner data the same way `handle_join` would.
    let joiner_id = nid(7, 7);
    state
        .joiner_uuid
        .insert(EndpointKey::from(&joiner), joiner_id);
    state
        .joiner_metadata
        .insert(EndpointKey::from(&joiner), pb::Metadata::default());

    let svc = MembershipService::spawn(state);
    let mut sub = svc.subscribe(ClusterEvent::ViewChange);

    let before = svc.configuration_id().await.unwrap();
    svc.apply_proposal(vec![joiner.clone()]).await.unwrap();
    let after = svc.configuration_id().await.unwrap();
    assert_ne!(before, after);

    let event = tokio::time::timeout(Duration::from_millis(200), sub.recv())
        .await
        .expect("ViewChange event arrives")
        .expect("channel open");
    assert_eq!(event.membership.len(), 2);
    assert_eq!(event.delta.len(), 1);
    assert_eq!(event.delta[0].endpoint, joiner);
    assert_eq!(event.delta[0].status, pb::EdgeStatus::Up);
    svc.shutdown().await;
}

#[tokio::test]
async fn apply_proposal_removes_node_and_rebuilds_fd_tasks() {
    let self_addr = ep("127.0.0.1", 9700);
    let victim = ep("127.0.0.1", 9701);
    let view = build_view(9700, &[9701, 9702]);
    let cd = MultiNodeCutDetector::new(10, 9, 4).unwrap();
    let metadata = MetadataManager::new();
    let settings = Settings::default();

    let (notifier_tx, _notifier_rx) = mpsc::channel::<rapid::monitoring::factory::EdgeFailure>(16);
    let factory: Arc<dyn rapid::monitoring::factory::EdgeFailureDetectorFactory> =
        Arc::new(NoOpFactory);
    let state = ServiceState::new(self_addr, view, cd, metadata, settings)
        .with_fd_factory(factory, notifier_tx);

    let svc = MembershipService::spawn(state);
    let mut sub = svc.subscribe(ClusterEvent::ViewChange);
    // Pre-populate FD tasks for the 3-node view.
    let before_count = svc.rebuild_failure_detectors().await.unwrap();
    assert_eq!(before_count, 10);

    svc.apply_proposal(vec![victim.clone()]).await.unwrap();

    let event = tokio::time::timeout(Duration::from_millis(200), sub.recv())
        .await
        .expect("ViewChange event")
        .expect("channel open");
    assert_eq!(event.membership.len(), 2);
    assert_eq!(event.delta.len(), 1);
    assert_eq!(event.delta[0].endpoint, victim);
    assert_eq!(event.delta[0].status, pb::EdgeStatus::Down);
    // FD tasks were rebuilt for the new (smaller) view, still K=10
    // because get_subjects_of returns K with duplicates.
    let after_count = svc.rebuild_failure_detectors().await.unwrap();
    assert_eq!(after_count, 10);
    svc.shutdown().await;
}

#[tokio::test]
async fn leave_message_enqueues_down_alert() {
    let self_addr = ep("127.0.0.1", 9800);
    let leaver = ep("127.0.0.1", 9801);
    let view = build_view(9800, &[9801]);
    let cd = MultiNodeCutDetector::new(10, 9, 4).unwrap();
    let metadata = MetadataManager::new();
    let settings = Settings::default();
    let state = ServiceState::new(self_addr.clone(), view, cd, metadata, settings);
    let svc = MembershipService::spawn(state);

    let req = pb::RapidRequest {
        content: Some(pb::rapid_request::Content::LeaveMessage(pb::LeaveMessage {
            sender: Some(leaver),
        })),
    };
    svc.dispatch(req).await.unwrap();
    let queue_len = svc.alert_queue_len().await.unwrap();
    assert!(queue_len >= 1, "expected at least one DOWN alert enqueued");
    svc.shutdown().await;
}

#[tokio::test]
async fn cluster_leave_gracefully_returns_within_timeout() {
    let net = InProcessNetwork::new();
    let observer_addr: SocketAddr = "127.0.0.1:9900".parse().unwrap();
    let _server = net.spawn(observer_addr, ProbeOnlyHandler);

    let self_endpoint = ep("127.0.0.1", 9999);
    let client = Arc::new(net.client());
    let settings = Settings::default();
    let clock = TokioClock;
    let start = std::time::Instant::now();
    leave_gracefully(
        self_endpoint,
        vec![observer_addr],
        &settings,
        client,
        &clock,
    )
    .await
    .unwrap();
    assert!(
        start.elapsed() < settings.leave_message_timeout + Duration::from_millis(500),
        "leave_gracefully exceeded its budget"
    );
}

#[tokio::test]
async fn cluster_subscribe_delivers_view_change_event_to_late_subscriber() {
    // Cluster::subscribe must be callable AFTER bootstrap; the subscriber
    // sees only future events. We test by subscribing, then injecting an
    // ApplyProposal.
    let net = InProcessNetwork::new();
    let seed_addr: SocketAddr = "127.0.0.1:9950".parse().unwrap();
    let cluster = ClusterBuilder::new(seed_addr, net).start().await.unwrap();
    let mut sub = cluster.subscribe(ClusterEvent::ViewChange);

    let joiner = ep("127.0.0.1", 9951);
    let joiner_id = nid(11, 22);
    // Reach into the service and stage the joiner uuid/metadata, then
    // apply a proposal. The actor accepts both via the public API.
    // For Phase 3c we don't have a "stage joiner" command; instead we
    // simulate by directly dispatching a JoinMessage with the right
    // configuration_id, which parks the reply and stages the joiner.
    let config_id = cluster.configuration_id().await.unwrap().as_i64();
    let join_req = pb::RapidRequest {
        content: Some(pb::rapid_request::Content::JoinMessage(pb::JoinMessage {
            sender: Some(joiner.clone()),
            node_id: Some(joiner_id),
            ring_number: vec![0],
            configuration_id: config_id,
            metadata: Some(pb::Metadata::default()),
        })),
    };
    let svc = cluster.service().clone();
    tokio::spawn(async move {
        let _ = svc.dispatch(join_req).await;
    });
    tokio::task::yield_now().await;

    cluster
        .service()
        .apply_proposal(vec![joiner.clone()])
        .await
        .unwrap();
    let event = tokio::time::timeout(Duration::from_millis(200), sub.recv())
        .await
        .expect("event arrives")
        .expect("channel open");
    assert!(event.membership.iter().any(|e| e == &joiner));
    cluster.shutdown().await;
}
