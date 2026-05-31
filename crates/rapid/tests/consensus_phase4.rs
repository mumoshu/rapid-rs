//! Phase 4 integration tests:
//! - Cut-detector proposal → `FastPaxos` → decision → view-change (end-to-end
//!   actor pipeline, single-node).
//! - Classic-Paxos fallback path (`start_classic` via `StartClassicRound`).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;

use rapid::cut_detector::MultiNodeCutDetector;
use rapid::events::ClusterEvent;
use rapid::messaging::traits::Broadcaster;
use rapid::metadata::MetadataManager;
use rapid::pb;
use rapid::service::state::EndpointKey;
use rapid::service::{MembershipService, ServiceState};
use rapid::settings::Settings;
use rapid::view::MembershipView;

fn ep(host: &str, port: i32) -> pb::Endpoint {
    pb::Endpoint {
        hostname: host.as_bytes().to_vec(),
        port,
    }
}

fn nid(high: i64, low: i64) -> pb::NodeId {
    pb::NodeId { high, low }
}

/// Loopback broadcaster: every broadcast is routed back into the same
/// `MembershipService`'s dispatch. Used to model "broadcast to all current
/// members" for a 1-node cluster.
struct LoopbackBroadcaster {
    service: Mutex<Option<MembershipService>>,
}

impl LoopbackBroadcaster {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            service: Mutex::new(None),
        })
    }

    fn set_service(&self, svc: MembershipService) {
        *self.service.lock() = Some(svc);
    }
}

#[async_trait]
impl Broadcaster for LoopbackBroadcaster {
    async fn set_membership(&self, _recipients: Vec<SocketAddr>) {}
    async fn broadcast(&self, req: pb::RapidRequest) {
        let svc = self.service.lock().clone();
        if let Some(svc) = svc {
            let _ = svc.dispatch(req).await;
        }
    }
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
async fn single_node_cluster_proposal_decides_via_fast_paxos() {
    // 1-node seed: send a batched alert with K UP entries for a joiner;
    // the cut detector trips, FastPaxos propose + immediate decision
    // (N=1, F=0, quorum=1), apply_proposal fires the ViewChange event.
    let self_addr = ep("127.0.0.1", 6500);
    let joiner = ep("127.0.0.1", 6501);
    let view = build_view(6500, &[]);
    let cd = MultiNodeCutDetector::new(10, 9, 4).unwrap();
    let metadata = MetadataManager::new();
    let settings = Settings::default();

    let broadcaster = LoopbackBroadcaster::new();
    let mut state = ServiceState::new(self_addr.clone(), view, cd, metadata, settings)
        .with_broadcaster(broadcaster.clone() as Arc<dyn Broadcaster>);
    // Install FastPaxos for the seed configuration.
    rapid::service::consensus_dispatch::install_fast_paxos(&mut state);
    // Pre-stage the joiner UUID + metadata the way `handle_join` would.
    let joiner_id = nid(11, 11);
    state
        .joiner_uuid
        .insert(EndpointKey::from(&joiner), joiner_id);
    state
        .joiner_metadata
        .insert(EndpointKey::from(&joiner), pb::Metadata::default());

    let svc = MembershipService::spawn(state);
    broadcaster.set_service(svc.clone());
    let mut sub = svc.subscribe(ClusterEvent::ViewChange);

    let config_id = svc.configuration_id().await.unwrap().as_i64();
    // Build a BatchedAlertMessage with K UP entries about the joiner.
    let mut alerts = Vec::new();
    for k in 0..10 {
        alerts.push(pb::AlertMessage {
            edge_src: Some(ep("127.0.0.1", 6_500 + k)),
            edge_dst: Some(joiner.clone()),
            edge_status: pb::EdgeStatus::Up as i32,
            configuration_id: config_id,
            ring_number: vec![k],
            node_id: Some(joiner_id),
            metadata: Some(pb::Metadata::default()),
        });
    }
    let req = pb::RapidRequest {
        content: Some(pb::rapid_request::Content::BatchedAlertMessage(
            pb::BatchedAlertMessage {
                sender: Some(self_addr.clone()),
                messages: alerts,
            },
        )),
    };
    svc.dispatch(req).await.unwrap();

    let event = tokio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("ViewChange arrives")
        .expect("channel open");
    assert!(event.membership.iter().any(|e| e == &joiner));
    assert_eq!(event.delta.len(), 1);
    assert_eq!(event.delta[0].endpoint, joiner);
    assert_eq!(event.delta[0].status, pb::EdgeStatus::Up);

    svc.shutdown().await;
}

#[tokio::test]
async fn classic_round_can_be_started_after_fast_round_failure() {
    // Sanity: StartClassicRound command produces an outgoing Phase1a
    // broadcast (visible to a recording broadcaster).
    let self_addr = ep("127.0.0.1", 6700);
    let view = build_view(6700, &[]);
    let cd = MultiNodeCutDetector::new(10, 9, 4).unwrap();
    let metadata = MetadataManager::new();
    let settings = Settings::default();
    let bc: Arc<Recording> = Arc::new(Recording::default());
    let mut state = ServiceState::new(self_addr, view, cd, metadata, settings)
        .with_broadcaster(bc.clone() as Arc<dyn Broadcaster>);
    rapid::service::consensus_dispatch::install_fast_paxos(&mut state);
    let svc = MembershipService::spawn(state);
    svc.sender()
        .send(rapid::service::ServiceCommand::StartClassicRound)
        .await
        .unwrap();
    // Allow the dispatched broadcast task to land.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let captured = bc.captured.lock().clone();
    let saw_phase1a = captured.iter().any(|r| {
        matches!(
            r.content,
            Some(pb::rapid_request::Content::Phase1aMessage(_))
        )
    });
    assert!(
        saw_phase1a,
        "expected Phase1a broadcast after start-classic"
    );
    svc.shutdown().await;
}

#[derive(Default)]
struct Recording {
    captured: Mutex<Vec<pb::RapidRequest>>,
}

#[async_trait]
impl Broadcaster for Recording {
    async fn set_membership(&self, _recipients: Vec<SocketAddr>) {}
    async fn broadcast(&self, req: pb::RapidRequest) {
        self.captured.lock().push(req);
    }
}
