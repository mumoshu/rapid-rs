//! Phase 3b integration tests:
//! - K alerts about a subject crossing H produce exactly one proposal.
//! - 50 alerts enqueued inside one batching window fan out as a single
//!   `BatchedAlertMessage`.
//! - Per-subject FD task scheduling: N>=2 view spawns K detector tasks.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use rapid::clock::{Clock, MockClock};
use rapid::cut_detector::MultiNodeCutDetector;
use rapid::messaging::traits::Broadcaster;
use rapid::metadata::MetadataManager;
use rapid::monitoring::NoOpFactory;
use rapid::pb;
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

/// Recording broadcaster that captures every `BatchedAlertMessage` sent.
struct RecordingBroadcaster {
    sent: Mutex<Vec<pb::RapidRequest>>,
}

impl RecordingBroadcaster {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            sent: Mutex::new(Vec::new()),
        })
    }

    fn count(&self) -> usize {
        self.sent.lock().len()
    }

    fn last_batched_alert(&self) -> Option<pb::BatchedAlertMessage> {
        let sent = self.sent.lock();
        let last = sent.last()?.clone();
        match last.content? {
            pb::rapid_request::Content::BatchedAlertMessage(b) => Some(b),
            _ => None,
        }
    }
}

#[async_trait]
impl Broadcaster for RecordingBroadcaster {
    async fn set_membership(&self, _recipients: Vec<SocketAddr>) {}
    async fn broadcast(&self, req: pb::RapidRequest) {
        self.sent.lock().push(req);
    }
}

fn build_view(self_port: i32, other_ports: &[i32]) -> MembershipView {
    let endpoints: Vec<pb::Endpoint> = std::iter::once(self_port)
        .chain(other_ports.iter().copied())
        .map(|p| ep("127.0.0.1", p))
        .collect();
    let ids: Vec<pb::NodeId> = (0..endpoints.len())
        .map(|i| nid(0, i64::try_from(i).expect("fits")))
        .collect();
    MembershipView::bootstrap(10, ids, endpoints).expect("view bootstraps")
}

#[tokio::test]
async fn k_down_alerts_emit_one_proposal() {
    // View contains self + the subject. K=10 DOWN alerts about the subject
    // (each on a different ring) cross H=9 and emit a single proposal.
    let self_addr = ep("127.0.0.1", 9200);
    let subject = ep("127.0.0.1", 9201);
    let view = build_view(9200, &[9201]);
    let cd = MultiNodeCutDetector::new(10, 9, 4).unwrap();
    let metadata = MetadataManager::new();
    let settings = Settings::default();
    let configuration_id = {
        let mut v = view;
        v.current_configuration_id().as_i64()
    };
    let view = build_view(9200, &[9201]); // rebuild after consuming
    let (proposal_tx, mut proposal_rx) = mpsc::unbounded_channel::<Vec<pb::Endpoint>>();
    let bc = RecordingBroadcaster::new();
    let state = ServiceState::new(self_addr.clone(), view, cd, metadata, settings)
        .with_broadcaster(bc.clone())
        .with_proposal_sink(proposal_tx);
    let svc = MembershipService::spawn(state);

    // Build the batched alert: 10 entries, each ring 0..9, edge_dst = subject.
    let mut alerts = Vec::new();
    for k in 0..10 {
        alerts.push(pb::AlertMessage {
            edge_src: Some(ep("127.0.0.1", 9000 + k)), // arbitrary unique src
            edge_dst: Some(subject.clone()),
            edge_status: pb::EdgeStatus::Down as i32,
            configuration_id,
            ring_number: vec![k],
            node_id: None,
            metadata: None,
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
    svc.dispatch(req).await.expect("actor responds");

    let proposal = tokio::time::timeout(Duration::from_millis(200), proposal_rx.recv())
        .await
        .expect("proposal arrives")
        .expect("channel open");
    assert_eq!(
        proposal.len(),
        1,
        "expected exactly one endpoint in proposal"
    );
    assert_eq!(proposal[0], subject);
    // Channel must not emit a second proposal.
    let second = tokio::time::timeout(Duration::from_millis(100), proposal_rx.recv()).await;
    assert!(second.is_err(), "expected exactly one proposal");

    svc.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn fifty_alerts_in_window_fan_out_one_batched_message() {
    // Configure the batcher to tick every 100 ms (default). Enqueue 50
    // alerts via the join-handler side-effect; after one window the
    // recording broadcaster sees exactly one BatchedAlertMessage with all
    // 50 alerts.

    let self_addr = ep("127.0.0.1", 9300);
    let view = build_view(9300, &[9301]);
    let cd = MultiNodeCutDetector::new(10, 9, 4).unwrap();
    let metadata = MetadataManager::new();
    let settings = Settings::default();
    let clock: Arc<dyn Clock> = Arc::new(MockClock::new());
    let bc = RecordingBroadcaster::new();
    let state = ServiceState::new(self_addr.clone(), view, cd, metadata, settings.clone())
        .with_clock(clock.clone())
        .with_broadcaster(bc.clone());
    let svc = MembershipService::spawn(state);
    let _batcher_handle = rapid::service::alert_batcher::spawn_batcher_loop(
        clock,
        svc.sender(),
        settings.batching_window,
    );

    let config_id = svc.configuration_id().await.unwrap().as_i64();

    // 50 PreJoin/Join roundtrips would be unwieldy — instead drive 50 UP
    // alerts directly through 50 distinct joiner sends.
    for i in 0..50 {
        let joiner = ep("127.0.0.1", 10_000 + i);
        let req = pb::RapidRequest {
            content: Some(pb::rapid_request::Content::JoinMessage(pb::JoinMessage {
                sender: Some(joiner),
                node_id: Some(nid(0, i64::from(i + 100))),
                ring_number: vec![0],
                configuration_id: config_id,
                metadata: Some(pb::Metadata::default()),
            })),
        };
        // Fire and forget — the reply is parked.
        let svc_clone = svc.clone();
        tokio::spawn(async move {
            let _ = svc_clone.dispatch(req).await;
        });
    }
    // Give the dispatch tasks a chance to land in the actor's queue.
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(5)).await;
    tokio::task::yield_now().await;

    // Confirm 50 alerts are queued before any window has elapsed.
    let len_before = svc.alert_queue_len().await.unwrap();
    assert_eq!(len_before, 50, "expected 50 queued alerts before window");

    // Advance the clock past the batching window. The batcher tick should
    // drain the queue and call broadcast() exactly once.
    tokio::time::advance(settings.batching_window + Duration::from_millis(5)).await;
    // The batcher loop needs to wake, post a tick, the actor needs to
    // process it, and the broadcaster.broadcast() must record it.
    for _ in 0..10 {
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        if bc.count() > 0 {
            break;
        }
    }

    assert_eq!(bc.count(), 1, "expected one BatchedAlertMessage broadcast");
    let batch = bc
        .last_batched_alert()
        .expect("a BatchedAlertMessage was sent");
    assert_eq!(batch.messages.len(), 50, "expected 50 alerts in batch");
    assert!(batch.sender.is_some());
    svc.shutdown().await;
}

#[tokio::test]
async fn fd_rebuild_spawns_k_tasks_for_multi_node_view() {
    // With N>=2 and K=10, get_subjects_of returns K endpoints → K FD tasks.
    let self_addr = ep("127.0.0.1", 9400);
    let view = build_view(9400, &[9401, 9402, 9403]);
    let cd = MultiNodeCutDetector::new(10, 9, 4).unwrap();
    let metadata = MetadataManager::new();
    let settings = Settings::default();
    let (notifier_tx, _notifier_rx) = mpsc::channel::<rapid::monitoring::factory::EdgeFailure>(16);
    let factory: Arc<dyn rapid::monitoring::factory::EdgeFailureDetectorFactory> =
        Arc::new(NoOpFactory);
    let state = ServiceState::new(self_addr, view, cd, metadata, settings)
        .with_fd_factory(factory, notifier_tx);
    let svc = MembershipService::spawn(state);
    let count = svc.rebuild_failure_detectors().await.unwrap();
    assert_eq!(count, 10);
    // Calling rebuild again replaces the set — count stays at 10 because
    // the view is unchanged. The implementation must abort the old set
    // before spawning the new one (verified by the abort() call in
    // fd_scheduler::rebuild).
    let count2 = svc.rebuild_failure_detectors().await.unwrap();
    assert_eq!(count2, 10);
    svc.shutdown().await;
}

#[tokio::test]
async fn fd_rebuild_returns_zero_for_single_node_view() {
    // Sanity check: a single-node cluster has no subjects.
    let self_addr = ep("127.0.0.1", 9500);
    let view = build_view(9500, &[]);
    let cd = MultiNodeCutDetector::new(10, 9, 4).unwrap();
    let metadata = MetadataManager::new();
    let settings = Settings::default();
    let (notifier_tx, _notifier_rx) = mpsc::channel::<rapid::monitoring::factory::EdgeFailure>(16);
    let factory: Arc<dyn rapid::monitoring::factory::EdgeFailureDetectorFactory> =
        Arc::new(NoOpFactory);
    let state = ServiceState::new(self_addr, view, cd, metadata, settings)
        .with_fd_factory(factory, notifier_tx);
    let svc = MembershipService::spawn(state);
    let count = svc.rebuild_failure_detectors().await.unwrap();
    assert_eq!(count, 0);
    svc.shutdown().await;
}
