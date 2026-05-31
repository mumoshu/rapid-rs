//! Owned state of the membership-service actor.
//!
//! `ServiceState` is single-owner — never wrapped in a lock. The actor
//! task is the only thread that ever holds it. External callers go through
//! `ServiceCommand` mailbox entries.
//!
//! Concrete handlers live in `handlers.rs` (join/probe), `alert_handler.rs`
//! (batched alerts), `alert_batcher.rs` (the 100 ms batching loop), and
//! `fd_scheduler.rs` (per-subject failure-detector tasks).

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::clock::{Clock, TokioClock};
use crate::consensus::FastPaxos;
use crate::cut_detector::MultiNodeCutDetector;
use crate::error::Result;
use crate::events::{ClusterEvent, ClusterStatusChange};
use crate::messaging::traits::{Broadcaster, MessagingClient};
use crate::metadata::MetadataManager;
use crate::monitoring::factory::EdgeFailureDetectorFactory;
use crate::pb;
use crate::settings::Settings;
use crate::view::MembershipView;

/// Capacity of the per-event-type broadcast channel. Pinned in PLAN.md
/// (`Event channel` decision).
pub const EVENT_CHANNEL_CAPACITY: usize = 64;

/// Endpoint-comparable key. `view.rs` keeps its own copy private to make
/// the field layout there a pure implementation detail; ours is `pub`
/// for use by sibling modules in `service`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EndpointKey {
    /// hostname bytes
    pub hostname: Vec<u8>,
    /// port
    pub port: i32,
}

impl From<&pb::Endpoint> for EndpointKey {
    fn from(value: &pb::Endpoint) -> Self {
        Self {
            hostname: value.hostname.clone(),
            port: value.port,
        }
    }
}

/// Actor lifecycle. Mirrors Java's `hasShutdown` and `announcedProposal`
/// booleans as one enum (RULES §Types).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleState {
    /// Steady-state: handle messages normally.
    Running,
    /// A proposal has been emitted; further alert-driven decisions are
    /// suppressed until the view change is applied.
    AnnouncedProposal,
    /// Shutdown has been requested or applied.
    ShuttingDown,
}

/// Full actor state.
pub struct ServiceState {
    /// Settings (immutable per Cluster instance).
    pub settings: Settings,
    /// Our own endpoint.
    pub my_addr: pb::Endpoint,
    /// K monitoring rings + identifier set.
    pub view: MembershipView,
    /// H/L cut detector.
    pub cut_detector: MultiNodeCutDetector,
    /// Application metadata.
    pub metadata: MetadataManager,
    /// Per-joiner pending response futures (Phase 4 fires).
    pub joiners_to_respond_to:
        HashMap<EndpointKey, Vec<oneshot::Sender<Result<pb::RapidResponse>>>>,
    /// Per-joiner identifier learned in Phase 2 of the bootstrap protocol.
    pub joiner_uuid: HashMap<EndpointKey, pb::NodeId>,
    /// Per-joiner metadata.
    pub joiner_metadata: HashMap<EndpointKey, pb::Metadata>,
    /// Lifecycle state.
    pub lifecycle: LifecycleState,

    /// Outgoing `AlertMessage` queue (Java `sendQueue`).
    pub send_queue: VecDeque<pb::AlertMessage>,
    /// Monotonic timestamp of the most recent enqueue (Java
    /// `lastEnqueueTimestamp`). `None` means "queue idle".
    pub last_enqueue_at: Option<Instant>,
    /// Broadcaster used to fan-out `BatchedAlertMessage` to current
    /// membership.
    pub broadcaster: Option<Arc<dyn Broadcaster>>,
    /// Optional sink for proposal events. The Fast Paxos integration
    /// (Phase 4) listens here; Phase 3b tests subscribe directly.
    pub proposal_tx: Option<mpsc::UnboundedSender<Vec<pb::Endpoint>>>,

    /// Currently-running edge-failure-detector tasks. Cancelled on
    /// view-change (Phase 3c) or shutdown.
    pub failure_detector_tasks: Vec<JoinHandle<()>>,
    /// Factory used to spawn FD tasks. Phase 5 supplies the real one.
    pub fd_factory: Option<Arc<dyn EdgeFailureDetectorFactory>>,
    /// Notifier channel used by FD tasks to inform the actor of edge
    /// failures (`edgeFailureNotification`).
    pub fd_notifier_tx: Option<mpsc::Sender<crate::monitoring::factory::EdgeFailure>>,

    /// Injected clock. Phase 3b's `AlertBatcher` loop uses this.
    pub clock: Arc<dyn Clock>,

    /// One broadcast sender per [`ClusterEvent`] variant. Subscribers
    /// derive `Receiver`s via `Sender::subscribe`.
    pub subscriptions: HashMap<ClusterEvent, broadcast::Sender<ClusterStatusChange>>,

    /// Fast Paxos instance for the current configuration. Re-built on
    /// every `decideViewChange`. `None` if the configuration ID has
    /// never been bootstrapped (e.g. test-only `ServiceState::new` calls
    /// that don't go through `Cluster::start`).
    pub fast_paxos: Option<FastPaxos>,

    /// Client used to send Phase1b unicasts (classic-Paxos coordinator
    /// reply). Phase 4 sets this from the messaging-client; tests that
    /// only exercise fast-round paths can leave it `None`.
    pub consensus_client: Option<Arc<dyn MessagingClient>>,

    /// Handle to the in-flight classic-Paxos fallback timer. Replaced
    /// (and the prior handle aborted) every time `consensus_dispatch::propose`
    /// schedules a new fallback. Aborted unconditionally when the
    /// `FastPaxos` instance is reinstated after a view change so stale
    /// timers can't fire on the fresh state.
    pub pending_fallback: Option<JoinHandle<()>>,
}

impl ServiceState {
    /// Phase-3a constructor. Caller supplies an already-populated view
    /// (containing at minimum `my_addr`) and a fresh cut detector.
    #[must_use]
    pub fn new(
        my_addr: pb::Endpoint,
        view: MembershipView,
        cut_detector: MultiNodeCutDetector,
        metadata: MetadataManager,
        settings: Settings,
    ) -> Self {
        let mut subscriptions = HashMap::new();
        for ev in [
            ClusterEvent::ViewChangeProposal,
            ClusterEvent::ViewChange,
            ClusterEvent::ViewChangeOneStepFailed,
            ClusterEvent::Kicked,
        ] {
            let (tx, _rx) = broadcast::channel::<ClusterStatusChange>(EVENT_CHANNEL_CAPACITY);
            subscriptions.insert(ev, tx);
        }
        Self {
            settings,
            my_addr,
            view,
            cut_detector,
            metadata,
            joiners_to_respond_to: HashMap::new(),
            joiner_uuid: HashMap::new(),
            joiner_metadata: HashMap::new(),
            lifecycle: LifecycleState::Running,
            send_queue: VecDeque::new(),
            last_enqueue_at: None,
            broadcaster: None,
            proposal_tx: None,
            failure_detector_tasks: Vec::new(),
            fd_factory: None,
            fd_notifier_tx: None,
            clock: Arc::new(TokioClock),
            subscriptions,
            fast_paxos: None,
            consensus_client: None,
            pending_fallback: None,
        }
    }

    /// Attach a messaging client used for consensus unicasts.
    #[must_use]
    pub fn with_consensus_client(mut self, client: Arc<dyn MessagingClient>) -> Self {
        self.consensus_client = Some(client);
        self
    }

    /// Snapshot of the per-event-type senders for `MembershipService`
    /// handle plumbing.
    #[must_use]
    pub fn subscription_senders(
        &self,
    ) -> HashMap<ClusterEvent, broadcast::Sender<ClusterStatusChange>> {
        self.subscriptions.clone()
    }

    /// Replace the clock (Phase-3b tests pause time via `MockClock`).
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Attach a broadcaster — required before alerts can be sent.
    #[must_use]
    pub fn with_broadcaster(mut self, broadcaster: Arc<dyn Broadcaster>) -> Self {
        self.broadcaster = Some(broadcaster);
        self
    }

    /// Subscribe a proposal-emitted channel.
    #[must_use]
    pub fn with_proposal_sink(mut self, tx: mpsc::UnboundedSender<Vec<pb::Endpoint>>) -> Self {
        self.proposal_tx = Some(tx);
        self
    }

    /// Attach the FD factory + notifier channel.
    #[must_use]
    pub fn with_fd_factory(
        mut self,
        factory: Arc<dyn EdgeFailureDetectorFactory>,
        notifier_tx: mpsc::Sender<crate::monitoring::factory::EdgeFailure>,
    ) -> Self {
        self.fd_factory = Some(factory);
        self.fd_notifier_tx = Some(notifier_tx);
        self
    }

    /// Snapshot of ring 0 — Java `MembershipService.getMembershipView`.
    #[must_use]
    pub fn memberlist(&self) -> Vec<pb::Endpoint> {
        self.view.get_ring(0).unwrap_or_default()
    }

    /// Java `MembershipService.getMembershipSize`.
    #[must_use]
    pub fn membership_size(&self) -> usize {
        self.view.membership_size()
    }

    /// Java `MembershipService.getMetadata`.
    #[must_use]
    pub fn metadata_snapshot(&self) -> Vec<(pb::Endpoint, pb::Metadata)> {
        self.metadata.all()
    }

    /// `MembershipView::current_configuration_id`.
    pub fn configuration_id(&mut self) -> crate::types::ConfigurationId {
        self.view.current_configuration_id()
    }
}
