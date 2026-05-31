//! Actor mailbox payloads.
//!
//! Every external interaction with the membership-service actor is a
//! `ServiceCommand` carrying a oneshot reply channel (or a fire-and-forget
//! signal). Internal handlers consume these.

use tokio::sync::oneshot;

use crate::error::Result;
use crate::pb;

/// One entry in the actor's mailbox.
pub enum ServiceCommand {
    /// gRPC / in-process request dispatched from the inbound server.
    Request {
        /// Wire payload.
        request: pb::RapidRequest,
        /// Where the response is delivered.
        reply: oneshot::Sender<Result<pb::RapidResponse>>,
    },

    /// `Cluster::memberlist()` query.
    Memberlist {
        /// Ring-0 endpoints.
        reply: oneshot::Sender<Vec<pb::Endpoint>>,
    },

    /// `Cluster::membership_size()` query.
    MembershipSize {
        /// Number of endpoints currently in the view.
        reply: oneshot::Sender<usize>,
    },

    /// `Cluster::metadata()` query.
    Metadata {
        /// Snapshot of `(endpoint, metadata)` pairs.
        reply: oneshot::Sender<Vec<(pb::Endpoint, pb::Metadata)>>,
    },

    /// `Cluster::configuration_id()` query.
    ConfigurationId {
        /// Current configuration ID.
        reply: oneshot::Sender<crate::types::ConfigurationId>,
    },

    /// Cooperative shutdown signal.
    Shutdown,

    /// Periodic `AlertBatcher` tick (fired by `alert_batcher::spawn_batcher_loop`).
    TickAlertBatcher,

    /// FD-task failure notification (fired by `EdgeFailureNotifier`).
    EdgeFailure(crate::monitoring::factory::EdgeFailure),

    /// Test-only: rebuild the FD task set for the current view.
    RebuildFailureDetectors {
        /// Acknowledgement once the rebuild has applied.
        reply: oneshot::Sender<usize>,
    },

    /// Test-only: snapshot of the outgoing alert queue size.
    AlertQueueLen {
        /// Current length.
        reply: oneshot::Sender<usize>,
    },

    /// Apply a proposal — Java `decideViewChange`. Tests inject directly;
    /// Phase 4 wires this to the `FastPaxos` decision channel.
    ApplyProposal {
        /// Endpoints to add (joiners) or remove (current members).
        proposal: Vec<pb::Endpoint>,
        /// Acknowledgement once applied.
        reply: oneshot::Sender<()>,
    },

    /// Scheduled classic-Paxos fallback trigger. Java's
    /// `scheduledClassicRoundTask` posts the equivalent.
    StartClassicRound,

    /// Java parity: `MembershipService` constructor fires a synthetic
    /// `VIEW_CHANGE` callback with every current member marked `UP`,
    /// so subscribers attached *before* the actor runs observe the
    /// bootstrap state. The actor processes this as its very first
    /// queued command, by which point F9 pre-subscription forwarders
    /// have been wired in by `ClusterBuilder::finish_cluster`.
    PublishInitialView {
        /// Acknowledgement once the broadcast has been issued.
        reply: oneshot::Sender<()>,
    },
}
