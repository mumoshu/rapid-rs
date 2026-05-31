//! Cluster-event types subscribers receive via
//! `Cluster::subscribe_*`.
//!
//! Java port of `ClusterEvents.java`, `NodeStatusChange.java`, and
//! `ClusterStatusChange.java`.

use crate::pb;
use crate::types::ConfigurationId;

/// What kind of event the subscriber wants. Mirrors Java
/// `ClusterEvents`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClusterEvent {
    /// A view-change proposal was announced by the cut detector.
    ViewChangeProposal,
    /// A Fast-Paxos quorum decided a view change and it has been applied.
    ViewChange,
    /// The fast round failed to converge; a classic Paxos fallback ran.
    ViewChangeOneStepFailed,
    /// Local node was removed from the new configuration.
    Kicked,
}

/// Per-node delta inside a cluster-status change.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeStatusChange {
    /// The endpoint being added or removed.
    pub endpoint: pb::Endpoint,
    /// `UP` for joiners, `DOWN` for removed nodes.
    pub status: pb::EdgeStatus,
    /// Application metadata associated with the endpoint at the time of
    /// the event.
    pub metadata: pb::Metadata,
}

/// One cluster-status change as observed by subscribers.
#[derive(Debug, Clone)]
pub struct ClusterStatusChange {
    /// `ConfigurationId` of the configuration this event represents.
    pub configuration_id: ConfigurationId,
    /// Ring-0 membership snapshot.
    pub membership: Vec<pb::Endpoint>,
    /// Per-node delta. Empty for the initial view-change-after-bootstrap.
    pub delta: Vec<NodeStatusChange>,
}
