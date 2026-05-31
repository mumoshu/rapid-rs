//! Public `MembershipService` handle — a thin wrapper around the actor's
//! `mpsc::Sender<ServiceCommand>`.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc, oneshot};

use crate::error::{Error, Result};
use crate::events::{ClusterEvent, ClusterStatusChange};
use crate::pb;
use crate::service::command::ServiceCommand;
use crate::service::state::ServiceState;
use crate::service::task;
use crate::types::ConfigurationId;

/// Public-facing handle.
#[derive(Clone)]
pub struct MembershipService {
    tx: mpsc::Sender<ServiceCommand>,
    join_handle: Arc<tokio::task::JoinHandle<()>>,
    subscriptions: HashMap<ClusterEvent, broadcast::Sender<ClusterStatusChange>>,
}

impl MembershipService {
    /// Spawn the actor and return a handle.
    #[must_use]
    pub fn spawn(state: ServiceState) -> Self {
        let subscriptions = state.subscription_senders();
        let (tx, rx) = mpsc::channel::<ServiceCommand>(1024);
        let tx_clone = tx.clone();
        let join_handle = Arc::new(tokio::spawn(task::run(state, rx, tx_clone)));
        Self {
            tx,
            join_handle,
            subscriptions,
        }
    }

    /// Subscribe to a `ClusterEvent` stream. Slow consumers receive
    /// `RecvError::Lagged` when the broadcast buffer (capacity 64) is
    /// outpaced.
    ///
    /// # Panics
    ///
    /// Panics if the invariant — `ServiceState::subscription_senders`
    /// populates every `ClusterEvent` variant — was violated.
    #[must_use]
    pub fn subscribe(&self, event: ClusterEvent) -> broadcast::Receiver<ClusterStatusChange> {
        self.subscriptions
            .get(&event)
            .expect("invariant: subscription_senders populates every variant")
            .subscribe()
    }

    /// Inject a proposal directly (test path; Phase 4 binds `FastPaxos`
    /// to this).
    ///
    /// # Errors
    /// Returns [`Error::Shutdown`] if the actor has exited.
    pub async fn apply_proposal(&self, proposal: Vec<pb::Endpoint>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ServiceCommand::ApplyProposal {
                proposal,
                reply: tx,
            })
            .await
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)
    }

    /// Forward an inbound `RapidRequest` (called by the messaging-server
    /// handler).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Shutdown`] if the actor has already exited.
    pub async fn dispatch(&self, request: pb::RapidRequest) -> Result<pb::RapidResponse> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ServiceCommand::Request { request, reply: tx })
            .await
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)?
    }

    /// Java `Cluster::getMemberlist()`.
    ///
    /// # Errors
    /// Returns [`Error::Shutdown`] if the actor has exited.
    pub async fn memberlist(&self) -> Result<Vec<pb::Endpoint>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ServiceCommand::Memberlist { reply: tx })
            .await
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)
    }

    /// Java `Cluster::getMembershipSize()`.
    ///
    /// # Errors
    /// Returns [`Error::Shutdown`] if the actor has exited.
    pub async fn membership_size(&self) -> Result<usize> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ServiceCommand::MembershipSize { reply: tx })
            .await
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)
    }

    /// Java `Cluster::getClusterMetadata()`.
    ///
    /// # Errors
    /// Returns [`Error::Shutdown`] if the actor has exited.
    pub async fn metadata(&self) -> Result<Vec<(pb::Endpoint, pb::Metadata)>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ServiceCommand::Metadata { reply: tx })
            .await
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)
    }

    /// Convenience for tests / replay drivers — the current configuration
    /// ID. Not exposed by Java's `Cluster` directly (`MembershipService`
    /// exposes it).
    ///
    /// # Errors
    /// Returns [`Error::Shutdown`] if the actor has exited.
    pub async fn configuration_id(&self) -> Result<ConfigurationId> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ServiceCommand::ConfigurationId { reply: tx })
            .await
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)
    }

    /// Cooperative shutdown. Returns once the actor task has drained.
    pub async fn shutdown(self) {
        let _ = self.tx.send(ServiceCommand::Shutdown).await;
        if let Ok(handle) = Arc::try_unwrap(self.join_handle) {
            let _ = handle.await;
        }
    }

    /// Borrow the underlying mailbox sender (for spawning background
    /// tasks that need to post to the actor).
    #[must_use]
    pub fn sender(&self) -> tokio::sync::mpsc::Sender<ServiceCommand> {
        self.tx.clone()
    }

    /// Test/replay helper: rebuild the FD task set and return the new
    /// count.
    ///
    /// # Errors
    /// Returns [`Error::Shutdown`] if the actor has exited.
    pub async fn rebuild_failure_detectors(&self) -> Result<usize> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ServiceCommand::RebuildFailureDetectors { reply: tx })
            .await
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)
    }

    /// Test helper: current `send_queue` length.
    ///
    /// # Errors
    /// Returns [`Error::Shutdown`] if the actor has exited.
    pub async fn alert_queue_len(&self) -> Result<usize> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ServiceCommand::AlertQueueLen { reply: tx })
            .await
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)
    }
}
