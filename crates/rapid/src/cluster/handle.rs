//! `Cluster` — the running handle returned by `ClusterBuilder::start`/`join`.
//!
//! Owns the `MembershipService` actor + messaging server + messaging
//! client, and hosts the best-effort `leave_gracefully` fan-out. The
//! construction/bootstrap logic lives in [`super::bootstrap`].

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use crate::clock::Clock;
use crate::error::Result;
use crate::events::{ClusterEvent, ClusterStatusChange};
use crate::messaging::traits::{MessagingClient, MessagingServer};
use crate::pb;
use crate::service::MembershipService;
use crate::settings::Settings;

use super::bootstrap::endpoints_to_socket_addrs;

/// Running cluster handle.
pub struct Cluster {
    pub(super) service: MembershipService,
    pub(super) server: Box<dyn MessagingServer>,
    pub(super) listen_addr: SocketAddr,
    pub(super) listen_endpoint: pb::Endpoint,
    pub(super) client: Arc<dyn MessagingClient>,
    pub(super) settings: Settings,
    pub(super) clock: Arc<dyn Clock>,
}

impl Cluster {
    /// The endpoint we advertise to peers.
    #[must_use]
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// The proto `Endpoint` for `listen_addr`.
    #[must_use]
    pub fn listen_endpoint(&self) -> &pb::Endpoint {
        &self.listen_endpoint
    }

    /// Java `Cluster::getMemberlist()`.
    ///
    /// # Errors
    /// See [`MembershipService::memberlist`].
    pub async fn memberlist(&self) -> Result<Vec<pb::Endpoint>> {
        self.service.memberlist().await
    }

    /// Java `Cluster::getMembershipSize()`.
    ///
    /// # Errors
    /// See [`MembershipService::membership_size`].
    pub async fn membership_size(&self) -> Result<usize> {
        self.service.membership_size().await
    }

    /// Current configuration ID (test/replay accessor).
    ///
    /// # Errors
    /// See [`MembershipService::configuration_id`].
    pub async fn configuration_id(&self) -> Result<crate::types::ConfigurationId> {
        self.service.configuration_id().await
    }

    /// Borrow the underlying actor handle. Used by integration tests that
    /// want to dispatch a hand-crafted `RapidRequest` and observe the
    /// reply without going through the messaging-server.
    #[must_use]
    pub fn service(&self) -> &MembershipService {
        &self.service
    }

    /// Subscribe to a `ClusterEvent` stream — Java
    /// `Cluster::registerSubscription`.
    #[must_use]
    pub fn subscribe(&self, event: ClusterEvent) -> broadcast::Receiver<ClusterStatusChange> {
        self.service.subscribe(event)
    }

    /// Java parity for the initial `VIEW_CHANGE` callback Java fires
    /// in `MembershipService` constructor: returns a `ClusterStatusChange`
    /// describing the current view as if every member just joined. Use
    /// alongside `subscribe(ClusterEvent::ViewChange)` to receive
    /// initial-state + live updates without race conditions.
    ///
    /// # Errors
    /// Bubble up [`crate::error::Error::Shutdown`] from the underlying
    /// service if the actor has exited.
    pub async fn initial_view_event(&self) -> Result<ClusterStatusChange> {
        let configuration_id = self.service.configuration_id().await?;
        let membership = self.service.memberlist().await?;
        let metadata: std::collections::HashMap<_, _> = self
            .service
            .metadata()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| ((k.hostname.clone(), k.port), v))
            .collect();
        let delta = membership
            .iter()
            .map(|e| crate::events::NodeStatusChange {
                endpoint: e.clone(),
                status: pb::EdgeStatus::Up,
                metadata: metadata
                    .get(&(e.hostname.clone(), e.port))
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect();
        Ok(ClusterStatusChange {
            configuration_id,
            membership,
            delta,
        })
    }

    /// Java `Cluster::shutdown()`.
    pub async fn shutdown(self) {
        tracing::info!(target: "rapid", listen = %self.listen_addr, "cluster.shutdown");
        self.server.shutdown().await;
        self.service.shutdown().await;
    }

    /// Java `Cluster::leaveGracefully()` — inform observers then shut down.
    pub async fn leave_gracefully(self) {
        let memberlist = self.service.memberlist().await.unwrap_or_default();
        let observers: Vec<SocketAddr> = endpoints_to_socket_addrs(&memberlist)
            .into_iter()
            .filter(|a| *a != self.listen_addr)
            .collect();
        let _ = leave_gracefully(
            self.listen_endpoint.clone(),
            observers,
            &self.settings,
            self.client.clone(),
            self.clock.as_ref(),
        )
        .await;
        self.shutdown().await;
    }
}

/// Java `MembershipService.leave` — best-effort fan-out of a
/// `LeaveMessage` to every current observer, with a timeout governed by
/// `Settings.leave_message_timeout`.
///
/// # Errors
///
/// Currently infallible — best-effort failures are swallowed.
pub async fn leave_gracefully(
    self_endpoint: pb::Endpoint,
    observers: Vec<SocketAddr>,
    settings: &Settings,
    client: Arc<dyn MessagingClient>,
    clock: &dyn Clock,
) -> Result<()> {
    tracing::info!(target: "rapid", observers = observers.len(), "leave.sent");
    let leave = pb::RapidRequest {
        content: Some(pb::rapid_request::Content::LeaveMessage(pb::LeaveMessage {
            sender: Some(self_endpoint),
        })),
    };
    let mut handles = Vec::with_capacity(observers.len());
    for observer in observers {
        let req = leave.clone();
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            let _ = client.send_best_effort(observer, req).await;
        }));
    }
    let deadline = clock.now() + settings.leave_message_timeout;
    let join_all = async move {
        for h in handles {
            let _ = h.await;
        }
    };
    tokio::select! {
        () = join_all => {}
        () = clock.sleep_until(deadline) => {}
    }
    Ok(())
}

/// `Duration` type sink — anchors the unused-import lint when the
/// `leave_gracefully` body changes shape during refactors.
#[allow(dead_code)]
const _LEAVE_TIMEOUT_TYPE: Duration = Duration::from_secs(0);
