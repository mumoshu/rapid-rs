//! Transport-layer traits.
//!
//! The traits are parameterised in `pb::RapidRequest` / `pb::RapidResponse`
//! for now. The proto-message trait layer (`proto_traits`) sits *above* this
//! one — wire transport is a separate concern from algorithmic plumbing.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::pb;

/// Unicast send/receive against a remote endpoint.
///
/// Java parity: `IMessagingClient.sendMessage` (with retries) and
/// `sendMessageBestEffort` (no retries). The Rust port collapses these
/// onto two methods; implementations choose the retry policy.
#[async_trait]
pub trait MessagingClient: Send + Sync {
    /// Send a request with retransmissions. Mirrors Java's
    /// `IMessagingClient.sendMessage`.
    async fn send(&self, remote: SocketAddr, req: pb::RapidRequest) -> Result<pb::RapidResponse>;

    /// Best-effort send (no retries). Mirrors Java's
    /// `IMessagingClient.sendMessageBestEffort`.
    async fn send_best_effort(
        &self,
        remote: SocketAddr,
        req: pb::RapidRequest,
    ) -> Result<pb::RapidResponse>;
}

/// Server-side dispatch surface. Tightly coupled to a
/// [`RequestHandler`](super::handler::RequestHandler).
#[async_trait]
pub trait MessagingServer: Send + Sync {
    /// Local address the server is bound to.
    fn local_addr(&self) -> SocketAddr;

    /// Shut the server down, waking any blocked workers.
    async fn shutdown(&self);
}

/// Fan-out broadcast to a stored recipient list.
///
/// Java parity: `IBroadcaster` keeps the current recipient list as state
/// (mutated by `setMembership`) and `broadcast(msg)` returns a list of
/// per-recipient futures. The Rust port keeps the same shape with an
/// `async fn broadcast` that fires concurrently and yields when every
/// recipient call has been spawned (not when they complete — see
/// `UnicastToAllBroadcaster` below).
#[async_trait]
pub trait Broadcaster: Send + Sync {
    /// Replace the recipient list. Implementations may reorder the input
    /// (Java's `UnicastToAllBroadcaster` shuffles).
    async fn set_membership(&self, recipients: Vec<SocketAddr>);

    /// Send the request to every stored recipient. Slow receivers must
    /// not block siblings.
    async fn broadcast(&self, req: pb::RapidRequest);
}

/// Convenience type alias used by the in-process transport and tests.
pub type SharedClient = Arc<dyn MessagingClient>;
