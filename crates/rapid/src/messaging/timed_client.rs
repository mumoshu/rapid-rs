//! `TimedClient` — per-message-type deadline decorator.
//!
//! Java parity: `GrpcClient.getTimeoutForMessageMs` switches on the
//! `RapidRequest` oneof:
//! - `PROBEMESSAGE` → `Settings.grpcProbeTimeoutMs`  (default 1000ms)
//! - `JOINMESSAGE`  → `Settings.grpcJoinTimeoutMs`   (default 5000ms)
//! - default        → `Settings.grpcTimeoutMs`      (default 1000ms)
//!
//! The Rust decorator wraps any [`MessagingClient`] and applies
//! `tokio::time::timeout` per request.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::error::{Error, Result};
use crate::messaging::traits::MessagingClient;
use crate::pb;

/// Per-message timeout policy.
#[derive(Debug, Clone, Copy)]
pub struct MessageTimeouts {
    /// `ProbeMessage` deadline. Default: 1 s.
    pub probe: Duration,
    /// `JoinMessage` / `PreJoinMessage` deadline. Default: 5 s.
    pub join: Duration,
    /// Fallback deadline for every other message. Default: 1 s.
    pub default: Duration,
}

impl Default for MessageTimeouts {
    fn default() -> Self {
        Self {
            probe: Duration::from_secs(1),
            join: Duration::from_secs(5),
            default: Duration::from_secs(1),
        }
    }
}

impl From<&crate::settings::Settings> for MessageTimeouts {
    fn from(s: &crate::settings::Settings) -> Self {
        Self {
            probe: s.grpc_probe_timeout,
            join: s.grpc_join_timeout,
            default: s.grpc_default_timeout,
        }
    }
}

impl MessageTimeouts {
    /// Resolve the timeout for a specific request shape.
    #[must_use]
    pub fn for_request(&self, req: &pb::RapidRequest) -> Duration {
        match req.content.as_ref() {
            Some(pb::rapid_request::Content::ProbeMessage(_)) => self.probe,
            Some(
                pb::rapid_request::Content::JoinMessage(_)
                | pb::rapid_request::Content::PreJoinMessage(_),
            ) => self.join,
            _ => self.default,
        }
    }
}

/// Wraps an inner [`MessagingClient`] and enforces a per-message-type
/// deadline on every send.
pub struct TimedClient<C: ?Sized> {
    inner: Arc<C>,
    timeouts: MessageTimeouts,
}

impl<C: ?Sized> TimedClient<C> {
    /// Construct.
    pub fn new(inner: Arc<C>, timeouts: MessageTimeouts) -> Self {
        Self { inner, timeouts }
    }
}

#[async_trait]
impl<C> MessagingClient for TimedClient<C>
where
    C: MessagingClient + ?Sized + 'static,
{
    async fn send(&self, remote: SocketAddr, req: pb::RapidRequest) -> Result<pb::RapidResponse> {
        let deadline = self.timeouts.for_request(&req);
        match tokio::time::timeout(deadline, self.inner.send(remote, req)).await {
            Ok(res) => res,
            Err(_) => Err(Error::Transport(format!("timed out after {deadline:?}"))),
        }
    }

    async fn send_best_effort(
        &self,
        remote: SocketAddr,
        req: pb::RapidRequest,
    ) -> Result<pb::RapidResponse> {
        let deadline = self.timeouts.for_request(&req);
        match tokio::time::timeout(deadline, self.inner.send_best_effort(remote, req)).await {
            Ok(res) => res,
            Err(_) => Err(Error::Transport(format!(
                "best-effort timed out after {deadline:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto_traits;

    #[test]
    fn timeouts_resolve_per_variant() {
        let t = MessageTimeouts::default();
        let probe = proto_traits::probe_request(pb::ProbeMessage::default());
        let join = pb::RapidRequest {
            content: Some(pb::rapid_request::Content::JoinMessage(
                pb::JoinMessage::default(),
            )),
        };
        let leave = pb::RapidRequest {
            content: Some(pb::rapid_request::Content::LeaveMessage(
                pb::LeaveMessage::default(),
            )),
        };
        assert_eq!(t.for_request(&probe), Duration::from_secs(1));
        assert_eq!(t.for_request(&join), Duration::from_secs(5));
        assert_eq!(t.for_request(&leave), Duration::from_secs(1));
    }
}
