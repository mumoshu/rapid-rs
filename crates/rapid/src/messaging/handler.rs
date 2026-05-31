//! Request-handler abstraction used by both transports.
//!
//! Phase 0 ships a single concrete handler — [`ProbeOnlyHandler`] — that
//! answers `ProbeMessage` with `ProbeResponse { OK }` and rejects every
//! other variant. The membership service actor in later phases will be a
//! [`RequestHandler`] impl over a `tokio::sync::mpsc` mailbox.

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::{Error, Result};
use crate::pb;
use crate::proto_traits;

/// Server-side dispatch. The transport implementation owns the handler and
/// calls `handle` once per inbound request.
#[async_trait]
pub trait RequestHandler: Send + Sync {
    /// Handle a single inbound request.
    async fn handle(&self, req: pb::RapidRequest) -> Result<pb::RapidResponse>;
}

#[async_trait]
impl<T: RequestHandler + ?Sized> RequestHandler for Arc<T> {
    async fn handle(&self, req: pb::RapidRequest) -> Result<pb::RapidResponse> {
        (**self).handle(req).await
    }
}

/// Phase-0 stub: answers `ProbeMessage` with `OK`, otherwise rejects.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProbeOnlyHandler;

#[async_trait]
impl RequestHandler for ProbeOnlyHandler {
    async fn handle(&self, req: pb::RapidRequest) -> Result<pb::RapidResponse> {
        let Some(content) = req.content else {
            return Err(Error::Decode("RapidRequest.content is None".into()));
        };
        match content {
            pb::rapid_request::Content::ProbeMessage(_) => {
                Ok(proto_traits::probe_response(pb::ProbeResponse {
                    status: pb::NodeStatus::Ok as i32,
                }))
            }
            other => Err(Error::ProtocolRejected(format!(
                "ProbeOnlyHandler: unsupported variant: {}",
                discriminant_name(&other)
            ))),
        }
    }
}

fn discriminant_name(c: &pb::rapid_request::Content) -> &'static str {
    use pb::rapid_request::Content as C;
    match c {
        C::PreJoinMessage(_) => "PreJoinMessage",
        C::JoinMessage(_) => "JoinMessage",
        C::BatchedAlertMessage(_) => "BatchedAlertMessage",
        C::ProbeMessage(_) => "ProbeMessage",
        C::FastRoundPhase2bMessage(_) => "FastRoundPhase2bMessage",
        C::Phase1aMessage(_) => "Phase1aMessage",
        C::Phase1bMessage(_) => "Phase1bMessage",
        C::Phase2aMessage(_) => "Phase2aMessage",
        C::Phase2bMessage(_) => "Phase2bMessage",
        C::LeaveMessage(_) => "LeaveMessage",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn probe_handler_returns_ok() {
        let h = ProbeOnlyHandler;
        let req = proto_traits::probe_request(pb::ProbeMessage::default());
        let resp = h.handle(req).await.unwrap();
        let Some(pb::rapid_response::Content::ProbeResponse(p)) = resp.content else {
            panic!("expected ProbeResponse");
        };
        assert_eq!(p.status(), pb::NodeStatus::Ok);
    }

    #[tokio::test]
    async fn probe_handler_rejects_other_variant() {
        let h = ProbeOnlyHandler;
        let req = pb::RapidRequest {
            content: Some(pb::rapid_request::Content::LeaveMessage(
                pb::LeaveMessage::default(),
            )),
        };
        let err = h.handle(req).await.unwrap_err();
        assert!(matches!(err, Error::ProtocolRejected(_)));
    }
}
