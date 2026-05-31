//! `LazyServiceHandler` — bridges the gap between server startup and the
//! `MembershipService` actor being ready.
//!
//! Java parity: `GrpcServer` special-cases the
//! `MembershipService == null` window by returning `ProbeResponse {
//! BOOTSTRAPPING }` to inbound probes. The Rust port mirrors this with a
//! handler that holds `Arc<RwLock<Option<MembershipService>>>` —
//! `BOOTSTRAPPING` until the service is installed, then dispatches.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::error::Result;
use crate::messaging::handler::RequestHandler;
use crate::pb;
use crate::proto_traits;
use crate::service::handle::MembershipService;

/// Handler that defers requests to a [`MembershipService`] once one is
/// installed. Before installation, `ProbeMessage` receives a
/// `BOOTSTRAPPING` response and every other message gets the default
/// empty `RapidResponse`.
#[derive(Clone)]
pub struct LazyServiceHandler {
    inner: Arc<RwLock<Option<MembershipService>>>,
}

impl LazyServiceHandler {
    /// Empty handler — pre-install state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(None)),
        }
    }

    /// Install the real service. After this returns, every inbound
    /// request is dispatched to `service.dispatch`.
    pub fn install(&self, service: MembershipService) {
        *self.inner.write() = Some(service);
    }
}

impl Default for LazyServiceHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RequestHandler for LazyServiceHandler {
    async fn handle(&self, req: pb::RapidRequest) -> Result<pb::RapidResponse> {
        let service = self.inner.read().clone();
        if let Some(service) = service {
            return service.dispatch(req).await;
        }
        // Pre-install: BOOTSTRAPPING for probes, default for everything else.
        match req.content {
            Some(pb::rapid_request::Content::ProbeMessage(_)) => {
                Ok(proto_traits::probe_response(pb::ProbeResponse {
                    status: pb::NodeStatus::Bootstrapping as i32,
                }))
            }
            _ => Ok(pb::RapidResponse::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pre_install_probe_returns_bootstrapping() {
        let h = LazyServiceHandler::new();
        let req = proto_traits::probe_request(pb::ProbeMessage::default());
        let resp = h.handle(req).await.unwrap();
        let Some(pb::rapid_response::Content::ProbeResponse(p)) = resp.content else {
            panic!("expected ProbeResponse");
        };
        assert_eq!(p.status(), pb::NodeStatus::Bootstrapping);
    }

    #[tokio::test]
    async fn pre_install_other_message_returns_default() {
        let h = LazyServiceHandler::new();
        let req = pb::RapidRequest {
            content: Some(pb::rapid_request::Content::LeaveMessage(
                pb::LeaveMessage::default(),
            )),
        };
        let resp = h.handle(req).await.unwrap();
        assert!(resp.content.is_none());
    }
}
