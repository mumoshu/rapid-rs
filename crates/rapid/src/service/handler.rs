//! `RequestHandler` impl that forwards inbound transport requests to a
//! [`MembershipService`] mailbox.
//!
//! Java parity: `GrpcServer.sendRequest` calls
//! `membershipService.handleMessage(request)`. The Rust port wraps the
//! handle in a small struct that implements
//! [`crate::messaging::handler::RequestHandler`].

use async_trait::async_trait;

use crate::error::Result;
use crate::messaging::handler::RequestHandler;
use crate::pb;
use crate::service::handle::MembershipService;

/// Adapter from `MembershipService` to `RequestHandler`.
#[derive(Clone)]
pub struct ServiceRequestHandler {
    service: MembershipService,
}

impl ServiceRequestHandler {
    /// Wrap a `MembershipService` handle.
    #[must_use]
    pub fn new(service: MembershipService) -> Self {
        Self { service }
    }
}

#[async_trait]
impl RequestHandler for ServiceRequestHandler {
    async fn handle(&self, req: pb::RapidRequest) -> Result<pb::RapidResponse> {
        self.service.dispatch(req).await
    }
}
