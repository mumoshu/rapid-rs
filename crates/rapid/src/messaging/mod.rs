//! Messaging traits and transport implementations.
//!
//! Three layers:
//! - [`MessagingClient`]: unicast send-and-receive.
//! - [`MessagingServer`]: inbound dispatch.
//! - [`Broadcaster`]: fan-out to a set of endpoints.
//!
//! Phase 0 ships the trait shapes, an in-process channel transport
//! scaffold, and a dummy gRPC server that only knows how to answer
//! `ProbeMessage`. Real implementations grow in Phase 2.

pub mod broadcaster;
pub mod fault_injection;
pub mod grpc;
pub mod handler;
pub mod in_process;
pub mod lazy_handler;
pub mod timed_client;
pub mod traits;

pub use broadcaster::UnicastToAllBroadcaster;
pub use fault_injection::{
    as_kind, Chain, Disposition, DropAtDests, EnvelopeFilter, FirstN, MessageKind,
};
pub use grpc::{GrpcClient, GrpcPool, GrpcServerHandle};
pub use handler::{ProbeOnlyHandler, RequestHandler};
pub use in_process::{InProcessClient, InProcessNetwork, InProcessServerHandle};
pub use lazy_handler::LazyServiceHandler;
pub use timed_client::{MessageTimeouts, TimedClient};
pub use traits::{Broadcaster, MessagingClient, MessagingServer};
