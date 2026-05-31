//! `MembershipService` actor — single-owner protocol orchestrator.

pub mod alert_batcher;
pub mod alert_handler;
pub mod command;
pub mod consensus_dispatch;
pub mod fd_scheduler;
pub mod handle;
pub mod handler;
pub mod handlers;
pub mod state;
pub mod task;
pub mod view_change;

pub use command::ServiceCommand;
pub use handle::MembershipService;
pub use handler::ServiceRequestHandler;
pub use state::ServiceState;
