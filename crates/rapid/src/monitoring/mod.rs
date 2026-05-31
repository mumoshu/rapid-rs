//! Edge-failure-detection traits.
//!
//! Phase 3b ships the trait shapes and a no-op implementation used by
//! tests. Phase 5 wires the production `PingPongFailureDetector`.

pub mod factory;
pub mod no_op;
pub mod ping_pong;
pub mod static_fd;

pub use factory::{EdgeFailureDetector, EdgeFailureDetectorFactory, EdgeFailureNotifier};
pub use no_op::NoOpFactory;
pub use ping_pong::{PingPongDetector, PingPongFactory};
pub use static_fd::{Blacklist, StaticFailureDetectorFactory};
