//! `rapid` — Rust port of the Rapid distributed membership service
//! (Suresh et al., USENIX ATC '18).
//!
//! This crate's public surface is intentionally narrow during early phases.
//! Phase 0 ships the proto bindings, the [`Clock`] trait, the crate-wide
//! [`Error`], the proto-message trait stubs, and the in-process transport
//! scaffold. Subsequent phases fill in views, cut detection, the membership
//! service actor, consensus, and a default failure detector.
//!
//! # Compatibility
//!
//! `rapid.proto` is a verbatim copy of the upstream Java sources at
//! `references/rapid-java/rapid/src/main/proto/rapid.proto`. Wire-format
//! parity with the Java implementation is a hard requirement: gRPC interop
//! is the default transport.

#![deny(warnings, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod clock;
pub mod cluster;
pub mod cluster_join;
pub mod consensus;
pub mod cut_detector;
pub mod error;
pub mod events;
pub mod messaging;
pub mod metadata;
pub mod monitoring;
pub mod pb;
pub mod proto_traits;
pub(crate) mod ring;
pub mod service;
pub mod settings;
pub mod types;
pub mod view;
pub mod view_hash;

pub use clock::{Clock, MockClock, TokioClock};
pub use error::{Error, Result};
pub use settings::Settings;
pub use types::{ConfigurationId, NodeIndex, RingNumber, Round};
