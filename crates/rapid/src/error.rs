//! Crate-wide error type.
//!
//! `rapid` exposes a single `Error` enum (Rules: §Errors). The variants
//! cover transport failure, protocol-level rejections, decoding mishaps, and
//! local invariant violations. New variants are added rather than re-using
//! `Other` — the discriminant is part of the contract surface.

use thiserror::Error;

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// All errors surfaced by the `rapid` crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Generic transport failure (gRPC, channel, I/O).
    #[error("transport error: {0}")]
    Transport(String),

    /// A peer rejected our message at the protocol level.
    #[error("protocol rejection: {0}")]
    ProtocolRejected(String),

    /// Wire decoding failed (truncated message, invalid `oneof`, etc.).
    #[error("decode error: {0}")]
    Decode(String),

    /// A local invariant did not hold. Programmer error.
    #[error("internal invariant violated: {0}")]
    Internal(String),

    /// Operation was attempted on a cluster handle that has been shut down.
    #[error("cluster shut down")]
    Shutdown,

    /// `MembershipService` is busy bootstrapping and cannot serve the request.
    #[error("cluster still bootstrapping")]
    Bootstrapping,
}

impl From<prost::DecodeError> for Error {
    fn from(value: prost::DecodeError) -> Self {
        Self::Decode(value.to_string())
    }
}

impl From<prost::EncodeError> for Error {
    fn from(value: prost::EncodeError) -> Self {
        Self::Decode(value.to_string())
    }
}

impl From<tonic::Status> for Error {
    fn from(value: tonic::Status) -> Self {
        Self::Transport(format!("{}: {}", value.code(), value.message()))
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(value: tonic::transport::Error) -> Self {
        Self::Transport(value.to_string())
    }
}
