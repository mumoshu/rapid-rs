//! Re-exports of the prost/tonic-generated protobuf types.
//!
//! The build script compiles `proto/rapid.proto` into a single `remoting`
//! module. This file is the single canonical entry-point — internal callers
//! should import from `crate::pb::…` and never from
//! `remoting::*` directly so the package path stays renamable.

#[allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::derive_partial_eq_without_eq,
    clippy::large_enum_variant,
    clippy::similar_names,
    rust_2018_idioms,
    unreachable_pub,
    warnings,
    missing_docs
)]
mod generated {
    tonic::include_proto!("remoting");
}

pub use generated::*;

/// Path used by `tonic::include_file_descriptor_set!` consumers.
pub const PROTO_PACKAGE: &str = "remoting";
