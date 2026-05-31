//! `Cluster` + `ClusterBuilder` — Java `Cluster.Builder` analogue.
//!
//! Constructs and owns the `MembershipService` actor + messaging server +
//! messaging client. `ClusterBuilder::start()` boots a seed; `::join()`
//! drives the two-phase bootstrap protocol (Phase 4 deliverable —
//! Phase 3a implements the wiring and the seed path only).
//!
//! The implementation is split across three submodules:
//! - `builder`: the [`ClusterBuilder`] configuration surface +
//!   [`TransportChoice`].
//! - `bootstrap`: `start`/`join` and the messaging-stack assembly.
//! - `handle`: the running [`Cluster`] handle + [`leave_gracefully`].

mod bootstrap;
mod builder;
mod handle;

pub use builder::{ClusterBuilder, ServerFactory, TransportChoice};
pub use handle::{leave_gracefully, Cluster};
