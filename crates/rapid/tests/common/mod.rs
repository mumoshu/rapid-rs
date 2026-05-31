//! Shared test harnesses (F7).
//!
//! Cargo treats every `.rs` file under `tests/` as its own integration
//! test binary unless it's inside a subdirectory's `mod.rs`. Putting
//! the helpers here lets each integration test do `mod common;` to
//! pull them in without producing an empty test binary.

#![allow(dead_code)] // Different integration tests use different subsets.

pub mod cluster_harness;
pub mod direct_paxos_bus;
