//! Consensus layer — single-decree Paxos + leaderless Fast Paxos.
//!
//! See `paxos.rs` for the classic single-decree port and `fast_paxos.rs`
//! for the fast-round + classic-fallback wrapper. The membership service
//! actor owns a `FastPaxos` instance per configuration; on each
//! view-change `decideViewChange` constructs a fresh one.

pub mod fast_paxos;
pub mod paxos;

pub use fast_paxos::FastPaxos;
pub use paxos::Paxos;
