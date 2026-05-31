//! Java parity ports for the core scenarios of `PaxosTests.java`.
//!
//! Refactored to use the shared `DirectPaxosBus` harness in
//! `tests/common/`. F12 ports the remaining 40 parametric rows on
//! top of the same harness.
//!
//! Ported methods (Java → Rust):
//!  - `testRecoveryForSinglePropose`           → `recovery_for_single_propose`
//!  - `testRecoveryFromFastRoundWithDifferentProposals` →
//!    `recovery_from_fast_round_with_different_proposals`
//!  - `testClassicRoundAfterSuccessfulFastRound` →
//!    `classic_round_after_successful_fast_round`

mod common;

use common::direct_paxos_bus::{ep, DirectPaxosBus, MsgKind};

#[test]
fn recovery_for_single_propose() {
    for &n in &[5usize, 6, 10, 11, 20] {
        let mut h = DirectPaxosBus::new(n, 7);
        let proposal = vec![ep("172.14.12.3", 1234)];
        h.propose(0, proposal.clone());
        h.start_classic_round(0);
        assert!(h.all_decided_same(&proposal), "N={n}: all should decide");
    }
}

#[test]
fn recovery_from_fast_round_with_different_proposals() {
    for &n in &[5usize, 6, 10, 11, 20] {
        let mut h = DirectPaxosBus::new(n, 7);
        let singletons: Vec<Vec<rapid::pb::Endpoint>> =
            (0..n).map(|i| vec![h.endpoint(i).clone()]).collect();
        for (i, singleton) in singletons.iter().enumerate().take(n) {
            h.propose(i, singleton.clone());
        }
        for i in 0..n {
            assert!(
                h.decision(i).is_none(),
                "N={n}: no decision expected before fallback",
            );
        }
        for i in 0..n {
            h.start_classic_round(i);
        }
        let first = h.decision(0).expect("instance 0 decides").clone();
        for i in 1..n {
            assert_eq!(h.decision(i), Some(&first), "N={n}: decisions must agree");
        }
        assert!(
            singletons.iter().any(|s| s == &first),
            "N={n}: decision must be one of the proposed singletons"
        );
    }
}

#[test]
fn classic_round_after_successful_fast_round() {
    for &n in &[5usize, 6, 10, 11, 20] {
        let mut h = DirectPaxosBus::new(n, 7);
        h.drop_kind(MsgKind::FastRoundPhase2b);
        let proposal = vec![ep("127.0.0.1", 1234)];
        for i in 0..n {
            h.propose(i, proposal.clone());
        }
        for i in 0..n {
            assert!(
                h.decision(i).is_none(),
                "N={n}: fast round dropped → no decision yet",
            );
        }
        for i in 0..n {
            h.start_classic_round(i);
        }
        assert!(
            h.all_decided_same(&proposal),
            "N={n}: classic round must converge on the original proposal",
        );
    }
}
