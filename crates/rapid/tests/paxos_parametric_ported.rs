//! F12 — full parametric ports of `PaxosTests.java`.
//!
//! Mapping:
//!  - `testClassicRoundAfterSuccessfulFastRoundMixedValues` (4 rows)
//!  - `coordinatorRuleTests` (19 rows)
//!  - `coordinatorRuleTestsSameRank` (17 rows)
//!
//! Total: 40 rows. Each row asserts independently; failures
//! report which row.

mod common;

use std::collections::HashSet;
use std::time::Duration;

use rand::seq::SliceRandom;
use rand::SeedableRng;

use rapid::consensus::fast_paxos::FastOutgoing;
use rapid::consensus::{FastPaxos, Paxos};
use rapid::pb;

use common::direct_paxos_bus::{ep, DirectPaxosBus, MsgKind};

fn host_at(port: i32) -> pb::Endpoint {
    ep("127.0.0.1", port)
}

fn rnk(round: i32, node_index: i32) -> pb::Rank {
    pb::Rank { round, node_index }
}

// ====================================================================
// testClassicRoundAfterSuccessfulFastRoundMixedValues — 8 rows.
// (Note: Java's data table has 8 entries — re-counted at the top of
// `coordinatorRuleTestsSameRank`; the original audit said 4. Both
// counts hit the same scenario family.)
//
// Format: (N, p1_endpoints, p2_endpoints, p2votes, decisionChoices).
// `p1_endpoints` gets N - p2votes votes; `p2_endpoints` gets p2votes.
// The fast round is dropped. Classic Paxos must converge on a value
// in `decisionChoices`.
// ====================================================================

#[test]
#[allow(clippy::too_many_lines)]
fn classic_round_after_successful_fast_round_mixed_values() {
    struct Case<'a> {
        n: usize,
        p1: &'a [pb::Endpoint],
        p2: &'a [pb::Endpoint],
        p2_votes: usize,
        valid: Vec<Vec<pb::Endpoint>>,
    }

    let p1 = vec![host_at(5891), host_at(5821)];
    let p2 = vec![host_at(5821), host_at(5872)];
    let p1_or_p2: Vec<Vec<pb::Endpoint>> = vec![p1.clone(), p2.clone()];

    let cases: Vec<Case> = vec![
        Case {
            n: 6,
            p1: &p1,
            p2: &p2,
            p2_votes: 5,
            valid: vec![p2.clone()],
        },
        Case {
            n: 6,
            p1: &p1,
            p2: &p2,
            p2_votes: 1,
            valid: vec![p1.clone()],
        },
        Case {
            n: 6,
            p1: &p1,
            p2: &p2,
            p2_votes: 4,
            valid: p1_or_p2.clone(),
        },
        Case {
            n: 6,
            p1: &p1,
            p2: &p2,
            p2_votes: 2,
            valid: p1_or_p2.clone(),
        },
        Case {
            n: 5,
            p1: &p1,
            p2: &p2,
            p2_votes: 4,
            valid: vec![p2.clone()],
        },
        Case {
            n: 5,
            p1: &p1,
            p2: &p2,
            p2_votes: 1,
            valid: vec![p1.clone()],
        },
        Case {
            n: 10,
            p1: &p1,
            p2: &p2,
            p2_votes: 4,
            valid: p1_or_p2.clone(),
        },
        Case {
            n: 10,
            p1: &p1,
            p2: &p2,
            p2_votes: 1,
            valid: p1_or_p2.clone(),
        },
    ];

    for (idx, c) in cases.iter().enumerate() {
        let mut bus = DirectPaxosBus::new(c.n, 42);
        bus.drop_kind(MsgKind::FastRoundPhase2b);
        for i in 0..c.n {
            let proposal = if i < c.n - c.p2_votes {
                c.p1.to_vec()
            } else {
                c.p2.to_vec()
            };
            bus.propose(i, proposal);
        }
        for i in 0..c.n {
            bus.start_classic_round(i);
        }
        let decided = bus
            .decision(0)
            .expect("instance 0 decides under classic")
            .clone();
        for i in 1..c.n {
            assert_eq!(
                bus.decision(i),
                Some(&decided),
                "row {idx} N={} p2votes={}: all decide same value",
                c.n,
                c.p2_votes
            );
        }
        assert!(
            c.valid.iter().any(|v| v == &decided),
            "row {idx} N={} p2votes={}: decided {decided:?} must be in {:?}",
            c.n,
            c.p2_votes,
            c.valid
        );
    }
}

// ====================================================================
// coordinatorRuleTests — 19 rows. Drives
// `Paxos::select_proposal_from_messages` over a randomised quorum
// of Phase-1b messages (100 iterations per row).
// ====================================================================

#[derive(Clone)]
struct CoordCase {
    n: usize,
    p1n: usize,
    p2n: usize,
    proposals: Vec<Vec<pb::Endpoint>>,
    valid_indices: HashSet<usize>,
}

fn run_coordinator_rule(cases: &[CoordCase], same_rank: bool, label: &str) {
    let noise = vec![host_at(1), host_at(2)];
    // Java runs 100 iterations per row to exercise the shuffle; we
    // do the same. Seeded RNG so failures are reproducible.
    for (idx, c) in cases.iter().enumerate() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(0x0a11_c0de_u64 + idx as u64);
        for iter in 0..100 {
            let mut messages: Vec<pb::Phase1bMessage> = Vec::new();
            // Highest-ranked proposal: vrnd = (round=1, node_index=1).
            let rank_p1 = rnk(1, 1);
            for _ in 0..c.p1n {
                messages.push(pb::Phase1bMessage {
                    configuration_id: 1,
                    rnd: None,
                    sender: None,
                    vrnd: Some(rank_p1),
                    vval: c.proposals[0].clone(),
                });
            }
            // Second proposal: vrnd depends on test variant. For
            // coordinatorRuleTests, second-ranked = (0, MAX); for
            // coordinatorRuleTestsSameRank, same rank as p1.
            let rank_p2 = if same_rank { rank_p1 } else { rnk(0, i32::MAX) };
            for _ in 0..c.p2n {
                messages.push(pb::Phase1bMessage {
                    configuration_id: 1,
                    rnd: None,
                    sender: None,
                    vrnd: Some(rank_p2),
                    vval: c.proposals[1].clone(),
                });
            }
            // Lower ranks — Java's "noiseProposal".
            for i in (c.p1n + c.p2n)..c.n {
                let vrnd = rnk(0, i32::try_from(i).expect("noise index fits i32"));
                messages.push(pb::Phase1bMessage {
                    configuration_id: 1,
                    rnd: None,
                    sender: None,
                    vrnd: Some(vrnd),
                    vval: c.proposals.get(2).cloned().unwrap_or_else(|| noise.clone()),
                });
            }
            // Take a random quorum: shuffle then truncate to (N/2 + 1).
            messages.shuffle(&mut rng);
            messages.truncate(c.n / 2 + 1);

            let chosen = Paxos::select_proposal_from_messages(&messages, c.n);
            let valid: Vec<Vec<pb::Endpoint>> = c
                .valid_indices
                .iter()
                .map(|i| c.proposals[*i].clone())
                .collect();
            assert!(
                valid.iter().any(|v| v == &chosen),
                "{label} row {idx} iter {iter} N={n} p1n={p1n} p2n={p2n}: \
                 chose {chosen:?}, expected one of {valid:?}",
                n = c.n,
                p1n = c.p1n,
                p2n = c.p2n,
            );
        }
    }
}

#[test]
fn coordinator_rule_tests() {
    let p1 = vec![host_at(5891), host_at(5821)];
    let p2 = vec![host_at(5821), host_at(5872)];
    let noise = vec![host_at(1), host_at(2)];
    let canonical = vec![p1.clone(), p2.clone(), noise.clone()];
    let swapped = vec![p2.clone(), p1.clone(), noise.clone()];

    let cases: Vec<CoordCase> = vec![
        // p1N + p2N == N
        coord_case(6, 4, 2, &canonical, &[0]),
        coord_case(6, 5, 1, &canonical, &[0]),
        coord_case(6, 6, 0, &canonical, &[0]),
        coord_case(9, 6, 3, &canonical, &[0, 1]),
        coord_case(9, 7, 2, &canonical, &[0]),
        coord_case(9, 8, 1, &canonical, &[0]),
        coord_case(6, 1, 5, &canonical, &[0, 1]),
        coord_case(6, 2, 4, &canonical, &[0, 1]),
        coord_case(6, 3, 3, &canonical, &[0]),
        coord_case(6, 3, 3, &swapped, &[0]),
        // p1N + p2N < N
        coord_case(6, 4, 1, &canonical, &[0]),
        coord_case(6, 5, 1, &canonical, &[0]),
        coord_case(9, 6, 1, &canonical, &[0, 1, 2]),
        coord_case(9, 7, 1, &canonical, &[0]),
        coord_case(9, 8, 1, &canonical, &[0]),
        coord_case(6, 1, 2, &canonical, &[0, 1, 2]),
        coord_case(6, 2, 1, &canonical, &[0, 1, 2]),
        coord_case(6, 3, 0, &canonical, &[0]),
        coord_case(6, 3, 0, &swapped, &[0]),
    ];
    assert_eq!(cases.len(), 19, "row count matches Java");
    run_coordinator_rule(&cases, false, "coordinatorRuleTests");
}

#[test]
fn coordinator_rule_tests_same_rank() {
    let p1 = vec![host_at(5891), host_at(5821)];
    let p2 = vec![host_at(5821), host_at(5872)];
    let noise = vec![host_at(1), host_at(2)];
    let canonical = vec![p1.clone(), p2.clone(), noise.clone()];
    let swapped = vec![p2.clone(), p1.clone(), noise.clone()];

    let cases: Vec<CoordCase> = vec![
        // p1N + p2N == N
        coord_case(6, 4, 2, &canonical, &[0, 1]),
        coord_case(6, 5, 1, &canonical, &[0]),
        coord_case(6, 6, 0, &canonical, &[0]),
        coord_case(9, 6, 3, &canonical, &[0, 1]),
        coord_case(9, 7, 2, &canonical, &[0]),
        coord_case(9, 8, 1, &canonical, &[0]),
        coord_case(6, 3, 3, &canonical, &[0, 1]),
        coord_case(6, 3, 3, &swapped, &[0, 1]),
        // p1N + p2N < N
        coord_case(6, 4, 1, &canonical, &[0, 1]),
        coord_case(6, 5, 0, &canonical, &[0]),
        coord_case(9, 6, 1, &canonical, &[0, 1, 2]),
        coord_case(9, 7, 1, &canonical, &[0]),
        coord_case(9, 8, 1, &canonical, &[0]),
        coord_case(6, 1, 2, &canonical, &[0, 1, 2]),
        coord_case(6, 2, 1, &canonical, &[0, 1, 2]),
        coord_case(6, 3, 0, &canonical, &[0]),
        coord_case(6, 3, 0, &swapped, &[0]),
    ];
    assert_eq!(cases.len(), 17, "row count matches Java");
    run_coordinator_rule(&cases, true, "coordinatorRuleTestsSameRank");
}

fn coord_case(
    n: usize,
    p1n: usize,
    p2n: usize,
    proposals: &[Vec<pb::Endpoint>],
    valid: &[usize],
) -> CoordCase {
    CoordCase {
        n,
        p1n,
        p2n,
        proposals: proposals.to_vec(),
        valid_indices: valid.iter().copied().collect(),
    }
}

// Suppress dead-code warning for the optional FastPaxos / Duration
// re-exports — used in expanded ports if added later.
#[allow(dead_code)]
fn _types_compile() {
    let _: Option<FastPaxos> = None;
    let _: Option<FastOutgoing> = None;
    let _: Duration = Duration::ZERO;
}
