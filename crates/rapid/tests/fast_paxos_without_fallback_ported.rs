//! Java parity ports for `FastPaxosWithoutFallbackTests.java`.
//!
//! Java's tests drive the full `MembershipService` and observe
//! membership size after applying `FastRoundPhase2bMessage`s. We
//! exercise `FastPaxos` directly because:
//! - it's the unit under test for the quorum + conflict-resolution
//!   behaviour the Java tests assert;
//! - the membership change Java observes is the same `FastOutgoing::Decision`
//!   our impl emits.
//!
//! Java N/quorum data tables are reproduced verbatim.
//!
//! Mapping:
//!  - `fastQuorumTestNoConflicts` → `fast_quorum_test_no_conflicts`
//!  - `fastQuorumTestWithConflicts` → `fast_quorum_test_with_conflicts`

use std::time::Duration;

use rapid::consensus::fast_paxos::FastOutgoing;
use rapid::consensus::FastPaxos;
use rapid::pb;

fn ep(host: &str, port: i32) -> pb::Endpoint {
    pb::Endpoint {
        hostname: host.as_bytes().to_vec(),
        port,
    }
}

fn addr_for_base(base_port: i32, i: usize) -> pb::Endpoint {
    ep("127.0.0.1", base_port + i32::try_from(i).unwrap())
}

fn vote(sender: pb::Endpoint, vval: Vec<pb::Endpoint>, config: i64) -> pb::FastRoundPhase2bMessage {
    pb::FastRoundPhase2bMessage {
        sender: Some(sender),
        endpoints: vval,
        configuration_id: config,
    }
}

fn was_decided(outs: &[FastOutgoing]) -> bool {
    outs.iter().any(|o| matches!(o, FastOutgoing::Decision(_)))
}

#[test]
fn fast_quorum_test_no_conflicts() {
    // Java parameter table (Java line 86–89). Each row: (N, quorum).
    let cases: &[(usize, usize)] = &[
        (6, 5),
        (48, 37),
        (50, 38),
        (100, 76),
        (102, 77),
        (5, 4),
        (51, 39),
        (49, 37),
        (99, 75),
        (101, 76),
    ];
    let base_port = 1234;
    let self_addr = ep("127.0.0.1", base_port);
    let proposal_node = ep("127.0.0.1", base_port + 1);
    let proposal = vec![proposal_node];
    let config_id = 42;
    for &(n, quorum) in cases {
        let mut fp = FastPaxos::new(
            self_addr.clone(),
            config_id,
            n,
            Duration::from_secs(1000), // jitter irrelevant; we never trigger fallback
        );
        let mut decided_at: Option<usize> = None;
        for i in 0..quorum {
            let msg = vote(addr_for_base(base_port, i), proposal.clone(), config_id);
            let outs = fp.handle_fast_round_proposal(&msg);
            if was_decided(&outs) {
                decided_at = Some(i);
                break;
            }
        }
        assert_eq!(
            decided_at,
            Some(quorum - 1),
            "N={n} quorum={quorum}: must decide on the quorum-th vote",
        );
    }
}

#[test]
fn fast_quorum_test_with_conflicts() {
    // Java parameter tables (Java line 132-142). Each row:
    //   (N, quorum, num_conflicts, should_decide).
    let cases: &[(usize, usize, usize, bool)] = &[
        // One conflict → decides.
        (6, 5, 1, true),
        (48, 37, 1, true),
        (50, 38, 1, true),
        (100, 76, 1, true),
        (102, 77, 1, true),
        // Boundary: F conflicts + N-F non-conflicts → decides.
        (48, 37, 11, true),
        (50, 38, 12, true),
        (100, 76, 24, true),
        (102, 77, 25, true),
        // Too many conflicts → no decision.
        (6, 5, 2, false),
        (48, 37, 14, false),
        (50, 38, 13, false),
        (100, 76, 25, false),
        (102, 77, 26, false),
    ];
    let base_port = 1234;
    let self_addr = ep("127.0.0.1", base_port);
    let proposal_node = ep("127.0.0.1", base_port + 1);
    let conflict_node = ep("127.0.0.1", base_port + 2);
    let proposal = vec![proposal_node];
    let conflict = vec![conflict_node];
    let config_id = 99;
    for &(n, quorum, num_conflicts, should_decide) in cases {
        let mut fp = FastPaxos::new(self_addr.clone(), config_id, n, Duration::from_secs(1000));
        // First num_conflicts senders vote for `conflict`.
        let mut decided = false;
        for i in 0..num_conflicts {
            let outs = fp.handle_fast_round_proposal(&vote(
                addr_for_base(base_port, i),
                conflict.clone(),
                config_id,
            ));
            if was_decided(&outs) {
                decided = true;
            }
        }
        assert!(
            !decided,
            "N={n}: should not decide while only conflicts received"
        );
        // Then non-conflicting votes for `proposal`. Java uses
        // nonConflictCount = min(numConflicts + quorum - 1, N - 1).
        let non_conflict_count = (num_conflicts + quorum - 1).min(n - 1);
        for i in num_conflicts..non_conflict_count {
            let outs = fp.handle_fast_round_proposal(&vote(
                addr_for_base(base_port, i),
                proposal.clone(),
                config_id,
            ));
            if was_decided(&outs) {
                decided = true;
            }
        }
        // Final vote (the one Java asserts on).
        let outs = fp.handle_fast_round_proposal(&vote(
            addr_for_base(base_port, non_conflict_count),
            proposal.clone(),
            config_id,
        ));
        if was_decided(&outs) {
            decided = true;
        }
        assert_eq!(
            decided, should_decide,
            "N={n} quorum={quorum} conflicts={num_conflicts}: decided={decided} expected={should_decide}",
        );
    }
}
