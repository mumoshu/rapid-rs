//! Coordinator-rule N/4 majority test — F5 gate.
//!
//! Java parity reference:
//!   `references/rapid-java/.../Paxos.java::selectProposalUsingCoordinatorRule`
//!
//! When the Phase-1b replies contain *different* `vval`s at the same
//! max-vrnd, the rule must pick a value held by *more than N/4* nodes;
//! if no value crosses that threshold, any non-empty vval is acceptable.

use rapid::consensus::paxos::PaxosOutgoing;
use rapid::consensus::Paxos;
use rapid::pb;

fn ep(host: &str, port: i32) -> pb::Endpoint {
    pb::Endpoint {
        hostname: host.as_bytes().to_vec(),
        port,
    }
}

fn rank(round: i32, node_index: i32) -> pb::Rank {
    pb::Rank { round, node_index }
}

/// Drive `paxos.start_phase1a(round)` and return the `crnd` it broadcast.
/// The coordinator's `node_index` is derived from a private hash of
/// `my_addr`; this helper recovers it without exposing the internal.
fn start_phase1a_and_recover_crnd(paxos: &mut Paxos, round: i32) -> pb::Rank {
    let outs = paxos.start_phase1a(round);
    for o in outs {
        if let PaxosOutgoing::Broadcast(req) = o {
            if let Some(pb::rapid_request::Content::Phase1aMessage(p1a)) = req.content {
                if let Some(r) = p1a.rank {
                    return r;
                }
            }
        }
    }
    panic!("start_phase1a did not broadcast a Phase1aMessage with a rank");
}

fn b_msg(
    sender: pb::Endpoint,
    crnd: pb::Rank,
    vrnd: pb::Rank,
    vval: Vec<pb::Endpoint>,
) -> pb::Phase1bMessage {
    pb::Phase1bMessage {
        configuration_id: 1,
        rnd: Some(crnd),
        sender: Some(sender),
        vrnd: Some(vrnd),
        vval,
    }
}

#[test]
fn coordinator_rule_picks_value_over_n_over_4() {
    // 8 acceptors, quarter = 8/4 = 2 → "more than N/4" means count > 2,
    // i.e. >= 3. Build two competing values, one of which has 3 votes
    // and the other 2.
    let n = 8usize;
    let mut paxos = Paxos::new(ep("127.0.0.1", 1), 1, n);
    let crnd = start_phase1a_and_recover_crnd(&mut paxos, 2);
    let val_a = vec![ep("10.0.0.1", 1)];
    let val_b = vec![ep("10.0.0.2", 2)];
    let max_vrnd = rank(1, 0);
    let acceptors = (0..5)
        .map(|i| ep("127.0.0.1", 1000 + i))
        .collect::<Vec<_>>();
    let votes = vec![
        b_msg(acceptors[0].clone(), crnd, max_vrnd, val_a.clone()),
        b_msg(acceptors[1].clone(), crnd, max_vrnd, val_a.clone()),
        b_msg(acceptors[2].clone(), crnd, max_vrnd, val_a.clone()),
        b_msg(acceptors[3].clone(), crnd, max_vrnd, val_b.clone()),
        b_msg(acceptors[4].clone(), crnd, max_vrnd, val_b.clone()),
    ];
    for m in votes {
        paxos.handle_phase1b(&m);
    }
    let chosen = paxos.select_proposal_using_coordinator_rule();
    assert_eq!(chosen, val_a, "majority of >N/4 must win");
}

#[test]
fn coordinator_rule_falls_back_to_any_vval_when_no_majority() {
    // 12 acceptors, quarter = 3 → "more than N/4" means count > 3.
    // Two values with 2 votes each (neither > 3): fall back to "any
    // non-empty vval". The picked value depends on iteration order;
    // we only assert it's one of the two.
    let n = 12usize;
    let mut paxos = Paxos::new(ep("127.0.0.1", 1), 1, n);
    let crnd = start_phase1a_and_recover_crnd(&mut paxos, 2);
    let val_a = vec![ep("10.0.0.1", 1)];
    let val_b = vec![ep("10.0.0.2", 2)];
    let max_vrnd = rank(1, 0);
    let acceptors = (0..4)
        .map(|i| ep("127.0.0.1", 1000 + i))
        .collect::<Vec<_>>();
    let votes = vec![
        b_msg(acceptors[0].clone(), crnd, max_vrnd, val_a.clone()),
        b_msg(acceptors[1].clone(), crnd, max_vrnd, val_a.clone()),
        b_msg(acceptors[2].clone(), crnd, max_vrnd, val_b.clone()),
        b_msg(acceptors[3].clone(), crnd, max_vrnd, val_b.clone()),
    ];
    for m in votes {
        paxos.handle_phase1b(&m);
    }
    let chosen = paxos.select_proposal_using_coordinator_rule();
    assert!(
        chosen == val_a || chosen == val_b,
        "fallback path must return one of the candidate vvals"
    );
}

#[test]
fn coordinator_rule_picks_unique_max_vrnd_value() {
    // Only one acceptor has the maximum vrnd → must pick its vval.
    let n = 8usize;
    let mut paxos = Paxos::new(ep("127.0.0.1", 1), 1, n);
    let crnd = start_phase1a_and_recover_crnd(&mut paxos, 2);
    let old_val = vec![ep("10.0.0.99", 99)];
    let new_val = vec![ep("10.0.0.1", 1)];
    paxos.handle_phase1b(&b_msg(
        ep("127.0.0.1", 1001),
        crnd,
        rank(1, 0),
        old_val.clone(),
    ));
    paxos.handle_phase1b(&b_msg(
        ep("127.0.0.1", 1002),
        crnd,
        rank(1, 0),
        old_val.clone(),
    ));
    paxos.handle_phase1b(&b_msg(
        ep("127.0.0.1", 1003),
        crnd,
        rank(1, 5),
        new_val.clone(),
    ));
    let chosen = paxos.select_proposal_using_coordinator_rule();
    assert_eq!(chosen, new_val);
}
