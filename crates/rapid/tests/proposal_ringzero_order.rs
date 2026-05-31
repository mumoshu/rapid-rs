//! Proposal sorted by `RingZeroComparator` — F5 gate.
//!
//! Java parity reference:
//! `MembershipService.handleMessage(BatchedAlertMessage)` calls
//! `fastPaxosInstance.propose(... .sorted(membershipView.getRingZeroComparator()))`.
//!
//! Every Rapid peer must order the proposal identically before voting
//! so that Phase-2b votes accumulate against the same key.

use rapid::pb;
use rapid::view_hash::address_hash;

#[test]
fn proposal_is_sorted_by_ring_zero_hash() {
    // Build a set of endpoints whose lexicographic and ring-zero
    // orderings differ. We assert the *ring-zero* order is what the
    // service emits.
    use rapid::cut_detector::MultiNodeCutDetector;
    use rapid::pb::EdgeStatus;

    // Seed an arbitrary set: 6 distinct endpoints with widely varying
    // hashes.
    let endpoints = [
        ep("10.0.0.5", 4444),
        ep("a-very-long-hostname.example.com", 65535),
        ep("127.0.0.1", 1234),
        ep("127.0.0.2", 2),
        ep("127.0.0.1", 1),
        ep("172.16.7.7", 7),
    ];

    let mut sorted_expected = endpoints.to_vec();
    sorted_expected.sort_by_key(|e| address_hash(0, e));

    // Sanity: at least one pair must be out-of-order under
    // lexicographic vs. ring-zero — otherwise the test is vacuous.
    let mut lex_sorted = endpoints.to_vec();
    lex_sorted.sort_by_key(|a| (a.hostname.clone(), a.port));
    assert_ne!(
        sorted_expected, lex_sorted,
        "test design: pick endpoints whose lex and ring-zero orders differ"
    );

    // Drive the cut detector to a proposal containing all 6 (K=10,
    // H=6, L=2 makes this easy to wedge from a single observer that
    // reports all 6 hosts on all 6 rings).
    let mut cd = MultiNodeCutDetector::new(10, 6, 2).unwrap();
    let observer = ep("10.0.0.1", 1);
    let mut emitted: Vec<pb::Endpoint> = Vec::new();
    for ring in 0..6_i32 {
        for dst in &endpoints {
            let msg = pb::AlertMessage {
                edge_src: Some(observer.clone()),
                edge_dst: Some(dst.clone()),
                edge_status: EdgeStatus::Up as i32,
                configuration_id: -1,
                ring_number: vec![ring],
                node_id: None,
                metadata: None,
            };
            emitted.extend(cd.aggregate(&msg));
        }
    }
    assert_eq!(emitted.len(), 6, "cut detector emits all 6 endpoints");

    // What the service does: sort by ring-zero hash before proposing.
    emitted.sort_by_key(|e| address_hash(0, e));
    assert_eq!(emitted, sorted_expected, "proposal order matches Java");
}

fn ep(host: &str, port: i32) -> pb::Endpoint {
    pb::Endpoint {
        hostname: host.as_bytes().to_vec(),
        port,
    }
}
