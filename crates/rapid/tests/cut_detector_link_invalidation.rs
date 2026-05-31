//! Port of `CutDetectionTest.cutDetectionTestLinkInvalidation`. Needs
//! cross-module access to both `MembershipView` and `MultiNodeCutDetector`
//! so it lives as an integration test rather than inside either unit-test
//! block.

#![allow(clippy::needless_range_loop)]

use std::collections::HashSet;

use rapid::cut_detector::MultiNodeCutDetector;
use rapid::pb;
use rapid::view::MembershipView;

fn ep(host: &str, port: i32) -> pb::Endpoint {
    pb::Endpoint {
        hostname: host.as_bytes().to_vec(),
        port,
    }
}

fn nid(high: i64, low: i64) -> pb::NodeId {
    pb::NodeId { high, low }
}

fn alert(
    src: pb::Endpoint,
    dst: pb::Endpoint,
    status: pb::EdgeStatus,
    ring_number: i32,
) -> pb::AlertMessage {
    pb::AlertMessage {
        edge_src: Some(src),
        edge_dst: Some(dst),
        edge_status: status as i32,
        configuration_id: -1,
        ring_number: vec![ring_number],
        node_id: None,
        metadata: None,
    }
}

#[test]
fn cut_detection_link_invalidation() {
    const K: u8 = 10;
    const H: u8 = 8;
    const L: u8 = 2;
    let mut mview = MembershipView::new(K).expect("invariant: K > 0");
    let mut cd = MultiNodeCutDetector::new(K, H, L).expect("valid thresholds");

    let n: i32 = 30;
    let mut endpoints = Vec::new();
    for i in 0..n {
        let endpoint = ep("127.0.0.2", 2 + i);
        endpoints.push(endpoint.clone());
        mview
            .ring_add(&endpoint, &nid(0, i64::from(i)))
            .expect("ring_add succeeds");
    }

    let dst = endpoints[0].clone();
    let observers = mview.get_observers_of(&dst).expect("dst in ring");
    assert_eq!(observers.len(), K as usize);

    // H - 1 alerts about dst, from observers[0..H-1).
    for i in 0..usize::from(H - 1) {
        let ret = cd.aggregate(&alert(
            observers[i].clone(),
            dst.clone(),
            pb::EdgeStatus::Down,
            i32::try_from(i).unwrap(),
        ));
        assert!(ret.is_empty());
        assert_eq!(cd.num_proposals(), 0);
    }

    // Alerts *about* observers[H-1..K) past H.
    let mut failed_observers: HashSet<(Vec<u8>, i32)> = HashSet::new();
    for (idx, observer) in observers
        .iter()
        .enumerate()
        .skip(usize::from(H - 1))
        .take(usize::from(K - (H - 1)))
    {
        let observers_of_observer = mview
            .get_observers_of(&observer.clone())
            .expect("observer in ring");
        failed_observers.insert((observer.hostname.clone(), observer.port));
        for j in 0..usize::from(K) {
            let ret = cd.aggregate(&alert(
                observers_of_observer[j].clone(),
                observer.clone(),
                pb::EdgeStatus::Down,
                i32::try_from(j).unwrap(),
            ));
            assert!(
                ret.is_empty(),
                "iteration idx={idx} j={j} unexpected proposal"
            );
            assert_eq!(cd.num_proposals(), 0);
        }
    }

    let ret = cd.invalidate_failing_edges(&mut mview);
    assert_eq!(ret.len(), 4, "expected proposal of size 4, got {ret:?}");
    assert_eq!(cd.num_proposals(), 1);
    let dst_key = (dst.hostname.clone(), dst.port);
    for node in &ret {
        let key = (node.hostname.clone(), node.port);
        assert!(
            failed_observers.contains(&key) || key == dst_key,
            "unexpected node in proposal: {node:?}"
        );
    }
}
