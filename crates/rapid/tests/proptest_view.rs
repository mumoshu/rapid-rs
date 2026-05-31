//! Proptest invariants — F5 gate.
//!
//! 1. **Insertion-order independence**: a `MembershipView` populated
//!    with N (endpoint, node-id) pairs has the same memberlist (= ring
//!    zero) and the same `ConfigurationId` regardless of the order
//!    `ring_add` is called.
//! 2. **Configuration-id deterministic on equal sets**: two views with
//!    identical (sorted) member sets agree on `ConfigurationId`.

use proptest::collection::vec;
use proptest::prelude::*;

use rapid::pb;
use rapid::view::MembershipView;

fn endpoint_strategy() -> impl Strategy<Value = pb::Endpoint> {
    (0u16..1000, 1u32..=65000).prop_map(|(host, port)| pb::Endpoint {
        hostname: format!("10.{}.{}.{}", (host >> 8) & 0xff, host & 0xff, port % 256).into_bytes(),
        #[allow(clippy::cast_possible_wrap)]
        port: port as i32,
    })
}

fn node_id_strategy() -> impl Strategy<Value = pb::NodeId> {
    (any::<i64>(), any::<i64>()).prop_map(|(high, low)| pb::NodeId { high, low })
}

fn pairs_strategy(n: usize) -> impl Strategy<Value = Vec<(pb::Endpoint, pb::NodeId)>> {
    vec((endpoint_strategy(), node_id_strategy()), n..=n)
        .prop_filter("distinct endpoints", |pairs| {
            let mut seen = std::collections::HashSet::new();
            pairs
                .iter()
                .all(|(e, _)| seen.insert((e.hostname.clone(), e.port)))
        })
        .prop_filter("distinct node ids", |pairs| {
            let mut seen = std::collections::HashSet::new();
            pairs.iter().all(|(_, n)| seen.insert((n.high, n.low)))
        })
}

fn build(pairs: &[(pb::Endpoint, pb::NodeId)]) -> MembershipView {
    let mut v = MembershipView::new(10).unwrap();
    for (ep, nid) in pairs {
        v.ring_add(ep, nid)
            .expect("ring_add succeeds for distinct pairs");
    }
    v
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn view_insertion_order_independent(pairs in pairs_strategy(8)) {
        let mut reversed = pairs.clone();
        reversed.reverse();
        let mut a = build(&pairs);
        let mut b = build(&reversed);
        let ring_a = a.get_ring(0).unwrap();
        let ring_b = b.get_ring(0).unwrap();
        prop_assert_eq!(ring_a, ring_b);
        prop_assert_eq!(a.current_configuration_id(), b.current_configuration_id());
    }

    #[test]
    fn equal_sets_same_configuration_id(pairs in pairs_strategy(10)) {
        let mut shuffled = pairs.clone();
        shuffled.rotate_left(3);
        let mut a = build(&pairs);
        let mut b = build(&shuffled);
        prop_assert_eq!(a.current_configuration_id(), b.current_configuration_id());
    }
}
