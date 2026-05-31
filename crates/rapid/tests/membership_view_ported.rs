//! Java parity ports for `MembershipViewTest.java`.
//!
//! Each `#[test]` here corresponds 1:1 to a Java test method of the
//! same name. Method names use Rust `snake_case` per project
//! convention; mapping table:
//!
//! | Java                                | Rust                                  |
//! |-------------------------------------|---------------------------------------|
//! | `oneRingAddition`                   | `one_ring_addition`                   |
//! | `multipleRingAdditions`             | `multiple_ring_additions`             |
//! | `ringReAdditions`                   | `ring_re_additions`                   |
//! | `ringDeletionsOnly`                 | `ring_deletions_only`                 |
//! | `ringAdditionsAndDeletions`         | `ring_additions_and_deletions`        |
//! | `monitoringRelationshipEdge`        | `monitoring_relationship_edge`        |
//! | `monitoringRelationshipEmpty`       | `monitoring_relationship_empty`       |
//! | `monitoringRelationshipTwoNodes`    | `monitoring_relationship_two_nodes`   |
//! | `monitoringRelationshipThreeNodesWithDelete` | `monitoring_relationship_three_nodes_with_delete` |
//! | `monitoringRelationshipMultipleNodes`        | `monitoring_relationship_multiple_nodes`         |
//! | `monitoringRelationshipBootstrap`            | `monitoring_relationship_bootstrap`              |
//! | `monitoringRelationshipBootstrapMultiple`    | `monitoring_relationship_bootstrap_multiple`     |
//! | `nodeUniqueIdNoDeletions`           | `node_unique_id_no_deletions`         |
//! | `nodeUniqueIdWithDeletions`         | `node_unique_id_with_deletions`       |
//! | `nodeConfigurationChange`           | `node_configuration_change`           |
//! | `nodeConfigurationsAcrossMViews`    | `node_configurations_across_mviews`   |
// Test-only casts: `num_nodes` are small +ve `i32`; UUID halves reinterpreted as `i64`.
#![allow(clippy::cast_sign_loss, clippy::cast_possible_wrap)]

use std::collections::HashSet;

use rapid::error::Error;
use rapid::pb;
use rapid::view::MembershipView;
use uuid::Uuid;

#[derive(Hash, PartialEq, Eq, Clone)]
struct EndpointKey {
    hostname: Vec<u8>,
    port: i32,
}
impl From<&pb::Endpoint> for EndpointKey {
    fn from(e: &pb::Endpoint) -> Self {
        Self {
            hostname: e.hostname.clone(),
            port: e.port,
        }
    }
}

const K: u8 = 10;

fn ep(host: &str, port: i32) -> pb::Endpoint {
    pb::Endpoint {
        hostname: host.as_bytes().to_vec(),
        port,
    }
}

fn random_node_id() -> pb::NodeId {
    let uuid = Uuid::new_v4();
    let (high, low) = uuid.as_u64_pair();
    pb::NodeId {
        high: high as i64,
        low: low as i64,
    }
}

#[test]
fn one_ring_addition() {
    let mut mview = MembershipView::new(K).unwrap();
    let addr = ep("127.0.0.1", 123);
    mview
        .ring_add(&addr, &random_node_id())
        .expect("ring_add should succeed");
    for k in 0..K {
        let list = mview.get_ring(k).unwrap();
        assert_eq!(list.len(), 1);
        for e in list {
            assert_eq!(e, addr);
        }
    }
}

#[test]
fn multiple_ring_additions() {
    let mut mview = MembershipView::new(K).unwrap();
    let num_nodes: i32 = 10;
    for i in 0..num_nodes {
        mview
            .ring_add(&ep("127.0.0.1", i), &random_node_id())
            .expect("ring_add should succeed");
    }
    for k in 0..K {
        assert_eq!(mview.get_ring(k).unwrap().len(), num_nodes as usize);
    }
}

#[test]
fn ring_re_additions() {
    let mut mview = MembershipView::new(K).unwrap();
    let num_nodes: i32 = 10;
    for i in 0..num_nodes {
        mview
            .ring_add(&ep("127.0.0.1", i), &random_node_id())
            .expect("ring_add should succeed");
    }
    for k in 0..K {
        assert_eq!(mview.get_ring(k).unwrap().len(), num_nodes as usize);
    }
    let mut num_throws = 0;
    for i in 0..num_nodes {
        if mview
            .ring_add(&ep("127.0.0.1", i), &random_node_id())
            .is_err()
        {
            num_throws += 1;
        }
    }
    assert_eq!(num_throws, num_nodes);
}

#[test]
fn ring_deletions_only() {
    let mut mview = MembershipView::new(K).unwrap();
    let num_nodes: i32 = 10;
    let mut num_throws = 0;
    for i in 0..num_nodes {
        if mview.ring_delete(&ep("127.0.0.1", i)).is_err() {
            num_throws += 1;
        }
    }
    assert_eq!(num_throws, num_nodes);
}

#[test]
fn ring_additions_and_deletions() {
    let mut mview = MembershipView::new(K).unwrap();
    let num_nodes: i32 = 10;
    for i in 0..num_nodes {
        mview
            .ring_add(&ep("127.0.0.1", i), &random_node_id())
            .unwrap();
    }
    for i in 0..num_nodes {
        mview.ring_delete(&ep("127.0.0.1", i)).unwrap();
    }
    for k in 0..K {
        assert_eq!(mview.get_ring(k).unwrap().len(), 0);
    }
}

#[test]
fn monitoring_relationship_edge() {
    let mut mview = MembershipView::new(K).unwrap();
    let n1 = ep("127.0.0.1", 1);
    mview.ring_add(&n1, &random_node_id()).unwrap();
    assert_eq!(mview.get_subjects_of(&n1).unwrap().len(), 0);
    assert_eq!(mview.get_observers_of(&n1).unwrap().len(), 0);
    let n2 = ep("127.0.0.1", 2);
    assert!(matches!(
        mview.get_subjects_of(&n2),
        Err(Error::ProtocolRejected(_))
    ));
    assert!(matches!(
        mview.get_observers_of(&n2),
        Err(Error::ProtocolRejected(_))
    ));
}

#[test]
fn monitoring_relationship_empty() {
    let mut mview = MembershipView::new(K).unwrap();
    let n = ep("127.0.0.1", 1);
    assert!(matches!(
        mview.get_subjects_of(&n),
        Err(Error::ProtocolRejected(_))
    ));
    assert!(matches!(
        mview.get_observers_of(&n),
        Err(Error::ProtocolRejected(_))
    ));
}

#[test]
fn monitoring_relationship_two_nodes() {
    let mut mview = MembershipView::new(K).unwrap();
    let n1 = ep("127.0.0.1", 1);
    let n2 = ep("127.0.0.1", 2);
    mview.ring_add(&n1, &random_node_id()).unwrap();
    mview.ring_add(&n2, &random_node_id()).unwrap();
    assert_eq!(mview.get_subjects_of(&n1).unwrap().len(), K as usize);
    assert_eq!(mview.get_observers_of(&n1).unwrap().len(), K as usize);
    let s_unique: HashSet<EndpointKey> = mview
        .get_subjects_of(&n1)
        .unwrap()
        .iter()
        .map(EndpointKey::from)
        .collect();
    let o_unique: HashSet<EndpointKey> = mview
        .get_observers_of(&n1)
        .unwrap()
        .iter()
        .map(EndpointKey::from)
        .collect();
    assert_eq!(s_unique.len(), 1);
    assert_eq!(o_unique.len(), 1);
}

#[test]
fn monitoring_relationship_three_nodes_with_delete() {
    let mut mview = MembershipView::new(K).unwrap();
    let n1 = ep("127.0.0.1", 1);
    let n2 = ep("127.0.0.1", 2);
    let n3 = ep("127.0.0.1", 3);
    mview.ring_add(&n1, &random_node_id()).unwrap();
    mview.ring_add(&n2, &random_node_id()).unwrap();
    mview.ring_add(&n3, &random_node_id()).unwrap();
    assert_eq!(mview.get_subjects_of(&n1).unwrap().len(), K as usize);
    assert_eq!(mview.get_observers_of(&n1).unwrap().len(), K as usize);
    let s_unique: HashSet<EndpointKey> = mview
        .get_subjects_of(&n1)
        .unwrap()
        .iter()
        .map(EndpointKey::from)
        .collect();
    let o_unique: HashSet<EndpointKey> = mview
        .get_observers_of(&n1)
        .unwrap()
        .iter()
        .map(EndpointKey::from)
        .collect();
    assert_eq!(s_unique.len(), 2);
    assert_eq!(o_unique.len(), 2);
    mview.ring_delete(&n2).unwrap();
    assert_eq!(mview.get_subjects_of(&n1).unwrap().len(), K as usize);
    assert_eq!(mview.get_observers_of(&n1).unwrap().len(), K as usize);
    let s_unique: HashSet<EndpointKey> = mview
        .get_subjects_of(&n1)
        .unwrap()
        .iter()
        .map(EndpointKey::from)
        .collect();
    let o_unique: HashSet<EndpointKey> = mview
        .get_observers_of(&n1)
        .unwrap()
        .iter()
        .map(EndpointKey::from)
        .collect();
    assert_eq!(s_unique.len(), 1);
    assert_eq!(o_unique.len(), 1);
}

#[test]
fn monitoring_relationship_multiple_nodes() {
    let mut mview = MembershipView::new(K).unwrap();
    let num_nodes: i32 = 1000;
    let mut list = Vec::new();
    for i in 0..num_nodes {
        let n = ep("127.0.0.1", i);
        list.push(n.clone());
        mview.ring_add(&n, &random_node_id()).unwrap();
    }
    for n in &list {
        assert_eq!(mview.get_subjects_of(n).unwrap().len(), K as usize);
        assert_eq!(mview.get_observers_of(n).unwrap().len(), K as usize);
    }
}

#[test]
fn monitoring_relationship_bootstrap() {
    let mut mview = MembershipView::new(K).unwrap();
    let server_port = 1234;
    let n = ep("127.0.0.1", server_port);
    mview.ring_add(&n, &random_node_id()).unwrap();
    let joining = ep("127.0.0.1", server_port + 1);
    let expected = mview.get_expected_observers_of(&joining);
    assert_eq!(expected.len(), K as usize);
    let unique: HashSet<EndpointKey> = expected.iter().map(EndpointKey::from).collect();
    assert_eq!(unique.len(), 1);
    assert_eq!(expected[0], n);
}

#[test]
fn monitoring_relationship_bootstrap_multiple() {
    let mut mview = MembershipView::new(K).unwrap();
    let num_nodes: i32 = 20;
    let server_port_base: i32 = 1234;
    let joining = ep("127.0.0.1", server_port_base - 1);
    let mut num_observers = 0;
    for i in 0..num_nodes {
        let n = ep("127.0.0.1", server_port_base + i);
        mview.ring_add(&n, &random_node_id()).unwrap();
        let num_actual = mview.get_expected_observers_of(&joining).len();
        assert!(num_observers <= num_actual);
        num_observers = num_actual;
    }
    assert!((K as usize) - 3 <= num_observers);
    assert!((K as usize) >= num_observers);
}

#[test]
fn node_unique_id_no_deletions() {
    let mut mview = MembershipView::new(K).unwrap();
    let n1 = ep("127.0.0.1", 1);
    let id1 = random_node_id();
    mview.ring_add(&n1, &id1).unwrap();
    // Same host, same id → fails.
    let n2 = ep("127.0.0.1", 1);
    assert!(mview.ring_add(&n2, &id1).is_err());
    // Same host, different id → still fails (endpoint already in ring).
    assert!(mview.ring_add(&n2, &random_node_id()).is_err());
    // Different host, same id → fails (uuid already in ring).
    let n3 = ep("127.0.0.1", 2);
    assert!(mview.ring_add(&n3, &id1).is_err());
    // Different host, different id → succeeds.
    mview.ring_add(&n3, &random_node_id()).unwrap();
    assert_eq!(mview.get_ring(0).unwrap().len(), 2);
}

#[test]
fn node_unique_id_with_deletions() {
    let mut mview = MembershipView::new(K).unwrap();
    let n1 = ep("127.0.0.1", 1);
    let id1 = random_node_id();
    mview.ring_add(&n1, &id1).unwrap();
    let n2 = ep("127.0.0.1", 2);
    let id2 = random_node_id();
    mview.ring_add(&n2, &id2).unwrap();
    mview.ring_delete(&n2).unwrap();
    assert_eq!(mview.get_ring(0).unwrap().len(), 1);
    // Same node, same id after delete → still uuid-already-seen.
    assert!(mview.ring_add(&n2, &id2).is_err());
    // Same node, new id → succeeds.
    mview.ring_add(&n2, &random_node_id()).unwrap();
    assert_eq!(mview.get_ring(0).unwrap().len(), 2);
}

#[test]
fn node_configuration_change() {
    let mut mview = MembershipView::new(K).unwrap();
    let num_nodes: i32 = 1000;
    let mut set: HashSet<_> = HashSet::new();
    for i in 0..num_nodes {
        let n = ep("127.0.0.1", i);
        mview.ring_add(&n, &deterministic_id(&n)).unwrap();
        set.insert(mview.current_configuration_id());
    }
    assert_eq!(set.len(), num_nodes as usize);
}

#[test]
fn node_configurations_across_mviews() {
    let mut mv1 = MembershipView::new(K).unwrap();
    let mut mv2 = MembershipView::new(K).unwrap();
    let num_nodes: i32 = 1000;
    let mut list1 = Vec::new();
    let mut list2 = Vec::new();
    for i in 0..num_nodes {
        let n = ep("127.0.0.1", i);
        mv1.ring_add(&n, &deterministic_id(&n)).unwrap();
        list1.push(mv1.current_configuration_id());
    }
    for i in (0..num_nodes).rev() {
        let n = ep("127.0.0.1", i);
        mv2.ring_add(&n, &deterministic_id(&n)).unwrap();
        list2.push(mv2.current_configuration_id());
    }
    assert_eq!(list1.len(), num_nodes as usize);
    assert_eq!(list2.len(), num_nodes as usize);
    // Every intermediate id differs because the running set differs.
    let last = (num_nodes - 1) as usize;
    for i in 0..last {
        assert_ne!(list1[i], list2[i]);
    }
    // The final configuration is identical (same set, by hashing
    // identifiers + ring-0 endpoints — both are order-invariant).
    assert_eq!(list1[last], list2[last]);
}

// Java uses `UUID.nameUUIDFromBytes(addr.toString())` for stable
// per-endpoint ids in `nodeConfigurationChange` /
// `nodeConfigurationsAcrossMViews`. Our deterministic mirror hashes
// the endpoint into a `pb::NodeId` so cross-`MembershipView` configs
// converge on equal final ids regardless of insertion order.
fn deterministic_id(endpoint: &pb::Endpoint) -> pb::NodeId {
    use std::hash::{Hash, Hasher};
    let mut h1 = std::collections::hash_map::DefaultHasher::new();
    endpoint.hostname.hash(&mut h1);
    endpoint.port.hash(&mut h1);
    1u64.hash(&mut h1);
    let mut h2 = std::collections::hash_map::DefaultHasher::new();
    endpoint.hostname.hash(&mut h2);
    endpoint.port.hash(&mut h2);
    2u64.hash(&mut h2);
    pb::NodeId {
        #[allow(clippy::cast_possible_wrap)]
        high: h1.finish() as i64,
        #[allow(clippy::cast_possible_wrap)]
        low: h2.finish() as i64,
    }
}
