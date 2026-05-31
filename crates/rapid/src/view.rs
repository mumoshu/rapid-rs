//! `MembershipView` — K monitoring rings + identifier set + configuration id.
//!
//! Single-owner Rust port of Java's
//! `MembershipView` (`references/rapid-java/.../MembershipView.java`).
//! The Rust version drops the Java `ReentrantReadWriteLock`: the membership
//! service actor owns its view exclusively (see RULES §Async).
//!
//! # Naming caveat
//!
//! Java's `getObserversOf(X)` returns the K successors of X on each ring,
//! and `getSubjectsOf(X)` returns the K predecessors. The wire protocol
//! and downstream code all depend on this convention, so we preserve it
//! verbatim even though it inverts the paper's `p_j observes p_(j+1)`
//! phrasing. In our terms:
//!
//! * **observers of X** = the K endpoints that monitor X (= ring successors).
//! * **subjects of X**  = the K endpoints X monitors (= ring predecessors).

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;

use crate::error::{Error, Result};
use crate::pb;
use crate::ring::Ring;
use crate::types::ConfigurationId;
use crate::view_hash::configuration_id_for;

/// Java `MembershipView.NodeIdComparator` order: `(high, low)` lexicographic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct NodeIdKey(i64, i64);

impl From<&pb::NodeId> for NodeIdKey {
    fn from(value: &pb::NodeId) -> Self {
        Self(value.high, value.low)
    }
}

/// K monitoring rings + the bookkeeping required by the cut detector.
pub struct MembershipView {
    k: u8,
    rings: Vec<Ring>,
    identifiers: BTreeSet<NodeIdKey>,
    identifier_objs: HashMap<NodeIdKey, pb::NodeId>,
    all_nodes: HashSet<EndpointKey>,
    cached_observers: HashMap<EndpointKey, Vec<pb::Endpoint>>,
    configuration_id: ConfigurationId,
    configuration_dirty: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EndpointKey {
    hostname: Vec<u8>,
    port: i32,
}

impl From<&pb::Endpoint> for EndpointKey {
    fn from(value: &pb::Endpoint) -> Self {
        Self {
            hostname: value.hostname.clone(),
            port: value.port,
        }
    }
}

/// Status of a join attempt — direct port of Java
/// `MembershipView.isSafeToJoin` return values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinDisposition {
    /// `JoinStatusCode.HOSTNAME_ALREADY_IN_RING`.
    HostnameAlreadyInRing,
    /// `JoinStatusCode.UUID_ALREADY_IN_RING`.
    UuidAlreadyInRing,
    /// `JoinStatusCode.SAFE_TO_JOIN`.
    SafeToJoin,
}

impl MembershipView {
    /// Empty view with `k` rings.
    ///
    /// # Errors
    /// Returns [`Error::Internal`] if `k` is 0.
    pub fn new(k: u8) -> Result<Self> {
        if k == 0 {
            return Err(Error::Internal("MembershipView requires K > 0".into()));
        }
        let rings = (0..k).map(|i| Ring::new(u64::from(i))).collect();
        Ok(Self {
            k,
            rings,
            identifiers: BTreeSet::new(),
            identifier_objs: HashMap::new(),
            all_nodes: HashSet::new(),
            cached_observers: HashMap::new(),
            configuration_id: ConfigurationId::SENTINEL,
            configuration_dirty: true,
        })
    }

    /// Bootstrap a view with an initial membership and identifier set.
    ///
    /// # Errors
    /// Returns [`Error::Internal`] if `k` is 0.
    pub fn bootstrap(
        k: u8,
        identifiers: impl IntoIterator<Item = pb::NodeId>,
        endpoints: impl IntoIterator<Item = pb::Endpoint>,
    ) -> Result<Self> {
        let mut v = Self::new(k)?;
        for id in identifiers {
            v.identifiers.insert(NodeIdKey::from(&id));
            v.identifier_objs.insert(NodeIdKey::from(&id), id);
        }
        for endpoint in endpoints {
            v.all_nodes.insert(EndpointKey::from(&endpoint));
            for ring in &mut v.rings {
                ring.insert(endpoint.clone());
            }
        }
        v.configuration_dirty = true;
        Ok(v)
    }

    /// Number of rings.
    #[must_use]
    pub fn k(&self) -> u8 {
        self.k
    }

    /// Number of endpoints currently in the view.
    #[must_use]
    pub fn membership_size(&self) -> usize {
        self.rings[0].len()
    }

    /// Whether `endpoint` is in the current membership.
    #[must_use]
    pub fn is_host_present(&self, endpoint: &pb::Endpoint) -> bool {
        self.all_nodes.contains(&EndpointKey::from(endpoint))
    }

    /// Whether `id` has been seen before (current or removed members).
    #[must_use]
    pub fn is_identifier_present(&self, id: &pb::NodeId) -> bool {
        self.identifiers.contains(&NodeIdKey::from(id))
    }

    /// Java `isSafeToJoin(node, uuid)`.
    #[must_use]
    pub fn is_safe_to_join(&self, endpoint: &pb::Endpoint, id: &pb::NodeId) -> JoinDisposition {
        if self.is_host_present(endpoint) {
            JoinDisposition::HostnameAlreadyInRing
        } else if self.is_identifier_present(id) {
            JoinDisposition::UuidAlreadyInRing
        } else {
            JoinDisposition::SafeToJoin
        }
    }

    /// Java `ringAdd(node, nodeId)`.
    ///
    /// # Errors
    /// Returns [`Error::ProtocolRejected`] if the identifier was already
    /// seen, or if the endpoint is already present in the ring.
    pub fn ring_add(&mut self, endpoint: &pb::Endpoint, id: &pb::NodeId) -> Result<()> {
        if self.is_identifier_present(id) {
            return Err(Error::ProtocolRejected(format!(
                "UUID {:x}{:x} already seen",
                id.high, id.low
            )));
        }
        if self.rings[0].contains(endpoint) {
            return Err(Error::ProtocolRejected(format!(
                "endpoint {}:{} already in ring",
                String::from_utf8_lossy(&endpoint.hostname),
                endpoint.port
            )));
        }

        let mut affected_subjects: HashSet<EndpointKey> = HashSet::new();
        for ring in &mut self.rings {
            ring.insert(endpoint.clone());
            if let Some(subject) = ring.lower(endpoint) {
                affected_subjects.insert(EndpointKey::from(subject));
            }
        }
        self.all_nodes.insert(EndpointKey::from(endpoint));
        for key in affected_subjects {
            self.cached_observers.remove(&key);
        }
        self.identifiers.insert(NodeIdKey::from(id));
        self.identifier_objs.insert(NodeIdKey::from(id), *id);
        self.configuration_dirty = true;
        Ok(())
    }

    /// Java `ringDelete(node)`.
    ///
    /// # Errors
    /// Returns [`Error::ProtocolRejected`] if the endpoint is not present.
    pub fn ring_delete(&mut self, endpoint: &pb::Endpoint) -> Result<()> {
        if !self.rings[0].contains(endpoint) {
            return Err(Error::ProtocolRejected(format!(
                "endpoint {}:{} not in ring",
                String::from_utf8_lossy(&endpoint.hostname),
                endpoint.port
            )));
        }

        let mut affected_subjects: HashSet<EndpointKey> = HashSet::new();
        for ring in &mut self.rings {
            if let Some(subject) = ring.lower(endpoint) {
                affected_subjects.insert(EndpointKey::from(subject));
            }
            ring.remove(endpoint);
        }
        let endpoint_key = EndpointKey::from(endpoint);
        self.all_nodes.remove(&endpoint_key);
        self.cached_observers.remove(&endpoint_key);
        for key in affected_subjects {
            self.cached_observers.remove(&key);
        }
        self.configuration_dirty = true;
        Ok(())
    }

    /// Java `getObserversOf(node)` — successors of `node` on each ring.
    ///
    /// # Errors
    /// Returns [`Error::ProtocolRejected`] if `node` is not in the view.
    pub fn get_observers_of(&mut self, node: &pb::Endpoint) -> Result<Vec<pb::Endpoint>> {
        if !self.is_host_present(node) {
            return Err(Error::ProtocolRejected("node not in ring".into()));
        }
        let key = EndpointKey::from(node);
        if let Some(cached) = self.cached_observers.get(&key) {
            return Ok(cached.clone());
        }
        let observers = self.compute_observers_of(node);
        self.cached_observers.insert(key, observers.clone());
        Ok(observers)
    }

    fn compute_observers_of(&self, node: &pb::Endpoint) -> Vec<pb::Endpoint> {
        if self.rings[0].len() <= 1 {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(self.k as usize);
        for ring in &self.rings {
            let next = ring.higher(node).cloned().or_else(|| ring.first().cloned());
            if let Some(e) = next {
                out.push(e);
            }
        }
        out
    }

    /// Java `getSubjectsOf(node)` — predecessors of `node` on each ring.
    ///
    /// # Errors
    /// Returns [`Error::ProtocolRejected`] if `node` is not in the view.
    pub fn get_subjects_of(&self, node: &pb::Endpoint) -> Result<Vec<pb::Endpoint>> {
        if !self.is_host_present(node) {
            return Err(Error::ProtocolRejected("node not in ring".into()));
        }
        if self.rings[0].len() <= 1 {
            return Ok(Vec::new());
        }
        Ok(self.predecessors_of(node))
    }

    /// Java `getExpectedObserversOf(node)` — predecessors of `node`, also
    /// callable for nodes not yet in the ring. Returns the empty list when
    /// the ring is empty.
    #[must_use]
    pub fn get_expected_observers_of(&self, node: &pb::Endpoint) -> Vec<pb::Endpoint> {
        if self.rings[0].len() == 0 {
            return Vec::new();
        }
        self.predecessors_of(node)
    }

    fn predecessors_of(&self, node: &pb::Endpoint) -> Vec<pb::Endpoint> {
        let mut out = Vec::with_capacity(self.k as usize);
        for ring in &self.rings {
            let prev = ring.lower(node).cloned().or_else(|| ring.last().cloned());
            if let Some(e) = prev {
                out.push(e);
            }
        }
        out
    }

    /// Java `Configuration.nodeIds` — identifiers in `NodeIdComparator`
    /// order (high, low lexicographic). Used by `JoinResponse` payload in
    /// the race-case and by `decideViewChange` for the broadcast.
    #[must_use]
    pub fn current_identifiers(&self) -> Vec<pb::NodeId> {
        self.identifiers
            .iter()
            .map(|k| self.identifier_objs[k])
            .collect()
    }

    /// Java `getRing(k)` — snapshot of ring `k` in sort order.
    ///
    /// # Errors
    /// Returns [`Error::Internal`] if `k >= self.k`.
    pub fn get_ring(&self, k: u8) -> Result<Vec<pb::Endpoint>> {
        if k >= self.k {
            return Err(Error::Internal(format!(
                "ring index {k} out of bounds (K={})",
                self.k
            )));
        }
        Ok(self.rings[k as usize].ordered().cloned().collect())
    }

    /// Java `getRingNumbers(observer, subject)` — the ring indices on
    /// which `subject` is a predecessor of `observer` (= a subject of
    /// `observer`).
    ///
    /// # Errors
    /// Returns [`Error::ProtocolRejected`] if `observer` is not in the
    /// view.
    pub fn get_ring_numbers(
        &self,
        observer: &pb::Endpoint,
        subject: &pb::Endpoint,
    ) -> Result<Vec<u8>> {
        let subjects = self.get_subjects_of(observer)?;
        if subjects.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for (k, s) in subjects.iter().enumerate() {
            if s == subject {
                #[allow(clippy::cast_possible_truncation)]
                out.push(k as u8);
            }
        }
        Ok(out)
    }

    /// Java `getCurrentConfigurationId()`.
    pub fn current_configuration_id(&mut self) -> ConfigurationId {
        if self.configuration_dirty {
            self.refresh_configuration_id();
            self.configuration_dirty = false;
        }
        self.configuration_id
    }

    fn refresh_configuration_id(&mut self) {
        let identifiers: Vec<&pb::NodeId> = self
            .identifiers
            .iter()
            .map(|k| &self.identifier_objs[k])
            .collect();
        let endpoints: Vec<&pb::Endpoint> = self.rings[0].ordered().collect();
        self.configuration_id = configuration_id_for(identifiers, endpoints);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ep(host: &str, port: i32) -> pb::Endpoint {
        pb::Endpoint {
            hostname: host.as_bytes().to_vec(),
            port,
        }
    }

    fn nid(high: i64, low: i64) -> pb::NodeId {
        pb::NodeId { high, low }
    }

    fn name_uuid_from_bytes(input: &[u8]) -> pb::NodeId {
        // Matches Java `UUID.nameUUIDFromBytes`: MD5, set version 3, variant 2.
        // Replicated bit-for-bit so the Rust test harness can rebuild the
        // same `NodeId` set the Java `ViewDump` harness uses.
        let digest = md5_compat::md5(input);
        let mut bytes = digest;
        bytes[6] = (bytes[6] & 0x0f) | 0x30;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        let high = i64::from_be_bytes(bytes[0..8].try_into().expect("8 bytes"));
        let low = i64::from_be_bytes(bytes[8..16].try_into().expect("8 bytes"));
        pb::NodeId { high, low }
    }

    /// Tiny inline MD5 — only used by test helpers to mirror Java's
    /// `UUID.nameUUIDFromBytes`. The crate already has the `xxhash-rust`
    /// dep; pulling another for one test would inflate dev-deps.
    #[allow(
        clippy::many_single_char_names,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    mod md5_compat {
        // RFC 1321 reference implementation, condensed.
        const S: [u32; 64] = [
            7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20,
            5, 9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
            6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
        ];
        const K: [u32; 64] = [
            0xd76a_a478,
            0xe8c7_b756,
            0x2420_70db,
            0xc1bd_ceee,
            0xf57c_0faf,
            0x4787_c62a,
            0xa830_4613,
            0xfd46_9501,
            0x6980_98d8,
            0x8b44_f7af,
            0xffff_5bb1,
            0x895c_d7be,
            0x6b90_1122,
            0xfd98_7193,
            0xa679_438e,
            0x49b4_0821,
            0xf61e_2562,
            0xc040_b340,
            0x265e_5a51,
            0xe9b6_c7aa,
            0xd62f_105d,
            0x0244_1453,
            0xd8a1_e681,
            0xe7d3_fbc8,
            0x21e1_cde6,
            0xc337_07d6,
            0xf4d5_0d87,
            0x455a_14ed,
            0xa9e3_e905,
            0xfcef_a3f8,
            0x676f_02d9,
            0x8d2a_4c8a,
            0xfffa_3942,
            0x8771_f681,
            0x6d9d_6122,
            0xfde5_380c,
            0xa4be_ea44,
            0x4bde_cfa9,
            0xf6bb_4b60,
            0xbebf_bc70,
            0x289b_7ec6,
            0xeaa1_27fa,
            0xd4ef_3085,
            0x0488_1d05,
            0xd9d4_d039,
            0xe6db_99e5,
            0x1fa2_7cf8,
            0xc4ac_5665,
            0xf429_2244,
            0x432a_ff97,
            0xab94_23a7,
            0xfc93_a039,
            0x655b_59c3,
            0x8f0c_cc92,
            0xffef_f47d,
            0x8584_5dd1,
            0x6fa8_7e4f,
            0xfe2c_e6e0,
            0xa301_4314,
            0x4e08_11a1,
            0xf753_7e82,
            0xbd3a_f235,
            0x2ad7_d2bb,
            0xeb86_d391,
        ];

        pub(super) fn md5(input: &[u8]) -> [u8; 16] {
            let mut msg = input.to_vec();
            let bit_len = (input.len() as u64).wrapping_mul(8);
            msg.push(0x80);
            while msg.len() % 64 != 56 {
                msg.push(0);
            }
            msg.extend_from_slice(&bit_len.to_le_bytes());

            let mut a0: u32 = 0x6745_2301;
            let mut b0: u32 = 0xefcd_ab89;
            let mut c0: u32 = 0x98ba_dcfe;
            let mut d0: u32 = 0x1032_5476;

            for chunk in msg.chunks_exact(64) {
                let mut m = [0u32; 16];
                for (i, word) in chunk.chunks_exact(4).enumerate() {
                    m[i] = u32::from_le_bytes(word.try_into().unwrap());
                }
                let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
                for i in 0..64 {
                    let (f, g) = match i {
                        0..=15 => ((b & c) | (!b & d), i),
                        16..=31 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                        32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                        _ => (c ^ (b | !d), (7 * i) % 16),
                    };
                    let temp = d;
                    d = c;
                    c = b;
                    b = b.wrapping_add(
                        a.wrapping_add(f)
                            .wrapping_add(K[i])
                            .wrapping_add(m[g])
                            .rotate_left(S[i]),
                    );
                    a = temp;
                }
                a0 = a0.wrapping_add(a);
                b0 = b0.wrapping_add(b);
                c0 = c0.wrapping_add(c);
                d0 = d0.wrapping_add(d);
            }

            let mut out = [0u8; 16];
            out[0..4].copy_from_slice(&a0.to_le_bytes());
            out[4..8].copy_from_slice(&b0.to_le_bytes());
            out[8..12].copy_from_slice(&c0.to_le_bytes());
            out[12..16].copy_from_slice(&d0.to_le_bytes());
            out
        }
    }

    #[test]
    fn one_ring_addition() {
        let mut v = MembershipView::new(10).unwrap();
        v.ring_add(&ep("127.0.0.1", 123), &nid(1, 2)).unwrap();
        for k in 0..10 {
            let ring = v.get_ring(k).unwrap();
            assert_eq!(ring.len(), 1);
            assert_eq!(ring[0], ep("127.0.0.1", 123));
        }
    }

    #[test]
    fn multiple_ring_additions() {
        let mut v = MembershipView::new(10).unwrap();
        for i in 0..10 {
            v.ring_add(&ep("127.0.0.1", i), &nid(0, i64::from(i)))
                .unwrap();
        }
        for k in 0..10 {
            assert_eq!(v.get_ring(k).unwrap().len(), 10);
        }
    }

    #[test]
    fn ring_re_addition_rejected() {
        let mut v = MembershipView::new(10).unwrap();
        v.ring_add(&ep("127.0.0.1", 0), &nid(1, 2)).unwrap();
        let err = v.ring_add(&ep("127.0.0.1", 0), &nid(9, 9)).unwrap_err();
        assert!(matches!(err, Error::ProtocolRejected(_)));
    }

    #[test]
    fn ring_delete_unknown_rejected() {
        let mut v = MembershipView::new(10).unwrap();
        let err = v.ring_delete(&ep("127.0.0.1", 0)).unwrap_err();
        assert!(matches!(err, Error::ProtocolRejected(_)));
    }

    #[test]
    fn monitoring_relationship_single_node() {
        let mut v = MembershipView::new(10).unwrap();
        let n1 = ep("127.0.0.1", 1);
        v.ring_add(&n1, &nid(1, 2)).unwrap();
        assert_eq!(v.get_subjects_of(&n1).unwrap().len(), 0);
        assert_eq!(v.get_observers_of(&n1).unwrap().len(), 0);
        let err = v.get_observers_of(&ep("127.0.0.1", 2)).unwrap_err();
        assert!(matches!(err, Error::ProtocolRejected(_)));
    }

    #[test]
    fn monitoring_relationship_two_nodes() {
        let mut v = MembershipView::new(10).unwrap();
        let n1 = ep("127.0.0.1", 1);
        let n2 = ep("127.0.0.1", 2);
        v.ring_add(&n1, &nid(1, 1)).unwrap();
        v.ring_add(&n2, &nid(2, 2)).unwrap();
        let subj = v.get_subjects_of(&n1).unwrap();
        let obs = v.get_observers_of(&n1).unwrap();
        assert_eq!(subj.len(), 10);
        assert_eq!(obs.len(), 10);
        let subj_unique: HashSet<EndpointKey> = subj.iter().map(EndpointKey::from).collect();
        assert_eq!(subj_unique.len(), 1);
    }

    #[test]
    fn monitoring_relationship_multiple_nodes_invariant() {
        let mut v = MembershipView::new(10).unwrap();
        let mut list = Vec::new();
        for i in 0..50 {
            let endpoint = ep("127.0.0.1", i);
            list.push(endpoint.clone());
            v.ring_add(&endpoint, &nid(0, i64::from(i))).unwrap();
        }
        for endpoint in &list {
            assert_eq!(v.get_subjects_of(endpoint).unwrap().len(), 10);
            assert_eq!(v.get_observers_of(endpoint).unwrap().len(), 10);
        }
    }

    #[test]
    fn configuration_id_changes_on_add() {
        let mut v = MembershipView::new(10).unwrap();
        let mut seen = HashSet::new();
        for i in 0..50 {
            v.ring_add(&ep("127.0.0.1", i), &nid(0, i64::from(i)))
                .unwrap();
            seen.insert(v.current_configuration_id());
        }
        assert_eq!(seen.len(), 50);
    }

    #[test]
    fn ring_delete_then_add_restores_configuration_id() {
        let mut v = MembershipView::new(10).unwrap();
        for i in 0..20 {
            v.ring_add(&ep("127.0.0.1", i), &nid(0, i64::from(i)))
                .unwrap();
        }
        let before = v.current_configuration_id();
        let victim = ep("127.0.0.1", 7);
        v.ring_delete(&victim).unwrap();
        // The deleted identifier stays in `identifiersSeen` (Java parity),
        // so re-adding requires a fresh NodeId.
        v.ring_add(&victim, &nid(0, 99_999)).unwrap();
        let after = v.current_configuration_id();
        // Note: adding back changes identifiersSeen — but the endpoint set
        // restored, so config-id depends on identifiers as well. We assert
        // that the *endpoint* contribution is restored by recomputing via
        // a fresh view with the same endpoint set + same identifier set.
        let mut fresh = MembershipView::new(10).unwrap();
        for i in 0..20 {
            if i == 7 {
                fresh
                    .ring_add(&ep("127.0.0.1", 7), &nid(0, 99_999))
                    .unwrap();
            } else {
                fresh
                    .ring_add(&ep("127.0.0.1", i), &nid(0, i64::from(i)))
                    .unwrap();
            }
        }
        // Insert the original id=7 identifier into the fresh view's
        // identifier set to mirror the after-state of `v`.
        fresh.identifiers.insert(NodeIdKey(0, 7));
        fresh.identifier_objs.insert(NodeIdKey(0, 7), nid(0, 7));
        fresh.configuration_dirty = true;
        assert_eq!(fresh.current_configuration_id(), after);
        // Sanity: deleted + re-added gives a different ID than `before`
        // because identifiersSeen grew.
        assert_ne!(before, after);
    }

    #[test]
    fn java_golden_configuration_ids() {
        // From `tools/dump-vectors/ViewDump.java` output for n in {3, 10, 100}
        // with endpoints 127.0.0.1:10000..N and node-ids derived from
        // `UUID.nameUUIDFromBytes("127.0.0.1:port")`.
        #[allow(clippy::cast_possible_wrap)]
        let cases: &[(i32, i64)] = &[
            (3, 0xfdce_3a44_4324_4b8b_u64 as i64),
            (10, 0xb35f_0bab_18a9_f754_u64 as i64),
            (100, 0x0752_e470_bf97_187b_u64 as i64),
        ];
        for (n, expected) in cases {
            let mut v = MembershipView::new(10).unwrap();
            for i in 0..*n {
                let host = "127.0.0.1";
                let port = 10000 + i;
                let tag = format!("{host}:{port}");
                let id = name_uuid_from_bytes(tag.as_bytes());
                v.ring_add(&ep(host, port), &id).unwrap();
            }
            let got = v.current_configuration_id();
            assert_eq!(
                got,
                ConfigurationId(*expected),
                "config-id mismatch for n={n}"
            );
        }
    }

    #[test]
    fn java_golden_ring0_order() {
        // n=3 ring0 from ViewDump: 10002, 10000, 10001
        let mut v = MembershipView::new(10).unwrap();
        for port in [10000, 10001, 10002] {
            let tag = format!("127.0.0.1:{port}");
            let id = name_uuid_from_bytes(tag.as_bytes());
            v.ring_add(&ep("127.0.0.1", port), &id).unwrap();
        }
        let ring0 = v.get_ring(0).unwrap();
        let ports: Vec<i32> = ring0.iter().map(|e| e.port).collect();
        assert_eq!(ports, vec![10002, 10000, 10001]);
    }

    #[test]
    fn java_golden_observers_subjects_n3() {
        // From ViewDump for n=3, of=127.0.0.1:10000.
        // observers (per-ring): 10001×4, 10002×4, 10001, 10002 — but exact ports per ring
        // are: [10001, 10001, 10001, 10001, 10002, 10002, 10002, 10002, 10001, 10002]
        // subjects: [10002, 10002, 10002, 10002, 10001, 10001, 10001, 10001, 10002, 10001]
        let mut v = MembershipView::new(10).unwrap();
        for port in [10000, 10001, 10002] {
            let tag = format!("127.0.0.1:{port}");
            let id = name_uuid_from_bytes(tag.as_bytes());
            v.ring_add(&ep("127.0.0.1", port), &id).unwrap();
        }
        let probe = ep("127.0.0.1", 10000);
        let obs: Vec<i32> = v
            .get_observers_of(&probe)
            .unwrap()
            .iter()
            .map(|e| e.port)
            .collect();
        let subj: Vec<i32> = v
            .get_subjects_of(&probe)
            .unwrap()
            .iter()
            .map(|e| e.port)
            .collect();
        assert_eq!(
            obs,
            vec![10001, 10001, 10001, 10001, 10002, 10002, 10002, 10002, 10001, 10002]
        );
        assert_eq!(
            subj,
            vec![10002, 10002, 10002, 10002, 10001, 10001, 10001, 10001, 10002, 10001]
        );
    }

    #[test]
    fn safe_to_join_dispositions() {
        let mut v = MembershipView::new(10).unwrap();
        let n1 = ep("127.0.0.1", 1);
        let id1 = nid(1, 1);
        v.ring_add(&n1, &id1).unwrap();
        assert_eq!(
            v.is_safe_to_join(&n1, &nid(2, 2)),
            JoinDisposition::HostnameAlreadyInRing
        );
        assert_eq!(
            v.is_safe_to_join(&ep("127.0.0.1", 2), &id1),
            JoinDisposition::UuidAlreadyInRing
        );
        assert_eq!(
            v.is_safe_to_join(&ep("127.0.0.1", 2), &nid(2, 2)),
            JoinDisposition::SafeToJoin
        );
    }

    #[test]
    fn get_ring_numbers_returns_indices_where_subject_matches() {
        let mut v = MembershipView::new(10).unwrap();
        for port in [10000, 10001, 10002] {
            let tag = format!("127.0.0.1:{port}");
            let id = name_uuid_from_bytes(tag.as_bytes());
            v.ring_add(&ep("127.0.0.1", port), &id).unwrap();
        }
        // From the golden vectors: subjects of :10000 are
        // [10002, 10002, 10002, 10002, 10001, 10001, 10001, 10001, 10002, 10001]
        let observer = ep("127.0.0.1", 10000);
        let nums_for_10002 = v
            .get_ring_numbers(&observer, &ep("127.0.0.1", 10002))
            .unwrap();
        assert_eq!(nums_for_10002, vec![0, 1, 2, 3, 8]);
        let nums_for_10001 = v
            .get_ring_numbers(&observer, &ep("127.0.0.1", 10001))
            .unwrap();
        assert_eq!(nums_for_10001, vec![4, 5, 6, 7, 9]);
    }
}
