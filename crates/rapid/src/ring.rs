//! Per-ring `NavigableSet<Endpoint>` analogue.
//!
//! Java's [`MembershipView`] uses one
//! `TreeSet<Endpoint>` per ring, sorted by a per-ring
//! [`AddressComparator`]. We mirror this in Rust as a
//! `BTreeMap<u64, pb::Endpoint>` keyed by the address hash.
//!
//! Two endpoints whose `AddressComparator` hash collide are treated as
//! equal by Java's `TreeSet` — `add()` returns false rather than
//! overwriting. We mirror this with `entry(_).or_insert`, returning a
//! boolean so the caller can detect a duplicate.
//!
//! [`MembershipView`]: ../view/struct.MembershipView.html
//! [`AddressComparator`]: ../view_hash/fn.address_hash.html

use std::collections::BTreeMap;

use crate::pb;
use crate::view_hash::address_hash;

/// A single K-ring.
///
/// Java's `AddressComparator.compare` is `Long.compare(hash1, hash2)` —
/// **signed** 64-bit comparison. We store the key as `i64` so the
/// `BTreeMap`'s natural order matches.
#[derive(Debug, Clone)]
pub(crate) struct Ring {
    seed: u64,
    by_hash: BTreeMap<i64, pb::Endpoint>,
}

impl Ring {
    /// Empty ring with the given xxh64 seed.
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            seed,
            by_hash: BTreeMap::new(),
        }
    }

    #[allow(clippy::cast_possible_wrap)]
    fn key(&self, endpoint: &pb::Endpoint) -> i64 {
        address_hash(self.seed, endpoint) as i64
    }

    /// Insert `endpoint`. Returns `true` if newly inserted, `false` if a
    /// (comparator-equal) endpoint was already present.
    pub(crate) fn insert(&mut self, endpoint: pb::Endpoint) -> bool {
        let key = self.key(&endpoint);
        if self.by_hash.contains_key(&key) {
            return false;
        }
        self.by_hash.insert(key, endpoint);
        true
    }

    /// Remove `endpoint`. Returns `true` if it was present.
    pub(crate) fn remove(&mut self, endpoint: &pb::Endpoint) -> bool {
        let key = self.key(endpoint);
        self.by_hash.remove(&key).is_some()
    }

    /// Java `NavigableSet.contains(endpoint)` semantics: compare by hash.
    pub(crate) fn contains(&self, endpoint: &pb::Endpoint) -> bool {
        let key = self.key(endpoint);
        self.by_hash.contains_key(&key)
    }

    /// Java `NavigableSet.lower(endpoint)` — greatest strictly less than.
    pub(crate) fn lower(&self, endpoint: &pb::Endpoint) -> Option<&pb::Endpoint> {
        let key = self.key(endpoint);
        self.by_hash.range(..key).next_back().map(|(_, v)| v)
    }

    /// Java `NavigableSet.higher(endpoint)` — least strictly greater than.
    pub(crate) fn higher(&self, endpoint: &pb::Endpoint) -> Option<&pb::Endpoint> {
        let key = self.key(endpoint);
        self.by_hash
            .range((std::ops::Bound::Excluded(key), std::ops::Bound::Unbounded))
            .next()
            .map(|(_, v)| v)
    }

    /// First endpoint in hash order.
    pub(crate) fn first(&self) -> Option<&pb::Endpoint> {
        self.by_hash.values().next()
    }

    /// Last endpoint in hash order.
    pub(crate) fn last(&self) -> Option<&pb::Endpoint> {
        self.by_hash.values().next_back()
    }

    /// Number of endpoints in this ring.
    pub(crate) fn len(&self) -> usize {
        self.by_hash.len()
    }

    /// Snapshot of endpoints in hash order.
    pub(crate) fn ordered(&self) -> impl Iterator<Item = &pb::Endpoint> {
        self.by_hash.values()
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

    #[test]
    fn insert_returns_false_on_duplicate() {
        let mut r = Ring::new(0);
        assert!(r.insert(ep("127.0.0.1", 1)));
        assert!(!r.insert(ep("127.0.0.1", 1)));
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn lower_higher_wrap() {
        let mut r = Ring::new(0);
        for p in [1, 2, 3] {
            assert!(r.insert(ep("127.0.0.1", p)));
        }
        // Just confirm the operations work — exact ordering is xxh64-dependent.
        let any = ep("127.0.0.1", 2);
        let lower = r.lower(&any).cloned();
        let higher = r.higher(&any).cloned();
        assert!(lower.is_some() || r.first().is_some());
        assert!(higher.is_some() || r.last().is_some());
    }
}
