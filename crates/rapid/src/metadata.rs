//! Per-node application metadata.
//!
//! Bit-exact port of `references/rapid-java/.../MetadataManager.java`.
//! Java uses a `ConcurrentHashMap` because the manager is read from
//! background threads; the Rust port lives inside the membership-service
//! actor and is therefore single-owner.

use std::collections::HashMap;

use crate::pb;

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

/// `MetadataManager` — application-level tags keyed by endpoint.
#[derive(Debug, Default)]
pub struct MetadataManager {
    role_map: HashMap<EndpointKey, pb::Metadata>,
    endpoint_objs: HashMap<EndpointKey, pb::Endpoint>,
}

impl MetadataManager {
    /// Empty manager.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the metadata for `node`, or an empty `Metadata` if absent
    /// (matches Java `MetadataManager.get` which returns
    /// `Metadata.getDefaultInstance()` on miss).
    #[must_use]
    pub fn get(&self, node: &pb::Endpoint) -> pb::Metadata {
        self.role_map
            .get(&EndpointKey::from(node))
            .cloned()
            .unwrap_or_default()
    }

    /// `putIfAbsent` semantics — does not overwrite existing entries.
    pub fn add(&mut self, entries: impl IntoIterator<Item = (pb::Endpoint, pb::Metadata)>) {
        for (endpoint, meta) in entries {
            let key = EndpointKey::from(&endpoint);
            self.role_map.entry(key.clone()).or_insert(meta);
            self.endpoint_objs.entry(key).or_insert(endpoint);
        }
    }

    /// Drop all metadata for `node`.
    pub fn remove(&mut self, node: &pb::Endpoint) {
        let key = EndpointKey::from(node);
        self.role_map.remove(&key);
        self.endpoint_objs.remove(&key);
    }

    /// Snapshot of all stored `(endpoint, metadata)` pairs. The Java
    /// counterpart returns the underlying `ConcurrentHashMap` directly —
    /// we materialise a `Vec<_>` so callers can't accidentally mutate the
    /// actor's state.
    #[must_use]
    pub fn all(&self) -> Vec<(pb::Endpoint, pb::Metadata)> {
        self.role_map
            .iter()
            .map(|(k, v)| {
                let ep = self.endpoint_objs.get(k).cloned().unwrap_or_default();
                (ep, v.clone())
            })
            .collect()
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

    fn meta(role: &str) -> pb::Metadata {
        let mut m = pb::Metadata::default();
        m.metadata.insert("role".into(), role.as_bytes().to_vec());
        m
    }

    #[test]
    fn put_if_absent_does_not_overwrite() {
        let mut mm = MetadataManager::new();
        mm.add([(ep("a", 1), meta("web"))]);
        mm.add([(ep("a", 1), meta("db"))]);
        let got = mm.get(&ep("a", 1));
        assert_eq!(
            got.metadata.get("role").map(std::vec::Vec::as_slice),
            Some(b"web".as_slice())
        );
    }

    #[test]
    fn missing_returns_default() {
        let mm = MetadataManager::new();
        assert!(mm.get(&ep("x", 1)).metadata.is_empty());
    }

    #[test]
    fn remove_drops_entry() {
        let mut mm = MetadataManager::new();
        mm.add([(ep("a", 1), meta("web"))]);
        mm.remove(&ep("a", 1));
        assert!(mm.get(&ep("a", 1)).metadata.is_empty());
        assert!(mm.all().is_empty());
    }
}
