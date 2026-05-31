//! Hash primitives backing `MembershipView` (`AddressComparator` ring sort
//! key + `Configuration.getConfigurationId` long).
//!
//! Bit-exact port of `MembershipView.java`:
//! - `AddressComparator.computeHash` —
//!   `xxh64(host_bytes, seed) * 31 + xxh64(port_le_bytes, seed)` in Java's
//!   signed `long` arithmetic (two's-complement wrap on overflow).
//! - `Configuration.getConfigurationId` —
//!   `hash = 1; for nodeId: hash = hash*37 + xxh64(le8(high), 0); same for low;
//!    for endpoint: hash = hash*37 + xxh64(host_bytes, 0); hash = hash*37 + xxh64(le4(port), 0)`.
//!
//! `zero-allocation-hashing`'s `hashInt(int)` / `hashLong(long)` hash the
//! native little-endian bytes of the integer, which we mirror with
//! `i32::to_le_bytes` / `i64::to_le_bytes`.
//!
//! Golden vectors lifted from `tools/dump-vectors/ViewDump.java` cover both
//! primitives and the composite formulas.

use xxhash_rust::xxh64::xxh64;

use crate::pb;
use crate::types::ConfigurationId;

/// Java `LongHashFunction.hashLong(value)` — xxh64 over the 8 little-endian
/// bytes of `value`.
#[must_use]
pub fn xxh_long(seed: u64, value: i64) -> u64 {
    xxh64(&value.to_le_bytes(), seed)
}

/// Java `LongHashFunction.hashInt(value)` — xxh64 over the 4 little-endian
/// bytes of `value`.
#[must_use]
pub fn xxh_int(seed: u64, value: i32) -> u64 {
    xxh64(&value.to_le_bytes(), seed)
}

/// `AddressComparator.computeHash` — `xxh64(host_bytes) * 31 + xxh64.hashInt(port)`.
///
/// Java's `long` is signed and wraps on overflow; Rust's `u64`
/// `wrapping_mul` / `wrapping_add` give us the same bit pattern.
#[must_use]
pub fn address_hash(seed: u64, endpoint: &pb::Endpoint) -> u64 {
    let host_hash = xxh64(&endpoint.hostname, seed);
    let port_hash = xxh_int(seed, endpoint.port);
    host_hash.wrapping_mul(31).wrapping_add(port_hash)
}

/// `MembershipView.getRingZeroComparator()` key — sort-by helper.
#[must_use]
pub fn address_hash_seed_zero(endpoint: &pb::Endpoint) -> u64 {
    address_hash(0, endpoint)
}

/// `MembershipView.Configuration.getConfigurationId(identifiers, endpoints)`.
///
/// Identifiers are iterated in the order supplied (the Java caller pulls
/// from a `TreeSet<NodeId>` sorted by `(high, low)`). Endpoints are
/// iterated in the order of `rings.get(0)`, which is `AddressComparator(0)`
/// order.
#[must_use]
pub fn configuration_id_for<'a, I, E>(identifiers: I, endpoints: E) -> ConfigurationId
where
    I: IntoIterator<Item = &'a pb::NodeId>,
    E: IntoIterator<Item = &'a pb::Endpoint>,
{
    let mut hash: u64 = 1;
    for id in identifiers {
        hash = hash.wrapping_mul(37).wrapping_add(xxh_long(0, id.high));
        hash = hash.wrapping_mul(37).wrapping_add(xxh_long(0, id.low));
    }
    for endpoint in endpoints {
        hash = hash
            .wrapping_mul(37)
            .wrapping_add(xxh64(&endpoint.hostname, 0));
        hash = hash
            .wrapping_mul(37)
            .wrapping_add(xxh_int(0, endpoint.port));
    }
    #[allow(clippy::cast_possible_wrap)]
    ConfigurationId(hash as i64)
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
    fn hash_int_matches_java_vectors() {
        assert_eq!(xxh_int(0, 0), 0x3aef_a6fd_5cf2_deb4);
        assert_eq!(xxh_int(0, 1), 0xf42f_9400_1fcb_5351);
        assert_eq!(xxh_int(0, 1234), 0x2757_72fe_cb91_8454);
        assert_eq!(xxh_int(0, -1), 0x7f78_e4bd_a3ad_df93);
        assert_eq!(xxh_int(1, 0), 0x2db1_1370_7a70_2ac0);
        assert_eq!(xxh_int(1, 1234), 0x1de7_3cf8_2ae9_17de);
    }

    #[test]
    fn hash_long_matches_java_vectors() {
        assert_eq!(xxh_long(0, 0), 0x34c9_6acd_cadb_1bbb);
        assert_eq!(xxh_long(0, 1), 0x9f29_cb17_a2a4_9995);
        assert_eq!(xxh_long(0, 0x1122_3344_5566_7788), 0x7d38_3ebd_f158_0e8c);
        assert_eq!(xxh_long(0, -1), 0x85d1_36ad_b773_c6c9);
        assert_eq!(xxh_long(1, 0), 0x22c7_6afd_15f0_110f);
        assert_eq!(xxh_long(1, -1), 0x0ad7_f612_8987_5125);
    }

    #[test]
    fn address_hash_matches_java_vectors() {
        let cases: &[(u64, &str, i32, u64)] = &[
            (0, "127.0.0.1", 1234, 0x782f_0e72_d8e2_c18d),
            (0, "127.0.0.1", 1, 0x4507_2f74_2d1c_908a),
            (0, "127.0.0.2", 2, 0x9355_8fc1_aa22_654b),
            (0, "10.0.0.5", 4444, 0x1a38_6b80_5917_4c2b),
            (
                0,
                "a-very-long-hostname.example.com",
                65535,
                0x7263_015a_0068_8edd,
            ),
            (1, "127.0.0.1", 1234, 0xf172_e67b_67e1_c412),
            (3, "127.0.0.1", 1234, 0xfabb_b634_9f29_35d6),
        ];
        for (seed, host, port, expected) in cases {
            let got = address_hash(*seed, &ep(host, *port));
            assert_eq!(
                got, *expected,
                "addr seed={seed} {host}:{port}: expected 0x{expected:016x}, got 0x{got:016x}",
            );
        }
    }
}
