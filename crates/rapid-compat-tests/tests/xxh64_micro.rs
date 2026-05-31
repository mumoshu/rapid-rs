//! Phase 0 xxh64 micro-conformance: assert `xxhash_rust::xxh64::xxh64` matches
//! the values dumped by `tools/dump-vectors/Micro.java`
//! (`LongHashFunction.xx(seed).hashBytes(...)`).
//!
//! Mismatch here means observer/subject mappings between Rust and Java would
//! diverge — the entire compat surface is downstream of this check.

use xxhash_rust::xxh64::xxh64;

#[test]
fn xx64_matches_java_long_hash_function_xx() {
    let cases: &[(u64, &[u8], u64)] = &[
        (0, b"hello", 0x26c7_827d_889f_6da3),
        (1, b"hello", 0x23dd_71cb_04d0_a1b2),
        (0, b"127.0.0.1:1234", 0x8df8_67c9_789d_b1c1),
        (1, b"127.0.0.1:1234", 0xc096_b8f2_e42d_05f3),
        (2, b"127.0.0.1:1234", 0x049a_afa1_c533_4db1),
        (3, b"127.0.0.1:1234", 0x51e7_b31a_fb6d_4193),
    ];
    for (seed, input, expected) in cases {
        let got = xxh64(input, *seed);
        assert_eq!(
            got,
            *expected,
            "mismatch for seed={seed} input={:?}: expected 0x{:016x}, got 0x{:016x}",
            std::str::from_utf8(input).unwrap_or("<binary>"),
            expected,
            got
        );
    }
}
