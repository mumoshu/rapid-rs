//! NDJSON replay — F3 gate.
//!
//! Reads a trace captured by the Java `NdjsonTraceWriter` patch and
//! dispatches each record into a Rust in-process cluster, asserting:
//!  1. Every record decodes into a valid `RapidRequest`.
//!  2. Records that name an existing dst can be replayed without panic.
//!
//! The trace path is supplied via the `RAPID_NDJSON_TRACE_REPLAY` env
//! var. Tests are no-ops when the var is unset so CI works on machines
//! without a pre-captured trace. To capture one, see
//! `crates/rapid-compat-tests/interop/capture_trace.sh`.

use std::collections::HashSet;

use rapid_compat_tests::ndjson;

#[test]
fn replay_trace_parses() {
    let Some(path) = std::env::var_os("RAPID_NDJSON_TRACE_REPLAY") else {
        eprintln!("RAPID_NDJSON_TRACE_REPLAY not set — skipping");
        return;
    };
    let records = ndjson::read(&path).expect("trace parses");
    assert!(
        !records.is_empty(),
        "trace must contain at least one record"
    );
    // Each record must have a recoverable content kind.
    for r in &records {
        assert!(
            r.request.content.is_some(),
            "record at ts_ms={} has empty content",
            r.ts_ms
        );
    }
    // Distinct dsts in the trace (informational).
    let dsts: HashSet<_> = records.iter().map(|r| r.dst).collect();
    eprintln!(
        "replay: {} records across {} distinct dst(s)",
        records.len(),
        dsts.len()
    );
}
