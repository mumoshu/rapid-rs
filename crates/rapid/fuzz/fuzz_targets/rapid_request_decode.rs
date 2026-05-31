#![no_main]
//! Fuzz target — `RapidRequest::decode` must not panic on arbitrary bytes.
//!
//! Run with:
//!   cargo fuzz run rapid_request_decode -- -max_total_time=1800
//! (30-minute CI budget per F4 plan.)
//!
//! Seed corpus: prepopulate `fuzz/corpus/rapid_request_decode/` with
//! captured Java wire dumps from
//! `crates/rapid-compat-tests/vectors/wire/` and the
//! `RAPID_NDJSON_TRACE` NDJSON traces (each base64 line decoded).

use libfuzzer_sys::fuzz_target;
use prost::Message;
use rapid::pb;

fuzz_target!(|data: &[u8]| {
    // Decode-only contract: must never panic. Any error is fine.
    let _ = pb::RapidRequest::decode(data);
});
