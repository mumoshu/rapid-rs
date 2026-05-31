//! Shared helpers for rapid parity, replay, and interop tests.
//!
//! Public modules:
//! - [`ndjson`] — reader/writer for the NDJSON wire-trace format emitted
//!   by the Java-side `NdjsonTraceWriter` patch in
//!   `references/rapid-java/.../GrpcServer.java`. The format matches
//!   `PLAN.md` § *Replay trace format*:
//!   `{"ts_ms": u64, "dst": "host:port", "req_b64": "..."}` per line.

pub use rapid as upstream;

pub mod ndjson;
