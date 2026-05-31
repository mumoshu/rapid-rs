//! Protocol-message traits.
//!
//! Each logical proto message gets a Rust trait describing the fields the
//! core algorithms (cut detector, paxos, view) consume. Default impls are
//! the prost-generated structs. The core consumes `impl Trait` so users may
//! supply alternative wire encodings or extended types.
//!
//! At Phase 0 the trait set is intentionally a stub: just enough to prove
//! the layering works (`ProbeRequest`, `ProbeReply`). Phase 1+ extends the
//! set.

use crate::pb;

/// Read-only view of a `ProbeMessage`.
pub trait ProbeRequest {
    /// The endpoint that issued the probe.
    fn sender(&self) -> Option<&pb::Endpoint>;
    /// Optional opaque payload echoed back by some failure detectors.
    fn payload(&self) -> &[Vec<u8>];
}

/// Read-only view of a `ProbeResponse`.
pub trait ProbeReply {
    /// Reported status (OK or BOOTSTRAPPING).
    fn status(&self) -> pb::NodeStatus;
}

impl ProbeRequest for pb::ProbeMessage {
    fn sender(&self) -> Option<&pb::Endpoint> {
        self.sender.as_ref()
    }

    fn payload(&self) -> &[Vec<u8>] {
        &self.payload
    }
}

impl ProbeReply for pb::ProbeResponse {
    fn status(&self) -> pb::NodeStatus {
        // i32 → enum; protocol invariant: peer must emit a defined variant.
        pb::NodeStatus::try_from(self.status).unwrap_or(pb::NodeStatus::Ok)
    }
}

/// Build a `RapidRequest` wrapping a `ProbeMessage`.
#[must_use]
pub fn probe_request(msg: pb::ProbeMessage) -> pb::RapidRequest {
    pb::RapidRequest {
        content: Some(pb::rapid_request::Content::ProbeMessage(msg)),
    }
}

/// Build a `RapidResponse` wrapping a `ProbeResponse`.
#[must_use]
pub fn probe_response(msg: pb::ProbeResponse) -> pb::RapidResponse {
    pb::RapidResponse {
        content: Some(pb::rapid_response::Content::ProbeResponse(msg)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_request_round_trip() {
        let probe = pb::ProbeMessage {
            sender: Some(pb::Endpoint {
                hostname: b"127.0.0.1".to_vec(),
                port: 1234,
            }),
            payload: vec![b"hello".to_vec()],
        };
        let req = probe_request(probe.clone());
        let pb::rapid_request::Content::ProbeMessage(p) = req.content.unwrap() else {
            panic!("expected ProbeMessage variant");
        };
        assert_eq!(p.sender(), probe.sender.as_ref());
        assert_eq!(p.payload().len(), 1);
    }
}
