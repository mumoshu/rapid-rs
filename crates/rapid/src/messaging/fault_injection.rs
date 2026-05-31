//! Test-only fault injection for [`InProcessNetwork`](super::in_process::InProcessNetwork).
//!
//! Java parity references:
//! - `ServerDropInterceptors.FirstN`  (drop first N of a content kind)
//! - `ClientInterceptors.Delayer`     (block messages of a content kind
//!   on a `CountDownLatch`)
//!
//! Used by the multi-node `ClusterTest` ports (F11) and the parametric
//! `PaxosTests` ports (F12). Not enabled in production code paths —
//! `InProcessNetwork::set_interceptor` is the only entry point and is
//! never called outside `#[cfg(test)]` consumers.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::pb;

/// Per-envelope decision returned by an [`EnvelopeFilter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Deliver normally.
    Pass,
    /// Drop the envelope. The sender sees `Error::Transport`, just as
    /// it would for a gRPC `DEADLINE_EXCEEDED` over the wire.
    Drop,
    /// Sleep for `Duration`, then deliver.
    Delay(Duration),
}

/// Test-only interceptor on the in-process transport.
///
/// `filter` is invoked once per `client.send` / `client.send_best_effort`
/// before the envelope reaches the destination's inbox.
pub trait EnvelopeFilter: Send + Sync {
    /// Return the disposition for an outbound request bound for `dst`.
    fn filter(&self, dst: SocketAddr, req: &pb::RapidRequest) -> Disposition;
}

/// Discriminator over [`pb::rapid_request::Content`] variants. The
/// `as_kind` helper maps a request to one of these values; tests
/// match on the kind to scope drops to e.g. `JoinMessage` only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageKind {
    /// `pb::rapid_request::Content::PreJoinMessage`.
    PreJoin,
    /// `pb::rapid_request::Content::JoinMessage`.
    Join,
    /// `pb::rapid_request::Content::BatchedAlertMessage`.
    BatchedAlert,
    /// `pb::rapid_request::Content::ProbeMessage`.
    Probe,
    /// `pb::rapid_request::Content::LeaveMessage`.
    Leave,
    /// `pb::rapid_request::Content::FastRoundPhase2bMessage`.
    FastRoundPhase2b,
    /// `pb::rapid_request::Content::Phase1aMessage`.
    Phase1a,
    /// `pb::rapid_request::Content::Phase1bMessage`.
    Phase1b,
    /// `pb::rapid_request::Content::Phase2aMessage`.
    Phase2a,
    /// `pb::rapid_request::Content::Phase2bMessage`.
    Phase2b,
}

/// Map a request to its [`MessageKind`].
#[must_use]
pub fn as_kind(req: &pb::RapidRequest) -> Option<MessageKind> {
    Some(match req.content.as_ref()? {
        pb::rapid_request::Content::PreJoinMessage(_) => MessageKind::PreJoin,
        pb::rapid_request::Content::JoinMessage(_) => MessageKind::Join,
        pb::rapid_request::Content::BatchedAlertMessage(_) => MessageKind::BatchedAlert,
        pb::rapid_request::Content::ProbeMessage(_) => MessageKind::Probe,
        pb::rapid_request::Content::LeaveMessage(_) => MessageKind::Leave,
        pb::rapid_request::Content::FastRoundPhase2bMessage(_) => MessageKind::FastRoundPhase2b,
        pb::rapid_request::Content::Phase1aMessage(_) => MessageKind::Phase1a,
        pb::rapid_request::Content::Phase1bMessage(_) => MessageKind::Phase1b,
        pb::rapid_request::Content::Phase2aMessage(_) => MessageKind::Phase2a,
        pb::rapid_request::Content::Phase2bMessage(_) => MessageKind::Phase2b,
    })
}

/// Java parity: `ServerDropInterceptors.FirstN`. Drops the first `n`
/// envelopes matching `kind`. Once the counter reaches zero, every
/// subsequent envelope (of any kind) is passed.
pub struct FirstN {
    kind: MessageKind,
    remaining: AtomicU32,
}

impl FirstN {
    /// Construct a `FirstN` that drops the first `n` envelopes of
    /// `kind`.
    #[must_use]
    pub fn new(n: u32, kind: MessageKind) -> Self {
        Self {
            kind,
            remaining: AtomicU32::new(n),
        }
    }
}

impl EnvelopeFilter for FirstN {
    fn filter(&self, _dst: SocketAddr, req: &pb::RapidRequest) -> Disposition {
        if as_kind(req) != Some(self.kind) {
            return Disposition::Pass;
        }
        let prev = self.remaining.load(Ordering::Relaxed);
        if prev == 0 {
            return Disposition::Pass;
        }
        // Try to claim a drop slot.
        match self
            .remaining
            .compare_exchange(prev, prev - 1, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => Disposition::Drop,
            Err(_) => Disposition::Pass, // Lost the race; let it through.
        }
    }
}

/// Drop matching envelopes when they target *any* of the listed
/// destinations. Used by `injectAsymmetricDrops` and the random-failure
/// `ClusterTest` ports.
pub struct DropAtDests {
    kind: Option<MessageKind>,
    dests: HashSet<SocketAddr>,
    per_dest: AtomicU32,
    remaining: parking_lot::Mutex<std::collections::HashMap<SocketAddr, u32>>,
}

impl DropAtDests {
    /// Drop the first `per_dest` envelopes of `kind` (or every kind
    /// when `None`) for each destination in `dests`.
    #[must_use]
    pub fn new(per_dest: u32, kind: Option<MessageKind>, dests: HashSet<SocketAddr>) -> Self {
        Self {
            kind,
            dests,
            per_dest: AtomicU32::new(per_dest),
            remaining: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

impl EnvelopeFilter for DropAtDests {
    fn filter(&self, dst: SocketAddr, req: &pb::RapidRequest) -> Disposition {
        if !self.dests.contains(&dst) {
            return Disposition::Pass;
        }
        if let Some(k) = self.kind {
            if as_kind(req) != Some(k) {
                return Disposition::Pass;
            }
        }
        let mut map = self.remaining.lock();
        let entry = map
            .entry(dst)
            .or_insert_with(|| self.per_dest.load(Ordering::Relaxed));
        if *entry == 0 {
            return Disposition::Pass;
        }
        *entry -= 1;
        Disposition::Drop
    }
}

/// Composition of multiple filters. Returns the first non-`Pass`
/// disposition from the chain (in registration order).
pub struct Chain {
    filters: Vec<Arc<dyn EnvelopeFilter>>,
}

impl Chain {
    /// Construct an empty chain.
    #[must_use]
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
        }
    }

    /// Append `f` to the chain.
    #[must_use]
    pub fn then(mut self, f: Arc<dyn EnvelopeFilter>) -> Self {
        self.filters.push(f);
        self
    }
}

impl Default for Chain {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvelopeFilter for Chain {
    fn filter(&self, dst: SocketAddr, req: &pb::RapidRequest) -> Disposition {
        for f in &self.filters {
            match f.filter(dst, req) {
                Disposition::Pass => {}
                other => return other,
            }
        }
        Disposition::Pass
    }
}
