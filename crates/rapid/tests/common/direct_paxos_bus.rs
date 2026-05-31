//! `DirectPaxosBus` — in-memory message bus for `FastPaxos` tests.
//!
//! Java parity: `LinkedDirectMessenger` + `DirectBroadcaster` in
//! `PaxosTests.java`. Owns N `FastPaxos` instances and a
//! `VecDeque<Pending>` queue. Each `propose` / `start_classic_round`
//! / `inject` call BFS-drains the queue until quiescence.
//!
//! Drop / delay are not modelled per-message at this layer — for tests
//! that need it, use [`InProcessNetwork`] with an
//! [`crate::messaging::fault_injection::EnvelopeFilter`].
//!
//! Reentrancy: dispatch must not reentrantly borrow the same
//! `Vec<FastPaxos>` (a previous inline version of this harness did
//! exactly that and deadlocked). We use index-based access plus a
//! pending queue rather than `mem::take`.

use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use rapid::consensus::fast_paxos::FastOutgoing;
use rapid::consensus::FastPaxos;
use rapid::pb;

/// Discriminator for selective message drops.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum MsgKind {
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

/// Internal pending-delivery item.
enum Pending {
    Broadcast { req: pb::RapidRequest },
    Unicast { dst: usize, req: pb::RapidRequest },
}

/// N `FastPaxos` instances + a direct in-memory delivery queue.
pub struct DirectPaxosBus {
    instances: Vec<FastPaxos>,
    endpoints: Vec<pb::Endpoint>,
    dropped: HashSet<MsgKind>,
    decisions: Vec<Option<Vec<pb::Endpoint>>>,
    pending: VecDeque<Pending>,
}

/// Synthesise an endpoint.
pub fn ep(host: &str, port: i32) -> pb::Endpoint {
    pb::Endpoint {
        hostname: host.as_bytes().to_vec(),
        port,
    }
}

impl DirectPaxosBus {
    /// Spawn `n` instances bound to `127.0.0.1:(5000 + i)`. Each
    /// `FastPaxos` uses an absurd fallback delay so the per-instance
    /// classic-round timer never fires; tests drive
    /// `start_classic_round` explicitly.
    #[must_use]
    pub fn new(n: usize, configuration_id: i64) -> Self {
        let endpoints: Vec<pb::Endpoint> = (0..n)
            .map(|i| ep("127.0.0.1", 5000 + i32::try_from(i).unwrap()))
            .collect();
        let instances: Vec<FastPaxos> = endpoints
            .iter()
            .map(|e| FastPaxos::new(e.clone(), configuration_id, n, Duration::from_secs(1_000)))
            .collect();
        Self {
            instances,
            endpoints,
            dropped: HashSet::new(),
            decisions: vec![None; n],
            pending: VecDeque::new(),
        }
    }

    /// Spawn `n` instances with a caller-supplied endpoint slice.
    /// Useful when test data tables identify acceptors by name.
    ///
    /// # Panics
    ///
    /// Panics if `endpoints.len() != n`.
    #[must_use]
    pub fn with_endpoints(endpoints: Vec<pb::Endpoint>, configuration_id: i64) -> Self {
        let n = endpoints.len();
        let instances: Vec<FastPaxos> = endpoints
            .iter()
            .map(|e| FastPaxos::new(e.clone(), configuration_id, n, Duration::from_secs(1_000)))
            .collect();
        Self {
            instances,
            endpoints,
            dropped: HashSet::new(),
            decisions: vec![None; n],
            pending: VecDeque::new(),
        }
    }

    /// Drop every message of `kind` going forward.
    pub fn drop_kind(&mut self, kind: MsgKind) {
        self.dropped.insert(kind);
    }

    /// Borrow the endpoint at index `idx`.
    #[must_use]
    pub fn endpoint(&self, idx: usize) -> &pb::Endpoint {
        &self.endpoints[idx]
    }

    /// Borrow the full endpoint slice.
    #[must_use]
    pub fn endpoints(&self) -> &[pb::Endpoint] {
        &self.endpoints
    }

    /// Snapshot the decision recorded by instance `idx`, if any.
    #[must_use]
    pub fn decision(&self, idx: usize) -> Option<&Vec<pb::Endpoint>> {
        self.decisions[idx].as_ref()
    }

    /// Convenience: have every instance agreed on the same value?
    #[must_use]
    pub fn all_decided_same(&self, expected: &[pb::Endpoint]) -> bool {
        self.decisions
            .iter()
            .all(|d| d.as_deref() == Some(expected))
    }

    /// Number of instances.
    #[must_use]
    pub fn len(&self) -> usize {
        self.instances.len()
    }

    /// Whether the bus has zero instances.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.instances.is_empty()
    }

    /// Invoke `propose(proposal)` on instance `idx`, then drain the
    /// resulting message storm until quiescence.
    pub fn propose(&mut self, idx: usize, proposal: Vec<pb::Endpoint>) {
        let outs = self.instances[idx].propose(proposal);
        self.enqueue(idx, outs);
        self.drain();
    }

    /// Invoke `start_classic_round()` on instance `idx`, then drain.
    pub fn start_classic_round(&mut self, idx: usize) {
        let outs = self.instances[idx].start_classic_round();
        self.enqueue(idx, outs);
        self.drain();
    }

    /// Inject a synthetic message at instance `idx` (the receiver),
    /// then drain. Useful for parametric tests that need to hand-shape
    /// the Phase-1b vote distribution.
    pub fn inject(&mut self, idx: usize, req: &pb::RapidRequest) {
        let outs = self.instances[idx].handle_consensus_message(req);
        self.enqueue(idx, outs);
        self.drain();
    }

    fn enqueue(&mut self, idx: usize, outs: Vec<FastOutgoing>) {
        for o in outs {
            match o {
                FastOutgoing::Broadcast(req) => self.pending.push_back(Pending::Broadcast { req }),
                FastOutgoing::Unicast { target, request } => {
                    if let Some(dst) = self.endpoints.iter().position(|e| e == &target) {
                        self.pending
                            .push_back(Pending::Unicast { dst, req: request });
                    }
                }
                FastOutgoing::ScheduleClassicFallback(_) => {}
                FastOutgoing::Decision(d) => {
                    self.decisions[idx] = Some(d);
                }
            }
        }
    }

    fn drain(&mut self) {
        while let Some(item) = self.pending.pop_front() {
            match item {
                Pending::Broadcast { req } => {
                    if classify(&req).is_some_and(|k| self.dropped.contains(&k)) {
                        continue;
                    }
                    for i in 0..self.instances.len() {
                        let outs = self.instances[i].handle_consensus_message(&req);
                        self.enqueue(i, outs);
                    }
                }
                Pending::Unicast { dst, req } => {
                    if classify(&req).is_some_and(|k| self.dropped.contains(&k)) {
                        continue;
                    }
                    let outs = self.instances[dst].handle_consensus_message(&req);
                    self.enqueue(dst, outs);
                }
            }
        }
    }
}

/// Classify a `RapidRequest` by content kind for drop matching.
#[must_use]
pub fn classify(req: &pb::RapidRequest) -> Option<MsgKind> {
    match req.content.as_ref()? {
        pb::rapid_request::Content::FastRoundPhase2bMessage(_) => Some(MsgKind::FastRoundPhase2b),
        pb::rapid_request::Content::Phase1aMessage(_) => Some(MsgKind::Phase1a),
        pb::rapid_request::Content::Phase1bMessage(_) => Some(MsgKind::Phase1b),
        pb::rapid_request::Content::Phase2aMessage(_) => Some(MsgKind::Phase2a),
        pb::rapid_request::Content::Phase2bMessage(_) => Some(MsgKind::Phase2b),
        _ => None,
    }
}
