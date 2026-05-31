//! Fast-Paxos wrapper.
//!
//! Bit-exact port of `references/rapid-java/.../FastPaxos.java`. Single
//! decree, always starts with a fast round, falls back to classic Paxos
//! after a delay if quorum isn't reached.
//!
//! Same state-machine pattern as `paxos.rs`: methods return outgoing
//! messages or decisions; the caller (`MembershipService` actor) executes
//! the side-effects.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::consensus::paxos::{Paxos, PaxosOutgoing};
use crate::pb;

/// One outbound effect produced by a `FastPaxos` transition.
pub enum FastOutgoing {
    /// Broadcast to all current acceptors.
    Broadcast(pb::RapidRequest),
    /// Unicast to a specific acceptor (from inner classic Paxos).
    Unicast {
        /// Where to send.
        target: pb::Endpoint,
        /// Payload.
        request: pb::RapidRequest,
    },
    /// Schedule a classic-Paxos fallback after `delay`.
    ScheduleClassicFallback(Duration),
    /// Final decision. Wraps the inner Paxos decision.
    Decision(Vec<pb::Endpoint>),
}

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

/// Fast Paxos state machine.
pub struct FastPaxos {
    my_addr: pb::Endpoint,
    configuration_id: i64,
    membership_size: usize,
    paxos: Paxos,
    votes_received: HashSet<EndpointKey>,
    votes_per_proposal: HashMap<Vec<EndpointKey>, usize>,
    proposal_lookup: HashMap<Vec<EndpointKey>, Vec<pb::Endpoint>>,
    decided: bool,
    fallback_base_delay: Duration,
    /// Whether `start_classic_round` has already fired for this
    /// instance. Without this latch, stale `StartClassicRound`
    /// commands queued by an earlier instance's fallback timer would
    /// repeatedly trigger Phase1a broadcasts after a view change.
    classic_started: bool,
}

impl FastPaxos {
    /// Construct a fresh Fast Paxos instance bound to a configuration.
    #[must_use]
    pub fn new(
        my_addr: pb::Endpoint,
        configuration_id: i64,
        membership_size: usize,
        fallback_base_delay: Duration,
    ) -> Self {
        Self {
            my_addr: my_addr.clone(),
            configuration_id,
            membership_size,
            paxos: Paxos::new(my_addr, configuration_id, membership_size),
            votes_received: HashSet::new(),
            votes_per_proposal: HashMap::new(),
            proposal_lookup: HashMap::new(),
            decided: false,
            fallback_base_delay,
            classic_started: false,
        }
    }

    /// Whether a decision has been recorded locally.
    #[must_use]
    pub fn decided(&self) -> bool {
        self.decided
    }

    /// Propose a value for a fast round. Returns: a broadcast of the
    /// fast-round vote, plus a request to schedule the classic fallback.
    pub fn propose(&mut self, proposal: Vec<pb::Endpoint>) -> Vec<FastOutgoing> {
        self.paxos.register_fast_round_vote(proposal.clone());
        let msg = pb::FastRoundPhase2bMessage {
            configuration_id: self.configuration_id,
            endpoints: proposal,
            sender: Some(self.my_addr.clone()),
        };
        let req = pb::RapidRequest {
            content: Some(pb::rapid_request::Content::FastRoundPhase2bMessage(msg)),
        };
        vec![
            FastOutgoing::Broadcast(req),
            FastOutgoing::ScheduleClassicFallback(self.random_delay()),
        ]
    }

    /// Java `FastPaxos.getRandomDelayMs` — expovariate jitter over a
    /// per-instance rate of `1/N` plus `fallback_base_delay`. Each
    /// proposer schedules its classic-Paxos fallback at a different
    /// time, which keeps a proposal storm from triggering N parallel
    /// Phase-1a broadcasts.
    fn random_delay(&self) -> Duration {
        #[allow(clippy::cast_precision_loss)]
        let rate = 1.0_f64 / (self.membership_size.max(1) as f64);
        // `1.0 - rand` ∈ (0, 1], so ln(...) is in (-∞, 0]. Multiplied
        // by -1000 yields a non-negative jitter in milliseconds.
        let u: f64 = rand::random::<f64>();
        let one_minus_u = (1.0 - u).max(f64::MIN_POSITIVE);
        let jitter_ms = (-1000.0 * one_minus_u.ln() / rate).round();
        let jitter = if jitter_ms.is_finite() && jitter_ms >= 0.0 {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            Duration::from_millis(jitter_ms as u64)
        } else {
            Duration::ZERO
        };
        self.fallback_base_delay + jitter
    }

    /// Handle an inbound `FastRoundPhase2bMessage`.
    pub fn handle_fast_round_proposal(
        &mut self,
        msg: &pb::FastRoundPhase2bMessage,
    ) -> Vec<FastOutgoing> {
        if msg.configuration_id != self.configuration_id || self.decided {
            return Vec::new();
        }
        let Some(sender) = msg.sender.as_ref() else {
            return Vec::new();
        };
        let sender_key = EndpointKey::from(sender);
        if !self.votes_received.insert(sender_key) {
            return Vec::new();
        }
        let key: Vec<EndpointKey> = msg.endpoints.iter().map(EndpointKey::from).collect();
        self.proposal_lookup
            .entry(key.clone())
            .or_insert_with(|| msg.endpoints.clone());
        let count = self.votes_per_proposal.entry(key.clone()).or_insert(0);
        *count += 1;
        let count = *count;
        let f = (self.membership_size.saturating_sub(1)) / 4;
        tracing::debug!(target: "rapid", count, "paxos.fast.vote_received");
        if self.votes_received.len() >= self.membership_size - f
            && count >= self.membership_size - f
        {
            tracing::info!(target: "rapid", size = msg.endpoints.len(), "paxos.fast.decided");
            self.decided = true;
            return vec![FastOutgoing::Decision(msg.endpoints.clone())];
        }
        // Fast round may not succeed — classic fallback will run on the
        // scheduled timer (already armed by `propose`).
        Vec::new()
    }

    /// Trigger the classic round (Java `startClassicPaxosRound`).
    /// Idempotent per instance — subsequent calls (from stale fallback
    /// timers) are no-ops.
    pub fn start_classic_round(&mut self) -> Vec<FastOutgoing> {
        if self.decided || self.classic_started {
            return Vec::new();
        }
        self.classic_started = true;
        self.paxos.start_phase1a(2).into_iter().map(wrap).collect()
    }

    /// Dispatch a consensus request (Phase1a/1b/2a/2b) to the inner Paxos
    /// and propagate the decision-bit upwards.
    pub fn handle_consensus_message(&mut self, request: &pb::RapidRequest) -> Vec<FastOutgoing> {
        let Some(content) = request.content.as_ref() else {
            return Vec::new();
        };
        let outs = match content {
            pb::rapid_request::Content::Phase1aMessage(msg) => self.paxos.handle_phase1a(msg),
            pb::rapid_request::Content::Phase1bMessage(msg) => self.paxos.handle_phase1b(msg),
            pb::rapid_request::Content::Phase2aMessage(msg) => self.paxos.handle_phase2a(msg),
            pb::rapid_request::Content::Phase2bMessage(msg) => self.paxos.handle_phase2b(msg),
            pb::rapid_request::Content::FastRoundPhase2bMessage(msg) => {
                return self.handle_fast_round_proposal(msg);
            }
            _ => return Vec::new(),
        };
        if self.paxos.decided() {
            self.decided = true;
        }
        outs.into_iter().map(wrap).collect()
    }
}

fn wrap(out: PaxosOutgoing) -> FastOutgoing {
    match out {
        PaxosOutgoing::Broadcast(req) => FastOutgoing::Broadcast(req),
        PaxosOutgoing::Unicast { target, request } => FastOutgoing::Unicast { target, request },
        PaxosOutgoing::Decision(d) => FastOutgoing::Decision(d),
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
    fn single_node_fast_round_decides_on_self_vote() {
        let me = ep("127.0.0.1", 6000);
        let mut fp = FastPaxos::new(me.clone(), 1, 1, Duration::from_secs(1));
        // Propose to self.
        let outs = fp.propose(vec![ep("127.0.0.1", 6001)]);
        let mut broadcasted_msg = None;
        for o in &outs {
            if let FastOutgoing::Broadcast(req) = o {
                if let Some(pb::rapid_request::Content::FastRoundPhase2bMessage(m)) =
                    req.content.clone()
                {
                    broadcasted_msg = Some(m);
                }
            }
        }
        let msg = broadcasted_msg.expect("expected FastRoundPhase2b broadcast");
        // Self acceptor receives its own vote → decision.
        let decision_outs = fp.handle_fast_round_proposal(&msg);
        assert!(decision_outs
            .iter()
            .any(|o| matches!(o, FastOutgoing::Decision(_))));
        assert!(fp.decided());
    }

    #[test]
    fn three_node_fast_round_quorum_decides() {
        // N=3, F=(3-1)/4=0, quorum=3. Three identical votes from three peers
        // (including self) → decision.
        let nodes = ["127.0.0.1", "127.0.0.2", "127.0.0.3"].map(|h| ep(h, 6100));
        let proposal = vec![ep("127.0.0.4", 6101)];
        let mut fp = FastPaxos::new(nodes[0].clone(), 7, 3, Duration::from_secs(1));
        let _ = fp.propose(proposal.clone());
        // Two more peers vote for the same proposal.
        for src in &nodes {
            let msg = pb::FastRoundPhase2bMessage {
                configuration_id: 7,
                endpoints: proposal.clone(),
                sender: Some(src.clone()),
            };
            let outs = fp.handle_fast_round_proposal(&msg);
            if outs.iter().any(|o| matches!(o, FastOutgoing::Decision(_))) {
                return; // succeeded
            }
        }
        panic!("expected decision after N votes");
    }

    #[test]
    fn divergent_votes_no_decision_yet() {
        // N=3, three different proposals → no quorum.
        let nodes = ["127.0.0.1", "127.0.0.2", "127.0.0.3"].map(|h| ep(h, 6200));
        let mut fp = FastPaxos::new(nodes[0].clone(), 7, 3, Duration::from_secs(1));
        for (i, src) in nodes.iter().enumerate() {
            let msg = pb::FastRoundPhase2bMessage {
                configuration_id: 7,
                endpoints: vec![ep("127.0.0.99", i32::try_from(i).unwrap())],
                sender: Some(src.clone()),
            };
            let outs = fp.handle_fast_round_proposal(&msg);
            assert!(outs.iter().all(|o| !matches!(o, FastOutgoing::Decision(_))));
        }
        assert!(!fp.decided());
    }
}
