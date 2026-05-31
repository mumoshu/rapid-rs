//! Classic single-decree Paxos with the Fast-Paxos coordinator rule.
//!
//! Bit-exact port of `references/rapid-java/.../Paxos.java`.
//!
//! # Coordinator rule (verbatim transcription of `Paxos.java:262-328`)
//!
//! Let Q be the set of phase1b messages received in the current round.
//! Let k = max { vrnd | r in Q }. Let V = { vval(r) | r in Q ∧ vrnd(r) = k ∧ vval(r) non-empty }.
//!
//! 1. If |V| == 1: choose v.
//! 2. Else if |V| > 1: scan V (with multiplicity!) and pick the first value
//!    whose multiplicity-so-far exceeds N/4. (Fast Paxos resiliency.)
//! 3. Else (V empty): pick the first non-empty vval from Q, or empty list
//!    if all vvals are empty.
//!
//! Outputs from each transition are returned as `Vec<PaxosOutgoing>`; the
//! caller (`MembershipService` actor) is responsible for the side-effects.

use std::collections::HashMap;

use crate::pb;
use crate::view_hash::address_hash;

/// One outbound message produced by a Paxos transition.
pub enum PaxosOutgoing {
    /// Broadcast to all current acceptors.
    Broadcast(pb::RapidRequest),
    /// Send unicast to a specific acceptor (Java sends Phase1b via
    /// `client.sendMessage(phase1a.sender, request)`).
    Unicast {
        /// Where to send.
        target: pb::Endpoint,
        /// Payload.
        request: pb::RapidRequest,
    },
    /// Final decision; the caller applies it via `apply_proposal`.
    Decision(Vec<pb::Endpoint>),
}

/// Classic Paxos state for a single decree.
pub struct Paxos {
    my_addr: pb::Endpoint,
    configuration_id: i64,
    n: usize,
    rnd: pb::Rank,
    vrnd: pb::Rank,
    vval: Vec<pb::Endpoint>,
    phase1b_messages: Vec<pb::Phase1bMessage>,
    accept_responses: HashMap<RankKey, HashMap<EndpointKey, pb::Phase2bMessage>>,
    crnd: pb::Rank,
    cval: Vec<pb::Endpoint>,
    decided: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RankKey {
    round: i32,
    node_index: i32,
}

impl From<&pb::Rank> for RankKey {
    fn from(value: &pb::Rank) -> Self {
        Self {
            round: value.round,
            node_index: value.node_index,
        }
    }
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

impl Paxos {
    /// Construct a Paxos instance for one configuration.
    #[must_use]
    pub fn new(my_addr: pb::Endpoint, configuration_id: i64, n: usize) -> Self {
        let zero = pb::Rank {
            round: 0,
            node_index: 0,
        };
        Self {
            my_addr,
            configuration_id,
            n,
            rnd: zero,
            vrnd: zero,
            vval: Vec::new(),
            phase1b_messages: Vec::new(),
            accept_responses: HashMap::new(),
            crnd: zero,
            cval: Vec::new(),
            decided: false,
        }
    }

    /// Whether a decision has been recorded locally.
    #[must_use]
    pub fn decided(&self) -> bool {
        self.decided
    }

    /// At coordinator, start a classic round (Java `startPhase1a`).
    pub fn start_phase1a(&mut self, round: i32) -> Vec<PaxosOutgoing> {
        if self.crnd.round > round {
            return Vec::new();
        }
        self.crnd = pb::Rank {
            round,
            node_index: stable_node_index(&self.my_addr),
        };
        let prepare = pb::Phase1aMessage {
            configuration_id: self.configuration_id,
            sender: Some(self.my_addr.clone()),
            rank: Some(self.crnd),
        };
        let req = pb::RapidRequest {
            content: Some(pb::rapid_request::Content::Phase1aMessage(prepare)),
        };
        vec![PaxosOutgoing::Broadcast(req)]
    }

    /// At acceptor, handle a phase1a message (Java `handlePhase1aMessage`).
    pub fn handle_phase1a(&mut self, msg: &pb::Phase1aMessage) -> Vec<PaxosOutgoing> {
        if msg.configuration_id != self.configuration_id {
            return Vec::new();
        }
        let Some(rank) = msg.rank.as_ref() else {
            return Vec::new();
        };
        if compare_ranks(self.rnd, *rank) == std::cmp::Ordering::Less {
            self.rnd = *rank;
        } else {
            return Vec::new();
        }
        let response = pb::Phase1bMessage {
            configuration_id: self.configuration_id,
            rnd: Some(self.rnd),
            sender: Some(self.my_addr.clone()),
            vrnd: Some(self.vrnd),
            vval: self.vval.clone(),
        };
        let req = pb::RapidRequest {
            content: Some(pb::rapid_request::Content::Phase1bMessage(response)),
        };
        let Some(sender) = msg.sender.as_ref() else {
            return Vec::new();
        };
        vec![PaxosOutgoing::Unicast {
            target: sender.clone(),
            request: req,
        }]
    }

    /// At coordinator, handle a phase1b message (Java `handlePhase1bMessage`).
    pub fn handle_phase1b(&mut self, msg: &pb::Phase1bMessage) -> Vec<PaxosOutgoing> {
        if msg.configuration_id != self.configuration_id {
            return Vec::new();
        }
        let Some(rnd) = msg.rnd.as_ref() else {
            return Vec::new();
        };
        if compare_ranks(self.crnd, *rnd) != std::cmp::Ordering::Equal {
            return Vec::new();
        }
        self.phase1b_messages.push(msg.clone());
        if self.phase1b_messages.len() <= self.n / 2 {
            return Vec::new();
        }
        let chosen = self.select_proposal_using_coordinator_rule();
        if !self.cval.is_empty() || chosen.is_empty() {
            return Vec::new();
        }
        self.cval.clone_from(&chosen);
        let phase2a = pb::Phase2aMessage {
            sender: Some(self.my_addr.clone()),
            configuration_id: self.configuration_id,
            rnd: Some(self.crnd),
            vval: chosen,
        };
        let req = pb::RapidRequest {
            content: Some(pb::rapid_request::Content::Phase2aMessage(phase2a)),
        };
        vec![PaxosOutgoing::Broadcast(req)]
    }

    /// At acceptor, handle a phase2a message (Java `handlePhase2aMessage`).
    pub fn handle_phase2a(&mut self, msg: &pb::Phase2aMessage) -> Vec<PaxosOutgoing> {
        if msg.configuration_id != self.configuration_id {
            return Vec::new();
        }
        let Some(rnd) = msg.rnd.as_ref() else {
            return Vec::new();
        };
        if compare_ranks(self.rnd, *rnd) != std::cmp::Ordering::Greater && self.vrnd != *rnd {
            self.rnd = *rnd;
            self.vrnd = *rnd;
            self.vval.clone_from(&msg.vval);
            let response = pb::Phase2bMessage {
                configuration_id: self.configuration_id,
                rnd: Some(*rnd),
                sender: Some(self.my_addr.clone()),
                endpoints: self.vval.clone(),
            };
            let req = pb::RapidRequest {
                content: Some(pb::rapid_request::Content::Phase2bMessage(response)),
            };
            return vec![PaxosOutgoing::Broadcast(req)];
        }
        Vec::new()
    }

    /// At acceptor, learn about another acceptor's vote (Java `handlePhase2bMessage`).
    pub fn handle_phase2b(&mut self, msg: &pb::Phase2bMessage) -> Vec<PaxosOutgoing> {
        if msg.configuration_id != self.configuration_id || self.decided {
            return Vec::new();
        }
        let Some(rnd) = msg.rnd.as_ref() else {
            return Vec::new();
        };
        let Some(sender) = msg.sender.as_ref() else {
            return Vec::new();
        };
        let bucket = self.accept_responses.entry(RankKey::from(rnd)).or_default();
        bucket.insert(EndpointKey::from(sender), msg.clone());
        if bucket.len() > self.n / 2 {
            tracing::info!(target: "rapid", size = msg.endpoints.len(), "paxos.classic.decided");
            self.decided = true;
            return vec![PaxosOutgoing::Decision(msg.endpoints.clone())];
        }
        Vec::new()
    }

    /// `FastPaxos`'s "this is the only fast round" hook.
    /// Java `Paxos.registerFastRoundVote(vote)`.
    pub fn register_fast_round_vote(&mut self, vote: Vec<pb::Endpoint>) {
        if self.rnd.round > 1 {
            return;
        }
        self.rnd = pb::Rank {
            round: 1,
            node_index: 1,
        };
        self.vrnd = self.rnd;
        self.vval = vote;
    }

    /// The coordinator-rule (Java `selectProposalUsingCoordinatorRule`).
    /// Public for the port of `PaxosTests` — Java marked it `@VisibleForTesting`.
    #[must_use]
    pub fn select_proposal_using_coordinator_rule(&self) -> Vec<pb::Endpoint> {
        Self::select_proposal_from_messages(&self.phase1b_messages, self.n)
    }

    /// The coordinator-rule as a free function over a caller-provided
    /// message slice. Java exposes an equivalent overload that
    /// `PaxosTests` drives directly; we mirror it so F12's parametric
    /// rows don't have to round-trip through `handle_phase1b`.
    #[must_use]
    pub fn select_proposal_from_messages(
        messages: &[pb::Phase1bMessage],
        n: usize,
    ) -> Vec<pb::Endpoint> {
        if messages.is_empty() {
            return Vec::new();
        }
        let max_vrnd = messages
            .iter()
            .filter_map(|m| m.vrnd.as_ref())
            .max_by(|a, b| std::cmp::Ord::cmp(&rank_tuple(**a), &rank_tuple(**b)));
        let Some(max_vrnd) = max_vrnd else {
            return Vec::new();
        };
        let collected: Vec<Vec<pb::Endpoint>> = messages
            .iter()
            .filter(|m| m.vrnd.as_ref() == Some(max_vrnd))
            .filter(|m| !m.vval.is_empty())
            .map(|m| m.vval.clone())
            .collect();
        let unique: std::collections::HashSet<Vec<EndpointKey>> = collected
            .iter()
            .map(|v| v.iter().map(EndpointKey::from).collect())
            .collect();

        if unique.len() == 1 {
            return collected.into_iter().next().unwrap_or_default();
        }
        if !collected.is_empty() {
            let mut counters: HashMap<Vec<EndpointKey>, usize> = HashMap::new();
            let quarter = n / 4;
            for v in &collected {
                let key: Vec<EndpointKey> = v.iter().map(EndpointKey::from).collect();
                let entry = counters.entry(key).or_insert(0);
                if *entry + 1 > quarter {
                    return v.clone();
                }
                *entry += 1;
            }
        }
        messages
            .iter()
            .filter(|m| !m.vval.is_empty())
            .map(|m| m.vval.clone())
            .next()
            .unwrap_or_default()
    }
}

fn rank_tuple(r: pb::Rank) -> (i32, i32) {
    (r.round, r.node_index)
}

fn compare_ranks(left: pb::Rank, right: pb::Rank) -> std::cmp::Ordering {
    rank_tuple(left).cmp(&rank_tuple(right))
}

#[cfg(test)]
fn compare_ranks_int(left: pb::Rank, right: pb::Rank) -> i32 {
    match compare_ranks(left, right) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

/// Java `myAddr.hashCode()` becomes a stable `i32` derived from
/// `xxh64(hostname, 0) ^ port`. The wire-byte representation of `Rank`
/// uses this value; cross-language interop is **not** byte-stable for
/// the `nodeIndex` field. Within a Rust cluster all peers agree.
#[must_use]
fn stable_node_index(endpoint: &pb::Endpoint) -> i32 {
    let h = address_hash(0, endpoint);
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    {
        (h ^ (h >> 32)) as i32
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
    fn compare_ranks_orders_by_round_then_index() {
        let a = pb::Rank {
            round: 1,
            node_index: 0,
        };
        let b = pb::Rank {
            round: 2,
            node_index: 0,
        };
        assert_eq!(compare_ranks_int(a, b), -1);
        let c = pb::Rank {
            round: 1,
            node_index: 5,
        };
        assert_eq!(compare_ranks_int(a, c), -1);
        assert_eq!(compare_ranks_int(a, a), 0);
    }

    #[test]
    fn single_acceptor_classical_decides() {
        // N=1, one acceptor self. Drive through phase1a/1b/2a/2b.
        let me = ep("127.0.0.1", 7000);
        let mut p = Paxos::new(me.clone(), 42, 1);
        let out = p.start_phase1a(2);
        assert!(matches!(out[0], PaxosOutgoing::Broadcast(_)));
        // Self processes phase1a as acceptor.
        let inbound = pb::Phase1aMessage {
            configuration_id: 42,
            sender: Some(me.clone()),
            rank: Some(p.crnd),
        };
        let unicast = p.handle_phase1a(&inbound);
        assert!(matches!(unicast[0], PaxosOutgoing::Unicast { .. }));
        let reply = match &unicast[0] {
            PaxosOutgoing::Unicast { request, .. } => match &request.content {
                Some(pb::rapid_request::Content::Phase1bMessage(m)) => m.clone(),
                _ => panic!("expected Phase1b"),
            },
            _ => panic!("expected unicast"),
        };
        let phase2a_out = p.handle_phase1b(&reply);
        // N=1, N/2=0, |Q|>0 satisfies the quorum, but vval is empty, so we
        // pick "empty list" → cval stays empty → no Phase2a.
        assert!(phase2a_out.is_empty());
        // Now register a fast-round vote (the bootstrap path) and try again.
        p.register_fast_round_vote(vec![ep("127.0.0.1", 7001)]);
        let raised = pb::Phase1aMessage {
            configuration_id: 42,
            sender: Some(me.clone()),
            rank: Some(pb::Rank {
                round: 3,
                node_index: stable_node_index(&me),
            }),
        };
        let _ = p.handle_phase1a(&raised);
        // Build a fresh coordinator state for this larger round.
        let mut p2 = Paxos::new(me.clone(), 42, 1);
        p2.register_fast_round_vote(vec![ep("127.0.0.1", 7001)]);
        let _ = p2.start_phase1a(2);
        // The phase1b from acceptor includes the fast-round vote.
        let voted = pb::Phase1bMessage {
            configuration_id: 42,
            rnd: Some(p2.crnd),
            sender: Some(me.clone()),
            vrnd: Some(pb::Rank {
                round: 1,
                node_index: 1,
            }),
            vval: vec![ep("127.0.0.1", 7001)],
        };
        let phase2a_out = p2.handle_phase1b(&voted);
        assert!(matches!(phase2a_out[0], PaxosOutgoing::Broadcast(_)));
    }
}
