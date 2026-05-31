# Technical Summary: Rapid Distributed Membership Protocol

(Extracted from rapid-paper.md + blog-lalith.md + blog-murat.md)

## A. System Overview

Rapid is a scalable, distributed membership service that solves the dual problem of maintaining **stable** and **consistent** membership views across a cluster in the presence of complex network failures (one-way reachability, firewall misconfigurations, flip-flops in connectivity, high packet loss). Traditional membership solutions fail under these "gray failure" scenarios, leading to membership view oscillation that triggers expensive failure recovery operations repeatedly.

Rapid's core architecture comprises three building blocks:
1. **Expander-based monitoring topology (K-rings)** — deterministic observer/subject relationships for scalable monitoring
2. **Multi-process cut detection (CD)** — aggregation of alerts with H/L thresholds for stability
3. **Fast-Paxos consensus** — leaderless common case with fallback to classical Paxos

The system provides almost-everywhere agreement (unanimity among a large fraction of processes) on multi-node cuts, enabling a fast path to consensus.

## B. Expander-Based Monitoring Topology (K-Ring)

For a configuration C of N processes:
- K separate rings (typically K = 10).
- Each ring contains the full membership, ordered deterministically by hashing each process identifier with a per-ring seed.
- In ring i with ordered list [p0, p1, ..., pN-1], process pj observes p(j+1) mod N.

K Observers / K Subjects rule:
- Each process p monitors exactly K peers (subjects).
- Each process p is monitored by exactly K peers (observers).
- Every join/removal adds/removes exactly 2K monitoring edges.

Expander properties: with K = 10, observed λ₂/(2K) < 0.45, yielding detectable failure fraction |F|/|C| ≤ 0.25.

## C. Multi-Process Cut Detector

Per-process state:
- M(o, s) ∈ {0, 1}: whether observer o has reported on subject s.
- tally(s) = Σ M(*, s).

Two alert types: REMOVE (edge non-responsive) and JOIN (temporary alert about a new joiner).

Two watermarks:
- H (high): tally(s) ≥ H ⇒ stable report mode.
- L (low): L ≤ tally(s) < H ⇒ unstable report mode.
- tally(s) < L ⇒ ignored.

Proposal emitted when both:
1. At least one process is in stable report mode.
2. No process is in unstable report mode.

Implicit-alert mechanism: for each observer o of subject s, if both o and s are in unstable mode, apply an implicit REMOVE alert (o → s).

Reinforcement timeout: after subject s remains unstable, each observer of s broadcasts a reinforcement REMOVE to push toward stability.

Batching: observers batch multiple alerts into a single message before transmission.

## D. Consensus: Fast-Paxos with Fallback

Fast-Paxos common case:
- No explicit proposer; each process uses its own CD proposal as input.
- Vote counted via gossip.
- If three-quarters of the membership (3N/4) vote for the same proposal, decide without further communication or leader election.

Fallback to classical Paxos:
- If Fast Paxos cannot form a supermajority, fall back to classic Paxos.
- A leader is elected; rank-based ordering (round, nodeIndex).

## E. Bootstrap (Join Protocol)

Two-phase join:
1. PreJoinMessage / Seed Contact: joiner contacts seed; seed returns K temporary observers based on joiner identity and current C.
2. JoinMessage / Alert: joiner contacts each temporary observer; they broadcast JOIN alerts.

NodeStatus.BOOTSTRAPPING: temporary state during bootstrap.

## F. Edge Failure Detector

Default: ping-pong with observer→subject probes; mark faulty when 40% of last 10 attempts fail. User-pluggable via interface.

## G. Messaging

- gRPC (default) with Netty alternative.
- All-in-one envelope: `RapidRequest`/`RapidResponse` `oneof` of all message types.
- Unicast-to-all broadcaster (no explicit retries; gossip provides eventual delivery).

## H. Configuration / View

ConfigurationId: hash of membership set, deterministic across processes.
MembershipView holds the ConfigurationId, membership list, and per-process metadata.
Stale ConfigurationId → message rejected.

## I. Metadata

Per-node application-supplied key/value bytes; included with JOIN and view-change notifications.

## J. Key Constants and Defaults

| Parameter | Default | Notes |
|-----------|---------|-------|
| K | 10 | observers per subject |
| H | 9 | high watermark |
| L | 3 (paper) / 4 (Java code) | low watermark |
| Edge timeout | ~10 seconds | default detector |
| Fast Paxos quorum | 3N/4 | supermajority |
| Failure threshold | 40% of last 10 attempts | edge detector |

With K=10, H=9, L=3, λ₂/(2K) < 0.45 → detectable |F|/|C| ≤ 0.25.

## K. Failure Modes and Ordering

- Message loss: gossip + reinforcement timeout guarantee eventual stabilization.
- Partition: majority reconfigures (CP variant); minority logically departs.
- Slow links: H/L thresholds prevent oscillation.
- Consistency guarantee: all processes see the same sequence of membership changes.
