# Rapid Java Implementation: Structural Map

(Extracted from rapid-java/rapid/src/main/java + examples)

## Module overview

| Module | Role |
|--------|------|
| `Cluster` | Public facade: `Builder.start()`, `Builder.join(seed)`, `getMemberlist()`, `leaveGracefully()`, `shutdown()`. Owns `MembershipService` + `IMessagingServer`. |
| `MembershipService` | Core orchestrator. `@NotThreadSafe`; all logic runs on a single `protocolExecutor` thread. Handles bootstrap, alert batching, failure detector scheduling, consensus dispatch, view-change application. |
| `MembershipView` | K-ring data structure. `@ThreadSafe` via `ReadWriteLock`. K `NavigableSet<Endpoint>` rings sorted by per-ring `AddressComparator` (xxHash64 with ring-specific seed). |
| `MultiNodeCutDetector` | H/L cut detector. `reportsPerHost`, `preProposal`, `proposal` sets, `updatesInProgress` counter. `aggregateForProposal()` + `invalidateFailingEdges()`. |
| `FastPaxos` | Fast round (3/4 vote count). On failure, schedules a classic round via `scheduledClassicRoundTask`. |
| `Paxos` | Classic Paxos (single decree): phase 1a, 1b, 2a, 2b. Coordinator-rule value selection (Fast Paxos paper, §270-328). |
| `UnicastToAllBroadcaster` | Sends to all members in randomized order. |
| `MetadataManager` | `ConcurrentHashMap<Endpoint, Metadata>`. |
| `SharedResources` | Centralized executors (protocolExecutor, backgroundTasksExecutor, scheduledTasksExecutor, NioEventLoopGroup). |
| `Settings` | Configuration holder (timeouts, batching window, FD interval). |
| `IMessagingClient`/`IMessagingServer`/`IBroadcaster` | Pluggable transport traits. |
| `IEdgeFailureDetectorFactory`/`PingPongFailureDetector` | Pluggable failure detection. 10 consecutive failures or 30 BOOTSTRAPPING responses → DOWN. |
| `rapid.proto` | Single `RapidRequest`/`RapidResponse` oneof envelope for all messages. |

## Key constants (from Java code)

| Constant | Value | Source |
|---|---|---|
| K | 10 | Cluster.java |
| H | 9 | Cluster.java |
| L | 4 | Cluster.java |
| BATCHING_WINDOW_IN_MS | 100 | MembershipService.java |
| DEFAULT_FAILURE_DETECTOR_INTERVAL_IN_MS | 1000 | MembershipService.java |
| LEAVE_MESSAGE_TIMEOUT | 1500 ms | MembershipService.java |
| BASE_DELAY (paxos fallback) | 1000 ms | FastPaxos.java |
| FAILURE_THRESHOLD (probes) | 10 | PingPongFailureDetector.java |
| BOOTSTRAP_COUNT_THRESHOLD | 30 | PingPongFailureDetector.java |
| RETRIES (join) | 5 | Cluster.java |

## Threading model

- `MembershipService`: single-threaded `protocolExecutor` (newSingleThreadExecutor).
- `MembershipView`: `ReadWriteLock`.
- `MultiNodeCutDetector`: `synchronized(lock)`.
- `FastPaxos`/`Paxos`: serialized via `paxosLock` Object in FastPaxos.
- `AlertBatcher` (inner class): `ReentrantLock` over `sendQueue`; runs on `backgroundTasksExecutor.scheduleAtFixedRate(0, 100ms)`.
- Per-subject failure detectors: scheduled on `backgroundTasksExecutor`.

## Dependency graph

```
Cluster
  ├─ MembershipService
  ├─ IMessagingServer (GrpcServer default)
  ├─ IMessagingClient (GrpcClient default)
  ├─ IEdgeFailureDetectorFactory (PingPongFailureDetector.Factory default)
  └─ SharedResources

MembershipService
  ├─ MembershipView
  ├─ MultiNodeCutDetector
  ├─ FastPaxos
  │   └─ Paxos
  ├─ IBroadcaster (UnicastToAllBroadcaster)
  ├─ IMessagingClient
  ├─ MetadataManager
  ├─ IEdgeFailureDetectorFactory
  ├─ SharedResources
  └─ Settings
```

## Proto message catalog

Bootstrap: `PreJoinMessage`, `JoinMessage`, `JoinResponse` (+ `JoinStatusCode` enum).
Failure detection: `BatchedAlertMessage` / `AlertMessage` (+ `EdgeStatus`), `ProbeMessage`/`ProbeResponse` (+ `NodeStatus`).
Consensus: `FastRoundPhase2bMessage`, `Phase1aMessage`, `Phase1bMessage`, `Phase2aMessage`, `Phase2bMessage` (+ `Rank`).
Leave: `LeaveMessage`.
Utility: `Endpoint`, `NodeId`, `Metadata`, `Response`, `ConsensusResponse`.
Service: `MembershipService.sendRequest(RapidRequest) → RapidResponse`.

## Critical invariants

1. K-Observer: every node has exactly K observers and K subjects across the K rings.
2. ConfigurationId monotonicity: each accepted view bumps a deterministic hash.
3. Quorum: FastPaxos decides on ≥ N − F (F = ⌊(N−1)/4⌋) identical votes; classic Paxos decides on > N/2.
4. Single-threaded protocol: all `MembershipService` mutations on one executor thread; external callers use futures.
5. At-most-once: `decided: AtomicBoolean` in FastPaxos, `notified: boolean` in failure detectors.
6. Bootstrap atomicity: phase-1 retried up to 5 times; phase-2 committed once observers process UP alert.
