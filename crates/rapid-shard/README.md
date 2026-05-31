# rapid-shard

Strongly consistent per-shard replica discovery for sharded,
replicated systems built on top of a [Rapid](../rapid/) membership
cluster.

Each node tags itself at join time with the set of shards it
serves (a CSV in Rapid's bootstrap metadata). The
`ShardDirectory` provided by this crate derives "alive replicas of
shard X" from the cluster's membership view and keeps the per-shard
index strongly consistent across every consumer: two directories
anywhere in the cluster that report the same Rapid
`configuration_id` have byte-identical contents.

```rust
let cluster = ClusterBuilder::new(addr, net)
    .with_metadata([("shards", b"shard-A,shard-C".to_vec())])
    .join(contact).await?;

let dir = ShardDirectory::new(&cluster).await?;
let replicas = dir.replicas_of("shard-A");   // -> Vec<Endpoint>
let known    = dir.all_shards();              // -> Vec<String>
```

A full quickstart is in [§ Quickstart](#quickstart) below; the
sections before that explain the design choice this crate
embodies and the alternatives it's traded against, so you can
decide whether it's the right fit before adopting it.

## The problem

Rapid is a *cluster membership* protocol: one Rapid cluster has one
membership view, agreed on by every node via the K-ring failure
detector + multi-process cut detector + Fast Paxos. The output is
"which nodes are alive right now," strongly consistent.

A sharded, replicated system has **two** memberships:

1. **Node liveness** — "is node X alive?" — a property of physical
   nodes, independent of which shards live on them.
2. **Per-shard replica set** — "for shard S, which alive nodes hold
   its replicas right now?" — a property of the shard, intersected
   with liveness.

Rapid is built for (1). It can support (2), but you have to be
deliberate about how. There are three viable architectures; this
crate implements one of them. The next section lays them out so
you can pick.

## Three patterns for putting Rapid in a sharded system

The three architectures below are the ones that make sense once
you've decided to use Rapid. They differ in *what Rapid is asked
to do*: track per-shard replicas directly (A), track every node
and let consumers derive per-shard sets (B), or track only node
liveness while a separate service handles placement (C). The
table at [§ When to use what](#when-to-use-what) below summarises
the trade-offs; the prose subsections explain them.

### Pattern A — one Rapid cluster per shard

Each shard runs its own Rapid instance: own seed, own K-ring, own
`configuration_id`. Service discovery is just
`shard_S_rapid.memberlist()`.

- **Pros**: clean mental model (shard-as-cluster); per-shard FD is
  tight; failure isolation across shards.
- **Cons**: no protocol-level answer to "what shards exist"; a node
  in N shards runs N Rapid agents (N × K monitoring overhead);
  cross-shard liveness is fragmented.

Use when shards are big (tens of nodes each), failure isolation
matters, shards rarely correlate.

### Pattern B — one global Rapid cluster + metadata-derived shards

All physical nodes are in **one** Rapid cluster. Each node's
`pb::Metadata` carries a CSV of shard ids:

```rust
ClusterBuilder::with_grpc(addr)
    .with_metadata([("shards", b"shard-A,shard-C,shard-F".as_slice())])
    .join(contact).await?;
```

Per-shard membership is *derived* by every consumer: filter the
memberlist by metadata key. This crate does the filtering plus a
strongly consistent index that updates on every `VIEW_CHANGE`
event.

- **Pros**: one Rapid cluster, one FD pipeline, one
  `configuration_id`; "what shards exist" is answerable; per-shard
  replica set is automatically consistent across consumers at the
  same `configuration_id`.
- **Cons**: one blast radius; uniform FD (no shard-local fast
  path); **`pb::Metadata` is set at join time and never mutated**
  in the protocol.

Use when shard count is high (hundreds), shards are small (≈3
replicas), and you want one source of truth for the whole fleet's
view.

### Pattern C — split control plane and data plane

A small global Rapid cluster (control plane) tracks "which physical
nodes are alive" and "what shards exist and where." Each shard
runs its own replication protocol (Raft, Paxos, chain replication)
for its own consistency. Per-shard membership lives in the control
plane as data (e.g., a Raft-replicated `Placement` table).

- **Pros**: each layer's protocol matches its granularity; shard-
  level reconfig is a control-plane data change, not a Rapid view
  change; liveness and placement are decoupled.
- **Cons**: two systems to operate; per-shard discovery needs a
  control-plane query path.

Use when shards are heterogeneous, placement is non-trivial
(rebalancing, load-aware moves), and you'd want a placement
service anyway. This is how production systems with high-churn
placement (Slicer, FoundationDB's data distributor) end up doing
it.

## What this crate is

**`rapid-shard` implements Pattern B**: one Rapid cluster across
every node in the system, each node tagged with the shard ids it
serves, per-shard replica sets derived by filtering the
memberlist.

The reason Pattern B is worth a crate (rather than a few lines of
caller code) is the *forwarder loop* — to be useful, the
per-shard index has to stay consistent with the cluster's
`VIEW_CHANGE` stream, including handling joins, kicks, lagged
subscribers, and the reverse-index bookkeeping needed to know
which buckets to scrub on a DOWN event that doesn't re-carry
metadata. This crate handles all of that and exposes a synchronous
read API; you query `dir.replicas_of("shard-A")` and get the
current answer.

The trade-off vs Patterns A and C: you accept one Rapid blast
radius, uniform (non-shard-local) failure detection, and
bootstrap-time placement (no runtime shard reassignment) in
exchange for one cluster to operate and a strong-consistency
property that comes for free from Rapid's view-change pipeline.
If that trade-off works for you, the rest of the README is the
API + caveats. If it doesn't, [§ Caveats](#caveats) and
[§ When to use what](#when-to-use-what) point at A or C.

## Quickstart

```rust
use rapid::cluster::ClusterBuilder;
use rapid::messaging::InProcessNetwork;
use rapid_shard::{ShardDirectory, DEFAULT_METADATA_KEY};

let net = InProcessNetwork::new();
let seed_addr = "127.0.0.1:9000".parse()?;

// Bring up a 3-node cluster, each node tagged with the shards it serves.
let seed = ClusterBuilder::new(seed_addr, net.clone())
    .with_metadata([(DEFAULT_METADATA_KEY, b"A,B".to_vec())])
    .start().await?;
let n1 = ClusterBuilder::new("127.0.0.1:9001".parse()?, net.clone())
    .with_metadata([(DEFAULT_METADATA_KEY, b"A,C".to_vec())])
    .join(seed_addr).await?;
let n2 = ClusterBuilder::new("127.0.0.1:9002".parse()?, net.clone())
    .with_metadata([(DEFAULT_METADATA_KEY, b"B,C".to_vec())])
    .join(seed_addr).await?;

// Build a directory on any node — they all see the same index.
let dir = ShardDirectory::new(&seed).await?;
assert_eq!(dir.all_shards(), vec!["A", "B", "C"]);
assert_eq!(dir.replicas_of("A").len(), 2);   // seed + n1
```

## API surface

| Method | Returns | Behaviour |
|---|---|---|
| `ShardDirectory::new(&cluster)` | `Result<Self>` | Snapshot + start forwarder. Default key `"shards"`, delim `,`. |
| `ShardDirectory::builder()...build(&cluster)` | `Result<Self>` | Configurable `with_key` / `with_delimiter`. |
| `replicas_of(shard)` | `Vec<Endpoint>` | Empty if shard unknown. Synchronous, lock-guarded read. |
| `all_shards()` | `Vec<String>` | Sorted list of every currently-advertised shard. |
| `configuration_id()` | `ConfigurationId` | Rapid view this index last folded in. |
| `total_replica_slots()` | `usize` | Sum of `(node, shard)` pairs across all shards. |
| `shutdown(self).await` | `()` | Aborts the forwarder. Idempotent. `Drop` also handles this. |

`ShardDirectory` is `Clone` (single `Arc`); cloning is cheap and
shares the same index.

## How it works

```text
   join time            view change         shutdown
   ─────────            ───────────         ────────
   .with_metadata        ViewChange evt      Drop or .shutdown()
   ("shards","A,C")           ↓                    ↓
        ↓               apply_delta           abort forwarder
   propagated via              ↓                 task; index
   JoinResponse +         by_shard map           remains
   alert pipeline         updated
        ↓                       ↓
   in every node's       readers see new
   MetadataManager       config_id atomically
```

1. At construction, `Cluster::initial_view_event()` returns a
   synthetic `VIEW_CHANGE` listing every current member with
   `status = UP` and the metadata they bootstrapped with. The
   directory folds this into its index.

2. A `tokio::spawn`ed forwarder task subscribes to
   `ClusterEvent::ViewChange`. Each delivered event carries:
   - `configuration_id` of the new view,
   - `delta: Vec<NodeStatusChange>` — per-node changes,
     `EdgeStatus::Up` for joiners (carries metadata) or `Down` for
     removals (no metadata).
   The forwarder folds `delta` into the index.

3. A reverse `by_endpoint: HashMap<EndpointKey, Vec<String>>`
   index is maintained so that `DOWN` events (which don't re-carry
   metadata) know which shard buckets to scrub.

4. On `RecvError::Lagged` — a slow forwarder that missed events —
   the directory full-resyncs from the actor's
   `service.memberlist()` + `service.metadata()` and rebuilds the
   index from scratch.

The strong-consistency claim — two directories anywhere reporting
the same `configuration_id` have byte-identical `by_shard` maps —
follows directly from Rapid's almost-everywhere agreement: every
node receives the same `ViewChange` stream, so every directory
folds the same deltas in the same order.

## Caveats

### `pb::Metadata` is bootstrap-only

Upstream Rapid (Java and our impl) set node metadata at join time
and never mutate it. If you need to move shard X from node A to
node B without taking node A out of the cluster, you have three
options:

1. **Force a rejoin.** `cluster.leave_gracefully().await` then
   `ClusterBuilder::...join().await` with the new tag. Triggers a
   view change just to update placement. Workable for low-frequency
   reassignment (minutes apart), painful at high frequency.

2. **Extend Rapid with a `MetadataUpdate` event/RPC.** Java's
   `ClusterEvents` doesn't have this; neither do we. Roughly a
   day of work to add a new proto message, an actor command, a
   broadcaster path, and a `MetadataUpdate` event variant. The
   directory would then subscribe to two event types and fold both.

3. **Move to Pattern C.** Rapid does liveness only; placement is a
   Raft service. Shard reassignment is a Raft write, not a Rapid
   view change. Best for systems with continuous rebalancing.

### Uniform failure detection

Rapid's K-ring monitoring is uniform — every node monitors K random
peers regardless of shard membership. You don't get tighter FD for
intra-shard peers just because they share a shard. If shard-local
FD matters (e.g., for fast Raft leader election within a shard),
combine this with a per-shard FD layer or use Pattern A.

### One blast radius

Pattern B puts every shard's discovery on one Rapid cluster's
fate. A cluster-wide outage takes down discovery for every shard.
Pattern A or C separate fate; Pattern B trades that for simpler
operations.

## When to use what

| Property | Pattern A (per-shard Rapid) | Pattern B (this crate) | Pattern C (split planes) |
|---|---|---|---|
| Operational complexity | High (N clusters) | Low | High (2 systems) |
| Per-shard FD tightness | Tight | Loose | Tight (per-shard data plane) |
| Cross-shard "what exists" | Out-of-band | Native | Native (control plane) |
| Failure isolation | Per shard | Whole fleet | Per shard |
| Runtime reassignment | Cluster-level | Bootstrap-only | Native (control plane write) |
| Resource per node | N × K | K | K |
| Best for | Big shards, low correlation | Many small shards, static placement | Heterogeneous shards, rebalancing |

## Implementation status

End-to-end Pattern B works. The test matrix in
[`tests/pattern_b.rs`](tests/pattern_b.rs) covers:

- **Initial-state indexing** — 6 nodes × 3 shards × 2 replicas,
  every bucket comes out exactly right.
- **Failure propagation** — killing a multi-shard node removes it
  from every shard it served while leaving sibling shards
  untouched.
- **Dynamic join** — a node joining with a fresh shard id creates
  that bucket and joins existing ones without disturbing siblings.
- **Custom schemas** — non-default metadata key and delimiter.
- **No-metadata baseline** — nodes without the metadata key
  produce an empty index, no panics.

## See also

- [`rapid`](../rapid/) — the underlying membership crate.
- [`docs/k-h-l.md`](../../docs/k-h-l.md) — explanation of the K, H,
  L cut-detector parameters that govern cluster-wide FD behaviour.
- For dynamic Pattern B (mutable metadata at runtime): the
  extension would add a `MetadataUpdateMessage` to the proto, a
  `handle_metadata_update` actor command, and a `MetadataUpdate`
  variant on `ClusterEvent`. About half a day of work; not
  currently implemented.
