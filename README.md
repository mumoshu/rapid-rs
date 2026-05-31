# rapid-rs

A Rust port of [Rapid](https://github.com/lalithsuresh/rapid), the distributed
membership service from Suresh et al., USENIX ATC '18.

## What is Rapid?

Rapid maintains stable, consistent membership across a cluster in the
face of gray failures (one-way reachability, flip-flops, high packet
loss). It uses three building blocks:

1. **K-ring monitoring topology** — every node has exactly K observers
   and K subjects, with the relationships drawn from K deterministic
   permutations of the membership.
2. **Multi-process cut detector** — observer alerts are aggregated with
   H/L watermarks, producing almost-everywhere agreement on multi-node
   cuts. See [`docs/k-h-l.md`](docs/k-h-l.md) for the mental model and
   how our defaults compare to the paper and to Java upstream.
3. **Fast Paxos consensus** — the common case is leaderless; classic
   Paxos is the recovery fallback.

See [`docs/rapid_summary.md`](docs/rapid_summary.md) for the
algorithm and [`docs/rapid_java.md`](docs/rapid_java.md)
for the upstream Java structure.

### Further reading

- [`docs/k-h-l.md`](docs/k-h-l.md) — what `K`, `H`, `L` mean, the
  almost-everywhere agreement intuition, and the defaults we ship
  vs. the paper and Java upstream.

## Quickstart

```rust
use std::net::SocketAddr;

use rapid::cluster::ClusterBuilder;
use rapid::messaging::InProcessNetwork;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let network = InProcessNetwork::new();
    let seed: SocketAddr = "127.0.0.1:1234".parse()?;
    let cluster = ClusterBuilder::new(seed, network.clone()).start().await?;
    println!("seed memberlist: {:?}", cluster.memberlist().await?);

    let joiner: SocketAddr = "127.0.0.1:1235".parse()?;
    let joined = ClusterBuilder::new(joiner, network).join(seed).await?;
    println!("joined memberlist: {:?}", joined.memberlist().await?);

    joined.shutdown().await;
    cluster.shutdown().await;
    Ok(())
}
```

## Running the standalone agent

```bash
cargo run -p rapid-example --release -- -l 127.0.0.1:1234
```

prints `seed bootstrapped` followed by a periodic `view:` line.

## Tracing

The crate emits a fixed event taxonomy (see [`PLAN.md`](PLAN.md) §
*Decisions pinned upfront* → `tracing` events). Set
`RUST_LOG=rapid=info` to see the high-signal flow:

```
RUST_LOG=rapid=info cargo run -p rapid-example
```

## Layout

- `crates/rapid` — library: proto bindings, view, cut detector, messaging,
  service actor, consensus, monitoring, public `Cluster` API.
- `crates/rapid-example` — standalone agent CLI.
- `crates/rapid-compat-tests` — Java-parity test harness:
  - `vectors/` — golden vectors generated from Java
    (`tools/dump-vectors/`).
  - `parity/` — per-phase parity reports.

## Compatibility

Wire format and the xxh64-based ring ordering are byte-stable against
the upstream Java implementation.

## License

Apache-2.0 (matches upstream).
