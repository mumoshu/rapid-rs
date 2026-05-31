//! `rapid-example` — Java `StandaloneAgent` analogue (gRPC, joiner-capable).
//!
//! CLI parity with Java `examples.StandaloneAgent`:
//!   `-l <listen>` (required) — `host:port` to bind.
//!   `-s <seed>`   (required by Java; optional here — when equal to
//!                  `--listen` the agent boots as a seed, otherwise it
//!                  joins through the seed).
//!
//! Subscribes to `ViewChange`, `ViewChangeProposal`, `Kicked` and prints a
//! `view:` line every `--print-view-every` ms (the `mixed_cluster.sh`
//! harness greps for it).

use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;
use rapid::cluster::{Cluster, ClusterBuilder};
use rapid::events::ClusterEvent;

#[derive(Parser, Debug)]
#[command(name = "rapid-example", version)]
struct Cli {
    /// Listen address (`host:port`).
    #[arg(short = 'l', long)]
    listen: SocketAddr,

    /// Seed address (`host:port`). When equal to `--listen` boots as
    /// seed; otherwise joins through the seed.
    #[arg(short = 's', long)]
    seed: Option<SocketAddr>,

    /// How often to print the current view (milliseconds).
    #[arg(long, default_value_t = 1000)]
    print_view_every: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();

    let cluster = bootstrap(cli.listen, cli.seed).await?;

    let mut view_changes = cluster.subscribe(ClusterEvent::ViewChange);
    let mut proposals = cluster.subscribe(ClusterEvent::ViewChangeProposal);
    let mut kicked = cluster.subscribe(ClusterEvent::Kicked);

    tokio::spawn(async move {
        while let Ok(ev) = view_changes.recv().await {
            println!(
                "ViewChange: config={} membership={} delta-size={}",
                ev.configuration_id,
                summarize(&ev.membership),
                ev.delta.len()
            );
        }
    });
    tokio::spawn(async move {
        while let Ok(ev) = proposals.recv().await {
            println!(
                "ViewChangeProposal: config={} delta-size={}",
                ev.configuration_id,
                ev.delta.len()
            );
        }
    });
    tokio::spawn(async move {
        while let Ok(ev) = kicked.recv().await {
            println!("Kicked: config={}", ev.configuration_id);
        }
    });

    let interval = Duration::from_millis(cli.print_view_every);
    let print_handle = {
        let cluster_service = cluster.service().clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if let Ok(members) = cluster_service.memberlist().await {
                    if let Ok(id) = cluster_service.configuration_id().await {
                        println!("view: {} {}", id, summarize(&members));
                    }
                }
            }
        })
    };

    println!("rapid-example: ready on {}", cli.listen);
    tokio::signal::ctrl_c().await?;
    println!("rapid-example: ctrl-c received, shutting down");
    print_handle.abort();
    cluster.shutdown().await;
    Ok(())
}

async fn bootstrap(
    listen: SocketAddr,
    seed: Option<SocketAddr>,
) -> Result<Cluster, Box<dyn std::error::Error>> {
    let Some(s) = seed else {
        return Ok(ClusterBuilder::with_grpc(listen).start().await?);
    };
    if s == listen {
        return Ok(ClusterBuilder::with_grpc(listen).start().await?);
    }
    // Outer retry: the protocol's internal Phase-1 retry only runs for
    // 5 × 500ms = 2.5s. In container orchestration (e.g.,
    // docker-compose) the seed can take longer than that to bind its
    // gRPC port. Keep retrying the *whole* bootstrap for up to ~60s
    // so an unreachable seed eventually succeeds rather than crashing
    // the agent.
    let mut tries = 0u32;
    loop {
        match ClusterBuilder::with_grpc(listen).join(s).await {
            Ok(c) => return Ok(c),
            Err(e) if tries < 20 => {
                eprintln!("rapid-example: join attempt {tries} failed ({e}); retrying in 3s...");
                tries += 1;
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

fn summarize(endpoints: &[rapid::pb::Endpoint]) -> String {
    let mut parts: Vec<String> = endpoints
        .iter()
        .map(|e| {
            let host = String::from_utf8_lossy(&e.hostname).to_string();
            format!("{host}:{}", e.port)
        })
        .collect();
    parts.sort();
    format!("[{}]", parts.join(","))
}
