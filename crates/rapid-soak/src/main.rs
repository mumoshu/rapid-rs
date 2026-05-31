//! `rapid-soak` — leak / steady-state checker.
//!
//! Brings up a 10-node in-process cluster, then induces `N` no-op
//! `apply_proposal` view-changes while comparing dhat heap snapshots
//! taken at warm-up vs. end.
//!
//! Build with `--features dhat-heap` to swap in the dhat allocator:
//!   cargo build --release -p rapid-soak --features dhat-heap
//!
//! Run with:
//!   ./target/release/rapid-soak --view-changes 100 --duration-secs 1800
//!
//! Exit 0 if heap delta < `THRESHOLD_BYTES`.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use clap::Parser;
use rapid::cluster::ClusterBuilder;
use rapid::messaging::InProcessNetwork;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(feature = "dhat-heap")]
const THRESHOLD_BYTES: u64 = 5 * 1024 * 1024; // 5 MB per PLAN.md

#[derive(Parser)]
#[command(name = "rapid-soak")]
struct Cli {
    /// Number of induced view changes.
    #[arg(long, default_value_t = 100)]
    view_changes: u32,
    /// Soak duration (seconds). The loop sleeps between view changes
    /// to spread them across the window.
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,
    /// Cluster size (default 10 per PLAN.md).
    #[arg(long, default_value_t = 10)]
    nodes: u16,
    /// Base port for in-process address allocation.
    #[arg(long, default_value_t = 40_000)]
    base_port: u16,
}

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let net = InProcessNetwork::new();
    let seed = ClusterBuilder::new(addr(cli.base_port), net.clone())
        .with_settings(rapid::settings::Settings::for_tests())
        .start()
        .await?;
    let mut nodes: Vec<rapid::cluster::Cluster> = vec![seed];

    for i in 1..cli.nodes {
        let a = addr(cli.base_port + i);
        let c = ClusterBuilder::new(a, net.clone())
            .with_settings(rapid::settings::Settings::for_tests())
            .join(addr(cli.base_port))
            .await?;
        nodes.push(c);
    }
    println!("rapid-soak: {}-node cluster up", cli.nodes);

    // Warm-up window — let the cluster settle (alert batchers, FDs,
    // initial broadcasts) before snapshotting.
    tokio::time::sleep(Duration::from_secs(3)).await;
    #[cfg(feature = "dhat-heap")]
    let warmup_snapshot = dhat::HeapStats::get();

    let total_dur = Duration::from_secs(cli.duration_secs);
    let per_iter = total_dur / cli.view_changes.max(1);
    let start = Instant::now();
    let svc = nodes[0].service().clone();
    for i in 0..cli.view_changes {
        svc.apply_proposal(vec![]).await?;
        if start.elapsed() >= total_dur {
            println!("rapid-soak: ran out of time at iter {i}");
            break;
        }
        tokio::time::sleep(per_iter).await;
    }

    #[cfg(feature = "dhat-heap")]
    let final_snapshot = dhat::HeapStats::get();
    #[cfg(feature = "dhat-heap")]
    {
        let warm_bytes = warmup_snapshot.curr_bytes as i64;
        let final_bytes = final_snapshot.curr_bytes as i64;
        let delta = final_bytes - warm_bytes;
        println!(
            "rapid-soak: heap warm={} bytes, final={} bytes, delta={} bytes",
            warm_bytes, final_bytes, delta,
        );
        if delta > THRESHOLD_BYTES as i64 {
            return Err(format!(
                "heap delta {} bytes exceeds threshold {}",
                delta, THRESHOLD_BYTES
            )
            .into());
        }
    }

    // Cooperative shutdown.
    for c in nodes {
        c.shutdown().await;
    }
    println!("rapid-soak: PASS");
    Ok(())
}
