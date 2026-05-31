//! Cooperative-shutdown verification — F4 gate.
//!
//! After `Cluster::shutdown()` returns, the rapid-spawned tasks
//! (actor loop, alert batcher, FD notifier pump, FD probe tasks,
//! in-process inbox loop) must all be reaped. We can't query
//! tokio-console from a regular `#[tokio::test]`, but we can:
//!  1. Snapshot alive-task count before bootstrap.
//!  2. Bootstrap → shutdown a 4-node cluster in a loop.
//!  3. Assert post-loop alive-task count returns to baseline (with a
//!     small slack for runtime book-keeping tasks).
//!
//! `tokio::runtime::RuntimeMetrics::num_alive_tasks` is **stable since
//! tokio 1.39** — no `tokio_unstable` cfg required.

use std::net::SocketAddr;
use std::time::Duration;

use rapid::cluster::ClusterBuilder;
use rapid::messaging::InProcessNetwork;

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_bootstrap_shutdown_does_not_leak_tasks() {
    let handle = tokio::runtime::Handle::current();
    // Allow the runtime to settle its own background tasks.
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let baseline = handle.metrics().num_alive_tasks();

    for iter in 0..5 {
        let net = InProcessNetwork::new();
        let base = 34_000 + iter * 10;
        let seed = ClusterBuilder::new(addr(base), net.clone())
            .with_settings(rapid::settings::Settings::for_tests())
            .start()
            .await
            .expect("seed bootstraps");
        let n1 = ClusterBuilder::new(addr(base + 1), net.clone())
            .with_settings(rapid::settings::Settings::for_tests())
            .join(addr(base))
            .await
            .expect("n1 joins");
        let n2 = ClusterBuilder::new(addr(base + 2), net.clone())
            .with_settings(rapid::settings::Settings::for_tests())
            .join(addr(base))
            .await
            .expect("n2 joins");
        let n3 = ClusterBuilder::new(addr(base + 3), net.clone())
            .with_settings(rapid::settings::Settings::for_tests())
            .join(addr(base))
            .await
            .expect("n3 joins");
        seed.shutdown().await;
        n1.shutdown().await;
        n2.shutdown().await;
        n3.shutdown().await;
    }

    // Give the runtime a beat to reap.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = handle.metrics().num_alive_tasks();
    // Strict gate: should be at most a handful above baseline (1 == the
    // currently-running test task itself; we allow up to +4 for runtime
    // internals).
    let delta = i64::try_from(after).unwrap() - i64::try_from(baseline).unwrap();
    assert!(
        delta <= 4,
        "alive-task delta {delta} after 5 bootstrap/shutdown cycles \
         (baseline={baseline}, after={after}) — orphan tasks suspected",
    );
}
