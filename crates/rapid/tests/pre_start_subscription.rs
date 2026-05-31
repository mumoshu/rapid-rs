//! F9 gate — pre-start subscribers observe the synthetic initial
//! `VIEW_CHANGE`. Java parity: `Builder.addSubscription(VIEW_CHANGE, cb)`
//! before `start`/`join`.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rapid::cluster::ClusterBuilder;
use rapid::events::ClusterEvent;
use rapid::messaging::InProcessNetwork;

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_start_subscriber_sees_initial_view_change() {
    let net = InProcessNetwork::new();
    let seen = Arc::new(AtomicUsize::new(0));
    let observed_size = Arc::new(parking_lot::Mutex::new(0usize));
    let seen_cb = seen.clone();
    let observed_cb = observed_size.clone();
    let cluster = ClusterBuilder::new(addr(36_000), net)
        .with_settings(rapid::settings::Settings::for_tests())
        .add_subscription(ClusterEvent::ViewChange, move |ev| {
            seen_cb.fetch_add(1, Ordering::SeqCst);
            *observed_cb.lock() = ev.membership.len();
        })
        .start()
        .await
        .expect("seed bootstraps");
    // Give the forwarder task a beat.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "callback fires exactly once"
    );
    assert_eq!(*observed_size.lock(), 1, "initial view = 1 seed");
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_start_subscriber_sees_join_view_with_all_members() {
    let net = InProcessNetwork::new();
    let seed = ClusterBuilder::new(addr(36_100), net.clone())
        .with_settings(rapid::settings::Settings::for_tests())
        .start()
        .await
        .expect("seed bootstraps");

    let initial = Arc::new(parking_lot::Mutex::new(None::<usize>));
    let initial_cb = initial.clone();
    let n1 = ClusterBuilder::new(addr(36_101), net.clone())
        .with_settings(rapid::settings::Settings::for_tests())
        .add_subscription(ClusterEvent::ViewChange, move |ev| {
            let mut g = initial_cb.lock();
            if g.is_none() {
                *g = Some(ev.membership.len());
            }
        })
        .join(addr(36_100))
        .await
        .expect("n1 joins");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        *initial.lock(),
        Some(2),
        "joiner's pre-start subscriber sees 2-node view"
    );
    seed.shutdown().await;
    n1.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_start_subscribe_does_not_replay() {
    let net = InProcessNetwork::new();
    let cluster = ClusterBuilder::new(addr(36_200), net)
        .with_settings(rapid::settings::Settings::for_tests())
        .start()
        .await
        .expect("seed bootstraps");
    // Subscribe AFTER start — must NOT see the initial event (Java
    // parity: callbacks added post-construction see only future
    // events). The `Cluster::initial_view_event()` accessor is still
    // available for callers that want the snapshot on demand.
    let mut sub = cluster.subscribe(ClusterEvent::ViewChange);
    let r = tokio::time::timeout(Duration::from_millis(100), sub.recv()).await;
    assert!(
        r.is_err(),
        "post-start subscriber must not observe the initial event"
    );
    cluster.shutdown().await;
}
