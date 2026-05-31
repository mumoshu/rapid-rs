//! Mailbox backpressure characterisation — F4 gate.
//!
//! The actor's `mpsc::Sender<ServiceCommand>` has capacity 1024
//! (see `service::handle::MembershipService::spawn`). When producers
//! outrun the actor, `send.await` should *suspend* (back-pressure)
//! rather than panic. We saturate the mailbox with a slow handler and
//! assert:
//!  - No panics, no dropped commands.
//!  - The actor eventually drains and a final shutdown completes.
//!
//! Not a perf benchmark; a correctness gate for the mailbox protocol.

use std::net::SocketAddr;
use std::time::Duration;

use rapid::cluster::ClusterBuilder;
use rapid::messaging::InProcessNetwork;

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn high_volume_apply_proposals_dont_drop() {
    let net = InProcessNetwork::new();
    let seed = ClusterBuilder::new(addr(33_000), net)
        .with_settings(rapid::settings::Settings::for_tests())
        .start()
        .await
        .expect("seed bootstraps");

    // The seed is a 1-node cluster. apply_proposal with an empty vec
    // is a no-op view-change that touches the actor mailbox without
    // changing the view (no joiner to add, no incumbent to drop).
    // 2048 sends → exceeds the 1024 mailbox capacity; producers must
    // back-pressure on `send.await`.
    let svc = seed.service().clone();
    let mut handles = Vec::with_capacity(2048);
    for _ in 0..2048 {
        let svc = svc.clone();
        handles.push(tokio::spawn(
            async move { svc.apply_proposal(vec![]).await },
        ));
    }
    for h in handles {
        h.await.expect("task").expect("apply_proposal");
    }

    // Verify the actor is still healthy.
    let size = seed.membership_size().await.expect("alive after burst");
    assert_eq!(size, 1, "1-node cluster preserved after burst");
    tokio::time::timeout(Duration::from_secs(2), seed.shutdown())
        .await
        .expect("shutdown completes after burst");
}
