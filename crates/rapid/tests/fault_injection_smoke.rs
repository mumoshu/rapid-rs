//! F7 acceptance gate. Four smoke tests covering each harness piece.

mod common;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rapid::cluster::ClusterBuilder;
use rapid::messaging::fault_injection::{Disposition, DropAtDests, FirstN, MessageKind};
use rapid::messaging::traits::MessagingClient;
use rapid::messaging::{EnvelopeFilter, InProcessNetwork, ProbeOnlyHandler};
use rapid::monitoring::{Blacklist, StaticFailureDetectorFactory};
use rapid::pb;
use rapid::proto_traits;

use common::direct_paxos_bus::{ep, DirectPaxosBus};

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

// ----- 1. FirstN drops exactly N matching envelopes, then opens up.

#[tokio::test]
async fn first_n_drops_then_passes() {
    let net = InProcessNetwork::new();
    let server_addr = addr(40_001);
    let _handle = net.spawn(server_addr, ProbeOnlyHandler);
    net.set_interceptor(Some(Arc::new(FirstN::new(3, MessageKind::Probe))));
    let client = net.client();
    let req = proto_traits::probe_request(pb::ProbeMessage::default());
    // Drops 1, 2, 3.
    for i in 0..3 {
        let err = client.send(server_addr, req.clone()).await.unwrap_err();
        assert!(
            matches!(err, rapid::error::Error::Transport(_)),
            "iter {i}: expected Transport error, got {err:?}",
        );
    }
    // 4th onwards: passes through.
    for i in 0..5 {
        client
            .send(server_addr, req.clone())
            .await
            .unwrap_or_else(|e| panic!("iter {i}: expected pass, got {e:?}"));
    }
}

// ----- 2. DropAtDests is per-destination, not per-kind alone.

#[tokio::test]
async fn drop_at_dests_isolates_targets() {
    let net = InProcessNetwork::new();
    let target = addr(40_010);
    let bystander = addr(40_011);
    let _h1 = net.spawn(target, ProbeOnlyHandler);
    let _h2 = net.spawn(bystander, ProbeOnlyHandler);
    let mut dests = HashSet::new();
    dests.insert(target);
    net.set_interceptor(Some(Arc::new(DropAtDests::new(
        2,
        Some(MessageKind::Probe),
        dests,
    ))));
    let client = net.client();
    let req = proto_traits::probe_request(pb::ProbeMessage::default());
    // Target: 2 drops, then OK.
    assert!(client.send(target, req.clone()).await.is_err());
    assert!(client.send(target, req.clone()).await.is_err());
    assert!(client.send(target, req.clone()).await.is_ok());
    // Bystander: passes from the first envelope.
    assert!(client.send(bystander, req.clone()).await.is_ok());
    assert!(client.send(bystander, req.clone()).await.is_ok());
}

// ----- 3. StaticFailureDetector fires for blacklisted subjects only.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_fd_fires_on_blacklist() {
    let net = InProcessNetwork::new();
    let seed = addr(40_020);
    let blacklist = Blacklist::new();
    let factory = Arc::new(StaticFailureDetectorFactory::new(
        blacklist.clone(),
        Arc::new(rapid::clock::TokioClock),
        Duration::from_millis(50),
    ));
    let seed_cluster = ClusterBuilder::new(seed, net.clone())
        .with_settings(rapid::settings::Settings::for_tests())
        .with_failure_detector_factory(factory.clone())
        .start()
        .await
        .expect("seed bootstraps");
    let joiner = ClusterBuilder::new(addr(40_021), net.clone())
        .with_settings(rapid::settings::Settings::for_tests())
        .with_failure_detector_factory(factory)
        .join(seed)
        .await
        .expect("joiner converges");
    // Subscribe to ViewChange BEFORE setting the blacklist so we
    // observe the consensus that follows.
    let mut sub = seed_cluster.subscribe(rapid::events::ClusterEvent::ViewChange);
    // Mark the joiner as failed. Within the next FD interval the seed
    // should observe a ViewChange dropping it.
    blacklist.add_failed_nodes([joiner.listen_endpoint().clone()]);
    let ev = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("ViewChange fires within budget")
        .expect("channel open");
    assert_eq!(ev.membership.len(), 1, "joiner kicked");
    seed_cluster.shutdown().await;
    joiner.shutdown().await;
}

// ----- 4. DirectPaxosBus end-to-end: classic round agrees post-failure.

#[test]
fn direct_paxos_bus_classic_recovery() {
    use common::direct_paxos_bus::MsgKind;
    let mut bus = DirectPaxosBus::new(5, 42);
    bus.drop_kind(MsgKind::FastRoundPhase2b);
    let proposal = vec![ep("10.0.0.1", 9999)];
    for i in 0..bus.len() {
        bus.propose(i, proposal.clone());
    }
    for i in 0..bus.len() {
        bus.start_classic_round(i);
    }
    assert!(bus.all_decided_same(&proposal));
}

// Suppress dead-code warning when only some tests are run.
#[allow(dead_code)]
fn _is_envelope_filter<F: EnvelopeFilter>(_: &F) {}
#[allow(dead_code)]
fn _disposition_compiles() -> Disposition {
    Disposition::Pass
}
