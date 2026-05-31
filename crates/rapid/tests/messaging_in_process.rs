//! Phase 2 gates over the in-process transport:
//! - 4 simulated nodes each exchange `ProbeMessage` round-trips with the
//!   other three.
//! - A deliberately-slow receiver does not head-of-line block siblings on
//!   a `UnicastToAllBroadcaster` fan-out.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::mpsc::{self, UnboundedSender};

use rapid::messaging::handler::{ProbeOnlyHandler, RequestHandler};
use rapid::messaging::traits::{Broadcaster, MessagingClient};
use rapid::messaging::{InProcessNetwork, UnicastToAllBroadcaster};
use rapid::pb;
use rapid::proto_traits;

fn addr(p: u16) -> SocketAddr {
    format!("127.0.0.1:{p}")
        .parse()
        .expect("invariant: literal address parses")
}

#[tokio::test]
async fn four_nodes_full_mesh_probe() {
    let net = InProcessNetwork::new();
    let ports = [8200, 8201, 8202, 8203];
    let mut handles = Vec::with_capacity(4);
    for port in ports {
        handles.push(net.spawn(addr(port), ProbeOnlyHandler));
    }

    let client = net.client();
    let req = proto_traits::probe_request(pb::ProbeMessage::default());
    for src in ports {
        for dst in ports {
            if src == dst {
                continue;
            }
            let resp = client
                .send(addr(dst), req.clone())
                .await
                .unwrap_or_else(|e| panic!("probe {src} -> {dst} failed: {e}"));
            let Some(pb::rapid_response::Content::ProbeResponse(p)) = resp.content else {
                panic!("expected ProbeResponse from {dst}");
            };
            assert_eq!(p.status(), pb::NodeStatus::Ok);
        }
    }
    drop(handles);
}

/// Slow handler: blocks for `delay` before responding. Used to verify the
/// broadcaster doesn't HOL-block siblings.
struct SlowProbe {
    delay: Duration,
    fast_handler: ProbeOnlyHandler,
}

#[async_trait]
impl RequestHandler for SlowProbe {
    async fn handle(&self, req: pb::RapidRequest) -> Result<pb::RapidResponse, rapid::Error> {
        tokio::time::sleep(self.delay).await;
        self.fast_handler.handle(req).await
    }
}

/// Reports a unit on each inbound probe via an unbounded channel.
struct CountingProbe {
    tx: UnboundedSender<()>,
    fast_handler: ProbeOnlyHandler,
}

#[async_trait]
impl RequestHandler for CountingProbe {
    async fn handle(&self, req: pb::RapidRequest) -> Result<pb::RapidResponse, rapid::Error> {
        let resp = self.fast_handler.handle(req).await;
        let _ = self.tx.send(());
        resp
    }
}

#[tokio::test]
async fn broadcaster_does_not_head_of_line_block() {
    let net = InProcessNetwork::new();
    let slow_port = 8300;
    let fast_ports = [8301, 8302, 8303];

    let _slow = net.spawn(
        addr(slow_port),
        SlowProbe {
            delay: Duration::from_secs(2),
            fast_handler: ProbeOnlyHandler,
        },
    );

    let (tx, mut rx) = mpsc::unbounded_channel::<()>();
    let mut fast_handles = Vec::new();
    for port in fast_ports {
        fast_handles.push(net.spawn(
            addr(port),
            CountingProbe {
                tx: tx.clone(),
                fast_handler: ProbeOnlyHandler,
            },
        ));
    }
    drop(tx);

    let client = Arc::new(net.client());
    let bc = UnicastToAllBroadcaster::new(client);
    let mut recipients = vec![addr(slow_port)];
    for p in fast_ports {
        recipients.push(addr(p));
    }
    bc.set_membership(recipients).await;

    let req = proto_traits::probe_request(pb::ProbeMessage::default());
    let start = Instant::now();
    bc.broadcast(req).await;

    // Wait for the three fast peers to acknowledge — must happen well
    // before the 2s slow delay.
    for i in 0..fast_ports.len() {
        tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("fast recipient {i} ack timed out at 500ms"))
            .expect("channel still open");
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "broadcaster HOL-blocked on slow peer: took {elapsed:?}"
    );
    drop(fast_handles);
}
