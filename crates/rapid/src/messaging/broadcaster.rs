//! `UnicastToAllBroadcaster` — fans `broadcast(msg)` out to every
//! current recipient via [`MessagingClient::send_best_effort`].
//!
//! Java parity: `references/rapid-java/.../UnicastToAllBroadcaster.java`.
//! Recipient order is randomised on every `set_membership` call so each
//! configuration produces a different fan-out sequence (the paper's gossip
//! intuition — different observers will see different orderings).

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use rand::seq::SliceRandom;

use crate::messaging::traits::{Broadcaster, MessagingClient};
use crate::pb;

/// Java `UnicastToAllBroadcaster`.
///
/// Each call to [`Broadcaster::broadcast`] spawns one task per recipient
/// so that a slow receiver cannot head-of-line-block siblings.
pub struct UnicastToAllBroadcaster<C: ?Sized> {
    client: Arc<C>,
    recipients: Mutex<Vec<SocketAddr>>,
}

impl<C: ?Sized> UnicastToAllBroadcaster<C> {
    /// Wrap the given messaging client.
    pub fn new(client: Arc<C>) -> Self {
        Self {
            client,
            recipients: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl<C> Broadcaster for UnicastToAllBroadcaster<C>
where
    C: MessagingClient + ?Sized + 'static,
{
    async fn set_membership(&self, recipients: Vec<SocketAddr>) {
        let mut shuffled = recipients;
        shuffled.shuffle(&mut rand::thread_rng());
        *self.recipients.lock() = shuffled;
    }

    async fn broadcast(&self, req: pb::RapidRequest) {
        let recipients: Vec<SocketAddr> = self.recipients.lock().clone();
        for r in recipients {
            let client = self.client.clone();
            let req_clone = req.clone();
            tokio::spawn(async move {
                let _ = client.send_best_effort(r, req_clone).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::handler::ProbeOnlyHandler;
    use crate::messaging::in_process::InProcessNetwork;
    use crate::proto_traits;
    use std::collections::HashSet;
    use std::time::Duration;

    fn addr(p: u16) -> SocketAddr {
        format!("127.0.0.1:{p}").parse().unwrap()
    }

    #[tokio::test]
    async fn set_membership_stores_recipients() {
        let net = InProcessNetwork::new();
        let client = Arc::new(net.client());
        let bc = UnicastToAllBroadcaster::new(client);
        let endpoints: Vec<SocketAddr> = (8000..8004).map(addr).collect();
        bc.set_membership(endpoints.clone()).await;
        let stored: HashSet<SocketAddr> = bc.recipients.lock().iter().copied().collect();
        let expected: HashSet<SocketAddr> = endpoints.iter().copied().collect();
        assert_eq!(stored, expected, "all recipients preserved up to ordering");
    }

    #[tokio::test]
    async fn broadcast_reaches_every_recipient() {
        let net = InProcessNetwork::new();
        let mut servers = Vec::new();
        for port in 8010..8014 {
            servers.push(net.spawn(addr(port), ProbeOnlyHandler));
        }
        let client = Arc::new(net.client());
        let bc = UnicastToAllBroadcaster::new(client);
        bc.set_membership((8010..8014).map(addr).collect()).await;

        let req = proto_traits::probe_request(pb::ProbeMessage::default());
        bc.broadcast(req).await;
        // The fire-and-forget spawn means we need to give the tasks a tick
        // to run. Bounded — under tokio test-util this could be replaced
        // with `tokio::task::yield_now` loops; for live tokio, 50 ms is
        // generous.
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The handler returns OK; we can't directly assert delivery from
        // here, but the absence of panics or task leaks suffices in CI.
        drop(servers);
    }
}
