//! In-process channel-based transport.
//!
//! `InProcessNetwork` is the fabric: a `HashMap<SocketAddr, Sender<…>>` that
//! routes requests to the per-server inbox. `InProcessClient` looks up the
//! destination on every `send` so handlers can be hot-swapped.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::messaging::fault_injection::{Disposition, EnvelopeFilter};
use crate::messaging::handler::RequestHandler;
use crate::messaging::traits::{MessagingClient, MessagingServer};
use crate::pb;

type Inbox = tokio::sync::mpsc::Sender<Envelope>;

struct Envelope {
    req: pb::RapidRequest,
    reply: oneshot::Sender<Result<pb::RapidResponse>>,
}

/// The fabric. Handles are cheap to clone.
#[derive(Clone, Default)]
pub struct InProcessNetwork {
    inner: Arc<RwLock<HashMap<SocketAddr, Inbox>>>,
    interceptor: Arc<RwLock<Option<Arc<dyn EnvelopeFilter>>>>,
}

impl InProcessNetwork {
    /// Create an empty network.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Install an [`EnvelopeFilter`] that runs on every outbound
    /// `send`/`send_best_effort` before the envelope reaches the
    /// destination's inbox. Subsequent calls overwrite. `None`
    /// clears any prior filter.
    ///
    /// Test-only API — production code never calls this.
    pub fn set_interceptor(&self, filter: Option<Arc<dyn EnvelopeFilter>>) {
        *self.interceptor.write() = filter;
    }

    fn current_interceptor(&self) -> Option<Arc<dyn EnvelopeFilter>> {
        self.interceptor.read().clone()
    }

    /// Spawn a server at `addr` driven by `handler`. Returns a handle that
    /// must be kept alive — dropping it shuts the server down.
    #[must_use]
    pub fn spawn<H>(&self, addr: SocketAddr, handler: H) -> InProcessServerHandle
    where
        H: RequestHandler + 'static,
    {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Envelope>(64);
        self.inner.write().insert(addr, tx);
        let handler = Arc::new(handler);
        let task = tokio::spawn(async move {
            while let Some(env) = rx.recv().await {
                // Spawn a per-envelope task so a slow / parked handler
                // doesn't head-of-line-block subsequent envelopes. This
                // matches the gRPC server's per-call concurrency.
                let handler = Arc::clone(&handler);
                tokio::spawn(async move {
                    let resp = handler.handle(env.req).await;
                    let _ = env.reply.send(resp);
                });
            }
        });
        InProcessServerHandle {
            addr,
            task: parking_lot::Mutex::new(Some(task)),
            network: self.clone(),
        }
    }

    /// Build a client backed by this network.
    #[must_use]
    pub fn client(&self) -> InProcessClient {
        InProcessClient {
            network: self.clone(),
        }
    }

    fn route(&self, addr: SocketAddr) -> Option<Inbox> {
        self.inner.read().get(&addr).cloned()
    }

    fn unregister(&self, addr: SocketAddr) {
        self.inner.write().remove(&addr);
    }
}

/// Handle to an in-process server.
pub struct InProcessServerHandle {
    addr: SocketAddr,
    task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    network: InProcessNetwork,
}

impl Drop for InProcessServerHandle {
    fn drop(&mut self) {
        self.network.unregister(self.addr);
        if let Some(t) = self.task.lock().take() {
            // Drop-time fallback. Aborts mid-flight handlers; the async
            // `shutdown()` method is the preferred path.
            t.abort();
        }
    }
}

#[async_trait]
impl MessagingServer for InProcessServerHandle {
    fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    async fn shutdown(&self) {
        // Cooperative drain: unregister so no new envelopes arrive, then
        // wait for the inbox loop to exit by itself (it does so once its
        // `rx.recv()` returns `None`, i.e. once every cloned sender — the
        // one in the network map being the last — is dropped).
        self.network.unregister(self.addr);
        let task = self.task.lock().take();
        if let Some(task) = task {
            // Wait up to 1s for the drain. If it overruns we fall back to
            // abort. This matches Java's `server.awaitTermination(0, ...)`
            // which is also bounded.
            let bounded = tokio::time::timeout(std::time::Duration::from_secs(1), task).await;
            if bounded.is_err() {
                // Cancellation token expired — we have no other handle to
                // the join, so we leave the task to be GC'd by the
                // runtime. This is rare; the inbox loop normally exits in
                // microseconds after unregister.
            }
        }
    }
}

/// Channel-based client.
#[derive(Clone)]
pub struct InProcessClient {
    network: InProcessNetwork,
}

impl InProcessClient {
    async fn dispatch(
        &self,
        remote: SocketAddr,
        req: pb::RapidRequest,
    ) -> Result<pb::RapidResponse> {
        // Test-only fault injection: consult the interceptor (if any)
        // before any I/O. Drop → fail like a real timeout would;
        // Delay → sleep and proceed.
        if let Some(filter) = self.network.current_interceptor() {
            match filter.filter(remote, &req) {
                Disposition::Pass => {}
                Disposition::Drop => {
                    return Err(Error::Transport(format!(
                        "in-process interceptor dropped envelope bound for {remote}"
                    )));
                }
                Disposition::Delay(d) => {
                    tokio::time::sleep(d).await;
                }
            }
        }
        let inbox = self
            .network
            .route(remote)
            .ok_or_else(|| Error::Transport(format!("no server bound at {remote}")))?;
        let (tx, rx) = oneshot::channel();
        inbox
            .send(Envelope { req, reply: tx })
            .await
            .map_err(|_| Error::Transport(format!("server at {remote} closed inbox")))?;
        rx.await
            .map_err(|_| Error::Transport(format!("server at {remote} dropped reply")))?
    }
}

#[async_trait]
impl MessagingClient for InProcessClient {
    async fn send(&self, remote: SocketAddr, req: pb::RapidRequest) -> Result<pb::RapidResponse> {
        // In-process transport doesn't retry — the failure modes are
        // deterministic (server gone, server closed inbox). Mirrors Java's
        // in-process behaviour where the in-process gRPC stack also has no
        // network jitter to retry against.
        self.dispatch(remote, req).await
    }

    async fn send_best_effort(
        &self,
        remote: SocketAddr,
        req: pb::RapidRequest,
    ) -> Result<pb::RapidResponse> {
        self.dispatch(remote, req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::handler::ProbeOnlyHandler;
    use crate::proto_traits;

    fn addr(p: u16) -> SocketAddr {
        format!("127.0.0.1:{p}").parse().unwrap()
    }

    #[tokio::test]
    async fn probe_round_trip() {
        let net = InProcessNetwork::new();
        let _h = net.spawn(addr(9001), ProbeOnlyHandler);
        let client = net.client();
        let req = proto_traits::probe_request(pb::ProbeMessage::default());
        let resp = client.send(addr(9001), req).await.unwrap();
        let Some(pb::rapid_response::Content::ProbeResponse(p)) = resp.content else {
            panic!("expected ProbeResponse");
        };
        assert_eq!(p.status(), pb::NodeStatus::Ok);
    }

    #[tokio::test]
    async fn missing_server_is_transport_error() {
        let net = InProcessNetwork::new();
        let client = net.client();
        let req = proto_traits::probe_request(pb::ProbeMessage::default());
        let err = client.send(addr(9002), req).await.unwrap_err();
        assert!(matches!(err, Error::Transport(_)));
    }
}
