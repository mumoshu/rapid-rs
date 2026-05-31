//! Default tonic/gRPC transport.
//!
//! Server side: [`serve`] starts a `MembershipServiceServer` bound to a
//! socket and dispatches every inbound request to a [`RequestHandler`].
//!
//! Client side: [`GrpcClient`] connects to a single peer; [`GrpcPool`]
//! holds one channel per peer with lazy creation and a small expiry. The
//! pool implements [`MessagingClient`] and is what the production
//! `MembershipService` uses.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tonic::transport::{Channel, Server, Uri};
use tonic::{Request, Response, Status};

use crate::error::{Error, Result};
use crate::messaging::handler::RequestHandler;
use crate::messaging::traits::{MessagingClient, MessagingServer};
use crate::pb;

/// Run a gRPC server backed by the supplied request handler.
///
/// Returns a handle that must be kept alive — dropping it shuts the server
/// down via the bound `oneshot::Sender`.
///
/// # Errors
///
/// Returns [`Error::Transport`] if the socket cannot be bound.
pub async fn serve<H>(addr: SocketAddr, handler: H) -> Result<GrpcServerHandle>
where
    H: RequestHandler + 'static,
{
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let svc = pb::membership_service_server::MembershipServiceServer::new(Adapter {
        handler: Arc::new(handler),
    });
    let (ready_tx, ready_rx) = oneshot::channel::<Result<SocketAddr>>();

    let task = tokio::spawn(async move {
        let bound = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                let _ = ready_tx.send(Err(Error::Transport(e.to_string())));
                return;
            }
        };
        let local = match bound.local_addr() {
            Ok(a) => a,
            Err(e) => {
                let _ = ready_tx.send(Err(Error::Transport(e.to_string())));
                return;
            }
        };
        let stream = tokio_stream::wrappers::TcpListenerStream::new(bound);
        let _ = ready_tx.send(Ok(local));

        let _ = Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(stream, async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    let local = ready_rx
        .await
        .map_err(|_| Error::Transport("grpc server failed to start".into()))??;

    Ok(GrpcServerHandle {
        addr: local,
        task: Mutex::new(Some(task)),
        shutdown: Mutex::new(Some(shutdown_tx)),
    })
}

/// Server handle.
pub struct GrpcServerHandle {
    addr: SocketAddr,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
}

#[async_trait]
impl MessagingServer for GrpcServerHandle {
    fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    async fn shutdown(&self) {
        if let Some(tx) = self.shutdown.lock().take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.lock().take() {
            task.abort();
        }
    }
}

impl Drop for GrpcServerHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.lock().take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.lock().take() {
            task.abort();
        }
    }
}

struct Adapter<H: RequestHandler> {
    handler: Arc<H>,
}

#[async_trait]
impl<H: RequestHandler + 'static> pb::membership_service_server::MembershipService for Adapter<H> {
    async fn send_request(
        &self,
        request: Request<pb::RapidRequest>,
    ) -> std::result::Result<Response<pb::RapidResponse>, Status> {
        let req = request.into_inner();
        self.handler
            .handle(req)
            .await
            .map(Response::new)
            .map_err(|e| Status::internal(e.to_string()))
    }
}

/// gRPC client.
#[derive(Clone)]
pub struct GrpcClient {
    inner: pb::membership_service_client::MembershipServiceClient<Channel>,
}

impl GrpcClient {
    /// Connect to a remote `host:port`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] when the underlying tonic channel fails
    /// to dial.
    pub async fn connect(remote: SocketAddr) -> Result<Self> {
        let uri: Uri = format!("http://{remote}")
            .parse()
            .map_err(|e: tonic::codegen::http::uri::InvalidUri| Error::Transport(e.to_string()))?;
        let channel = Channel::builder(uri).connect().await?;
        Ok(Self {
            inner: pb::membership_service_client::MembershipServiceClient::new(channel),
        })
    }

    /// Send a single request.
    ///
    /// # Errors
    ///
    /// Surfaces transport-level [`Error::Transport`] for both `tonic::Status`
    /// returns and underlying I/O failures.
    pub async fn send(&self, req: pb::RapidRequest) -> Result<pb::RapidResponse> {
        let mut c = self.inner.clone();
        Ok(c.send_request(req).await?.into_inner())
    }
}

#[async_trait]
impl MessagingClient for GrpcClient {
    async fn send(&self, _remote: SocketAddr, req: pb::RapidRequest) -> Result<pb::RapidResponse> {
        self.send(req).await
    }

    async fn send_best_effort(
        &self,
        _remote: SocketAddr,
        req: pb::RapidRequest,
    ) -> Result<pb::RapidResponse> {
        self.send(req).await
    }
}

/// Multi-peer gRPC client with per-remote channel caching.
///
/// Java parity: `GrpcClient.channelMap` (Guava `LoadingCache` with
/// `expireAfterAccess(30s)`). The Rust port stores `(client, last_access)`
/// pairs and prunes on access — same observable semantics.
#[derive(Clone)]
pub struct GrpcPool {
    inner: Arc<Mutex<HashMap<SocketAddr, CachedClient>>>,
    expire_after: std::time::Duration,
}

impl Default for GrpcPool {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
struct CachedClient {
    client: GrpcClient,
    last_access: std::time::Instant,
}

impl GrpcPool {
    /// Pool with the Java default 30 s access-expiry.
    #[must_use]
    pub fn new() -> Self {
        Self::with_expiry(std::time::Duration::from_secs(30))
    }

    /// Pool with a custom access-expiry. Useful for tests that want to
    /// shorten the window.
    #[must_use]
    pub fn with_expiry(expire_after: std::time::Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            expire_after,
        }
    }

    async fn client_for(&self, remote: SocketAddr) -> Result<GrpcClient> {
        // Prune-on-access: drop any entry whose last_access is older than
        // expire_after. Java's `expireAfterAccess` is conceptually the
        // same; we use a single pass at lookup time.
        let now = std::time::Instant::now();
        {
            let mut map = self.inner.lock();
            map.retain(|_, c| now.duration_since(c.last_access) < self.expire_after);
            if let Some(c) = map.get_mut(&remote) {
                c.last_access = now;
                return Ok(c.client.clone());
            }
        }
        let client = GrpcClient::connect(remote).await?;
        self.inner.lock().entry(remote).or_insert(CachedClient {
            client: client.clone(),
            last_access: now,
        });
        Ok(client)
    }

    fn invalidate(&self, remote: SocketAddr) {
        self.inner.lock().remove(&remote);
    }

    /// Snapshot of the current cache size — exposed for tests.
    #[must_use]
    pub fn cache_size(&self) -> usize {
        self.inner.lock().len()
    }

    /// Issue a request with up-to-`retries` retransmissions on failure.
    /// Each retry invalidates the cached channel for that peer (matches
    /// Java's `onCallFailure -> channelMap.invalidate(remote)`).
    ///
    /// # Errors
    ///
    /// Returns the last [`Error::Transport`] after exhausting retries.
    pub async fn send_with_retries(
        &self,
        remote: SocketAddr,
        req: pb::RapidRequest,
        retries: u8,
    ) -> Result<pb::RapidResponse> {
        let mut last_err: Option<Error> = None;
        for _ in 0..=retries {
            match self.client_for(remote).await {
                Err(e) => {
                    last_err = Some(e);
                    self.invalidate(remote);
                }
                Ok(c) => match c.send(req.clone()).await {
                    Ok(resp) => return Ok(resp),
                    Err(e) => {
                        last_err = Some(e);
                        self.invalidate(remote);
                    }
                },
            }
        }
        Err(last_err.unwrap_or_else(|| Error::Transport("send_with_retries: no attempts".into())))
    }
}

#[async_trait]
impl MessagingClient for GrpcPool {
    async fn send(&self, remote: SocketAddr, req: pb::RapidRequest) -> Result<pb::RapidResponse> {
        // Java default: 5 retries via `Settings.getGrpcDefaultRetries()`.
        self.send_with_retries(remote, req, 5).await
    }

    async fn send_best_effort(
        &self,
        remote: SocketAddr,
        req: pb::RapidRequest,
    ) -> Result<pb::RapidResponse> {
        // Java parity: best-effort calls Retries.callWithRetries(..., 0, ...).
        self.send_with_retries(remote, req, 0).await
    }
}
