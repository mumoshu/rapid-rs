//! F10 gate — `TransportChoice::Custom` accepts a caller-supplied
//! `MessagingClient` / `MessagingServer` pair.
//!
//! We exercise the variant with a thin shim that delegates everything
//! to `InProcessNetwork` so we can compare it to the equivalent
//! `InProcess` path. Production callers would plug in a mock or a
//! third-party transport (QUIC, shared-memory IPC, …) here.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::future::FutureExt;

use rapid::cluster::{Cluster, ClusterBuilder, TransportChoice};
use rapid::error::Result;
use rapid::messaging::{
    InProcessNetwork, InProcessServerHandle, MessagingClient, MessagingServer, RequestHandler,
};
use rapid::pb;
use rapid::service::ServiceRequestHandler;

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

/// Trivial wrapper that delegates to a `InProcessNetwork`. Stands in
/// for any real custom transport.
struct CustomClient {
    inner: rapid::messaging::InProcessClient,
}

#[async_trait]
impl MessagingClient for CustomClient {
    async fn send(&self, remote: SocketAddr, req: pb::RapidRequest) -> Result<pb::RapidResponse> {
        self.inner.send(remote, req).await
    }
    async fn send_best_effort(
        &self,
        remote: SocketAddr,
        req: pb::RapidRequest,
    ) -> Result<pb::RapidResponse> {
        self.inner.send_best_effort(remote, req).await
    }
}

struct CustomServer {
    inner: InProcessServerHandle,
}

#[async_trait]
impl MessagingServer for CustomServer {
    fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr()
    }
    async fn shutdown(&self) {
        self.inner.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn custom_transport_two_node_bootstraps() {
    let net = InProcessNetwork::new();

    let seed = build_with_custom(addr(37_000), net.clone(), None).await;
    let joiner = build_with_custom(addr(37_001), net.clone(), Some(addr(37_000))).await;

    let members = joiner.memberlist().await.expect("memberlist");
    assert_eq!(members.len(), 2);
    let seed_members = seed.memberlist().await.expect("seed memberlist");
    assert_eq!(seed_members.len(), 2);

    seed.shutdown().await;
    joiner.shutdown().await;
}

async fn build_with_custom(
    listen: SocketAddr,
    net: InProcessNetwork,
    seed: Option<SocketAddr>,
) -> Cluster {
    let client_inner = net.client();
    let client: Arc<dyn MessagingClient> = Arc::new(CustomClient {
        inner: client_inner,
    });
    let net_for_server = net.clone();
    let server_factory: rapid::cluster::ServerFactory =
        Box::new(move |addr: SocketAddr, handler: ServiceRequestHandler| {
            async move {
                let handle = net_for_server.spawn(addr, ErasedHandler(handler));
                Ok::<Box<dyn MessagingServer>, rapid::error::Error>(Box::new(CustomServer {
                    inner: handle,
                }))
            }
            .boxed()
        });

    let transport = TransportChoice::Custom {
        client,
        server_factory,
    };

    let builder = ClusterBuilder::with_transport(listen, transport)
        .with_settings(rapid::settings::Settings::for_tests());
    match seed {
        Some(s) => tokio::time::timeout(Duration::from_secs(5), builder.join(s))
            .await
            .expect("join in budget")
            .expect("join succeeds"),
        None => builder.start().await.expect("seed bootstraps"),
    }
}

struct ErasedHandler(ServiceRequestHandler);

#[async_trait]
impl RequestHandler for ErasedHandler {
    async fn handle(&self, req: pb::RapidRequest) -> Result<pb::RapidResponse> {
        self.0.handle(req).await
    }
}
