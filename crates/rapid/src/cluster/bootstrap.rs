//! Bootstrap wiring for [`ClusterBuilder`]: `start` (seed) and `join`
//! (two-phase) plus the messaging-stack assembly that both paths funnel
//! through (`finish_cluster`).

use std::net::SocketAddr;
use std::sync::Arc;

use uuid::Uuid;

use crate::clock::Clock;
use crate::cluster_join::{endpoint_from_socket, node_id_from_uuid, run_join_protocol};
use crate::cut_detector::MultiNodeCutDetector;
use crate::error::Result;
use crate::messaging::grpc::{self, GrpcPool, GrpcServerHandle};
use crate::messaging::in_process::InProcessServerHandle;
use crate::messaging::lazy_handler::LazyServiceHandler;
use crate::messaging::timed_client::{MessageTimeouts, TimedClient};
use crate::messaging::traits::{Broadcaster, MessagingClient, MessagingServer};
use crate::messaging::UnicastToAllBroadcaster;
use crate::metadata::MetadataManager;
use crate::monitoring::factory::EdgeFailureDetectorFactory;
use crate::pb;
use crate::service::{MembershipService, ServiceRequestHandler, ServiceState};
use crate::settings::Settings;
use crate::view::MembershipView;

use super::builder::{ClusterBuilder, PreSubscription, TransportChoice};
use super::handle::Cluster;

impl ClusterBuilder {
    /// `Cluster.Builder.join(seed)` — bootstrap a new node by joining
    /// through `seed`. Runs the two-phase join protocol and constructs a
    /// `Cluster` from the final `JoinResponse`.
    ///
    /// # Terminology — `seed` (Java API parity, not BitTorrent/gossip)
    ///
    /// The parameter name `seed` matches the Java upstream
    /// (`Cluster.Builder.join(seedAddress)`). It does **not** mean a
    /// gossip seed: Rapid is not a gossip protocol (see
    /// [`crate::messaging::broadcaster`] — alerts fan out full-mesh,
    /// consensus is Paxos, monitoring is the deterministic K-ring,
    /// not random peer exchange).
    ///
    /// Semantically `seed` is **the address of any one node already
    /// in the cluster**, used only as the Phase-1 contact for the
    /// joiner's `PreJoinMessage`. The receiver responds with the
    /// current `configuration_id` plus the K observers the joiner
    /// must talk to in Phase 2 (see
    /// [`crate::service::handlers::handle_pre_join`]). The protocol
    /// gives `seed` no privileged role — any current member can
    /// answer. The original founder (the node that called
    /// [`Self::start`]) has no protocol-level distinction; if it
    /// dies, point a new joiner's `seed` at a surviving member.
    ///
    /// The Rapid paper (Suresh et al., USENIX ATC '18) just calls
    /// this "an existing member" / "a contact"; the `seed` naming
    /// is a Java library convention we keep for API parity. See
    /// `ClusterTest.concurrentNodeJoinsNetty` (mirrored in Rust as
    /// `concurrent_node_joins_multi_seed`) for a test that
    /// deliberately uses non-founder members as `seed`.
    ///
    /// # Errors
    /// Returns [`crate::error::Error::Transport`] /
    /// [`crate::error::Error::ProtocolRejected`] when the join protocol
    /// fails.
    pub async fn join(self, seed: SocketAddr) -> Result<Cluster> {
        tracing::info!(target: "rapid", listen = %self.listen_addr, %seed, "bootstrap.start");
        let timeouts = self
            .timeouts_override
            .unwrap_or_else(|| MessageTimeouts::from(&self.settings));
        let client: Arc<dyn MessagingClient> = build_client(&self.transport, &timeouts);
        let response = run_join_protocol(
            self.listen_addr,
            seed,
            &self.settings,
            client.as_ref(),
            self.clock.as_ref(),
            &self.metadata,
        )
        .await?;
        let endpoints = response.endpoints.clone();
        let identifiers = response.identifiers.clone();
        let metadata_pairs: Vec<(pb::Endpoint, pb::Metadata)> = response
            .metadata_keys
            .iter()
            .cloned()
            .zip(response.metadata_values.iter().cloned())
            .collect();
        let listen_endpoint = endpoint_from_socket(self.listen_addr);
        let view = MembershipView::bootstrap(self.settings.k, identifiers, endpoints)?;
        let cut_detector =
            MultiNodeCutDetector::new(self.settings.k, self.settings.h, self.settings.l)?;
        let mut metadata = MetadataManager::new();
        metadata.add(metadata_pairs);
        if !self.metadata.metadata.is_empty() {
            metadata.add([(listen_endpoint.clone(), self.metadata.clone())]);
        }
        Self::finish_cluster(
            self.listen_addr,
            listen_endpoint,
            self.transport,
            self.settings,
            self.clock,
            view,
            cut_detector,
            metadata,
            self.fd_factory,
            timeouts,
            self.pre_subscriptions,
        )
        .await
    }

    /// `Cluster.Builder.start()` — boot as a seed node.
    ///
    /// # Errors
    /// Returns [`crate::error::Error::Transport`] if the messaging server
    /// cannot bind.
    pub async fn start(self) -> Result<Cluster> {
        tracing::info!(target: "rapid", listen = %self.listen_addr, "bootstrap.start");
        let listen_endpoint = endpoint_from_socket(self.listen_addr);
        let node_id = node_id_from_uuid(Uuid::new_v4());
        let view =
            MembershipView::bootstrap(self.settings.k, [node_id], [listen_endpoint.clone()])?;
        let cut_detector =
            MultiNodeCutDetector::new(self.settings.k, self.settings.h, self.settings.l)?;
        let mut metadata = MetadataManager::new();
        if !self.metadata.metadata.is_empty() {
            metadata.add([(listen_endpoint.clone(), self.metadata.clone())]);
        }
        let timeouts = self
            .timeouts_override
            .unwrap_or_else(|| MessageTimeouts::from(&self.settings));
        Self::finish_cluster(
            self.listen_addr,
            listen_endpoint,
            self.transport,
            self.settings,
            self.clock,
            view,
            cut_detector,
            metadata,
            self.fd_factory,
            timeouts,
            self.pre_subscriptions,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_cluster(
        listen_addr: SocketAddr,
        listen_endpoint: pb::Endpoint,
        transport: TransportChoice,
        settings: Settings,
        clock: Arc<dyn Clock>,
        view: MembershipView,
        cut_detector: MultiNodeCutDetector,
        metadata: MetadataManager,
        fd_factory: Option<Arc<dyn EdgeFailureDetectorFactory>>,
        timeouts: MessageTimeouts,
        pre_subscriptions: Vec<PreSubscription>,
    ) -> Result<Cluster> {
        let raw_client = build_client(&transport, &timeouts);
        let broadcaster: Arc<dyn Broadcaster> =
            Arc::new(UnicastToAllBroadcaster::new(raw_client.clone()));
        let initial_recipients = endpoints_to_socket_addrs(&view.get_ring(0).unwrap_or_default());
        broadcaster.set_membership(initial_recipients).await;

        let (fd_notifier_tx, fd_notifier_rx) =
            tokio::sync::mpsc::channel::<crate::monitoring::factory::EdgeFailure>(64);
        let factory = resolve_fd_factory(&transport, fd_factory, &raw_client, &clock, &settings);

        let mut state = ServiceState::new(
            listen_endpoint.clone(),
            view,
            cut_detector,
            metadata,
            settings.clone(),
        )
        .with_broadcaster(broadcaster)
        .with_clock(clock.clone())
        .with_consensus_client(raw_client.clone());
        state = state.with_fd_factory(factory, fd_notifier_tx.clone());
        crate::service::consensus_dispatch::install_fast_paxos(&mut state);
        let service = MembershipService::spawn(state);
        let _batcher_handle = crate::service::alert_batcher::spawn_batcher_loop(
            clock.clone(),
            service.sender(),
            settings.batching_window,
        );
        // Pump FD edge-failure events into the actor mailbox.
        spawn_fd_notifier_pump(fd_notifier_rx, service.sender());

        // F9: register pre-start subscriptions *before* asking the
        // actor to publish its synthetic initial VIEW_CHANGE. The
        // actor processes commands FIFO, so subscribers are guaranteed
        // to be wired up before the event fans out.
        for (event, mut cb) in pre_subscriptions {
            let mut rx = service.subscribe(event);
            tokio::spawn(async move {
                while let Ok(ev) = rx.recv().await {
                    cb(ev);
                }
            });
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = service
            .sender()
            .send(crate::service::ServiceCommand::PublishInitialView { reply: tx })
            .await;
        let _ = rx.await;

        let server = spawn_server(transport, listen_addr, &service).await?;
        tracing::info!(target: "rapid", %listen_addr, "bootstrap.complete");
        Ok(Cluster {
            service,
            server,
            listen_addr,
            listen_endpoint,
            client: raw_client,
            settings,
            clock,
        })
    }
}

fn build_client(
    transport: &TransportChoice,
    timeouts: &MessageTimeouts,
) -> Arc<dyn MessagingClient> {
    let inner: Arc<dyn MessagingClient> = match transport {
        TransportChoice::InProcess(net) => Arc::new(net.client()),
        TransportChoice::Grpc => Arc::new(GrpcPool::new()),
        TransportChoice::Custom { client, .. } => client.clone(),
    };
    Arc::new(TimedClient::new(inner, *timeouts))
}

fn resolve_fd_factory(
    _transport: &TransportChoice,
    override_factory: Option<Arc<dyn EdgeFailureDetectorFactory>>,
    raw_client: &Arc<dyn MessagingClient>,
    clock: &Arc<dyn Clock>,
    settings: &Settings,
) -> Arc<dyn EdgeFailureDetectorFactory> {
    if let Some(f) = override_factory {
        return f;
    }
    // Default: real `PingPongFactory` on both transports. The detector
    // exchanges probes via the supplied `MessagingClient` regardless of
    // whether that's the in-process channel or real gRPC.
    Arc::new(crate::monitoring::PingPongFactory::new(
        pb::Endpoint::default(),
        raw_client.clone(),
        clock.clone(),
        settings.failure_detector_interval,
    ))
}

fn spawn_fd_notifier_pump(
    mut rx: tokio::sync::mpsc::Receiver<crate::monitoring::factory::EdgeFailure>,
    tx: tokio::sync::mpsc::Sender<crate::service::ServiceCommand>,
) {
    tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            if tx
                .send(crate::service::ServiceCommand::EdgeFailure(ev))
                .await
                .is_err()
            {
                return;
            }
        }
    });
}

pub(super) fn endpoints_to_socket_addrs(endpoints: &[pb::Endpoint]) -> Vec<SocketAddr> {
    endpoints
        .iter()
        .filter_map(|e| {
            let host = String::from_utf8(e.hostname.clone()).ok()?;
            let port = u16::try_from(e.port).ok()?;
            format!("{host}:{port}").parse().ok()
        })
        .collect()
}

async fn spawn_server(
    transport: TransportChoice,
    listen_addr: SocketAddr,
    service: &MembershipService,
) -> Result<Box<dyn MessagingServer>> {
    match transport {
        TransportChoice::InProcess(network) => {
            let handler = ServiceRequestHandler::new(service.clone());
            let handle: InProcessServerHandle = network.spawn(listen_addr, handler);
            Ok(Box::new(handle))
        }
        TransportChoice::Grpc => {
            // For gRPC we bind first, install a LazyServiceHandler that
            // returns BOOTSTRAPPING to probes, then promote it to the
            // real service. This matches Java's setMembershipService
            // pattern after server.start().
            let lazy = LazyServiceHandler::new();
            lazy.install(service.clone());
            let handle: GrpcServerHandle = grpc::serve(listen_addr, lazy).await?;
            Ok(Box::new(handle))
        }
        TransportChoice::Custom { server_factory, .. } => {
            let handler = ServiceRequestHandler::new(service.clone());
            server_factory(listen_addr, handler).await
        }
    }
}
