//! `ClusterBuilder` — Java `Cluster.Builder` analogue: the configuration
//! surface and transport selection. The bootstrap logic (`start`/`join`)
//! lives in [`super::bootstrap`].

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;

use crate::clock::{Clock, TokioClock};
use crate::error::Result;
use crate::events::{ClusterEvent, ClusterStatusChange};
use crate::messaging::in_process::InProcessNetwork;
use crate::messaging::timed_client::MessageTimeouts;
use crate::messaging::traits::{MessagingClient, MessagingServer};
use crate::monitoring::factory::EdgeFailureDetectorFactory;
use crate::pb;
use crate::service::ServiceRequestHandler;
use crate::settings::Settings;

/// Pre-start subscription callback. F9 stores these in the builder
/// so they're wired up *before* the actor publishes its synthetic
/// initial `VIEW_CHANGE`. Mirrors Java `Builder.addSubscription`.
pub(super) type PreSubscription = (
    ClusterEvent,
    Box<dyn FnMut(ClusterStatusChange) + Send + 'static>,
);

/// Builder for [`Cluster`](super::Cluster).
pub struct ClusterBuilder {
    pub(super) listen_addr: SocketAddr,
    pub(super) metadata: pb::Metadata,
    pub(super) settings: Settings,
    pub(super) transport: TransportChoice,
    pub(super) clock: Arc<dyn Clock>,
    pub(super) fd_factory: Option<Arc<dyn EdgeFailureDetectorFactory>>,
    /// `None` → derive from `settings` (Java parity path). `Some` →
    /// explicit override supplied via `with_timeouts`.
    pub(super) timeouts_override: Option<MessageTimeouts>,
    /// Callbacks attached via `add_subscription` *before* `start` /
    /// `join`. The actor emits an initial `VIEW_CHANGE` once these
    /// are wired up so they see the bootstrap view.
    pub(super) pre_subscriptions: Vec<PreSubscription>,
}

/// Asynchronous factory returning a `MessagingServer` ready to receive
/// requests. Invoked by `finish_cluster` once the `MembershipService`
/// actor and `ServiceRequestHandler` are available.
pub type ServerFactory = Box<
    dyn FnOnce(
            SocketAddr,
            ServiceRequestHandler,
        )
            -> futures_util::future::BoxFuture<'static, Result<Box<dyn MessagingServer>>>
        + Send,
>;

/// Which transport `ClusterBuilder` should use.
///
/// - [`TransportChoice::InProcess`] is the deterministic test fixture
///   (channel-based).
/// - [`TransportChoice::Grpc`] is the production carrier (tonic over TCP).
/// - [`TransportChoice::Custom`] lets callers plug an arbitrary
///   [`MessagingClient`]/[`MessagingServer`] pair (mock transport, QUIC,
///   shared-memory IPC, etc.). The internal types already hold trait
///   objects for both, so this variant adds no new dynamic-dispatch
///   surface area — the vtable cost is dominated by async I/O.
///
/// See `PLAN-PARITY.md` § F10 for the rationale behind enum-variant DI
/// over type-parameterised builders.
#[allow(clippy::large_enum_variant)]
pub enum TransportChoice {
    /// In-process channel transport.
    InProcess(InProcessNetwork),
    /// Real gRPC over a TCP listener bound to `listen_addr`.
    Grpc,
    /// Caller-supplied transport. `client` is the already-constructed
    /// outbound carrier. `server_factory` is invoked once during
    /// `finish_cluster` with the bound address + a request handler
    /// wrapping the live `MembershipService`.
    Custom {
        /// The client used by every outbound `send` / `send_best_effort`.
        client: Arc<dyn MessagingClient>,
        /// Async factory producing the server bound to `listen_addr`.
        server_factory: ServerFactory,
    },
}

impl ClusterBuilder {
    /// Start a new builder bound to `listen_addr` on the in-process
    /// transport.
    #[must_use]
    pub fn new(listen_addr: SocketAddr, network: InProcessNetwork) -> Self {
        Self {
            listen_addr,
            metadata: pb::Metadata::default(),
            settings: Settings::default(),
            transport: TransportChoice::InProcess(network),
            clock: Arc::new(TokioClock),
            fd_factory: None,
            timeouts_override: None,
            pre_subscriptions: Vec::new(),
        }
    }

    /// Start a new builder bound to `listen_addr` on the real gRPC
    /// transport. Mirrors Java's default (`Settings.useInProcessTransport
    /// = false`).
    #[must_use]
    pub fn with_grpc(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            metadata: pb::Metadata::default(),
            settings: Settings::default(),
            transport: TransportChoice::Grpc,
            clock: Arc::new(TokioClock),
            fd_factory: None,
            timeouts_override: None,
            pre_subscriptions: Vec::new(),
        }
    }

    /// Start a new builder bound to `listen_addr` with a caller-supplied
    /// [`TransportChoice`]. F10 entry point — used by tests with mock
    /// transports and by applications plugging in a third-party
    /// messaging stack (QUIC, IPC, …). For the built-in transports use
    /// [`Self::new`] (in-process) or [`Self::with_grpc`] (production).
    #[must_use]
    pub fn with_transport(listen_addr: SocketAddr, transport: TransportChoice) -> Self {
        Self {
            listen_addr,
            metadata: pb::Metadata::default(),
            settings: Settings::default(),
            transport,
            clock: Arc::new(TokioClock),
            fd_factory: None,
            timeouts_override: None,
            pre_subscriptions: Vec::new(),
        }
    }

    /// Java parity for `Builder.addSubscription`. Register a callback
    /// against `event` *before* the cluster boots. The first such
    /// callback receives a synthetic `VIEW_CHANGE` listing every
    /// current member as `UP` — matching Java's behaviour where
    /// `MembershipService`'s constructor fires `VIEW_CHANGE` to
    /// pre-attached subscribers so applications observe the bootstrap
    /// view atomically.
    ///
    /// Each subscription runs in its own `tokio::spawn`ed forwarder,
    /// so a slow callback can't head-of-line block siblings.
    #[must_use]
    pub fn add_subscription<F>(mut self, event: ClusterEvent, cb: F) -> Self
    where
        F: FnMut(ClusterStatusChange) + Send + 'static,
    {
        self.pre_subscriptions.push((event, Box::new(cb)));
        self
    }

    /// Override the per-message-type timeout policy. When unset, the
    /// timeouts are derived from
    /// [`Settings::grpc_default_timeout`]/`grpc_join_timeout`/`grpc_probe_timeout`
    /// (Java parity). Use this when you need a non-Settings-shaped
    /// override (e.g., asymmetric per-call deadlines in tests).
    #[must_use]
    pub fn with_timeouts(mut self, timeouts: MessageTimeouts) -> Self {
        self.timeouts_override = Some(timeouts);
        self
    }

    /// Install a custom `EdgeFailureDetectorFactory`. When `None` (the
    /// default), `finish_cluster` installs `PingPongFactory` for the
    /// gRPC transport and `NoOpFactory` for the in-process transport.
    #[must_use]
    pub fn with_failure_detector_factory(
        mut self,
        factory: Arc<dyn EdgeFailureDetectorFactory>,
    ) -> Self {
        self.fd_factory = Some(factory);
        self
    }

    /// Set application metadata for this node.
    #[must_use]
    pub fn with_metadata<K, V, I>(mut self, entries: I) -> Self
    where
        K: Into<String>,
        V: Into<Bytes>,
        I: IntoIterator<Item = (K, V)>,
    {
        let mut m = pb::Metadata::default();
        for (k, v) in entries {
            m.metadata.insert(k.into(), v.into().to_vec());
        }
        self.metadata = m;
        self
    }

    /// Override settings.
    #[must_use]
    pub fn with_settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }

    /// Override the clock (tests).
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }
}
