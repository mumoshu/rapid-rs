//! `PingPongFailureDetector` — the default `EdgeFailureDetector`.
//!
//! Bit-exact port of `references/rapid-java/.../PingPongFailureDetector.java`.
//! On each tick the detector sends a `ProbeMessage` to its subject; if the
//! response is `OK` the success counter resets; if the response is
//! `BOOTSTRAPPING` and the bootstrap counter crosses `BOOTSTRAP_COUNT_THRESHOLD`,
//! the edge is marked DOWN; transport errors increment the failure counter.
//! Once `FAILURE_THRESHOLD` consecutive failures have accumulated, the
//! configured [`EdgeFailureNotifier`] is invoked once.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::clock::Clock;
use crate::messaging::traits::MessagingClient;
use crate::monitoring::factory::{
    EdgeFailureDetector, EdgeFailureDetectorFactory, EdgeFailureNotifier,
};
use crate::pb;
use crate::proto_traits;

const FAILURE_THRESHOLD: u32 = 10;
const BOOTSTRAP_COUNT_THRESHOLD: u32 = 30;

/// Default ping-pong failure detector.
pub struct PingPongDetector {
    address: pb::Endpoint,
    subject: pb::Endpoint,
    client: Arc<dyn MessagingClient>,
    subject_addr: std::net::SocketAddr,
    notifier: EdgeFailureNotifier,
    failure_count: AtomicU32,
    bootstrap_response_count: AtomicU32,
    notified: Mutex<bool>,
    clock: Arc<dyn Clock>,
    interval: Duration,
}

impl PingPongDetector {
    /// Construct a detector. Returns `None` if `subject`'s endpoint cannot
    /// be parsed as a `SocketAddr` (hostname not UTF-8, port out of range).
    #[must_use]
    pub fn new(
        address: pb::Endpoint,
        subject: pb::Endpoint,
        client: Arc<dyn MessagingClient>,
        notifier: EdgeFailureNotifier,
        clock: Arc<dyn Clock>,
        interval: Duration,
    ) -> Option<Self> {
        let host = String::from_utf8(subject.hostname.clone()).ok()?;
        let port = u16::try_from(subject.port).ok()?;
        let subject_addr: std::net::SocketAddr = format!("{host}:{port}").parse().ok()?;
        Some(Self {
            address,
            subject,
            client,
            subject_addr,
            notifier,
            failure_count: AtomicU32::new(0),
            bootstrap_response_count: AtomicU32::new(0),
            notified: Mutex::new(false),
            clock,
            interval,
        })
    }

    async fn tick(&self) {
        if self.has_failed() {
            self.maybe_notify().await;
            return;
        }
        let req = proto_traits::probe_request(pb::ProbeMessage {
            sender: Some(self.address.clone()),
            payload: Vec::new(),
        });
        match self.client.send_best_effort(self.subject_addr, req).await {
            Ok(resp) => self.on_response(resp.content.as_ref()),
            Err(_) => self.on_failure(),
        }
    }

    fn has_failed(&self) -> bool {
        self.failure_count.load(Ordering::Relaxed) >= FAILURE_THRESHOLD
    }

    async fn maybe_notify(&self) {
        let should_notify = {
            let mut notified = self.notified.lock();
            if *notified {
                false
            } else {
                *notified = true;
                true
            }
        };
        if should_notify {
            self.notifier.notify().await;
        }
    }

    fn on_response(&self, content: Option<&pb::rapid_response::Content>) {
        let Some(pb::rapid_response::Content::ProbeResponse(p)) = content else {
            self.on_failure();
            return;
        };
        if pb::NodeStatus::try_from(p.status).unwrap_or(pb::NodeStatus::Ok)
            == pb::NodeStatus::Bootstrapping
        {
            let n = self
                .bootstrap_response_count
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            if n > BOOTSTRAP_COUNT_THRESHOLD {
                self.on_failure();
            }
        }
        // Success — Java keeps the failure counter but `hasFailed()`
        // short-circuits all future ticks once FAILURE_THRESHOLD is
        // reached. We mirror exactly.
    }

    fn on_failure(&self) {
        self.failure_count.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl EdgeFailureDetector for PingPongDetector {
    async fn run(self: Arc<Self>) {
        let _ = &self.subject; // suppress unused warning if subject ever stops being used.
        loop {
            self.clock.sleep(self.interval).await;
            self.tick().await;
            if *self.notified.lock() {
                return;
            }
        }
    }
}

/// `IEdgeFailureDetectorFactory.Factory` analogue.
pub struct PingPongFactory {
    address: pb::Endpoint,
    client: Arc<dyn MessagingClient>,
    clock: Arc<dyn Clock>,
    interval: Duration,
}

impl PingPongFactory {
    /// Construct a factory.
    #[must_use]
    pub fn new(
        address: pb::Endpoint,
        client: Arc<dyn MessagingClient>,
        clock: Arc<dyn Clock>,
        interval: Duration,
    ) -> Self {
        Self {
            address,
            client,
            clock,
            interval,
        }
    }
}

impl EdgeFailureDetectorFactory for PingPongFactory {
    fn create(
        &self,
        subject: pb::Endpoint,
        notifier: EdgeFailureNotifier,
    ) -> Arc<dyn EdgeFailureDetector> {
        if let Some(d) = PingPongDetector::new(
            self.address.clone(),
            subject.clone(),
            self.client.clone(),
            notifier.clone(),
            self.clock.clone(),
            self.interval,
        ) {
            Arc::new(d)
        } else {
            // Subject endpoint couldn't be parsed as a SocketAddr — fall
            // back to a no-op detector so the FD task lifecycle stays
            // intact. The cluster will eventually rebuild its FD set on
            // the next view change.
            crate::monitoring::no_op::NoOpFactory.create(subject, notifier)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicUsize;

    use tokio::sync::mpsc;

    use crate::clock::TokioClock;
    use crate::error::Result;
    use crate::monitoring::factory::EdgeFailure;
    use crate::types::ConfigurationId;

    /// Programmable client: yields a sequence of canned responses to
    /// successive `send_best_effort` calls. After the script runs out, it
    /// returns an error.
    struct ScriptedClient {
        script: Vec<ProbeOutcome>,
        index: AtomicUsize,
    }

    enum ProbeOutcome {
        Ok,
        Bootstrapping,
        Error,
    }

    impl ScriptedClient {
        fn new(script: Vec<ProbeOutcome>) -> Self {
            Self {
                script,
                index: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl MessagingClient for ScriptedClient {
        async fn send(
            &self,
            _remote: SocketAddr,
            _req: pb::RapidRequest,
        ) -> Result<pb::RapidResponse> {
            unreachable!("PingPongDetector calls send_best_effort only")
        }
        async fn send_best_effort(
            &self,
            _remote: SocketAddr,
            _req: pb::RapidRequest,
        ) -> Result<pb::RapidResponse> {
            let i = self.index.fetch_add(1, Ordering::Relaxed);
            let outcome = self.script.get(i).unwrap_or(&ProbeOutcome::Error);
            match outcome {
                ProbeOutcome::Ok => Ok(proto_traits::probe_response(pb::ProbeResponse {
                    status: pb::NodeStatus::Ok as i32,
                })),
                ProbeOutcome::Bootstrapping => {
                    Ok(proto_traits::probe_response(pb::ProbeResponse {
                        status: pb::NodeStatus::Bootstrapping as i32,
                    }))
                }
                ProbeOutcome::Error => Err(crate::error::Error::Transport("scripted error".into())),
            }
        }
    }

    fn ep(host: &str, port: i32) -> pb::Endpoint {
        pb::Endpoint {
            hostname: host.as_bytes().to_vec(),
            port,
        }
    }

    async fn drive_n_ticks(detector: &PingPongDetector, n: usize) {
        for _ in 0..n {
            detector.tick().await;
        }
    }

    #[tokio::test]
    async fn ten_failures_notifies_once() {
        let (tx, mut rx) = mpsc::channel::<EdgeFailure>(8);
        let client: Arc<dyn MessagingClient> = Arc::new(ScriptedClient::new(
            (0..15).map(|_| ProbeOutcome::Error).collect(),
        ));
        let detector = PingPongDetector::new(
            ep("127.0.0.1", 7700),
            ep("127.0.0.1", 7701),
            client,
            EdgeFailureNotifier::new(tx, ConfigurationId(1), ep("127.0.0.1", 7701)),
            Arc::new(TokioClock),
            Duration::from_millis(0),
        )
        .unwrap();
        drive_n_ticks(&detector, 10).await;
        // 11th tick observes hasFailed and notifies.
        drive_n_ticks(&detector, 1).await;
        let ev = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("notification arrives")
            .expect("channel open");
        assert_eq!(ev.subject.port, 7701);
        // Further ticks do not notify again.
        drive_n_ticks(&detector, 5).await;
        let again = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await;
        assert!(again.is_err(), "second notification should not fire");
    }

    #[tokio::test]
    async fn nine_failures_plus_one_success_does_not_notify() {
        let (tx, mut rx) = mpsc::channel::<EdgeFailure>(8);
        let mut script: Vec<ProbeOutcome> = (0..9).map(|_| ProbeOutcome::Error).collect();
        script.push(ProbeOutcome::Ok);
        let client: Arc<dyn MessagingClient> = Arc::new(ScriptedClient::new(script));
        let detector = PingPongDetector::new(
            ep("127.0.0.1", 7800),
            ep("127.0.0.1", 7801),
            client,
            EdgeFailureNotifier::new(tx, ConfigurationId(1), ep("127.0.0.1", 7801)),
            Arc::new(TokioClock),
            Duration::from_millis(0),
        )
        .unwrap();
        // 10 ticks total: 9 failures + 1 success. failure_count = 9 (not 10).
        drive_n_ticks(&detector, 10).await;
        let again = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await;
        assert!(again.is_err(), "9 failures + 1 success must not notify");
    }

    #[tokio::test]
    async fn bootstrap_responses_past_threshold_count_as_failures() {
        let (tx, mut rx) = mpsc::channel::<EdgeFailure>(8);
        // 31 bootstrapping responses — past BOOTSTRAP_COUNT_THRESHOLD (30).
        // The 31st bootstrapping response increments failure_count from 0 → 1.
        // We then need 9 more failures (10 total) to trigger notify. Make
        // the rest of the script be Errors.
        let mut script: Vec<ProbeOutcome> = (0..31).map(|_| ProbeOutcome::Bootstrapping).collect();
        script.extend((0..10).map(|_| ProbeOutcome::Error));
        let client: Arc<dyn MessagingClient> = Arc::new(ScriptedClient::new(script));
        let detector = PingPongDetector::new(
            ep("127.0.0.1", 7900),
            ep("127.0.0.1", 7901),
            client,
            EdgeFailureNotifier::new(tx, ConfigurationId(1), ep("127.0.0.1", 7901)),
            Arc::new(TokioClock),
            Duration::from_millis(0),
        )
        .unwrap();
        // 31 bootstrap ticks: 30 fine, 31st increments failure to 1.
        // Then 9 more failures → failure_count = 10 → next tick notifies.
        drive_n_ticks(&detector, 31 + 9 + 1).await;
        let ev = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("notification");
        assert!(
            ev.is_some(),
            "expected notification past combined threshold"
        );
    }
}
