//! Tunable defaults for the Rapid protocol.
//!
//! All numeric constants live here. RULES §Constants forbids `const X: u32 =
//! …` sprinkled through protocol modules.
//!
//! Defaults are taken from the Java upstream (`Cluster.java`, lines 72-75:
//! `K=10, H=9, L=4, RETRIES=5`), not the paper. Where Java and the paper
//! disagree we follow Java because the wire-level fixtures and golden
//! vectors are Java-generated.

use std::time::Duration;

/// Process-wide protocol parameters held by every `MembershipService`.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Number of monitoring rings (`K`).
    pub k: u8,
    /// Cut-detector high watermark (`H`).
    pub h: u8,
    /// Cut-detector low watermark (`L`).
    pub l: u8,
    /// Phase-1 join retries (`Cluster.RETRIES`).
    pub join_phase1_retries: u8,
    /// Constant interval between phase-1 join retries.
    pub join_phase1_retry_interval: Duration,
    /// Alert batching window (`MembershipService.BATCHING_WINDOW_IN_MS`).
    pub batching_window: Duration,
    /// Failure-detector tick interval (`MembershipService.DEFAULT_FAILURE_DETECTOR_INTERVAL_IN_MS`).
    pub failure_detector_interval: Duration,
    /// Leave-message client-side timeout (`MembershipService.LEAVE_MESSAGE_TIMEOUT`).
    pub leave_message_timeout: Duration,
    /// Paxos fallback base delay (`FastPaxos.BASE_DELAY`).
    pub paxos_fallback_base_delay: Duration,
    /// Probe failures before edge marked down (`PingPongFailureDetector.FAILURE_THRESHOLD`).
    pub failure_threshold: u32,
    /// Successive BOOTSTRAPPING responses before edge marked down
    /// (`PingPongFailureDetector.BOOTSTRAP_COUNT_THRESHOLD`).
    pub bootstrap_count_threshold: u32,
    /// Default gRPC client deadline for every `RapidRequest` not
    /// covered by a more specific timeout (`Settings.grpcTimeoutMs`).
    /// Default: 1 s. Mirrors `GrpcClient.DEFAULT_GRPC_TIMEOUT_MS`.
    pub grpc_default_timeout: Duration,
    /// Number of in-flight gRPC retry attempts for each
    /// `MessagingClient::send` call (`Settings.grpcDefaultRetries`).
    /// Default: 5. Currently advisory — the Rust default messaging
    /// client retries Phase-1 once per `join_phase1_retries`; the
    /// per-call retry budget is wired through this field by F11's
    /// `phase2MessageDropsRpcRetries` port.
    pub grpc_default_retries: u32,
    /// `JoinMessage` / `PreJoinMessage` deadline (`Settings.grpcJoinTimeoutMs`).
    /// Default: 5 s.
    pub grpc_join_timeout: Duration,
    /// `ProbeMessage` deadline (`Settings.grpcProbeTimeoutMs`).
    /// Default: 1 s.
    pub grpc_probe_timeout: Duration,
    /// Advisory mirror of Java `Settings.useInProcessTransport`. Read
    /// by application code that wants to inspect the cluster's
    /// configuration; the actual transport is chosen at builder time
    /// via [`crate::cluster::ClusterBuilder::new`] vs `with_grpc`. Default: `false`.
    pub use_in_process_transport: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            k: 10,
            h: 9,
            l: 4,
            join_phase1_retries: 5,
            join_phase1_retry_interval: Duration::from_millis(500),
            batching_window: Duration::from_millis(100),
            failure_detector_interval: Duration::from_secs(1),
            leave_message_timeout: Duration::from_millis(1500),
            paxos_fallback_base_delay: Duration::from_secs(1),
            failure_threshold: 10,
            bootstrap_count_threshold: 30,
            grpc_default_timeout: Duration::from_secs(1),
            grpc_default_retries: 5,
            grpc_join_timeout: Duration::from_secs(5),
            grpc_probe_timeout: Duration::from_secs(1),
            use_in_process_transport: false,
        }
    }
}

impl Settings {
    /// Test preset with short timeouts. FD interval is 200 ms (so the
    /// default 10-failure threshold trips in ~2 s); batching window is
    /// 30 ms; fast→classic fallback delay is 300 ms.
    #[must_use]
    pub fn for_tests() -> Self {
        Self {
            failure_detector_interval: Duration::from_millis(200),
            batching_window: Duration::from_millis(30),
            join_phase1_retry_interval: Duration::from_millis(100),
            paxos_fallback_base_delay: Duration::from_millis(300),
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        let s = Settings::default();
        assert_eq!(s.k, 10);
        assert_eq!(s.h, 9);
        assert_eq!(s.l, 4);
        assert_eq!(s.join_phase1_retries, 5);
        assert_eq!(s.batching_window, Duration::from_millis(100));
        assert_eq!(s.failure_detector_interval, Duration::from_secs(1));
        assert_eq!(s.failure_threshold, 10);
        assert_eq!(s.bootstrap_count_threshold, 30);
        // F8: gRPC tuning fields mirror Java's GrpcClient defaults.
        assert_eq!(s.grpc_default_timeout, Duration::from_secs(1));
        assert_eq!(s.grpc_default_retries, 5);
        assert_eq!(s.grpc_join_timeout, Duration::from_secs(5));
        assert_eq!(s.grpc_probe_timeout, Duration::from_secs(1));
        assert!(!s.use_in_process_transport);
    }

    #[test]
    fn message_timeouts_derive_from_settings() {
        use crate::messaging::MessageTimeouts;
        let s = Settings {
            grpc_probe_timeout: Duration::from_millis(50),
            grpc_join_timeout: Duration::from_millis(250),
            grpc_default_timeout: Duration::from_millis(125),
            ..Default::default()
        };
        let t = MessageTimeouts::from(&s);
        assert_eq!(t.probe, Duration::from_millis(50));
        assert_eq!(t.join, Duration::from_millis(250));
        assert_eq!(t.default, Duration::from_millis(125));
    }
}
