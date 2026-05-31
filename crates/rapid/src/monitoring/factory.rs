//! `IEdgeFailureDetectorFactory` / `LinkFailureDetector` analogues.
//!
//! Java's `IEdgeFailureDetectorFactory.createInstance(subject, notifier)`
//! returns a `Runnable` that the membership service schedules on its
//! `backgroundTasksExecutor`. We model the same as a Rust `async fn run()`
//! that owns its own scheduling cadence and calls back into the actor via
//! [`EdgeFailureNotifier`] when an edge is presumed down.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::pb;
use crate::types::ConfigurationId;

/// Mailbox-typed handle the FD uses to tell the actor an edge is down.
#[derive(Debug, Clone)]
pub struct EdgeFailureNotifier {
    tx: mpsc::Sender<EdgeFailure>,
    configuration_id: ConfigurationId,
    subject: pb::Endpoint,
}

/// A single edge-failure event.
pub struct EdgeFailure {
    /// The subject the observer believes failed.
    pub subject: pb::Endpoint,
    /// Configuration ID active at FD-creation time. Stale notifications
    /// (older than the actor's current view) are dropped per Java
    /// `MembershipService.edgeFailureNotification`.
    pub configuration_id: ConfigurationId,
}

impl EdgeFailureNotifier {
    /// Construct a notifier targeting `tx` with the FD's binding.
    #[must_use]
    pub fn new(
        tx: mpsc::Sender<EdgeFailure>,
        configuration_id: ConfigurationId,
        subject: pb::Endpoint,
    ) -> Self {
        Self {
            tx,
            configuration_id,
            subject,
        }
    }

    /// Notify the actor that the bound edge is down. Best-effort —
    /// if the actor's mailbox is closed (shutting down) the notification
    /// is silently dropped.
    pub async fn notify(&self) {
        let _ = self
            .tx
            .send(EdgeFailure {
                subject: self.subject.clone(),
                configuration_id: self.configuration_id,
            })
            .await;
    }
}

/// A single edge-failure detector instance. One per (observer=self, subject)
/// pair.
#[async_trait]
pub trait EdgeFailureDetector: Send + Sync {
    /// Run the detector until cancellation (the caller aborts the
    /// `JoinHandle`). Implementations decide their own cadence; they MUST
    /// use [`crate::Clock`] for any waits.
    async fn run(self: Arc<Self>);
}

/// Factory for per-edge detectors.
pub trait EdgeFailureDetectorFactory: Send + Sync {
    /// Create a fresh detector for the `(self, subject)` edge with the
    /// supplied notifier.
    fn create(
        &self,
        subject: pb::Endpoint,
        notifier: EdgeFailureNotifier,
    ) -> Arc<dyn EdgeFailureDetector>;
}
