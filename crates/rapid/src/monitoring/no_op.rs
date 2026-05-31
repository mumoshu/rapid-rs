//! Failure-detector that never fires. Used by Phase-3b tests that only
//! care about FD-task lifecycle (spawn / cancel / respawn), not about
//! actual probing.

use std::sync::Arc;

use async_trait::async_trait;

use crate::monitoring::factory::{
    EdgeFailureDetector, EdgeFailureDetectorFactory, EdgeFailureNotifier,
};
use crate::pb;

/// `IEdgeFailureDetectorFactory` impl that produces no-op detectors.
#[derive(Default, Clone, Copy)]
pub struct NoOpFactory;

impl EdgeFailureDetectorFactory for NoOpFactory {
    fn create(
        &self,
        _subject: pb::Endpoint,
        _notifier: EdgeFailureNotifier,
    ) -> Arc<dyn EdgeFailureDetector> {
        Arc::new(NoOpDetector)
    }
}

struct NoOpDetector;

#[async_trait]
impl EdgeFailureDetector for NoOpDetector {
    async fn run(self: Arc<Self>) {
        // Sleep forever — the parent task aborts us on view-change.
        // We deliberately do NOT use the Clock here: the FD's "run forever"
        // is a structural property, not a time-dependent one. The task is
        // cancelled via `JoinHandle::abort` rather than awaiting a timer.
        std::future::pending::<()>().await;
    }
}
