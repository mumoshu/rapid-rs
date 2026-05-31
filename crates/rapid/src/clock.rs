//! Injected clock abstraction.
//!
//! Protocol modules must not call `tokio::time::sleep` or `Instant::now`
//! directly (RULES §Async). Everything that depends on time takes a
//! `&dyn Clock`. The default impl wraps tokio; tests use [`MockClock`]
//! together with `tokio::time::pause()`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// A monotonic clock used by all time-dependent code in the crate.
#[async_trait]
pub trait Clock: Send + Sync + 'static {
    /// Current instant on the underlying monotonic clock.
    fn now(&self) -> Instant;

    /// Sleep until the supplied deadline. Implementations may wake early
    /// only if cancelled.
    async fn sleep_until(&self, deadline: Instant);

    /// Convenience wrapper.
    async fn sleep(&self, dur: Duration) {
        let deadline = self.now() + dur;
        self.sleep_until(deadline).await;
    }
}

/// Default real-time clock used in production.
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioClock;

#[async_trait]
impl Clock for TokioClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    async fn sleep_until(&self, deadline: Instant) {
        tokio::time::sleep_until(deadline).await;
    }
}

/// Deterministic clock used by tests under `tokio::time::pause()`.
///
/// Tests typically pause the runtime clock and then drive it forward with
/// `tokio::time::advance(…)`. `MockClock` delegates to that same runtime
/// clock so wakeups participate in the paused-time scheduler.
#[derive(Debug, Default, Clone)]
pub struct MockClock {
    inner: Arc<Mutex<MockState>>,
}

#[derive(Debug, Default)]
struct MockState {
    sleeps: u64,
}

impl MockClock {
    /// Create a fresh `MockClock`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of `sleep_until` calls observed since creation. Useful as a
    /// test assertion: timer was scheduled vs. swallowed.
    pub async fn sleep_call_count(&self) -> u64 {
        self.inner.lock().await.sleeps
    }
}

#[async_trait]
impl Clock for MockClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    async fn sleep_until(&self, deadline: Instant) {
        {
            let mut s = self.inner.lock().await;
            s.sleeps += 1;
        }
        tokio::time::sleep_until(deadline).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn mock_clock_counts_sleeps() {
        let c = MockClock::new();
        let t0 = c.now();
        c.sleep_until(t0 + Duration::from_millis(50)).await;
        c.sleep_until(t0 + Duration::from_millis(100)).await;
        assert_eq!(c.sleep_call_count().await, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn tokio_clock_sleeps_under_pause() {
        let c = TokioClock;
        let t0 = c.now();
        c.sleep(Duration::from_mins(1)).await;
        assert!(c.now() >= t0 + Duration::from_mins(1));
    }
}
