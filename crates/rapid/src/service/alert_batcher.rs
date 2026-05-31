//! `AlertBatcher` background loop.
//!
//! Mirrors Java `MembershipService.AlertBatcher` (which schedules itself
//! every `Settings.batchingWindowInMs`). The Rust port lives in a separate
//! task that ticks the actor mailbox; the actor's tick handler drains the
//! send queue, fans out a `BatchedAlertMessage`, and clears
//! `last_enqueue_at`.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::clock::Clock;
use crate::messaging::traits::Broadcaster;
use crate::pb;
use crate::service::command::ServiceCommand;
use crate::service::state::ServiceState;

/// Spawn the batcher loop. The loop sleeps `tick_interval` and posts
/// `ServiceCommand::TickAlertBatcher` to the actor's mailbox. Returns the
/// `JoinHandle` so the actor can abort it on shutdown.
pub fn spawn_batcher_loop(
    clock: Arc<dyn Clock>,
    tx: mpsc::Sender<ServiceCommand>,
    tick_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            clock.sleep(tick_interval).await;
            if tx.send(ServiceCommand::TickAlertBatcher).await.is_err() {
                return;
            }
        }
    })
}

/// Tick handler invoked by the actor when `ServiceCommand::TickAlertBatcher`
/// arrives. Mirrors Java's `AlertBatcher.run`:
/// - if `sendQueue` empty: noop.
/// - if `now - lastEnqueueAt > batchingWindow`: drain and broadcast.
pub async fn handle_tick(state: &mut ServiceState) {
    if state.send_queue.is_empty() {
        return;
    }
    let Some(last) = state.last_enqueue_at else {
        return;
    };
    let now = state.clock.now();
    if now.duration_since(last) <= state.settings.batching_window {
        return;
    }
    let drained: Vec<pb::AlertMessage> = state.send_queue.drain(..).collect();
    tracing::info!(target: "rapid", count = drained.len(), "alert.batched_send");
    state.last_enqueue_at = None;
    let batch = pb::BatchedAlertMessage {
        sender: Some(state.my_addr.clone()),
        messages: drained,
    };
    let req = pb::RapidRequest {
        content: Some(pb::rapid_request::Content::BatchedAlertMessage(batch)),
    };
    if let Some(bc) = state.broadcaster.as_ref() {
        broadcast(Arc::clone(bc), req).await;
    }
}

async fn broadcast(broadcaster: Arc<dyn Broadcaster>, req: pb::RapidRequest) {
    broadcaster.broadcast(req).await;
}
