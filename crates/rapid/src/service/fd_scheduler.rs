//! Per-subject failure-detector scheduling.
//!
//! Java `MembershipService.createFailureDetectorsForCurrentConfiguration`
//! schedules one detector per `getSubjectsOf(myAddr)` entry on the
//! background executor. Each detector's `Runnable` invokes
//! `edgeFailureNotification(subject, currentConfigurationId)` on failure.
//!
//! The Rust port mirrors the lifecycle exactly:
//! - `rebuild_failure_detectors_for_current_view` cancels every previously
//!   spawned task and starts a fresh set.
//! - Each task is an `EdgeFailureDetector::run` future supplied by the
//!   configured [`EdgeFailureDetectorFactory`](crate::monitoring::factory::EdgeFailureDetectorFactory).

use std::sync::Arc;

use crate::monitoring::factory::EdgeFailureNotifier;
use crate::pb;
use crate::service::state::ServiceState;

/// Replace the running set of failure-detector tasks with one per current
/// subject. Aborts the previous set first.
pub fn rebuild(state: &mut ServiceState) {
    // Abort the old tasks. JoinHandle::abort doesn't await — it's a
    // non-blocking cancellation signal. The drop here releases the
    // handles and frees the slot for the new tasks.
    for h in state.failure_detector_tasks.drain(..) {
        h.abort();
    }
    let Some(factory) = state.fd_factory.clone() else {
        return;
    };
    let Some(notifier_tx) = state.fd_notifier_tx.clone() else {
        return;
    };
    let configuration_id = state.view.current_configuration_id();
    let Ok(subjects) = state.view.get_subjects_of(&state.my_addr) else {
        return;
    };
    for subject in subjects {
        let notifier =
            EdgeFailureNotifier::new(notifier_tx.clone(), configuration_id, subject.clone());
        let detector = factory.create(subject, notifier);
        let task = tokio::spawn(async move {
            Arc::clone(&detector).run().await;
        });
        state.failure_detector_tasks.push(task);
    }
}

/// Java `MembershipService.edgeFailureNotification(subject, configurationId)`.
/// Called by the actor when an FD task posts an
/// [`EdgeFailure`](crate::monitoring::factory::EdgeFailure) event.
///
/// Drops stale notifications (from a prior configuration) and otherwise
/// enqueues a `DOWN` alert against the subject.
pub fn handle_edge_failure(state: &mut ServiceState, ev: &crate::monitoring::factory::EdgeFailure) {
    let current = state.view.current_configuration_id();
    if ev.configuration_id != current {
        return;
    }
    tracing::info!(
        target: "rapid",
        config = current.as_i64(),
        "edge.failure.detected",
    );
    let ring_numbers = state
        .view
        .get_ring_numbers(&state.my_addr, &ev.subject)
        .unwrap_or_default();
    let alert = pb::AlertMessage {
        edge_src: Some(state.my_addr.clone()),
        edge_dst: Some(ev.subject.clone()),
        edge_status: pb::EdgeStatus::Down as i32,
        configuration_id: current.as_i64(),
        ring_number: ring_numbers.into_iter().map(i32::from).collect(),
        node_id: None,
        metadata: None,
    };
    crate::service::alert_handler::enqueue_alert(state, alert);
}
