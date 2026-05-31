//! `BatchedAlertMessage` handler + alert enqueue helper.
//!
//! Bit-exact port of
//! `references/rapid-java/.../MembershipService.handleMessage(BatchedAlertMessage)`
//! plus `filterAlertMessages`, `extractJoinerUuidAndMetadata`, and
//! `enqueueAlertMessage`.

use std::collections::HashSet;

use crate::pb;
use crate::service::state::{EndpointKey, LifecycleState, ServiceState};

/// Enqueue an outgoing `AlertMessage` and stamp the last-enqueue time.
/// Java `MembershipService.enqueueAlertMessage`.
pub fn enqueue_alert(state: &mut ServiceState, alert: pb::AlertMessage) {
    state.last_enqueue_at = Some(state.clock.now());
    state.send_queue.push_back(alert);
}

/// Apply a received `BatchedAlertMessage` to the cut detector. Returns
/// the new proposal (possibly empty); the caller is responsible for
/// dispatching it to consensus (Phase 4) or notifying subscribers.
///
/// Mirrors Java exactly: filter → extract joiner data → aggregate → invalidate.
pub fn handle_batched_alerts(
    state: &mut ServiceState,
    batch: &pb::BatchedAlertMessage,
) -> Vec<pb::Endpoint> {
    let current_config = state.view.current_configuration_id().as_i64();
    let _ = batch.messages.len();

    if state.lifecycle == LifecycleState::AnnouncedProposal {
        return Vec::new();
    }

    // First pass: filter + extract joiner metadata + collect valid alerts.
    let mut valid: Vec<pb::AlertMessage> = Vec::with_capacity(batch.messages.len());
    for alert in &batch.messages {
        if !is_valid_alert(state, alert, current_config) {
            continue;
        }
        tracing::debug!(target: "rapid", "alert.received");
        extract_joiner(state, alert);
        valid.push(alert.clone());
    }

    // Second pass: aggregate through cut detector + apply implicit detection.
    let mut proposal: HashSet<EndpointKey> = HashSet::new();
    let mut endpoint_objs: std::collections::HashMap<EndpointKey, pb::Endpoint> =
        std::collections::HashMap::new();
    for alert in &valid {
        let emitted = state.cut_detector.aggregate(alert);
        for e in emitted {
            endpoint_objs.insert(EndpointKey::from(&e), e.clone());
            proposal.insert(EndpointKey::from(&e));
        }
    }
    for e in state.cut_detector.invalidate_failing_edges(&mut state.view) {
        endpoint_objs.insert(EndpointKey::from(&e), e.clone());
        proposal.insert(EndpointKey::from(&e));
    }

    if proposal.is_empty() {
        return Vec::new();
    }
    tracing::info!(
        target: "rapid",
        size = proposal.len(),
        config = current_config,
        "proposal.emitted",
    );
    state.lifecycle = LifecycleState::AnnouncedProposal;
    let mut ordered: Vec<pb::Endpoint> = proposal
        .into_iter()
        .map(|k| endpoint_objs.remove(&k).unwrap_or_default())
        .collect();
    // Java parity: `MembershipView.getRingZeroComparator()` orders
    // endpoints by their `address_hash(seed=0, endpoint)`. Consensus
    // expects the proposal in this order so peers compute identical
    // proposal keys when matching `Phase2bMessage::vval`.
    ordered.sort_by_key(crate::view_hash::address_hash_seed_zero);

    if let Some(tx) = state.proposal_tx.as_ref() {
        let _ = tx.send(ordered.clone());
    }

    ordered
}

/// Java `MembershipService.filterAlertMessages`.
fn is_valid_alert(state: &ServiceState, alert: &pb::AlertMessage, current_config: i64) -> bool {
    if alert.configuration_id != current_config {
        return false;
    }
    let Some(dst) = alert.edge_dst.as_ref() else {
        return false;
    };
    let is_present = state.view.is_host_present(dst);
    match pb::EdgeStatus::try_from(alert.edge_status).unwrap_or(pb::EdgeStatus::Up) {
        pb::EdgeStatus::Up => !is_present,
        pb::EdgeStatus::Down => is_present,
    }
}

/// Java `MembershipService.extractJoinerUuidAndMetadata`.
fn extract_joiner(state: &mut ServiceState, alert: &pb::AlertMessage) {
    if pb::EdgeStatus::try_from(alert.edge_status).unwrap_or(pb::EdgeStatus::Up)
        != pb::EdgeStatus::Up
    {
        return;
    }
    let Some(dst) = alert.edge_dst.as_ref() else {
        return;
    };
    let key = EndpointKey::from(dst);
    if let Some(id) = alert.node_id {
        state.joiner_uuid.insert(key.clone(), id);
    }
    if let Some(meta) = alert.metadata.clone() {
        state.joiner_metadata.insert(key, meta);
    }
}
