//! `decideViewChange` — apply a proposal to the membership view.
//!
//! Bit-exact port of
//! `references/rapid-java/.../MembershipService.decideViewChange` plus
//! `respondToJoiners`.

use crate::events::{ClusterEvent, ClusterStatusChange, NodeStatusChange};
use crate::pb;
use crate::service::fd_scheduler;
use crate::service::state::{EndpointKey, LifecycleState, ServiceState};

/// Apply a fast-paxos-decided proposal. Mirrors Java:
/// 1. Cancel current FD tasks.
/// 2. For each endpoint in the proposal: if present → remove, else → add.
/// 3. Emit `VIEW_CHANGE` event.
/// 4. Clear the cut detector + reset `announcedProposal`.
/// 5. Rebuild the broadcaster's recipient list.
/// 6. If we are still in the view → rebuild FD tasks.
/// 7. Else emit `KICKED`.
/// 8. Fire parked join replies (`respond_to_joiners`).
///
/// `async` so the broadcaster's `set_membership` can be awaited
/// in-place — must complete before the next consensus round broadcasts
/// or the new round will fan out to a stale recipient list.
pub async fn decide_view_change(state: &mut ServiceState, proposal: &[pb::Endpoint]) {
    cancel_failure_detectors(state);

    let mut status_changes: Vec<NodeStatusChange> = Vec::with_capacity(proposal.len());
    for node in proposal {
        if state.view.is_host_present(node) {
            // Currently a member — drop it.
            let metadata = state.metadata.get(node);
            let _ = state.view.ring_delete(node);
            state.metadata.remove(node);
            status_changes.push(NodeStatusChange {
                endpoint: node.clone(),
                status: pb::EdgeStatus::Down,
                metadata,
            });
        } else {
            // Joiner — must have a parked nodeId.
            let key = EndpointKey::from(node);
            let Some(node_id) = state.joiner_uuid.remove(&key) else {
                continue;
            };
            let meta = state.joiner_metadata.remove(&key).unwrap_or_default();
            if state.view.ring_add(node, &node_id).is_err() {
                continue;
            }
            if !meta.metadata.is_empty() {
                state.metadata.add([(node.clone(), meta.clone())]);
            }
            status_changes.push(NodeStatusChange {
                endpoint: node.clone(),
                status: pb::EdgeStatus::Up,
                metadata: meta,
            });
        }
    }

    let new_configuration_id = state.view.current_configuration_id();
    let new_membership = state.view.get_ring(0).unwrap_or_default();
    tracing::info!(
        target: "rapid",
        config = new_configuration_id.as_i64(),
        members = new_membership.len(),
        delta = status_changes.len(),
        "view_change.applied",
    );
    let event = ClusterStatusChange {
        configuration_id: new_configuration_id,
        membership: new_membership.clone(),
        delta: status_changes,
    };
    publish(state, ClusterEvent::ViewChange, &event);

    state.cut_detector.clear();
    state.lifecycle = LifecycleState::Running;

    // Replace the FastPaxos instance with one bound to the new
    // configuration. Without this, subsequent proposals would broadcast
    // `FastRoundPhase2bMessage`s stamped with the *old* configuration ID
    // and peers would silently drop them.
    crate::service::consensus_dispatch::reinstate_fast_paxos(state);

    if let Some(bc) = state.broadcaster.as_ref() {
        let recipients = endpoints_as_socket_addrs(&new_membership);
        // Synchronous wrt the actor task: must complete before any
        // subsequent consensus or alert broadcast, otherwise the next
        // round fans out to the stale recipient list and quorum is
        // unreachable.
        bc.set_membership(recipients).await;
    }

    if state.view.is_host_present(&state.my_addr) {
        fd_scheduler::rebuild(state);
    } else {
        publish(state, ClusterEvent::Kicked, &event);
    }

    respond_to_joiners(state, proposal, &new_membership);
}

fn respond_to_joiners(
    state: &mut ServiceState,
    proposal: &[pb::Endpoint],
    new_membership: &[pb::Endpoint],
) {
    let configuration_id = state.view.current_configuration_id().as_i64();
    let identifiers = state.view.current_identifiers();
    let all_metadata = state.metadata.all();
    let metadata_keys: Vec<pb::Endpoint> = all_metadata.iter().map(|(k, _)| k.clone()).collect();
    let metadata_values: Vec<pb::Metadata> = all_metadata.into_iter().map(|(_, v)| v).collect();
    let response = pb::JoinResponse {
        sender: Some(state.my_addr.clone()),
        status_code: pb::JoinStatusCode::SafeToJoin as i32,
        configuration_id,
        endpoints: new_membership.to_vec(),
        identifiers,
        metadata_keys,
        metadata_values,
    };
    let wrapped = pb::RapidResponse {
        content: Some(pb::rapid_response::Content::JoinResponse(response)),
    };
    for node in proposal {
        let key = EndpointKey::from(node);
        let Some(parked) = state.joiners_to_respond_to.remove(&key) else {
            continue;
        };
        for tx in parked {
            let _ = tx.send(Ok(wrapped.clone()));
        }
    }
}

fn cancel_failure_detectors(state: &mut ServiceState) {
    for h in state.failure_detector_tasks.drain(..) {
        h.abort();
    }
}

fn publish(state: &ServiceState, event: ClusterEvent, change: &ClusterStatusChange) {
    if let Some(tx) = state.subscriptions.get(&event) {
        // `broadcast::Sender::send` returns `Err` when there are no
        // subscribers — that's not an error condition for the actor.
        let _ = tx.send(change.clone());
    }
}

fn endpoints_as_socket_addrs(endpoints: &[pb::Endpoint]) -> Vec<std::net::SocketAddr> {
    endpoints
        .iter()
        .filter_map(|e| {
            let host = String::from_utf8(e.hostname.clone()).ok()?;
            let port = u16::try_from(e.port).ok()?;
            format!("{host}:{port}").parse().ok()
        })
        .collect()
}
