//! Per-message handlers extracted from `ServiceState` to keep the state
//! struct lean. Java parity comments live alongside each function.

use tokio::sync::oneshot;

use crate::error::Result;
use crate::pb;
use crate::service::state::{EndpointKey, ServiceState};
use crate::view::JoinDisposition;

/// Java `MembershipService.handleMessage(PreJoinMessage)`.
///
/// Always responds, regardless of disposition.
pub fn handle_pre_join(state: &mut ServiceState, msg: &pb::PreJoinMessage) -> pb::JoinResponse {
    let joiner = msg.sender.clone().unwrap_or_default();
    let node_id = msg.node_id.unwrap_or_default();
    let status = state.view.is_safe_to_join(&joiner, &node_id);
    let mut response = pb::JoinResponse {
        sender: Some(state.my_addr.clone()),
        configuration_id: state.view.current_configuration_id().as_i64(),
        status_code: java_status_code(status),
        endpoints: Vec::new(),
        identifiers: Vec::new(),
        metadata_keys: Vec::new(),
        metadata_values: Vec::new(),
    };
    if matches!(
        status,
        JoinDisposition::SafeToJoin | JoinDisposition::HostnameAlreadyInRing
    ) {
        response.endpoints = state.view.get_expected_observers_of(&joiner);
    }
    response
}

/// Java `MembershipService.handleMessage(JoinMessage)`.
///
/// Phase-3a behaviour: when the configuration matches, park the reply.
/// Phase-3b adds the alert-pipeline side-effect (enqueue an `UP` alert).
pub fn handle_join(
    state: &mut ServiceState,
    msg: &pb::JoinMessage,
    reply: oneshot::Sender<Result<pb::RapidResponse>>,
) {
    let sender = msg.sender.clone().unwrap_or_default();
    let node_id = msg.node_id.unwrap_or_default();
    let current_config = state.view.current_configuration_id().as_i64();

    if current_config == msg.configuration_id {
        state
            .joiners_to_respond_to
            .entry(EndpointKey::from(&sender))
            .or_default()
            .push(reply);
        state
            .joiner_uuid
            .insert(EndpointKey::from(&sender), node_id);
        if let Some(meta) = msg.metadata.clone() {
            state
                .joiner_metadata
                .insert(EndpointKey::from(&sender), meta);
        }
        // Phase 3b side-effect: enqueue an UP alert about the joiner on
        // every ring listed by the join request. The full
        // batcher → cut-detector → consensus path then runs as it does
        // for ordinary edge events.
        let alert = pb::AlertMessage {
            edge_src: Some(state.my_addr.clone()),
            edge_dst: Some(sender),
            edge_status: pb::EdgeStatus::Up as i32,
            configuration_id: current_config,
            ring_number: msg.ring_number.clone(),
            node_id: Some(node_id),
            metadata: msg.metadata.clone(),
        };
        crate::service::alert_handler::enqueue_alert(state, alert);
        return;
    }

    let response = build_config_mismatch_response(state, &sender, &node_id);
    let _ = reply.send(Ok(pb::RapidResponse {
        content: Some(pb::rapid_response::Content::JoinResponse(response)),
    }));
}

fn build_config_mismatch_response(
    state: &mut ServiceState,
    sender: &pb::Endpoint,
    node_id: &pb::NodeId,
) -> pb::JoinResponse {
    let config_id = state.view.current_configuration_id().as_i64();
    if state.view.is_host_present(sender) && state.view.is_identifier_present(node_id) {
        let endpoints = state.view.get_ring(0).unwrap_or_default();
        let (metadata_keys, metadata_values) = collect_metadata(state);
        return pb::JoinResponse {
            sender: Some(state.my_addr.clone()),
            configuration_id: config_id,
            status_code: pb::JoinStatusCode::SafeToJoin as i32,
            endpoints,
            // Phase 3c: also include identifiers (requires
            // `MembershipView::current_configuration()` accessor).
            identifiers: Vec::new(),
            metadata_keys,
            metadata_values,
        };
    }
    pb::JoinResponse {
        sender: Some(state.my_addr.clone()),
        configuration_id: config_id,
        status_code: pb::JoinStatusCode::ConfigChanged as i32,
        endpoints: Vec::new(),
        identifiers: Vec::new(),
        metadata_keys: Vec::new(),
        metadata_values: Vec::new(),
    }
}

fn collect_metadata(state: &ServiceState) -> (Vec<pb::Endpoint>, Vec<pb::Metadata>) {
    let all = state.metadata.all();
    let mut keys = Vec::with_capacity(all.len());
    let mut values = Vec::with_capacity(all.len());
    for (k, v) in all {
        keys.push(k);
        values.push(v);
    }
    (keys, values)
}

fn java_status_code(d: JoinDisposition) -> i32 {
    match d {
        JoinDisposition::HostnameAlreadyInRing => pb::JoinStatusCode::HostnameAlreadyInRing as i32,
        JoinDisposition::UuidAlreadyInRing => pb::JoinStatusCode::UuidAlreadyInRing as i32,
        JoinDisposition::SafeToJoin => pb::JoinStatusCode::SafeToJoin as i32,
    }
}
