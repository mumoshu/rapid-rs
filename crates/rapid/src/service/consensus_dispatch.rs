//! Glue between the `FastPaxos` state machine and the actor's
//! side-effecting world (broadcaster + clock-scheduled fallback +
//! `apply_proposal` mailbox command).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::clock::Clock;
use crate::consensus::fast_paxos::FastOutgoing;
use crate::consensus::FastPaxos;
use crate::messaging::traits::MessagingClient;
use crate::pb;
use crate::service::command::ServiceCommand;
use crate::service::state::ServiceState;
use crate::service::view_change;

/// Dispatch a list of Fast-Paxos outputs.
pub async fn dispatch(
    state: &mut ServiceState,
    outs: Vec<FastOutgoing>,
    tx: &mpsc::Sender<ServiceCommand>,
) {
    for o in outs {
        match o {
            FastOutgoing::Broadcast(req) => spawn_broadcast(state, req),
            FastOutgoing::Unicast { target, request } => spawn_unicast(state, &target, request),
            FastOutgoing::ScheduleClassicFallback(delay) => {
                tracing::info!(target: "rapid", ?delay, "paxos.fallback.scheduled");
                spawn_fallback(&mut *state, tx.clone(), delay);
            }
            FastOutgoing::Decision(proposal) => {
                view_change::decide_view_change(state, &proposal).await;
                reinstate_fast_paxos(state);
            }
        }
    }
}

fn spawn_broadcast(state: &ServiceState, req: pb::RapidRequest) {
    let Some(bc) = state.broadcaster.as_ref() else {
        return;
    };
    let bc = bc.clone();
    tokio::spawn(async move {
        bc.broadcast(req).await;
    });
}

fn spawn_unicast(state: &ServiceState, target: &pb::Endpoint, req: pb::RapidRequest) {
    let Some(client) = state.consensus_client.clone() else {
        return;
    };
    let Some(addr) = endpoint_to_socket_addr(target) else {
        return;
    };
    tokio::spawn(async move {
        let _ = client.send_best_effort(addr, req).await;
    });
}

fn spawn_fallback(state: &mut ServiceState, tx: mpsc::Sender<ServiceCommand>, delay: Duration) {
    if let Some(prior) = state.pending_fallback.take() {
        prior.abort();
    }
    let clock: Arc<dyn Clock> = state.clock.clone();
    let handle = tokio::spawn(async move {
        clock.sleep(delay).await;
        let _ = tx.send(ServiceCommand::StartClassicRound).await;
    });
    state.pending_fallback = Some(handle);
}

/// Invoked when a new proposal is emitted by the cut detector. Routes
/// through `FastPaxos::propose` and dispatches the outputs.
pub async fn propose(
    state: &mut ServiceState,
    proposal: Vec<pb::Endpoint>,
    tx: &mpsc::Sender<ServiceCommand>,
) {
    let Some(fp) = state.fast_paxos.as_mut() else {
        return;
    };
    let outs = fp.propose(proposal);
    dispatch(state, outs, tx).await;
}

/// Invoked on `ServiceCommand::StartClassicRound`.
pub async fn start_classic(state: &mut ServiceState, tx: &mpsc::Sender<ServiceCommand>) {
    let Some(fp) = state.fast_paxos.as_mut() else {
        return;
    };
    let outs = fp.start_classic_round();
    dispatch(state, outs, tx).await;
}

/// Invoked on inbound consensus messages.
pub async fn handle_consensus_request(
    state: &mut ServiceState,
    request: &pb::RapidRequest,
    tx: &mpsc::Sender<ServiceCommand>,
) {
    let Some(fp) = state.fast_paxos.as_mut() else {
        return;
    };
    let outs = fp.handle_consensus_message(request);
    dispatch(state, outs, tx).await;
}

fn endpoint_to_socket_addr(endpoint: &pb::Endpoint) -> Option<SocketAddr> {
    let host = String::from_utf8(endpoint.hostname.clone()).ok()?;
    let port = u16::try_from(endpoint.port).ok()?;
    format!("{host}:{port}").parse().ok()
}

/// Reset the actor's Fast Paxos instance after a view change. Aborts
/// any pending classic-fallback timer from the prior instance so a stale
/// `StartClassicRound` can't fire on the fresh state.
pub fn reinstate_fast_paxos(state: &mut ServiceState) {
    if let Some(prior) = state.pending_fallback.take() {
        prior.abort();
    }
    let configuration_id = state.view.current_configuration_id().as_i64();
    let membership_size = state.view.membership_size();
    state.fast_paxos = Some(FastPaxos::new(
        state.my_addr.clone(),
        configuration_id,
        membership_size,
        state.settings.paxos_fallback_base_delay,
    ));
}

/// Construct a fresh `FastPaxos` for an actor that's being bootstrapped.
pub fn install_fast_paxos(state: &mut ServiceState) {
    reinstate_fast_paxos(state);
}

/// Re-used utility for tests that don't supply a `MessagingClient` for
/// unicasts (Phase1b replies). When `consensus_client` is `None`,
/// unicasts are silently dropped — the test relies on Broadcasts only.
#[allow(dead_code)]
pub fn _set_client(state: &mut ServiceState, client: Arc<dyn MessagingClient>) {
    state.consensus_client = Some(client);
}
