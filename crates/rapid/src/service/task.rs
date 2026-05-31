//! Actor task — the only place the `ServiceState` is ever borrowed.

use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::pb;
use crate::proto_traits;
use crate::service::alert_batcher;
use crate::service::alert_handler;
use crate::service::command::ServiceCommand;
use crate::service::consensus_dispatch;
use crate::service::fd_scheduler;
use crate::service::handlers;
use crate::service::state::{LifecycleState, ServiceState};
use crate::service::view_change;

/// Drive the actor loop until shutdown.
pub async fn run(
    mut state: ServiceState,
    mut rx: mpsc::Receiver<ServiceCommand>,
    tx: mpsc::Sender<ServiceCommand>,
) {
    // Java parity: `MembershipService` constructor calls
    // `createFailureDetectorsForCurrentConfiguration` once. Joiners
    // bootstrap straight into the latest view without going through a
    // view-change, so without this seed call the joiner would have zero
    // failure detectors until the next consensus round.
    fd_scheduler::rebuild(&mut state);
    while let Some(cmd) = rx.recv().await {
        if handle_command(&mut state, cmd, &tx).await {
            break;
        }
    }
    for h in state.failure_detector_tasks.drain(..) {
        h.abort();
    }
}

async fn handle_command(
    state: &mut ServiceState,
    cmd: ServiceCommand,
    tx: &mpsc::Sender<ServiceCommand>,
) -> bool {
    match cmd {
        ServiceCommand::Shutdown => {
            state.lifecycle = LifecycleState::ShuttingDown;
            return true;
        }
        ServiceCommand::Memberlist { reply } => {
            let _ = reply.send(state.memberlist());
        }
        ServiceCommand::MembershipSize { reply } => {
            let _ = reply.send(state.membership_size());
        }
        ServiceCommand::Metadata { reply } => {
            let _ = reply.send(state.metadata_snapshot());
        }
        ServiceCommand::ConfigurationId { reply } => {
            let _ = reply.send(state.configuration_id());
        }
        ServiceCommand::Request { request, reply } => {
            handle_request(state, request, reply, tx).await;
        }
        ServiceCommand::TickAlertBatcher => {
            alert_batcher::handle_tick(state).await;
        }
        ServiceCommand::EdgeFailure(ev) => {
            fd_scheduler::handle_edge_failure(state, &ev);
        }
        ServiceCommand::RebuildFailureDetectors { reply } => {
            fd_scheduler::rebuild(state);
            let _ = reply.send(state.failure_detector_tasks.len());
        }
        ServiceCommand::AlertQueueLen { reply } => {
            let _ = reply.send(state.send_queue.len());
        }
        ServiceCommand::ApplyProposal { proposal, reply } => {
            view_change::decide_view_change(state, &proposal).await;
            consensus_dispatch::reinstate_fast_paxos(state);
            let _ = reply.send(());
        }
        ServiceCommand::StartClassicRound => {
            consensus_dispatch::start_classic(state, tx).await;
        }
        ServiceCommand::PublishInitialView { reply } => {
            publish_initial_view(state);
            let _ = reply.send(());
        }
    }
    false
}

/// Java parity: `MembershipService` ctor emits a synthetic
/// `VIEW_CHANGE` listing every current member with `UP` status.
fn publish_initial_view(state: &mut ServiceState) {
    use crate::events::{ClusterEvent, ClusterStatusChange, NodeStatusChange};
    let configuration_id = state.view.current_configuration_id();
    let membership = state.view.get_ring(0).unwrap_or_default();
    let metadata = state.metadata_snapshot();
    let metadata_lookup: std::collections::HashMap<_, _> = metadata
        .into_iter()
        .map(|(k, v)| ((k.hostname.clone(), k.port), v))
        .collect();
    let delta = membership
        .iter()
        .map(|e| NodeStatusChange {
            endpoint: e.clone(),
            status: pb::EdgeStatus::Up,
            metadata: metadata_lookup
                .get(&(e.hostname.clone(), e.port))
                .cloned()
                .unwrap_or_default(),
        })
        .collect();
    let event = ClusterStatusChange {
        configuration_id,
        membership,
        delta,
    };
    if let Some(tx) = state.subscriptions.get(&ClusterEvent::ViewChange) {
        let _ = tx.send(event);
    }
}

async fn handle_request(
    state: &mut ServiceState,
    request: pb::RapidRequest,
    reply: oneshot::Sender<Result<pb::RapidResponse>>,
    tx: &mpsc::Sender<ServiceCommand>,
) {
    if state.lifecycle == LifecycleState::ShuttingDown {
        let _ = reply.send(Err(Error::Shutdown));
        return;
    }
    let Some(content) = request.content else {
        let _ = reply.send(Err(Error::Decode("RapidRequest.content is None".into())));
        return;
    };
    match content {
        pb::rapid_request::Content::PreJoinMessage(msg) => {
            let resp = handlers::handle_pre_join(state, &msg);
            let _ = reply.send(Ok(pb::RapidResponse {
                content: Some(pb::rapid_response::Content::JoinResponse(resp)),
            }));
        }
        pb::rapid_request::Content::JoinMessage(msg) => {
            handlers::handle_join(state, &msg, reply);
        }
        pb::rapid_request::Content::BatchedAlertMessage(msg) => {
            let proposal = alert_handler::handle_batched_alerts(state, &msg);
            if !proposal.is_empty() {
                consensus_dispatch::propose(state, proposal, tx).await;
            }
            let _ = reply.send(Ok(pb::RapidResponse::default()));
        }
        pb::rapid_request::Content::ProbeMessage(_) => {
            let _ = reply.send(Ok(proto_traits::probe_response(pb::ProbeResponse {
                status: pb::NodeStatus::Ok as i32,
            })));
        }
        pb::rapid_request::Content::LeaveMessage(msg) => {
            if let Some(sender) = msg.sender {
                let configuration_id = state.view.current_configuration_id();
                fd_scheduler::handle_edge_failure(
                    state,
                    &crate::monitoring::factory::EdgeFailure {
                        subject: sender,
                        configuration_id,
                    },
                );
            }
            let _ = reply.send(Ok(pb::RapidResponse::default()));
        }
        pb::rapid_request::Content::FastRoundPhase2bMessage(_)
        | pb::rapid_request::Content::Phase1aMessage(_)
        | pb::rapid_request::Content::Phase1bMessage(_)
        | pb::rapid_request::Content::Phase2aMessage(_)
        | pb::rapid_request::Content::Phase2bMessage(_) => {
            // Reconstruct the RapidRequest so the dispatcher can pattern-match.
            let req = pb::RapidRequest {
                content: Some(content),
            };
            consensus_dispatch::handle_consensus_request(state, &req, tx).await;
            let _ = reply.send(Ok(pb::RapidResponse::default()));
        }
    }
}
