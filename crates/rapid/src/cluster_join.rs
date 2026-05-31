//! Two-phase join protocol used by `ClusterBuilder::join`.
//!
//! Port of `Cluster.Builder.join` + `joinAttempt` + `sendJoinPhase2Messages`
//! from `references/rapid-java/.../Cluster.java`.

use std::collections::HashMap;
use std::net::SocketAddr;

use uuid::Uuid;

use crate::clock::Clock;
use crate::error::{Error, Result};
use crate::messaging::traits::MessagingClient;
use crate::pb;
use crate::settings::Settings;

/// Full two-phase bootstrap. Phase 1 obtains the configuration + observer
/// list; Phase 2 sends `JoinMessage` to each unique observer (with the
/// matching ring indices) and awaits the first `SAFE_TO_JOIN` response
/// carrying a *new* configuration ID.
///
/// The `seed` parameter follows the Java API's naming convention but
/// is **not a gossip seed** — Rapid uses full-mesh broadcast plus
/// Paxos, not gossip. It is just the address of any one existing
/// cluster member used as the Phase-1 contact; the Rapid paper
/// (Suresh et al., USENIX ATC '18) calls this "an existing member" /
/// "a contact." See [`crate::cluster::ClusterBuilder::join`] for the
/// full discussion.
///
/// # Errors
///
/// Returns [`Error::Transport`] / [`Error::ProtocolRejected`] on protocol
/// failure.
///
/// # Panics
///
/// Panics only if `K` (the per-observer ring index) exceeds `i32::MAX`,
/// which would require `Settings.k > 2^31 — 1`. The default is `K = 10`.
pub async fn run_join_protocol(
    listen_addr: SocketAddr,
    seed: SocketAddr,
    settings: &Settings,
    client: &dyn MessagingClient,
    clock: &dyn Clock,
    metadata: &pb::Metadata,
) -> Result<pb::JoinResponse> {
    use futures_util::stream::FuturesUnordered;
    use futures_util::StreamExt;

    let mut current_id = node_id_from_uuid(Uuid::new_v4());
    let listen_endpoint = endpoint_from_socket(listen_addr);

    for attempt in 0..settings.join_phase1_retries {
        tracing::info!(target: "rapid", %seed, attempt, "join.phase1.send");
        let phase1 = run_phase1(listen_addr, seed, &current_id, client).await?;
        tracing::info!(
            target: "rapid",
            status = phase1.status_code,
            config = phase1.configuration_id,
            observers = phase1.endpoints.len(),
            "join.phase1.complete",
        );
        let configuration_to_join =
            if phase1.status_code == pb::JoinStatusCode::HostnameAlreadyInRing as i32 {
                -1
            } else {
                phase1.configuration_id
            };
        // Group observers by endpoint and accumulate ring numbers.
        let mut per_observer: HashMap<(Vec<u8>, i32), Vec<i32>> = HashMap::new();
        for (i, observer) in phase1.endpoints.iter().enumerate() {
            let key = (observer.hostname.clone(), observer.port);
            per_observer
                .entry(key)
                .or_default()
                .push(i32::try_from(i).expect("K fits"));
        }
        // Fire phase-2 messages concurrently. Java spawns each
        // `messagingClient.sendMessage` independently and then takes the
        // first `SAFE_TO_JOIN` reply whose configuration ID differs from
        // the requested one (`Futures.successfulAsList` + `findFirst`).
        // The Rust port mirrors this with `FuturesUnordered`: a sequential
        // `await` per send would block the entire batch on the slowest
        // (or dead) observer's `MessageTimeouts::join` deadline.
        let mut responses = FuturesUnordered::new();
        for ((host, port), ring_numbers) in per_observer {
            let endpoint = pb::Endpoint {
                hostname: host,
                port,
            };
            let Some(addr) = endpoint_to_socket_addr(&endpoint) else {
                continue;
            };
            let req = pb::RapidRequest {
                content: Some(pb::rapid_request::Content::JoinMessage(pb::JoinMessage {
                    sender: Some(listen_endpoint.clone()),
                    node_id: Some(current_id),
                    ring_number: ring_numbers,
                    configuration_id: configuration_to_join,
                    metadata: Some(metadata.clone()),
                })),
            };
            responses.push(client.send(addr, req));
        }
        while let Some(result) = responses.next().await {
            let Ok(resp) = result else { continue };
            if let Some(pb::rapid_response::Content::JoinResponse(jr)) = resp.content {
                if jr.status_code == pb::JoinStatusCode::SafeToJoin as i32
                    && jr.configuration_id != configuration_to_join
                {
                    tracing::info!(
                        target: "rapid",
                        new_config = jr.configuration_id,
                        members = jr.endpoints.len(),
                        "join.phase2.complete",
                    );
                    return Ok(jr);
                }
            }
        }
        // No observer returned a winning response; retry phase-1.
        if phase1.status_code == pb::JoinStatusCode::UuidAlreadyInRing as i32 {
            current_id = node_id_from_uuid(Uuid::new_v4());
        }
        clock.sleep(settings.join_phase1_retry_interval).await;
    }
    Err(Error::ProtocolRejected(
        "join: exhausted retries without a SAFE_TO_JOIN with a new config-id".into(),
    ))
}

// `seed` here is the contact address from `run_join_protocol` — see
// that function's docs for the naming-vs-protocol-role caveat.
async fn run_phase1(
    listen_addr: SocketAddr,
    seed: SocketAddr,
    current_id: &pb::NodeId,
    client: &dyn MessagingClient,
) -> Result<pb::JoinResponse> {
    let listen_endpoint = endpoint_from_socket(listen_addr);
    let req = pb::RapidRequest {
        content: Some(pb::rapid_request::Content::PreJoinMessage(
            pb::PreJoinMessage {
                sender: Some(listen_endpoint),
                node_id: Some(*current_id),
                ring_number: Vec::new(),
                configuration_id: -1,
            },
        )),
    };
    let resp = client.send(seed, req).await?;
    match resp.content {
        Some(pb::rapid_response::Content::JoinResponse(jr)) => Ok(jr),
        _ => Err(Error::Decode("expected JoinResponse from seed".into())),
    }
}

pub(crate) fn endpoint_to_socket_addr(endpoint: &pb::Endpoint) -> Option<SocketAddr> {
    let host = String::from_utf8(endpoint.hostname.clone()).ok()?;
    let port = u16::try_from(endpoint.port).ok()?;
    format!("{host}:{port}").parse().ok()
}

pub(crate) fn endpoint_from_socket(addr: SocketAddr) -> pb::Endpoint {
    pb::Endpoint {
        hostname: addr.ip().to_string().into_bytes(),
        port: i32::from(addr.port()),
    }
}

pub(crate) fn node_id_from_uuid(uuid: Uuid) -> pb::NodeId {
    let (high, low) = uuid.as_u64_pair();
    pb::NodeId {
        #[allow(clippy::cast_possible_wrap)]
        high: high as i64,
        #[allow(clippy::cast_possible_wrap)]
        low: low as i64,
    }
}
