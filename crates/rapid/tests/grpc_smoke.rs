//! Phase 0 gate: Rust client → Rust dummy server, `ProbeMessage` round-trip
//! over real gRPC.

use std::net::SocketAddr;

use rapid::messaging::{grpc, handler::ProbeOnlyHandler, traits::MessagingClient};
use rapid::pb;
use rapid::proto_traits;

#[tokio::test]
async fn rust_grpc_probe_round_trip() {
    let bind: SocketAddr = "127.0.0.1:0"
        .parse()
        .expect("invariant: literal addr parses");
    let server = grpc::serve(bind, ProbeOnlyHandler)
        .await
        .expect("server starts");
    let addr = rapid::messaging::traits::MessagingServer::local_addr(&server);

    let client = grpc::GrpcClient::connect(addr)
        .await
        .expect("client connects");
    let req = proto_traits::probe_request(pb::ProbeMessage::default());
    let resp = MessagingClient::send(&client, addr, req)
        .await
        .expect("probe responds");
    let Some(pb::rapid_response::Content::ProbeResponse(p)) = resp.content else {
        panic!("expected ProbeResponse");
    };
    assert_eq!(p.status(), pb::NodeStatus::Ok);
}
