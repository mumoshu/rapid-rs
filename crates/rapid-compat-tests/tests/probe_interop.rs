//! Black-box probe interop — F3 gate.
//!
//! Spawns the Java `standalone-agent.jar` as a seed and uses
//! `rapid::messaging::GrpcPool` to send a `ProbeMessage`. The response
//! status must be `OK`.
//!
//! Reverse direction (Java→Rust probe) is exercised continuously by
//! `mixed_cluster.sh` (the Java FD probes Rust peers every 50–1000ms);
//! we don't repeat it here.
//!
//! Gated behind the `interop` cargo feature. Required env:
//!   `RAPID_JAVA_JAR`   — path to the standalone-agent.jar.
//!   `JAVA_HOME`        — JDK 11+ root (Java 21 tested).

#![cfg(feature = "interop")]

use std::process::{Command, Stdio};
use std::time::Duration;

use rapid::messaging::traits::MessagingClient;
use rapid::messaging::GrpcPool;
use rapid::pb;

fn java_bin() -> String {
    std::env::var("JAVA_HOME")
        .map(|h| format!("{h}/bin/java"))
        .unwrap_or_else(|_| "java".into())
}

fn java_jar() -> String {
    std::env::var("RAPID_JAVA_JAR").expect("set RAPID_JAVA_JAR for interop tests")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_grpc_probe_java_seed_returns_ok() {
    let port = 28200u16;
    let addr = format!("127.0.0.1:{port}");
    let mut child = Command::new(java_bin())
        .args([
            "--add-opens",
            "java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens",
            "java.base/java.nio=ALL-UNNAMED",
            "-jar",
            &java_jar(),
            "-l",
            &addr,
            "-s",
            &addr,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn java standalone-agent");
    // Pause to let the Java agent finish gRPC server startup.
    tokio::time::sleep(Duration::from_secs(4)).await;

    let pool = GrpcPool::new();
    let sock: std::net::SocketAddr = addr.parse().unwrap();
    let req = pb::RapidRequest {
        content: Some(pb::rapid_request::Content::ProbeMessage(pb::ProbeMessage {
            sender: Some(pb::Endpoint {
                hostname: b"127.0.0.1".to_vec(),
                port: 28250,
            }),
            payload: Vec::new(),
        })),
    };
    let resp = pool.send(sock, req).await.expect("probe round-trip");
    let _ = child.kill();
    let Some(pb::rapid_response::Content::ProbeResponse(p)) = resp.content else {
        panic!("expected ProbeResponse, got {resp:?}");
    };
    assert_eq!(p.status, pb::NodeStatus::Ok as i32);
}
