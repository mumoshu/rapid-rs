//! Loom models for the rapid actor mailbox + shutdown protocol.
//!
//! This crate is a *detached* workspace member — it sets
//! `RUSTFLAGS="--cfg loom"` only for its own build. From repo root:
//!
//! ```bash
//! cd crates/rapid-loom-tests && RUSTFLAGS="--cfg loom" cargo test --release
//! ```
//!
//! The models mirror the protocol the live actor follows in
//! [`rapid::service::task`](../../rapid/src/service/task.rs):
//! - The actor's `mpsc::Receiver<ServiceCommand>` loop drains messages
//!   until every sender drops *or* a `Shutdown` command arrives.
//! - Multiple producers (RPC handlers, batcher tick, FD notifier pump)
//!   send concurrently and must never deadlock with each other.
//!
//! The models below are a minimal isolated translation. They lock in
//! the *protocol*; the actual `ServiceState` mutation tests live in
//! `crates/rapid/tests/`.

#![cfg(loom)]

#[cfg(test)]
mod tests {
    use loom::sync::mpsc;
    use loom::thread;

    /// Shutdown signal queued behind in-flight requests is honoured.
    #[test]
    fn shutdown_after_requests() {
        loom::model(|| {
            let (tx, rx) = mpsc::channel::<u8>();
            let tx2 = tx.clone();

            let producer = thread::spawn(move || {
                let _ = tx.send(1);
                let _ = tx.send(2);
            });
            let shutdown_producer = thread::spawn(move || {
                let _ = tx2.send(0);
            });

            let mut saw_shutdown = false;
            while let Ok(v) = rx.recv() {
                if v == 0 {
                    saw_shutdown = true;
                    break;
                }
            }
            producer.join().unwrap();
            shutdown_producer.join().unwrap();
            assert!(saw_shutdown);
        });
    }

    /// Two producers racing on the same channel never lose messages.
    #[test]
    fn no_lost_messages_under_concurrent_producers() {
        loom::model(|| {
            let (tx, rx) = mpsc::channel::<u8>();
            let tx2 = tx.clone();
            let p1 = thread::spawn(move || {
                tx.send(1).unwrap();
            });
            let p2 = thread::spawn(move || {
                tx2.send(2).unwrap();
            });
            let v1 = rx.recv().unwrap();
            let v2 = rx.recv().unwrap();
            p1.join().unwrap();
            p2.join().unwrap();
            let mut seen = [v1, v2];
            seen.sort();
            assert_eq!(seen, [1, 2]);
        });
    }
}
