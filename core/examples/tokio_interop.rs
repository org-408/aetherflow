//! Tokio interop — **embed** AetherFlow inside a Tokio app (gradual adoption).
//!
//!   cargo run --example tokio_interop
//!
//! Use case: keep your existing async I/O (HTTP server, DB client, ...) on Tokio, and move just the
//! **state and compute** onto AetherFlow actors. No full rewrite — move hot state management onto a
//! lock-free, single-owned actor.
//!
//! Two interop rules:
//!  - **async → actor sends are non-blocking** (`try_send` / `send_blocking`) — call them from async.
//!  - **`ask` (awaiting a reply) blocks the calling thread**, so call it inside
//!    `tokio::task::spawn_blocking` to avoid stalling the async runtime.

use aetherflow::{Actor, Responder, System};

/// Shared aggregation state hit by many async handlers on the Tokio side — but with no lock.
struct Metrics {
    requests: u64,
    bytes: u64,
}

enum Cmd {
    Record { bytes: u64 },
    Snapshot(Responder<(u64, u64)>),
}

impl Actor for Metrics {
    type Message = Cmd;
    fn handle(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Record { bytes } => {
                self.requests += 1;
                self.bytes += bytes;
            }
            Cmd::Snapshot(reply) => reply.reply((self.requests, self.bytes)),
        }
    }
}

#[tokio::main]
async fn main() {
    // Stand up an AetherFlow runtime inside the Tokio app (on a shared box you might prefer
    // `with_cores_idle(n, IdleStrategy::backoff())` to avoid burning CPU).
    let sys = System::with_cores(1);
    let metrics = sys.spawn_on(
        0,
        Metrics {
            requests: 0,
            bytes: 0,
        },
    );

    // Simulate 100 async request handlers. Each does some async I/O, then reports its result to the
    // AetherFlow actor with a non-blocking send.
    let mut handles = Vec::new();
    for i in 0..100u64 {
        let m = metrics.clone();
        handles.push(tokio::spawn(async move {
            // Real network I/O would await here (Tokio's job).
            tokio::task::yield_now().await;
            // Delegate the state update to the actor (no lock, single-owned). Send is non-blocking.
            let _ = m.try_send(Cmd::Record { bytes: 100 + i });
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Read the aggregate = ask (awaits a reply). It blocks, so run it on a blocking thread.
    let m = metrics.clone();
    let (requests, bytes) = tokio::task::spawn_blocking(move || m.ask(Cmd::Snapshot).unwrap())
        .await
        .unwrap();

    println!("requests = {requests}, bytes = {bytes}");
    assert_eq!(requests, 100);
    assert_eq!(bytes, (0..100u64).map(|i| 100 + i).sum());

    drop(metrics);
    // shutdown also blocks, so run it on a blocking thread (don't stall async).
    tokio::task::spawn_blocking(move || sys.shutdown())
        .await
        .unwrap();
    println!("ok — AetherFlow ran as the state core inside a Tokio app");
}
