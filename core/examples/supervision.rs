//! Fault tolerance — when an actor panics, isolate just that actor and **replace it with a fresh
//! one**, then keep going.
//!
//!   cargo run --example supervision
//!
//! Key idea: the type system guarantees actor state is single-owned (never shared), so a panicked
//! actor can be safely dropped and rebuilt (no corrupted state lingers, shared by anyone). Contrast
//! `Arc<Mutex>`, which poisons on panic and becomes unusable — "isolation unlocks fault tolerance".
//!
//! Note: Rust's default panic message on stderr is expected (the isolation still happens).

use aetherflow::{Actor, System};
use std::sync::atomic::{AtomicU32, Ordering};

/// Count builds process-wide, so we can see how many times a fresh actor was created (restarts).
static BUILDS: AtomicU32 = AtomicU32::new(0);

/// A worker that processes jobs. Given `0` it panics (simulating a buggy input).
struct Worker {
    generation: u32,
    processed: u32,
}

impl Worker {
    fn new() -> Self {
        let generation = BUILDS.fetch_add(1, Ordering::SeqCst);
        Worker {
            generation,
            processed: 0,
        }
    }
}

impl Actor for Worker {
    type Message = i32;

    fn on_restart(&mut self) {
        println!("  [worker] restarted as generation {}", self.generation);
    }

    fn handle(&mut self, job: i32) {
        if job == 0 {
            panic!("worker hit a poison job (0)");
        }
        self.processed += 1;
        println!("  [worker gen{}] processed job {job} (#{})", self.generation, self.processed);
    }
}

fn main() {
    let sys = System::with_cores(1);

    // supervised = rebuild via make (Worker::new) on panic; the mailbox is preserved.
    let worker = sys.spawn_on_supervised(0, Worker::new);

    println!("sending: 1, 2, 0(poison), 3, 4");
    for job in [1, 2, 0, 3, 4] {
        worker.send_blocking(job).unwrap();
    }

    drop(worker);
    sys.shutdown();

    // gen0 handles 1,2 → panics on 0 → gen1 replaces it and handles 3,4. The system stays alive.
    let builds = BUILDS.load(Ordering::SeqCst);
    println!("total worker generations built = {builds} (1 initial + restarts)");
    assert!(builds >= 2, "should have restarted at least once");
    println!("ok — the panic was isolated; the system kept running");
}
