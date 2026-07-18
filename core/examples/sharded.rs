//! sharding / fan-out — put N workers on N cores and route by key.
//!
//!   cargo run --example sharded
//!
//! Key idea: thread-per-core is **natural** to write. Each shard is a separate actor on a separate
//! core (separate thread), so they progress in parallel with no locks and no shared state. Routing
//! is by key hash (the same key always goes to the same shard).

use aetherflow::{Actor, Responder, System};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// One shard = a counter. It just counts occurrences of the keys routed to it (single-owned state).
struct Shard {
    id: usize,
    counts: std::collections::HashMap<String, u64>,
}

enum Msg {
    Bump(String),
    /// Return the total number of events this shard has seen.
    Total(Responder<(usize, u64)>),
}

impl Actor for Shard {
    type Message = Msg;
    fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Bump(k) => *self.counts.entry(k).or_insert(0) += 1,
            Msg::Total(reply) => {
                let sum: u64 = self.counts.values().sum();
                reply.reply((self.id, sum));
            }
        }
    }
}

fn shard_of(key: &str, n: usize) -> usize {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() % n as u64) as usize
}

fn main() {
    const SHARDS: usize = 4;
    let sys = System::with_cores(SHARDS);

    // Place shard i on core i; keep the handles in an array.
    let shards: Vec<_> = (0..SHARDS)
        .map(|id| {
            sys.spawn_on(
                id,
                Shard {
                    id,
                    counts: Default::default(),
                },
            )
        })
        .collect();

    // Route a stream of events by key (non-blocking fire-and-forget).
    let keys = ["alice", "bob", "carol", "dave", "erin", "frank"];
    for i in 0..10_000 {
        let key = keys[i % keys.len()];
        let s = shard_of(key, SHARDS);
        shards[s].send_blocking(Msg::Bump(key.to_string())).unwrap();
    }

    // Ask each shard for its total (they must sum to 10,000 = nothing dropped).
    let mut grand = 0u64;
    for s in &shards {
        let (id, total) = s.ask(Msg::Total).unwrap();
        println!("shard {id}: {total} events");
        grand += total;
    }
    println!("total = {grand}");
    assert_eq!(grand, 10_000);

    drop(shards);
    sys.shutdown();
    println!("ok");
}
