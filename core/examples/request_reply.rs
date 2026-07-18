//! request-reply (`ask`) — send and **wait for a reply**. Build a small key-value store as an actor.
//!
//!   cargo run --example request_reply
//!
//! Key idea: `ask` puts the reply slot on the **caller's stack** → **zero heap allocation**
//! (kameo/tokio allocate a fresh oneshot per call). This is speed unlocked by the ownership model.
//!
//! Note: `ask` blocks the calling thread, so call it from outside the runtime (main / an I/O thread).

use aetherflow::{Actor, Responder, System};
use std::collections::HashMap;

/// A KV store actor. Its state (`map`) is single-owned — no lock needed.
struct KvStore {
    map: HashMap<String, i64>,
}

/// Commands this actor accepts. Ones that need a reply carry a `Responder<R>` (the ask channel).
enum Cmd {
    /// No reply (fire-and-forget); send it with `send`.
    Set(String, i64),
    /// Needs a reply; send it with `ask`, the value comes back through the `Responder`.
    Get(String, Responder<Option<i64>>),
    /// Return the number of keys.
    Len(Responder<usize>),
}

impl Actor for KvStore {
    type Message = Cmd;

    fn handle(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Set(k, v) => {
                self.map.insert(k, v);
            }
            Cmd::Get(k, reply) => {
                reply.reply(self.map.get(&k).copied()); // a linear token you reply through exactly once
            }
            Cmd::Len(reply) => {
                reply.reply(self.map.len());
            }
        }
    }
}

fn main() {
    let sys = System::with_cores(1);
    let kv = sys.spawn_on(
        0,
        KvStore {
            map: HashMap::new(),
        },
    );

    // Writes are fire-and-forget (no reply awaited).
    kv.send_blocking(Cmd::Set("apples".into(), 3)).unwrap();
    kv.send_blocking(Cmd::Set("pears".into(), 7)).unwrap();

    // Reads use `ask` (await the reply). Pass the Responder into `|reply| ...` to build the message.
    let apples: Option<i64> = kv.ask(|reply| Cmd::Get("apples".into(), reply)).unwrap();
    let bananas: Option<i64> = kv.ask(|reply| Cmd::Get("bananas".into(), reply)).unwrap();
    let n: usize = kv.ask(Cmd::Len).unwrap();

    println!("apples  = {apples:?}"); // Some(3)
    println!("bananas = {bananas:?}"); // None
    println!("keys    = {n}"); // 2

    assert_eq!(apples, Some(3));
    assert_eq!(bananas, None);
    assert_eq!(n, 2);

    drop(kv);
    sys.shutdown();
    println!("ok");
}
