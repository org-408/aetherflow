//! The smallest AetherFlow program — spawn one actor and send it messages.
//!
//!   cargo run --example hello_actor
//!
//! Key idea: you write **plain synchronous Rust**. An actor is "one message type + one handler".
//! `send` **moves** the message's ownership into the runtime (using it after send is a compile
//! error, `E0382`).

use aetherflow::{Actor, System};

/// An actor that counts greetings. State (`count`) is just a field — no lock, no `Arc`, because a
/// single core thread runs it to completion, so `&mut self` is the sole owner.
struct Greeter {
    count: u32,
}

impl Actor for Greeter {
    type Message = String; // the (fixed) message type this actor receives

    fn on_start(&mut self) {
        println!("[greeter] started");
    }

    fn handle(&mut self, name: String) {
        self.count += 1;
        println!("[greeter] hello, {name}! (#{})", self.count);
    }

    fn on_stop(&mut self) {
        println!("[greeter] stopped after {} greetings", self.count);
    }
}

fn main() {
    // Start a runtime with 1 core (= 1 thread).
    let sys = System::with_cores(1);

    // Place the actor on core 0. The returned value is a cloneable send handle.
    let greeter = sys.spawn_on(0, Greeter { count: 0 });

    for name in ["Ada", "Alan", "Grace"] {
        // Move the string's ownership into the runtime; Err(Closed) if the actor is gone.
        greeter.send_blocking(name.to_string()).unwrap();
    }

    // Drop the send handle, drain the mailbox, then stop (on_stop runs).
    drop(greeter);
    sys.shutdown();
}
