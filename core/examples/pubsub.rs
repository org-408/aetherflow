//! pub/sub (one-to-many broadcast) — a chat-room style Hub that fans a message out to all subscribers.
//!
//!   cargo run --example pubsub
//!
//! Key idea: `ActorRef` is **Clone + Send**, so you can **pass it inside a message** (that's how you
//! subscribe). The Hub keeps the subscriber list as single-owned state and, on `Publish`, delivers
//! to everyone with **try_send** (non-blocking). From inside a handler, use `try_send` (`send_blocking`
//! would deadlock on the same core).

use aetherflow::{Actor, ActorRef, System};

/// A subscriber. It just prints whatever message it receives, prefixed with its name.
struct Subscriber {
    name: &'static str,
}
impl Actor for Subscriber {
    type Message = String;
    fn handle(&mut self, msg: String) {
        println!("  [{}] {msg}", self.name);
    }
}

/// The broadcast Hub. It holds subscriber send-handles and fans each Publish out to all of them.
struct Hub {
    subscribers: Vec<ActorRef<Subscriber>>,
}
enum HubMsg {
    /// Register a subscriber (its ActorRef is passed in the message).
    Subscribe(ActorRef<Subscriber>),
    /// Deliver to all subscribers.
    Publish(String),
}
impl Actor for Hub {
    type Message = HubMsg;
    fn handle(&mut self, msg: HubMsg) {
        match msg {
            HubMsg::Subscribe(sub) => self.subscribers.push(sub),
            HubMsg::Publish(text) => {
                for sub in &self.subscribers {
                    // Non-blocking; drop on full/gone (best-effort pub/sub delivery).
                    let _ = sub.try_send(text.clone());
                }
            }
        }
    }
}

fn main() {
    let sys = System::with_cores(1);

    let hub = sys.spawn_on(
        0,
        Hub {
            subscribers: Vec::new(),
        },
    );

    // Spawn 3 subscribers and register them with the Hub (passing the handle in a message).
    for name in ["alice", "bob", "carol"] {
        let sub = sys.spawn_on(0, Subscriber { name });
        hub.send_blocking(HubMsg::Subscribe(sub)).unwrap();
    }

    // A publish reaches everyone.
    println!("publishing 2 messages to 3 subscribers:");
    hub.send_blocking(HubMsg::Publish("hello everyone".into())).unwrap();
    hub.send_blocking(HubMsg::Publish("meeting at 3pm".into())).unwrap();

    drop(hub);
    sys.shutdown(); // drain then stop = every delivery is processed
    println!("ok");
}
