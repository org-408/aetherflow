//! Scheduling — delayed and periodic messages (`send_after` / `send_every`).
//!
//!   cargo run --example timers
//!
//! Key idea: an `ActorRef` can schedule messages to itself (or any actor). `send_after` fires once
//! after a delay; `send_every` fires on an interval and returns a `TimerHandle` you can `cancel`.
//! Delivery is non-blocking (best-effort), and a periodic timer stops on its own once the actor is gone.

use aetherflow::{Actor, System};
use std::time::Duration;

/// A heartbeat actor: counts ticks, and knows how to say a one-off "warmup done".
struct Heartbeat {
    ticks: u32,
}

enum Msg {
    Tick,
    WarmupDone,
}

impl Actor for Heartbeat {
    type Message = Msg;
    fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Tick => {
                self.ticks += 1;
                println!("  tick #{}", self.ticks);
            }
            Msg::WarmupDone => println!("  (warmup done — one-shot after 50ms)"),
        }
    }
}

fn main() {
    let sys = System::with_cores(1);
    let hb = sys.spawn_on(0, Heartbeat { ticks: 0 });

    // One-shot: fire once after 50ms (fire-and-forget — no need to keep the handle).
    hb.send_after(Duration::from_millis(50), Msg::WarmupDone);

    // Periodic: tick every 30ms. Keep the handle so we can stop it.
    println!("ticking every 30ms for ~200ms:");
    let ticker = hb.send_every(Duration::from_millis(30), || Msg::Tick);

    std::thread::sleep(Duration::from_millis(200));
    ticker.cancel(); // stop the periodic timer
    println!("cancelled the ticker");

    // Give the last in-flight messages a moment, then drain and stop.
    std::thread::sleep(Duration::from_millis(20));
    drop(hb);
    sys.shutdown();
    println!("ok");
}
