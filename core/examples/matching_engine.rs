//! A multi-core feel demo: a gateway routes orders to a matching engine sharded across cores by
//! symbol. `cargo run --release --example matching_engine`
//!
//! Layout (thread-per-core + sharding):
//!   gateway (core 0) ──route by symbol──▶ engine[s] (core 1+s, pinned to its core)
//!
//! This validates behavior and wiring, not latency (real pinning has no effect on macOS; benchmark
//! on Linux + Stage 0, kompicsbenches-style — see competitive-landscape.md).

use aetherflow::{pinning, Actor, ActorRef, System};
use std::sync::mpsc;

#[derive(Debug)]
struct Order {
    symbol: u8,
    id: u64,
    qty: u32,
}

/// A dead-simple matching engine for one symbol (just accumulates filled quantity).
struct Engine {
    symbol: u8,
    filled: u64,
    out: mpsc::Sender<String>,
}
impl Actor for Engine {
    type Message = Order;
    fn on_start(&mut self) {
        self.out
            .send(format!("engine[sym={}] started", self.symbol))
            .unwrap();
    }
    fn handle(&mut self, o: Order) {
        self.filled += o.qty as u64;
        self.out
            .send(format!(
                "  sym={} order#{} qty={} filled_total={}",
                o.symbol, o.id, o.qty, self.filled
            ))
            .unwrap();
    }
    fn on_stop(&mut self) {
        self.out
            .send(format!("engine[sym={}] stopped total={}", self.symbol, self.filled))
            .unwrap();
    }
}

/// Routes each order to the engine for its symbol (from a handler → try_send to another core).
struct Gateway {
    engines: Vec<ActorRef<Engine>>,
}
impl Actor for Gateway {
    type Message = Order;
    fn handle(&mut self, o: Order) {
        let idx = (o.symbol as usize) % self.engines.len();
        let _ = self.engines[idx].try_send(o); // cross-core routing
    }
}

fn main() {
    println!("available logical cores: {:?}", pinning::available_cores());
    const SYMBOLS: u8 = 3;

    let sys = System::with_cores((SYMBOLS as usize) + 1); // core 0 = gateway, 1.. = engines
    let (out, log) = mpsc::channel();

    // Place each engine on its own core, one per symbol.
    let engines: Vec<ActorRef<Engine>> = (0..SYMBOLS)
        .map(|s| {
            sys.spawn_on(
                (s as usize) + 1,
                Engine {
                    symbol: s,
                    filled: 0,
                    out: out.clone(),
                },
            )
        })
        .collect();

    let gateway = sys.spawn_on(0, Gateway { engines: engines.clone() });

    // Submit 12 orders (symbols round-robin).
    for id in 1..=12u64 {
        let order = Order {
            symbol: (id % SYMBOLS as u64) as u8,
            id,
            qty: (id * 10) as u32,
        };
        gateway.send_blocking(order).unwrap();
    }

    // Drop all send handles → drain → shutdown.
    drop(gateway);
    drop(engines);
    drop(out);
    sys.shutdown();

    let mut lines: Vec<String> = log.iter().collect();
    lines.sort(); // output order is nondeterministic across cores, so sort for display
    for l in lines {
        println!("{l}");
    }
}
