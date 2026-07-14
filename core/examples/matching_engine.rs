//! マルチコアの手触りデモ: シンボル別にコアへ分散した matching engine に、gateway が
//! 注文をルーティングする。`cargo run --release --example matching_engine`
//!
//! 構成(thread-per-core + sharding):
//!   gateway(コア0) ──symbol でルーティング──▶ engine[s](コア 1+s、各コアに固定)
//!
//! これは動作・配線の確認用で、レイテンシ計測ではない(macOS では真のピン留めが効かない。
//! ベンチは Linux + Stage 0 で kompicsbenches 流に測る — competitive-landscape.md 参照)。

use aetherflow::{pinning, Actor, ActorRef, System};
use std::sync::mpsc;

#[derive(Debug)]
struct Order {
    symbol: u8,
    id: u64,
    qty: u32,
}

/// シンボル 1 つ分の超単純な matching engine(累積約定数量を数えるだけ)。
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

/// 注文をシンボルで該当 engine へ振り分ける(handler 内 → 別コアへ try_send)。
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

    let sys = System::with_cores((SYMBOLS as usize) + 1); // コア0=gateway、1..=engines
    let (out, log) = mpsc::channel();

    // engine をシンボルごとに別コアへ。
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

    // 12 注文を投入(シンボルは round-robin)。
    for id in 1..=12u64 {
        let order = Order {
            symbol: (id % SYMBOLS as u64) as u8,
            id,
            qty: (id * 10) as u32,
        };
        gateway.send_blocking(order).unwrap();
    }

    // 送信端を全て落として drain → shutdown。
    drop(gateway);
    drop(engines);
    drop(out);
    sys.shutdown();

    let mut lines: Vec<String> = log.iter().collect();
    lines.sort(); // 出力順はコア間で非決定的なので整列して表示
    for l in lines {
        println!("{l}");
    }
}
