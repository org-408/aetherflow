//! 動かして手触りを見るデモ。`cargo run --example demo`
//!
//! typed + move の actor に注文メッセージを送る。送信は move なので、送った後の `order` は
//! 触れない(下のコメントを外すとコンパイルエラーになる = 隔離が型で保証されている)。

use capability_proof::{spawn, Actor};
use std::sync::mpsc;

#[derive(Debug)]
struct Order {
    id: u32,
    qty: u32,
}

struct MatchingEngine {
    filled: u32,
    out: mpsc::Sender<String>,
}

impl Actor for MatchingEngine {
    type Message = Order;
    fn handle(&mut self, msg: Order) {
        self.filled += msg.qty;
        self.out
            .send(format!(
                "order #{} matched qty={} (cumulative filled={})",
                msg.id, msg.qty, self.filled
            ))
            .unwrap();
    }
}

fn main() {
    let (tx, rx) = mpsc::channel();
    let engine = spawn(MatchingEngine { filled: 0, out: tx });

    for id in 1..=3 {
        let order = Order { id, qty: id * 100 };
        engine.send(order); // move で移譲
                            // println!("{:?}", order); // ← これを外すと: borrow of moved value `order`
    }

    for _ in 0..3 {
        println!("{}", rx.recv().unwrap());
    }
}
