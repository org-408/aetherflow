//! いちばん小さな AetherFlow — actor を1つ立ててメッセージを送る。
//!
//!   cargo run --example hello_actor
//!
//! ポイント: あなたが書くのは **普通の同期 Rust** だけ。actor = 「1つのメッセージ型 + 1つのハンドラ」。
//! `send` するとメッセージの所有権が runtime に **move** される(= 送った後に使うとコンパイルエラー)。

use aetherflow::{Actor, System};

/// 挨拶を数える actor。状態(`count`)はフィールドに持つだけ ── ロックも `Arc` も不要
/// (単一コアスレッドが run-to-completion で回すので `&mut self` が唯一の所有者)。
struct Greeter {
    count: u32,
}

impl Actor for Greeter {
    type Message = String; // この actor が受け取るメッセージの型(固定)

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
    // 1 コア(= 1 本のスレッド)の runtime を起動。
    let sys = System::with_cores(1);

    // コア 0 に actor を置く。返り値はクローン可能な送信ハンドル。
    let greeter = sys.spawn_on(0, Greeter { count: 0 });

    for name in ["Ada", "Alan", "Grace"] {
        // 文字列の所有権を runtime へ move。actor が gone なら Err(Closed) が返る。
        greeter.send_blocking(name.to_string()).unwrap();
    }

    // 送信端を落として、mailbox を drain し切ってから停止(on_stop が走る)。
    drop(greeter);
    sys.shutdown();
}
