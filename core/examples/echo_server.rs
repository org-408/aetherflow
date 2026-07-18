//! I/O as messages のデモ — echo サーバ(feature `net`)。
//!
//! 実行: `cargo run --example echo_server --features net -- 127.0.0.1:8079`
//!
//! ユーザーが書くのはこれだけ ── socket を read/write せず、届いたバイトを `on_data` で受け、
//! 送信は `io.write`(非ブロッキング)。await 無し・関数の色無し・`Pin` 無し。
//! 参照バックエンド(移植性優先)で動く。busy-poll 性能版は Linux で後追い。

use aetherflow::net::{serve, Connection, Io};
use std::time::Duration;

struct Echo;

impl Connection for Echo {
    fn on_open(&mut self, io: &mut Io) {
        io.write(b"welcome to aetherflow echo\n");
    }
    fn on_data(&mut self, buf: &[u8], io: &mut Io) {
        io.write(buf); // 受け取った分をそのまま返す。それで終わり(run-to-completion)。
    }
}

fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8079".to_string());
    let server = serve(&addr, || Echo).expect("bind failed");
    println!("listening on {}", server.local_addr());
    // 動かし続ける(Ctrl-C で終了)。
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}
