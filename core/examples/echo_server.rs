//! I/O as messages demo — an echo server (feature `net`).
//!
//! Run: `cargo run --example echo_server --features net -- 127.0.0.1:8079`
//!
//! This is all you write — you never read/write the socket. Bytes that arrive are delivered to
//! `on_data`, and you send with `io.write` (non-blocking). No `async`, no function coloring, no
//! `Pin`. Runs on the portable reference backend; the busy-poll performance path is Linux-only.

use aetherflow::net::{serve, Connection, Io};
use std::time::Duration;

struct Echo;

impl Connection for Echo {
    fn on_open(&mut self, io: &mut Io) {
        io.write(b"welcome to aetherflow echo\n");
    }
    fn on_data(&mut self, buf: &[u8], io: &mut Io) {
        io.write(buf); // echo the bytes straight back — done (run-to-completion).
    }
}

fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8079".to_string());
    let server = serve(&addr, || Echo).expect("bind failed");
    println!("listening on {}", server.local_addr());
    // Keep running (Ctrl-C to stop).
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}
