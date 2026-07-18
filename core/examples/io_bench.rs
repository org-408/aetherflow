//! I/O ベンチ — AetherFlow busy-poll echo vs 任意の TCP echo(glommio 等)。
//!
//! 使い方(同一マシンで server と client を別コアに固定して測る):
//!   # AetherFlow echo server(busy-poll + core 0 ピン)
//!   cargo run --release --example io_bench --features net -- aether-server 127.0.0.1:9001 0
//!   # client(RTT p50/p99 + throughput)を別コアで
//!   taskset -c 1 cargo run --release --example io_bench --features net -- client 127.0.0.1:9001 200000
//!
//! server はプロトコル非依存(受けたバイトをそのまま返す)なので、同じ client で glommio echo
//! (`echo_glommio`)も測れる = フェア比較。

use aetherflow::net::{serve_with, Connection, Io, ServeOptions};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

struct Echo;
impl Connection for Echo {
    fn on_data(&mut self, buf: &[u8], io: &mut Io) {
        io.write(buf);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("help");
    match mode {
        "aether-server" => {
            let addr = args.get(2).cloned().unwrap_or_else(|| "127.0.0.1:9001".into());
            let core: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
            let server = serve_with(
                &addr,
                || Echo,
                ServeOptions {
                    busy_poll: true,
                    pin_core: Some(core),
                },
            )
            .expect("bind");
            println!("aether busy-poll echo on {} (pinned core {core})", server.local_addr());
            loop {
                std::thread::sleep(Duration::from_secs(3600));
            }
        }
        "client" => {
            let addr = args.get(2).cloned().unwrap_or_else(|| "127.0.0.1:9001".into());
            let iters: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(200_000);
            run_client(&addr, iters);
        }
        _ => {
            eprintln!("modes: aether-server <addr> <core> | client <addr> <iters>");
        }
    }
}

fn run_client(addr: &str, iters: usize) {
    let mut s = TcpStream::connect(addr).expect("connect");
    s.set_nodelay(true).unwrap(); // Nagle 無効 = RTT を正しく測る
    let payload = [0xABu8; 32];
    let mut buf = [0u8; 32];

    // warmup
    for _ in 0..iters.min(20_000) {
        s.write_all(&payload).unwrap();
        s.read_exact(&mut buf).unwrap();
    }

    // RTT 測定(1 往復ずつ = ping-pong)
    let mut lat = Vec::with_capacity(iters);
    let t0 = Instant::now();
    for _ in 0..iters {
        let a = Instant::now();
        s.write_all(&payload).unwrap();
        s.read_exact(&mut buf).unwrap();
        lat.push(a.elapsed().as_nanos() as u64);
    }
    let elapsed = t0.elapsed();

    lat.sort_unstable();
    let pct = |p: f64| lat[((lat.len() as f64 * p) as usize).min(lat.len() - 1)];
    let thru = iters as f64 / elapsed.as_secs_f64();
    println!(
        "RTT ns  p50={:>7}  p90={:>7}  p99={:>7}  p999={:>7}  max={:>8}",
        pct(0.50),
        pct(0.90),
        pct(0.99),
        pct(0.999),
        lat[lat.len() - 1]
    );
    println!("throughput (req-resp/s, single conn seq) = {:.0}", thru);
}
