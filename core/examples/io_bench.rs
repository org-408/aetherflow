//! I/O bench — AetherFlow busy-poll echo vs any TCP echo (glommio, etc.).
//!
//! Usage (measure with server and client pinned to separate cores on the same machine):
//!   # AetherFlow echo server (busy-poll + pinned to core 0)
//!   cargo run --release --example io_bench --features net -- aether-server 127.0.0.1:9001 0
//!   # client (RTT p50/p99 + throughput) on a separate core
//!   taskset -c 1 cargo run --release --example io_bench --features net -- client 127.0.0.1:9001 200000
//!
//! The server is protocol-agnostic (echoes received bytes verbatim), so the same client can also
//! measure the glommio echo (`echo_glommio`) = fair comparison.

use aetherflow::net::{serve_on_cores, serve_with, Connection, Io, ServeOptions};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

#[derive(Clone)]
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
            // 3rd arg = core selection. "0" single / "0,1,2,3" multiple (thread-per-core, SO_REUSEPORT).
            let cores: Vec<usize> = args
                .get(3)
                .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
                .unwrap_or_else(|| vec![0]);
            // 4th arg "epoll" selects the Linux epoll readiness backend; default is scan busy-poll.
            let epoll = args.get(4).map(|s| s == "epoll").unwrap_or(false);
            let opts = ServeOptions {
                busy_poll: true,
                pin_core: cores.first().copied(),
                epoll,
            };
            let mode = if epoll { "epoll" } else { "scan" };
            let server = if cores.len() == 1 {
                serve_with(&addr, || Echo, opts).expect("bind")
            } else {
                serve_on_cores(&addr, &cores, || Echo, opts).expect("bind")
            };
            println!(
                "aether {mode} busy-poll echo on {} (cores {cores:?})",
                server.local_addr()
            );
            loop {
                std::thread::sleep(Duration::from_secs(3600));
            }
        }
        "client" => {
            let addr = args.get(2).cloned().unwrap_or_else(|| "127.0.0.1:9001".into());
            let iters: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(200_000);
            let conns: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(1);
            run_client(&addr, iters, conns);
        }
        _ => {
            eprintln!("modes: aether-server <addr> <cores csv> [epoll] | client <addr> <iters> [conns]");
        }
    }
}

/// Opens `conns` connections concurrently and splits a total of `iters` round-trips evenly across
/// them. Each connection runs sequential request-response on its own thread. RTTs from all
/// connections are aggregated to compute percentiles; throughput is measured over the whole run.
/// **Error tolerance**: a worker whose connect or send/recv fails stops there and returns whatever
/// it collected (no panic). The failure count is printed at the end (so hitting fd/backlog limits
/// under high concurrency doesn't bring the whole run down).
fn run_client(addr: &str, iters: usize, conns: usize) {
    let per = iters / conns.max(1);
    let t0 = Instant::now();
    // Under high concurrency (thousands of connections) we spawn one thread per connection, so the
    // default 8MB stack would OOM. Use a smaller (512KB) stack so thousands of threads fit in memory.
    let handles: Vec<_> = (0..conns)
        .map(|_| {
            let addr = addr.to_string();
            std::thread::Builder::new()
                .stack_size(512 * 1024)
                .spawn(move || conn_worker(&addr, per))
                .expect("spawn worker")
        })
        .collect();
    let mut lat: Vec<u64> = Vec::with_capacity(per * conns);
    let mut failed = 0usize;
    for h in handles {
        match h.join() {
            Ok(Ok(v)) => lat.extend(v),
            Ok(Err(())) | Err(_) => failed += 1,
        }
    }
    let elapsed = t0.elapsed();

    if lat.is_empty() {
        println!("conns={conns:>4}  (all {failed} workers failed — likely fd/backlog limit)");
        return;
    }
    lat.sort_unstable();
    let pct = |p: f64| lat[((lat.len() as f64 * p) as usize).min(lat.len() - 1)];
    let thru = lat.len() as f64 / elapsed.as_secs_f64();
    let fail_note = if failed > 0 {
        format!("  (failed_conns={failed})")
    } else {
        String::new()
    };
    println!(
        "conns={:>4}  RTT ns p50={:>7} p90={:>7} p99={:>8} p999={:>8}  throughput(req-resp/s)={:>10.0}{}",
        conns,
        pct(0.50),
        pct(0.90),
        pct(0.99),
        pct(0.999),
        thru,
        fail_note
    );
}

/// Sequential request-response for a single connection. On an io error it stops and returns what it
/// collected (`Err(())` means total failure, e.g. the connection itself couldn't be established).
fn conn_worker(addr: &str, iters: usize) -> Result<Vec<u64>, ()> {
    let s = TcpStream::connect(addr).map_err(|_| ())?;
    let _ = s.set_nodelay(true); // disable Nagle = measure RTT accurately
    let mut s = s;
    let payload = [0xABu8; 32];
    let mut buf = [0u8; 32];
    // warmup
    for _ in 0..iters.min(5_000) {
        if s.write_all(&payload).is_err() || s.read_exact(&mut buf).is_err() {
            return Err(());
        }
    }
    let mut lat = Vec::with_capacity(iters);
    for _ in 0..iters {
        let a = Instant::now();
        if s.write_all(&payload).is_err() || s.read_exact(&mut buf).is_err() {
            break; // mid-run failure: return what was collected
        }
        lat.push(a.elapsed().as_nanos() as u64);
    }
    Ok(lat)
}
