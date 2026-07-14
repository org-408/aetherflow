//! Stage 0 ベンチ(その1): ping-pong 往復レイテンシと片方向スループットを、
//! 本ランタイム(thread-per-core)と Tokio(work-stealing)で比較する。
//!
//! 走らせ方: `cargo bench --bench latency`(harness=false の素の main)。
//!
//! **重要な但し書き**:
//! - macOS ではハードなコアピン留めが効かない(no-op)。tail latency の**保証**と絶対 p99.9 は
//!   Linux(理想は ARM Graviton)で測ること。ここで出るのは方向性の相対シグナル。
//! - 本ランタイムの消費者はビジースピン(LMAX 流)。低レイテンシと引き換えにアイドルでも CPU を
//!   使う。Tokio は park/wake で CPU を使わない代わりにレイテンシが乗る。**同条件ではない**ので、
//!   「レイテンシ ↔ CPU 使用のトレードオフ」として読むこと。

use aetherflow::{Actor, IdleStrategy, Responder, System};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::time::Instant;

const WARMUP: usize = 20_000;
const SAMPLES: usize = 200_000;
const THROUGHPUT_N: usize = 2_000_000;

fn percentiles(v: &mut [u64]) -> (u64, u64, u64, u64, u64) {
    v.sort_unstable();
    let at = |q: f64| v[(((v.len() as f64) * q) as usize).min(v.len() - 1)];
    (at(0.50), at(0.90), at(0.99), at(0.999), *v.last().unwrap())
}

// ---- 本ランタイム(thread-per-core) ------------------------------------------

/// ponger: 受けたら reply チャネルに () を返すだけ。
struct Ponger {
    reply: SyncSender<()>,
}
impl Actor for Ponger {
    type Message = ();
    fn handle(&mut self, _msg: ()) {
        self.reply.send(()).unwrap();
    }
}

fn aether_pingpong(idle: IdleStrategy) -> (u64, u64, u64, u64, u64) {
    let sys = System::with_cores_idle(1, idle);
    let (reply_tx, reply_rx) = sync_channel::<()>(1);
    let addr = sys.spawn_on(0, Ponger { reply: reply_tx });

    for _ in 0..WARMUP {
        addr.send_blocking(()).unwrap();
        reply_rx.recv().unwrap();
    }
    let mut lat = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let t0 = Instant::now();
        addr.send_blocking(()).unwrap();
        reply_rx.recv().unwrap();
        lat.push(t0.elapsed().as_nanos() as u64);
    }
    drop(addr);
    sys.shutdown();
    percentiles(&mut lat)
}

/// sink: 受け取るだけ。最後の 1 通で完了シグナルを返す。
struct Sink {
    remaining: usize,
    done: SyncSender<()>,
}
impl Actor for Sink {
    type Message = u64;
    fn handle(&mut self, _msg: u64) {
        self.remaining -= 1;
        if self.remaining == 0 {
            self.done.send(()).unwrap();
        }
    }
}

fn aether_throughput() -> f64 {
    let sys = System::with_cores(1);
    let (done_tx, done_rx) = sync_channel::<()>(1);
    let addr = sys.spawn_on_with(
        0,
        Sink {
            remaining: THROUGHPUT_N,
            done: done_tx,
        },
        1 << 16,
    );
    let t0 = Instant::now();
    for i in 0..THROUGHPUT_N {
        addr.send_blocking(i as u64).unwrap();
    }
    done_rx.recv().unwrap();
    let secs = t0.elapsed().as_secs_f64();
    drop(addr);
    sys.shutdown();
    (THROUGHPUT_N as f64) / secs
}

// ---- 本ランタイム: ゼロアロ ask(request-reply) -------------------------------

/// ask 用 ponger: Responder に返信するだけ。
struct AskPonger;
impl Actor for AskPonger {
    type Message = Responder<u64>;
    fn handle(&mut self, resp: Responder<u64>) {
        resp.reply(1);
    }
}

fn aether_ask() -> (u64, u64, u64, u64, u64) {
    let sys = System::with_cores(1);
    let addr = sys.spawn_on(0, AskPonger);
    for _ in 0..WARMUP {
        addr.ask(|resp| resp).unwrap();
    }
    let mut lat = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let t0 = Instant::now();
        addr.ask(|resp| resp).unwrap();
        lat.push(t0.elapsed().as_nanos() as u64);
    }
    drop(addr);
    sys.shutdown();
    percentiles(&mut lat)
}

// ---- Tokio ベースライン(work-stealing) --------------------------------------

fn tokio_pingpong() -> (u64, u64, u64, u64, u64) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (req_tx, mut req_rx) = tokio::sync::mpsc::channel::<tokio::sync::oneshot::Sender<()>>(64);
        tokio::spawn(async move {
            while let Some(reply) = req_rx.recv().await {
                let _ = reply.send(());
            }
        });

        for _ in 0..WARMUP {
            let (o_tx, o_rx) = tokio::sync::oneshot::channel();
            req_tx.send(o_tx).await.unwrap();
            o_rx.await.unwrap();
        }
        let mut lat = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let (o_tx, o_rx) = tokio::sync::oneshot::channel();
            let t0 = Instant::now();
            req_tx.send(o_tx).await.unwrap();
            o_rx.await.unwrap();
            lat.push(t0.elapsed().as_nanos() as u64);
        }
        percentiles(&mut lat)
    })
}

fn tokio_throughput() -> f64 {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(1 << 16);
        let consumer = tokio::spawn(async move {
            let mut n = 0usize;
            while rx.recv().await.is_some() {
                n += 1;
                if n == THROUGHPUT_N {
                    break;
                }
            }
        });
        let t0 = Instant::now();
        for i in 0..THROUGHPUT_N {
            tx.send(i as u64).await.unwrap();
        }
        consumer.await.unwrap();
        let secs = t0.elapsed().as_secs_f64();
        (THROUGHPUT_N as f64) / secs
    })
}

// ---- kameo ベースライン(実 actor フレームワーク、Tokio 上) --------------------

use kameo::actor::Spawn;
use kameo::message::{Context, Message};

#[derive(kameo::Actor)]
struct KameoPonger;
struct KPing;
impl Message<KPing> for KameoPonger {
    type Reply = ();
    async fn handle(&mut self, _msg: KPing, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {}
}

fn kameo_pingpong() -> (u64, u64, u64, u64, u64) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let actor_ref = KameoPonger::spawn(KameoPonger);
        for _ in 0..WARMUP {
            let _ = actor_ref.ask(KPing).await;
        }
        let mut lat = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let t0 = Instant::now();
            let _ = actor_ref.ask(KPing).await;
            lat.push(t0.elapsed().as_nanos() as u64);
        }
        percentiles(&mut lat)
    })
}

// ---- kompact ベースライン(高速 Rust MP、cross-platform) ---------------------
// kompact::Actor が aetherflow::Actor と名前衝突するため module に閉じる。
mod kompact_bench {
    use kompact::prelude::*;
    use std::time::Instant;

    #[derive(ComponentDefinition)]
    struct KomPonger {
        ctx: ComponentContext<Self>,
    }
    impl KomPonger {
        fn new() -> Self {
            KomPonger {
                ctx: ComponentContext::uninitialised(),
            }
        }
    }
    ignore_lifecycle!(KomPonger);
    impl Actor for KomPonger {
        type Message = Ask<(), ()>;
        fn receive_local(&mut self, msg: Self::Message) -> Handled {
            msg.reply(()).expect("reply");
            Handled::Ok
        }
        fn receive_network(&mut self, _msg: NetMessage) -> Handled {
            Handled::Ok
        }
    }

    /// request-reply(ask)の往復レイテンシ。aether の ask / kameo の ask と対応。
    pub fn pingpong() -> (u64, u64, u64, u64, u64) {
        let system = KompactConfig::default().build().expect("system");
        let ponger = system.create(KomPonger::new);
        system.start(&ponger);
        let pref: ActorRef<Ask<(), ()>> = ponger.actor_ref();

        for _ in 0..super::WARMUP {
            pref.ask(()).wait();
        }
        let mut lat = Vec::with_capacity(super::SAMPLES);
        for _ in 0..super::SAMPLES {
            let t0 = Instant::now();
            pref.ask(()).wait();
            lat.push(t0.elapsed().as_nanos() as u64);
        }
        system.shutdown().expect("shutdown");
        super::percentiles(&mut lat)
    }

    #[derive(ComponentDefinition)]
    struct KomSink {
        ctx: ComponentContext<Self>,
        remaining: u64,
        done: std::sync::mpsc::SyncSender<()>,
    }
    ignore_lifecycle!(KomSink);
    impl Actor for KomSink {
        type Message = u64;
        fn receive_local(&mut self, _msg: u64) -> Handled {
            self.remaining -= 1;
            if self.remaining == 0 {
                self.done.send(()).ok();
            }
            Handled::Ok
        }
        fn receive_network(&mut self, _msg: NetMessage) -> Handled {
            Handled::Ok
        }
    }

    /// one-way throughput: 単一生産者 → 単一 component。aether/tokio の throughput と対応
    /// (kompact の本領。latency だけでなくここも測って公正にする)。
    pub fn throughput() -> f64 {
        let system = KompactConfig::default().build().expect("system");
        let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);
        let sink = system.create(|| KomSink {
            ctx: ComponentContext::uninitialised(),
            remaining: super::THROUGHPUT_N as u64,
            done: tx,
        });
        system.start(&sink);
        let sref: ActorRef<u64> = sink.actor_ref();
        let t0 = Instant::now();
        for i in 0..super::THROUGHPUT_N {
            sref.tell(i as u64);
        }
        rx.recv().unwrap();
        let secs = t0.elapsed().as_secs_f64();
        system.shutdown().expect("shutdown");
        super::THROUGHPUT_N as f64 / secs
    }
}

fn main() {
    println!("# Stage 0 latency bench (macOS/no-pin — 相対シグナル。権威ある数字は Linux で)");
    println!("cores: {:?}", aetherflow::pinning::available_cores());
    println!("samples={SAMPLES} warmup={WARMUP} throughput_n={THROUGHPUT_N}\n");

    println!("## ping-pong RTT (nanoseconds), jitter = p99/p50");
    println!(
        "{:<12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8}",
        "runtime", "p50", "p90", "p99", "p999", "max", "jitter"
    );
    let (a50, a90, a99, a999, amax) = aether_pingpong(IdleStrategy::BusySpin);
    println!(
        "{:<12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8.1}",
        "aether-spin", a50, a90, a99, a999, amax, a99 as f64 / a50 as f64
    );
    let (b50, b90, b99, b999, bmax) = aether_pingpong(IdleStrategy::backoff());
    println!(
        "{:<12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8.1}",
        "aether-backoff", b50, b90, b99, b999, bmax, b99 as f64 / b50 as f64
    );
    let (t50, t90, t99, t999, tmax) = tokio_pingpong();
    println!(
        "{:<12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8.1}",
        "tokio", t50, t90, t99, t999, tmax, t99 as f64 / t50 as f64
    );
    let (k50, k90, k99, k999, kmax) = kameo_pingpong();
    println!(
        "{:<12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8.1}",
        "kameo-ask", k50, k90, k99, k999, kmax, k99 as f64 / k50 as f64
    );
    let (c50, c90, c99, c999, cmax) = kompact_bench::pingpong();
    println!(
        "{:<12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8.1}",
        "kompact-ask", c50, c90, c99, c999, cmax, c99 as f64 / c50 as f64
    );

    println!("\n## ask (request-reply) RTT (ns) — zero-alloc vs kameo's per-call oneshot");
    let (q50, q90, q99, q999, qmax) = aether_ask();
    println!(
        "{:<12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8.1}",
        "aether-ask", q50, q90, q99, q999, qmax, q99 as f64 / q50 as f64
    );
    println!(
        "{:<12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8.1}",
        "kameo-ask", k50, k90, k99, k999, kmax, k99 as f64 / k50 as f64
    );

    println!("\n## one-way throughput (messages/sec, single producer→single consumer)");
    let a_tp = aether_throughput();
    let t_tp = tokio_throughput();
    let k_tp = kompact_bench::throughput();
    println!("{:<10} {:>16.0}", "aether", a_tp);
    println!("{:<10} {:>16.0}", "tokio", t_tp);
    println!("{:<10} {:>16.0}", "kompact", k_tp);

    println!("\n注: aether はビジースピン(低レイテンシ↔CPU 消費)。Tokio は park/wake。同条件ではない。");
}
