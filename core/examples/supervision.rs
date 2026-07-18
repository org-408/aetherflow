//! 耐障害 — actor が panic しても、その actor だけ切り離して**新品に入れ替えて続行**する。
//!
//!   cargo run --example supervision
//!
//! ポイント: actor 状態は単一所有(共有なし)と**型が保証**するので、panic した actor を安全に
//! 捨てて作り直せる(壊れた状態が誰にも共有されず残らない)。`Arc<Mutex>` 系は panic で Mutex が
//! poison して続行不能になるのと対照的 ── 「隔離が耐障害性を解禁する」。
//!
//! 注: panic 時に Rust 既定のパニックメッセージが stderr に出るのは正常(切り離しは起きている)。

use aetherflow::{Actor, System};
use std::sync::atomic::{AtomicU32, Ordering};

/// 生成回数をプロセス全体で数える(restart で作り直された回数が見える)。
static BUILDS: AtomicU32 = AtomicU32::new(0);

/// ジョブを処理する worker。`0` を渡されると panic する(= バグのある入力の模擬)。
struct Worker {
    generation: u32,
    processed: u32,
}

impl Worker {
    fn new() -> Self {
        let generation = BUILDS.fetch_add(1, Ordering::SeqCst);
        Worker {
            generation,
            processed: 0,
        }
    }
}

impl Actor for Worker {
    type Message = i32;

    fn on_restart(&mut self) {
        println!("  [worker] restarted as generation {}", self.generation);
    }

    fn handle(&mut self, job: i32) {
        if job == 0 {
            panic!("worker hit a poison job (0)");
        }
        self.processed += 1;
        println!("  [worker gen{}] processed job {job} (#{})", self.generation, self.processed);
    }
}

fn main() {
    let sys = System::with_cores(1);

    // supervised = パニックしたら make(=Worker::new)で作り直す。mailbox は保たれる。
    let worker = sys.spawn_on_supervised(0, Worker::new);

    println!("sending: 1, 2, 0(poison), 3, 4");
    for job in [1, 2, 0, 3, 4] {
        worker.send_blocking(job).unwrap();
    }

    drop(worker);
    sys.shutdown();

    // gen0 が 1,2 を処理 → 0 で panic → gen1 に入れ替わり 3,4 を処理。system は生きたまま。
    let builds = BUILDS.load(Ordering::SeqCst);
    println!("total worker generations built = {builds} (1 initial + restarts)");
    assert!(builds >= 2, "should have restarted at least once");
    println!("ok — the panic was isolated; the system kept running");
}
