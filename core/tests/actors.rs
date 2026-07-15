//! ランタイム統合テスト: System → spawn → move 送信 → run-to-completion → shutdown。

use aetherflow::{
    Actor, ActorRef, AskError, Responder, RestartPolicy, SchedulingPolicy, SendError, System,
    TrySendError,
};
use std::sync::mpsc;
use std::time::Duration;

/// handler 内から同一コアの actor へ ask すると、ハングせず `WouldBlockCallingCore` で返る
/// (デッドロックガード)。沈黙のハングを明確なエラーに変える。
#[test]
fn ask_from_handler_to_same_core_returns_error_not_hang() {
    struct Callee;
    impl Actor for Callee {
        type Message = Responder<u64>;
        fn handle(&mut self, r: Responder<u64>) {
            r.reply(1);
        }
    }
    struct Caller {
        callee: ActorRef<Callee>,
        out: mpsc::Sender<Result<u64, AskError>>,
    }
    impl Actor for Caller {
        type Message = ();
        fn handle(&mut self, _m: ()) {
            // 同一コアの callee へ ask → ガードが Err を返す(ブロックしてハングしない)
            let r = self.callee.ask(|resp| resp);
            self.out.send(r).unwrap();
        }
    }

    let sys = System::with_cores(1);
    let callee = sys.spawn_on(0, Callee);
    let (out, rx) = mpsc::channel();
    let caller = sys.spawn_on(0, Caller {
        callee: callee.clone(),
        out,
    });
    caller.send_blocking(()).unwrap(); // main スレッドからは通常送信
    assert_eq!(rx.recv().unwrap(), Err(AskError::WouldBlockCallingCore));

    drop(caller);
    drop(callee);
    sys.shutdown();
}

/// 型隔離のおかげで、同一コアの 1 actor がパニックしても他 actor・コアは生き続ける
/// (Arc<Mutex> 系なら Mutex poison で続行不能になる所)。
#[test]
fn panic_in_one_actor_does_not_kill_others_on_same_core() {
    struct Bomb;
    impl Actor for Bomb {
        type Message = bool;
        fn handle(&mut self, boom: bool) {
            if boom {
                panic!("boom"); // stderr にパニック表示が出るのは想定内
            }
        }
    }
    struct Survivor {
        out: mpsc::Sender<u32>,
    }
    impl Actor for Survivor {
        type Message = u32;
        fn handle(&mut self, m: u32) {
            self.out.send(m).unwrap();
        }
    }

    let sys = System::with_cores(1);
    let bomb = sys.spawn_on(0, Bomb);
    let (out, rx) = mpsc::channel();
    let survivor = sys.spawn_on(0, Survivor { out }); // 同一コア

    bomb.send_blocking(true).unwrap(); // bomb を爆発させる → 切り離される
    survivor.send_blocking(42).unwrap(); // 爆発後も survivor は動く
    assert_eq!(rx.recv().unwrap(), 42);

    drop(bomb);
    drop(survivor);
    sys.shutdown();
}

/// 消滅した actor への `send_blocking` は永久スピンせず `Err(Closed)` で返る(元メッセージ付き)。
/// = actor 障害を送信側スレッドに panic 伝播させず、値として明示する。
#[test]
fn send_blocking_to_dead_actor_returns_closed_not_hang() {
    struct Bomb;
    impl Actor for Bomb {
        type Message = ();
        fn handle(&mut self, _m: ()) {
            panic!("die"); // 非 supervised → 切り離される
        }
    }
    let sys = System::with_cores(1);
    let addr = sys.spawn_on(0, Bomb);
    addr.send_blocking(()).unwrap(); // これは成功(mailbox に入り、処理で爆発 → detach)
    // detach 完了(consumer 消滅)まで待つ。以後の try_send は Closed を返す。
    loop {
        match addr.try_send(()) {
            Err(TrySendError::Closed(_)) => break,
            _ => std::hint::spin_loop(),
        }
    }
    // send_blocking も Closed を返す(永久スピンしない = 退行なら以下でハング)。
    assert!(matches!(addr.send_blocking(()), Err(SendError::Closed(_))));
    drop(addr);
    sys.shutdown();
}

/// ライフサイクルフックも障害境界に含まれる: on_start がパニックしても、その actor が
/// 捨てられるだけでコアスレッドは死なず、同一コアの他 actor は動き続ける。
#[test]
fn panic_in_on_start_does_not_kill_core() {
    struct StartBomb;
    impl Actor for StartBomb {
        type Message = ();
        fn on_start(&mut self) {
            panic!("on_start boom"); // 想定内(stderr に出る)
        }
        fn handle(&mut self, _m: ()) {}
    }
    struct Survivor {
        out: mpsc::Sender<u32>,
    }
    impl Actor for Survivor {
        type Message = u32;
        fn handle(&mut self, m: u32) {
            self.out.send(m).unwrap();
        }
    }

    let sys = System::with_cores(1);
    let bomb = sys.spawn_on(0, StartBomb); // on_start パニック → 捨てられる(コアは生存)
    let (out, rx) = mpsc::channel();
    let survivor = sys.spawn_on(0, Survivor { out }); // 同一コア

    survivor.send_blocking(7).unwrap(); // コアが生きていれば処理される(死んでいれば recv がハング)
    assert_eq!(rx.recv().unwrap(), 7);

    drop(bomb);
    drop(survivor);
    sys.shutdown();
}

/// ビルダーで instrumented にすると processing-latency が取れ、非 instrumented は None(ゼロコスト既定)。
#[test]
fn instrumented_builder_records_latency() {
    struct Work;
    impl Actor for Work {
        type Message = u64;
        fn handle(&mut self, _m: u64) {}
    }
    let sys = System::with_cores(2);

    let addr = sys.build(|| Work).core(0).instrumented().spawn();
    assert!(addr.latency().is_some());
    const N: usize = 5_000;
    for i in 0..N {
        addr.send_blocking(i as u64).unwrap();
    }
    // total_processed()=mailbox の dequeue 数は handle/記録の *前* に増える近似値。
    // これで待つと最後の1通が「取り出し済み・未記録」のまま assert して flaky になる。
    // 記録は handle の後なので、histogram の count が N に達するまで待つのが正しい同期点。
    while addr.total_processed() < N {
        std::hint::spin_loop();
    }
    while addr.latency().unwrap().count < N as u64 {
        std::hint::spin_loop();
    }
    let snap = addr.latency().unwrap();
    assert_eq!(snap.count, N as u64);

    // 非 instrumented は latency 無し(既定ゼロコスト)。
    let plain = sys.spawn_on(1, Work);
    assert!(plain.latency().is_none());

    drop(addr);
    drop(plain);
    sys.shutdown();
}

/// observability カウンタ(送信総数 / 処理総数 / 滞留数)が正しく、hot path 追加コストなしで読める。
#[test]
fn observability_counters() {
    struct Nop;
    impl Actor for Nop {
        type Message = u64;
        fn handle(&mut self, _m: u64) {}
    }
    let sys = System::with_cores(1);
    let addr = sys.spawn_on(0, Nop);
    const N: usize = 10_000;
    for i in 0..N {
        addr.send_blocking(i as u64).unwrap();
    }
    assert_eq!(addr.total_sent(), N);
    while addr.total_processed() < N {
        std::hint::spin_loop();
    }
    assert_eq!(addr.total_processed(), N);
    assert_eq!(addr.mailbox_depth(), 0);
    drop(addr);
    sys.shutdown();
}

/// supervised actor はパニック後に工場で作り直され、mailbox を保ったまま継続する(restart)。
/// 新品なので状態はリセットされる = 隔離ゆえ壊れた状態が残らないことの証拠。
#[test]
fn supervised_actor_restarts_after_panic() {
    struct Counter {
        n: u64,
        out: mpsc::Sender<u64>,
    }
    impl Actor for Counter {
        type Message = bool; // true = 爆発
        fn handle(&mut self, boom: bool) {
            if boom {
                panic!("boom");
            }
            self.n += 1;
            self.out.send(self.n).unwrap();
        }
    }

    let sys = System::with_cores(1);
    let (out, rx) = mpsc::channel();
    let addr = sys.spawn_on_supervised(0, move || Counter { n: 0, out: out.clone() });

    addr.send_blocking(false).unwrap(); // n=1
    addr.send_blocking(false).unwrap(); // n=2
    assert_eq!(rx.recv().unwrap(), 1);
    assert_eq!(rx.recv().unwrap(), 2);

    addr.send_blocking(true).unwrap(); // パニック → restart(状態リセット)
    addr.send_blocking(false).unwrap(); // 新品: n=1 に戻る
    assert_eq!(rx.recv().unwrap(), 1); // 状態リセットの証拠

    drop(addr);
    sys.shutdown();
}

/// restart 上限: 毎回パニックする supervised actor は、上限(ここでは 2 回)を超えたら
/// 無限に再起動せず停止・切り離される(= restart-loop でコアを焼き続けない)。
/// 上限が効いていなければ actor は永久に restart し、下の Closed 待ちがハングして退行を検出する。
#[test]
fn supervised_restart_limit_stops_after_exceeding() {
    struct AlwaysPanic;
    impl Actor for AlwaysPanic {
        type Message = ();
        fn handle(&mut self, _m: ()) {
            panic!("always"); // 毎回爆発 → 毎回 restart 要求
        }
    }
    let sys = System::with_cores(1);
    let addr = sys
        .build(|| AlwaysPanic)
        .core(0)
        .restart_policy(RestartPolicy::Limited {
            max_restarts: 2,
            within: Duration::from_secs(10),
        })
        .spawn();
    for _ in 0..5 {
        let _ = addr.try_send(()); // 何度も爆発させる
    }
    // 上限超過で detach → 以後の送信は Closed(無制限なら永久 restart でここがハング)。
    loop {
        match addr.try_send(()) {
            Err(TrySendError::Closed(_)) => break,
            _ => std::hint::spin_loop(),
        }
    }
    drop(addr);
    sys.shutdown();
}

/// パニックする actor へ ask すると、Responder が返信前に drop されて呼び出し側は
/// デッドロックせず NoReply で起きる(ask とパニック分離の合成)。
#[test]
fn ask_to_panicking_actor_returns_no_reply() {
    struct Panicker;
    impl Actor for Panicker {
        type Message = Responder<u64>;
        fn handle(&mut self, _resp: Responder<u64>) {
            panic!("nope");
        }
    }
    let sys = System::with_cores(1);
    let addr = sys.spawn_on(0, Panicker);
    assert_eq!(addr.ask(|resp| resp), Err(AskError::NoReply));
    drop(addr);
    sys.shutdown();
}

/// 状態を持つ actor がメッセージを順序どおり処理し、`&mut self` をロック無しで更新する。
#[test]
fn stateful_actor_processes_in_order() {
    struct Counter {
        total: u64,
        out: mpsc::Sender<u64>,
    }
    impl Actor for Counter {
        type Message = u64;
        fn handle(&mut self, msg: u64) {
            self.total += msg;
            self.out.send(self.total).unwrap();
        }
    }

    let sys = System::with_cores(1);
    let (out, results) = mpsc::channel();
    let addr = sys.spawn_on(0, Counter { total: 0, out });

    for i in 1..=100u64 {
        addr.send_blocking(i).unwrap();
    }
    drop(addr);
    sys.shutdown();

    let mut expected = 0u64;
    for i in 1..=100u64 {
        expected += i;
        assert_eq!(results.recv().unwrap(), expected);
    }
    assert!(results.recv().is_err());
}

/// on_start / on_stop が起動時・停止時に 1 回ずつ呼ばれる。
#[test]
fn lifecycle_hooks_fire_once() {
    struct L {
        out: mpsc::Sender<&'static str>,
    }
    impl Actor for L {
        type Message = ();
        fn handle(&mut self, _msg: ()) {
            self.out.send("handle").unwrap();
        }
        fn on_start(&mut self) {
            self.out.send("start").unwrap();
        }
        fn on_stop(&mut self) {
            self.out.send("stop").unwrap();
        }
    }

    let sys = System::with_cores(1);
    let (out, ev) = mpsc::channel();
    let addr = sys.spawn_on(0, L { out });
    addr.send_blocking(()).unwrap();
    drop(addr);
    sys.shutdown();

    assert_eq!(ev.recv().unwrap(), "start");
    assert_eq!(ev.recv().unwrap(), "handle");
    assert_eq!(ev.recv().unwrap(), "stop");
}

/// 非 Copy 型を move で受け取る。
#[test]
fn moves_owned_non_copy_messages() {
    struct Sink {
        out: mpsc::Sender<String>,
    }
    impl Actor for Sink {
        type Message = String;
        fn handle(&mut self, msg: String) {
            self.out.send(msg).unwrap();
        }
    }

    let sys = System::with_cores(1);
    let (out, results) = mpsc::channel();
    let addr = sys.spawn_on(0, Sink { out });
    addr.send_blocking(String::from("owned")).unwrap();
    addr.send_blocking(String::from("moved")).unwrap();
    drop(addr);
    sys.shutdown();
    assert_eq!(results.recv().unwrap(), "owned");
    assert_eq!(results.recv().unwrap(), "moved");
}

/// 複数コアに actor を配置し、actor 間で **コアを跨いで** routing する。
/// source(コア0) → worker×3(round-robin 配置) → aggregator(コア0)。
#[test]
fn cross_core_routing() {
    let sys = System::with_cores(4);

    // aggregator: worker からの部分和を集めて合計、最後に外へ出す。
    struct Aggregator {
        remaining: u32,
        total: u64,
        out: mpsc::Sender<u64>,
    }
    impl Actor for Aggregator {
        type Message = u64;
        fn handle(&mut self, partial: u64) {
            self.total += partial;
            self.remaining -= 1;
            if self.remaining == 0 {
                self.out.send(self.total).unwrap();
            }
        }
    }

    // worker: 受けた数を 2 倍して aggregator に転送(handler 内 → try_send)。
    struct Worker {
        agg: aetherflow::ActorRef<Aggregator>,
    }
    impl Actor for Worker {
        type Message = u64;
        fn handle(&mut self, n: u64) {
            let _ = self.agg.try_send(n * 2);
        }
    }

    let (out, result) = mpsc::channel();
    // aggregator はコア0。worker が 12 個の値を送るので remaining=12。
    let agg = sys.spawn_on(0, Aggregator {
        remaining: 12,
        total: 0,
        out,
    });

    // worker を 3 個、別コアへ。
    let workers: Vec<_> = (0..3)
        .map(|k| sys.spawn_on(k + 1, Worker { agg: agg.clone() }))
        .collect();

    // 各 worker に 1..=4 を送る(worker が 2 倍 → aggregator へ)。合計 = 2*(1+2+3+4)*3 = 60。
    for w in &workers {
        for n in 1..=4u64 {
            w.send_blocking(n).unwrap();
        }
    }

    let total = result.recv().unwrap();
    assert_eq!(total, 60);

    drop(workers);
    drop(agg);
    sys.shutdown();
}

/// round-robin の `spawn` が全コアに散らばる(配置が偏らない)。
#[test]
fn round_robin_placement_runs() {
    struct Echo {
        out: mpsc::Sender<u32>,
    }
    impl Actor for Echo {
        type Message = u32;
        fn handle(&mut self, m: u32) {
            self.out.send(m).unwrap();
        }
    }

    let sys = System::with_cores(3);
    let (out, results) = mpsc::channel();
    let addrs: Vec<_> = (0..9).map(|_| sys.spawn(Echo { out: out.clone() })).collect();
    for (i, a) in addrs.iter().enumerate() {
        a.send_blocking(i as u32).unwrap();
    }
    drop(addrs);
    drop(out);
    sys.shutdown();

    let mut got: Vec<u32> = results.iter().collect();
    got.sort_unstable();
    assert_eq!(got, (0..9).collect::<Vec<_>>());
}

// ---- SchedulingPolicy(バッチドレイン)----

/// `batch_drain(1)` = 1 訪問 1 通(バッチ無効)でも全メッセージが届く。
/// バッチ長がメッセージ配送の正しさに影響しないことの確認。
#[test]
fn batch_drain_one_still_delivers_all() {
    struct Counter {
        out: mpsc::Sender<u32>,
    }
    impl Actor for Counter {
        type Message = u32;
        fn handle(&mut self, m: u32) {
            self.out.send(m).unwrap();
        }
    }

    let sys = System::with_policy(1, SchedulingPolicy::default().batch_drain(1));
    let (out, rx) = mpsc::channel();
    let addr = sys.spawn_on(0, Counter { out });
    for i in 0..300u32 {
        addr.send_blocking(i).unwrap();
    }
    let got: Vec<u32> = (0..300).map(|_| rx.recv().unwrap()).collect();
    assert_eq!(got, (0..300).collect::<Vec<u32>>(), "順序込みで全件届くこと");
}

/// バッチ長がメッセージ数を超えても(300 通 < batch 1024)過剰ポーリングで壊れない。
/// バッチは「空になったら抜ける」ので、上限に届かなくても正常終了する。
#[test]
fn batch_drain_larger_than_backlog_is_fine() {
    struct Echo {
        out: mpsc::Sender<u32>,
    }
    impl Actor for Echo {
        type Message = u32;
        fn handle(&mut self, m: u32) {
            self.out.send(m).unwrap();
        }
    }

    let sys = System::with_policy(1, SchedulingPolicy::default().batch_drain(1024));
    let (out, rx) = mpsc::channel();
    let addr = sys.spawn_on(0, Echo { out });
    for i in 0..300u32 {
        addr.send_blocking(i).unwrap();
    }
    for i in 0..300u32 {
        assert_eq!(rx.recv().unwrap(), i);
    }
}

/// **バッチの途中でパニックした actor だけを切り離し、同一コアの別 actor は生き続ける。**
/// バッチドレインはパニック時に専用の分岐(バッチを抜けて切り離し)を通るので、
/// 「バッチ内パニック」でも分離が壊れないことを退行検出する。
#[test]
fn panic_inside_batch_detaches_only_that_actor() {
    struct Bomb {
        seen: u32,
    }
    impl Actor for Bomb {
        type Message = ();
        fn handle(&mut self, _m: ()) {
            self.seen += 1;
            // バッチ(既定 128)の途中で爆発する = 1 通目ではない点が肝。
            if self.seen == 5 {
                panic!("boom mid-batch");
            }
        }
    }
    struct Survivor {
        out: mpsc::Sender<u32>,
    }
    impl Actor for Survivor {
        type Message = u32;
        fn handle(&mut self, m: u32) {
            self.out.send(m).unwrap();
        }
    }

    // 同一コアに同居させる(バッチ内パニックが隣を巻き込まないことを見る)。
    let sys = System::with_policy(1, SchedulingPolicy::default().batch_drain(128));
    let (out, rx) = mpsc::channel();
    let bomb = sys.spawn_on(0, Bomb { seen: 0 });
    let survivor = sys.spawn_on(0, Survivor { out });

    // 1 バッチに収まる量を積んでから爆発させる。
    for _ in 0..20 {
        let _ = bomb.try_send(());
    }
    // bomb が切り離される(consumer 消滅 → Closed)まで待つ。
    let mut detached = false;
    for _ in 0..10_000 {
        if let Err(TrySendError::Closed(_)) = bomb.try_send(()) {
            detached = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(detached, "バッチ内でパニックした actor は切り離されること");

    // 同一コアの隣人は無事で、以後も普通に処理を続ける。
    for i in 0..50u32 {
        survivor.send_blocking(i).unwrap();
    }
    for i in 0..50u32 {
        assert_eq!(rx.recv().unwrap(), i, "隣の actor は巻き込まれないこと");
    }
}

/// `batch_drain(0)` は進捗しなくなるので、設定時点で弾く(沈黙のハングにしない)。
#[test]
#[should_panic(expected = "batch_drain must be >= 1")]
fn batch_drain_zero_is_rejected() {
    let _ = SchedulingPolicy::default().batch_drain(0);
}
