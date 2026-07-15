//! マルチコア thread-per-core スケジューラ。
//!
//! [`System`] は要求された数だけコアスレッドを立て、各スレッドをコアにピン留めして、
//! 割り当てられた actor 群を run-to-completion で回す。actor はコア間を移動しない(静的配置)。
//!
//! 同一コアに異種の actor(`A::Message` が違う)を同居させるため、格納は型消去
//! ([`ErasedActor`])。送信は typed な [`ActorRef`] のまま。

use crate::{mpsc, pinning, Actor, SendError, TrySendError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as ctrl; // 制御面(main → コア): 低頻度なので std チャネルで十分
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

thread_local! {
    /// このスレッドがコアスレッドなら、その論理コア番号。非コアスレッド(main 等)なら None。
    static CURRENT_CORE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// 現在のスレッドが回しているコア番号(非コアスレッドなら None)。
pub(crate) fn current_core() -> Option<usize> {
    CURRENT_CORE.with(|c| c.get())
}

/// メッセージが無いときのコアスレッドの振る舞い(レイテンシ ↔ CPU/tail 堅牢性のトレードオフ)。
#[derive(Clone, Copy, Debug, Default)]
pub enum IdleStrategy {
    /// 常にビジースピン。**最低レイテンシ・最大 CPU**。専有ベアメタルの隔離コア向け。
    /// 仮想化/オーバーサブスクリプション環境では、スピン中の preempt で tail が暴発しうる。
    /// レイテンシ優先が既定。
    #[default]
    BusySpin,
    /// `spins` 回スピン → `yields` 回 `yield_now` → 以降 `park` だけスリープ、と段階的に譲る。
    /// median を少し犠牲にする代わりに、CPU を抑え、仮想化下の tail 暴発を和らげる。共有環境向け。
    Backoff {
        spins: u32,
        yields: u32,
        park: Duration,
    },
}

impl IdleStrategy {
    /// 共有/仮想化環境向けの無難なバックオフ。
    pub fn backoff() -> Self {
        IdleStrategy::Backoff {
            spins: 128,
            yields: 128,
            park: Duration::from_micros(50),
        }
    }

    #[inline]
    fn idle(&self, count: u32) {
        match *self {
            IdleStrategy::BusySpin => std::hint::spin_loop(),
            IdleStrategy::Backoff {
                spins,
                yields,
                park,
            } => {
                if count < spins {
                    std::hint::spin_loop();
                } else if count < spins + yields {
                    std::thread::yield_now();
                } else {
                    std::thread::sleep(park);
                }
            }
        }
    }
}


/// 1 巡のポーリング結果。
enum PollOutcome {
    /// メッセージを 1 つ処理した。
    Worked,
    /// mailbox が空だった。
    Empty,
    /// handle がパニックした(この actor は切り離す)。
    Panicked,
}

/// ライフサイクルフック(on_start / on_stop / restart)の結果。
/// フックのパニックも actor 境界に閉じ込め、コアスレッドを巻き込まないための戻り値。
enum LifecycleOutcome {
    Ok,
    Panicked,
}

/// 型消去した actor セル。コアスレッドはこの trait 越しに actor を回す。
trait ErasedActor: Send {
    fn start(&mut self) -> LifecycleOutcome;
    fn poll_one(&mut self) -> PollOutcome;
    /// 送信端が全て落ち、かつ mailbox が空(= もう仕事は来ない)。
    fn closed_and_empty(&mut self) -> bool;
    fn stop(&mut self) -> LifecycleOutcome;
}

/// supervised actor の再起動方針。無制限 restart は暴走ループになりうるので、既定は上限つき。
#[derive(Clone, Copy, Debug)]
pub enum RestartPolicy {
    /// 再起動しない(パニックしたら停止・切り離し)。
    Never,
    /// `within` の窓の中で `max_restarts` 回まで再起動。超えたら停止。
    Limited { max_restarts: u32, within: Duration },
    /// 無制限に再起動(暴走 restart-loop に注意)。
    Always,
}

impl RestartPolicy {
    /// `.supervised()` の既定。BusySpin コアで暴れないよう控えめ(5 回 / 10 秒)。
    pub fn default_supervised() -> Self {
        RestartPolicy::Limited {
            max_restarts: 5,
            within: Duration::from_secs(10),
        }
    }
}

/// 再起動回数トラッカ(固定窓 + 経過でリセット)。長時間正常後の 1 発では止まらない。
#[derive(Default)]
struct RestartState {
    window_start: Option<Instant>,
    count: u32,
}

struct Cell<A: Actor> {
    actor: A,
    rx: mpsc::Receiver<A::Message>,
    /// `Some` なら supervised: パニック時に工場で作り直して継続(restart)。`None` なら stop。
    factory: Option<Box<dyn Fn() -> A + Send>>,
    /// 再起動方針(supervised のときのみ意味を持つ)。
    restart_policy: RestartPolicy,
    /// 再起動回数の追跡(方針の上限判定用)。
    restarts: RestartState,
    /// `Some` なら instrumented: handle 所要時間を記録(opt-in、既定 None = ゼロコスト)。
    metrics: Option<std::sync::Arc<crate::metrics::LatencyHistogram>>,
}

impl<A: Actor> Cell<A> {
    /// このパニック(= 再起動要求)を記録し、方針に照らして再起動を許可するか返す。
    /// 失敗した再生成(factory / on_restart のパニック)も 1 回としてカウントする。
    fn allow_restart(&mut self) -> bool {
        match self.restart_policy {
            RestartPolicy::Never => false,
            RestartPolicy::Always => true,
            RestartPolicy::Limited { max_restarts, within } => {
                let now = Instant::now();
                match self.restarts.window_start {
                    Some(start) if now.duration_since(start) <= within => {
                        self.restarts.count += 1;
                    }
                    // 窓外(または初回)→ リセット。長時間正常後の 1 発で誤停止しない。
                    _ => {
                        self.restarts.window_start = Some(now);
                        self.restarts.count = 1;
                    }
                }
                self.restarts.count <= max_restarts
            }
        }
    }
}

impl<A: Actor> ErasedActor for Cell<A> {
    fn start(&mut self) -> LifecycleOutcome {
        // on_start のパニックも捕捉して境界化(でないとコアスレッド全体が死ぬ)。
        let actor = &mut self.actor;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| actor.on_start())) {
            Ok(()) => LifecycleOutcome::Ok,
            Err(_) => LifecycleOutcome::Panicked,
        }
    }
    fn poll_one(&mut self) -> PollOutcome {
        match self.rx.try_recv() {
            Some(msg) => {
                // instrumented のときだけ計測(Instant のホットコストは opt-in)。
                let started = self.metrics.as_ref().map(|_| std::time::Instant::now());
                // handle をパニック捕捉で包む。actor 状態は単一所有(型が隔離を保証)なので、
                // パニックしても他 actor の状態を壊しようがない。
                let actor = &mut self.actor;
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| actor.handle(msg)));
                if let (Some(m), Some(t0)) = (&self.metrics, started) {
                    m.record(t0.elapsed().as_nanos() as u64);
                }
                match result {
                    Ok(()) => PollOutcome::Worked,
                    // supervised かつ 再起動方針が許すなら restart、そうでなければ切り離す。
                    // 再起動が健全なのも隔離のおかげ(壊れた状態が誰にも共有されず残らない)。
                    // 原因メッセージは消費済み = poison ループにならない。工場/on_restart もパニック境界に含める。
                    Err(_) => {
                        if self.factory.is_some() && self.allow_restart() {
                            let make = self.factory.as_ref().unwrap();
                            let rebuilt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                                || {
                                    let mut a = make();
                                    a.on_restart();
                                    a
                                },
                            ));
                            match rebuilt {
                                Ok(a) => {
                                    self.actor = a;
                                    PollOutcome::Worked
                                }
                                // 工場 or on_restart がパニック → 諦めて切り離す(既に count 済み)。
                                Err(_) => PollOutcome::Panicked,
                            }
                        } else {
                            // 非 supervised、または再起動上限に到達 → 停止・切り離し。
                            PollOutcome::Panicked
                        }
                    }
                }
            }
            None => PollOutcome::Empty,
        }
    }
    fn closed_and_empty(&mut self) -> bool {
        !self.rx.producers_alive() && self.rx.is_empty()
    }
    fn stop(&mut self) -> LifecycleOutcome {
        // on_stop のパニックも境界化。
        let actor = &mut self.actor;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| actor.on_stop())) {
            Ok(()) => LifecycleOutcome::Ok,
            Err(_) => LifecycleOutcome::Panicked,
        }
    }
}

enum Control {
    Add(Box<dyn ErasedActor>),
    Shutdown,
}

struct CoreHandle {
    control: ctrl::Sender<Control>,
    join: Option<thread::JoinHandle<()>>,
}

/// マルチコア actor ランタイム。
pub struct System {
    cores: Vec<CoreHandle>,
    next: AtomicUsize, // round-robin 配置カーソル
}

impl System {
    /// `n` 本のコアスレッドを立てる(ビジースピン既定。最低レイテンシ)。
    pub fn with_cores(n: usize) -> System {
        System::with_cores_idle(n, IdleStrategy::default())
    }

    /// idle 戦略を指定してコアスレッドを立てる(各スレッドを論理コア 0..n に best-effort でピン留め)。
    pub fn with_cores_idle(n: usize, idle: IdleStrategy) -> System {
        assert!(n >= 1, "System needs at least 1 core");
        let mut cores = Vec::with_capacity(n);
        for core_index in 0..n {
            let (control, rx) = ctrl::channel::<Control>();
            let join = thread::Builder::new()
                .name(format!("aether-core-{core_index}"))
                .spawn(move || core_loop(core_index, rx, idle))
                .expect("spawn core thread");
            cores.push(CoreHandle {
                control,
                join: Some(join),
            });
        }
        System {
            cores,
            next: AtomicUsize::new(0),
        }
    }

    /// コア数。
    pub fn num_cores(&self) -> usize {
        self.cores.len()
    }

    /// 指定コアに actor を配置し、typed な送信ハンドルを返す(mailbox 容量はデフォルト)。
    pub fn spawn_on<A: Actor>(&self, core: usize, actor: A) -> ActorRef<A> {
        self.spawn_on_with(core, actor, DEFAULT_MAILBOX_CAPACITY)
    }

    /// 指定コアに actor を配置(mailbox 容量を指定)。
    pub fn spawn_on_with<A: Actor>(
        &self,
        core: usize,
        actor: A,
        mailbox_capacity: usize,
    ) -> ActorRef<A> {
        self.install(core, mailbox_capacity, actor, None, RestartPolicy::Never, None)
    }

    /// round-robin でコアを選んで actor を配置する。
    pub fn spawn<A: Actor>(&self, actor: A) -> ActorRef<A> {
        let core = self.next.fetch_add(1, Ordering::Relaxed) % self.cores.len();
        self.spawn_on(core, actor)
    }

    /// 指定コアに **supervised** actor を配置する。`make` は初期インスタンスと、パニック時の
    /// **再生成**の両方に使う工場。パニックしても mailbox は保たれ、新品の actor が残りの
    /// メッセージを処理し続ける(restart)。再起動が健全なのは、状態が単一所有(型が隔離を保証)で
    /// 壊れた状態が誰にも共有されず残らないから。原因メッセージは消費済みなので poison ループにならない。
    pub fn spawn_on_supervised<A: Actor>(
        &self,
        core: usize,
        make: impl Fn() -> A + Send + 'static,
    ) -> ActorRef<A> {
        self.spawn_on_supervised_with(core, make, DEFAULT_MAILBOX_CAPACITY)
    }

    /// supervised 配置(mailbox 容量を指定)。
    pub fn spawn_on_supervised_with<A: Actor>(
        &self,
        core: usize,
        make: impl Fn() -> A + Send + 'static,
        mailbox_capacity: usize,
    ) -> ActorRef<A> {
        let actor = make();
        self.install(
            core,
            mailbox_capacity,
            actor,
            Some(Box::new(make)),
            RestartPolicy::default_supervised(),
            None,
        )
    }

    /// 上級ノブ(コア指定 / mailbox 容量 / supervised / instrumented)をまとめて指定するビルダー。
    /// 単純な場合は `spawn_on` 等でよい ── 進んだ調整だけ opt-in する(progressive disclosure)。
    ///
    /// 例: `sys.build(|| MyActor::new()).core(0).mailbox(4096).supervised().instrumented().spawn()`
    pub fn build<A: Actor, F: Fn() -> A + Send + 'static>(&self, make: F) -> SpawnBuilder<'_, A, F> {
        SpawnBuilder {
            sys: self,
            make,
            core: None,
            mailbox: DEFAULT_MAILBOX_CAPACITY,
            supervised: false,
            restart_policy: None,
            instrumented: false,
        }
    }

    /// actor をコアへ設置する内部共通処理。
    fn install<A: Actor>(
        &self,
        core: usize,
        mailbox: usize,
        actor: A,
        factory: Option<Box<dyn Fn() -> A + Send>>,
        restart_policy: RestartPolicy,
        metrics: Option<std::sync::Arc<crate::metrics::LatencyHistogram>>,
    ) -> ActorRef<A> {
        assert!(core < self.cores.len(), "core index {core} out of range");
        let (tx, rx) = mpsc::channel::<A::Message>(mailbox);
        let cell: Box<dyn ErasedActor> = Box::new(Cell {
            actor,
            rx,
            factory,
            restart_policy,
            restarts: RestartState::default(),
            metrics: metrics.clone(),
        });
        self.cores[core]
            .control
            .send(Control::Add(cell))
            .unwrap_or_else(|_| panic!("core {core} thread is not running"));
        ActorRef { tx, metrics, core }
    }

    /// 全コアを停止して join する(system を drop するのと同じ。明示用)。
    pub fn shutdown(self) {
        // drop(self) が下の Drop を走らせる。
    }
}

impl Drop for System {
    fn drop(&mut self) {
        for c in &self.cores {
            let _ = c.control.send(Control::Shutdown);
        }
        for c in &mut self.cores {
            if let Some(j) = c.join.take() {
                let _ = j.join();
            }
        }
    }
}

/// 1 actor 訪問あたりに連続処理する最大メッセージ数(バッチドレイン)。
/// 外側スケジューラループ(制御チャネル確認等)のオーバヘッドを償却して throughput を上げる。
/// 大きすぎると同一コアの他 actor の公平性が落ちるので上限を設ける。
const BATCH_DRAIN: usize = 128;

/// コアスレッドの本体。制御メッセージと actor 群を交互に捌く run-to-completion ループ。
fn core_loop(core_index: usize, control: ctrl::Receiver<Control>, idle: IdleStrategy) {
    pinning::pin_current_thread_to(core_index); // best-effort(macOS は no-op)
    CURRENT_CORE.with(|c| c.set(Some(core_index))); // このスレッドの担当コアを記録(デッドロックガード用)

    let mut actors: Vec<Box<dyn ErasedActor>> = Vec::new();
    let mut shutting_down = false;
    let mut idle_count: u32 = 0;

    loop {
        // 1) 制御面を捌く(actor 追加 / 停止指示)
        let mut ctrl_activity = false;
        loop {
            match control.try_recv() {
                Ok(Control::Add(mut cell)) => {
                    // on_start がパニックしたら、この actor だけ捨てる(コアは巻き込まない)。
                    match cell.start() {
                        LifecycleOutcome::Ok => actors.push(cell),
                        LifecycleOutcome::Panicked => drop(cell),
                    }
                    ctrl_activity = true;
                }
                Ok(Control::Shutdown) => shutting_down = true,
                Err(ctrl::TryRecvError::Empty) => break,
                Err(ctrl::TryRecvError::Disconnected) => {
                    // System が drop 済み(Shutdown も届かない安全網)
                    shutting_down = true;
                    break;
                }
            }
        }

        // 2) actor 群を 1 巡スケジュール
        let mut did_work = false;
        let mut i = 0;
        while i < actors.len() {
            match actors[i].poll_one() {
                PollOutcome::Worked => {
                    did_work = true;
                    // バッチドレイン: 同一 actor を最大 BATCH_DRAIN 通まで続けて処理し、
                    // 外側ループ(制御チャネル try_recv 等)のオーバヘッドを 1/BATCH に償却する。
                    // パニック分離は維持(バッチ内でパニックしたら通常どおり切り離す)。
                    let mut panicked = false;
                    for _ in 1..BATCH_DRAIN {
                        match actors[i].poll_one() {
                            PollOutcome::Worked => {}
                            PollOutcome::Empty => break,
                            PollOutcome::Panicked => {
                                panicked = true;
                                break;
                            }
                        }
                    }
                    if panicked {
                        drop(actors.swap_remove(i)); // i 据え置き(末尾が詰まる)
                    } else {
                        i += 1;
                    }
                }
                PollOutcome::Panicked => {
                    // パニックした actor を切り離す。壊れた状態には on_stop を呼ばず drop するだけ。
                    // 型隔離のおかげで他 actor / コアには波及しない = 安全に続行できる。
                    drop(actors.swap_remove(i));
                    did_work = true; // 進捗扱い(据え置き i で詰めた末尾を次に見る)
                }
                PollOutcome::Empty => {
                    if actors[i].closed_and_empty() {
                        // 送信端が全て落ち、空 → 停止して外す(on_stop はパニック境界化済み)
                        let mut cell = actors.swap_remove(i);
                        let _ = cell.stop();
                        // swap_remove は末尾を i に詰めるので i は据え置き
                    } else {
                        i += 1;
                    }
                }
            }
        }

        // 3) 停止指示が来ていたら、残りを drain して停止し、ループを抜ける
        if shutting_down {
            for mut cell in actors.drain(..) {
                // drain 中にパニックした actor には on_stop を呼ばない(通常経路と整合)。
                let mut panicked = false;
                loop {
                    match cell.poll_one() {
                        PollOutcome::Worked => {}
                        PollOutcome::Empty => break,
                        PollOutcome::Panicked => {
                            panicked = true;
                            break;
                        }
                    }
                }
                if !panicked {
                    let _ = cell.stop();
                }
            }
            break;
        }

        // 4) idle 戦略に従って譲る(仕事があればカウンタをリセット)
        if did_work || ctrl_activity {
            idle_count = 0;
        } else {
            idle.idle(idle_count);
            idle_count = idle_count.saturating_add(1);
        }
    }
}

/// actor のメールボックスへの送信ハンドル(MPSC の生産者)。`Clone` 可能 = 複数送信元 OK。
///
/// `try_send` が `A::Message` を **値で** 取るので、送信後の use-after-send はコンパイルエラー。
pub struct ActorRef<A: Actor> {
    tx: mpsc::Sender<A::Message>,
    /// instrumented spawn のときだけ Some。処理遅延ヒストグラムを共有。
    metrics: Option<std::sync::Arc<crate::metrics::LatencyHistogram>>,
    /// この actor が居るコア(デッドロックガード用)。
    core: usize,
}

impl<A: Actor> ActorRef<A> {
    /// このハンドルへのブロッキング呼び出しが、呼び出し元コアスレッドを自己ブロックするか
    /// (= handler 内から同一コアの actor へブロッキングして deadlock する状況)。
    pub(crate) fn would_deadlock_calling_core(&self) -> bool {
        current_core() == Some(self.core)
    }
}

impl<A: Actor> ActorRef<A> {
    /// メッセージを move で送る。満杯なら `Err(TrySendError::Full)`(一時的バックプレッシャ)、
    /// 受信 actor 消滅なら `Err(TrySendError::Closed)`(恒久的に送信不能)。いずれも元メッセージを返す。
    pub fn try_send(&self, msg: A::Message) -> Result<(), TrySendError<A::Message>> {
        self.tx.try_send(msg)
    }

    /// 満杯の間スピンして必ず送る。
    ///
    /// **注意**: handler の中から**同一コアの別 actor**へ `send_blocking` するとデッドロックし得る
    /// (送信側がこのコアスレッドを占有したまま、受信側 actor が drain できない)。
    /// runtime の外(main スレッド等)からの投入に使うこと。handler 内では `try_send` を使い、
    /// 満杯はバックプレッシャとして扱う。
    pub fn send_blocking(&self, msg: A::Message) -> Result<(), SendError<A::Message>> {
        // デッドロックガード: handler 内から同一コアの actor へブロッキングすると、この
        // コアスレッドが自分自身をブロックして永久ハングする。これは呼び出し側の確定的な設計ミス
        // なので、沈黙のハングより明確な panic に(actor 消滅の Closed とは別扱い)。
        assert!(
            !self.would_deadlock_calling_core(),
            "send_blocking from within a handler to a same-core actor would deadlock; use try_send"
        );
        let mut item = msg;
        loop {
            match self.tx.try_send(item) {
                Ok(()) => return Ok(()),
                // 満杯 = 一時的バックプレッシャ。空くまで待つ。
                Err(TrySendError::Full(returned)) => {
                    item = returned;
                    std::hint::spin_loop();
                }
                // 受信 actor が消滅 = 恒久的に送れない。永久スピンせず、元メッセージを返して呼び出し側に委ねる
                // (再ルーティング/永続化/ログ)。actor 障害を送信側スレッドに panic 伝播させない。
                Err(TrySendError::Closed(returned)) => return Err(SendError::Closed(returned)),
            }
        }
    }

    /// 現在の mailbox 滞留数(近似)。バックプレッシャ監視に。
    ///
    /// **hot path 追加コストゼロ**: 単一消費者の lock-free mailbox が既に持つ enqueue/dequeue
    /// カウンタを読むだけ。共有/work-stealing 系が競合 atomic を要するのと対照的(モデルの帰結)。
    pub fn mailbox_depth(&self) -> usize {
        self.tx.depth()
    }
    /// この actor に送られた総数。
    pub fn total_sent(&self) -> usize {
        self.tx.total_enqueued()
    }
    /// この actor が mailbox から取り出して処理した総数(近似)。
    pub fn total_processed(&self) -> usize {
        self.tx.total_dequeued()
    }

    /// 処理遅延のスナップショット。`instrumented()` で spawn したときだけ `Some`。
    pub fn latency(&self) -> Option<crate::metrics::LatencySnapshot> {
        self.metrics.as_ref().map(|m| m.snapshot())
    }
}

impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        ActorRef {
            tx: self.tx.clone(),
            metrics: self.metrics.clone(),
            core: self.core,
        }
    }
}

/// [`System::build`] が返す spawn ビルダー。上級ノブを opt-in で積む(既定は単純 spawn 相当)。
pub struct SpawnBuilder<'s, A: Actor, F: Fn() -> A + Send + 'static> {
    sys: &'s System,
    make: F,
    core: Option<usize>,
    mailbox: usize,
    supervised: bool,
    restart_policy: Option<RestartPolicy>,
    instrumented: bool,
}

impl<'s, A: Actor, F: Fn() -> A + Send + 'static> SpawnBuilder<'s, A, F> {
    /// 配置コアを固定する(未指定なら round-robin)。
    pub fn core(mut self, core: usize) -> Self {
        self.core = Some(core);
        self
    }
    /// mailbox 容量を指定する。
    pub fn mailbox(mut self, capacity: usize) -> Self {
        self.mailbox = capacity;
        self
    }
    /// supervised にする(パニック時に工場で作り直して継続 = restart)。
    /// 再起動方針は既定で `RestartPolicy::default_supervised()`(5 回 / 10 秒で停止)。
    pub fn supervised(mut self) -> Self {
        self.supervised = true;
        self
    }
    /// 再起動方針を明示する(`supervised()` の既定を上書き。呼ぶと supervised 扱いになる)。
    pub fn restart_policy(mut self, policy: RestartPolicy) -> Self {
        self.supervised = true;
        self.restart_policy = Some(policy);
        self
    }
    /// 処理遅延ヒストグラムを有効化する(`ActorRef::latency()` で読める。Instant のホットコスト有り)。
    pub fn instrumented(mut self) -> Self {
        self.instrumented = true;
        self
    }
    /// 設置して送信ハンドルを返す。
    pub fn spawn(self) -> ActorRef<A> {
        let core = self
            .core
            .unwrap_or_else(|| self.sys.next.fetch_add(1, Ordering::Relaxed) % self.sys.cores.len());
        let metrics = if self.instrumented {
            Some(std::sync::Arc::new(crate::metrics::LatencyHistogram::new()))
        } else {
            None
        };
        let actor = (self.make)();
        let (factory, restart_policy): (Option<Box<dyn Fn() -> A + Send>>, RestartPolicy) =
            if self.supervised {
                (
                    Some(Box::new(self.make)),
                    self.restart_policy
                        .unwrap_or_else(RestartPolicy::default_supervised),
                )
            } else {
                (None, RestartPolicy::Never)
            };
        self.sys
            .install(core, self.mailbox, actor, factory, restart_policy, metrics)
    }
}
