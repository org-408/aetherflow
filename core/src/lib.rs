//! # aetherflow-runtime — マルチコア thread-per-core actor ランタイム
//!
//! design.md の 4 本柱を実装:
//! - ①型による隔離 / ②zero-copy move → [`ActorRef::try_send`] が `A::Message` を **move** で取る。
//!   use-after-send はコンパイルエラー(E0382)。
//! - ③物理配置 → [`System`] が **コアごとに 1 スレッド**を立ててピン留めし、各スレッドが
//!   そのコアに割り当てられた多数の actor を run-to-completion で回す。mailbox はコアを跨がない
//!   lock-free キュー(`mpsc`)。
//! - ④work-stealing 排除 → Tokio 不使用。actor はコア間を移動しない(静的配置)。
//!
//! ## thread-per-core であって thread-per-actor ではない
//! 1 コア = 1 OS スレッドが**多数の actor** を回す。actor ごとにスレッドを立てない(それは
//! thread-per-actor でスケールしない)。同一コアの異種 actor は型消去して 1 スレッドで回し、
//! 送信は typed な [`ActorRef`] のまま — capability-mapping §7 の「制御面=erased / データ面=typed」。
//!
//! ## 使い方
//! ```
//! use aetherflow::{System, Actor};
//! use std::sync::mpsc;
//!
//! struct Adder { sum: u64, out: mpsc::Sender<u64> }
//! impl Actor for Adder {
//!     type Message = u64;
//!     fn handle(&mut self, msg: u64) { self.sum += msg; self.out.send(self.sum).unwrap(); }
//! }
//!
//! let sys = System::with_cores(2);
//! let (out, results) = mpsc::channel();
//! let addr = sys.spawn_on(0, Adder { sum: 0, out });   // コア 0 に配置
//! addr.send_blocking(10);
//! addr.send_blocking(5);
//! drop(addr);              // 送信端が全て落ちると、その actor はコアから外れる
//! sys.shutdown();          // 全コアを停止して join
//! assert_eq!(results.recv().unwrap(), 10);
//! assert_eq!(results.recv().unwrap(), 15);
//! ```
//!
//! ## 隔離はコンパイル時に強制される
//! ```compile_fail
//! use aetherflow::{System, Actor};
//! struct Order { qty: u32 }
//! struct E;
//! impl Actor for E { type Message = Order; fn handle(&mut self, m: Order) { let _ = m.qty; } }
//! let sys = System::with_cores(1);
//! let addr = sys.spawn_on(0, E);
//! let order = Order { qty: 100 };
//! addr.try_send(order).ok();     // move で移譲
//! println!("{}", order.qty);     // use of moved value `order` (E0382)：コンパイル不能
//! ```

// SPSC は単一生産者の高速パス / 将来のコア間ペアキュー(Seastar 流)用に温存。
// 現在の公開 API(System)は MPSC mailbox を使うため、これは予約の tested primitive。
#[allow(dead_code)]
mod spsc;

mod ask;
mod metrics;
mod mpsc;
mod system;

pub mod pinning;

pub use ask::{AskError, Responder};
pub use metrics::LatencySnapshot;
pub use system::{ActorRef, IdleStrategy, RestartPolicy, SpawnBuilder, System};

/// 非ブロッキング送信 `try_send` の失敗理由。いずれも**元のメッセージを返す**ので、
/// 呼び出し側は再ルーティング・永続化・ログ記録ができる。
#[derive(Debug)]
pub enum TrySendError<T> {
    /// mailbox が満杯。**一時的なバックプレッシャ**(あとで空けば送れる)。
    Full(T),
    /// 受信 actor が消滅済み(panic 停止 / restart 上限 / shutdown 等)。**恒久的に送信不能**。
    Closed(T),
}

/// ブロッキング送信 `send_blocking` の失敗理由。満杯は待つので、残るのは Closed のみ。
#[derive(Debug)]
pub enum SendError<T> {
    /// 受信 actor が消滅済み(恒久的に送信不能)。元のメッセージを返す。
    Closed(T),
}

/// 型付き actor。メッセージ型は固定で `Send`(= sendable な `iso`/`val` を型で要求)。
pub trait Actor: Send + 'static {
    /// この actor が受け取るメッセージの型。
    type Message: Send + 'static;

    /// メッセージ 1 通を処理する。`&mut self`(= `ref`)なので自状態の唯一の所有者。
    /// 単一コアスレッドが run-to-completion で回すのでロックは要らない。
    fn handle(&mut self, msg: Self::Message);

    /// コアに配置され、最初のメッセージ処理前に 1 回呼ばれる。
    fn on_start(&mut self) {}
    /// supervised actor がパニック後に新品へ差し替えられた直後に呼ばれる(restart)。
    /// 既定では [`Actor::on_start`] と同じ扱い。
    fn on_restart(&mut self) {
        self.on_start()
    }
    /// 送信端が全て落ち mailbox を drain し切った後(または system 停止時)に 1 回呼ばれる。
    fn on_stop(&mut self) {}
}
