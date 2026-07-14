//! # Stage C — capability proof
//!
//! design.md §4 が「最優先で潰すべきリスク」とした問いの実証:
//! **Rust の型システムは actor の隔離(`iso`)をコンパイル時に強制できるか。**
//!
//! ここでは 2 つの API を並べ、`cargo test`(= doctest のコンパイル判定)で対比する:
//!
//! - [`ActorRef::send`] … メッセージを **move** で渡す typed API。送信後に元の値へ触れると
//!   **コンパイルが通らない**(= 隔離を型が保証する。狙いの形)。
//! - [`akka_style`] … `Arc<Mutex<_>>` で **共有** する Akka 流 / 現状 AetherFlow。送信後も
//!   触れてしまうコードが **素通りでコンパイルできる**(= 隔離が規約でしかない)。
//!
//! 詳しい理論は `docs/pony-rust-capability-mapping.md`、噛み砕きは `docs/concepts-explained.md`。
//!
//! ---
//!
//! ## 証明 1: 正しい版 — 送信後に触るとコンパイルエラー(`iso` を型が強制)
//!
//! メッセージ `order` は `send` に **move** される。送った後の `order` は「唯一の原本を手渡し
//! した後の空の手」であり、触ろうとするとコンパイラが弾く:
//!
//! ```compile_fail
//! use capability_proof::{Actor, spawn};
//!
//! struct Order { qty: u32 }
//! struct Trader;
//! impl Actor for Trader {
//!     type Message = Order;
//!     fn handle(&mut self, msg: Order) { let _ = msg.qty; }
//! }
//!
//! let trader = spawn(Trader);
//! let order = Order { qty: 100 };
//! trader.send(order);            // move で移譲
//! println!("{}", order.qty);     // ← use of moved value `order` (E0382)：コンパイル不能
//! ```
//!
//! (この doctest は「コンパイルが**失敗する**こと」を検証する。もし将来 `send` が値をコピー
//! したり `&M` を取る設計に退化すると、このブロックがコンパイルできてしまい、テストが赤くなる。
//!
//! 注意: 検証には `order` を **実際に使う**必要がある。`let _ = order.qty;` の `let _ =` は
//! 「使用」とみなされず move を発火しないため、隔離違反を検出できない。`println!` 等で使うこと。)
//!
//! ## 証明 2: 正しい版でも「送る前」なら当然使える(過剰制約ではない)
//!
//! ```
//! use capability_proof::{Actor, spawn};
//!
//! struct Order { qty: u32 }
//! struct Trader;
//! impl Actor for Trader {
//!     type Message = Order;
//!     fn handle(&mut self, msg: Order) { let _ = msg.qty; }
//! }
//!
//! let trader = spawn(Trader);
//! let mut order = Order { qty: 100 };
//! order.qty = 200;           // 送る前の変更は自由
//! trader.send(order);        // その後 move で移譲
//! ```
//!
//! ## 証明 3: Akka 流(Arc 共有)は「送信後に触る」バグが素通りする
//!
//! `tell` に渡すのは `Arc` のクローン。送信元は **同じデータへのハンドルを持ち続ける** ので、
//! 送った後に中身を書き換えられてしまう。しかもコンパイルは通る = 型は何も守っていない:
//!
//! ```
//! use std::sync::{Arc, Mutex};
//! use capability_proof::akka_style::SharedRef;
//!
//! struct Order { qty: u32 }
//! let order = Arc::new(Mutex::new(Order { qty: 100 }));
//! let handle = SharedRef;
//! handle.tell(order.clone());              // "送信"。だが送信元はハンドルを保持
//! order.lock().unwrap().qty = 200;         // 送信後に共有データを変更 — 通ってしまう
//! ```

use std::sync::mpsc::{self, Receiver, Sender};

/// 型付き actor。メッセージ型は固定で `Send`(= sendable な `iso`/`val` であることを型で要求)。
pub trait Actor: Send + 'static {
    /// この actor が受け取るメッセージの型。`Send` が「actor 間を渡せる(`iso`)」の型レベル要件。
    type Message: Send + 'static;

    /// メッセージ 1 通を処理する。`&mut self`(= `ref`)なので actor は自状態の唯一の所有者。
    /// 単一 actor が単一スレッドで run-to-completion に回すため、ロックは要らない。
    fn handle(&mut self, msg: Self::Message);
}

/// actor のメールボックスへのハンドル。メッセージを **値で(move で)** 送る。
///
/// `send` が `msg: A::Message` を値で取ることが肝: 呼び出し側は所有権を失い、送信後の
/// use-after-send がコンパイルエラーになる。これが `iso`(隔離)の型による保証。
pub struct ActorRef<A: Actor> {
    tx: Sender<A::Message>,
}

impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<A: Actor> ActorRef<A> {
    /// メッセージを move で送る。所有権はここで actor 側へ移り、送信元からは消える。
    pub fn send(&self, msg: A::Message) {
        // `msg` をチャネルへ move。以後この値の所有者は actor スレッドだけ。
        let _ = self.tx.send(msg);
    }
}

/// actor を専用スレッドで起動する(コアピン留め run-to-completion ループのスタンドイン)。
///
/// 本番ではここが「コアに固定した単一スレッド + SPSC リングバッファ」になる。ここでは
/// 型の性質(move による隔離)を示すのが目的なので、標準の mpsc チャネルで代用している。
pub fn spawn<A: Actor>(mut actor: A) -> ActorRef<A> {
    let (tx, rx): (Sender<A::Message>, Receiver<A::Message>) = mpsc::channel();
    std::thread::spawn(move || {
        while let Ok(msg) = rx.recv() {
            actor.handle(msg); // 受け取った所有権を handle に move
        }
    });
    ActorRef { tx }
}

/// Akka 流 / 現状 AetherFlow の対比用: メッセージを `Arc<Mutex<_>>` で **共有** する形。
///
/// 隔離は「送った後は触らないでね」という **規約** でしかなく、型は何も強制しない。
/// 証明 3 の doctest が示す通り、送信後の共有変更が素通りでコンパイルできてしまう。
pub mod akka_style {
    use std::sync::{Arc, Mutex};

    /// 共有ハンドル。`tell` は `Arc` のクローンを受け取る(= 送信元も同じデータを保持し続ける)。
    pub struct SharedRef;

    impl SharedRef {
        /// メッセージを「共有」で受け取る。move ではないので送信元の手元にも残る。
        pub fn tell<M: Send>(&self, _msg: Arc<Mutex<M>>) {
            // 実際のランタイムなら mailbox へ enqueue する。ここでは対比のため何もしない。
        }
    }
}
