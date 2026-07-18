//! request-reply(`ask`)— 送って**返事を待つ**。小さな key-value ストアを actor で作る。
//!
//!   cargo run --example request_reply
//!
//! ポイント: `ask` は reply スロットを**呼び出しスタック**に置く → **ヒープ確保ゼロ**。
//! kameo/tokio の ask が毎回 oneshot を heap 確保するのと対照的(所有権モデルが解禁した速さ)。
//! 上限を付けたいときは `ask_timeout(dur, ..)`。
//!
//! **注意**: `ask` は呼び出しスレッドをブロックするので、runtime の外(main / I/O スレッド)から呼ぶ。

use aetherflow::{Actor, Responder, System};
use std::collections::HashMap;

/// KV ストア actor。状態(`map`)は単一所有 ── ロック不要。
struct KvStore {
    map: HashMap<String, i64>,
}

/// この actor が受け取るコマンド。返事が要るものは `Responder<R>` を運ぶ(= ask の受け口)。
enum Cmd {
    /// 返事不要(fire-and-forget)。`send` で送る。
    Set(String, i64),
    /// 返事が要る。`ask` で送り、`Responder` 経由で値が返る。
    Get(String, Responder<Option<i64>>),
    /// キー数を返す。
    Len(Responder<usize>),
}

impl Actor for KvStore {
    type Message = Cmd;

    fn handle(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Set(k, v) => {
                self.map.insert(k, v);
            }
            Cmd::Get(k, reply) => {
                reply.reply(self.map.get(&k).copied()); // 一度だけ返信できる線形トークン
            }
            Cmd::Len(reply) => {
                reply.reply(self.map.len());
            }
        }
    }
}

fn main() {
    let sys = System::with_cores(1);
    let kv = sys.spawn_on(
        0,
        KvStore {
            map: HashMap::new(),
        },
    );

    // 書き込みは fire-and-forget(返事を待たない)。
    kv.send_blocking(Cmd::Set("apples".into(), 3)).unwrap();
    kv.send_blocking(Cmd::Set("pears".into(), 7)).unwrap();

    // 読み出しは ask(返事を待つ)。`|reply| ...` に Responder を渡してメッセージを組む。
    let apples: Option<i64> = kv.ask(|reply| Cmd::Get("apples".into(), reply)).unwrap();
    let bananas: Option<i64> = kv.ask(|reply| Cmd::Get("bananas".into(), reply)).unwrap();
    let n: usize = kv.ask(Cmd::Len).unwrap();

    println!("apples  = {apples:?}"); // Some(3)
    println!("bananas = {bananas:?}"); // None
    println!("keys    = {n}"); // 2

    assert_eq!(apples, Some(3));
    assert_eq!(bananas, None);
    assert_eq!(n, 2);

    drop(kv);
    sys.shutdown();
    println!("ok");
}
