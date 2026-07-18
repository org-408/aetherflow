//! sharding / fan-out — N コアに N 個の worker を置き、キーで振り分ける。
//!
//!   cargo run --example sharded
//!
//! ポイント: thread-per-core が**自然に**書ける。各 shard は別コアの別 actor = 別スレッドなので、
//! ロックも共有状態も無しに並列に進む。振り分けはキーのハッシュ(同じキーは必ず同じ shard へ)。

use aetherflow::{Actor, Responder, System};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// 1 shard = カウンタ。自分に来たキーの出現回数を数えるだけ(状態は単一所有)。
struct Shard {
    id: usize,
    counts: std::collections::HashMap<String, u64>,
}

enum Msg {
    Bump(String),
    /// この shard が見たキーの総数を返す。
    Total(Responder<(usize, u64)>),
}

impl Actor for Shard {
    type Message = Msg;
    fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Bump(k) => *self.counts.entry(k).or_insert(0) += 1,
            Msg::Total(reply) => {
                let sum: u64 = self.counts.values().sum();
                reply.reply((self.id, sum));
            }
        }
    }
}

fn shard_of(key: &str, n: usize) -> usize {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() % n as u64) as usize
}

fn main() {
    const SHARDS: usize = 4;
    let sys = System::with_cores(SHARDS);

    // shard i をコア i に置く。返りハンドルを配列で持つ。
    let shards: Vec<_> = (0..SHARDS)
        .map(|id| {
            sys.spawn_on(
                id,
                Shard {
                    id,
                    counts: Default::default(),
                },
            )
        })
        .collect();

    // 大量のイベントをキーで振り分けて投げる(非ブロッキング fire-and-forget)。
    let keys = ["alice", "bob", "carol", "dave", "erin", "frank"];
    for i in 0..10_000 {
        let key = keys[i % keys.len()];
        let s = shard_of(key, SHARDS);
        shards[s].send_blocking(Msg::Bump(key.to_string())).unwrap();
    }

    // 各 shard に総数を聞く(合計が 10,000 になるはず = 取りこぼしゼロ)。
    let mut grand = 0u64;
    for s in &shards {
        let (id, total) = s.ask(Msg::Total).unwrap();
        println!("shard {id}: {total} events");
        grand += total;
    }
    println!("total = {grand}");
    assert_eq!(grand, 10_000);

    drop(shards);
    sys.shutdown();
    println!("ok");
}
