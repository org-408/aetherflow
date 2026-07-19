<div align="center">
  <img src="docs/assets/logo-mark.png" width="140" alt="AetherFlow" />

  # AetherFlow

  **Flow at the speed of hardware.**

  Rust 向けの高性能 actor ランタイム — thread-per-core、lock-free、zero-copy。
  型システムが隔離をコンパイル時に証明し、他のランタイムが安全にはできない最適化を解禁する。

  [Docs](docs/design.md) · [Why AetherFlow?](docs/direction-and-roadmap.md) · [Benchmarks](docs/stage0-bench-notes.md)

  [English](README.md) · 🌐 日本語
</div>

---

AetherFlow は Tokio の置き換え**ではありません**。別系統です — CPU から設計した actor ランタイムで、
コア・キャッシュ・メッセージの所有権が後付けでなく一級市民です。

あなたは**普通の型付き Rust** を書きます。メッセージを `send` すると、その所有権が runtime に **move**
され、送った後に使うとコンパイルできません(`E0382`)。これは Pony の `iso` を Rust で `T: Send` の move
として取り戻したもので、runtime はメッセージを**ロック無し・GC 無し・メッセージ毎の `Arc`/refcount 無し**で
ルーティングします — ベンチではなく保証です。(移送されるのは*メッセージ値*の所有権です。Rust の `Send` は
`Arc<Mutex<_>>` のような明示的共有状態をメッセージ*内*に入れることは許します — [Known limitations](#既知の制約) 参照。)

- 🛡️ **コンパイル時に証明される隔離** — データ競合フリーは*型エラー*であって実行時の規約ではない。
  use-after-send はコンパイルできない(`E0382`)。
- ⚡ **thread-per-core・run-to-completion** — コアごと 1 OS スレッド、actor はピン留め、work-stealing
  無し、コア間移動無し。キャッシュ局所性が構造的に保たれる。
- 📨 **zero-copy メッセージ** — `send` が所有権を move。clone も `Arc<Mutex>` も無い。
- 🔒 **lock-free mailbox** — 有界 MPSC リング、head/tail を別キャッシュラインに分離して false sharing を回避。
- 🎯 **三点セットを同時に** — 静的データ競合フリー **+** GC ポーズ無し **+** メッセージ毎の heap 確保・
  clone・`Arc` refcount 無し。(lock-free mailbox は他の MPSC 同様 atomic を使う — 無いのは*メッセージ毎*の
  refcount であって全 atomic ではない。)Pony は capability を証明したが GC を払う。他の Rust actor
  フレームワークはどちらも持たない。

## クイックスタート

```toml
[dependencies]
aetherflow = "0.1"
```

```rust
use aetherflow::{System, Actor};

// actor は普通の型付き Rust: メッセージ型 1 つ、ハンドラ 1 つ。
struct OrderBook { bids: u64 }

impl Actor for OrderBook {
    type Message = Order;                 // 固定・`Send` = sendable な `iso`

    fn handle(&mut self, order: Order) {  // &mut self: 唯一の所有者、ロック不要
        self.bids += order.qty as u64;
        println!("matched {} @ {}", order.qty, order.price);
    }
}

struct Order { qty: u32, price: u32 }

fn main() {
    let sys = System::with_cores(4);              // 4 コア、4 本のピン留めスレッド
    let book = sys.spawn_on(0, OrderBook { bids: 0 });

    let order = Order { qty: 100, price: 42 };
    book.send_blocking(order).unwrap();            // `order` をコア 0 へ move。actor が gone なら Err(Closed)
    // println!("{}", order.qty);                  // ← コンパイルできない(E0382)

    sys.shutdown();
}
```

> **注意:** `with_cores(n)` は既定で `IdleStrategy::BusySpin`(最低レイテンシだが `n` コアを ~100% CPU で
> 使い続ける)。ラップトップや共有機では `System::with_cores_idle(n, IdleStrategy::backoff())` を。

返事が要る? `ask` は reply cell を呼び出しスタックに置く — 呼び出しごとの heap 確保無し(actor が返信する
まで呼び出し側はブロック):

```rust
let depth: u64 = book.ask(|reply| Query::Depth(reply))?;
```

## 例で学ぶ

「hello actor」から実務パターンまで 10 分の道のり — [**ガイド**](docs/guide.ja.md) を参照。各節は実行できる例に対応:

| `cargo run --example …` | 何を示すか |
|---|---|
| [`hello_actor`](core/examples/hello_actor.rs) | 最小の actor — 状態・`handle`・ライフサイクル |
| [`request_reply`](core/examples/request_reply.rs) | `ask` の request-reply — ゼロアロ KV ストア |
| [`sharded`](core/examples/sharded.rs) | コアへの fan-out(thread-per-core、ロック無し) |
| [`supervision`](core/examples/supervision.rs) | panic 分離 + 自動 restart |
| [`pubsub`](core/examples/pubsub.rs) | 一対多ブロードキャスト — `ActorRef` をメッセージで渡す |
| [`tokio_interop`](core/examples/tokio_interop.rs) | AetherFlow を Tokio アプリの state コアとして埋める |
| [`echo_server`](core/examples/echo_server.rs) | I/O as messages — `async` 無しの TCP サーバ(`--features net`) |

## なぜ「型システムが速さを解禁する」のか

性能の機構(バッチ・per-core プール・emplace)は treadmill — 数字を出せば誰でも 1 リリースで真似できる。
速さ単体は堀にならない。

堀は**型システム**。メッセージ値が(メッセージ境界で単一所有・コンパイル時検査され)*move* されるので、
メッセージ毎の atomic refcount 無し・GC 無し・per-core メッセージ再利用といった攻めた最適化が*構造的に*
安全になる。型システムを持たないランタイムはそれを安全に真似できない — 型システムごと作る羽目になる
(Pony 級のコスト)。

そして capability 注釈は一切書かない。理論は床下で働き、表面では普通の Rust を書けば保証がタダで付く。
これが設計原則: **deep theory, shallow surface**。[design.md](docs/design.md) §2.4–2.6 参照。

## ステータスとスコープ

**単一ノード v1。** これは活発に開発中のシステムプロジェクト。

- ✅ typed actor・move メッセージ・lock-free MPSC mailbox・thread-per-core・コアピン留め(best-effort)・
  ゼロアロ `ask`
- ✅ **実ハードで tail latency 検証済み** — AWS Graviton3(実 Linux、ネイティブコアピン留め)で busy-spin の
  tail がミリ秒から ~3–5µs に締まり、全 percentile で Tokio に勝つ(median ~10倍、p99 ~13倍、p999 ~3–4.5倍)。
  ゼロアロ `ask` はサブ µs(p50 268ns / p999 399ns)。コアピン留めは macOS では no-op(ARM でなく OS の制約)。
  [benchmarks](docs/stage0-bench-notes.md) 参照。
- 🎯 **次:** 隔離コア(isolcpus/nohz_full、ベアメタル)で p99.9 を単桁 µs へ — matching engine / HFT 領域。
- 🧊 **当面凍結:** distributed・clustering・persistence・streams。ロードマップにはあるが現行ビルドには無い。
  [direction & roadmap](docs/direction-and-roadmap.md) 参照。

エリート HFT 向けではない(彼らは自作か FPGA)。ターゲットは Disruptor 級の速さ**かつ**安全と生産性を欲しいが
HFT チームを持たない層: 取引所・ブローカー・実時間リスク・マーケットデータ・広告 RTB・ゲーム tick サーバ。

### 既知の制約

若いシステムプロジェクトです。コンセプトとコアは堅牢ですが、いくつかの correctness/robustness 項目は
意図的に未完です — 発見されるより先に明記します:

- **隔離はメッセージ境界であって deep ではない。** `send` はメッセージ値を move する(use-after-send は
  `E0382`)が、Rust の `Send` は明示的共有内部を禁じない — メッセージは書けば `Arc<Mutex<_>>` を運べる。
  これは所有権の*移送*であって Pony-`iso` 級の deep uniqueness ではない。
- **`ask` の liveness は callee の返信に依存する。** `ask` 時点で actor が既に gone なら
  `Err(AskError::Closed)`。reply cell は呼び出しスタックに載るので、actor が返信せず `Responder` を
  *保持*すると `ask` はブロックする ── 上限を付けたいなら **`ask_timeout(dur, ..)`**(cell を `Arc` で
  持ち、timeout 後の遅延返信は安全に破棄。use-after-free / 二重 drop 経路は Miri 検証済み)。
- **lock-free キュー検証。** MPSC mailbox と SPSC リングは Loom で検証(小モデルの全合法インターリーブを
  探索)、Miri が unsafe 経路の UB をカバー。モデルは意図的に極小なので順序規律を証明するものでキュー全体
  ではない: `RUSTFLAGS="--cfg aetherflow_loom"` で `cargo test --lib --release -p aetherflow`。
- **`IdleStrategy::BusySpin` が既定**(コアあたり 100% CPU)— 共有機/バッテリ機では `IdleStrategy::backoff()`。

## ドキュメント

- [guide.ja.md](docs/guide.ja.md) — 10 分ガイド(本 README の例の学習パス)
- [design.md](docs/design.md) — 技術 thesis(4 本柱)と先行研究(Pony / LMAX)
- [io-surface-design.md](docs/io-surface-design.md) — I/O as messages の表面設計とベンチ

## ライセンス

`MIT OR Apache-2.0` のデュアルライセンス。
