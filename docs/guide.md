# AetherFlow を使う — 10 分ガイド

> あなたが書くのは **普通の同期 Rust** だけ。actor = 「1つのメッセージ型 + 1つのハンドラ」。
> 保証(データ競合フリー・GC 無し・ロック無し)は型システムが床下で付ける ── 表面は浅く保つ。
> 各節は `core/examples/` の**実行できる例**に対応(`cargo run --example <名前>`)。

## 0. 30 秒メンタルモデル

- **actor** = 状態(struct のフィールド)+ 振る舞い(`handle`)。状態は**単一所有**なのでロック不要。
- **メッセージ** = `send` すると所有権が runtime に **move** する(送った後に使うと `E0382` = コンパイルエラー)。
- **System** = コアごと 1 スレッドの runtime。actor はコアに固定され、コア間を移動しない(thread-per-core)。

## 1. 最初の actor — `hello_actor`

```rust
use aetherflow::{Actor, System};

struct Greeter { count: u32 }

impl Actor for Greeter {
    type Message = String;                       // 受け取るメッセージ型(固定)
    fn handle(&mut self, name: String) {         // &mut self = 唯一の所有者。ロック不要
        self.count += 1;
        println!("hello, {name}! (#{})", self.count);
    }
}

let sys = System::with_cores(1);                 // 1 コアの runtime
let greeter = sys.spawn_on(0, Greeter { count: 0 });
greeter.send_blocking("Ada".to_string()).unwrap();
drop(greeter);                                   // 送信端を落とす → drain して停止
sys.shutdown();
```

ライフサイクル: `on_start`(配置直後)/ `on_stop`(mailbox drain 後)/ `on_restart`(§4)。

## 2. 返事を待つ — `request_reply`(`ask`)

送りっぱなし(`send_blocking` / `try_send`)と、**返事を待つ**(`ask`)を使い分ける。

```rust
enum Cmd { Set(String, i64), Get(String, Responder<Option<i64>>) }
// ...
kv.send_blocking(Cmd::Set("apples".into(), 3)).unwrap();          // fire-and-forget
let v: Option<i64> = kv.ask(|reply| Cmd::Get("apples".into(), reply)).unwrap();  // 返事待ち
```

- `ask` は reply スロットを**呼び出しスタック**に置く → **ヒープ確保ゼロ**。
- 上限を付けるなら `ask_timeout(dur, ..)`(遅延返信は安全に破棄)。
- `ask` は呼び出しスレッドをブロックするので **runtime の外(main / I/O スレッド)から**呼ぶ。
  同一コアの handler 内から呼ぶと `Err(WouldBlockCallingCore)` で弾かれる(サイレントハングにしない)。

## 3. 並列に捌く — `sharded`(fan-out / thread-per-core)

N コアに N 個の worker を置き、キーのハッシュで振り分ける。同じキーは必ず同じ shard へ。

```rust
let sys = System::with_cores(4);
let shards: Vec<_> = (0..4).map(|id| sys.spawn_on(id, Shard::new(id))).collect();
let s = shard_of(key, 4);
shards[s].send_blocking(Msg::Bump(key)).unwrap();   // 別コア = 別スレッドで並列、共有なし
```

work-stealing しないので、配置が予測可能でキャッシュ局所性が保たれる。

## 4. 落ちても続ける — `supervision`

`spawn_on_supervised` にすると、actor が panic しても**その actor だけ捨てて新品に入れ替え**、
mailbox はそのまま続行する。

```rust
let worker = sys.spawn_on_supervised(0, Worker::new);   // panic したら Worker::new で作り直し
```

状態は単一所有(共有なし)と**型が保証**するので、壊れた状態が誰にも残らず安全に restart できる
(`Arc<Mutex>` は panic で poison して続行不能になるのと対照的)。

## 4.5 一対多に配る — `pubsub`(ブロードキャスト)

`ActorRef` は **Clone + Send** なので**メッセージに載せて渡せる** ── これで購読を登録する。
Hub が購読者リストを単一所有で持ち、`Publish` を全員に `try_send`(非ブロッキング)で配る。

```rust
enum HubMsg { Subscribe(ActorRef<Subscriber>), Publish(String) }
// handler 内(= 他 actor への送信)は try_send を使う(send_blocking は同一コアで deadlock):
for sub in &self.subscribers { let _ = sub.try_send(text.clone()); }
```

## 5. tokio に埋める — `tokio_interop`(段階導入)

既存の async I/O は tokio のまま、**状態と計算だけ** AetherFlow に載せる。規則は2つ:

- **async → actor の送信は非ブロッキング**(`try_send` / `send_blocking`)。そのまま async から呼べる。
- **`ask`(返事待ち)は `tokio::task::spawn_blocking` の中で**呼ぶ(async ランタイムを止めない)。

```rust
// 非同期ハンドラから:
let _ = metrics.try_send(Cmd::Record { bytes });
// 集計を読む(ブロックするので blocking スレッドで):
let snap = tokio::task::spawn_blocking(move || m.ask(Cmd::Snapshot).unwrap()).await.unwrap();
```

## 6. 送信 API の使い分け

| 呼び方 | ブロック | 満杯のとき | 返事 | 使いどころ |
|---|---|---|---|---|
| `try_send` | しない | `Err(Full(msg))` を返す | なし | async / ホットパス / backpressure を自分で捌く |
| `send_blocking` | 満杯時のみ待つ | 空くまで待つ | なし | 単純な fire-and-forget |
| `ask` / `ask_timeout` | 返事まで待つ | — | あり | request-reply(runtime の外から) |

失敗は**元のメッセージを返す**(`err.into_message()`)ので、再送・永続化・ログに使える。

## 7. ネットワーク I/O(feature `net`)

`net` feature で「I/O as messages」の server が書ける(接続=actor、受信=メッセージ、送信=非ブロッキング
handle、async 無し)。`echo_server` / `io_bench` 例と `io-surface-design.md` を参照。

## 関連
- `design.md` — 4本柱の技術 thesis と「deep theory, shallow surface」
- `concepts-explained.md` — 概念のやさしい解説
- `core/examples/` — 本ガイドの全例(実行可能)
