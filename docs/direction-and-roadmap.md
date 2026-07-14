# AetherFlow 方向性とロードマップ

> この文書の位置づけ: `design.md` が「何を作るか(技術的 Thesis)」を述べるのに対し、本書は
> **「なぜその作り方に決めたか」** という意思決定と技術的な道筋を残す。
> 2026-07-02 の設計議論の結論。将来の自分が「また同じ所で悶える」のを防ぐための地図。
>
> ※ 本書は技術ロードマップに徹する(事業・ドメイン方面の検討は本書の範囲外)。

---

## 1. 頓挫の総括 — なぜ一度止まったか

コードが汚いことが原因ではない。真因は **「未決の分岐を放置したまま両方の都合を混ぜたこと」**。

当時は Tokio(async ランタイム)を研究し、Akka(actor 実行モデル)を研究し、**どちらに乗るかを決めきらないまま**実装した。結果、`core/src` には:

- `Arc<Mutex<T>>`(Akka 的な共有 + 実行時ロック)
- `async fn receive`(Tokio 的な非同期)
- `Mailbox` / `Dispatcher` / `Runtime` トレイト(自前ランタイム的な抽象)

が**同居し、どれも中途半端**になった。スキルの問題ではなく、**意思決定の欠落**が頓挫の正体。

→ 教訓: **技術に手を付ける前に分岐を潰す。** 方向が一意なら、コードは一方向にしか流れず、悶える余地が消える。

---

## 2. 現状の実装の実態(2026-07 時点)

> ※ 以下は再構築前(旧 `Arc<Mutex>` 実装)のスナップショット。§6 の着手ログのとおり、現在は
> typed / move / SPSC・MPSC / コアピン留めの新ランタイム(`core/`, package `aetherflow`)に置換済み。
> ここは「なぜ作り直したか」の記録として残す。

`core/src` は **トレイト定義のスケルトンが大半で、動く実体は「メッセージを受けてログ出力するだけ」**。
データが流れる唯一の経路は `main.rs` → `ActorRef::tell` → `receiver.receive()` で、そこで `println!` / `info!` するだけ。

### 動く部分
- `reference.rs` の `ActorRef<T> = { actor: Arc<Mutex<T>>, sender: Arc<Mutex<dyn ActorSender>>, receiver: Arc<Mutex<dyn ActorReceiver>> }`。`tell` は `receiver.lock().await` して `receive(msg)` を呼ぶだけ。
- `message.rs`: `Arc<dyn Message>`(全 `T: Any+Send+Sync+Debug` へのブランケット実装)。
- `lifecycle.rs`: 状態遷移マシンは実装済みだが **どこからも呼ばれていない**。
- `path.rs`: `ActorPath` ビルダーのみ。

### トレイトだけで中身が空(未接続)
- `mailbox.rs`: `Mailbox` トレイトはあるが `UnboundedMailbox` / `BoundedMailbox` 実装は **全部コメントアウト**。
- `dispatcher.rs` / `runtime.rs` / `executor.rs`: メソッド 1〜2 個のトレイトのみ、実装ゼロ。
- `behavior.rs` / `action.rs` / `state.rs` / `task.rs` / `encapsulator.rs`(`Envelope`)/ `decapsulator.rs`: 型はあるが送受信経路と **繋がっていない**。
- `derive/src/lib.rs`: `#[derive(Actor)]` がコメントアウト済みの `UnboundedMailbox::new(10)` を前提にするため **今使うとコンパイル不能**。
- `remote` / `cluster` / `persistence` / `streams`: crate 枠だけ。

### 現状のメッセージ経路(動く1本)
```
ActorRef<T>          msg.clone()          receiver.lock()      receive()
Arc<Mutex<T>>   ──▶  Arc<dyn Message> ──▶ .await 実行時ロック ──▶ println! だけ
  共有+ロック          送信後も触れる         ロックフリーでない
   (①の逆)             (②の逆)
```

---

## 3. 理論 ⇄ 実装 対比表

`design.md` の 4 本柱(①型による隔離 / ②コピーゼロの move / ③物理配置の一級化 / ④自前スケジューラ)を軸に。

| 観点 | 理論(design.md / Hewitt) | 旧実装(再構築前) | 判定 |
|---|---|---|---|
| ①隔離の保証 | 型システムでコンパイル時保証(Pony caps) | `Arc<Mutex<T>>` 実行時ロック | ❌ 逆 |
| ②メッセージ移動 | `move`=所有権移譲、送信後は触れない | `Arc<dyn Message>` を `clone()` 共有 | ❌ 逆 |
| ③物理配置 | コアピン留め + コア内 SPSC mailbox | mailbox 実装コメントアウト・配置概念なし | ⛔ 未実装 |
| ④スケジューラ | 自前・work-stealing 排除 | 素の `tokio::runtime::Runtime`(work-stealing) | ❌ 逆 |
| メッセージ型 | (含意)typed / associated Message 型 | `dyn Message` 型消去(move 不能化の元凶) | ❌ 逆 |
| Hewitt: state×msg→(state,msgs,behavior) | `Behavior` 型・`Action` enum で表現 | 型はあるが送受信経路と未接続 | ⛔ 未接続 |
| Hewitt: become | `Action::Become` | enum のみ、実行機構なし | ⛔ 未実装 |
| Hewitt: create | `Action::Create` / `Parent::spawn` | トレイトのみ、実体なし | ⛔ 未実装 |
| ライフサイクル | preStart/postStop 等(Akka 由来) | 状態機械は完成、**未接続** | ⚠ 孤立 |
| supervisor 階層 | akka-analyze で分析済み | `parent.rs` = 空トレイト 1 個 | ⛔ 未実装 |

凡例: ❌=Thesis と逆方向 / ⛔=未実装 / ⚠=実装済みだが孤立(いずれも再構築前の状態)

### 崩れの根本原因は一点
**`dyn Message` による型消去が `move` を不可能にし、その代償で `Arc<Mutex>` の共有・ロックに逃げている。**
ここが ①②③④ すべての崩れの震源。逆に言えば、ここを直すと 4 本柱が一直線に揃う。

**目指す経路(=再構築で実現した姿):**
```
Actor<M>            send(msg: M)         SPSC mailbox         &mut self
associated 型 M ──▶ move で移譲     ──▶ コア内・lock-free ──▶ コアピン留め
                    送信後 msg に触れると compile error(②を型で保証)
```

鍵 = **型消去をやめる**:
- `dyn Message`(型消去)→ move 不能 → Arc に頼る → 共有 → ①②が崩れる(旧実装)
- actor ごとに associated `Message` 型を持たせ、mailbox を型付き SPSC に
- すると `msg: M` を move で渡せる → Arc も Mutex も不要 → ①②③④が一直線に揃う
- ＝ Pony の `iso`(一意・送信可)を Rust の「owned `T: Send` を move」で再現する、が土台

---

## 4. 決めた方向性 — 4 分岐の決着

頓挫の再発を防ぐため、着手前に以下 4 分岐を確定した。

### 分岐1: async/await に乗るか → **乗らない(B 案)**
両立不可。震源はここ。
- **A 案(却下): async 全面採用** — 楽だが work-stealing でコア間を跨ぎ、`.await` 越しにロックが要り、Thesis の「コア局所・ロックフリー・zero-copy」が原理的に成立しない。行き着く先は「Rust 版 Akka」= 既に kameo/ractor/xtra があり、**1 から作る意味が薄い**。
- **B 案(採用): hot path から async を追放(Pony / LMAX / kompact 流)** — actor は `fn handle(&mut self, msg: M)` の **同期・run-to-completion**。コアにピン留めした OS スレッド上で、SPSC リングバッファの mailbox から取り出して回す自前スケジューラ。**Tokio は I/O の縁(ネットワーク等)にだけオプションで使う**。Thesis が本当に成立するのはこれだけ。

### 分岐2: 何を作るか(ゴール) → **(b) 新規性のあるランタイム**
- (a) 学習・研究 / **(b) 新規ランタイム(採用)** / (c) 実用ライブラリ
- (b) を選ぶ以上、コア局所性・レイテンシで既存 Rust actor 群を上回ることが目標。数ヶ月級の systems project。
- 過去の失敗は「(b) の理想を掲げ (c) の便利さを実装し (a) の厳密さが抜けた」混在状態。**混ぜない。**

### 分岐3: スコープ → **v1 は単一ノード。分散/永続化/streams は凍結**
- リモート/分散は zero-copy と矛盾(ネットワーク越し = serialize = コピー必須)。
- `remote` / `cluster` / `persistence` / `streams` は **クレート枠だけ残して当面凍結**。捨てるのではなく将来の拡張面として後ろに送る。

### 分岐4: typed か dynamic か → **typed(分岐 1 で自動決定)**
- B 案 = typed actor(associated Message 型)一択。move できて Arc も Mutex も不要になる。
- typed の現実的コストは受容する:
  - 1 つの actor が複数種類のメッセージを扱う → enum か複数チャネル
  - 親が型の違う子を束ねる → `tag` 相当の型消去がどこかで必要
  - これらは実装で必ずぶつかるので逃げずに設計する。

### 確定した姿(たたき台)
> **ゴール (b)。B 案(async を core から追放)。v1 は単一ノード、分散/永続化/streams は凍結。typed actor。**
> 最初のマイルストーンは極小に:**1 コアにピン留めした 1 スレッド + 1 つの typed actor + 1 本の SPSC mailbox + send は move + run-to-completion ループ。**
> マルチコア・ルーティング・supervision は N=1 で Thesis が成立してから足す。

---

## 5. 事業・ドメイン方面(本書の範囲外)

本書は技術ロードマップに徹する。事業・ドメイン方面の検討は別途管理しており、この公開ドキュメントの
範囲外とする。以下 §6 の技術進捗に続く。

---

## 6. 実装の進捗と次の技術ステップ

### 着手ログ(方向が確定して実装したもの)
1. **理論の詰め(B)** ✅ **完了(2026-07-03)** → `docs/pony-rust-capability-mapping.md`
   - Pony の 6 capability(`iso`/`trn`/`ref`/`val`/`box`/`tag`)⇄ Rust の所有権・`Send`/`Sync` 対応表を作成。
   - **go/no-go = go**: runtime に必要な `iso`/`val`/`tag`/`ref` は全て Rust に写り、核心の `iso`(move するメッセージ)は Pony より綺麗(構造的・静的に完全)。埋まらない差分(`trn`/`recover`/viewpoint adaptation)は critical path 外で non-goal。
   - 導出された API 形: `trait Actor { type Message: Send; fn handle(&mut self, msg: Self::Message); }`(typed・move・同期 run-to-completion)。型消去(`dyn Message`)をやめれば ①②③④ が一直線に揃う、と確定。
2. **検証実験(C)** ✅ **完了(2026-07-03)** → `experiments/capability-proof/`(親 workspace から detach した独立 crate)
   - `cargo test` が 3 つの doctest + 2 つの実行時テストで理論を裏取り:
     - **証明1(compile_fail)**: typed + move API で `send(order)` 後に `order` を使うと `E0382: borrow of moved value` で**コンパイル不能**。隔離を型が強制することを実証。
     - **証明3(pass)**: Akka 流 `Arc<Mutex<_>>` は `tell(order.clone())` 後に `order.lock().qty = 200` が**素通りでコンパイル**。隔離が規約でしかないことを実証。
   - **副産物の学び**: 検証には move した値を**実際に使う**必要がある。`let _ = order.qty;` の `let _ =` は「使用」とみなされず move を発火しないため、最初この罠で compile_fail が誤って通った。テストは `println!` 等で実使用すること(crate の doc に注記済み)。
   - **メタ教訓**: パイプ後の `$?` は最後のコマンド(`head`)の終了コードで、`rustc` のものではない。コンパイル可否はパイプなしで exit を取る。
3. **競合ランドスケープ調査(差別化ゲート下調べ)** ✅ **完了(2026-07-03)**
   - **判定: 全敗ではないが空白でもない。** thread-per-core(glommio/Seastar)/ capability隔離(Pony)/ zero-copy move(Pony)/ fast typed Rust MP(kompact 400M msg/s)は**個別には全て既存**。差別化は**その組み合わせ**(Rust actor で thread-per-core+コアピン留め+型付きSPSC+capability隔離を丸ごと)にある。
   - **design.md §2.3 の主張の生き残りは1点**: Pony に対する「物理配置(CPUトポロジ)一体化」。thread-per-core 自体は新規性なし。
   - **勝ちが十分大きいかリスク**: thread-per-core の勝ち(tail latency 71〜92%改善)は既製 glommio でも得られる。→ 「速い actor」単体では glommio と kompact に挟まれる。**勝ち筋は 速さ×コンパイル時安全×actor抽象 の三点セット**。
   - **Stage 0 ベンチの的が確定**: 軸=tail latency(p99/p9999)+jitter(throughput 一本は kompact に不利)。相手=①kameo/actix(楽勝ベースライン)②kompact(latencyで挑む)③glommio+薄actor(delta の本丸テスト)。手法=kompicsbenches 流用。
   - 注: 検証パネルが session limit で全滅、数値はソース付きだが独立検証は未了。
4. **N=1 の再構築(A)** ✅ **完了(2026-07-04)** → `core/`(package `aetherflow`、旧 core を置換)
   - typed actor + move メッセージ + **有界 SPSC リングバッファ mailbox**(lock-free・head/tail を 64B 分離で false-sharing 回避)+ コアピン留め(best-effort)+ run-to-completion ループ(空時ビジースピン=LMAX 流)+ on_start/on_stop。
   - `cargo test` 全緑(SPSC 6 + 統合 4 + doctest 2)、`cargo clippy` クリーン、デモ動作(16コア検出→コア0固定→注文 move 処理)。
   - 隔離のコンパイル時保証(E0382)は capability-proof から引き継ぎ、doctest `compile_fail` で担保。
   - **既知の制約**: コアピン留めは macOS では no-op(ハードアフィニティ無し)。実ピン留め+レイテンシ計測は Linux + Stage 0 で。
5. **マルチコア化** ✅ **完了(2026-07-04)** → `core/`(System API に統一)
   - **thread-per-core**(≠ thread-per-actor): `System::with_cores(N)` がコアごと 1 スレッド + ピン留め、各スレッドが多数の actor を run-to-completion で回す。actor はコア間を移動しない静的配置。
   - **routing**: `ActorRef<A>` は clone 可能な MPSC 送信端。cross-core 送信は lock-free。同一コアの異種 actor は型消去(`dyn ErasedActor`)して 1 スレッドで回す(制御面=erased / データ面=typed)。
   - **mailbox = 有界 MPSC**(Vyukov のスロット sequence、lock-free、64B 分離で false-sharing 回避)を新規実装。SPSC は単一生産者高速パス/将来のコア間ペアキュー用に温存。
   - 検証: `cargo test` 全緑(spsc 6 + mpsc 5 + 統合 5 + doctest 2)、clippy クリーン、並行 MPSC・cross-core routing を release で各 20/20 反復パス、sharding デモ動作(gateway→3コアの engine)。
   - 既知の制約: 静的配置のみ(work-stealing しない=偏り再分散は未着手)、同一コア handler 内の `send_blocking` はデッドロック注意(try_send を使う)、macOS ピン留め no-op。
6. **Stage 0 ベンチ(勝負どころ)** 🔄 **予備測定 + AWS Tier1 実測済み** → `docs/stage0-bench-notes.md`、`core/benches/latency.rs`
   - vs Tokio(work-stealing)で **中央値レイテンシ ~3.2倍・スループット ~3.9倍**(macOS/no-pin 予備)。倍数の勝ち=「十分大きいか」バーは予備的にクリア。
   - **macOS では tail(p999)は同着**、max は跳ねる(ピン留め無しの予測どおり)。
   - **Linux コンテナ実測(Docker on Mac, 4コア)**: median ~14倍・p99 ~8倍 aether 優位。ただし純 busy-spin は p999=2.6ms と tail 爆発(仮想化下の preempt)。
   - **対策実装 `IdleStrategy::Backoff`**(spin→yield→park): p999 を 20倍改善(2.6ms→128µs、median +10% のみ)。**backoff は tokio/kameo を全分位で上回る**。既定は latency 優先 BusySpin、共有/仮想化は Backoff、と使い分け。
   - **AWS Tier1 実測(実 Linux, 専有 vCPU)**: median ~10倍・ask ~40倍・throughput ~4.6倍、**tail が Docker の 2.5ms → 3.3µs に締まった**。看板は実ハードで証明済み。
   - **残る本番(任意/後追い)**: 隔離コア bare-metal(Tier2)で絶対 tail を確定。AWS quota を待たず Latitude.sh/Vultr で撃てる(`docs/run-on-linux.md`)。Tier2 は公開の必須ゲートではなく ceiling-flex。
   - **positioning(2026-07-04): cloud-first を主戦場に。** ベアメタル絶対 tail(HFT級)は狭いニッチの "天井 flex"。主戦場は「専有 vCPU のクラウドで Tokio/kameo に全分位圧勝、しかも backoff でアイドル CPU を焼かない、しかもコンパイル時安全」。詳細は `positioning.md`。

### 進め方
**B → C → A** の順で進めた。理論で土台を固め、最小例で裏取りし、それから core を作り直す。手戻り最小。

### 旧 core 退役 / リポジトリ整合化 ✅ 完了(2026-07-04)
- 会話の発端だった「汚い状態」を解消。**新ランタイムを正式な `core/`(package `aetherflow`)に昇格**、
  旧 `Arc<Mutex>` の Akka クローン(thesis と逆)を置換。
- 旧 API 専用の `derive` / `macros` / `main-macro` crate を削除、root の死んだ scaffolding(examples/
  tests/benches)も削除。detach していた `runtime/` を core に統合。
- `remote` / `cluster` / `persistence` / `streams` は凍結 stub のまま workspace に残置(将来の拡張面)。
- `cargo test --workspace` 全緑・clippy クリーン。**workspace が1つの coherent なビルドに回復**。

## 6.6 GPT 外部レビューの取り込み(2026-07-04)

外部レビュー(perf-first レンズ)を選別。**決着済みの分岐(work-stealing 排除・async 追放)や
Tokio 互換を目標化する提案は退け**(thesis に反する。縁での相互運用=可・互換目標=否)、NUMA は
v1 凍結維持。**拾ったもの**:

- **Observability(最優先の新規)** 🔄 **第一弾実装済(2026-07-04)**: `ActorRef::mailbox_depth()` /
  `total_sent()` / `total_processed()`。**hot path 追加コストゼロ** — 単一消費者の lock-free mailbox が
  既に持つ enqueue/dequeue カウンタを露出するだけ(共有/work-stealing 系が競合 atomic を要するのと
  対照的=モデルの帰結)。**processing-latency ヒストグラムも実装済(opt-in)**: `sys.build(..).instrumented().spawn()`
  で有効化、`ActorRef::latency()` が p50/p99/p999/mean を返す。Instant のホットコストがあるので既定 OFF
  (zero-overhead by default)。単一ライターゆえバケット更新は relaxed で競合なし。**残り**: cross-core
  migration 数。これで以後の最適化が全部データ駆動になる。
- ✅ **[実装済] SpawnBuilder(shallow surface の実践, 2026-07-04)**: 増えた `spawn_on_*` を
  `sys.build(make).core(0).mailbox(4096).supervised().instrumented().spawn()` に整理。単純な場合は
  `spawn_on` のまま、上級ノブだけ opt-in(progressive disclosure = §2.5・GPT #7)。`core/src/system.rs`。
- ✅ **[実装済] デッドロックガード(type-justified safety 第3号, 2026-07-04)**: thread-local で
  コアを識別し、handler 内から**同一コア**の actor へ `ask`/`send_blocking` する自己ブロックを検知。
  **沈黙のハングを明確なエラー/panic に変える**(`ask` は `Err(WouldBlockCallingCore)`、`send_blocking`
  は panic→パニック分離で捕捉)。footgun をランタイムが弾く。`core/src/system.rs` / `ask.rs`。
- **cache-aware scheduler**: 現在の round-robin ポーリングは素朴。observability を付けてから
  キャッシュ意識の順序付けへ(「一点突破三点セット」で唯一まだ甘い所)。
- `#[actor]` derive はエルゴノミクス改善として保留(低優先)。

### リスク register: 同一コア内 head-of-line blocking(GPT が見落とした最鋭リスク)
thread-per-core は「1 コア = 1 スレッドが多数 actor を run-to-completion」ゆえ、**1 つの handle が
長い/重いと同じコアの全 actor が待たされる**(work-stealing なら逃がせるが静的配置は逃げ場なし)。
= thread-per-core の構造的弱点、"fairness vs locality" の我々版。緩和候補: per-message 時間予算、
協調 yield、重い actor の隔離配置/専用コア。Stage 0 の後に実測で顕在化するはず。設計課題として登録。

## 7. 関連文書
- `concepts-explained.md` — 概念のやさしい解説(用語集)。カプや CPU 概念を日常のたとえで。腹落ちの原点。
- `design.md` — 技術的 Thesis(4 本柱)と先行研究(Pony / LMAX / work-stealing)
- `positioning.md` — 4象限マトリクス + 総合力(公開ピッチ)
- `stage0-bench-notes.md` — ベンチ実測(macOS / Docker / AWS Tier1)
- `pony-rust-capability-mapping.md` — Pony caps ⇄ Rust 所有権の対応表(§6 着手 1、作成済み)
- `actor-model-theoretical-concepts.md` — Hewitt actor モデルの形式化
- `akka-analyze.md` — Akka 内部構造の詳細分析(ActorRef→mailbox→Dispatcher、Supervisor 階層、LifeCycle)
