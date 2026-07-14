# 競合ランドスケープ — AetherFlow は何と、どう違うか

> AetherFlow の各構成要素(thread-per-core / capability 隔離 / zero-copy move / 型付き MP)は
> **個別には既存**。本書は「では delta は何か」を、一次ソース付きで正直に示す。
> 数値は各出典に基づく(リンクは §7)。関連: `design.md`(技術 Thesis)、`stage0-bench-notes.md`(実測)。

---

## 0. 結論(先出し)

各要素は既存 —— thread-per-core も、capability 隔離も、zero-copy move も、個別には先行例がある。
**AetherFlow の delta は「どれか1つ」ではなく「その組み合わせ」**:

> **Rust の actor ランタイムで、thread-per-core + コアピン留め + 型付き SPSC/MPSC + capability による
> 隔離保証を、全部同時にやる。** これを丸ごとやっている競合は(我々の調査した限り)見つからない。

比較の相手は「Tokio+actix」ではなく **glommio(既製の thread-per-core)+ kompact(高スループット Rust MP)
+ Pony(capability の元祖)** の3方向。以下、順に。

---

## 1. thread-per-core vs work-stealing — 勝ちは本物、しかし既製品もある

**thread-per-core は tail latency で明確に勝つ。しかも既に実証・実装済み(= その部分自体に新規性は無い)。**

| ソース | 数値 | 出典 |
|---|---|---|
| glommio(Datadog製 Rust thread-per-core) | tail latency 最大 **71%改善** vs 従来スレッド。context switch ≈5µs。Seastar/ScyllaDB の直系 | datadoghq.com/blog/engineering/introducing-glommio |
| Apache Iggy(Tokio work-stealing → thread-per-core 移行, 2026-02) | 16x16 で **P9999 86.30ms → 7.17ms(~92%減)** @ ~1000MB/s。32x32 で **P99 4.52→1.82ms(60%減)**。機構=コアピン留め+リソース分割で task 移動・キャッシュ無効化・ロック競合を排除 | iggy.apache.org/blogs/2026/02/27 |
| Redpanda(vs Kafka) | Seastar ベース thread-per-core、1コア1スレッドをピン留め、shared-nothing、ロック無し | redpanda.com/blog/what-makes-redpanda-fast |

**thread-per-core が効きにくい条件(正直に):**
- **低並列**: Iggy の 8x8 では P95/P99 がほぼ横ばい。**効果はコア数・並列度が上がるほど出る**。
- **負荷偏り**: 自動再分散(work-stealing)が無いので、ホット actor が1コアで詰まると助けが来ない(= thread-per-core の構造的トレードオフ)。

→ **thread-per-core の勝ちは本物。ただし既製の glommio でも得られる。** よって AetherFlow の意味ある比較対象は
「Tokio actor」ではなく **「glommio(thread-per-core)+ actor 層」**。差分は actor 抽象と capability 隔離。

---

## 2. kompact — 高スループットな先行 Rust MP

| 観点 | kompact(KTH) | 出典 |
|---|---|---|
| モデル | Kompics コンポーネント + actor の**ハイブリッド**(純 typed actor ではない) | github.com/kompics/kompact |
| 性能 | **最大 400M msg/s @ 36コア** | 同上 |
| ベンチ手法 | kompicsbenches で Rust Kompact / Actix vs Akka / Kompics / Erlang OTP を単一ハーネスで横断比較 | github.com/kompics/kompicsbenches |
| 掲げていない事 | README に **thread-per-core / コアピン留め / SPSC / capability 隔離の記載なし** | github.com/kompics/kompact |

→ **差別化はある**(kompact は AetherFlow の中核機構をどれも掲げていない)。スループットは高い一方、
**tail latency / jitter / コンパイル時安全**は kompact の主眼ではない —— そこが AetherFlow の立ち位置。

---

## 3. 既存 Rust actor 勢 — ここに明快な delta

| フレームワーク | ランタイム | 型 | コアピン留め |
|---|---|---|:--:|
| actix | 独自(Tokio 上) | typed(Handler) | ❌ |
| coerce | Tokio | — | ❌ |
| kameo | Tokio | typed | ❌ |
| ractor | Tokio(+async-std) | — | ❌ |
| xtra | 複数ランタイム対応 | typed | ❌ |

出典: tqwewe.com/blog/comparing-rust-actor-libraries, github.com/tqwewe/actor-benchmarks

→ **主要 Rust actor クレートは全て Tokio work-stealing。thread-per-core / コアピン留めをやっているものは無い。**
これが AetherFlow の最も明快な delta。(ただし §1 の通り glommio という「actor ではない thread-per-core」が
既にあるので、**"actor 化 + capability" が付加価値の本体**。)

---

## 4. capability による隔離 — Pony が元祖

| 主張 | 出典 |
|---|---|
| Pony は 6 capability(iso/val/ref/box/trn/tag)を型に持ち、**コンパイル時に**データ競合を排除 | tutorial.ponylang.io |
| Pony は **iso の move で zero-copy** メッセージ受け渡し。ランタイムロック・refcount 不要(GC 用の refcount 変化はある) | ponylang.io/media/papers/fast-cheap.pdf |
| HN: **Rust の borrow checker は Pony の reference capabilities と大きく重なる** | news.ycombinator.com/item?id=17195873 |

→ **「capability でコンパイル時に actor を隔離」は Pony(2015)の既存成果。その部分に新規性は無い。**
AetherFlow の立ち位置は「Pony の理論を **Rust で**、しかも **CPU トポロジ一体で**」。Pony のランタイムは
thread-per-core/コアピン留めではない(= 物理配置が差分)。ここが本当のオリジナリティの核。

---

## 5. 金融/matching engine — 数値の壁と「actor を使わない」現実

| 対象 | レイテンシ | 出典 |
|---|---|---|
| LMAX Disruptor | 平均 **52ns/hop**、99% が 128ns 未満、25M+ msg/s | lmax-exchange.github.io/disruptor |
| exchange-core(Disruptor 上の Java matching engine) | **p50 0.5µs / p99 4µs / p99.9 22µs / 最悪 45µs @ 1M+ ops/s** | github.com/exchange-core/exchange-core |
| LMAX 本体 | Disruptor で order matching / risk を構築 = **自前データ構造。actor ランタイムではない** | lmax-exchange.github.io/disruptor |

→ **matching engine の現実は「actor を使わず、自前 Disruptor/thread-per-core」。** これは両刃:
- **機会**: 「Disruptor 級の速さ + actor の安全性/生産性 + capability 隔離」を出せれば新しい。
- **正直な壁**: この領域は**わざとフレームワークを避けて**レイテンシを稼ぐ。exchange-core の
  **p99 4µs / p99.9 22µs** が参照点。actor 抽象のコストでこれに迫れるかは実測で示す領域。

---

## 6. 差別化は本物か

**条件付きで yes。**
- 単一要素ではどれも既存(thread-per-core=glommio、capability=Pony、高速 typed Rust MP=kompact、zero-copy=Pony)。
- **組み合わせは空いている**: 「thread-per-core + コアピン留め + 型付き SPSC/MPSC + capability 隔離を、Rust の
  actor ランタイムとして丸ごと」。最も強い差分は **Pony に対する物理配置(CPUトポロジ)一体化** と、
  **Rust actor 勢に対する thread-per-core**。
- **「勝ちが十分大きいか」**: thread-per-core の速さの勝ち自体は既製 glommio でも得られる。**速さ単体では
  glommio と kompact に挟まれる**。勝ち筋は **「Disruptor 級の tail latency を、capability でコンパイル時に
  安全な actor 抽象として出す」= 速さ × 安全 × 抽象の三点セット**(= `positioning.md` の主張)。

---

## 7. 一次ソース一覧
- glommio(datadoghq)/ kompact・kompicsbenches(github/kompics)/ LMAX Disruptor(lmax-exchange.github.io)
- Pony(tutorial.ponylang.io, fast-cheap.pdf, OGC.pdf)/ Rust actor 比較(tqwewe.com, actor-benchmarks)
- Apache Iggy(iggy.apache.org)/ exchange-core(github)/ Redpanda(redpanda.com)

## 8. 関連文書
- `design.md` — §2.3「Pony に無くてこの構想にある差分」= 本書で裏取り(物理配置が生き残り差分)
- `stage0-bench-notes.md` — 上の参照点に対する実測
- `pony-rust-capability-mapping.md` — capability 理論の詰め
