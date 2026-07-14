# ポジショニング — AetherFlow は「唯一の交差点」

> この設計の資産は特許ではなく **ポジショニング(カテゴリの所有)+ 実行の先行 + 数字**。
> 個々の技術は公知でも、**それらを全部同時に満たす交差点は空いている** ── そこを一枚で言い切る。
> branding / README / ランディングの中核素材。根拠は `competitive-landscape.md`(実測は `stage0-bench-notes.md`)。
>
> **正直な但し書き**:「唯一」は "我々が調査した限り既存が見当たらない" の意(世界初の証明は不能)。
> 数字は AWS 実 Linux(Tier 1、単一 actor マイクロベンチ)ベース。

## 0. 一言(カテゴリ)

> **AetherFlow = 「Disruptor 級に速く、かつコンパイル時に安全」な Rust actor ランタイム。**
> 速さ(thread-per-core)と 安全(型で証明する隔離)を **1つのランタイムで両立**した、我々が知る限り唯一の例。

タグライン候補:**"Flow at the speed of hardware."**(速さ)/ 補助:**"Fast like a hand-rolled engine. Safe like Rust."**

## 1. 4象限マトリクス(性能 × コンパイル時安全)

2軸で見ると、既存は3象限に散らばり、**右上(速い×安全)が空いている**。

```
  コンパイル時
  安全(型で
  データ競合ゼロ・         Pony               │        ★ AetherFlow
  GC/ロック無し)      (capability だが         │      thread-per-core +
        ▲            汎用sched + GC)          │      capability + zero-GC
        │            Erlang/Akka(実行時       │      + per-msg refcount 無
        │            安全・速くない)           │      = 右上に単独
        │──────────────────────────────────────┼──────────────────────────
        │            Tokio / actix / kameo      │      Seastar / glommio /
        │            (work-stealing・           │      Redpanda
        │             実行時規律・安全でない)    │     (thread-per-core だが
        │                                       │      actor でも型安全でもない)
        └────────────────────────────────────────────────────────▶
                          速さ(thread-per-core / ハードウェア効率)
```

- **左下**:Tokio/actix/kameo ── 便利で普及、でも work-stealing(遅い尾)+ 実行時規律(footgun)。
- **右下**:Seastar/glommio/Redpanda ── 速いが actor 抽象でも型安全でもない(共有状態を footgun 可能)。
- **左上**:Pony(型で安全だが汎用スケジューラ + GC)/ Erlang・Akka(実行時に安全だが速くない)。
- **右上**:**AetherFlow だけ**。速さと安全を同時に、しかも GC も atomic refcount も無い経路で。

## 2. 詳細マトリクス(誰が何を満たすか)

| | thread-per-core | コンパイル時 隔離 | zero-GC | per-msg refcount 無 | actor 抽象 |
|---|:--:|:--:|:--:|:--:|:--:|
| Tokio / actix / kameo | ❌ | ❌ | ✅ | ❌(Arc) | ○ |
| Seastar / glommio / Redpanda | ✅ | ❌ | ✅ | 部分 | ❌ |
| Pony | ❌ | ✅ | ❌(GC) | ❌(GC refcount) | ✅ |
| Erlang / Akka | ❌ | 実行時のみ | ❌(GC/コピー) | ❌ | ✅ |
| kompact | 部分 | ❌(Arc) | ✅ | ❌ | ✅ |
| **AetherFlow** | ✅ | ✅ | ✅ | ✅ | ✅ |

**全列 ✅ は AetherFlow のみ。** 各技術は公知だが、**この組み合わせを"ここまで"追求した例が見当たらない**。

## 2.5 総合力(complete runtime)— 売りは「一番速い」ではない

看板は **「runtime が"解決済み問題"になる」**:速さは"忘れていい"レベルで、その上に **安全・耐障害・
可観測・効率** が全部乗る。既存はどれか1つが必ず欠ける。

| 能力 | Tokio/kameo | Ractor | Pony | Erlang/Akka | Seastar/glommio | **AetherFlow** |
|---|:--:|:--:|:--:|:--:|:--:|:--:|
| 速さ(tail/throughput) | △ | △ | ○ | △ | ◎ | **◎** |
| コンパイル時安全(データ競合) | ✗ | ✗ | ◎ | ✗(実行時) | ✗ | **◎** |
| 耐障害(supervision/restart) | △ | ○ | ○ | ◎ | ✗ | **○(実装済)** |
| 可観測(組込・ゼロコスト) | △ | △ | ✗ | ○ | ✗ | **○** |
| backpressure(有界) | ○ | ○ | ○ | ○ | ○ | **○** |
| GC ポーズ無し | ✓ | ✓ | ✗ | ✗ | ✓ | **✓** |
| エルゴノミクス | ◎ | ○ | △(学習曲線) | ○ | △ | **○(shallow surface)** |

- **全行で ◎/○ は AetherFlow だけ。** Erlang/Akka が「完全」に近いが 遅い+実行時安全+GC。Seastar は
  速いが 非actor/非安全。→ **「完全 かつ 速い かつ コンパイル時安全」= 総合力の交差点。**
- **オーケストレーションで compounding**:コアで勝った性質が調整層(ゲームサーバ/AI/取引)で掛け算に効く
  ── 安全(複雑グラフでも heisenbug 無し)+ 耐障害(1つ落ちても隔離+復帰、型が隔離を保証)+ 可観測 +
  backpressure + 効率。**勝負は「3µs vs 10µs」でなく「同じ物を組んだら、どれが 安全・自己修復・可観測・効率
  を"箱から出してすぐ"揃えるか」= 総合で圧倒。**
- **正直な一線**:コアの総合力は"今 本物"。だが **「オーケストレーションで真価」は showcase を作って
  見せるまで aspirational**(参照実装未着手・distribution 凍結・実負荷未検証)。**見せる前に主張しない。**
  検証本丸 = ドメイン showcase(`direction-and-roadmap.md` §6 ドメイン showcase:ゲームサーバ sim を先に)。

## 3. 数字(アピールの実体・AWS 実 Linux, Tier 1)

| 指標 | AetherFlow | Tokio | kameo | 差 |
|---|--:|--:|--:|--:|
| ping-pong p50 | ~554 ns | ~5,835 ns | ~10,400 ns | **~10倍 / ~19倍** |
| ping-pong p999 | ~3.3 µs | ~15 µs | ~17 µs | **~4.5倍**(尾も勝つ) |
| ask p50(ゼロアロ) | **~268 ns** | — | ~10,400 ns | **~40倍** |
| throughput | ~30.4M msg/s | ~6.6M | — | **~4.6倍** |

**「速い・全分位で勝つ・そのうえコンパイル時に安全」** ── 再現可能(`core/scripts/bench-linux.sh`)。

## 4. なぜ真似されにくいか(=なぜ資産か)

- **型システムの堀**:`iso` の静的証明があるから atomic/GC 無しの最適化(ゼロアロ ask・パニック分離・
  将来の per-core プール)が**"構造的に安全"**。型を持たない競合は**安全には真似できない**(Pony 級・数年の作り直し)。
- **カテゴリの所有(narrative moat)**:「safe low-latency actors」の第一人者になる = mindshare。
- **実行の先行(execution moat)**:ピースは公開でも、全部を正しく統合するのは難しい ── 通常 12〜24ヶ月の先行。
  **その窓で普及・旗艦顧客・ブランドに変換して粘着させる**(だから "早めに" が効く)。

※ 防御は特許に依存しない(公知の組み合わせ・Apache は特許許諾込み)。
守りは **商標 + 型の堀 + 先行速度**。

## 5. メッセージング(branding が README/LP に使える形)

- **見出し**:Disruptor-fast. Compile-time-safe. Pick both.
- **サブ**:A thread-per-core Rust actor runtime — zero-copy moves, lock-free mailboxes, isolation proven
  at compile time. No locks, no GC, no atomic refcounts on the hot path.
- **証拠**:上の数字 + 4象限マトリクス(この2枚が刺さる)。
- **誠実さ**:「唯一」は "we're not aware of any runtime that combines all of these"、数字は再現手順つき。
  盛らない = 長期の信用。

## 6. 関連
- `competitive-landscape.md` — マトリクスの根拠(glommio/kompact/Pony 調査)
- `stage0-bench-notes.md` — 数字の実測(AWS 実 Linux)
- `design.md` §2.4-2.6 — 堀=型システム / deep theory, shallow surface / ドメイン現実
