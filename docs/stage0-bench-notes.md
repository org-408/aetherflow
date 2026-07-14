# Stage 0 ベンチ ノート

> `direction-and-roadmap.md` #6 の勝負どころ。`core/benches/latency.rs` を走らせた記録と解釈。

## ★ 権威ある実測: AWS 実 Linux(2026-07-04)= go/no-go は **GO**

**AWS `c7g.large`(Graviton3 / aarch64 / 2 vCPU / Ubuntu 24.04、ネイティブ pin が効く実 Linux。
Tier 1 = isolcpus 無しの通常起動)** で実測。ハード購入ゼロ・~$0.02。

### ping-pong RTT (ns), jitter = p99/p50
| runtime | p50 | p99 | **p999** | max | jitter |
|---|--:|--:|--:|--:|--:|
| **aether-spin** | 618 | 995 | **4,805** | 45,077 | 1.6 |
| **aether-backoff** | 554 | 728 | **3,322** | 162,646 | 1.3 |
| tokio | 5,835 | 12,343 | 15,009 | 632,243 | 2.1 |
| kameo | 11,338 | 13,387 | 17,042 | 2,548,022 | 1.2 |

### ask RTT (ns) — ゼロアロ vs kameo の per-call oneshot
| runtime | p50 | p99 | p999 | jitter |
|---|--:|--:|--:|--:|
| **aether-ask** | **268** | **392** | **399** | 1.5 |
| kameo-ask | 11,338 | 13,387 | 17,042 | 1.2 |

throughput: **aether 30.4M msg/s vs tokio 6.6M(~4.6倍)**。

### 決定的な発見
- **tail が完全に締まった**: p999 が macOS/Docker の **2.5ms → 3.3〜4.8µs(~500倍改善)**。
  busy-spin の tail 爆発は**まるごと仮想化アーティファクト**だったと確定(仮説どおり)。
  **しかも Tier 1(isolcpus 無し)ですらこれ。**
- **全分位で圧勝**: aether は tokio に対し median ~10倍・p99 ~13倍・**p999 ~3〜4.5倍**、jitter も上(1.3 vs 2.1)。
  macOS では tail 同着・jitter 劣勢だったのが、実 Linux で**全部ひっくり返って優位**。
- **ゼロアロ ask が異常に強い**: **p50 268ns / p999 399ns**(サブ µs、尾がほぼ平ら)。
  kameo ask(heap 確保)の **~42倍**速く、tail は ~43倍締まる。所有権モデルが解禁した速度の実証。
- **exchange-core(p99.9 22µs)の領域に既に足がかかっている**(aether-backoff p999 3.3µs、別ワークロードだが桁は同等以下)。

### 含意
- **thesis は実ハードで本物**。competitive-landscape の「勝ちが十分大きいか」= 倍数で明確にクリア。
- **Tier 1(普通のクラウド)で既にこの数字** → 「クラウドで安全なまま Tokio/kameo に全分位圧勝」の
  マーケ看板が成立。**cloud-first で戦える。**
- 次は **Tier 2(c7g.metal + isolcpus)** で tail を中央値へさらに潰し、HFT 級(単桁µs p99.9)の天井を見る。
- 但し書き: aether はビジースピン(低レイテンシ↔CPU)。単一 actor マイクロベンチ。実ワークロード
  (fan-out・多対多・大メッセージ)と HoL blocking は別途。

---

## 予備結果(macOS, Apple Silicon 16 論理コア, no-pin, release+LTO)

## 予備結果(macOS, Apple Silicon 16 論理コア, no-pin, release+LTO)

`cargo bench --bench latency`、2 回の代表値。ping-pong は 1 往復 RTT(ns)、スループットは
単一 producer→単一 consumer の msgs/sec。

### ping-pong RTT (ns), jitter = p99/p50
| runtime | p50 | p90 | p99 | p999 | max | jitter |
|---|--:|--:|--:|--:|--:|--:|
| **aether**(thread-per-core) | ~3,000 | ~3,200 | ~26,000 | ~66,000 | 跳ねる(0.1–2.7M) | ~9.0 |
| tokio 素チャネル(work-stealing) | ~9,600 | ~11,000 | ~34,000 | ~57,000 | 跳ねる | ~3.6 |
| kameo(実 actor FW、Tokio 上) | ~10,400 | ~11,900 | ~36,000 | ~62,000 | 跳ねる | ~3.5 |

### one-way throughput (msgs/sec)
| runtime | throughput |
|---|--:|
| **aether** | ~26.7M |
| tokio | ~6.9M |

## 解釈(正直に)

- **絶対値では aether が p50/p90/p99 すべてで Tokio と kameo を上回る**(p50 は tokio の ~3.2倍、
  **実 actor フレームワーク kameo の ~3.6倍**、p99 は 26µs vs 34–36µs)。**スループットは Tokio 素
  チャネルの約 3.9 倍**。「5% じゃなく倍数」= competitive-landscape の「勝ちが十分大きいか」バーは、
  この範囲では**予備的にクリア**。kameo ≈ tokio(フレームワークぶん少し遅い、想定どおり)。
- **ただし極端な尾(p999)は macOS では同着(~60µs)**、max は両者跳ねる。
  → 予測どおり: **ハードピン留めが無いと OS がスレッドを移してキャッシュが冷え、尾が跳ねる**。
- **jitter 比(p99/p50)は aether の方が悪い(~8.5 vs ~3.5)**。これは aether の中央値が極端に低い
  ぶん相対的な広がりが大きく見えるため(絶対 p99 は aether が勝っている)。だが「tail の**安定**こそが
  売り物」である以上、**相対 jitter が締まらないのは macOS では弱点**。matching engine が金を払う
  p99/p99.9 の**保証**は、**この環境では未証明**。

### macOS QoS の効果(2026-07-04 追試)— この機体は Intel Mac
`pinning` が macOS で `USER_INTERACTIVE` QoS を要求するよう実装。ただし **本機は Intel Mac で
P/E コアが無い**ため、QoS は「P コアに寄せる」効果を持たない(無害だが尾は締まらない)。
→ **尾は目立って締まらず**(想定どおり)。中央値/スループットの勝ちは維持。QoS が効くのは
Apple Silicon。**尾の改善はハードピン留め(=隔離コア実機 Linux)が要る**という結論は不変。

## ゼロアロ ask(request-reply)の対決(2026-07-04, 堀第一号)

`ask` = send + 返事待ち。kameo/tokio は**呼び出しごとに reply チャネル(oneshot)を heap 確保**する。
うちは reply cell を**呼び出しスタックに置き**、`Responder`(一度だけ返信する線形トークン=iso 的)に
生ポインタで渡す(`core/src/ask.rs`)= **heap 確保ゼロ**。

| runtime | p50 | p99 | jitter(p99/p50) |
|---|--:|--:|--:|
| **aether-ask**(ゼロアロ) | **~1,000 ns** | **~1,500 ns** | **~1.5** |
| kameo-ask(毎回 oneshot 確保) | ~10,000–96,000 ns | 崩壊(10ms 級) | 4〜105 |

- **median ~10〜90倍速い**(この計測回はマシンが並行負荷下で kameo が特に崩れたため上振れ。idle でも
  kameo ask は ~10µs なので**保守的に見ても ~10倍**)。
- **jitter が桁違いに安定**(1.5 vs 4〜105)。aether-ask はスタック cell への tight spin なので、
  負荷下でも ~1µs を保つ(std sync_channel を使う tell-pingpong 計測より安定・高速ですらある)。
- **意味**: 「型/所有権モデルが、フレームワークより速い ask を**構造的に**生む」の実証。しかも API は
  `addr.ask(|resp| Msg(resp))?` と普通(deep theory, shallow surface)。**堀=型システムの最初の
  具体的成果物**(design.md §2.4)。

## 条件の但し書き(過大評価しないため)

- **aether の消費者はビジースピン**(LMAX 流)。低レイテンシと引き換えにアイドルでも 1 コアを
  焼く。Tokio は park/wake で CPU を使わない。**同条件の比較ではない** → 「レイテンシ↔CPU/電力」の
  トレードオフとして読む。将来、park オプション(スピン→短スリープ)も要検討。
- 単一 actor・単一 producer のマイクロベンチ。実ワークロード(fan-out, 多対多, 大きいメッセージ)は別。
- macOS はハードピン留め非対応(no-op)。**ARM の問題ではなく OS の問題**(ARM Linux/Graviton は pin 可)。

## Linux コンテナ実測(Docker on Mac, ARM64 VM, cpuset 0-3 = 4コア, 2026-07-04)

`./core/scripts/bench-linux.sh` の実測。**median は macOS より差が拡大、しかし tail は爆発**という
重要な結果。

### ping-pong RTT (ns)
| runtime | p50 | p90 | p99 | p999 | max | jitter |
|---|--:|--:|--:|--:|--:|--:|
| **aether** | ~7,900 | ~8,600 | ~34,000 | **~2,540,000** | ~4,150,000 | 4.3 |
| tokio | ~112,000 | ~174,000 | ~277,000 | ~402,000 | ~22M | 2.5 |
| kameo | ~114,000 | ~174,000 | ~275,000 | ~381,000 | ~29M | 2.4 |

throughput: aether ~21.7M msg/s vs tokio ~5.9M。

### 解釈(重要・一部は不都合)
- **median で ~14倍、p99 で ~8倍** aether が速い。コアが少ない(競合が強い)ほど thread-per-core の
  優位が広がる = work-stealing が競合下で苦しむのを裏取り。**median/throughput の勝ちは Linux で更に強い**。
- **だが p999 = 2.5ms、max = 4.1ms と tail が爆発**(tokio p999 0.4ms の ~6倍悪い)。
  → 原因: **busy-spin は諸刃の剣**。仮想化(Docker=軽量 VM)環境ではハイパーバイザがスピン中の
  コアスレッドを ms 単位で preempt する瞬間があり、park/wake が無いぶん復帰が遅れて tail が跳ねる。
  ベアメタルの**隔離コア**(isolcpus/nohz_full、オーバーサブスクリプション無し)なら起きにくいが、
  **Mac の Docker VM はその条件を満たさない**。
- **教訓2つ**: (a) 権威ある tail はやはり**隔離コアの実機**が要る(Docker VM でも不十分)。
  (b) **純 busy-spin は堅牢でない** → spin→yield→短 park の**ハイブリッド idle 戦略**が要る。

### ハイブリッド idle 戦略の効果(`IdleStrategy::Backoff`, 同コンテナ実測)
`spin 128 → yield 128 → park 50µs` を実装し、busy-spin と比較。

| runtime | p50 | p90 | p99 | p999 | max |
|---|--:|--:|--:|--:|--:|
| aether-**spin** | ~7,900 | ~9,600 | ~37,000 | **~2,580,000** | ~4,300,000 |
| aether-**backoff** | ~8,700 | ~17,000 | ~49,000 | **~128,000** | ~1,440,000 |
| tokio | ~123,000 | ~142,000 | ~218,000 | ~393,000 | ~25M |
| kameo | ~144,000 | ~175,000 | ~242,000 | ~443,000 | ~19M |

- **backoff で p999 が 20倍改善(2.6ms → 128µs)**。代償は median +10%・p99 +34% のみ。
- **backoff は tokio/kameo を全分位で上回る**(median ~14倍、p99 ~4倍、**p999 も ~3倍**)。
  → 仮想化下でも「速さ」と「tail の堅牢さ」を両立できる。**busy-spin は隔離コア専有時、backoff は
  共有/仮想化時、と使い分ける**設計にした(既定は latency 優先の BusySpin)。
- 依然、**絶対 p999(128µs〜)は matching engine の目標(p99.9 22µs)には届かない**。これはコンテナ
  仮想化のジッタ由来が大きく、**隔離コア実機での測り直しが必要**という結論は変わらない。

## Mac で tail を体感するには(Docker 経由)

macOS ネイティブでは尾が締まらないが、**Mac 上の Linux コンテナなら `sched_setaffinity` が効く**。
Docker Desktop を起動して:

```sh
cd core && ./scripts/bench-linux.sh
```

`rust:slim` コンテナ内で同じ `cargo bench` を走らせる(専有 cpuset 付き)。コンテナ CPU は
仮想化されているので bare-metal ほど正確ではないが、**macOS ネイティブより実 Linux の挙動に近い** —
ここで p999 にピン留めの効果が出れば、差別化の核が(暫定的に)見える。権威ある数字は実機/VM で。

## 次(権威ある Stage 0)

1. **Linux で取り直す**(理想は ARM Graviton の安い VM)。`core_affinity` が OS 別に分岐済みなので
   同じ `cargo bench` でよい。ここで **tail(p99/p99.9)にピン留めの効果が出るか**が本番の問い。
   出れば差別化の核が実証、出なければ「うちの追加価値は tail 保証ではない」と判明 → 戦略見直し。
2. **競合を足す**: kameo/actix(足場)→ **kompact**(latency で挑む)→ **glommio+薄 actor**
   (delta の本丸)。手法は kompicsbenches を流用。
3. **金融 North Star**: order→match で p99 4µs / p99.9 22µs(exchange-core)に対する立ち位置。
4. **jitter を明示計測**(p999/p50 比 など)。tail の安定こそが売り物。

## 関連
- `competitive-landscape.md` — ベンチの的(相手・軸・North Star)
- `core/benches/latency.rs` — 測定コード
- `core/README.md` — ランタイム構成と既知の制約
