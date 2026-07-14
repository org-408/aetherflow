# AetherFlow Design Document

## 1. 核心の主張(Thesis)

Actorモデルの「隔離(isolation)」を、Akka/BEAMのような**実行時の規約**としてではなく、Rustの型システムを使って**コンパイル時に保証**し、さらにそれを**物理CPUトポロジ(コア・キャッシュ・NUMA)と一体設計**することで、既存のactorランタイムが構造的に到達できない領域(ロックフリー・コピーゼロ・GCポーズゼロ・キャッシュ局所性保証)に踏み込む。

## 2. 理論的な先行研究

### 2.1 Actorモデルの原典
Carl Hewitt (1973) の Actor Model — 「actor = 計算の普遍的プリミティブ、メッセージ受信・新actor生成・次の振る舞い決定のみで並行計算を記述する」という原点。Akka/BEAMはこの実装だが、**隔離の保証方法を明記していない**(実行時の規律に依存)。

### 2.2 決定的な先行研究: Pony言語
「型システムでactorの隔離をコンパイル時に保証する」というアイデア自体は、**既に学術的に実現されている**。Sylvan Clebsch らの Pony言語(Imperial College London発)がまさにそれで、論文 *"Deny Capabilities for Safe, Fast Actors"* (AGERE 2015) が理論的支柱。

- Ponyは **reference capabilities** (`iso`, `val`, `ref`, `box`, `trn`, `tag`) という型修飾子で、「このデータは一意所有か、不変か、actorローカルか」を型レベルで表現し、データ競合をコンパイル時に排除
- GCも「actorごとに独立した並行GC」(stop-the-worldなし)を実現している

→ **これを知らずに再発明するのは車輪の再発明になるので、Ponyの reference capabilities 理論を土台にする/差分を明確にするのが正しい出発点**。RustのownershipはPonyのreference capabilitiesと似て非なるもの(Rustは所有権+借用、Ponyはcapability格子)なので、「Rustの型システムでPony相当のことがどこまで再現できるか」を最初に検証すべき。

### 2.3 Ponyに無くて、この構想にある差分
Ponyのランタイムは自前で、**CPUトポロジ・キャッシュライン・NUMAを意識した設計にはなっていない**(汎用スケジューラ)。ここがオリジナリティの余地:

- **Mechanical Sympathy** (Martin Thompson, LMAX Disruptor): ロックフリーリングバッファ、シングルライター原則、キャッシュライン意識したデータレイアウトで、金融取引システムなどで実証済みの思想
- **Work-stealing vs 静的配置**: Blumofe & Leiserson (1999) の work-stealing 理論は「負荷分散」に強いが「キャッシュ局所性」を犠牲にする。actorをコアに固定する静的配置は逆のトレードオフ
- NUMA-aware scheduling の研究群(データを触るCPUの近くに置く)

### 2.4 堀(moat)の在り処 — 差別化は「速さ」ではなく「型が速さを解禁すること」(2026-07 追記)

競合調査(`competitive-landscape.md`)と Stage 0 実測で分かった、より鋭い差別化の定義:

- **性能の機構(バッチ・per-core プール・emplace 等)は堀にならない。** 数字を出した瞬間に誰でも
  1 リリースで真似できる treadmill。「速いだけ」では追いつかれる。
- **堀は型システム。** AetherFlow が `iso`(単一所有)を**コンパイル時に証明**するなら、atomic も GC も
  無いアグレッシブな最適化が**構造的に安全**になる。型システムを持たない競合(glommio=共有を footgun
  可能 / kompact=Arc)は、その最適化を**安全には真似できない** → 真似するには型システムごと作る羽目に
  なる = Pony 級・数年がかりの高コスト部分。**「安全が速さを解禁する」フライホイールを回せるのは
  型を持つ側だけ。**
- **前回の訂正**: 「capability は性能中立」は単体での話。真価は「**他者が安全には真似できない最適化の
  解禁**」にある。
- **定理型の差別化**: 「静的データ競合フリー + zero-GC(ポーズ無し)+ メッセージ毎の Arc/refcount 無し
  (mailbox 自体は lock-free で atomic は使う)」を **全部同時に持つのはうちだけ**。Pony はデータ競合
  フリーだが GC を払う。Rust actor 勢は両方無い。
  ベンチではなく**証明可能な性質**として掲げられる。
- **型が解禁した安全性(実例)**: **パニック分離** — actor 状態が単一所有(`ref`、共有なし)と型が
  保証するので、handle のパニックはその actor だけを安全に切り離して続行できる。`Arc<Mutex>` 系は
  パニックで Mutex が poison して続行不能。「隔離が健全性を解禁する」= 共有状態システムに真似できない。
  同様に **ゼロアロ ask**(スタック reply cell + 線形 `Responder`)は所有権モデルが解禁した速度。
  どちらも「安全/速さは型の帰結」の具体例。
- **Pony が止めた所から先へ**: Pony は capability を証明したが GC と汎用スケジューラに縛った。
  最前線 = **capability を GC 無しで(決定的・プール型)+ CPU トポロジ一体で**。未占有の理論領域。

**規律**: 堀は型で掘る。ただし「性能フロンティアが到達可能」と分かる程度に**測ってから**深掘りする
(象牙の塔=誰も欲しがらない城を守る堀、を避ける)。競合は"バーを知る"ために使い、"追う"のはやめる。

### 2.5 設計制約: Deep theory, shallow surface(2026-07 追記)

堀を深く掘る(§2.4)ことと、使いやすさは**両立させねばならない**。理論を**ユーザーの表面に出すほど**
学習コストが上がり離れる ── **Pony が理論的に美しいのに niche に留まった、まさにその理由**。

- **原則**: 理論の追加は「保証か性能」で必ず元を取る。**ただしユーザーが基本パスを使うのに学ぶ
  新概念/新注釈を増やしてはならない。** 増やすなら隠すか、捨てる。
- **実現**: `capability-mapping` の結論どおり、load-bearing なカプは**注釈なし・構造的に** Rust で
  再現できる(iso=move, val=`Arc<Sync>`, ref=`&mut self`, tag=`ActorRef`)。ユーザーは**普通の Rust**を
  書き、保証はタダで付く。型システムは床下で働き、ユーザーには「危ないコードは通らない、そして速い」
  としか見えない。
- **例**: per-core メッセージプールは**ランタイムが iso メッセージを透過的にプール**する(ユーザーは
  何もしない)= 堀のまま無税。「この actor はレイテンシ重要」等の上級ノブは **opt-in の上級 API** に
  隔離し、基本パスには出さない(progressive disclosure)。
- **標語**: **堀=床下の型システム(深く掘る)/ 表面=普通の Rust(浅く保つ)。** 理論の仕事は
  「コンパイラに危険を弾かせ、速いパスを解禁する」であって「ユーザーに capability 計算を教える」ではない。

### 2.6 ドメインの現実(2026-07 追記)

`competitive-landscape.md` の finance 分析の補足。**22µs(exchange-core p99.9)は"製品ライバル"では
なく"信頼のバー"と"自作という慣行"**:

- トレーディングのホットパスは**基本フレームワークを使わず自作**(自前 Disruptor/Aeron/Chronicle)。
- さらに下(サブ µs)は**カーネルバイパス/FPGA** の領域で、**software ランタイムは原理的に届かない**。
- → **最速エリート HFT は顧客ではない**(自作 or ハード)。狙えるのは「**Disruptor 級の速さ + 安全 +
  生産性が欲しいが、自作チームも FPGA も持てない第二階層**」(取引所/ブローカー/リアルタイムリスク/
  マーケットデータ/広告 RTB)。value = 「HFT チーム無しで Disruptor 級に近づける安全な道」。

## 3. 技術的な柱(4本)

1. **型による隔離**: Pony の reference capabilities 理論をベースに、Rustのownership/Send+Syncで代替可能な範囲を型システムに落とし込む
2. **コピーゼロのメッセージ移動**: `move`セマンティクスでメッセージ送信=所有権移譲、コンパイラが「送信元はもう触れない」を保証(Akkaは規約でコピーするが、これは所有権で不要にできる)
3. **物理配置の一級化**: actorを論理概念で終わらせず、コア単位の`tokio::task::LocalSet`(またはスケジューラ自作)に静的にピン留め、mailboxはコアを跨がないSPSCリングバッファ
4. **Tokioとの緊張関係の明示的解決**: デフォルトのwork-stealingスケジューラは使わず、コア数だけシングルスレッドランタイムを立てて自前ルーティングする設計

Tokioのデフォルトのmulti-thread schedulerはwork-stealing(暇なワーカースレッドが他のスレッドのタスクを奪って実行する)前提で、ロードバランスのためにわざとタスクをコア間で動かす設計。これは「actorを特定のコアに固定してキャッシュ局所性を活かす」という発想と真っ向から矛盾する。Tokioの一部だけを部品として使い、スケジューリングの主導権は握る必要がある。

## 4. 最初の検証実験(沼を避けるための最小スコープ)

「Akkaなら気づかず壊れるが、この設計ならコンパイルが通らない」ようなコード例を1つ作る前に、まず**Ponyの論文とreference capabilitiesの実装をRustでどこまで模倣できるか**を1週間くらいで検証する。ここがそもそも成立しなければ土台から崩れるので、最優先で潰すべきリスク。

## 5. 読むべき文献リスト
- Clebsch et al., *"Deny Capabilities for Safe, Fast Actors"* (AGERE 2015)
- Clebsch & Drossopoulou, *"Fully Concurrent Garbage Collection of Actors on Many-Core Machines"* (OOPSLA 2013)
- Hewitt, Bishop, Steiger, *"A Universal Modular Actor Formalism for Artificial Intelligence"* (1973)
- Blumofe & Leiserson, *"Scheduling Multithreaded Computations by Work Stealing"* (1999)
- Martin Thompson, "Mechanical Sympathy" ブログ + LMAX Disruptor 技術論文

## 6. 関連資料

- `docs/actor-model-theoretical-concepts.md` — Hewittのactorモデルの数式による形式化、Thread Pool/Scheduler/Runtime(rayon, tokio, async-std)の実装論点(2024-08、org-408/docsより移設)
- `docs/akka-analyze.md` — Akka内部構造(ActorRef→mailbox→Dispatcher、Supervisor階層、LifeCycleフック)の詳細分析(2024-08、org-408/docsより移設)

## 7. 過去の経緯(2024-10)

2024年10月に `aether` / `aioncore` / `aion_core` という別リポジトリでも同系統の実験を行っていたが、いずれもこのリポジトリ(`aetherflow`)ほど実装が進んでおらず、コンセプトの重複と判断して2026-07に削除・本リポジトリに一本化した。
