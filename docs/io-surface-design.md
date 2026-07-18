# I/O 表面設計(DRAFT / PROPOSAL)— async 無しで、glommio より素直に

> 2026-07-16 ドラフト。**実装より先に「ユーザーがどう書くか」を決める**(公開=使いやすさが先)。
> 原則: **I/O as messages**(受信=メッセージ / 送信=非ブロッキング handle)。await 無し・関数の色無し・
> `Pin` 無し。run-to-completion の同期ハンドラのまま。狙い = glommio の `async fn` と並べて**同等以上の
> 書き味**。busy-poll 実装(Linux)は後。まず表面の合意を取る。

## 0. 核心の一文

ユーザーは **socket を read/write しない**。ランタイムが各コアで socket を busy-poll し、**読めたバイトを
接続 actor へのメッセージに変える**。送信は非ブロッキング handle への append(poll ループが書き出す)。
→ **ハンドラの中に await する場所がそもそも無い**(= async 追放が"我慢"でなく構造的に不要になる)。

## 1. 一番単純な形 — echo

### AetherFlow(提案)
```rust
struct Echo;

impl Connection for Echo {
    // バイトが届いた = メッセージが来た。処理して返す。それで終わり(run-to-completion)。
    fn on_data(&mut self, buf: &[u8], io: &mut Io) {
        io.write(buf);            // 送信 = 非ブロッキング append。await しない
    }
}

fn main() {
    System::with_cores(4)
        .listen("0.0.0.0:8080", || Echo)   // 接続ごとに Echo を1つ。各コアが自分の接続を busy-poll
        .run();
}
```

### glommio(対比)
```rust
async fn serve(mut stream: TcpStream) -> io::Result<()> {
    let mut buf = [0u8; 1024];
    loop {
        let n = stream.read(&mut buf).await?;   // ← await(色が伝染)
        if n == 0 { return Ok(()); }
        stream.write_all(&buf[..n]).await?;      // ← await
    }
}
```

**書き味の比較(echo)**: AetherFlow は read/write ループが無い(ランタイムが駆動)、await 無し、
色無し、接続状態 = struct のフィールド。**行数も概念も少ない。** ここは素直さで勝てる。

## 2. 接続ごとの状態 — カウンタ付き echo

```rust
struct Counting { seen: u64 }

impl Connection for Counting {
    fn on_open(&mut self, io: &mut Io) { io.write(b"hello\n"); }   // 任意
    fn on_data(&mut self, buf: &[u8], io: &mut Io) {
        self.seen += buf.len() as u64;     // 状態はフィールドに持つだけ(Arc/Mutex 不要)
        io.write(buf);
    }
    fn on_close(&mut self) { /* self.seen を集計へ送る等 */ }   // 任意
}
```
状態は actor のフィールド = 単一所有。async の「await 越しに `Arc<Mutex>`」問題が**発生しない**。

## 3. 正直な泣き所 — 多段プロトコル(length-prefixed)

「4バイトのヘッダで本文長を読み、その長さの本文を読み、応答する」。glommio は await で逐次に書ける:
```rust
let len = stream.read_u32().await?;            // 逐次に見える
let mut body = vec![0; len as usize];
stream.read_exact(&mut body).await?;
stream.write_all(&respond(&body)).await?;
```
AetherFlow の素の `on_data` は**バイトが分割で届く**ため、手で状態機械を書くことになる:
```rust
enum St { Header, Body(usize) }
struct Proto { st: St, buf: Vec<u8> }
impl Connection for Proto {
    fn on_data(&mut self, chunk: &[u8], io: &mut Io) {
        self.buf.extend_from_slice(chunk);
        loop {
            match self.st {
                St::Header if self.buf.len() >= 4 => {
                    let n = u32::from_be_bytes(self.buf[..4].try_into().unwrap()) as usize;
                    self.buf.drain(..4); self.st = St::Body(n);
                }
                St::Body(n) if self.buf.len() >= n => {
                    let body: Vec<u8> = self.buf.drain(..n).collect();
                    io.write(&respond(&body)); self.st = St::Header;
                }
                _ => break,   // 足りない。次の on_data を待つ
            }
        }
    }
}
```
**ここが async が楽な領域**(= 会話で確認した「線形の書き味」コスト)。**隠さず認める。**

### 緩和 — `Framed` アダプタ(共通プロトコルの状態機械をランタイム側に寄せる)
length-prefixed / 行区切りは**定型**なので、ランタイムが枠組みを持ち、**フレーム単位でメッセージ配送**する:
```rust
struct Proto;
impl FramedConnection for Proto {
    type Codec = LengthPrefixed;                       // or LinesCodec 等(組込)
    fn on_frame(&mut self, frame: &[u8], io: &mut Io) {
        io.write(&respond(frame));                     // 1フレーム = 1メッセージ。状態機械は消える
    }
}
```
→ **90% のプロトコルは `on_frame` で shallow surface を保てる**。手書き状態機械は生バイトが要る
上級者だけ(progressive disclosure = design.md §2.5)。**"async が楽な領域"を、定型に限りランタイムが
肩代わりする**のが答え。

## 4. backpressure — もう一つの正直な点

async は「`write().await` が詰まったら自然に待つ」でbackpressure が効く。メッセージモデルでは
**送信 handle が有界**(ランタイムの有界 mailbox 思想と一致)。満杯なら:
```rust
fn on_data(&mut self, buf: &[u8], io: &mut Io) {
    if io.write(buf).is_err() {      // 送信バッファ満杯 = 相手が遅い
        // 選択肢: 受信を一時停止(io.pause_reads())→ on_writable で再開
    }
}
fn on_writable(&mut self, io: &mut Io) { io.resume_reads(); /* 続き */ }   // 任意コールバック
```
- 既定は**単純な形**(`io.write` は満杯なら内部でバッファ、上限超で接続を落とす)。
- 厳密な flow control が要る上級者だけ `on_writable` + `pause_reads`/`resume_reads` を opt-in。
- **正直**: これは async の `await` 一発より概念が1つ多い。但し**上級 API に隔離**して基本パスには出さない。

## 5. API サマリ(表面に出す概念の総量)

| 概念 | 必須? | 対応する async の重さ |
|---|---|---|
| `Connection`(`on_data` のみ必須) | 基本 | `async fn serve` |
| `Io::write` / `Io::close` | 基本 | `stream.write().await` |
| `on_open` / `on_close` | 任意 | 接続の begin/end 手書き |
| `FramedConnection` + `on_frame` | 定型プロトコル | 逐次 `read_u32().await` 等 |
| `on_writable` / `pause_reads`(backpressure) | 上級のみ | `write().await` の暗黙backpressure |

**新概念は「接続 = actor、I/O = メッセージ」の1つだけ。** 残りは同期 Rust。色・`Pin`・await 越しの
ライフタイム/`Send` 地獄は**ゼロ**。

## 6. 主張の線引き(広く語り、鋭く証明する)

- **広く**: I/O をメッセージに一元化すれば、actor モデルは async 無しで**一貫して**書ける。これは
  Erlang が実証済みの形(`{tcp, S, Data}`)。CPU の物理(share-nothing コア + メッセージ)とも一致。
- **鋭く**: busy-poll により**低レイテンシ server slice で glommio に end-to-end で勝つ**(要 Linux 実測)。
- **正直**: 線形多段(await チェーン)は async が楽 → `Framed` で定型を肩代わり + 残りは縁で async 相互運用。

## 7. 未決(実装前に決めること)
1. `Connection` を独立 trait にするか、既存 `Actor`(Message = Io イベント)に寄せるか。→ 独立 trait 推奨
   (shallow surface。上級者は raw actor に落とせる)。
2. 送信 handle `Io` の所有形(ハンドラ引数 &mut か、actor が保持か)。→ 引数 &mut 推奨(状態を持たせない)。
3. `Framed` の組込 codec 範囲(length-prefixed / lines / まず2つ)。
4. accept したコネクションのコア割り当て(round-robin / least-conn)。
5. TLS・部分書き込み・EOF 半クローズの扱い(v1 スコープに含めるか)。

## 7.5 権威 I/O 実測(2026-07-18, AWS c7g.large / Graviton3 / 2 vCPU / aarch64)

**フェアな echo 比較** ── 同一 client(`io_bench client`)、server=core0 / client=core1 に taskset 固定、
localhost、payload 32B。AetherFlow は busy-poll(`ServeOptions{busy_poll, pin_core}`)、glommio は
io_uring + `Placement::Fixed`。接続数(conns)を振って**勝ちの slice 境界**を測った。

### 単一接続 RTT(3回・変動小)
| | p50 | p99 | p999 | throughput |
|---|--:|--:|--:|--:|
| **AetherFlow** | **9.9–10.2 µs** | **13.7–14.3 µs** | 20–22 µs¹ | **85–90k/s** |
| glommio | 16.1–16.3 µs | 19.5–19.8 µs | 21.9–22.1 µs | 60k/s |
| 比 | **~1.6×** | **~1.4×** | ~同等 | **~1.5×** |
¹ 3回中1回 p999 に 170µs のスパイク(単発外れ値)。中央値・p99 は安定。

### ⚠ 測定方法の訂正(2 vCPU → 8 vCPU)
初回スイープは **2 vCPU(server core0 / client core1)** で取り、「高並行(256)の tail は glommio 有利、
AF の tail が暴発」と結論した。**これは誤りだった。** client 256スレッドが1コアを奪い合う**client 側競合**が
tail を作っていただけで、サーバの限界ではなかった。**8 vCPU(server=core0 / client=core1–7)** で client を
枯渇させずに測り直すと、**AF の高並行 tail 暴発は消え、結論が逆転**した。以下は 8 vCPU の権威値。

### 接続数スイープ(8 vCPU, server core0 / client core1–7, 32B echo)
| conns | 指標 | **AF scan** | AF epoll | glommio |
|--:|---|--:|--:|--:|
| 1 | p50 / thru | **10.7µs** / 86.7k | 10.3µs / **91.7k** | 16.4µs / 58.5k |
| 64 | p50 / p999 / thru | **334µs / 386µs / 95.4k** | 369 / 388 / 86.6k | 386 / 407 / 82.4k |
| 256 | p50 / p999 / thru | **1350µs / 1386µs / 94.5k** | 1500 / 1530 / 85.2k | 1660 / 1698 / 76.9k |
| 512 | p50 / p999 / thru | **2676µs / 2826µs / 95.1k** | (未取得¹) | (未取得¹) |
| 1024 | p50 / p999 / thru | 5466µs / 6407µs / 92.7k | (未取得¹) | (未取得¹) |
¹ 512+ は epoll/glommio 側の計測が harness 都合で未完(conns=4096 で単一サーバコア + 素朴な bench
client が接続リセット→タイムアウト)。scan の 512/1024 は取得済。

### 読み取り(訂正後・正直に)
- **8 vCPU では AF scan が conns 1→256 で全指標(p50・p999・throughput)glommio に勝ち、tail は平ら**
  (256 で p50 1350→p999 1386µs = spread 1.03×)。前回の「高並行で tail 暴発」は**測定アーティファクト**。
- **scan が epoll にも勝つ**(中程度 conns)。サーバコアが飽和していないと epoll_wait の syscall が純粋な
  オーバーヘッドになるため。**epoll(readiness/O(ready))の理論的優位は超高 fd 数(数千)でのみ効くはず**
  だが、そこは harness 限界(client スレッド爆発・単一サーバコア飽和)で clean に測れていない = 宿題。
- epoll の価値は現状「**低 conns の tail が最も締まる**」(conns=1 p999 16.8µs、scan の 20µs 超・スパイク
  ありに対し平ら)点。高並行の決定打ではまだない。

### 勝ちの slice(訂正・確定)
- ✅ **conns 1→256(少なくとも 8 vCPU 単一サーバコアで測れた範囲)で throughput・中央値・tail すべて
  AetherFlow(scan)勝ち。** 低レイテンシ server の主戦場(取引/マーケットデータ/ゲーム tick)を広く取れる。
- ⚠ **超高並行(数千接続)の scan vs epoll vs io_uring は未決着**。ここは (a) より堅い bench harness
  (別マシン client・reset 耐性)と (b) マルチコアサーバ(thread-per-core で N コアに接続を分散)で測り直す宿題。
- **主張**:「**低レイテンシ server I/O で glommio に勝つ(測れた範囲で全指標)**」。「全 I/O 制覇」は
  超高並行を詰めるまで保留 ── だが少なくとも「高並行で必ず負ける」という前回の弱気な結論は**撤回**。

**教訓(記録)**: ベンチは client 側がボトルネックだと server の性質を測れない。2 vCPU の tail 結論は
client 競合の写像だった。**負荷生成側を必ず過剰プロビジョニングして測る**(8 vCPU で逆転した)。
コスト: 2 vCPU + 8 vCPU 実測で計 ~$0.2、都度 terminate。

## 7.6 N 対 N スケール実測(2026-07-18, AWS c7g.4xlarge / 16 vCPU / 30GB)★決定的

**thread-per-core 対決**: AetherFlow は **8 reactor + SO_REUSEPORT**(`serve_on_cores`)、glommio は
**8 executor**(`LocalExecutorPoolBuilder` + reuseport)。**server=cores 0–7 / client=cores 8–15** で
サーバとクライアントのコアを完全分離、32B echo、conns 256→8192。

| conns | 指標 | **AF scan(8)** | AF epoll(8) | glommio(8) |
|--:|---|--:|--:|--:|
| 256 | p50 / p99 / thru | **168µs / 330µs / 500k** | 167 / 387 / 482k | 208 / 418 / 466k |
| 1024 | p50 / p99 / thru | **457µs / 1180µs / 587k** | 535 / 1297 / 481k | 777 / 1192 / 479k |
| 4096 | p50 / p99 / thru | **362µs / 890µs / 643k** | 3845 / 7180 / 459k | 4237 / 4793 / 437k |
| 8192 | p50 / p99 / thru | **408µs / 2036µs / 620k** | 2188 / 5692 / 501k | 9306 / 10071 / 414k |

### 結論(決定的)
- **AF scan(thread-per-core busy-poll)が conns 256→8192 の全接続数で中央値・tail・throughput すべて
  glommio に勝ち**、しかも**差はスケールで広がる**:
  - 8192 接続で **中央値 408µs vs glommio 9306µs = ~23×**、**throughput 620k vs 414k = ~1.5×**、
    **p99 2.0ms vs 10.1ms = ~5×**。
- **前回までの「高並行 tail/スケールは readiness(io_uring)有利」という懸念は決定的に否定された。**
  thread-per-core busy-poll は**専有コアではスケールでも io_uring に勝つ**(park/wake の wake レイテンシが
  高 fan-in で効いて glommio の中央値が ms 級に膨らむ一方、busy-poll は syscall/スケジューリング無しで
  中央値を ~400µs に保つ)。これはメッセージパッシングで勝った busy-spin > park/wake と同じ構造。
- **epoll backend は高並行で退行**(4096 で 3.8ms、scan にも glommio にも負け)。→ **勝ち筋は scan
  busy-poll。epoll は不要**(low-conn tail の綺麗さ以外に利点なし)。opt-in のまま既定 scan を確定。

### 正直な但し書き
- **busy-poll は 8 コアを 100% 焼く**(負荷に依らず)。glommio は仕事量に比例。= 「レイテンシ↔CPU/電力」の
  トレードオフ(常設の但し書き)。専有コア前提の勝ち。
- localhost・各点 1 回・client も同一機(8 コア分離)。実ネットワーク・複数回・変動幅は後追い。
- glommio は既定設定 + 素直な echo 実装(専門的チューニングはしていない)。
- **主張(更新)**: 「**低レイテンシ server I/O は、少数接続から高並行(〜8192)まで、専有コアなら
  thread-per-core busy-poll で glommio に全指標勝ち**」。前回保留した「全 I/O 制覇」の高並行側は、
  **thread-per-core で決着(勝ち)**。コスト: 16 vCPU 実測で ~$0.15、終了後 terminate。

## 8. 状態と次の一歩
- **[済 2026-07-16] 表面 API + ポータブル参照バックエンドを実装**(`core/src/net.rs`、feature `net`、
  既定 OFF)。`Connection`/`Io`/`FramedConnection`/`Codec`(`LengthPrefixed`/`Lines`)/`serve`。
  macOS で compile & test 済(echo・framed lines・length-prefixed・on_open/on_close、4 テスト緑)。
  参照 reactor は nonblocking 単一スレッド = **busy-poll 性能版ではない**(API 確定用の土台)。
- **次**: (a) busy-poll reactor(各コアが socket を回す)を `#[cfg(target_os="linux")]` で実装し
  `System::listen` に統合 → (b) glommio と echo で end-to-end latency + throughput をフェア比較
  → (c) backpressure(`on_writable`/`pause_reads`)を上級 opt-in で追加。
- 実測は AWS/Latitude(macOS 不可)。設計・API は macOS で進められる(済)。
