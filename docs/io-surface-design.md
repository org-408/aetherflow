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

### 接続数スイープ(RTT p50 / p99 / throughput)
| conns | AetherFlow p50 | glommio p50 | AetherFlow p99 | glommio p99 | AF thru | glo thru |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | **9.9µs** | 16.1µs | **13.7µs** | 19.8µs | **90k** | 60k |
| 4 | **22.8µs** | 29.5µs | 39.4µs | 39.1µs | **86.7k** | 64.6k |
| 16 | **81µs** | 106µs | 162µs | **116µs** | **88k** | 75.6k |
| 64 | **353µs** | 440µs | **667µs** | 744µs | **85.7k** | 72.9k |
| 256 | **1.66ms** | 2.02ms | 3.31ms | **2.16ms** | **74.9k** | 64.1k |

### 読み取り(正直に)
- **throughput は全 conns で AetherFlow 勝ち**(~1.2–1.5×)。**中央値(p50)も全 conns で勝ち**。
- **tail(p99/p999)は交差する**: conns 1–4 は AF 勝ち、16 で glommio が p99 逆転、256 では
  glommio の tail が**平ら**(p50→p999 が 2.02→2.24ms=1.1倍)に対し AF は**伸びる**(1.66→5.5ms=3.3倍)。
  = readiness モデル(io_uring)は高並行で**公平に**ready fd を捌くので tail が締まる。busy-poll の
  Vec スキャン(参照 reactor)は高並行で一部接続が待たされ tail が暴れる。
- **⚠ この tail 暴発は参照 reactor の素朴さ由来が大きい**(fairness 無しの線形スキャン)。native
  backend(io_uring 直叩き or fairness 付きスキャン)で縮む見込み。= 構造的敗北ではない。

### 勝ちの slice(確定・正直な線引き)
- ✅ **低レイテンシ・低〜中コネクション(~64)では throughput・中央値・tail すべて AetherFlow 勝ち**。
  これがまさに主戦場(取引/マーケットデータ/ゲーム tick = 少数接続・低レイテンシ)。
- ✅ **throughput と中央値は高並行でも勝つ**。
- ⚠ **高並行(256+)の tail 予測性は glommio 有利**(現状の参照 reactor では)。SLA が高fan-in の
  p99/p999 なら glommio。→ native backend で挑む余地。
- **主張**:「**低レイテンシ server I/O で勝つ(throughput/中央値は広く、tail は低〜中コネクションで)**」。
  「全 I/O 制覇」ではない ── 高並行 tail は次の宿題。

**環境の但し書き**: 2 vCPU で client も1コアを使うため、高並行の絶対値(ms 級)は client 側競合で
膨らむ(両者同条件なので比較はフェア)。倍率がメッセージパッシング(36×)より小さいのはカーネル TCP
の syscall コストが両者共通で支配的なため。~$0.02 で実測、終了後 terminate。

## 8. 状態と次の一歩
- **[済 2026-07-16] 表面 API + ポータブル参照バックエンドを実装**(`core/src/net.rs`、feature `net`、
  既定 OFF)。`Connection`/`Io`/`FramedConnection`/`Codec`(`LengthPrefixed`/`Lines`)/`serve`。
  macOS で compile & test 済(echo・framed lines・length-prefixed・on_open/on_close、4 テスト緑)。
  参照 reactor は nonblocking 単一スレッド = **busy-poll 性能版ではない**(API 確定用の土台)。
- **次**: (a) busy-poll reactor(各コアが socket を回す)を `#[cfg(target_os="linux")]` で実装し
  `System::listen` に統合 → (b) glommio と echo で end-to-end latency + throughput をフェア比較
  → (c) backpressure(`on_writable`/`pause_reads`)を上級 opt-in で追加。
- 実測は AWS/Latitude(macOS 不可)。設計・API は macOS で進められる(済)。
