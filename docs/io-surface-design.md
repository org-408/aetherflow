# I/O Surface Design (DRAFT / PROPOSAL) — more natural than glommio, without async

> 🌐 English | [日本語](io-surface-design.ja.md)


> 2026-07-16 draft. **Decide "how the user writes it" before the implementation** (public = usability comes first).
> Principle: **I/O as messages** (receive = message / send = non-blocking handle). No await, no function coloring,
> no `Pin`. Stays as run-to-completion synchronous handlers. Goal = **an ergonomic experience equal to or better than**
> glommio's `async fn`. The busy-poll implementation (Linux) comes later. First, get agreement on the surface.

## 0. The core in one sentence

The user **does not read/write the socket**. The runtime busy-polls the socket on each core, and **turns the bytes it
reads into messages to the connection actor**. Sending is an append to a non-blocking handle (the poll loop writes it out).
→ **There is simply no place inside a handler to await** (= banishing async becomes structurally unnecessary rather than a "sacrifice").

## 1. The simplest form — echo

### AetherFlow (proposal)
```rust
struct Echo;

impl Connection for Echo {
    // Bytes arrived = a message came in. Process it and reply. That's all (run-to-completion).
    fn on_data(&mut self, buf: &[u8], io: &mut Io) {
        io.write(buf);            // send = non-blocking append. no await
    }
}

fn main() {
    System::with_cores(4)
        .listen("0.0.0.0:8080", || Echo)   // one Echo per connection. each core busy-polls its own connections
        .run();
}
```

### glommio (for contrast)
```rust
async fn serve(mut stream: TcpStream) -> io::Result<()> {
    let mut buf = [0u8; 1024];
    loop {
        let n = stream.read(&mut buf).await?;   // ← await (color spreads)
        if n == 0 { return Ok(()); }
        stream.write_all(&buf[..n]).await?;      // ← await
    }
}
```

**Ergonomics comparison (echo)**: AetherFlow has no read/write loop (the runtime drives it), no await,
no coloring, connection state = struct fields. **Fewer lines and fewer concepts.** Here we win on plainness.

## 2. Per-connection state — echo with a counter

```rust
struct Counting { seen: u64 }

impl Connection for Counting {
    fn on_open(&mut self, io: &mut Io) { io.write(b"hello\n"); }   // optional
    fn on_data(&mut self, buf: &[u8], io: &mut Io) {
        self.seen += buf.len() as u64;     // state just lives in a field (no Arc/Mutex needed)
        io.write(buf);
    }
    fn on_close(&mut self) { /* e.g. send self.seen off to be aggregated */ }   // optional
}
```
State is a field of the actor = single ownership. The async "`Arc<Mutex>` across an await" problem **does not arise**.

## 3. The honest sore spot — multi-stage protocols (length-prefixed)

"Read a 4-byte header for the body length, read a body of that length, then respond." glommio can write it
sequentially with await:
```rust
let len = stream.read_u32().await?;            // reads sequentially
let mut body = vec![0; len as usize];
stream.read_exact(&mut body).await?;
stream.write_all(&respond(&body)).await?;
```
Because AetherFlow's bare `on_data` **receives bytes in fragments**, you end up writing the state machine by hand:
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
                _ => break,   // not enough. wait for the next on_data
            }
        }
    }
}
```
**This is the territory where async is easy** (= the "linear ergonomics" cost we confirmed in discussion). **We admit it, we don't hide it.**

### Mitigation — the `Framed` adapter (push the common-protocol state machine into the runtime)
length-prefixed / line-delimited are **boilerplate**, so the runtime holds the framing and **delivers messages per frame**:
```rust
struct Proto;
impl FramedConnection for Proto {
    type Codec = LengthPrefixed;                       // or LinesCodec, etc. (built-in)
    fn on_frame(&mut self, frame: &[u8], io: &mut Io) {
        io.write(&respond(frame));                     // 1 frame = 1 message. the state machine disappears
    }
}
```
→ **90% of protocols can keep a shallow surface with `on_frame`.** The hand-written state machine is only for advanced
users who need raw bytes (progressive disclosure = design.md §2.5). The answer is to **have the runtime take over the
"territory where async is easy," limited to boilerplate.**

## 4. backpressure — another honest point

With async, backpressure works because "if `write().await` stalls, it naturally waits." In the message model,
**the send handle is bounded** (consistent with the runtime's bounded-mailbox philosophy). When full:
```rust
fn on_data(&mut self, buf: &[u8], io: &mut Io) {
    if io.write(buf).is_err() {      // send buffer full = the peer is slow
        // options: pause reads temporarily (io.pause_reads()) → resume on on_writable
    }
}
fn on_writable(&mut self, io: &mut Io) { io.resume_reads(); /* continue */ }   // optional callback
```
- The default is the **simple form** (`io.write` buffers internally when full, and drops the connection if it exceeds the cap).
- Only advanced users who need strict flow control opt in to `on_writable` + `pause_reads`/`resume_reads`.
- **Honest**: this is one concept more than a single async `await`. But it is **isolated to the advanced API** and kept off the basic path.

## 5. API summary (total amount of concepts exposed on the surface)

| Concept | Required? | Corresponding async weight |
|---|---|---|
| `Connection` (only `on_data` required) | basic | `async fn serve` |
| `Io::write` / `Io::close` | basic | `stream.write().await` |
| `on_open` / `on_close` | optional | hand-written connection begin/end |
| `FramedConnection` + `on_frame` | boilerplate protocols | sequential `read_u32().await`, etc. |
| `on_writable` / `pause_reads` (backpressure) | advanced only | implicit backpressure of `write().await` |

**The only new concept is "connection = actor, I/O = message."** The rest is synchronous Rust. Coloring, `Pin`, and the
lifetime/`Send` hell across await are **zero**.

## 6. Drawing the line on our claims (speak broadly, prove sharply)

- **Broadly**: if you unify I/O into messages, the actor model can be written **consistently** without async. This is
  the form Erlang already proved (`{tcp, S, Data}`). It also matches the physics of the CPU (share-nothing cores + messages).
- **Sharply**: with busy-poll we **win end-to-end against glommio on the low-latency server slice** (requires Linux measurement).
- **Honestly**: linear multi-stage (await chains) are easier with async → `Framed` takes over the boilerplate + the rest interoperates with async at the edge.

## 7. Open questions (to decide before implementation)
1. Make `Connection` a standalone trait, or fold it into the existing `Actor` (Message = Io event)? → standalone trait recommended
   (shallow surface; advanced users can drop down to the raw actor).
2. The ownership form of the send handle `Io` (a `&mut` handler argument, or held by the actor)? → `&mut` argument recommended (keeps no state).
3. The scope of `Framed` built-in codecs (length-prefixed / lines / start with these two).
4. Core assignment for accepted connections (round-robin / least-conn).
5. Handling of TLS, partial writes, and EOF half-close (include in the v1 scope?).

## 7.5 Authoritative I/O measurements (2026-07-18, AWS c7g.large / Graviton3 / 2 vCPU / aarch64)

**Fair echo comparison** ── same client (`io_bench client`), server=core0 / client=core1 pinned with taskset,
localhost, payload 32B. AetherFlow uses busy-poll (`ServeOptions{busy_poll, pin_core}`), glommio uses
io_uring + `Placement::Fixed`. We swept the connection count (conns) to measure **the boundary of the winning slice**.

### Single-connection RTT (3 runs, low variance)
| | p50 | p99 | p999 | throughput |
|---|--:|--:|--:|--:|
| **AetherFlow** | **9.9–10.2 µs** | **13.7–14.3 µs** | 20–22 µs¹ | **85–90k/s** |
| glommio | 16.1–16.3 µs | 19.5–19.8 µs | 21.9–22.1 µs | 60k/s |
| ratio | **~1.6×** | **~1.4×** | ~equal | **~1.5×** |
¹ In 1 of the 3 runs, a 170µs spike in p999 (a one-off outlier). The median and p99 are stable.

### ⚠ Correction to the methodology (2 vCPU → 8 vCPU)
The first sweep was taken on **2 vCPU (server core0 / client core1)** and concluded that "at high concurrency (256) the
tail favors glommio, and AF's tail blows up." **This was wrong.** It was just **client-side contention** — 256 client
threads fighting over one core — that produced the tail; it was not a server limit. Re-measuring on **8 vCPU (server=core0 /
client=core1–7)** without starving the client, **AF's high-concurrency tail blow-up disappeared, and the conclusion reversed.**
The following are the authoritative 8 vCPU values.

### Connection-count sweep (8 vCPU, server core0 / client core1–7, 32B echo)
| conns | metric | **AF scan** | AF epoll | glommio |
|--:|---|--:|--:|--:|
| 1 | p50 / thru | **10.7µs** / 86.7k | 10.3µs / **91.7k** | 16.4µs / 58.5k |
| 64 | p50 / p999 / thru | **334µs / 386µs / 95.4k** | 369 / 388 / 86.6k | 386 / 407 / 82.4k |
| 256 | p50 / p999 / thru | **1350µs / 1386µs / 94.5k** | 1500 / 1530 / 85.2k | 1660 / 1698 / 76.9k |
| 512 | p50 / p999 / thru | **2676µs / 2826µs / 95.1k** | (not taken¹) | (not taken¹) |
| 1024 | p50 / p999 / thru | 5466µs / 6407µs / 92.7k | (not taken¹) | (not taken¹) |
¹ 512+ on the epoll/glommio side is incomplete due to harness circumstances (at conns=4096 a single server core + a naive bench
client causes connection resets → timeouts). scan's 512/1024 have been taken.

### Reading (after correction, honestly)
- **On 8 vCPU, AF scan wins against glommio on every metric (p50, p999, throughput) from conns 1→256, and the tail is flat**
  (at 256, p50 1350 → p999 1386µs = spread 1.03×). The earlier "tail blows up at high concurrency" was a **measurement artifact**.
- **scan also beats epoll** (at moderate conns). Because when the server core is not saturated, the epoll_wait syscall becomes pure
  overhead. **The theoretical advantage of epoll (readiness / O(ready)) should only pay off at very high fd counts (thousands)**,
  but that regime can't be cleanly measured due to harness limits (client-thread explosion / single-server-core saturation) = homework.
- The value of epoll is currently that "**the tail at low conns is the tightest**" (conns=1 p999 16.8µs, flat versus scan's >20µs
  with spikes). It is not yet a decisive factor at high concurrency.

### The winning slice (corrected, confirmed)
- ✅ **From conns 1→256 (at least within the range measurable on 8 vCPU with a single server core), throughput, median, and tail are
  all wins for AetherFlow (scan).** We can broadly claim the main battleground of low-latency servers (trading / market data / game ticks).
- ⚠ **scan vs epoll vs io_uring at very high concurrency (thousands of connections) is undecided.** This is homework to re-measure with
  (a) a sturdier bench harness (client on a separate machine, reset-resistant) and (b) a multi-core server (thread-per-core spreading connections across N cores).
- **Claim**: "**We win against glommio on low-latency server I/O (all metrics, within the measured range).**" "Total I/O domination"
  is on hold until very high concurrency is nailed down ── but at least the earlier timid conclusion that "we will necessarily lose at high concurrency" is **retracted**.

**Lesson (for the record)**: a benchmark can't measure the server's properties when the client side is the bottleneck. The 2 vCPU tail
conclusion was a projection of client contention. **Always over-provision the load generator when measuring** (8 vCPU reversed it).
Cost: ~$0.2 for the 2 vCPU + 8 vCPU measurements combined, terminated each time.

## 7.6 N-to-N scale measurements (2026-07-18, AWS c7g.4xlarge / 16 vCPU / 30GB) ★decisive

**thread-per-core showdown**: AetherFlow uses **8 reactors + SO_REUSEPORT** (`serve_on_cores`), glommio uses
**8 executors** (`LocalExecutorPoolBuilder` + reuseport). **server=cores 0–7 / client=cores 8–15**, fully separating the
server and client cores, 32B echo, conns 256→8192.

| conns | metric | **AF scan (8)** | AF epoll (8) | glommio (8) |
|--:|---|--:|--:|--:|
| 256 | p50 / p99 / thru | **168µs / 330µs / 500k** | 167 / 387 / 482k | 208 / 418 / 466k |
| 1024 | p50 / p99 / thru | **457µs / 1180µs / 587k** | 535 / 1297 / 481k | 777 / 1192 / 479k |
| 4096 | p50 / p99 / thru | **362µs / 890µs / 643k** | 3845 / 7180 / 459k | 4237 / 4793 / 437k |
| 8192 | p50 / p99 / thru | **408µs / 2036µs / 620k** | 2188 / 5692 / 501k | 9306 / 10071 / 414k |

### Conclusion (decisive)
- **AF scan (thread-per-core busy-poll) wins against glommio on median, tail, and throughput across every connection count
  from conns 256→8192**, and moreover **the gap widens with scale**:
  - at 8192 connections, **median 408µs vs glommio 9306µs = ~23×**, **throughput 620k vs 414k = ~1.5×**,
    **p99 2.0ms vs 10.1ms = ~5×**.
- **The earlier concern that "the high-concurrency tail / scale favors readiness (io_uring)" is decisively refuted.**
  thread-per-core busy-poll **wins over io_uring even at scale on dedicated cores** (park/wake wake latency bites at high
  fan-in and inflates glommio's median to the ms range, whereas busy-poll keeps the median at ~400µs with no
  syscalls/scheduling). This is the same structure as busy-spin > park/wake, which won in message passing.
- **The epoll backend regresses at high concurrency** (3.8ms at 4096, losing to both scan and glommio). → **The winning path
  is scan busy-poll. epoll is unnecessary** (no advantage beyond the cleanliness of the low-conn tail). Keep it opt-in and confirm scan as the default.

### Honest caveats
- **busy-poll burns 8 cores at 100%** (regardless of load). glommio scales with the amount of work. = the "latency ↔ CPU/power"
  trade-off (a standing caveat). A win predicated on dedicated cores.
- localhost, one run per point, client also on the same machine (8 cores separated). Real networks, multiple runs, and variance to follow.
- glommio uses default settings + a plain echo implementation (no specialist tuning).
- **Claim (updated)**: "**For low-latency server I/O, from few connections to high concurrency (~8192), on dedicated cores,
  thread-per-core busy-poll wins against glommio on every metric.**" The high-concurrency side of the "total I/O domination"
  previously held on hold is **settled (a win) with thread-per-core**. Cost: ~$0.15 for the 16 vCPU measurement, terminated afterward.

## 8. Status and next steps
- **[done 2026-07-16] Implemented the surface API + a portable reference backend** (`core/src/net.rs`, feature `net`,
  default OFF). `Connection`/`Io`/`FramedConnection`/`Codec` (`LengthPrefixed`/`Lines`)/`serve`.
  Compiled & tested on macOS (echo, framed lines, length-prefixed, on_open/on_close, 4 tests green).
  The reference reactor is nonblocking single-threaded = **not the busy-poll performance version** (the base for finalizing the API).
- **Next**: (a) implement a busy-poll reactor (each core spins the socket) under `#[cfg(target_os="linux")]` and integrate it
  into `System::listen` → (b) fairly compare end-to-end latency + throughput against glommio with echo
  → (c) add backpressure (`on_writable`/`pause_reads`) as an advanced opt-in.
- Measurement is on AWS/Latitude (macOS not possible). Design and API can proceed on macOS (done).
