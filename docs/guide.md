# Using AetherFlow — a 10-minute guide

> 🌐 English | [日本語](guide.ja.md)
>
> You write **plain synchronous Rust**. An actor is "one message type + one handler". The guarantees
> (data-race freedom, no GC, no locks) are added by the type system under the floor — the surface
> stays shallow. Each section maps to a **runnable example** in `core/examples/`
> (`cargo run --example <name>`).

## 0. 30-second mental model

- **actor** = state (struct fields) + behavior (`handle`). State is **single-owned**, so no lock.
- **message** = `send` **moves** its ownership into the runtime (using it after send is `E0382`, a
  compile error).
- **System** = a runtime with one thread per core. Actors are pinned to a core and never migrate
  (thread-per-core).

## 1. Your first actor — `hello_actor`

```rust
use aetherflow::{Actor, System};

struct Greeter { count: u32 }

impl Actor for Greeter {
    type Message = String;                       // the (fixed) message type
    fn handle(&mut self, name: String) {         // &mut self = sole owner, no lock
        self.count += 1;
        println!("hello, {name}! (#{})", self.count);
    }
}

let sys = System::with_cores(1);                 // 1-core runtime
let greeter = sys.spawn_on(0, Greeter { count: 0 });
greeter.send_blocking("Ada".to_string()).unwrap();
drop(greeter);                                   // drop send handle → drain → stop
sys.shutdown();
```

Lifecycle hooks: `on_start` (after placement) / `on_stop` (after the mailbox drains) / `on_restart`
(§4).

## 2. Awaiting a reply — `request_reply` (`ask`)

Choose between fire-and-forget (`send_blocking` / `try_send`) and **awaiting a reply** (`ask`).

```rust
enum Cmd { Set(String, i64), Get(String, Responder<Option<i64>>) }
// ...
kv.send_blocking(Cmd::Set("apples".into(), 3)).unwrap();          // fire-and-forget
let v: Option<i64> = kv.ask(|reply| Cmd::Get("apples".into(), reply)).unwrap();  // await reply
```

- `ask` puts the reply slot on the **caller's stack** → **zero heap allocation**.
- `ask` blocks the calling thread, so call it **from outside the runtime** (main / an I/O thread).
  Calling it from inside a handler on the same core returns `Err(WouldBlockCallingCore)` instead of
  hanging silently.

## 3. Handling work in parallel — `sharded` (fan-out / thread-per-core)

Put N workers on N cores and route by key hash. The same key always lands on the same shard.

```rust
let sys = System::with_cores(4);
let shards: Vec<_> = (0..4).map(|id| sys.spawn_on(id, Shard::new(id))).collect();
let s = shard_of(key, 4);
shards[s].send_blocking(Msg::Bump(key)).unwrap();   // separate core = separate thread, no sharing
```

There is no work-stealing, so placement is predictable and cache locality is preserved.

## 4. Surviving failures — `supervision`

With `spawn_on_supervised`, a panicking actor is **dropped and replaced with a fresh one**, and the
mailbox keeps going.

```rust
let worker = sys.spawn_on_supervised(0, Worker::new);   // rebuild with Worker::new on panic
```

Because the type system guarantees state is single-owned (never shared), a panicked actor can be
restarted safely — no corrupted state lingers (unlike `Arc<Mutex>`, which poisons on panic).

## 4.5 Broadcasting one-to-many — `pubsub`

`ActorRef` is **Clone + Send**, so you can **pass it inside a message** — that's how you subscribe.
A Hub holds the subscriber list as single-owned state and delivers each `Publish` to everyone with
`try_send` (non-blocking).

```rust
enum HubMsg { Subscribe(ActorRef<Subscriber>), Publish(String) }
// Inside a handler (i.e. sending to other actors) use try_send (send_blocking would deadlock same-core):
for sub in &self.subscribers { let _ = sub.try_send(text.clone()); }
```

## 5. Embedding in Tokio — `tokio_interop` (gradual adoption)

Keep your existing async I/O on Tokio and move just the **state and compute** onto AetherFlow. Two
rules:

- **async → actor sends are non-blocking** (`try_send` / `send_blocking`) — call them from async.
- Call **`ask` inside `tokio::task::spawn_blocking`** (it blocks; don't stall the async runtime).

```rust
// From an async handler:
let _ = metrics.try_send(Cmd::Record { bytes });
// Read the aggregate (blocks, so run it on a blocking thread):
let snap = tokio::task::spawn_blocking(move || m.ask(Cmd::Snapshot).unwrap()).await.unwrap();
```

## 6. Choosing a send API

| Call | Blocks? | On full mailbox | Reply? | Use when |
|---|---|---|---|---|
| `try_send` | no | returns `Err(Full(msg))` | no | async / hot path / handle backpressure yourself |
| `send_blocking` | only when full | waits until space frees | no | simple fire-and-forget |
| `ask` | until reply | — | yes | request-reply (from outside the runtime) |

On failure the **original message is returned** (`err.into_message()`), so you can retry, persist, or
log it.

## 7. Network I/O (feature `net`)

The `net` feature lets you write servers as "I/O as messages" (connection = actor, inbound =
messages, outbound = a non-blocking handle, no `async`). See the `echo_server` / `io_bench` examples
and `io-surface-design.md`.

## See also
- `design.md` — the four-pillar technical thesis and "deep theory, shallow surface"
- `concepts-explained.md` — a gentle explanation of the concepts
- `core/examples/` — every example from this guide (runnable)
