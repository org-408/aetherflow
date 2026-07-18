<div align="center">
  <img src="docs/assets/logo-mark.png" width="140" alt="AetherFlow" />

  # AetherFlow

  **Flow at the speed of hardware.**

  A high-performance actor runtime for Rust — thread-per-core, lock-free,
  zero-copy. The type system proves isolation at compile time, which unlocks
  optimizations other runtimes can't safely make.

  [![CI](https://github.com/org-408/aetherflow/actions/workflows/rust.yml/badge.svg)](https://github.com/org-408/aetherflow/actions/workflows/rust.yml)
  [![crates.io](https://img.shields.io/crates/v/aetherflow.svg?color=2563EB)](https://crates.io/crates/aetherflow)
  [![docs.rs](https://img.shields.io/docsrs/aetherflow?color=06B6D4)](https://docs.rs/aetherflow)
  [![license](https://img.shields.io/crates/l/aetherflow.svg)](#license)

  [Docs](docs/design.md) · [Why AetherFlow?](docs/direction-and-roadmap.md) · [Benchmarks](docs/stage0-bench-notes.md)
</div>

---

AetherFlow is **not** a Tokio replacement. It's a different lineage: an actor
runtime designed from the CPU up — cores, caches, and message ownership are
first-class, not afterthoughts.

You write **plain typed Rust**. When you `send` a message, its ownership is
**moved** into the runtime — using it afterward doesn't compile (`E0382`). This is
Pony's `iso` idea recovered in Rust as a moved `T: Send`, so the runtime routes
the message **without locks, without GC, and with no per-message `Arc`/refcount** —
guarantees, not benchmarks. (Ownership of the *message value* is transferred; Rust's
`Send` still lets you put explicitly-shared state like `Arc<Mutex<_>>` *inside* a
message if you choose — see [Known limitations](#status--scope).)

- 🛡️ **Isolation, proven at compile time** — data-race freedom is a *type error*,
  not a runtime convention. Use-after-send doesn't compile (`E0382`).
- ⚡ **Thread-per-core, run-to-completion** — one OS thread per core, actors pinned,
  no work-stealing, no cross-core migration. Cache locality by construction.
- 📨 **Zero-copy messages** — `send` moves ownership. No clone, no `Arc<Mutex>`.
- 🔒 **Lock-free mailboxes** — bounded MPSC ring, head/tail on separate cache lines
  to avoid false sharing.
- 🎯 **The whole triple, at once** — static data-race-free **+** zero-GC-pause **+**
  no per-message heap alloc, clone, or `Arc` refcount. (The lock-free mailbox uses
  atomics like any MPSC — what's absent is *per-message* refcounting, not all atomics.)
  Pony proved capabilities but pays GC; other Rust actor frameworks have neither.

## Quick start

```toml
[dependencies]
aetherflow = "0.1"
```

```rust
use aetherflow::{System, Actor};

// An actor is plain typed Rust: one message type, one handler.
struct OrderBook { bids: u64 }

impl Actor for OrderBook {
    type Message = Order;                 // fixed, `Send` — the sendable `iso`

    fn handle(&mut self, order: Order) {  // &mut self: sole owner, no lock needed
        self.bids += order.qty as u64;
        println!("matched {} @ {}", order.qty, order.price);
    }
}

struct Order { qty: u32, price: u32 }

fn main() {
    let sys = System::with_cores(4);              // 4 cores, 4 pinned threads
    let book = sys.spawn_on(0, OrderBook { bids: 0 });

    let order = Order { qty: 100, price: 42 };
    book.send_blocking(order).unwrap();            // moves `order` into core 0; Err(Closed) if the actor is gone
    // println!("{}", order.qty);                  // ← would NOT compile (E0382)

    sys.shutdown();
}
```

> **Heads-up:** `with_cores(n)` defaults to `IdleStrategy::BusySpin` — lowest
> latency, but it keeps `n` cores at ~100% CPU. On a laptop or shared box, use
> `System::with_cores_idle(n, IdleStrategy::backoff())`.

Need a reply? `ask` puts the reply cell on the call stack — no per-call heap
allocation (the caller blocks until the actor replies):

```rust
let depth: u64 = book.ask(|reply| Query::Depth(reply))?;
```

## Learn by example

A 10-minute path from "hello actor" to a real pattern — see the
[**guide**](docs/guide.md), each section backed by a runnable example:

| `cargo run --example …` | Shows |
|---|---|
| [`hello_actor`](core/examples/hello_actor.rs) | The minimal actor — state, `handle`, lifecycle |
| [`request_reply`](core/examples/request_reply.rs) | `ask` request-reply — a zero-alloc KV store |
| [`sharded`](core/examples/sharded.rs) | Fan-out across cores (thread-per-core, no locks) |
| [`supervision`](core/examples/supervision.rs) | Panic isolation + automatic restart |
| [`tokio_interop`](core/examples/tokio_interop.rs) | Embed AetherFlow as the state core inside a Tokio app |
| [`echo_server`](core/examples/echo_server.rs) | I/O as messages — a TCP server, no `async` (`--features net`) |

## Why "the type system unlocks the speed"

Performance mechanisms — batching, per-core pools, emplace — are a treadmill:
anyone copies your numbers in one release. Speed alone is not a moat.

The moat is the **type system**. Because the message value is *moved* (single-owned
at the message boundary, checked at compile time), aggressive optimizations — no
per-message atomic refcount, no GC, per-core message reuse — become *structurally*
safe. A runtime without a type system can't copy
them safely; it would have to build the type system too (Pony-scale cost).

And you never write a capability annotation. The theory works under the floor;
on the surface you write ordinary Rust and the guarantees come free. That's the
design principle: **deep theory, shallow surface.** See
[design.md](docs/design.md) §2.4–2.6.

## Status & scope

**Single-node v1.** This is a systems project under active development.

- ✅ typed actors · move messages · lock-free MPSC mailbox · thread-per-core ·
  core pinning (best-effort) · zero-alloc `ask`
- ✅ **Tail latency validated on real hardware** — on AWS Graviton3 (real Linux,
  native core pinning) the busy-spin tail collapses from milliseconds to ~3–5µs,
  and AetherFlow wins every percentile vs Tokio (median ~10×, p99 ~13×, p999
  ~3–4.5×). Zero-alloc `ask` runs sub-µs (p50 268ns / p999 399ns). Core pinning
  is a no-op on macOS (an OS limitation, not ARM). See
  [benchmarks](docs/stage0-bench-notes.md).
- 🎯 **Next:** isolated cores (isolcpus/nohz_full, bare metal) to drive p99.9 into
  single-digit µs — matching-engine / HFT territory.
- 🧊 **Frozen for now:** distributed, clustering, persistence, and streams. These
  are on the roadmap, not the current build. See
  [direction & roadmap](docs/direction-and-roadmap.md).

Not for elite HFT (they build their own or go FPGA). The target is the tier that
wants Disruptor-class speed **with** safety and productivity but has no HFT team:
exchanges, brokers, real-time risk, market data, ad RTB, game tick servers.

### Known limitations (honest scope for 0.1)

This is a young systems project. The concept and core are solid, but several
correctness/robustness items are deliberately not done yet — we'd rather state
them than have you discover them:

- **Isolation is at the message boundary, not deep.** `send` moves the message
  value (use-after-send is `E0382`), but Rust's `Send` doesn't forbid explicitly
  shared internals — a message can still carry `Arc<Mutex<_>>` if you write it that
  way. This is ownership *transfer*, not Pony-`iso`-strength deep uniqueness.
- **`ask` liveness depends on the callee replying.** If the actor is already gone
  when you `ask`, you get `Err(AskError::Closed)`. But the reply cell lives on the
  caller's stack, so if an actor *stores* the `Responder` instead of replying, the
  caller blocks. Generation-tagged reply slots are on the roadmap.
- **Lock-free queue verification.** The MPSC mailbox and the SPSC ring are checked
  by Loom, which explores every legal thread interleaving of small models rather
  than whichever one the hardware happened to produce; Miri covers UB on the
  unsafe paths. The models are deliberately tiny (two threads, capacity of one or
  two) because the search space explodes, so they prove the ordering discipline
  rather than the whole queue: `cargo test --lib --release -p aetherflow` with
  `RUSTFLAGS="--cfg aetherflow_loom"`. The `ask` reply cell is not yet modelled —
  it is being redesigned (see the liveness limitation above).
- **`IdleStrategy::BusySpin` is the default** (100% CPU per core) — use
  `IdleStrategy::backoff()` on shared or battery-powered machines.

## Documentation

- [design.md](docs/design.md) — the technical thesis (four pillars) and prior art (Pony / LMAX)
- [direction-and-roadmap.md](docs/direction-and-roadmap.md) — why this shape, and the path forward
- [competitive-landscape.md](docs/competitive-landscape.md) — how it differs from glommio / kompact / Pony
- [concepts-explained.md](docs/concepts-explained.md) — plain-language glossary
- [pony-rust-capability-mapping.md](docs/pony-rust-capability-mapping.md) — Pony capabilities ⇄ Rust ownership

## Contributing

AetherFlow is open source and contributions are welcome — bug reports, feature
requests, and pull requests alike. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option. Unless you explicitly state otherwise, any contribution
intentionally submitted for inclusion in the work by you, as defined in the
Apache-2.0 license, shall be dual licensed as above, without any additional
terms or conditions.
