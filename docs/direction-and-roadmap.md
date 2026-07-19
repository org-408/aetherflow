# AetherFlow Direction and Roadmap

> 🌐 English | [日本語](direction-and-roadmap.ja.md)


> Where this document sits: whereas `design.md` states **what we are building** (the technical thesis),
> this document records the decisions and technical reasoning behind **why we settled on building it this way**.
> It is the conclusion of the 2026-07-02 design discussion — a map to keep a future version of me from
> "agonizing over the same fork all over again."
>
> Note: this document stays strictly a technical roadmap (business and domain considerations are out of scope here).

---

## 1. Post-mortem of the stall — why it once ground to a halt

The cause was not dirty code. The real cause was **leaving an unresolved fork open and mixing in the conveniences of both sides at once**.

Back then I studied Tokio (an async runtime), studied Akka (an actor execution model), and implemented **without ever committing to which one to build on**. As a result, `core/src` ended up with:

- `Arc<Mutex<T>>` (Akka-style sharing + runtime locking)
- `async fn receive` (Tokio-style asynchrony)
- `Mailbox` / `Dispatcher` / `Runtime` traits (a homegrown-runtime-style abstraction)

**all coexisting, none of them finished**. It was not a skills problem; the real identity of the stall was the **absence of a decision**.

→ Lesson: **kill the fork before touching the technology.** When the direction is singular, code can only flow one way, and the room to agonize disappears.

---

## 2. The actual state of the implementation (as of 2026-07)

> Note: the following is a snapshot from before the rebuild (the old `Arc<Mutex>` implementation). As the work log in §6 shows, it has since been
> replaced by the new runtime with typed / move / SPSC & MPSC / core-pinning (`core/`, package `aetherflow`).
> This section is kept as a record of "why we rebuilt."

`core/src` was **mostly a skeleton of trait definitions, and the only working substance was "receive a message and log it."**
The one path through which data actually flowed was `main.rs` → `ActorRef::tell` → `receiver.receive()`, where it merely did `println!` / `info!`.

### The working parts
- `reference.rs`'s `ActorRef<T> = { actor: Arc<Mutex<T>>, sender: Arc<Mutex<dyn ActorSender>>, receiver: Arc<Mutex<dyn ActorReceiver>> }`. `tell` merely does `receiver.lock().await` and calls `receive(msg)`.
- `message.rs`: `Arc<dyn Message>` (a blanket impl for every `T: Any+Send+Sync+Debug`).
- `lifecycle.rs`: the state-transition machine is implemented but **is never called from anywhere**.
- `path.rs`: only the `ActorPath` builder.

### Traits with empty bodies (unwired)
- `mailbox.rs`: the `Mailbox` trait exists, but the `UnboundedMailbox` / `BoundedMailbox` implementations are **all commented out**.
- `dispatcher.rs` / `runtime.rs` / `executor.rs`: only traits with one or two methods, zero implementation.
- `behavior.rs` / `action.rs` / `state.rs` / `task.rs` / `encapsulator.rs` (`Envelope`) / `decapsulator.rs`: the types exist but are **not connected** to the send/receive path.
- `derive/src/lib.rs`: `#[derive(Actor)]` assumes the commented-out `UnboundedMailbox::new(10)`, so using it now **fails to compile**.
- `remote` / `cluster` / `persistence` / `streams`: crate scaffolding only.

### The current message path (the one that works)
```
ActorRef<T>          msg.clone()          receiver.lock()      receive()
Arc<Mutex<T>>   ──▶  Arc<dyn Message> ──▶ .await runtime lock ──▶ println! only
  shared+locked        touchable even         not lock-free
   (inverse of ①)      after send (inverse of ②)
```

---

## 3. Theory ⇄ implementation comparison table

Organized around the four pillars of `design.md` (① isolation by types / ② zero-copy move / ③ physical placement as a first-class concern / ④ homegrown scheduler).

| Aspect | Theory (design.md / Hewitt) | Old implementation (pre-rebuild) | Verdict |
|---|---|---|---|
| ① Isolation guarantee | Compile-time guarantee via the type system (Pony caps) | `Arc<Mutex<T>>` runtime lock | ❌ inverse |
| ② Message movement | `move` = ownership transfer, untouchable after send | shares `Arc<dyn Message>` via `clone()` | ❌ inverse |
| ③ Physical placement | core pinning + in-core SPSC mailbox | mailbox impl commented out, no placement concept | ⛔ not implemented |
| ④ Scheduler | homegrown, no work-stealing | plain `tokio::runtime::Runtime` (work-stealing) | ❌ inverse |
| Message type | (implied) typed / associated Message type | `dyn Message` type erasure (the root of move-impossibility) | ❌ inverse |
| Hewitt: state×msg→(state,msgs,behavior) | expressed via the `Behavior` type / `Action` enum | the types exist but are unwired from the send/receive path | ⛔ unwired |
| Hewitt: become | `Action::Become` | enum only, no execution mechanism | ⛔ not implemented |
| Hewitt: create | `Action::Create` / `Parent::spawn` | trait only, no substance | ⛔ not implemented |
| Lifecycle | preStart/postStop etc. (from Akka) | the state machine is complete, but **unwired** | ⚠ orphaned |
| Supervisor hierarchy | analyzed in akka-analyze | `parent.rs` = one empty trait | ⛔ not implemented |

Legend: ❌ = opposite direction from the thesis / ⛔ = not implemented / ⚠ = implemented but orphaned (all reflect the pre-rebuild state)

### The root cause of the collapse is a single point
**Type erasure via `dyn Message` makes `move` impossible, and to pay for that it escapes into `Arc<Mutex>` sharing and locking.**
This is the epicenter of every collapse across ①②③④. Conversely, fix this and the four pillars all line up in a straight line.

**The target path (= what the rebuild achieved):**
```
Actor<M>            send(msg: M)         SPSC mailbox         &mut self
associated type M ──▶ transfer via move ──▶ in-core, lock-free ──▶ core-pinned
                    touching msg after send is a compile error (② guaranteed by types)
```

The key = **stop erasing types**:
- `dyn Message` (type erasure) → move impossible → lean on Arc → sharing → ①② collapse (old implementation)
- give each actor an associated `Message` type, and make the mailbox a typed SPSC
- then `msg: M` can be passed by move → no Arc, no Mutex needed → ①②③④ all line up in a straight line
- = the foundation is reproducing Pony's `iso` (unique, sendable) as "move an owned `T: Send`" in Rust

---

## 4. The chosen direction — settling the four forks

To prevent the stall from recurring, the following four forks were settled before starting work.

### Fork 1: build on async/await? → **No (Option B)**
The two are irreconcilable. This is the epicenter.
- **Option A (rejected): go all-in on async** — easy, but work-stealing crosses cores, locks are needed across `.await`, and the thesis's "core-local, lock-free, zero-copy" cannot hold in principle. Where it ends up is "Rust's Akka" = kameo/ractor/xtra already exist, so **there is little point in building from scratch**.
- **Option B (adopted): banish async from the hot path (Pony / LMAX / kompact style)** — an actor is a **synchronous, run-to-completion** `fn handle(&mut self, msg: M)`. On an OS thread pinned to a core, a homegrown scheduler pulls from an SPSC ring-buffer mailbox and spins. **Tokio is used, optionally, only at the I/O edge (networking, etc.).** This is the only way the thesis actually holds.

### Fork 2: what to build (the goal) → **(b) a runtime with novelty**
- (a) learning/research / **(b) a novel runtime (adopted)** / (c) a practical library
- Choosing (b) means the goal is to beat the existing Rust actor crates on core-locality and latency. A systems project on the order of months.
- The past failure was a mixed state that "held up the ideals of (b), implemented the convenience of (c), and lost the rigor of (a)." **Do not mix them.**

### Fork 3: scope → **v1 is single-node. Distribution / persistence / streams are frozen**
- Remote/distributed contradicts zero-copy (over the network = serialize = copy required).
- `remote` / `cluster` / `persistence` / `streams` **keep only their crate scaffolding and are frozen for now**. Not discarded, but pushed to the back as a future extension surface.

### Fork 4: typed or dynamic → **typed (automatically decided by Fork 1)**
- Option B = the single choice of a typed actor (associated Message type). It can move, and no Arc or Mutex is needed.
- Accept the realistic costs of typed:
  - one actor handling multiple message kinds → enum or multiple channels
  - a parent bundling children of different types → a `tag`-equivalent type erasure is needed somewhere
  - these will inevitably come up in the implementation, so design for them head-on instead of dodging.

### The settled picture (working draft)
> **Goal (b). Option B (banish async from core). v1 is single-node; distribution/persistence/streams are frozen. Typed actor.**
> Make the first milestone tiny: **one thread pinned to one core + one typed actor + one SPSC mailbox + send by move + a run-to-completion loop.**
> Multicore, routing, and supervision get added only after the thesis holds at N=1.

---

## 5. Business and domain matters (out of scope here)

This document stays strictly a technical roadmap. Business and domain considerations are managed separately and are out of scope for this public document. It continues into the technical progress in §6 below.

---

## 6. Implementation progress and the next technical steps

### Work log (things implemented once the direction was settled)
1. **Nailing down the theory (B)** ✅ **Done (2026-07-03)** → `docs/pony-rust-capability-mapping.md`
   - Built a mapping table of Pony's 6 capabilities (`iso`/`trn`/`ref`/`val`/`box`/`tag`) ⇄ Rust's ownership, `Send`/`Sync`.
   - **go/no-go = go**: the `iso`/`val`/`tag`/`ref` needed by the runtime all map onto Rust, and the core `iso` (a moved message) is cleaner than in Pony (structurally, statically complete). The gaps that cannot be filled (`trn`/`recover`/viewpoint adaptation) are off the critical path and non-goals.
   - The derived API shape: `trait Actor { type Message: Send; fn handle(&mut self, msg: Self::Message); }` (typed, move, synchronous run-to-completion). Confirmed that dropping type erasure (`dyn Message`) makes ①②③④ line up in a straight line.
2. **Verification experiment (C)** ✅ **Done (2026-07-03)** → `experiments/capability-proof/` (an independent crate detached from the parent workspace)
   - `cargo test` backs the theory with 3 doctests + 2 runtime tests:
     - **Proof 1 (compile_fail)**: with the typed + move API, using `order` after `send(order)` **fails to compile** with `E0382: borrow of moved value`. Demonstrates that the type enforces isolation.
     - **Proof 3 (pass)**: the Akka-style `Arc<Mutex<_>>` **compiles cleanly** even when `order.lock().qty = 200` follows `tell(order.clone())`. Demonstrates that isolation is merely a convention.
   - **Incidental learning**: the verification needs to **actually use** the moved value. `let _ = order.qty;` — the `let _ =` — is not counted as a "use" and does not fire the move, so at first this trap let compile_fail pass by mistake. Tests must actually use the value via `println!` etc. (noted in the crate's docs).
   - **Meta-lesson**: `$?` after a pipe is the exit code of the last command (`head`), not of `rustc`. Take the exit status without a pipe to judge compilability.
3. **Competitive landscape survey (groundwork for the differentiation gate)** ✅ **Done (2026-07-03)**
   - **Verdict: not a total loss, but not empty space either.** thread-per-core (glommio/Seastar) / capability isolation (Pony) / zero-copy move (Pony) / fast typed Rust message passing (kompact, 400M msg/s) **all already exist individually**. The differentiation is in **their combination** (bringing thread-per-core + core-pinning + typed SPSC + capability isolation all together in a Rust actor).
   - **Only one claim in design.md §2.3 survives**: "integration with physical placement (CPU topology)" against Pony. thread-per-core itself has no novelty.
   - **Risk of whether the win is big enough**: the thread-per-core win (71–92% tail-latency improvement) is also obtainable with off-the-shelf glommio. → As a standalone "fast actor" it is squeezed between glommio and kompact. **The winning line is the three-piece set of speed × compile-time safety × actor abstraction.**
   - **The Stage 0 benchmark target is fixed**: axis = tail latency (p99/p9999) + jitter (throughput alone is unfavorable against kompact). Opponents = ① kameo/actix (easy baseline), ② kompact (challenge on latency), ③ glommio + a thin actor (the real test of the delta). Method = reuse kompicsbenches.
   - Note: the verification panel was wiped out by the session limit; the numbers are sourced but not yet independently verified.
4. **N=1 rebuild (A)** ✅ **Done (2026-07-04)** → `core/` (package `aetherflow`, replacing the old core)
   - typed actor + move messages + a **bounded SPSC ring-buffer mailbox** (lock-free, head/tail separated by 64B to avoid false sharing) + core pinning (best-effort) + a run-to-completion loop (busy-spin when empty = LMAX style) + on_start/on_stop.
   - `cargo test` all green (SPSC 6 + integration 4 + doctest 2), `cargo clippy` clean, demo working (detect 16 cores → pin to core 0 → process an order by move).
   - The compile-time isolation guarantee (E0382) is inherited from capability-proof and secured by a `compile_fail` doctest.
   - **Known limitation**: core pinning is a no-op on macOS (no hard affinity). Real pinning + latency measurement to be done on Linux + Stage 0.
5. **Going multicore** ✅ **Done (2026-07-04)** → `core/` (unified under the System API)
   - **thread-per-core** (≠ thread-per-actor): `System::with_cores(N)` gives one thread + pinning per core, and each thread runs many actors in run-to-completion. Actors have a static placement and do not migrate across cores.
   - **routing**: `ActorRef<A>` is a clonable MPSC sending end. Cross-core sends are lock-free. Heterogeneous actors on the same core are type-erased (`dyn ErasedActor`) and run on one thread (control plane = erased / data plane = typed).
   - **mailbox = bounded MPSC** (Vyukov slot sequences, lock-free, separated by 64B to avoid false sharing), newly implemented. SPSC is kept for the single-producer fast path / a future cross-core paired queue.
   - Verification: `cargo test` all green (spsc 6 + mpsc 5 + integration 5 + doctest 2), clippy clean, concurrent MPSC and cross-core routing pass 20/20 iterations each in release, sharding demo working (gateway → engines on 3 cores).
   - Known limitations: static placement only (no work-stealing = skew rebalancing not yet started), `send_blocking` from within a same-core handler risks deadlock (use try_send), macOS pinning is a no-op.
6. **Stage 0 benchmark (the decisive point)** 🔄 **Preliminary measurements + AWS Tier1 measured** → `docs/stage0-bench-notes.md`, `core/benches/latency.rs`
   - vs Tokio (work-stealing): **~3.2× median latency, ~3.9× throughput** (macOS/no-pin, preliminary). A multiplicative win = the "is it big enough" bar is preliminarily cleared.
   - **On macOS the tail (p999) ties**, and max spikes (as predicted without pinning).
   - **Linux container measurement (Docker on Mac, 4 cores)**: ~14× median, ~8× p99 in aether's favor. However, pure busy-spin blows up the tail with p999 = 2.6ms (preemption under virtualization).
   - **Countermeasure implemented: `IdleStrategy::Backoff`** (spin → yield → park): improves p999 by 20× (2.6ms → 128µs, median only +10%). **Backoff beats tokio/kameo across all quantiles.** The default is latency-first BusySpin; use Backoff for shared/virtualized environments, chosen per situation.
   - **AWS Tier1 measurement (real Linux, dedicated vCPU)**: ~10× median, ~40× ask, ~4.6× throughput, and **the tail tightened from Docker's 2.5ms to 3.3µs**. The headline is proven on real hardware.
   - **The remaining production run (optional / follow-up)**: fix the absolute tail on isolated-core bare metal (Tier2). It can be fired on Latitude.sh/Vultr without waiting for the AWS quota (`docs/run-on-linux.md`). Tier2 is not a mandatory gate for going public but a ceiling flex.
   - **positioning (2026-07-04): cloud-first as the main battleground.** Bare-metal absolute tail (HFT class) is a narrow-niche "ceiling flex." The main battleground is "on dedicated-vCPU cloud, beat Tokio/kameo across every quantile, without burning idle CPU thanks to backoff, and compile-time safe on top of that." Details in `positioning.md`.

### How we proceeded
Proceeded in the order **B → C → A**. Solidify the foundation in theory, back it with a minimal example, then rebuild core. Minimal rework.

### Retiring the old core / making the repository coherent ✅ Done (2026-07-04)
- Resolved the "dirty state" that started the conversation. **Promoted the new runtime to the official `core/` (package `aetherflow`)**,
  replacing the old `Arc<Mutex>` Akka clone (which was the inverse of the thesis).
- Deleted the old-API-only `derive` / `macros` / `main-macro` crates, and the dead scaffolding at the root (examples/tests/benches) too. Integrated the detached `runtime/` into core.
- `remote` / `cluster` / `persistence` / `streams` remain in the workspace as frozen stubs (a future extension surface).
- `cargo test --workspace` all green, clippy clean. **The workspace is restored to a single coherent build.**

## 6.6 Incorporating the GPT external review (2026-07-04)

Triaged an external review (a perf-first lens). **Rejected proposals that reopen already-settled forks (removing work-stealing, banishing async) or that make Tokio compatibility a goal** (these run counter to the thesis; interop at the edge = OK, compatibility as a goal = No), and kept NUMA frozen for v1. **What we took**:

- **Observability (the top-priority new item)** 🔄 **First installment implemented (2026-07-04)**: `ActorRef::mailbox_depth()` /
  `total_sent()` / `total_processed()`. **Zero added cost on the hot path** — the single-consumer lock-free mailbox
  already holds the enqueue/dequeue counters, and we simply expose them (in contrast to shared/work-stealing systems
  that need contended atomics = a consequence of the model). **A processing-latency histogram is also implemented (opt-in)**: enable it with `sys.build(..).instrumented().spawn()`,
  and `ActorRef::latency()` returns p50/p99/p999/mean. Because `Instant` has a hot cost, it is OFF by default
  (zero-overhead by default). Being single-writer, bucket updates are relaxed and uncontended. **Remaining**: cross-core
  migration count. With this, all subsequent optimization becomes data-driven.
- ✅ **[implemented] SpawnBuilder (putting the shallow surface into practice, 2026-07-04)**: organized the proliferating `spawn_on_*` into
  `sys.build(make).core(0).mailbox(4096).supervised().instrumented().spawn()`. In simple cases it stays as
  `spawn_on`, with only the advanced knobs opt-in (progressive disclosure = §2.5 · GPT #7). `core/src/system.rs`.
- ✅ **[implemented] Deadlock guard (type-justified safety No. 3, 2026-07-04)**: identifies the core with a thread-local
  and detects self-blocking when a handler does `ask`/`send_blocking` to an actor on the **same core**.
  **Turns a silent hang into an explicit error/panic** (`ask` returns `Err(WouldBlockCallingCore)`, `send_blocking`
  panics → caught via panic isolation). The runtime rejects the footgun. `core/src/system.rs` / `ask.rs`.
- **cache-aware scheduler**: the current round-robin polling is naïve. After adding observability, move to
  cache-aware ordering (the one spot still weak in the "single-breakthrough three-piece set").
- The `#[actor]` derive is deferred as an ergonomics improvement (low priority).

### Risk register: same-core head-of-line blocking (the sharpest risk GPT missed)
Because thread-per-core is "1 core = 1 thread running many actors run-to-completion," **if one handle is
long/heavy, all actors on the same core are made to wait** (work-stealing could offload it, but static placement has no escape).
= a structural weakness of thread-per-core, our version of "fairness vs locality." Mitigation candidates: a per-message time budget,
cooperative yield, isolated placement / a dedicated core for heavy actors. It should surface in measurement after Stage 0. Registered as a design issue.

## 7. Related documents
- `concepts-explained.md` — a gentle explanation of the concepts (glossary). Capabilities and CPU concepts by everyday analogy. The origin of intuition.
- `design.md` — the technical thesis (four pillars) and prior work (Pony / LMAX / work-stealing)
- `positioning.md` — the four-quadrant matrix + overall strength (the public pitch)
- `stage0-bench-notes.md` — benchmark measurements (macOS / Docker / AWS Tier1)
- `pony-rust-capability-mapping.md` — the mapping table of Pony caps ⇄ Rust ownership (§6 work item 1, created)
- `actor-model-theoretical-concepts.md` — a formalization of the Hewitt actor model
- `akka-analyze.md` — a detailed analysis of Akka's internal structure (ActorRef → mailbox → Dispatcher, Supervisor hierarchy, LifeCycle)
