# AetherFlow Design Document

> 🌐 English | [日本語](design.ja.md)


## 1. Core Thesis

Rather than treating actor-model **isolation** as a **runtime convention** the way Akka/BEAM do, AetherFlow **guarantees it at compile time** using Rust's type system, and then **co-designs it with the physical CPU topology (cores, caches, NUMA)**. This pushes into territory that existing actor runtimes are structurally unable to reach: lock-free, zero-copy, zero-GC-pause, and guaranteed cache locality.

## 2. Prior Theoretical Work

### 2.1 The Origin of the Actor Model
Carl Hewitt's Actor Model (1973) — the foundational idea that "an actor is a universal primitive of computation, and concurrent computation can be described using only message reception, spawning new actors, and deciding the next behavior." Akka/BEAM are implementations of this, but **they do not specify how isolation is guaranteed** (they rely on runtime discipline).

### 2.2 The Decisive Prior Work: The Pony Language
The idea of "guaranteeing actor isolation at compile time with a type system" has **already been realized academically**. Sylvan Clebsch et al.'s Pony language (out of Imperial College London) does exactly this, with the paper *"Deny Capabilities for Safe, Fast Actors"* (AGERE 2015) as its theoretical backbone.

- Pony uses **reference capabilities** (`iso`, `val`, `ref`, `box`, `trn`, `tag`) — type qualifiers that express at the type level whether "this data is uniquely owned, immutable, or actor-local" — and eliminates data races at compile time.
- Its GC is also a "per-actor, independent concurrent GC" (no stop-the-world).

→ **Reinventing this without knowing it would be reinventing the wheel, so the right starting point is to build on Pony's reference-capability theory and clearly delineate the differences.** Rust's ownership is similar to but distinct from Pony's reference capabilities (Rust is ownership + borrowing; Pony is a capability lattice), so the first thing to validate is "how far Rust's type system can reproduce the equivalent of Pony."

### 2.3 What This Concept Has That Pony Lacks
Pony's runtime is home-grown, but **its design is not aware of CPU topology, cache lines, or NUMA** (it is a general-purpose scheduler). This is where the room for originality lies:

- **Mechanical Sympathy** (Martin Thompson, LMAX Disruptor): lock-free ring buffers, the single-writer principle, and cache-line-aware data layout — a philosophy proven in domains like financial trading systems.
- **Work-stealing vs. static placement**: Blumofe & Leiserson's (1999) work-stealing theory is strong on "load balancing" but sacrifices "cache locality." Statically pinning actors to cores is the opposite trade-off.
- The body of NUMA-aware scheduling research (placing data near the CPU that touches it).

### 2.4 Where the Moat Lies — the Differentiator Is Not "Speed" but "Types Unlocking Speed" (added 2026-07)

A sharper definition of the differentiator, derived from the competitive survey (`competitive-landscape.md`) and Stage 0 measurements:

- **Performance mechanisms (batching, per-core pools, emplace, etc.) are not a moat.** The moment you publish the numbers, they become a treadmill anyone can copy in one release. "Just fast" gets caught up to.
- **The moat is the type system.** If AetherFlow **proves `iso` (single ownership) at compile time**, then aggressive optimizations with no atomics and no GC become **structurally safe**. Competitors without a type system (glommio, where sharing is a possible footgun / kompact, which uses `Arc`) **cannot safely copy** that optimization → to copy it they would have to build the whole type system, which is the Pony-class, multi-year, high-cost part. **Only the side that has the types can turn the "safety unlocks speed" flywheel.**
- **Correction from last time**: "capabilities are performance-neutral" was true only in isolation. Their real value lies in **unlocking optimizations that others cannot safely copy**.
- **Theorem-style differentiation**: "static data-race freedom + zero-GC (no pauses) + no per-message Arc/refcount (the mailbox itself is lock-free and does use atomics)" — **being the only one to hold all of these simultaneously**. Pony is data-race free but pays for GC. The Rust actor crowd has neither.
  These can be presented not as benchmarks but as **provable properties**.
- **Safety unlocked by types (concrete example)**: **panic isolation** — because the type system guarantees that an actor's state is singly owned (`ref`, not shared), a panic in a handler can safely sever just that one actor and continue. `Arc<Mutex>`-style systems poison the Mutex on panic and cannot continue. "Isolation unlocks soundness" = something shared-state systems cannot copy. Likewise, **zero-allocation ask** (a stack reply cell + a linear `Responder`) is speed unlocked by the ownership model. Both are concrete cases of "safety/speed as a consequence of types."
- **Beyond where Pony stopped**: Pony proved capabilities but bound them to GC and a general-purpose scheduler. The frontier = **capabilities without GC (deterministic, pool-based) + co-designed with CPU topology**. Unoccupied theoretical territory.

**Discipline**: dig the moat with types. But dig deep only **after measuring** enough to know the performance frontier is reachable (avoid the ivory tower = a moat guarding a castle nobody wants). Use competitors to "learn the bar," and stop "chasing" them.

### 2.5 Design Constraint: Deep Theory, Shallow Surface (added 2026-07)

Digging the moat deep (§2.4) and being easy to use **must coexist**. The more theory you expose **on the user's surface**, the higher the learning cost, and people leave — **precisely the reason Pony, for all its theoretical beauty, stayed niche**.

- **Principle**: every addition of theory must pay for itself in "a guarantee or performance." **But it must not add a new concept or annotation the user has to learn to use the basic path.** If it would, hide it or drop it.
- **Realization**: as the `capability-mapping` conclusion shows, load-bearing capabilities can be reproduced in Rust **without annotations, structurally** (iso = move, val = `Arc<Sync>`, ref = `&mut self`, tag = `ActorRef`). The user writes **ordinary Rust**, and the guarantees come for free. The type system works under the floorboards; all the user sees is "unsafe code won't compile, and it's fast."
- **Example**: the per-core message pool has **the runtime transparently pool iso messages** (the user does nothing) = the moat stays, tax-free. Advanced knobs like "this actor is latency-critical" are quarantined into an **opt-in advanced API** and kept off the basic path (progressive disclosure).
- **Slogan**: **moat = the type system under the floorboards (dig deep) / surface = ordinary Rust (keep shallow).** The job of theory is "to have the compiler reject danger and unlock the fast path," not "to teach the user capability calculus."

### 2.6 The Reality of the Domain (added 2026-07)

A supplement to the finance analysis in `competitive-landscape.md`. **22µs (exchange-core p99.9) is not a "product rival" but a "bar of trust" and a "build-it-yourself custom."**

- Trading hot paths **do not use a base framework; they build their own** (in-house Disruptor/Aeron/Chronicle).
- Below that (sub-µs) is the realm of **kernel bypass / FPGA**, where a **software runtime fundamentally cannot reach**.
- → **The fastest elite HFT is not a customer** (they build their own, or use hardware). What we can target is the "**second tier that wants Disruptor-class speed + safety + productivity but can't afford an in-house team or FPGA**" (exchanges/brokers/real-time risk/market data/ad RTB). The value = "a safe path to get close to Disruptor-class speed without an HFT team."

## 3. The Technical Pillars (Four)

1. **Isolation by types**: building on Pony's reference-capability theory, mapping the range replaceable by Rust's ownership/Send+Sync into the type system.
2. **Zero-copy message move**: with `move` semantics, sending a message = transferring ownership, and the compiler guarantees "the sender can no longer touch it" (Akka copies by convention, but ownership makes that unnecessary).
3. **First-class physical placement**: actors do not end as logical concepts; they are statically pinned to a per-core `tokio::task::LocalSet` (or a hand-written scheduler), and mailboxes are SPSC ring buffers that never cross cores.
4. **Explicit resolution of the tension with Tokio**: a design that does not use the default work-stealing scheduler, but instead stands up as many single-threaded runtimes as there are cores and routes messages itself.

Tokio's default multi-thread scheduler assumes work-stealing (an idle worker thread steals and runs tasks from other threads) and deliberately moves tasks across cores for load balancing. This directly contradicts the idea of "pinning actors to specific cores to exploit cache locality." We need to use only parts of Tokio as components while keeping control of scheduling ourselves.

## 4. The First Validation Experiment (Minimal Scope to Avoid the Swamp)

Before building even one code example of the form "Akka would break without noticing, but this design won't compile," we first **validate, over about a week, how far Rust can imitate the Pony paper and the reference-capability implementation**. If this doesn't hold in the first place, the foundation collapses, so it is the highest-priority risk to crush.

## 5. Reading List
- Clebsch et al., *"Deny Capabilities for Safe, Fast Actors"* (AGERE 2015)
- Clebsch & Drossopoulou, *"Fully Concurrent Garbage Collection of Actors on Many-Core Machines"* (OOPSLA 2013)
- Hewitt, Bishop, Steiger, *"A Universal Modular Actor Formalism for Artificial Intelligence"* (1973)
- Blumofe & Leiserson, *"Scheduling Multithreaded Computations by Work Stealing"* (1999)
- Martin Thompson, "Mechanical Sympathy" blog + LMAX Disruptor technical papers

## 6. Related Material

- `docs/actor-model-theoretical-concepts.md` — a mathematical formalization of Hewitt's actor model, and the implementation issues of Thread Pool/Scheduler/Runtime (rayon, tokio, async-std) (2024-08, migrated from org-408/docs).
- `docs/akka-analyze.md` — a detailed analysis of Akka's internal structure (ActorRef → mailbox → Dispatcher, the Supervisor hierarchy, LifeCycle hooks) (2024-08, migrated from org-408/docs).

## 7. Past History (2024-10)

In October 2024, experiments of the same lineage were also carried out in separate repositories named `aether` / `aioncore` / `aion_core`, but none had progressed as far in implementation as this repository (`aetherflow`). Judging them to be conceptual duplicates, they were deleted in 2026-07 and consolidated into this repository.
