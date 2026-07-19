# Concepts, Explained Gently (Glossary)

> 🌐 English | [日本語](concepts-explained.ja.md)


> Where this document fits: `pony-rust-capability-mapping.md` is theoretically accurate but lofty and hard to read.
> This document is its **plain-language version**. It sets the jargon aside for a moment and lines up "meaning, use, pros and cons" using everyday analogies.
> It's the home base to **come back to** whenever you get lost. Only reach for the capability-mapping doc when you need a rigorous argument.

---

## 0. First, in One Sentence (if you remember nothing else)

> **"If you only use data that isn't shared with anyone else, that data can stay put on a single CPU core.
> Staying put keeps the cache (the fast memory close at hand) warm, and warm is fast."**

Pony's capability theory (the type system enforces "not shared") and the CPU story (pin to a core to go fast) are
**two sides of the same coin**. Forbid sharing with types → the data physically doesn't move → fast.
Conversely, the moment you reach for `Arc` (sharing) or a lock, the data starts bouncing between cores, the cache goes cold, and things slow down.

---

## 1. Six Ways to "Handle" Data (Pony's reference capabilities)

Think of data as a **"thing."** There are six sets of rules for who can hold it, read it, and write it.

| Capability | In a word | Everyday analogy | In Rust | Can hand off? | Where it's used |
|---|---|---|---|:--:|---|
| `iso` | Hand off the one and only original | Hand over the original of an envelope. **Once handed off, you no longer have it** | move an `owned T` | ✅ | **the message body** |
| `val` | An immutable notice everyone can read | A stone monument. Anyone can read it, but **no one can rewrite it** | `Arc<T: Sync>` | ✅ | config distributed to everyone, etc. |
| `ref` | The whiteboard in your own room | You read and write it yourself, but **it can't leave the room** | `&mut self` (own state) | ❌ | **the actor's own state** |
| `box` | Just looking at someone else's board | You peek and read, but can't write | `&T` | ❌ | read-only borrow |
| `tag` | You only know the phone number | You can make the call, but **you can't see the contents** | `ActorRef` | ✅ | a reference to an actor (for sending) |
| `trn` | A draft only you can write | Others can read it. Once finalized, it becomes a stone monument (`val`) | weak correspondence | ❌ | **not used this time** |

### The key pros and cons
- `iso` (the handed-off original) = **fast and safe (because it isn't shared)**. Downside: once you hand it off, you can't use it (but that's the whole point).
- `val` (the stone monument) = **any number of readers at once, safely**. Downside: once created, it can't be rewritten.
- `ref` (your own room's board) = **freely writable without locks**. Downside: it can't leave (i.e., stays inside the actor).
- `box` (looking at someone else's board) = safe if you only read. Downside: you can't write.
- `tag` (the phone number) = **you can reach out without knowing the contents**. Downside: you can't read what's inside (but that's enough).
- `trn` (the draft) = you can assemble it mutably, then freeze it into an immutable form. Downside: Rust has no clean correspondence → **set aside**.

### Why only `iso`/`val`/`tag` are "sendable"
Data races happen because **two or more parties write to the same thing at the same time**. So:
- `iso` = always narrows the writers down to **exactly one** (once handed off, nothing remains with you)
- `val` = **no one writes** (a stone monument)
- `tag` = **no one is allowed to touch the inside** (a phone number)

None of these three can possibly race → no locks are needed at all → which is why they can be safely handed between actors.
And **Rust's type system can enforce "once handed off, you can't touch it anymore" at compile time** = turning this from a "convention" into a "guarantee."

```
The dangerous shape (Arc + Mutex / the current implementation):
  actor A (core 0) ──writes──▶ same data ◀──writes── actor B (core 1)
                          = a data race. Guarding it means a lock = slow

The safe shape:
  iso = the handed-off original (the writer is always exactly one)
  val = the stone monument (no one writes)
  tag = the phone number (no one looks inside)
```

---

## 2. CPU-Level Concepts (including the "CPU concepts to set aside")

A CPU has **main memory (RAM, slow)** and a **cache (right next to the core, blazingly fast)**.
Whether you can keep frequently used data in the cache is what decides your speed.

| Concept | In a word | Everyday analogy | Pro | Con / caveat |
|---|---|---|---|---|
| Cache locality | Touching the same data on the same core over and over is fast | Keep your frequently used tools **within reach** | Orders of magnitude faster | Ruined the moment locality breaks down |
| Core pinning | Fix an actor to a specific core | Give each person a **fixed seat** | The cache stays warm | If load is uneven, some cores sit idle |
| work-stealing | An idle thread grabs work from others | A free hand snatches up work | Good at load balancing | actors fly between cores and **the cache goes cold** → **not adopted** |
| SPSC ring buffer | A queue dedicated to one writer and one reader | A **dedicated checkout** (only one line forms) | Lock-free and fastest | Limited to one-to-one (many-to-one takes extra effort) |
| NUMA | Memory is split per CPU socket; far memory is slow | A warehouse in another building. The near warehouse is fast | Pays off at large scale | Far memory is slow and complex → **deferred in v1** |
| false sharing | Unrelated data shares the same cache line and gets needlessly invalidated | Two strangers share a table, and when one stands, **both are made to stand** | (workaround = padding) | An easy-to-miss trap → address it during optimization |

### NUMA in detail (why it's frozen for v1)
**NUMA = Non-Uniform Memory Access.** On a large server (two or more CPU sockets), the memory physically
hangs off each socket. When a core touches **the memory on its own socket** it's fast; when it touches **memory on another socket**,
it goes through the inter-socket interconnect (Intel UPI / AMD Infinity Fabric) and is slow. Speed changes based on "which memory, from where"
= Non-Uniform. Analogy: two office buildings, each with its own warehouse. Your building is fast, the next building is a walk.

What it means for us: if you pin an actor to a core on socket 0 but its state / mailbox memory sits on socket 1, then every time
it's "a walk to the next building" = the cache-locality win is undone. NUMA-aware = allocate memory on the same socket as the core.

**Why it's frozen:**
1. **NUMA only exists on multi-socket large servers.** Single-socket machines (dev boxes, small cloud instances,
   laptops, Intel Macs) have a single NUMA node = uniform memory = **no-op**. We don't even have the hardware to touch yet.
2. v1 is single-node. We haven't even proven the single-socket tail win (Stage 0). NUMA is a secondary effect that comes after that.
3. The machinery is heavy (topology detection, per-node allocators, placement policy, cross-NUMA). All for hardware we aren't targeting.
4. The "measure before you add" discipline. We don't build it until a real multi-socket workload shows it to be the bottleneck.
→ **Frozen = a matter of ordering, not abandonment.** A note: lately, single-socket many-core parts like Graviton/EPYC are
becoming more common, and the very need for multi-socket is shrinking (even on a single socket it shows up faintly via chiplet / sub-NUMA, but that's secondary).

### Why dropping sharing makes things fast
```
Not adopted (Arc sharing + work-stealing):
  core 0 (cold) ─stolen, moves→ core 1 (cold) ─stolen, moves→ core 2 (cold)
  each move re-fetches from main memory → a cold cache every time → slow

Adopted (move + core pinning):
  core 0 (fixed seat) … the actor stays right here … cache warm
  the SPSC mailbox is on the same core too → lock-free → fast
```

**The type story = the physics story**: enforce `iso` (not shared) with the type system → the data doesn't move → it can be pinned to a core →
the cache stays warm. "Guaranteeing isolation at compile time" and "physical speed" being two sides of the same coin
is what the claim in design.md is really about.

---

## 3. What v1 Uses / What Gets Set Aside (deferred)

| | Type concepts | CPU concepts |
|---|---|---|
| ✅ **Used in v1** | `iso` (messages) / `val` (immutable sharing) / `tag` (references) / `ref` (own state) | cache locality / core pinning / SPSC ring buffer |
| ⏸ **Set aside (deferred, not used)** | `trn` / `recover` / viewpoint adaptation | **work-stealing (not adopted)** / NUMA (when multi-node) / false sharing mitigation (during optimization) |

### Reasons for setting aside (in a sentence each)
- `trn` / `recover` / viewpoint adaptation = Pony's advanced features for handling "fine-grained sharing inside an object."
  **A runtime that just passes messages around doesn't need them**, so you can forget about them for now.
- **work-stealing = deliberately not adopted**. It's the mechanism Tokio uses for speed — "flinging actors between cores" —
  which is exactly the culprit that cools the cache. **Throwing this out is the heart of this design.**
- NUMA / false sharing = they don't show up at the stage of measuring speed on a single node (Stage 0). **Later optimization topics.**

→ It's enough to remember just **the 7 we use**. The `trn`/`recover`/viewpoint sections in capability-mapping are
footnotes saying "Pony has them, but **we don't use them**," so you can skip past them.

---

## 4. Related Documents
- `pony-rust-capability-mapping.md` — the rigorous version of this document (the theoretical details). Go here when you need precision.
- `design.md` — the technical thesis (the four pillars). §2.2 covers Pony, §4 covers the top-priority risks.
- `direction-and-roadmap.md` — direction and the path forward. This glossary is the home base for making "why this design" click.
