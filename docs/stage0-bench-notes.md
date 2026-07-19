# Stage 0 Bench Notes

> 🌐 English | [日本語](stage0-bench-notes.ja.md)


> The decisive battleground for `direction-and-roadmap.md` #6. A record and interpretation of running `core/benches/latency.rs`.

## ★★ Authoritative measurements v2: overall-strength validation (2026-07-14) = **wins across every percentile** vs tokio/kameo/kompact/glommio

An overall-strength validation that widens the field of comparison to include "the fast implementations in the same genre." Measured on **two real Linux AWS Graviton3 machines**:

- **`c7g.large`** — 2 dedicated vCPUs / Ubuntu 24.04 / `taskset -c 0,1` (Tier1 = ordinary boot without isolcpus)
- **`c7g.metal`** — bare metal / IRQs excluded from cores 0-1 at runtime / `taskset -c 0,1` (**no isolcpus**; see "Finding" below)

`samples=200,000` / `warmup=20,000` / `throughput_n=2,000,000`, release + LTO.

### ping-pong RTT (ns) — `c7g.metal` (bare metal)
| runtime | p50 | p90 | p99 | p999 | jitter(p99/p50) |
|---|--:|--:|--:|--:|--:|
| **aether-spin** | **709** | **797** | **1195** | **4911** | 1.7 |
| aether-backoff | 688 | 738 | 1035 | 4844 | 1.5 |
| kompact | 4965 | 5394 | 6305 | 8731 | 1.3 |
| tokio | 4325 | 6013 | 10992 | 12582 | 2.5 |
| kameo | 6536 | 11158 | 11853 | 16161 | 1.8 |
| glommio | 19043 | 19387 | 20971 | 22677 | 1.1 |

### ping-pong RTT (ns) — `c7g.large` (dedicated vCPU)
| runtime | p50 | p90 | p99 | p999 | jitter(p99/p50) |
|---|--:|--:|--:|--:|--:|
| **aether-spin** | **575** | **879** | **979** | **3119** | 1.7 |
| aether-backoff | 567 | 762 | 933 | 3144 | 1.6 |
| kompact | 4769 | 5912 | 6801 | 10188 | 1.4 |
| tokio | 5611 | 10148 | 10980 | 14332 | 2.0 |
| kameo | 11661 | 11908 | 14030 | 16944 | 1.2 |
| glommio | 20644 | 21029 | 24559 | 26764 | 1.2 |

→ **A clean sweep across every percentile.** Against glommio on large: 36x at p50 / 25x at p99 / 8.6x at p999.

### one-way throughput (msgs/sec)
| runtime | c7g.metal | c7g.large |
|---|--:|--:|
| **aether** | **37,600,935** | **38,232,513** |
| glommio | 21,855,834 | 24,699,078 |
| kompact | 13,300,173 | 13,602,329 |
| tokio | 6,918,156 | 6,993,581 |

On throughput we were initially **losing** on Docker, with glommio 13.8M > aether 12.6M. We flipped it by adding **batch-drain** to the core loop
(processing up to `BATCH_DRAIN=128` messages in a row per actor visit = amortizing the control-loop overhead).
Note there is a trade-off: throughput ↔ fairness (a single actor can monopolize the core for up to 128 messages). The default is 128, but it is tunable via
`System::with_policy(n, SchedulingPolicy::default().batch_drain(8))` (`1` = one message per visit = the fairest).

### The decisive finding: the ping-pong tail was the benchmark's own park
| aether-**ask** (ns) | p50 | p90 | p99 | **p999** | jitter |
|---|--:|--:|--:|--:|--:|
| c7g.metal | 335 | 379 | 393 | **396** | 1.2 |
| c7g.large | 300 | 334 | 353 | **410** | 1.2 |

- The reason ping-pong's p999 stretches to 4.9µs (metal) / 3.1µs (large) is **an artifact of the benchmark**: because ping-pong receives the reply over a std channel,
  **the main thread parks/wakes**. That wake was creating the tail.
- `ask` is fully busy-spin (no park) = **the runtime's native path**. Its p999 is **396ns / 410ns = sub-microsecond**, and
  consistent between large and metal. Against glommio's p999 of 22,677ns, that is **57x**.
- → **isolcpus was unnecessary.** What actually drove the absolute tail was not "isolated cores" but "a path that does not park."
  (We attempted isolcpus on Tier2 but it failed to boot. Yet as shown above, **the number we were after was already achieved without isolcpus**.)
- Also: the tail blowups (13ms) we observed on Docker are now confirmed to be **caused by virtualization preemption** — they vanished on dedicated cores.
  The premise that busy-spin presupposes dedicated cores was borne out on real hardware.

### Caveats (to avoid overstating)
- **This is not an apples-to-apples comparison.** aether busy-spins (low latency ↔ burns CPU), while tokio/kameo/glommio park/wake.
  This is a difference in design philosophy, and "a perfectly fair identical setup" does not exist. Instead, **both are measured in their natural form**
  (glommio uses 2 executors + `shared_channel` for a **cross-thread** setup; we did not rig it as a same-thread comparison).
- **This is the message-passing arena.** glommio's true strength is async I/O (io_uring), which is **unmeasured = a different arena**.
  The correct claim is not "beat glommio at everything" but "**won at message passing**."
- The `max` column (omitted from the tables) includes first-time costs such as executor startup and spikes to 1.8-2.7ms for glommio. It is only a reference value.
- Since kameo only has an ask API, the kameo row in the ping-pong table shows the same values as the ask table.

---

## ★ Authoritative measurements: real AWS Linux (2026-07-04) = go/no-go is **GO**

Measured on **AWS `c7g.large` (Graviton3 / aarch64 / 2 vCPU / Ubuntu 24.04, real Linux where native pinning works.
Tier 1 = ordinary boot without isolcpus)**. Zero hardware purchase, ~$0.02.

### ping-pong RTT (ns), jitter = p99/p50
| runtime | p50 | p99 | **p999** | max | jitter |
|---|--:|--:|--:|--:|--:|
| **aether-spin** | 618 | 995 | **4,805** | 45,077 | 1.6 |
| **aether-backoff** | 554 | 728 | **3,322** | 162,646 | 1.3 |
| tokio | 5,835 | 12,343 | 15,009 | 632,243 | 2.1 |
| kameo | 11,338 | 13,387 | 17,042 | 2,548,022 | 1.2 |

### ask RTT (ns) — zero-alloc vs kameo's per-call oneshot
| runtime | p50 | p99 | p999 | jitter |
|---|--:|--:|--:|--:|
| **aether-ask** | **268** | **392** | **399** | 1.5 |
| kameo-ask | 11,338 | 13,387 | 17,042 | 1.2 |

throughput: **aether 30.4M msg/s vs tokio 6.6M (~4.6x)**.

### The decisive finding
- **The tail closed up completely**: p999 went from **2.5ms on macOS/Docker → 3.3–4.8µs (~500x improvement)**.
  Confirmed that the busy-spin tail blowup was **entirely a virtualization artifact** (exactly as hypothesized).
  **And this is even on Tier 1 (no isolcpus).**
- **A landslide across every percentile**: aether beats tokio by ~10x at median, ~13x at p99, and **~3–4.5x at p999**, with better jitter too (1.3 vs 2.1).
  Where macOS gave us a tail tie and an unfavorable jitter, on real Linux **it all flipped to our advantage**.
- **Zero-alloc ask is extraordinarily strong**: **p50 268ns / p999 399ns** (sub-µs, an almost flat tail).
  It is **~42x** faster than kameo ask (which allocates on the heap), and the tail is ~43x tighter. A demonstration of the speed unlocked by the ownership model.
- **We already have a foothold in exchange-core's territory (p99.9 22µs)** (aether-backoff p999 is 3.3µs; a different workload, but the same order of magnitude or better).

### Implications
- **The thesis holds up on real hardware.** competitive-landscape's "is the win big enough" bar is clearly cleared in multiples.
- **We already hit these numbers on Tier 1 (ordinary cloud)** → the marketing banner "a landslide across every percentile against Tokio/kameo, while staying safe in the cloud" holds. **We can fight cloud-first.**
- Next is **Tier 2 (c7g.metal + isolcpus)** to crush the tail further toward the median and see the ceiling of HFT class (single-digit-µs p99.9).
- Caveat: aether busy-spins (low latency ↔ CPU). A single-actor microbenchmark. Real workloads (fan-out, many-to-many, large messages) and HoL blocking are separate.

---

## Preliminary results (macOS, Apple Silicon 16 logical cores, no-pin, release+LTO)

## Preliminary results (macOS, Apple Silicon 16 logical cores, no-pin, release+LTO)

`cargo bench --bench latency`, representative values from 2 runs. ping-pong is a single round-trip RTT (ns); throughput is
single producer→single consumer in msgs/sec.

### ping-pong RTT (ns), jitter = p99/p50
| runtime | p50 | p90 | p99 | p999 | max | jitter |
|---|--:|--:|--:|--:|--:|--:|
| **aether** (thread-per-core) | ~3,000 | ~3,200 | ~26,000 | ~66,000 | spiky (0.1–2.7M) | ~9.0 |
| tokio raw channel (work-stealing) | ~9,600 | ~11,000 | ~34,000 | ~57,000 | spiky | ~3.6 |
| kameo (real actor FW, on Tokio) | ~10,400 | ~11,900 | ~36,000 | ~62,000 | spiky | ~3.5 |

### one-way throughput (msgs/sec)
| runtime | throughput |
|---|--:|
| **aether** | ~26.7M |
| tokio | ~6.9M |

## Interpretation (honestly)

- **In absolute terms, aether beats both Tokio and kameo at p50/p90/p99** (p50 is ~3.2x tokio,
  **~3.6x the real actor framework kameo**, p99 is 26µs vs 34–36µs). **Throughput is about 3.9x Tokio's raw
  channel.** "Not 5% but multiples" = competitive-landscape's "is the win big enough" bar is
  **preliminarily cleared** in this range. kameo ≈ tokio (slightly slower by the framework's overhead, as expected).
- **However, the extreme tail (p999) ties on macOS (~60µs)**, and max is spiky for both.
  → As predicted: **without hardware pinning the OS migrates threads, caches go cold, and the tail spikes.**
- **The jitter ratio (p99/p50) is worse for aether (~8.5 vs ~3.5)**. This is because aether's median is extremely low,
  so the relative spread looks larger (in absolute p99, aether wins). But given that **stable tails are the very
  selling point**, **the relative jitter not tightening up is a weakness on macOS**. The **guarantee** of the p99/p99.9
  that matching engines pay for is **unproven in this environment**.

### Effect of macOS QoS (2026-07-04 follow-up) — this box is an Intel Mac
Implemented `pinning` to request `USER_INTERACTIVE` QoS on macOS. However, **this box is an Intel Mac with
no P/E cores**, so QoS has no effect of "steering toward P cores" (harmless, but does not tighten the tail).
→ **The tail does not tighten noticeably** (as expected). The median/throughput wins are retained. QoS matters on
Apple Silicon. The conclusion — **tail improvement requires hardware pinning (= isolated cores on real Linux)** — is unchanged.

## Zero-alloc ask (request-reply) showdown (2026-07-04, moat #1)

`ask` = send + wait for reply. kameo/tokio **allocate a reply channel (oneshot) on the heap per call**.
We place the reply cell **on the call stack** and pass it by raw pointer to a `Responder` (a linear token that replies exactly once = iso-like)
(`core/src/ask.rs`) = **zero heap allocation**.

| runtime | p50 | p99 | jitter(p99/p50) |
|---|--:|--:|--:|
| **aether-ask** (zero-alloc) | **~1,000 ns** | **~1,500 ns** | **~1.5** |
| kameo-ask (allocates a oneshot each call) | ~10,000–96,000 ns | collapses (10ms class) | 4–105 |

- **~10–90x faster at median** (this measurement run was skewed high because kameo degraded especially badly under concurrent machine load; even idle,
  kameo ask is ~10µs, so **conservatively still ~10x**).
- **Jitter is stable by orders of magnitude** (1.5 vs 4–105). Because aether-ask is a tight spin on a stack cell,
  it holds ~1µs even under load (even more stable and faster than the tell-pingpong measurement that uses std sync_channel).
- **Meaning**: a demonstration that "the type/ownership model **structurally** produces an ask faster than the framework's." And the API is
  ordinary: `addr.ask(|resp| Msg(resp))?` (deep theory, shallow surface). **The moat = the first concrete
  deliverable of the type system** (design.md §2.4).

## Caveats on conditions (to avoid overstating)

- **aether's consumer busy-spins** (LMAX style). In exchange for low latency it burns 1 core even when idle.
  Tokio uses no CPU thanks to park/wake. **This is not an apples-to-apples comparison** → read it as a
  "latency ↔ CPU/power" trade-off. A park option (spin → short sleep) is worth considering in the future.
- A single-actor, single-producer microbenchmark. Real workloads (fan-out, many-to-many, large messages) are separate.
- macOS does not support hardware pinning (no-op). **This is an OS problem, not an ARM problem** (ARM Linux/Graviton can pin).

## Linux container measurements (Docker on Mac, ARM64 VM, cpuset 0-3 = 4 cores, 2026-07-04)

Measurements from `./core/scripts/bench-linux.sh`. An important result: **the median gap widens versus macOS, but the tail blows up.**

### ping-pong RTT (ns)
| runtime | p50 | p90 | p99 | p999 | max | jitter |
|---|--:|--:|--:|--:|--:|--:|
| **aether** | ~7,900 | ~8,600 | ~34,000 | **~2,540,000** | ~4,150,000 | 4.3 |
| tokio | ~112,000 | ~174,000 | ~277,000 | ~402,000 | ~22M | 2.5 |
| kameo | ~114,000 | ~174,000 | ~275,000 | ~381,000 | ~29M | 2.4 |

throughput: aether ~21.7M msg/s vs tokio ~5.9M.

### Interpretation (important, some of it inconvenient)
- **aether is ~14x faster at median and ~8x at p99.** The fewer the cores (the stronger the contention), the wider
  thread-per-core's advantage = corroborating that work-stealing struggles under contention. **The median/throughput win is even stronger on Linux.**
- **But the tail blows up, with p999 = 2.5ms and max = 4.1ms** (~6x worse than tokio's p999 of 0.4ms).
  → Cause: **busy-spin is a double-edged sword.** In a virtualized environment (Docker = lightweight VM), the hypervisor
  occasionally preempts the spinning core thread for milliseconds at a time, and with no park/wake the recovery lags, so the tail spikes.
  On bare-metal **isolated cores** (isolcpus/nohz_full, no oversubscription) this is unlikely, but
  **Mac's Docker VM does not meet those conditions.**
- **Two lessons**: (a) an authoritative tail still requires **isolated cores on real hardware** (even a Docker VM is insufficient).
  (b) **Pure busy-spin is not robust** → a **hybrid idle strategy** of spin → yield → short park is needed.

### Effect of the hybrid idle strategy (`IdleStrategy::Backoff`, same container measurement)
Implemented `spin 128 → yield 128 → park 50µs` and compared against busy-spin.

| runtime | p50 | p90 | p99 | p999 | max |
|---|--:|--:|--:|--:|--:|
| aether-**spin** | ~7,900 | ~9,600 | ~37,000 | **~2,580,000** | ~4,300,000 |
| aether-**backoff** | ~8,700 | ~17,000 | ~49,000 | **~128,000** | ~1,440,000 |
| tokio | ~123,000 | ~142,000 | ~218,000 | ~393,000 | ~25M |
| kameo | ~144,000 | ~175,000 | ~242,000 | ~443,000 | ~19M |

- **backoff improves p999 by 20x (2.6ms → 128µs).** The cost is only +10% median and +34% p99.
- **backoff beats tokio/kameo across every percentile** (median ~14x, p99 ~4x, **p999 ~3x too**).
  → Even under virtualization it can combine "speed" and "tail robustness." We designed it to **use busy-spin when
  monopolizing isolated cores, and backoff when shared/virtualized** (the default is BusySpin, prioritizing latency).
- Still, **the absolute p999 (128µs and up) does not reach the matching-engine target (p99.9 22µs)**. This is largely
  due to container-virtualization jitter, and the conclusion — **a re-measurement on isolated cores on real hardware is needed** — is unchanged.

## To feel the tail on a Mac (via Docker)

macOS native does not tighten the tail, but **on a Linux container on the Mac, `sched_setaffinity` works**.
Start Docker Desktop and:

```sh
cd core && ./scripts/bench-linux.sh
```

This runs the same `cargo bench` inside a `rust:slim` container (with a dedicated cpuset). The container CPU is
virtualized, so it is not as accurate as bare metal, but **it is closer to real Linux behavior than macOS native** —
if the pinning effect shows up in p999 here, the core of the differentiation becomes (tentatively) visible. Authoritative numbers come from real hardware/VMs.

## Next (authoritative Stage 0)

1. **Re-measure on Linux** (ideally a cheap ARM Graviton VM). Since `core_affinity` already branches per OS, the
   same `cargo bench` will do. The real question here is **whether the pinning effect shows up in the tail (p99/p99.9)**.
   If it does, the core of the differentiation is demonstrated; if not, we learn that "our added value is not tail guarantees" → strategy rethink.
2. **Add competitors**: kameo/actix (baseline) → **kompact** (challenging on latency) → **glommio + a thin actor**
   (the main event for delta). The methodology reuses kompicsbenches.
3. **Financial North Star**: our standing against order→match at p99 4µs / p99.9 22µs (exchange-core).
4. **Measure jitter explicitly** (p999/p50 ratio, etc.). Stable tails are the very selling point.

## Related
- `competitive-landscape.md` — the benchmark's targets (opponents, axes, North Star)
- `core/benches/latency.rs` — the measurement code
- `README.md` — runtime architecture and known constraints
