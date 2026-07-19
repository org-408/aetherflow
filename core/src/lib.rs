//! # AetherFlow — a multi-core, thread-per-core actor runtime for Rust
//!
//! You write **plain synchronous Rust**. An actor is "one message type + one handler"; the type
//! system proves isolation at compile time, so the hot path has no locks, no GC, and no per-message
//! `Arc`/refcount. New here? Start with the [guide](https://github.com/org-408/aetherflow/blob/main/docs/guide.md)
//! and the runnable examples in `core/examples/`.
//!
//! The four pillars:
//! - **Type-proven isolation / zero-copy move** — [`ActorRef::try_send`] takes `A::Message` by
//!   **move**; use-after-send is a compile error (`E0382`).
//! - **Physical placement** — [`System`] spawns **one pinned thread per core**, and each thread runs
//!   the many actors placed on that core to completion. Mailboxes are lock-free queues that never
//!   cross cores.
//! - **No work-stealing** — no Tokio; actors never migrate between cores (static placement).
//!
//! ## Thread-per-core, not thread-per-actor
//! One core = one OS thread runs **many actors** (spawning a thread per actor would not scale). On a
//! given core, differently-typed actors are type-erased to share one thread, while sends stay typed
//! through [`ActorRef`] — "erased control plane, typed data plane".
//!
//! ## Quick start
//! ```
//! use aetherflow::{System, Actor};
//! use std::sync::mpsc;
//!
//! struct Adder { sum: u64, out: mpsc::Sender<u64> }
//! impl Actor for Adder {
//!     type Message = u64;
//!     fn handle(&mut self, msg: u64) { self.sum += msg; self.out.send(self.sum).unwrap(); }
//! }
//!
//! let sys = System::with_cores(2);
//! let (out, results) = mpsc::channel();
//! let addr = sys.spawn_on(0, Adder { sum: 0, out });   // place on core 0
//! addr.send_blocking(10);
//! addr.send_blocking(5);
//! drop(addr);              // when all send handles drop, the actor leaves the core
//! sys.shutdown();          // stop every core and join
//! assert_eq!(results.recv().unwrap(), 10);
//! assert_eq!(results.recv().unwrap(), 15);
//! ```
//!
//! ## Isolation is enforced at compile time
//! ```compile_fail
//! use aetherflow::{System, Actor};
//! struct Order { qty: u32 }
//! struct E;
//! impl Actor for E { type Message = Order; fn handle(&mut self, m: Order) { let _ = m.qty; } }
//! let sys = System::with_cores(1);
//! let addr = sys.spawn_on(0, E);
//! let order = Order { qty: 100 };
//! addr.try_send(order).ok();     // ownership moved into the runtime
//! println!("{}", order.qty);     // use of moved value `order` (E0382): does not compile
//! ```

// SPSC is kept as a single-producer fast path / for future cross-core pair queues (Seastar-style).
// The current public API (System) uses the MPSC mailbox, so this is a reserved tested primitive.
#[allow(dead_code)]
mod spsc;
mod sync; // std/loom shim (swaps in loom types only during verification)

mod ask;
mod metrics;
mod mpsc;
mod system;
mod timer;

pub mod pinning;

/// I/O as messages (DRAFT, feature `net`): connection = actor, inbound bytes = messages, outbound =
/// a non-blocking handle. Portable scan reactor plus a Linux epoll backend and thread-per-core
/// serving. See `docs/io-surface-design.md`.
#[cfg(feature = "net")]
pub mod net;

pub use ask::{AskError, Responder};
pub use metrics::LatencySnapshot;
pub use system::{ActorRef, IdleStrategy, RestartPolicy, SchedulingPolicy, SpawnBuilder, System};
pub use timer::TimerHandle;

/// Why a non-blocking `try_send` failed. Both variants **return the original message** so the caller
/// can re-route, persist, or log it.
pub enum TrySendError<T> {
    /// The mailbox is full — **transient backpressure** (retry once it drains).
    Full(T),
    /// The receiving actor is gone (stopped on panic / restart limit / shutdown) — **permanently
    /// unsendable**.
    Closed(T),
}

impl<T> TrySendError<T> {
    /// Recover the message that came back on failure (for retry, persistence, or logging).
    pub fn into_message(self) -> T {
        match self {
            TrySendError::Full(m) | TrySendError::Closed(m) => m,
        }
    }
}

/// Why a blocking `send_blocking` failed. A full mailbox is waited on, so only `Closed` remains.
pub enum SendError<T> {
    /// The receiving actor is gone (permanently unsendable). Returns the original message.
    Closed(T),
}

impl<T> SendError<T> {
    /// Recover the message that came back on failure.
    pub fn into_message(self) -> T {
        match self {
            SendError::Closed(m) => m,
        }
    }
}

// Debug/Display do not depend on the message payload (no `T: Debug` bound), so `send*(..).unwrap()`
// works even for messages that carry a `Responder`.
impl<T> std::fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("TrySendError::Full(mailbox full)"),
            TrySendError::Closed(_) => f.write_str("TrySendError::Closed(actor gone)"),
        }
    }
}
impl<T> std::fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("mailbox full (backpressure)"),
            TrySendError::Closed(_) => f.write_str("actor is gone (permanently closed)"),
        }
    }
}
impl<T> std::error::Error for TrySendError<T> {}

impl<T> std::fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SendError::Closed(actor gone)")
    }
}
impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("actor is gone (permanently closed)")
    }
}
impl<T> std::error::Error for SendError<T> {}

/// A typed actor. The message type is fixed and `Send` (the type demands a sendable value).
pub trait Actor: Send + 'static {
    /// The type of message this actor receives.
    type Message: Send + 'static;

    /// Handle one message. `&mut self` is the sole owner of the actor's state, and a single core
    /// thread runs it to completion, so no lock is needed.
    fn handle(&mut self, msg: Self::Message);

    /// Called once after the actor is placed on a core, before it handles its first message.
    fn on_start(&mut self) {}
    /// Called right after a supervised actor is replaced with a fresh one following a panic
    /// (restart). Defaults to the same behavior as [`Actor::on_start`].
    fn on_restart(&mut self) {
        self.on_start()
    }
    /// Called once after all send handles drop and the mailbox is fully drained (or on system stop).
    fn on_stop(&mut self) {}
}
