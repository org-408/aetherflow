//! Zero-allocation request-reply (`ask`), plus bounded-wait [`ActorRef::ask_timeout`].
//!
//! The `ask` in kameo/tokio **heap-allocates a reply channel (oneshot) on every
//! call**. That is the hidden cost of `ask`, and one reason `ask` through a
//! framework is slow.
//!
//! [`ActorRef::ask`] places the reply cell on the **caller's stack** and hands it
//! to [`Responder`] as a raw pointer. The caller blocks until the reply arrives, so
//! **the cell is guaranteed to stay alive for the duration of the block** → zero
//! heap allocation. Safety is upheld by the protocol that "the caller blocks and
//! keeps the cell alive."
//!
//! [`ActorRef::ask_timeout`] bounds the wait. But if the caller returns on timeout,
//! its stack cell disappears, so an actor replying late would write to freed memory
//! (a use-after-free). To avoid that, **only the timeout path holds the cell in an
//! [`Arc`]** (one heap allocation); whichever side is last (caller or actor) frees
//! it. The fast path ([`ActorRef::ask`]) stays zero-alloc.
//!
//! [`Responder`] is a **linear token that can reply exactly once** (`reply`
//! consumes self) = iso-like. If it is dropped without replying, the caller wakes
//! up with [`AskError::NoReply`] (no deadlock).
//!
//! **Constraint**: both block the calling thread. **Asking an actor on the same
//! core from within a handler would deadlock**, so that situation is rejected with
//! [`AskError::WouldBlockCallingCore`].

use crate::sync::{AtomicU8, Arc, Ordering, UnsafeCell};
use crate::{Actor, ActorRef, SendError};
use std::mem::MaybeUninit;
use std::time::{Duration, Instant};

const PENDING: u8 = 0;
const REPLIED: u8 = 1;
const ABANDONED: u8 = 2;
// The caller has read REPLIED and moved the value out. A guard so the cell's Drop
// does not drop the value a second time (relevant mainly to the timeout Arc cell).
const CONSUMED: u8 = 3;

/// Reply slot. The actor writes it, the caller reads it. On the stack for `ask`,
/// in an [`Arc`] for `ask_timeout`.
struct ReplyCell<R> {
    state: AtomicU8,
    slot: UnsafeCell<MaybeUninit<R>>,
}

impl<R> ReplyCell<R> {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(PENDING),
            slot: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

impl<R> Drop for ReplyCell<R> {
    fn drop(&mut self) {
        // Drop the value here only if it was written (REPLIED) but never read out.
        // This is the timeout path's only occurrence: after the caller gives up and
        // drops its Arc, the actor replies late and was the last Arc holder. There
        // is no longer a recipient, so drop the written value (no leak, no UAF).
        // The fast path (stack cell) transitions to CONSUMED on read, so it never gets here.
        // &mut self = exclusive, so a Relaxed load reads the final state (no ordering needed).
        if self.state.load(Ordering::Relaxed) == REPLIED {
            // SAFETY: REPLIED = the slot is initialized. This cell owns it, and there is no other reader.
            self.slot.with_mut(|p| unsafe { (*p).assume_init_drop() });
        }
    }
}

// How the cell the Responder points to is held: the fast path borrows (raw pointer),
// the timeout path shares ownership (Arc).
enum Cell<R> {
    /// `ask`: borrows the cell on the caller's stack; the caller blocks to keep it alive.
    Borrowed(*const ReplyCell<R>),
    /// `ask_timeout`: shared via Arc, so the cell survives even if the caller leaves on timeout.
    Owned(Arc<ReplyCell<R>>),
}

impl<R> Cell<R> {
    #[inline]
    fn get(&self) -> &ReplyCell<R> {
        match self {
            // SAFETY(Borrowed): the caller is blocked, so the cell is alive (the fast-path invariant).
            Cell::Borrowed(p) => unsafe { &**p },
            Cell::Owned(a) => a,
        }
    }
}

/// Linear token that can reply exactly once (iso-like). `reply` consumes self.
pub struct Responder<R> {
    cell: Cell<R>,
}

// Access to the cell is synchronized to a single writer/reader via the state
// atomic (Release/Acquire). If R: Send, it is safe to hand across cores.
unsafe impl<R: Send> Send for Responder<R> {}

impl<R> Responder<R> {
    /// Reply (consumes the token).
    pub fn reply(self, value: R) {
        // Take the cell out without running Drop (the ABANDONED-on-no-reply CAS).
        // SAFETY: we forget self afterwards, so there is no double free.
        let cell = unsafe { std::ptr::read(&self.cell) };
        std::mem::forget(self);

        let cref = cell.get();
        // SAFETY: the caller does not touch the slot until it reads the state with Acquire. This is the only writer.
        cref.slot.with_mut(|p| unsafe { (*p).write(value) });
        // This Release store is the boundary that releases the caller. For Borrowed,
        // touching the cell afterwards could race the caller returning and freeing the
        // stack (UAF), so we finish using cref here.
        cref.state.store(REPLIED, Ordering::Release);

        // Drop the cell. Borrowed = a raw pointer, so this is a no-op (never touches the cell).
        // Owned = drops one Arc. If the actor is the last holder (the caller already left on
        // timeout), ReplyCell::Drop runs and discards the value that has nowhere to go.
        drop(cell);
    }
}

impl<R> Drop for Responder<R> {
    fn drop(&mut self) {
        // Only when dropped without replying, set ABANDONED to wake a waiting caller with an Err.
        // If the caller already left (timeout), the state may still be PENDING; the CAS may
        // succeed but no one observes it, and the cell is reclaimed on the Arc's final drop (harmless).
        let cref = self.cell.get();
        let _ =
            cref.state
                .compare_exchange(PENDING, ABANDONED, Ordering::Release, Ordering::Relaxed);
        // For Owned, the Arc is dropped by the normal drop of self.cell right after this.
    }
}

/// Reasons an `ask` can fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AskError {
    /// The actor dropped the message (Responder) without replying (stopped, dropped, panicked, etc. after send).
    NoReply,
    /// The receiving actor was already gone at send time (nowhere to send).
    /// Distinct from NoReply (died after the message was sent without replying).
    Closed,
    /// Asked an actor on the **same core** from within a handler = a situation that would deadlock if it blocked.
    /// Returns this error instead of hanging. If a cross-actor response is needed, place it on a different core or design with tell.
    WouldBlockCallingCore,
    /// [`ActorRef::ask_timeout`] did not receive a reply within the given time. The actor may still
    /// reply later, but that reply is discarded (the caller has already left).
    Timeout,
}

impl<A: Actor> ActorRef<A> {
    /// Request-reply. Passes a [`Responder`] to `make_msg` to build the message, sends it, and waits for the reply.
    /// **Zero heap allocation** (the reply cell lives on the caller's stack).
    ///
    /// Example: `let r: u64 = addr.ask(|resp| MyMsg::Get(resp))?;`
    ///
    /// **Note**: blocks the calling thread. Do not ask the same core from within a handler (deadlock).
    /// If the actor holds the [`Responder`] without replying, this **never returns**. To bound the
    /// wait, use [`ask_timeout`](Self::ask_timeout).
    pub fn ask<R>(&self, make_msg: impl FnOnce(Responder<R>) -> A::Message) -> Result<R, AskError>
    where
        R: Send + 'static,
    {
        if self.would_deadlock_calling_core() {
            return Err(AskError::WouldBlockCallingCore);
        }
        let cell = ReplyCell::<R>::new();
        let responder = Responder {
            cell: Cell::Borrowed(&cell),
        };
        let msg = make_msg(responder);
        if let Err(SendError::Closed(_)) = self.send_blocking(msg) {
            return Err(AskError::Closed);
        }

        let mut waited: u32 = 0;
        loop {
            match cell.state.load(Ordering::Acquire) {
                REPLIED => {
                    // SAFETY: observing REPLIED = the actor's write (Release) is visible. Move out exactly once.
                    let value = cell.slot.with_mut(|p| unsafe { (*p).assume_init_read() });
                    // Record that it was moved out, so the stack cell's Drop does not double-drop.
                    cell.state.store(CONSUMED, Ordering::Relaxed);
                    return Ok(value);
                }
                ABANDONED => return Err(AskError::NoReply),
                _ => backoff(&mut waited),
            }
        }
    }

    /// [`ask`](Self::ask) with an upper bound on the wait. Returns [`AskError::Timeout`] if no reply
    /// arrives within `timeout`.
    ///
    /// Guarantees the caller **never blocks forever** in cases where an actor keeps the [`Responder`]
    /// (a late reply, or a bug). Unlike the fast path [`ask`](Self::ask), it holds the reply cell in
    /// an [`Arc`], so it costs **one heap allocation** (the necessary price so a late reply after the
    /// timeout does not touch freed memory). If low latency with a guaranteed reply is the norm, use
    /// [`ask`](Self::ask).
    ///
    /// If the actor replies after the timeout fires, that value is discarded (the caller has left).
    pub fn ask_timeout<R>(
        &self,
        timeout: Duration,
        make_msg: impl FnOnce(Responder<R>) -> A::Message,
    ) -> Result<R, AskError>
    where
        R: Send + 'static,
    {
        if self.would_deadlock_calling_core() {
            return Err(AskError::WouldBlockCallingCore);
        }
        let cell = Arc::new(ReplyCell::<R>::new());
        let responder = Responder {
            cell: Cell::Owned(Arc::clone(&cell)),
        };
        let msg = make_msg(responder);
        if let Err(SendError::Closed(_)) = self.send_blocking(msg) {
            return Err(AskError::Closed);
        }

        let deadline = Instant::now().checked_add(timeout);
        let mut waited: u32 = 0;
        loop {
            match cell.state.load(Ordering::Acquire) {
                REPLIED => {
                    // SAFETY: observing REPLIED = the actor's write (Release) is visible. Move out exactly once.
                    let value = cell.slot.with_mut(|p| unsafe { (*p).assume_init_read() });
                    // Record that it was moved out, so the cell's (Arc) final Drop does not double-drop.
                    cell.state.store(CONSUMED, Ordering::Relaxed);
                    return Ok(value);
                }
                ABANDONED => return Err(AskError::NoReply),
                _ => {
                    // Give up past the deadline. One Arc to the cell is dropped here, but if the
                    // actor's Responder still holds one, the cell stays alive and the late reply is
                    // discarded safely.
                    let expired = match deadline {
                        Some(d) => Instant::now() >= d,
                        None => false, // timeout so large it overflows = effectively infinite
                    };
                    if expired {
                        return Err(AskError::Timeout);
                    }
                    backoff(&mut waited);
                }
            }
        }
    }
}

/// Backoff: spin → yield → short sleep. Keeps a blocking wait from burning CPU.
#[inline]
fn backoff(waited: &mut u32) {
    if *waited < 256 {
        std::hint::spin_loop();
    } else if *waited < 512 {
        std::thread::yield_now();
    } else {
        std::thread::sleep(Duration::from_micros(20));
    }
    *waited = waited.saturating_add(1);
}

// Loom model of the ReplyCell handshake. Loom can't run the real runtime, so we model the two
// participants directly: an "actor" thread that drives a `Responder` (reply or drop), and a
// "caller" thread that runs the same wait loop as `ask` (Acquire-load the state, read the slot on
// REPLIED, error on ABANDONED). Loom exhaustively explores the interleavings and, via its
// instrumented `UnsafeCell`, proves the slot is never accessed concurrently — i.e. the
// Release/Acquire discipline actually establishes the happens-before the unsafe code relies on.
// (UB / drop-exactly-once is Miri's job; ordering/visibility is Loom's.)
#[cfg(all(test, aetherflow_loom))]
mod loom_tests {
    use super::*;

    // The caller's wait loop, expressed with loom primitives (yield_now lets loom schedule the
    // other thread). Mirrors the REPLIED/ABANDONED handling in `ActorRef::ask`.
    fn caller_wait<R>(cell: &ReplyCell<R>) -> Option<R> {
        loop {
            match cell.state.load(Ordering::Acquire) {
                REPLIED => {
                    let v = cell.slot.with_mut(|p| unsafe { (*p).assume_init_read() });
                    cell.state.store(CONSUMED, Ordering::Relaxed);
                    break Some(v);
                }
                ABANDONED => break None,
                _ => loom::thread::yield_now(),
            }
        }
    }

    #[test]
    fn reply_delivers_value_race_free() {
        loom::model(|| {
            let cell = Arc::new(ReplyCell::<u32>::new());
            let responder = Responder {
                cell: Cell::Owned(Arc::clone(&cell)),
            };
            let actor = loom::thread::spawn(move || responder.reply(42));
            let got = caller_wait(&cell);
            actor.join().unwrap();
            assert_eq!(got, Some(42));
        });
    }

    #[test]
    fn drop_without_reply_wakes_caller() {
        loom::model(|| {
            let cell = Arc::new(ReplyCell::<u32>::new());
            let responder = Responder {
                cell: Cell::Owned(Arc::clone(&cell)),
            };
            let actor = loom::thread::spawn(move || drop(responder)); // no reply → ABANDONED
            let got = caller_wait(&cell);
            actor.join().unwrap();
            assert_eq!(got, None);
        });
    }
}

// Excluded under loom builds, since the real runtime (= loom types) cannot run outside the model.
#[cfg(all(test, not(aetherflow_loom)))]
mod tests {
    use super::*;
    use crate::System;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn ask_returns_reply() {
        struct Doubler;
        struct Req(u64, Responder<u64>);
        impl Actor for Doubler {
            type Message = Req;
            fn handle(&mut self, req: Req) {
                let Req(n, resp) = req;
                resp.reply(n * 2);
            }
        }
        let sys = System::with_cores(1);
        let addr = sys.spawn_on(0, Doubler);
        for n in 0..1000u64 {
            let r = addr.ask(|resp| Req(n, resp)).unwrap();
            assert_eq!(r, n * 2);
        }
        drop(addr);
        sys.shutdown();
    }

    #[test]
    fn ask_across_cores() {
        struct Echo;
        impl Actor for Echo {
            type Message = Responder<&'static str>;
            fn handle(&mut self, resp: Responder<&'static str>) {
                resp.reply("pong");
            }
        }
        let sys = System::with_cores(3);
        let addr = sys.spawn_on(2, Echo);
        assert_eq!(addr.ask(|resp| resp).unwrap(), "pong");
        drop(addr);
        sys.shutdown();
    }

    #[test]
    fn ask_errors_when_actor_drops_without_reply() {
        struct Rude;
        impl Actor for Rude {
            type Message = Responder<u64>;
            fn handle(&mut self, _resp: Responder<u64>) {}
        }
        let sys = System::with_cores(1);
        let addr = sys.spawn_on(0, Rude);
        assert_eq!(addr.ask(|resp| resp), Err(AskError::NoReply));
        drop(addr);
        sys.shutdown();
    }

    #[test]
    fn ask_timeout_returns_reply_when_prompt() {
        struct Doubler;
        struct Req(u64, Responder<u64>);
        impl Actor for Doubler {
            type Message = Req;
            fn handle(&mut self, req: Req) {
                let Req(n, resp) = req;
                resp.reply(n * 2);
            }
        }
        let sys = System::with_cores(1);
        let addr = sys.spawn_on(0, Doubler);
        for n in 0..1000u64 {
            let r = addr
                .ask_timeout(Duration::from_secs(5), |resp| Req(n, resp))
                .unwrap();
            assert_eq!(r, n * 2);
        }
        drop(addr);
        sys.shutdown();
    }

    #[test]
    fn ask_timeout_fires_when_responder_is_held() {
        // An actor that holds the Responder and never replies. The fast-path `ask` would block
        // forever, but ask_timeout returns Timeout.
        struct Hoarder {
            held: Vec<Responder<u64>>,
        }
        impl Actor for Hoarder {
            type Message = Responder<u64>;
            fn handle(&mut self, resp: Responder<u64>) {
                self.held.push(resp); // hold it, do not reply
            }
        }
        let sys = System::with_cores(1);
        let addr = sys.spawn_on(0, Hoarder { held: Vec::new() });
        let t = Instant::now();
        let r = addr.ask_timeout(Duration::from_millis(50), |resp| resp);
        assert_eq!(r, Err(AskError::Timeout));
        assert!(t.elapsed() >= Duration::from_millis(50));
        drop(addr);
        sys.shutdown();
    }

    #[test]
    fn ask_timeout_late_reply_is_dropped_without_uaf() {
        // The actor replies late, after the timeout fires. The cell is an Arc, so there is no UAF,
        // and the value with nowhere to go is dropped exactly once by the cell's Drop (no leak, no
        // double drop).
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        struct Tracked;
        impl Drop for Tracked {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::SeqCst);
            }
        }

        struct SlowThenReplies {
            held: Vec<Responder<Tracked>>,
        }
        enum Msg {
            Ask(Responder<Tracked>),
            Flush,
        }
        impl Actor for SlowThenReplies {
            type Message = Msg;
            fn handle(&mut self, msg: Msg) {
                match msg {
                    Msg::Ask(resp) => self.held.push(resp),
                    Msg::Flush => {
                        for resp in self.held.drain(..) {
                            resp.reply(Tracked); // a late reply after the timeout
                        }
                    }
                }
            }
        }

        let sys = System::with_cores(1);
        let addr = sys.spawn_on(0, SlowThenReplies { held: Vec::new() });

        // R = Tracked has no Debug/PartialEq, so match with matches!.
        let r = addr.ask_timeout(Duration::from_millis(30), Msg::Ask);
        assert!(matches!(r, Err(AskError::Timeout)));
        assert_eq!(
            DROPS.load(Ordering::SeqCst),
            0,
            "not replied yet, so nothing is dropped"
        );

        // Now make it reply late. The value has nowhere to go, so the cell's Drop drops it exactly once.
        assert!(addr.send_blocking(Msg::Flush).is_ok());

        // Wait for Flush to be processed and the late reply's value to be dropped.
        let t = Instant::now();
        while DROPS.load(Ordering::SeqCst) == 0 && t.elapsed() < Duration::from_secs(2) {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            DROPS.load(Ordering::SeqCst),
            1,
            "the late reply's value is dropped exactly once"
        );

        drop(addr);
        sys.shutdown();
    }
}
