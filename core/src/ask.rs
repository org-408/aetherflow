//! Zero-allocation request-reply (`ask`).
//!
//! The `ask` in kameo/tokio **heap-allocates a reply channel (oneshot) on every
//! call**. That is the hidden cost of `ask`, and one reason `ask` through a
//! framework is slow.
//!
//! This implementation places the reply cell on the **caller's stack** and hands
//! it to [`Responder`] as a raw pointer. The caller blocks until the reply
//! arrives, so **the cell is guaranteed to stay alive for the duration of the
//! block** → zero heap allocation. The raw pointer is needed because the message
//! (`A::Message: Send + 'static`) cannot carry a borrow. Safety is upheld by the
//! protocol that "the caller blocks and keeps the cell alive."
//!
//! [`Responder`] is a **linear token that can reply exactly once** (`reply`
//! consumes self) = iso-like. If it is dropped without replying, the caller wakes
//! up with [`AskError::NoReply`] (no deadlock).
//!
//! **Constraint**: `ask` blocks the calling thread. **Asking an actor on the same
//! core from within a handler deadlocks** (same as `send_blocking`). Use it from
//! outside the runtime (the main / I/O threads).

use crate::{Actor, ActorRef, SendError};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

const PENDING: u8 = 0;
const REPLIED: u8 = 1;
const ABANDONED: u8 = 2;

/// Reply slot placed on the caller's stack. The actor writes it, the caller reads it.
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

/// Linear token that can reply exactly once (iso-like). `reply` consumes self.
pub struct Responder<R> {
    cell: *const ReplyCell<R>,
}

// Access to the cell is synchronized to a single writer/reader via the state
// atomic (Release/Acquire). If R: Send, it is safe to hand across cores.
unsafe impl<R: Send> Send for Responder<R> {}

impl<R> Responder<R> {
    /// Reply (consumes the token).
    pub fn reply(self, value: R) {
        // SAFETY: the caller blocks until it observes REPLIED with Acquire, so the cell is alive.
        unsafe {
            (*(*self.cell).slot.get()).write(value);
            // This Release store is the boundary that releases the caller. The cell
            // must not be touched afterwards (the caller may return and free the
            // cell (on the stack) = a use-after-free).
            (*self.cell).state.store(REPLIED, Ordering::Release);
        }
        // Hence do not run Drop (the CAS to ABANDONED). Drop only performs the CAS
        // when the token is dropped without replying.
        std::mem::forget(self);
    }
}

impl<R> Drop for Responder<R> {
    fn drop(&mut self) {
        // Only when dropped without replying, set ABANDONED to wake the caller with an Err.
        // SAFETY: the caller keeps the cell alive by blocking.
        unsafe {
            let _ = (*self.cell).state.compare_exchange(
                PENDING,
                ABANDONED,
                Ordering::Release,
                Ordering::Relaxed,
            );
        }
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
}

impl<A: Actor> ActorRef<A> {
    /// Request-reply. Passes a [`Responder`] to `make_msg` to build the message, sends it, and waits for the reply.
    /// **Zero heap allocation** (the reply cell lives on the caller's stack).
    ///
    /// Example: `let r: u64 = addr.ask(|resp| MyMsg::Get(resp))?;`
    ///
    /// **Note**: blocks the calling thread. Do not ask the same core from within a handler (deadlock).
    pub fn ask<R>(&self, make_msg: impl FnOnce(Responder<R>) -> A::Message) -> Result<R, AskError>
    where
        R: Send + 'static,
    {
        // Deadlock guard: asking from within a handler on the same core would hang,
        // so return an error instead of hanging (unlike send_blocking, ask can return a Result).
        if self.would_deadlock_calling_core() {
            return Err(AskError::WouldBlockCallingCore);
        }
        let cell = ReplyCell::<R>::new();
        let responder = Responder { cell: &cell };
        let msg = make_msg(responder);
        // If the actor is already gone at send time, Closed (NoReply is when there is no reply after sending).
        // Bailing out here also prevents any deferred write into the ReplyCell (on the stack).
        if let Err(SendError::Closed(_)) = self.send_blocking(msg) {
            return Err(AskError::Closed);
        }

        // Wait for the reply (spin → yield → short sleep backoff. Blocking, but does not keep burning CPU).
        let mut waited: u32 = 0;
        loop {
            match cell.state.load(Ordering::Acquire) {
                REPLIED => {
                    // SAFETY: observing REPLIED = the actor's write (Release) is visible. Move out exactly once.
                    let value = unsafe { (*cell.slot.get()).assume_init_read() };
                    return Ok(value);
                }
                ABANDONED => return Err(AskError::NoReply),
                _ => {
                    if waited < 256 {
                        std::hint::spin_loop();
                    } else if waited < 512 {
                        std::thread::yield_now();
                    } else {
                        std::thread::sleep(Duration::from_micros(20));
                    }
                    waited = waited.saturating_add(1);
                }
            }
        }
    }
}

// Excluded under loom builds, since the real runtime (= loom types) cannot run outside the model.
#[cfg(all(test, not(aetherflow_loom)))]
mod tests {
    use super::*;
    use crate::System;

    #[test]
    fn ask_returns_reply() {
        struct Doubler;
        // Message = (input, reply token)
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
        let addr = sys.spawn_on(2, Echo); // a different core
        assert_eq!(addr.ask(|resp| resp).unwrap(), "pong");
        drop(addr);
        sys.shutdown();
    }

    #[test]
    fn ask_errors_when_actor_drops_without_reply() {
        struct Rude;
        impl Actor for Rude {
            type Message = Responder<u64>;
            fn handle(&mut self, _resp: Responder<u64>) {
                // Drop without replying → the caller wakes up with NoReply
            }
        }
        let sys = System::with_cores(1);
        let addr = sys.spawn_on(0, Rude);
        assert_eq!(addr.ask(|resp| resp), Err(AskError::NoReply));
        drop(addr);
        sys.shutdown();
    }
}
