//! Scheduling — delayed and periodic message delivery ([`ActorRef::send_after`] /
//! [`ActorRef::send_every`]).
//!
//! A single lazily-started background thread holds a min-heap of pending timers keyed by deadline.
//! It never touches the core hot loop or `shutdown`; delivery is a non-blocking [`ActorRef::try_send`]
//! (best-effort, like pub/sub). This is meant for "tick every N ms" / "fire once after a delay", not
//! sub-microsecond scheduling.
//!
//! A **periodic timer self-terminates** when its target actor is gone (a `try_send` returns
//! `Closed`), so a dropped [`TimerHandle`] does not leak a thread of firing forever. Dropping the
//! handle does **not** cancel — call [`TimerHandle::cancel`] for that. This makes fire-and-forget
//! (`addr.send_after(dur, msg);`) just work without keeping the handle alive.

use crate::{Actor, ActorRef};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// A pending timer. `fire` returns `Some(next_deadline)` to reschedule (periodic) or `None` (one-shot).
struct Entry {
    deadline: Instant,
    seq: u64, // FIFO tiebreaker among equal deadlines (stable ordering)
    cancelled: Arc<AtomicBool>,
    fire: Box<dyn FnMut() -> Option<Instant> + Send>,
}

// BinaryHeap is a max-heap; we want the *earliest* deadline to pop first, so reverse the ordering
// (earlier deadline / smaller seq compares as "greater").
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.seq == other.seq
    }
}
impl Eq for Entry {}

struct Shared {
    heap: Mutex<BinaryHeap<Entry>>,
    cv: Condvar,
    seq: AtomicU64,
}

static TIMER: OnceLock<Arc<Shared>> = OnceLock::new();

/// Get (or lazily start) the process-wide timer service.
fn shared() -> &'static Arc<Shared> {
    TIMER.get_or_init(|| {
        let shared = Arc::new(Shared {
            heap: Mutex::new(BinaryHeap::new()),
            cv: Condvar::new(),
            seq: AtomicU64::new(0),
        });
        let worker = Arc::clone(&shared);
        std::thread::Builder::new()
            .name("aetherflow-timer".into())
            .spawn(move || run(worker))
            .expect("failed to spawn aetherflow timer thread");
        shared
    })
}

fn run(shared: Arc<Shared>) {
    let mut heap = shared.heap.lock().unwrap();
    loop {
        let now = Instant::now();
        // Fire everything due.
        while heap.peek().is_some_and(|top| top.deadline <= now) {
            let mut entry = heap.pop().unwrap();
            if entry.cancelled.load(AtomicOrdering::Relaxed) {
                continue;
            }
            // Release the lock while firing (fire() does a try_send and may run user code via the
            // periodic message factory).
            drop(heap);
            let next = (entry.fire)();
            heap = shared.heap.lock().unwrap();
            if let Some(next_deadline) = next {
                if !entry.cancelled.load(AtomicOrdering::Relaxed) {
                    entry.deadline = next_deadline;
                    heap.push(entry);
                }
            }
        }
        // Sleep until the next deadline, or until a registration wakes us (a new/earlier timer).
        let timeout = match heap.peek() {
            Some(top) => top.deadline.saturating_duration_since(Instant::now()),
            None => Duration::from_secs(3600),
        };
        let (g, _) = shared.cv.wait_timeout(heap, timeout).unwrap();
        heap = g;
    }
}

fn schedule(deadline: Instant, cancelled: Arc<AtomicBool>, fire: Box<dyn FnMut() -> Option<Instant> + Send>) {
    let s = shared();
    let seq = s.seq.fetch_add(1, AtomicOrdering::Relaxed);
    s.heap.lock().unwrap().push(Entry {
        deadline,
        seq,
        cancelled,
        fire,
    });
    s.cv.notify_one();
}

/// A cancellation handle for a scheduled timer.
///
/// Dropping it does **not** cancel (the timer keeps running); call [`cancel`](Self::cancel) to stop
/// a periodic timer early. A periodic timer also stops on its own once the target actor is gone.
pub struct TimerHandle {
    cancelled: Arc<AtomicBool>,
}

impl TimerHandle {
    /// Cancel the timer. A one-shot that has not fired will not fire; a periodic timer stops.
    pub fn cancel(&self) {
        self.cancelled.store(true, AtomicOrdering::Relaxed);
    }
}

impl<A: Actor> ActorRef<A>
where
    A::Message: Send + 'static,
{
    /// Deliver `msg` to this actor once, after `delay`.
    ///
    /// Delivery is a non-blocking `try_send` (dropped if the mailbox is full or the actor is gone).
    /// Returns a [`TimerHandle`]; you can ignore it for fire-and-forget, or keep it to
    /// [`cancel`](TimerHandle::cancel) before it fires.
    pub fn send_after(&self, delay: Duration, msg: A::Message) -> TimerHandle {
        let cancelled = Arc::new(AtomicBool::new(false));
        let addr = self.clone();
        let mut msg = Some(msg);
        schedule(
            Instant::now() + delay,
            Arc::clone(&cancelled),
            Box::new(move || {
                if let Some(m) = msg.take() {
                    let _ = addr.try_send(m);
                }
                None // one-shot
            }),
        );
        TimerHandle { cancelled }
    }

    /// Deliver a message built by `make` to this actor every `interval` (first fire after `interval`).
    ///
    /// Delivery is a non-blocking `try_send`. The timer **stops automatically** once the actor is
    /// gone, so a dropped handle does not leak. Keep the [`TimerHandle`] to
    /// [`cancel`](TimerHandle::cancel) it earlier.
    ///
    /// # Panics
    /// If `interval` is zero (it would fire in a tight loop and make no forward progress).
    pub fn send_every(
        &self,
        interval: Duration,
        mut make: impl FnMut() -> A::Message + Send + 'static,
    ) -> TimerHandle {
        assert!(
            interval > Duration::ZERO,
            "send_every interval must be non-zero"
        );
        let cancelled = Arc::new(AtomicBool::new(false));
        let addr = self.clone();
        schedule(
            Instant::now() + interval,
            Arc::clone(&cancelled),
            Box::new(move || {
                // Stop rescheduling once the actor is gone (Closed); otherwise fire again next interval.
                match addr.try_send(make()) {
                    Err(crate::TrySendError::Closed(_)) => None,
                    _ => Some(Instant::now() + interval),
                }
            }),
        );
        TimerHandle { cancelled }
    }
}

#[cfg(all(test, not(aetherflow_loom)))]
mod tests {
    use super::*;
    use crate::System;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn send_after_fires_once_after_delay() {
        static HITS: AtomicU32 = AtomicU32::new(0);
        struct A;
        impl Actor for A {
            type Message = ();
            fn handle(&mut self, _: ()) {
                HITS.fetch_add(1, AtomicOrdering::SeqCst);
            }
        }
        let sys = System::with_cores(1);
        let addr = sys.spawn_on(0, A);
        let t = Instant::now();
        let _h = addr.send_after(Duration::from_millis(40), ());
        // Not fired immediately.
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(HITS.load(AtomicOrdering::SeqCst), 0);
        // Fired after the delay.
        while HITS.load(AtomicOrdering::SeqCst) == 0 && t.elapsed() < Duration::from_secs(2) {
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(HITS.load(AtomicOrdering::SeqCst), 1);
        assert!(t.elapsed() >= Duration::from_millis(40));
        drop(addr);
        sys.shutdown();
    }

    #[test]
    fn send_every_ticks_and_cancel_stops() {
        static TICKS: AtomicU32 = AtomicU32::new(0);
        struct A;
        impl Actor for A {
            type Message = ();
            fn handle(&mut self, _: ()) {
                TICKS.fetch_add(1, AtomicOrdering::SeqCst);
            }
        }
        let sys = System::with_cores(1);
        let addr = sys.spawn_on(0, A);
        let h = addr.send_every(Duration::from_millis(20), || ());
        // Let a handful of ticks land.
        std::thread::sleep(Duration::from_millis(130));
        h.cancel();
        let after_cancel = TICKS.load(AtomicOrdering::SeqCst);
        assert!(after_cancel >= 3, "expected several ticks, got {after_cancel}");
        // No further ticks after cancel.
        std::thread::sleep(Duration::from_millis(80));
        assert_eq!(TICKS.load(AtomicOrdering::SeqCst), after_cancel);
        drop(addr);
        sys.shutdown();
    }

    #[test]
    fn send_every_stops_when_actor_gone() {
        struct A;
        impl Actor for A {
            type Message = ();
            fn handle(&mut self, _: ()) {}
        }
        let sys = System::with_cores(1);
        let addr = sys.spawn_on(0, A);
        // Detached periodic timer (handle dropped): must not keep a live entry once the actor dies.
        let _ = addr.send_every(Duration::from_millis(10), || ());
        drop(addr);
        sys.shutdown();
        // The actor is gone; the next fire sees Closed and stops rescheduling. Give it time to happen.
        std::thread::sleep(Duration::from_millis(60));
        // Nothing to assert beyond "no panic / no hang"; the self-termination is exercised.
    }
}
