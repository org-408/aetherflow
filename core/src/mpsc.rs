//! A bounded MPMC lock-free queue (Dmitry Vyukov's algorithm), used as an MPSC
//! (multi-producer, single-consumer) mailbox for actors.
//!
//! Why MPSC: on multicore, several actors send to a single actor (routing), so an
//! N=1 SPSC (single-producer) queue is not enough. By giving each slot a sequence
//! number, a producer can enqueue lock-free by merely reserving the tail via CAS.
//!
//! The consumer is single (the core thread that owns that actor). Producers are any
//! thread (= the senders).
//! - `tail` (enqueue_pos): advanced by producers via CAS
//! - `head` (dequeue_pos): advanced by the single consumer
//!
//! Capacity is rounded up to a power of two, and the index is taken with `& mask`.

use crate::sync::{Arc, AtomicBool, AtomicUsize, Ordering, UnsafeCell};
use std::mem::MaybeUninit;

pub use crate::TrySendError;

#[repr(align(64))]
struct CachePadded<T>(T);

struct Slot<T> {
    seq: AtomicUsize,
    val: UnsafeCell<MaybeUninit<T>>,
}

struct Queue<T> {
    buffer: Box<[Slot<T>]>,
    mask: usize,
    enqueue_pos: CachePadded<AtomicUsize>,
    dequeue_pos: CachePadded<AtomicUsize>,
    /// Number of live producers. Once it reaches 0, "no more messages will arrive".
    producers: CachePadded<AtomicUsize>,
    /// Whether the consumer (Receiver) is alive. false = the receiving actor is gone = sends are impossible.
    consumer_alive: CachePadded<AtomicBool>,
}

unsafe impl<T: Send> Send for Queue<T> {}
unsafe impl<T: Send> Sync for Queue<T> {}

impl<T> Queue<T> {
    fn enqueue(&self, item: T) -> Result<(), TrySendError<T>> {
        // If the receiving actor is gone, sending is impossible regardless of fullness (return the original message).
        if !self.consumer_alive.0.load(Ordering::Acquire) {
            return Err(TrySendError::Closed(item));
        }
        let mask = self.mask;
        let mut pos = self.enqueue_pos.0.load(Ordering::Relaxed);
        loop {
            let slot = &self.buffer[pos & mask];
            let seq = slot.seq.load(Ordering::Acquire);
            let dif = (seq as isize).wrapping_sub(pos as isize);
            if dif == 0 {
                // This slot is writable. Reserve the tail via CAS.
                match self.enqueue_pos.0.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        slot.val.with_mut(|p| unsafe { (*p).write(item) });
                        // The consumer reads in sync with this Release.
                        slot.seq.store(pos.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(actual) => pos = actual,
                }
            } else if dif < 0 {
                // One lap behind = full. If the consumer vanished meanwhile, Closed; if still alive, Full.
                if !self.consumer_alive.0.load(Ordering::Acquire) {
                    return Err(TrySendError::Closed(item));
                }
                return Err(TrySendError::Full(item));
            } else {
                // Another producer just advanced it. Re-read.
                pos = self.enqueue_pos.0.load(Ordering::Relaxed);
            }
        }
    }

    fn dequeue(&self) -> Option<T> {
        let mask = self.mask;
        let mut pos = self.dequeue_pos.0.load(Ordering::Relaxed);
        loop {
            let slot = &self.buffer[pos & mask];
            let seq = slot.seq.load(Ordering::Acquire);
            let dif = (seq as isize).wrapping_sub(pos.wrapping_add(1) as isize);
            if dif == 0 {
                match self.dequeue_pos.0.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let item = slot.val.with_mut(|p| unsafe { (*p).assume_init_read() });
                        // Set the slot's seq one full lap ahead to release it back to producers.
                        slot.seq
                            .store(pos.wrapping_add(mask).wrapping_add(1), Ordering::Release);
                        return Some(item);
                    }
                    Err(actual) => pos = actual,
                }
            } else if dif < 0 {
                return None; // empty
            } else {
                pos = self.dequeue_pos.0.load(Ordering::Relaxed);
            }
        }
    }

    /// Consumer-only: non-destructively check whether it is empty.
    fn is_empty(&self) -> bool {
        let pos = self.dequeue_pos.0.load(Ordering::Relaxed);
        let slot = &self.buffer[pos & self.mask];
        let seq = slot.seq.load(Ordering::Acquire);
        (seq as isize).wrapping_sub(pos.wrapping_add(1) as isize) < 0
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        // Drain and drop any remaining items.
        while self.dequeue().is_some() {}
    }
}

/// The sending end (multi-producer). `Clone`-able = multiple senders are allowed.
pub struct Sender<T> {
    q: Arc<Queue<T>>,
}

/// The receiving end (single consumer).
pub struct Receiver<T> {
    q: Arc<Queue<T>>,
}

/// Create an MPSC with capacity at least `capacity` (rounded up to a power of two).
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let cap = capacity.max(1).next_power_of_two();
    let mut buffer = Vec::with_capacity(cap);
    for i in 0..cap {
        buffer.push(Slot {
            seq: AtomicUsize::new(i),
            val: UnsafeCell::new(MaybeUninit::uninit()),
        });
    }
    let q = Arc::new(Queue {
        buffer: buffer.into_boxed_slice(),
        mask: cap - 1,
        enqueue_pos: CachePadded(AtomicUsize::new(0)),
        dequeue_pos: CachePadded(AtomicUsize::new(0)),
        producers: CachePadded(AtomicUsize::new(1)),
        consumer_alive: CachePadded(AtomicBool::new(true)),
    });
    (Sender { q: q.clone() }, Receiver { q })
}

impl<T> Sender<T> {
    /// Send an item by move. Returns `Err(TrySendError::Full)` if full, or
    /// `Err(TrySendError::Closed)` if the receiving actor is gone (both return the original item).
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.q.enqueue(item)
    }

    // --- observability (merely exposes the existing enqueue/dequeue counters = zero added hot-path cost) ---

    /// Total number enqueued so far (total sent).
    pub fn total_enqueued(&self) -> usize {
        self.q.enqueue_pos.0.load(Ordering::Relaxed)
    }
    /// Total number dequeued so far (total consumed).
    pub fn total_dequeued(&self) -> usize {
        self.q.dequeue_pos.0.load(Ordering::Relaxed)
    }
    /// Current mailbox depth (enqueue − dequeue). Approximate (a snapshot during concurrent updates).
    pub fn depth(&self) -> usize {
        self.total_enqueued().saturating_sub(self.total_dequeued())
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.q.producers.0.fetch_add(1, Ordering::Relaxed);
        Sender { q: self.q.clone() }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // Publish via Release that the last producer is gone (the consumer observes via Acquire).
        self.q.producers.0.fetch_sub(1, Ordering::Release);
    }
}

impl<T> Receiver<T> {
    /// Take one item. Returns `None` if empty.
    pub fn try_recv(&self) -> Option<T> {
        self.q.dequeue()
    }

    /// Whether any producers are alive (= whether more messages may still arrive).
    pub fn producers_alive(&self) -> bool {
        self.q.producers.0.load(Ordering::Acquire) > 0
    }

    /// Non-destructive emptiness check.
    pub fn is_empty(&self) -> bool {
        self.q.is_empty()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // Publish via Release that the consumer (the core owning this actor) is gone.
        // Subsequent enqueues return Closed, preventing send_blocking from spinning forever.
        self.q.consumer_alive.0.store(false, Ordering::Release);
    }
}

// In loom builds, loom types cannot be touched outside the model, so exclude the ordinary tests.
#[cfg(all(test, not(aetherflow_loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn rounds_capacity_up_to_pow2() {
        let (tx, rx) = channel::<u32>(3); // → 4
        for i in 0..4 {
            assert!(tx.try_send(i).is_ok());
        }
        assert!(tx.try_send(99).is_err()); // full
        for i in 0..4 {
            assert_eq!(rx.try_recv(), Some(i));
        }
        assert_eq!(rx.try_recv(), None);
    }

    #[test]
    fn producers_alive_tracks_senders() {
        let (tx, rx) = channel::<u8>(2);
        assert!(rx.producers_alive());
        let tx2 = tx.clone();
        drop(tx);
        assert!(rx.producers_alive()); // tx2 is still alive
        drop(tx2);
        assert!(!rx.producers_alive());
    }

    #[test]
    fn is_empty_is_nondestructive() {
        let (tx, rx) = channel::<u8>(4);
        assert!(rx.is_empty());
        tx.try_send(7).ok();
        assert!(!rx.is_empty());
        assert_eq!(rx.try_recv(), Some(7));
        assert!(rx.is_empty());
    }

    #[test]
    fn try_send_after_receiver_dropped_is_closed() {
        let (tx, rx) = channel::<u32>(4);
        drop(rx); // consumer gone → consumer_alive=false
        match tx.try_send(1) {
            Err(TrySendError::Closed(v)) => assert_eq!(v, 1), // returns the original item
            other => panic!("expected Closed, got {other:?}"),
        }
    }

    #[test]
    fn multi_producer_single_consumer_no_loss() {
        const PRODUCERS: usize = 4;
        const PER: usize = 50_000;
        let (tx, rx) = channel::<usize>(1024);

        let mut handles = vec![];
        for p in 0..PRODUCERS {
            let tx = tx.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..PER {
                    // If full, spin until the consumer drains it (backpressure).
                    let mut item = p * PER + i;
                    loop {
                        match tx.try_send(item) {
                            Ok(()) => break,
                            Err(TrySendError::Full(v)) => {
                                item = v;
                                std::hint::spin_loop();
                            }
                            Err(TrySendError::Closed(v)) => {
                                item = v;
                                std::hint::spin_loop();
                            }
                        }
                    }
                }
            }));
        }
        drop(tx); // drop the original sender (clones are still alive)

        let total = PRODUCERS * PER;
        let mut seen = vec![false; total];
        let mut count = 0;
        while count < total {
            if let Some(v) = rx.try_recv() {
                assert!(!seen[v], "duplicate {v}");
                seen[v] = true;
                count += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(seen.iter().all(|&b| b), "some items lost");
        assert_eq!(rx.try_recv(), None);
    }

    #[test]
    fn drops_unconsumed_on_teardown() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        struct Tracked;
        impl Drop for Tracked {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::SeqCst);
            }
        }
        {
            let (tx, _rx) = channel::<Tracked>(4);
            tx.try_send(Tracked).ok();
            tx.try_send(Tracked).ok();
        }
        assert_eq!(DROPS.load(Ordering::SeqCst), 2);
    }
}

/// Concurrent interleaving verification with Loom (only runs under `RUSTFLAGS="--cfg aetherflow_loom" cargo test --lib`).
///
/// Miri checks "is this execution UB", but cannot find bugs that **break only under a
/// different thread ordering**. Loom exhaustively explores the allowed orderings, so it can
/// detect a missing Acquire/Release (= a written value not visible to the other side) or a
/// lost update under CAS contention.
///
/// The model is deliberately tiny (2 threads, capacity 2, one message each) ── because loom's
/// exploration combinatorially explodes, "small but see every ordering" is the correct usage.
#[cfg(all(test, aetherflow_loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    /// **The crux of multi-producer**: even when 2 producers contend for `enqueue_pos` via CAS,
    /// no message is lost or duplicated (each arrives exactly once under any ordering).
    #[test]
    fn two_producers_no_loss_no_duplication() {
        loom::model(|| {
            let (tx1, rx) = channel::<u32>(2);
            let tx2 = tx1.clone();

            let h1 = thread::spawn(move || tx1.try_send(1).is_ok());
            let h2 = thread::spawn(move || tx2.try_send(2).is_ok());

            let ok1 = h1.join().unwrap();
            let ok2 = h2.join().unwrap();
            assert!(ok1 && ok2, "capacity 2 with 2 messages, so both should fit");

            let mut got = Vec::new();
            while let Some(v) = rx.try_recv() {
                got.push(v);
            }
            got.sort_unstable();
            assert_eq!(got, vec![1, 2], "no loss and no duplication");
        });
    }

    /// **Producer→consumer visibility**: if the Release/Acquire on `seq` is correct, then by the
    /// time the consumer can observe the value, the payload write is necessarily visible too.
    #[test]
    fn producer_consumer_handshake_publishes_value() {
        loom::model(|| {
            let (tx, rx) = channel::<u32>(1);

            let h = thread::spawn(move || {
                tx.try_send(42).ok();
            });

            // Loom tries every ordering for when the producer runs. Once observed, the payload is necessarily 42.
            loop {
                if let Some(v) = rx.try_recv() {
                    assert_eq!(v, 42, "observed seq but value not visible = ordering bug");
                    break;
                }
                thread::yield_now();
            }
            h.join().unwrap();
        });
    }

    /// **Publishing consumer teardown**: a send after `Receiver` is dropped is always `Closed`
    /// (the precondition for `send_blocking` not spinning forever).
    #[test]
    fn send_after_receiver_drop_is_closed() {
        loom::model(|| {
            let (tx, rx) = channel::<u32>(1);

            let h = thread::spawn(move || {
                drop(rx);
            });
            h.join().unwrap();

            // Since the consumer is definitively gone, nothing but Closed is possible.
            match tx.try_send(1) {
                Err(TrySendError::Closed(v)) => assert_eq!(v, 1, "the original value is returned"),
                other => panic!("expected Closed but got {:?}", other.is_ok()),
            }
        });
    }
}
