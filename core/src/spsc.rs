//! A single-producer, single-consumer (SPSC) bounded ring buffer.
//!
//! The concrete realization of pillar ③ in design.md, the "SPSC mailbox that never crosses
//! cores". A Lamport-style queue that uses no locks and merely exchanges the head/tail atomics
//! via Acquire/Release.
//!
//! Design highlights (mechanical sympathy):
//! - **Single-writer principle**: only the producer writes `tail`, only the consumer writes `head`.
//! - **False-sharing avoidance**: `head` and `tail` are isolated onto separate cache lines (64B).
//!   If they shared one, an update to either would invalidate the other's cache, wasting work (see concepts-explained).
//! - **Bounded**: when full, returns `Full` for backpressure. No unbounded buffer melting memory.

use crate::sync::{Arc, AtomicBool, AtomicUsize, Ordering, UnsafeCell};
use std::mem::MaybeUninit;

/// Align to a 64-byte boundary to prevent cache-line sharing (false sharing) with adjacent fields.
#[repr(align(64))]
struct CachePadded<T>(T);

struct Ring<T> {
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    cap: usize,
    /// The position the consumer reads next (monotonically increasing). Only the consumer writes it.
    head: CachePadded<AtomicUsize>,
    /// The position the producer writes next (monotonically increasing). Only the producer writes it.
    tail: CachePadded<AtomicUsize>,
    /// true once the producer (`Producer`) is dropped. The consumer's stop condition.
    closed: AtomicBool,
}

// Safe because slot access is confined to a single thread by the head/tail discipline.
unsafe impl<T: Send> Send for Ring<T> {}
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        // Drop the slots (head..tail) still holding unconsumed items.
        // &mut self = no other thread exists, so relaxed suffices (loom's atomics have no
        // get_mut, so use a load that stays identical across both builds).
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Relaxed);
        for pos in head..tail {
            let idx = pos % self.cap;
            // Do not touch uninitialized slots (only head..tail are initialized).
            self.slots[idx].with_mut(|p| unsafe { (*p).assume_init_drop() });
        }
    }
}

/// The sending end. Single producer only (the S in SPSC). Does not implement `Clone` = the single-writer principle enforced by the type.
pub struct Producer<T> {
    ring: Arc<Ring<T>>,
}

/// The receiving end. Single consumer only.
pub struct Consumer<T> {
    ring: Arc<Ring<T>>,
}

pub use crate::TrySendError;

/// Create an SPSC ring with capacity `capacity` (at least 1).
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(capacity >= 1, "SPSC capacity must be >= 1");
    let mut slots = Vec::with_capacity(capacity);
    for _ in 0..capacity {
        slots.push(UnsafeCell::new(MaybeUninit::uninit()));
    }
    let ring = Arc::new(Ring {
        slots: slots.into_boxed_slice(),
        cap: capacity,
        head: CachePadded(AtomicUsize::new(0)),
        tail: CachePadded(AtomicUsize::new(0)),
        closed: AtomicBool::new(false),
    });
    (
        Producer { ring: ring.clone() },
        Consumer { ring },
    )
}

impl<T> Producer<T> {
    /// Push an item by move. Returns `Err(TrySendError::Full(item))` if full.
    /// (Closed never arises in SPSC because there is a single producer.)
    pub fn try_push(&self, item: T) -> Result<(), TrySendError<T>> {
        let ring = &self.ring;
        // Only the producer writes tail, so Relaxed can read our own value.
        let tail = ring.tail.0.load(Ordering::Relaxed);
        // Observe the consumer's progress via Acquire (gaining visibility of free slots).
        let head = ring.head.0.load(Ordering::Acquire);
        if tail - head == ring.cap {
            return Err(TrySendError::Full(item));
        }
        let idx = tail % ring.cap;
        ring.slots[idx].with_mut(|p| unsafe { (*p).write(item) });
        // Publish that the write is complete. The consumer reads the item in sync with this Release.
        ring.tail.0.store(tail + 1, Ordering::Release);
        Ok(())
    }

    /// Spin while full and push (backpressure). Always succeeds as long as the consumer is draining.
    pub fn push_blocking(&self, mut item: T) {
        loop {
            match self.try_push(item) {
                Ok(()) => return,
                // SPSC never produces Closed (single producer). When full, wait until space opens.
                Err(TrySendError::Full(returned)) | Err(TrySendError::Closed(returned)) => {
                    item = returned;
                    std::hint::spin_loop();
                }
            }
        }
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.ring.closed.store(true, Ordering::Release);
    }
}

impl<T> Consumer<T> {
    /// Take one item. Returns `None` if empty.
    pub fn try_pop(&self) -> Option<T> {
        let ring = &self.ring;
        // Only the consumer writes head, so Relaxed.
        let head = ring.head.0.load(Ordering::Relaxed);
        // Observe the producer's progress via Acquire (gaining visibility of the written data).
        let tail = ring.tail.0.load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let idx = head % ring.cap;
        let item = ring.slots[idx].with_mut(|p| unsafe { (*p).assume_init_read() });
        // Publish that the slot has been freed.
        ring.head.0.store(head + 1, Ordering::Release);
        Some(item)
    }

    /// Whether the producer has been dropped (= no more messages will arrive).
    pub fn is_closed(&self) -> bool {
        self.ring.closed.load(Ordering::Acquire)
    }
}

// In loom builds, loom types cannot be touched outside the model, so exclude the ordinary tests.
#[cfg(all(test, not(aetherflow_loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn push_pop_fifo_order() {
        let (tx, rx) = channel::<u32>(4);
        tx.try_push(1).ok();
        tx.try_push(2).ok();
        tx.try_push(3).ok();
        assert_eq!(rx.try_pop(), Some(1));
        assert_eq!(rx.try_pop(), Some(2));
        assert_eq!(rx.try_pop(), Some(3));
        assert_eq!(rx.try_pop(), None);
    }

    #[test]
    fn full_returns_item() {
        let (tx, _rx) = channel::<u32>(2);
        assert!(tx.try_push(1).is_ok());
        assert!(tx.try_push(2).is_ok());
        match tx.try_push(3) {
            Err(TrySendError::Full(v)) => assert_eq!(v, 3),
            other => panic!("expected Full, got {other:?}"),
        }
    }

    #[test]
    fn wraps_around_many_times() {
        let (tx, rx) = channel::<usize>(3);
        for i in 0..1000 {
            tx.try_push(i).expect("space (drained each iter)");
            assert_eq!(rx.try_pop(), Some(i));
        }
        assert_eq!(rx.try_pop(), None);
    }

    #[test]
    fn drops_unconsumed_items_on_teardown() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        struct Tracked;
        impl Drop for Tracked {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::SeqCst);
            }
        }
        {
            let (tx, _rx) = channel::<Tracked>(4);
            tx.try_push(Tracked).ok();
            tx.try_push(Tracked).ok();
            // Scope ends without consuming → Ring's Drop should drop the remaining 2.
        }
        assert_eq!(DROPS.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn closed_after_producer_drops() {
        let (tx, rx) = channel::<u8>(2);
        assert!(!rx.is_closed());
        drop(tx);
        assert!(rx.is_closed());
    }

    #[test]
    fn concurrent_producer_consumer() {
        let (tx, rx) = channel::<u64>(64);
        let producer = std::thread::spawn(move || {
            for i in 0..100_000u64 {
                tx.push_blocking(i);
            }
        });
        let mut next = 0u64;
        while next < 100_000 {
            if let Some(v) = rx.try_pop() {
                assert_eq!(v, next); // verify FIFO with no loss
                next += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        producer.join().unwrap();
    }
}

/// Concurrent interleaving verification with Loom (only runs under `RUSTFLAGS="--cfg aetherflow_loom" cargo test --lib`).
///
/// SPSC relies on the "single-writer principle" (only the producer writes tail, only the consumer
/// writes head), and correctness is guaranteed by the head/tail Acquire/Release alone. We confirm
/// that this guarantee truly holds via exhaustive exploration of the orderings.
#[cfg(all(test, aetherflow_loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    /// **Ring wraparound**: push 2 messages through capacity 1 (the 2nd is Full until the 1st is consumed).
    /// The full→consume→reuse handshake must not corrupt values under any ordering.
    #[test]
    fn wraparound_preserves_values_and_order() {
        loom::model(|| {
            let (tx, rx) = channel::<u32>(1);

            let h = thread::spawn(move || {
                // While Full, yield and retry (spelling out the same logic as push_blocking).
                for i in 1..=2u32 {
                    loop {
                        match tx.try_push(i) {
                            Ok(()) => break,
                            Err(_) => thread::yield_now(),
                        }
                    }
                }
            });

            let mut got = Vec::new();
            while got.len() < 2 {
                if let Some(v) = rx.try_pop() {
                    got.push(v);
                } else {
                    thread::yield_now();
                }
            }
            h.join().unwrap();
            assert_eq!(got, vec![1, 2], "FIFO order is preserved (including slot reuse)");
        });
    }

    /// **Publishing producer teardown**: once `Producer` is dropped, the consumer can observe it via
    /// `is_closed`, and values sent before the drop are not lost (no case where closure becomes visible
    /// first while the value is not).
    #[test]
    fn close_is_visible_after_pending_value() {
        loom::model(|| {
            let (tx, rx) = channel::<u32>(1);

            let h = thread::spawn(move || {
                tx.try_push(7).ok();
                drop(tx); // publish closed = true
            });
            h.join().unwrap();

            assert!(rx.is_closed(), "producer teardown is observable");
            assert_eq!(rx.try_pop(), Some(7), "unconsumed values remain even after closing");
        });
    }
}
