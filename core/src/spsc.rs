//! 単一生産者・単一消費者(SPSC)の有界リングバッファ。
//!
//! design.md 柱③の「コアを跨がない SPSC mailbox」の実体。ロックを一切使わず、
//! head/tail の atomic を Acquire/Release でやり取りするだけの Lamport 型キュー。
//!
//! 設計の要点(mechanical sympathy):
//! - **単一ライター原則**: `tail` は生産者だけが書き、`head` は消費者だけが書く。
//! - **false sharing 回避**: `head` と `tail` を別キャッシュライン(64B)に隔離。
//!   同居すると、片方の更新が他方のキャッシュを無効化して無駄が出る(concepts-explained 参照)。
//! - **有界**: 満杯時は `Full` を返してバックプレッシャ。無限バッファでメモリを溶かさない。

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// 64 バイト境界に整列させ、隣接フィールドとのキャッシュライン共有(false sharing)を防ぐ。
#[repr(align(64))]
struct CachePadded<T>(T);

struct Ring<T> {
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    cap: usize,
    /// 消費者が次に読む位置(単調増加)。消費者のみが書く。
    head: CachePadded<AtomicUsize>,
    /// 生産者が次に書く位置(単調増加)。生産者のみが書く。
    tail: CachePadded<AtomicUsize>,
    /// 生産者(`Producer`)が drop されたら true。消費者の停止条件。
    closed: AtomicBool,
}

// スロットへのアクセスは head/tail の規律で単一スレッドに限定されるため安全。
unsafe impl<T: Send> Send for Ring<T> {}
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        // 未消費のまま残っているスロット(head..tail)を drop する。
        let head = *self.head.0.get_mut();
        let tail = *self.tail.0.get_mut();
        for pos in head..tail {
            let idx = pos % self.cap;
            // 未初期化スロットには触れない(head..tail のみが初期化済み)。
            unsafe { (*self.slots[idx].get()).assume_init_drop() };
        }
    }
}

/// 送信端。単一生産者のみ(SPSC の S)。`Clone` は実装しない = 単一ライター原則の型による強制。
pub struct Producer<T> {
    ring: Arc<Ring<T>>,
}

/// 受信端。単一消費者のみ。
pub struct Consumer<T> {
    ring: Arc<Ring<T>>,
}

pub use crate::TrySendError;

/// 容量 `capacity`(1 以上)の SPSC リングを作る。
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
    /// アイテムを move で押し込む。満杯なら `Err(TrySendError::Full(item))`。
    /// (SPSC は単一生産者のため Closed は生じない。)
    pub fn try_push(&self, item: T) -> Result<(), TrySendError<T>> {
        let ring = &self.ring;
        // tail は生産者のみが書くので Relaxed で自分の値を読める。
        let tail = ring.tail.0.load(Ordering::Relaxed);
        // 消費者の進捗を Acquire で観測(空きスロットの可視性を得る)。
        let head = ring.head.0.load(Ordering::Acquire);
        if tail - head == ring.cap {
            return Err(TrySendError::Full(item));
        }
        let idx = tail % ring.cap;
        unsafe { (*ring.slots[idx].get()).write(item) };
        // 書き込み完了を公開。消費者はこの Release と同期して item を読む。
        ring.tail.0.store(tail + 1, Ordering::Release);
        Ok(())
    }

    /// 満杯の間スピンして押し込む(バックプレッシャ)。消費者が drain していれば必ず通る。
    pub fn push_blocking(&self, mut item: T) {
        loop {
            match self.try_push(item) {
                Ok(()) => return,
                // SPSC は Closed を生じない(単一生産者)。満杯は空くまで待つ。
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
    /// アイテムを 1 つ取り出す。空なら `None`。
    pub fn try_pop(&self) -> Option<T> {
        let ring = &self.ring;
        // head は消費者のみが書くので Relaxed。
        let head = ring.head.0.load(Ordering::Relaxed);
        // 生産者の進捗を Acquire で観測(書き込み済みデータの可視性を得る)。
        let tail = ring.tail.0.load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let idx = head % ring.cap;
        let item = unsafe { (*ring.slots[idx].get()).assume_init_read() };
        // スロットを空けたことを公開。
        ring.head.0.store(head + 1, Ordering::Release);
        Some(item)
    }

    /// 生産者が drop 済みか(= もう新しいメッセージは来ない)。
    pub fn is_closed(&self) -> bool {
        self.ring.closed.load(Ordering::Acquire)
    }
}

#[cfg(test)]
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
            // 消費せずにスコープ終了 → Ring の Drop が残り 2 個を drop するはず。
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
                assert_eq!(v, next); // FIFO・ロスなしを確認
                next += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        producer.join().unwrap();
    }
}
