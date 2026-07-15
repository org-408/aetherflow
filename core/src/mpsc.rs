//! 有界 MPMC ロックフリーキュー(Dmitry Vyukov のアルゴリズム)を、actor mailbox 向けに
//! MPSC(多生産者・単一消費者)として使う。
//!
//! なぜ MPSC か: マルチコアでは複数の actor が 1 つの actor に送る(routing)ので、
//! N=1 の SPSC(単一生産者)では足りない。各スロットに sequence 番号を持たせることで、
//! 生産者は tail を CAS で予約するだけでロック無しに enqueue できる。
//!
//! 消費者は単一(その actor を所有するコアスレッド)。生産者は任意スレッド(= 送信側)。
//! - `tail`(enqueue_pos): 生産者が CAS で進める
//! - `head`(dequeue_pos): 単一消費者が進める
//!
//! 容量は 2 の冪に切り上げ、`& mask` で index を取る。

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
    /// 生きている生産者数。0 になったら「もう新しいメッセージは来ない」。
    producers: CachePadded<AtomicUsize>,
    /// 消費者(Receiver)が生きているか。false = 受信 actor が消滅済み = 送信不能。
    consumer_alive: CachePadded<AtomicBool>,
}

unsafe impl<T: Send> Send for Queue<T> {}
unsafe impl<T: Send> Sync for Queue<T> {}

impl<T> Queue<T> {
    fn enqueue(&self, item: T) -> Result<(), TrySendError<T>> {
        // 受信 actor が消滅済みなら、満杯かどうかに関わらず送信不能(元メッセージを返す)。
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
                // このスロットは書ける。tail を CAS で予約。
                match self.enqueue_pos.0.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        slot.val.with_mut(|p| unsafe { (*p).write(item) });
                        // 消費者はこの Release と同期して読む。
                        slot.seq.store(pos.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(actual) => pos = actual,
                }
            } else if dif < 0 {
                // 一周遅れ = 満杯。消費者がこの間に消えていれば Closed、生きていれば Full。
                if !self.consumer_alive.0.load(Ordering::Acquire) {
                    return Err(TrySendError::Closed(item));
                }
                return Err(TrySendError::Full(item));
            } else {
                // 別の生産者が進めた直後。再読込。
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
                        // スロットを次の一周ぶん先の seq にして生産者へ解放。
                        slot.seq
                            .store(pos.wrapping_add(mask).wrapping_add(1), Ordering::Release);
                        return Some(item);
                    }
                    Err(actual) => pos = actual,
                }
            } else if dif < 0 {
                return None; // 空
            } else {
                pos = self.dequeue_pos.0.load(Ordering::Relaxed);
            }
        }
    }

    /// 消費者専用: 非破壊で空かどうか。
    fn is_empty(&self) -> bool {
        let pos = self.dequeue_pos.0.load(Ordering::Relaxed);
        let slot = &self.buffer[pos & self.mask];
        let seq = slot.seq.load(Ordering::Acquire);
        (seq as isize).wrapping_sub(pos.wrapping_add(1) as isize) < 0
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        // 残っているアイテムを drain して drop。
        while self.dequeue().is_some() {}
    }
}

/// 送信端(多生産者)。`Clone` 可能 = 複数の送信元を許す。
pub struct Sender<T> {
    q: Arc<Queue<T>>,
}

/// 受信端(単一消費者)。
pub struct Receiver<T> {
    q: Arc<Queue<T>>,
}

/// 容量 `capacity` 以上(2 の冪に切り上げ)の MPSC を作る。
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
    /// アイテムを move で送る。満杯なら `Err(TrySendError::Full)`、受信 actor 消滅なら
    /// `Err(TrySendError::Closed)`(いずれも元アイテムを返す)。
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.q.enqueue(item)
    }

    // --- observability(既存の enqueue/dequeue カウンタを露出するだけ = hot path 追加コストゼロ) ---

    /// これまでに enqueue された総数(送信総数)。
    pub fn total_enqueued(&self) -> usize {
        self.q.enqueue_pos.0.load(Ordering::Relaxed)
    }
    /// これまでに dequeue された総数(消費総数)。
    pub fn total_dequeued(&self) -> usize {
        self.q.dequeue_pos.0.load(Ordering::Relaxed)
    }
    /// 現在の mailbox 滞留数(enqueue − dequeue)。近似(並行更新中のスナップショット)。
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
        // 最後の生産者が消えたことを Release で公開(消費者が Acquire で観測)。
        self.q.producers.0.fetch_sub(1, Ordering::Release);
    }
}

impl<T> Receiver<T> {
    /// アイテムを 1 つ取り出す。空なら `None`。
    pub fn try_recv(&self) -> Option<T> {
        self.q.dequeue()
    }

    /// 生きている生産者がいるか(= まだ送られてくる可能性があるか)。
    pub fn producers_alive(&self) -> bool {
        self.q.producers.0.load(Ordering::Acquire) > 0
    }

    /// 非破壊の空判定。
    pub fn is_empty(&self) -> bool {
        self.q.is_empty()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // 消費者(この actor を所有するコア)の消滅を Release で公開。
        // 以後の enqueue は Closed を返し、send_blocking の永久スピンを防ぐ。
        self.q.consumer_alive.0.store(false, Ordering::Release);
    }
}

// loom ビルドでは loom 型をモデル外で触れないため、通常テストは対象外にする。
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
        assert!(tx.try_send(99).is_err()); // 満杯
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
        assert!(rx.producers_alive()); // tx2 が生きている
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
        drop(rx); // 消費者消滅 → consumer_alive=false
        match tx.try_send(1) {
            Err(TrySendError::Closed(v)) => assert_eq!(v, 1), // 元アイテムを返す
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
                    // 満杯なら消費者が捌くまでスピン(バックプレッシャ)。
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
        drop(tx); // 元の送信端を落とす(クローンが生きている)

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

/// Loom による並行インターリーブ検証(`RUSTFLAGS="--cfg aetherflow_loom" cargo test --lib` でのみ動く)。
///
/// Miri は「その実行が UB か」を見るが、**別のスレッド順序なら壊れる**バグは見つけられない。
/// Loom は許される順序を網羅探索するので、Acquire/Release の張り忘れ(= 書いた値が
/// 相手に見えない)や CAS 競合での取りこぼしを検出できる。
///
/// モデルは意図的に極小(スレッド2・容量2・1通ずつ)── loom の探索は組合せ爆発するため、
/// 「小さくても順序は全部見る」のが正しい使い方。
#[cfg(all(test, aetherflow_loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    /// **多生産者の肝**: 2 生産者が `enqueue_pos` を CAS で奪い合っても、
    /// メッセージが消えも重複もしない(どの順序でも ちょうど 1 回ずつ届く)。
    #[test]
    fn two_producers_no_loss_no_duplication() {
        loom::model(|| {
            let (tx1, rx) = channel::<u32>(2);
            let tx2 = tx1.clone();

            let h1 = thread::spawn(move || tx1.try_send(1).is_ok());
            let h2 = thread::spawn(move || tx2.try_send(2).is_ok());

            let ok1 = h1.join().unwrap();
            let ok2 = h2.join().unwrap();
            assert!(ok1 && ok2, "容量 2 に 2 通なので両方入るはず");

            let mut got = Vec::new();
            while let Some(v) = rx.try_recv() {
                got.push(v);
            }
            got.sort_unstable();
            assert_eq!(got, vec![1, 2], "取りこぼし/重複が無いこと");
        });
    }

    /// **生産者→消費者の可視性**: `seq` の Release/Acquire が正しければ、
    /// 消費者が値を観測できた時点で中身の書き込みも必ず見えている。
    #[test]
    fn producer_consumer_handshake_publishes_value() {
        loom::model(|| {
            let (tx, rx) = channel::<u32>(1);

            let h = thread::spawn(move || {
                tx.try_send(42).ok();
            });

            // 生産者がいつ走るかは loom が全順序を試す。観測できたら中身は必ず 42。
            loop {
                if let Some(v) = rx.try_recv() {
                    assert_eq!(v, 42, "seq を観測できたのに値が見えない = 順序バグ");
                    break;
                }
                thread::yield_now();
            }
            h.join().unwrap();
        });
    }

    /// **消費者消滅の公開**: `Receiver` が drop された後の送信は必ず `Closed` になる
    /// (`send_blocking` が永久スピンしないための前提)。
    #[test]
    fn send_after_receiver_drop_is_closed() {
        loom::model(|| {
            let (tx, rx) = channel::<u32>(1);

            let h = thread::spawn(move || {
                drop(rx);
            });
            h.join().unwrap();

            // 消費者が確実に消えた後なので Closed 以外はありえない。
            match tx.try_send(1) {
                Err(TrySendError::Closed(v)) => assert_eq!(v, 1, "元の値が返ること"),
                other => panic!("Closed を期待したが {:?} だった", other.is_ok()),
            }
        });
    }
}
