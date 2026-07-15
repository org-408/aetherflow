//! `std` / `loom` 切替シム。
//!
//! Loom は並行実行の**インターリーブを網羅探索**する検証器で、そのために atomic と
//! `UnsafeCell` を自前の計装済み型へ差し替える必要がある。`--cfg aetherflow_loom` のときだけ loom の
//! 型を使い、通常ビルドでは `std` をそのまま使う(= 本番バイナリに loom は一切入らない)。
//!
//! 厄介なのは `UnsafeCell` の API が両者で違うこと: loom は「生ポインタをクロージャに渡す」形
//! (アクセス範囲を検証器に知らせるため)で、`std` は `.get()` で生ポインタを返す。
//! そこで `std` 側に loom と同じ `with` / `with_mut` を持つ薄いラッパを被せ、
//! 呼び出し側(`mpsc` / `spsc`)を**一本のコードに保つ**。
//!
//! Miri が UB(未定義動作)を見るのに対し、Loom は**順序と可視性**を見る ── 役割が違うので両方要る。
//!
//! cfg 名が `loom` ではなく `aetherflow_loom` なのは、`RUSTFLAGS` の `--cfg` が**依存クレート全体に
//! 伝播する**ため。素の `--cfg loom` を使うと、自前の `cfg(loom)` 経路を持つが loom を依存に持たない
//! クレート(dev-dependencies 経由の `concurrent-queue` 等)が壊れる。crossbeam が
//! `crossbeam_loom` を使うのと同じ回避策。

#[cfg(aetherflow_loom)]
pub(crate) use loom::cell::UnsafeCell;
#[cfg(aetherflow_loom)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(aetherflow_loom)]
pub(crate) use loom::sync::Arc;

#[cfg(not(aetherflow_loom))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(not(aetherflow_loom))]
pub(crate) use std::sync::Arc;

/// `std` 版 `UnsafeCell`(loom と同じ `with` / `with_mut` API に揃えるためのラッパ)。
///
/// 呼び出し側を loom 版と同一に保つためだけの存在で、`#[inline(always)]` により
/// 通常ビルドでは素の `UnsafeCell::get()` と同じコードに落ちる(ゼロコスト)。
#[cfg(not(aetherflow_loom))]
#[derive(Debug)]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(aetherflow_loom))]
impl<T> UnsafeCell<T> {
    pub(crate) fn new(data: T) -> UnsafeCell<T> {
        UnsafeCell(std::cell::UnsafeCell::new(data))
    }

    /// 読み書きとも `with_mut` に寄せている(スロットへのアクセスは常に
    /// 「単一スレッドが排他的に触る」規律の下でのみ行うため)。loom 版にある
    /// 共有アクセス用の `with` は現状 呼び出しが無いので、ラッパ側にも生やさない。
    #[inline(always)]
    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}
