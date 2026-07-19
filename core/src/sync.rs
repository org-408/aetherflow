//! `std` / `loom` switching shim.
//!
//! Loom is a verifier that **exhaustively explores the interleavings** of concurrent execution,
//! and to do so it must replace the atomics and `UnsafeCell` with its own instrumented types.
//! Only under `--cfg aetherflow_loom` do we use loom's types; ordinary builds use `std` as-is
//! (= no loom ends up in the production binary).
//!
//! The awkward part is that the `UnsafeCell` API differs between the two: loom "passes a raw
//! pointer to a closure" (to tell the verifier the access scope), whereas `std` returns a raw
//! pointer via `.get()`. So we wrap the `std` side in a thin wrapper with the same `with` /
//! `with_mut` as loom, **keeping the call sites (`mpsc` / `spsc`) as a single body of code**.
//!
//! Whereas Miri looks at UB (undefined behavior), Loom looks at **ordering and visibility** ──
//! different roles, so both are needed.
//!
//! The cfg is named `aetherflow_loom` rather than `loom` because `RUSTFLAGS`'s `--cfg`
//! **propagates to all dependency crates**. Using a bare `--cfg loom` would break crates that
//! have their own `cfg(loom)` paths but do not depend on loom (e.g. `concurrent-queue` via
//! dev-dependencies). This is the same workaround crossbeam uses with `crossbeam_loom`.

#[cfg(aetherflow_loom)]
pub(crate) use loom::cell::UnsafeCell;
#[cfg(aetherflow_loom)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
#[cfg(aetherflow_loom)]
pub(crate) use loom::sync::Arc;

#[cfg(not(aetherflow_loom))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
#[cfg(not(aetherflow_loom))]
pub(crate) use std::sync::Arc;

/// The `std` version of `UnsafeCell` (a wrapper to match loom's `with` / `with_mut` API).
///
/// It exists solely to keep the call sites identical to the loom version, and thanks to
/// `#[inline(always)]` it compiles down to the same code as a bare `UnsafeCell::get()` in
/// ordinary builds (zero cost).
#[cfg(not(aetherflow_loom))]
#[derive(Debug)]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(aetherflow_loom))]
impl<T> UnsafeCell<T> {
    pub(crate) fn new(data: T) -> UnsafeCell<T> {
        UnsafeCell(std::cell::UnsafeCell::new(data))
    }

    /// Both reads and writes go through `with_mut` (because slot access is always performed under
    /// the discipline that "a single thread touches it exclusively"). The `with` for shared access
    /// present in the loom version currently has no callers, so we do not add it to the wrapper either.
    #[inline(always)]
    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}
