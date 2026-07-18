//! Core placement (best-effort). Does "the best it can" on each OS.
//!
//! - **Linux / Windows**: actually hard-pins to a specific core via `core_affinity`
//!   (`sched_setaffinity`, etc.). The authoritative benchmark numbers are measured
//!   here. ARM Linux (Graviton) works too.
//! - **macOS**: the OS does not permit hard-pinning to a specific core (an OS
//!   constraint, not an ARM one). As a best-effort, it sets the QoS class to
//!   `USER_INTERACTIVE`.
//!   - **Apple Silicon**: with P/E cores, this QoS has the effect of **steering
//!     toward the P cores (performance cores)**, avoiding having busy-spin threads
//!     land on E cores.
//!   - **Intel Mac**: cores are homogeneous with no P/E distinction → **this QoS
//!     does not change core placement** (harmless, but the tail is not tightened).
//!     Intel macOS also has an affinity tag (`THREAD_AFFINITY_POLICY`) as a
//!     cache-sharing hint, but it is not a hard pin and its effect is limited.
//!     Not implemented (real-hardware Linux is the priority).

/// Steer the current thread toward a core, in the best form the OS allows.
/// Returns true if some placement adjustment took effect.
pub fn pin_current_thread_to(core: usize) -> bool {
    // macOS: hard-pinning not possible. Request a QoS that steers toward the P cores.
    #[cfg(target_os = "macos")]
    let macos_qos = request_performance_qos();
    #[cfg(not(target_os = "macos"))]
    let macos_qos = false;

    // Linux/Windows: actually pin to the specified core (on macOS, set_for_current has no effect).
    let hard_pin = match core_affinity::get_core_ids() {
        Some(ids) => match ids.get(core).copied() {
            Some(id) => core_affinity::set_for_current(id),
            None => false,
        },
        None => false,
    };

    hard_pin || macos_qos
}

/// Number of available logical cores (None if it cannot be obtained).
pub fn available_cores() -> Option<usize> {
    core_affinity::get_core_ids().map(|ids| ids.len())
}

/// macOS: set the calling thread's QoS to `USER_INTERACTIVE`.
///
/// Not a hard pin. On **Apple Silicon** it can be expected to curb migration
/// between P/E cores and tighten the tail, but **Intel Mac has no P/E, so core
/// placement does not change** (harmless). The function is always present in
/// libSystem, so it is declared extern and called without any extra crate.
#[cfg(target_os = "macos")]
pub fn request_performance_qos() -> bool {
    // <sys/qos.h>: QOS_CLASS_USER_INTERACTIVE = 0x21
    const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21;
    extern "C" {
        fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
    }
    unsafe { pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0) == 0 }
}
