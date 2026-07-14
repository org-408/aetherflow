//! コア配置(best-effort)。OS ごとに「できる範囲で最も良い」ことをする。
//!
//! - **Linux / Windows**: `core_affinity` 経由で実際に特定コアへハードピン留め
//!   (`sched_setaffinity` 等)。ベンチの権威ある数字はここで測る。ARM Linux(Graviton)も可。
//! - **macOS**: 特定コアへのハードピン留めは OS が許さない(ARM の制約ではなく OS の制約)。
//!   best-effort として QoS クラスを `USER_INTERACTIVE` にする。
//!   - **Apple Silicon**: P/E コアがあるので、この QoS は **P コア(高性能コア)に寄せる**効果があり、
//!     busy-spin スレッドが E コアに載るのを避けられる。
//!   - **Intel Mac**: コアが均質で P/E の区別が無い → **この QoS はコア配置を変えない**(無害だが
//!     尾は締まらない)。Intel macOS には affinity タグ(`THREAD_AFFINITY_POLICY`)という
//!     キャッシュ共有ヒントもあるが、ハード pin ではなく効果は限定的。未実装(実機 Linux が本命)。

/// 現在のスレッドを、その OS でできる最良の形でコアに寄せる。
/// 何らかの配置調整が効いたら true。
pub fn pin_current_thread_to(core: usize) -> bool {
    // macOS: ハードピン留め不可。P コアへ寄せる QoS を要求。
    #[cfg(target_os = "macos")]
    let macos_qos = request_performance_qos();
    #[cfg(not(target_os = "macos"))]
    let macos_qos = false;

    // Linux/Windows: 実際に指定コアへピン留め(macOS では set_for_current は効かない)。
    let hard_pin = match core_affinity::get_core_ids() {
        Some(ids) => match ids.get(core).copied() {
            Some(id) => core_affinity::set_for_current(id),
            None => false,
        },
        None => false,
    };

    hard_pin || macos_qos
}

/// 利用可能な論理コア数(取得できなければ None)。
pub fn available_cores() -> Option<usize> {
    core_affinity::get_core_ids().map(|ids| ids.len())
}

/// macOS: 呼び出しスレッドの QoS を `USER_INTERACTIVE` にする。
///
/// ハードピン留めではない。**Apple Silicon** では P/E コア間の移動を抑えて tail を締める効果が
/// 期待できるが、**Intel Mac は P/E が無いのでコア配置は変わらない**(無害)。libSystem に常に
/// ある関数なので追加クレート不要で extern 宣言して呼ぶ。
#[cfg(target_os = "macos")]
pub fn request_performance_qos() -> bool {
    // <sys/qos.h>: QOS_CLASS_USER_INTERACTIVE = 0x21
    const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21;
    extern "C" {
        fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
    }
    unsafe { pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0) == 0 }
}
