//! ゼロアロケーション request-reply(`ask`)。
//!
//! kameo/tokio の `ask` は**呼び出しごとに reply チャネル(oneshot)を heap 確保**する。これが
//! ask の隠れコストで、フレームワーク越しの ask が遅い一因。
//!
//! 本実装は reply cell を**呼び出し側のスタック**に置き、[`Responder`] に生ポインタで渡す。
//! 呼び出し側は返事が来るまでブロックするので、**cell の生存はブロック期間中に保証される** →
//! heap 確保ゼロ。生ポインタを使うのは、メッセージ(`A::Message: Send + 'static`)に借用を載せ
//! られないため。安全性は「呼び出し側がブロックして cell を生かし続ける」プロトコルで担保する。
//!
//! [`Responder`] は **一度だけ返信できる線形トークン**(`reply` が self を consume)= iso 的。
//! 返信せず捨てられたら、呼び出し側は [`AskError::NoReply`] で起きる(デッドロックしない)。
//!
//! **制約**: `ask` は呼び出しスレッドをブロックする。**handler の中から同一コアの actor へ ask
//! するとデッドロック**(send_blocking と同じ)。runtime の外(main / I/O スレッド)から使うこと。

use crate::{Actor, ActorRef, SendError};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

const PENDING: u8 = 0;
const REPLIED: u8 = 1;
const ABANDONED: u8 = 2;

/// 呼び出し側スタックに置かれる返信スロット。actor が書き、呼び出し側が読む。
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

/// 一度だけ返信できる線形トークン(iso 的)。`reply` が self を consume する。
pub struct Responder<R> {
    cell: *const ReplyCell<R>,
}

// cell へのアクセスは state atomic(Release/Acquire)で単一 writer/reader に同期される。
// R: Send なら別コアへ渡して安全。
unsafe impl<R: Send> Send for Responder<R> {}

impl<R> Responder<R> {
    /// 返信する(トークンを消費)。
    pub fn reply(self, value: R) {
        // SAFETY: 呼び出し側が REPLIED を Acquire で観測するまでブロックしており、cell は生存。
        unsafe {
            (*(*self.cell).slot.get()).write(value);
            // この Release store が呼び出し側を解放する境界。以降 cell に触れてはならない
            // (呼び出し側が return して cell(スタック)を解放しうる = UAF になる)。
            (*self.cell).state.store(REPLIED, Ordering::Release);
        }
        // ゆえに Drop(ABANDONED 化 CAS)を走らせない。未返信で捨てられた場合のみ Drop が CAS する。
        std::mem::forget(self);
    }
}

impl<R> Drop for Responder<R> {
    fn drop(&mut self) {
        // reply されずに捨てられた場合のみ ABANDONED にして呼び出し側を Err で起こす。
        // SAFETY: cell は呼び出し側がブロックして生かしている。
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

/// `ask` の失敗理由。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AskError {
    /// actor が返信せずにメッセージ(Responder)を捨てた(送信後に停止・drop・panic 等)。
    NoReply,
    /// 送信時点で受信 actor が既に消滅していた(送る先が無い)。
    /// NoReply(送った後に返信されずに死んだ)とは区別される。
    Closed,
    /// handler 内から**同一コア**の actor へ ask した = ブロックすればデッドロックする状況。
    /// ハングせずこのエラーで返す。cross-actor の応答が要るなら別コアに置くか tell で設計する。
    WouldBlockCallingCore,
}

impl<A: Actor> ActorRef<A> {
    /// request-reply。`make_msg` に [`Responder`] を渡してメッセージを組み立て、送って返事を待つ。
    /// **heap 確保ゼロ**(reply cell は呼び出しスタック上)。
    ///
    /// 例: `let r: u64 = addr.ask(|resp| MyMsg::Get(resp))?;`
    ///
    /// **注意**: 呼び出しスレッドをブロックする。handler 内から同一コアへ ask しないこと(deadlock)。
    pub fn ask<R>(&self, make_msg: impl FnOnce(Responder<R>) -> A::Message) -> Result<R, AskError>
    where
        R: Send + 'static,
    {
        // デッドロックガード: 同一コアの handler 内から ask するとハングするので、
        // ハングせずエラーで返す(send_blocking と違い ask は Result を返せる)。
        if self.would_deadlock_calling_core() {
            return Err(AskError::WouldBlockCallingCore);
        }
        let cell = ReplyCell::<R>::new();
        let responder = Responder { cell: &cell };
        let msg = make_msg(responder);
        // 送信時点で actor が消滅していれば Closed(NoReply は送信後に返信されない場合)。
        // ここで抜けることで、ReplyCell(スタック上)への遅延書き込みも起こらない。
        if let Err(SendError::Closed(_)) = self.send_blocking(msg) {
            return Err(AskError::Closed);
        }

        // 返事待ち(spin → yield → 短 sleep のバックオフ。ブロックだが CPU を焼き続けない)。
        let mut waited: u32 = 0;
        loop {
            match cell.state.load(Ordering::Acquire) {
                REPLIED => {
                    // SAFETY: REPLIED を観測 = actor の書き込み(Release)が可視。1 回だけ move out。
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::System;

    #[test]
    fn ask_returns_reply() {
        struct Doubler;
        // メッセージ = (入力, 返信トークン)
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
        let addr = sys.spawn_on(2, Echo); // 別コア
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
                // 返信せずに drop → 呼び出し側は NoReply で起きる
            }
        }
        let sys = System::with_cores(1);
        let addr = sys.spawn_on(0, Rude);
        assert_eq!(addr.ask(|resp| resp), Err(AskError::NoReply));
        drop(addr);
        sys.shutdown();
    }
}
