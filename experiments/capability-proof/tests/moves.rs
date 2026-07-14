//! typed + move API が実行時にも正しく動くことの確認(コンパイル判定は lib.rs の doctest 側)。

use capability_proof::{spawn, Actor};
use std::sync::mpsc;

/// move で渡したメッセージが actor に届き、処理されることを確認する。
#[test]
fn message_is_delivered_by_move() {
    struct Echo {
        out: mpsc::Sender<u32>,
    }
    impl Actor for Echo {
        type Message = u32;
        fn handle(&mut self, msg: u32) {
            self.out.send(msg).unwrap();
        }
    }

    let (tx, rx) = mpsc::channel();
    let echo = spawn(Echo { out: tx });

    echo.send(42);
    echo.send(7);

    assert_eq!(rx.recv().unwrap(), 42);
    assert_eq!(rx.recv().unwrap(), 7);
}

/// actor が自状態(`ref` = `&mut self`)をロック無しで更新できることを確認する。
#[test]
fn actor_owns_and_mutates_its_state_without_locks() {
    struct Counter {
        total: u64,
        out: mpsc::Sender<u64>,
    }
    impl Actor for Counter {
        type Message = u64;
        fn handle(&mut self, msg: u64) {
            self.total += msg; // &mut self: 単一所有者による更新、ロック不要
            self.out.send(self.total).unwrap();
        }
    }

    let (tx, rx) = mpsc::channel();
    let counter = spawn(Counter { total: 0, out: tx });

    counter.send(10);
    counter.send(20);
    counter.send(5);

    assert_eq!(rx.recv().unwrap(), 10);
    assert_eq!(rx.recv().unwrap(), 30);
    assert_eq!(rx.recv().unwrap(), 35);
}
