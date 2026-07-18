//! tokio 相互運用 — AetherFlow を tokio アプリの中に**埋める**(段階導入)。
//!
//!   cargo run --example tokio_interop
//!
//! 使いどころ: 既存の async I/O(HTTP サーバ・DB クライアント等)は tokio のまま、**状態と計算だけ**
//! AetherFlow の actor に載せる。フル書き換え無しで、ホットな状態管理をロックフリー・単一所有に移せる。
//!
//! 相互運用の規則(2つだけ):
//!  - **async → actor の送信は非ブロッキング**(`try_send` / `send_blocking`)。そのまま async から呼べる。
//!  - **`ask`(返事待ち)は呼び出しスレッドをブロック**するので、async ランタイムを止めないよう
//!    `tokio::task::spawn_blocking` の中で呼ぶ。

use aetherflow::{Actor, Responder, System};

/// tokio 側の多数の非同期ハンドラから叩かれる、共有の集計状態(でもロックは無い)。
struct Metrics {
    requests: u64,
    bytes: u64,
}

enum Cmd {
    Record { bytes: u64 },
    Snapshot(Responder<(u64, u64)>),
}

impl Actor for Metrics {
    type Message = Cmd;
    fn handle(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Record { bytes } => {
                self.requests += 1;
                self.bytes += bytes;
            }
            Cmd::Snapshot(reply) => reply.reply((self.requests, self.bytes)),
        }
    }
}

#[tokio::main]
async fn main() {
    // AetherFlow runtime を tokio アプリの中で立てる(共有機なので backoff で CPU を焼かない設定でもよい)。
    let sys = System::with_cores(1);
    let metrics = sys.spawn_on(
        0,
        Metrics {
            requests: 0,
            bytes: 0,
        },
    );

    // 100 個の「非同期リクエストハンドラ」を模擬。各タスクは async I/O をした体で、
    // 結果を AetherFlow の actor に**非ブロッキングで**送る。
    let mut handles = Vec::new();
    for i in 0..100u64 {
        let m = metrics.clone();
        handles.push(tokio::spawn(async move {
            // ここで本来は await でネットワーク I/O をする(tokio の領分)。
            tokio::task::yield_now().await;
            // 状態更新は actor に委譲(ロック不要・単一所有)。送信は非ブロッキング。
            let _ = m.try_send(Cmd::Record { bytes: 100 + i });
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // 集計を読む = ask(返事待ち)。ブロックするので spawn_blocking の中で呼ぶ。
    let m = metrics.clone();
    let (requests, bytes) = tokio::task::spawn_blocking(move || m.ask(Cmd::Snapshot).unwrap())
        .await
        .unwrap();

    println!("requests = {requests}, bytes = {bytes}");
    assert_eq!(requests, 100);
    assert_eq!(bytes, (0..100u64).map(|i| 100 + i).sum());

    drop(metrics);
    // shutdown もブロックするので blocking スレッドで(async を止めない)。
    tokio::task::spawn_blocking(move || sys.shutdown())
        .await
        .unwrap();
    println!("ok — AetherFlow ran as the state core inside a tokio app");
}
