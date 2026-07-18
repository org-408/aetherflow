//! pub/sub(一対多ブロードキャスト)— チャットルーム風。Hub が購読者全員に配る。
//!
//!   cargo run --example pubsub
//!
//! ポイント: `ActorRef` は **Clone + Send** なので、**メッセージに載せて渡せる**(= 購読の登録)。
//! Hub は購読者リストを単一所有で持ち、`Publish` を受けたら全員に **try_send**(非ブロッキング)で配る。
//! handler の中から他 actor へ送るときは `try_send`(`send_blocking` は同一コアだとデッドロック)。

use aetherflow::{Actor, ActorRef, System};

/// 購読者。受け取ったメッセージを自分の名前付きで表示するだけ。
struct Subscriber {
    name: &'static str,
}
impl Actor for Subscriber {
    type Message = String;
    fn handle(&mut self, msg: String) {
        println!("  [{}] {msg}", self.name);
    }
}

/// ブロードキャスト Hub。購読者の送信ハンドルを保持し、Publish を全員に配る。
struct Hub {
    subscribers: Vec<ActorRef<Subscriber>>,
}
enum HubMsg {
    /// 購読登録(ActorRef をメッセージに載せて渡す)。
    Subscribe(ActorRef<Subscriber>),
    /// 全購読者へ配信。
    Publish(String),
}
impl Actor for Hub {
    type Message = HubMsg;
    fn handle(&mut self, msg: HubMsg) {
        match msg {
            HubMsg::Subscribe(sub) => self.subscribers.push(sub),
            HubMsg::Publish(text) => {
                for sub in &self.subscribers {
                    // 非ブロッキング。相手が満杯/消滅なら握りつぶす(pub/sub の best-effort 配信)。
                    let _ = sub.try_send(text.clone());
                }
            }
        }
    }
}

fn main() {
    let sys = System::with_cores(1);

    let hub = sys.spawn_on(
        0,
        Hub {
            subscribers: Vec::new(),
        },
    );

    // 3 人の購読者を立てて Hub に登録(ハンドルをメッセージで渡す)。
    for name in ["alice", "bob", "carol"] {
        let sub = sys.spawn_on(0, Subscriber { name });
        hub.send_blocking(HubMsg::Subscribe(sub)).unwrap();
    }

    // 配信すると全員に届く。
    println!("publishing 2 messages to 3 subscribers:");
    hub.send_blocking(HubMsg::Publish("hello everyone".into())).unwrap();
    hub.send_blocking(HubMsg::Publish("meeting at 3pm".into())).unwrap();

    drop(hub);
    sys.shutdown(); // drain してから停止 = 全配信が処理される
    println!("ok");
}
