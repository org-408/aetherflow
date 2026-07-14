# capability-proof (Stage C)

`docs/pony-rust-capability-mapping.md` の理論を、**コンパイラに証明させる**独立 crate。
親 workspace(現在ビルド不能)から `[workspace]` で detach してある。

## 走らせる

```sh
cargo test              # 理論の裏取り(doctest 3 + 実行時テスト 2)
cargo run --example demo   # typed + move actor を実際に動かす
```

## 何を証明しているか

| | 主張 | 実証 |
|---|---|---|
| 証明1 | typed + move は隔離を**型が強制** | `send(order)` 後に `order` を使うと `E0382: borrow of moved value` でコンパイル不能(`compile_fail` doctest) |
| 証明3 | Akka 流 `Arc<Mutex<_>>` は隔離が**規約だけ** | `tell(order.clone())` 後の `order.lock().qty = 200` が素通りでコンパイル |

結論: **Rust の型システムは actor の隔離(`iso`)をコンパイル時に保証できる。** design.md §4 の最優先リスクは解消。

## 落とし穴(将来テストを足す人へ)

- move した値の使用検出には**実際に使う**こと。`let _ = x.field;` は move を発火しない → 偽の合格になる。`println!` 等で使う。
- コンパイル可否をシェルで見るとき、`rustc ... | head` の後の `$?` は `head` の終了コード。パイプ無しで exit を取ること。

詳しい理論は `../../docs/pony-rust-capability-mapping.md`、噛み砕きは `../../docs/concepts-explained.md`。
