# Pony reference capabilities ⇄ Rust 所有権 対応表

> この文書の位置づけ: `design.md` §4 が **「最優先で潰すべきリスク」** と自認している問い ——
> 「Rust の型システムで Pony 相当の reference capabilities をどこまで再現できるか」—— に答える。
> `direction-and-roadmap.md` の着手 1(理論詰め B)の成果物。
>
> **結論(先出し)**: **土台は成立する。** この runtime が必要とするカプ(`iso`/`val`/`tag`/`ref`)は
> すべて Rust に写り、特に核心の `iso`(actor 間で move するメッセージ)は Pony より**綺麗に・静的に完全に**
> 再現できる。埋まらない差分(`trn`/`recover`/viewpoint adaptation)は **critical path の外**。
> → design.md §4 の go/no-go は **go**。

---

## 1. なぜこれを最初に詰めるか

design.md の 4 本柱のうち ①型による隔離・②コピーゼロの move は、「Rust の型システムが Pony の
capability 相当を表現できる」ことに全面的に依存している。ここが成立しなければ土台から崩れるので、
コードより先に潰す。裏返せば **ここが通れば、実装は typed actor + move という一本道に確定する**。

---

## 2. Pony の reference capabilities とは

Pony は 6 つの capability で「この参照で何ができるか」+「同じオブジェクトへの**別名(alias)がどこまで
存在してよいか**」を型レベルで表現する。中心概念は **deny capabilities** = 「その参照が、他の参照に
何を**禁止する**か」。禁止が強いほど、そのオブジェクトを actor 間で安全に渡せる。

| cap | 読/書 | 許す別名(ローカル) | 許す別名(グローバル=他 actor) | 送信可 |
|---|---|---|---|---|
| `iso` | 読+書・一意 | なし(読取含め自分だけ) | なし | ✅ |
| `trn` | 読+書・書込一意 | 読取専用(`box`)は可 | なし | ❌ |
| `ref` | 読+書 | `ref`/`box` 可 | なし | ❌ |
| `val` | 読のみ・不変 | 読取のみ多数 | 読取のみ多数 | ✅ |
| `box` | 読のみ(ビュー) | 他所に `ref` 書込者がいても可 | なし | ❌ |
| `tag` | 不可(識別のみ) | 何でも | 何でも | ✅ |

- **sendable = {`iso`, `val`, `tag`}**。理由: `iso` は自分以外に読み手すらいない、`val` は誰も書かない、
  `tag` はそもそも中身に触れない —— いずれもデータ競合が原理的に起きない。
- **subtype 格子**: `iso <: trn <: ref <: box <: tag`、`trn <: val <: box`、`iso <: val`。
  上ほど「他への制約が強い(=送信安全に近い)」、下ほど弱い。
- **`consume x`** = ローカル束縛 `x` を破棄して値を取り出す = **move**。`iso` と組むと Rust の move そのもの。
- **`recover` ブロック** = sendable な入力だけで組み上げたオブジェクトを、末尾で `ref → iso` / `box → val`
  に**昇格**する仕組み(別名が漏れていないことをコンパイラが証明)。「可変で組んでから不変/一意として送る」。
- **viewpoint adaptation(arrow 型)** = フィールドを参照経由で読むと、得られるカプは「経路のカプ × フィールドの
  カプ」の合成になる(例: `box` 経由で `iso` フィールドを読むと `tag` に潰れる)。細粒度の別名合成規則。

---

## 3. Rust の型システムの 2 軸

Rust は直交する 2 軸 + move で、上記に相当する仕事をする。

1. **所有権と借用**(コンパイル時の別名規律): owned `T` / `&mut T`(排他借用)/ `&T`(共有借用)。
   借用チェッカが「可変 1 つ XOR 共有多数」を強制。
2. **`Send` / `Sync` マーカ**(型のスレッド安全性): `Send` = 所有権を別スレッドへ移して安全 /
   `Sync` = `&T` を別スレッドで共有して安全(`T: Sync ⟺ &T: Send`)。
3. **move セマンティクス**: 値を move すると元の束縛は無効化され、以後の使用は**コンパイルエラー**。
   = `consume` + `iso`。

**重要な構造差**: Pony のカプは**参照ごと**に付く(同じ `String` を場所により `iso`/`val`/`ref` と別名でき、
格子が整合性を追う)。Rust の `Send`/`Sync` は**型ごと**に固定。別名規律(`&mut`/`&`)は借用ごとだが、
値が持ち運ぶ一級の capability ではない。→ Rust は「参照ごとの細粒度カプ」は持たないが、
**runtime に必要なカプは型 + 借用 + move で足りる**(§5)。

---

## 4. 対応表(6 caps → Rust、忠実度 ★)

| Pony cap | 意味 | Rust 対応 | 忠実度 | 備考 |
|---|---|---|---|---|
| `iso` | 一意可変・sendable | owned `T: Send` を **move**(`Box<T>`) | ★★★ | Rust の方が上。借用チェッカが静的に完全に一意性を強制、注釈不要 |
| `val` | 不変共有・sendable | `Arc<T> where T: Sync`(不変運用)/ `&'static T` | ★★★ | broadcast など多数共有に |
| `tag` | 識別のみ・sendable | 不透明ハンドル `ActorRef`(送信のみ、状態は読めない) | ★★★ | actor ハンドルそのもの |
| `ref` | actor ローカル可変 | `&mut self` / actor 自身のフィールド、`Rc<RefCell<T>>`(ローカル共有) | ★★☆ | Rust `&mut` は**排他**、Pony `ref` は他ローカル `ref` を許す |
| `box` | 可変かもしれない物の読取ビュー | `&T` 共有借用 | ★★☆ | `&T` は同時 `&mut` を禁止、Pony `box` はローカル書込者を許容 |
| `trn` | 書込一意・ローカル読取可 | **直接の対応なし** | ★☆☆ | `&mut T`(読者ゼロ)か `RefCell`(実行時)で代用 |

---

## 5. Rust が勝つ所 / 再現できない所

### 勝つ所 —— 主役の `iso` は Pony より綺麗
- **`iso` + `consume` = owned 値 + move**。Rust では注釈ゼロで、move すれば use-after-move が
  コンパイルエラー。これは design.md 柱②そのもの。**actor が実際に送るもの = sendable な一意可変 = `iso`**
  を、Rust は**言語の既定動作として**、静的に完全に表現する。ランタイムにとって一番重いカプを一番綺麗に持つ。
- カプ注釈を全型に付ける必要がない(所有権は構造的)。

### 再現できない所 —— ただし critical path の外
1. **`trn`**(書込一意 + ローカル読取可): 「自分だけが書くが他はローカルに読める」を静的に言えない。
   `&mut`(読者も禁止)か `RefCell`(実行時借用)に落ちる。
2. **`recover`**(`ref→iso` / `box→val` 昇格): 共有型を一意型へ静的に**格上げ**できない。
   `Arc<T>` を選んだら、静的に一意 `T` を取り戻せない(`Arc::try_unwrap` は実行時)。
   Rust の代替は**スコープ**(関数内で組んで move で返す)—— 表現力は recover に劣る。
3. **viewpoint adaptation(arrow 型)**: 「フィールドを読んで得るカプが経路のカプに依存する」機構が無い。
   `&self.field` は経路に関係なく `&Field`。`box->iso = tag` の自動崩しに相当するものが無い。

**なぜ外なのか**: 上の 3 つはいずれも**オブジェクト内部の細粒度別名の柔軟性**に関する話。
AetherFlow のコア Thesis が要求するのは「メッセージを move(iso)/ 不変を共有(val)/ 不透明ハンドル(tag)/
actor ローカル状態(ref)」の 4 つだけで、そのすべてが ★★★〜★★☆ で写る。
→ 差分は **「Pony より弱い所」として明記した上で non-goal にする**。ブロッカーにしない。

---

## 6. AetherFlow への設計含意(ここが本命)

カプ対応から、API 形が一意に決まる。

```rust
trait Actor: Sized + Send + 'static {
    type Message: Send + 'static;              // 送るもの = iso 相当。Send がその sendability を型で強制
    fn handle(&mut self, msg: Self::Message);  // &mut self = ref(自状態の一意所有・ロック不要)
                                               // msg by value = iso の consume(move で受け取る)
                                               // 同期・run-to-completion（async を hot path から追放）
}
```

- `Self::Message: Send` = `iso` の送信可能性を、型システムが保証。
- `handle` が `msg` を**値で**受け取る = move = `iso` の `consume`。→ `Arc` も `Mutex` も `dyn` も不要。
- `&mut self` = actor が自状態の唯一の所有者(`ref`)。ロック不要。単一 actor が単一スレッドで回すから成立。
- `ActorRef<A>` = 型付き `Sender<A::Message>`(SPSC の producer 端)を持つ。
  = actor への `tag`(識別 + 送信のみ)+ `iso` メッセージを enqueue する能力。
- 不変を多数に配る場合のみ `val` = `Arc<M> where M: Sync`。

### 現状実装がなぜ崩れていたか(再掲・因果が繋がる)
`dyn Message` に型消去 → move 不能 → `Arc` に頼る → **`iso` を失い `val` 的共有に落ちる** → 柱②(zero-copy)崩壊。
`Arc<Mutex<Actor>>` → actor が `ref`(ローカル)でなく `val`+ロックに → 「単一所有者がロック無しで回す」性質を喪失。
→ **型消去をやめて associated `Message` 型 + move にするだけで、①②③④が一直線に揃う。**

---

## 7. typed の代償と、その代償を Pony と同じ境界で払う

typed(associated Message 型)には現実的コストがあるが、**それを払う場所は Pony が `tag` を使う場所と一致する**。

- **異種の子を親が束ねる**: `Vec<ActorRef<A>>` は `A` が違うと持てない。
  → **制御面(control-plane)だけを `tag` 相当の不透明ハンドルに型消去**する:
    停止・再起動・監視など**識別 + 制御のみ**を提供する `dyn` ハンドル。**型付き send はしない**。
    データ面(data-plane)の送信は `ActorRef<A>` に残す。
  → これは Pony が「識別と制御は `tag`、中身の受け渡しは `iso`/`val`」と引く境界そのもの。
    型消去は**恣意的な妥協ではなく、理論が示す正しい切れ目**に置かれる。
- **1 つの actor が複数種のメッセージを扱う**: `enum Message { A(..), B(..) }`。
  Akka の `Any` に比べ手間だが、これが `iso` + zero-copy の代価。受け入れる。

---

## 8. 「Akka なら気づかず壊れる / これはコンパイルが通らない」プレビュー

Stage C(検証実験)で作る最小例の芯。理論が実コードで効くことの予告。

**現状 AetherFlow / Akka 流(隔離は実行時の規約)**:
```rust
let order = Arc::new(Order { qty: 100 });
actor_ref.tell(order.clone()).await;  // clone で送信元・受信側の両方が保持
order // ← まだ使える。内部が可変なら送信後に共有可変が観測されうる = 隔離は規約でしかない
```

**目指す形(隔離を型が強制)**:
```rust
let order = Order { qty: 100 };
actor.send(order);   // move で移譲
order.qty = 200;     // ← コンパイルエラー: borrow of moved value `order`
```

後者は `iso` の「送ったらもう触れない」を**コンパイラが**保証する。これが「Akka なら気づかず壊れるが、
この設計ではコンパイルが通らない」の最小形。

---

## 9. 結論 —— go/no-go

- runtime にとって load-bearing なカプ = **`iso`(actor 間で move するメッセージ)は Rust の方が綺麗**
  (構造的・注釈不要・静的に完全)。
- `val`(不変共有)/`tag`(不透明ハンドル)/`ref`(actor ローカル状態)も綺麗に写る。
- 埋まらない差分 `trn`/`recover`/viewpoint adaptation は **細粒度の内部別名の話で、この runtime の
  critical path から外れる**。既知の弱点として明記し non-goal にする。

**→ design.md §4 の「最優先で潰すべきリスク」は解消。Rust の型システムは、この runtime が要求する
reference capabilities を再現できる。実装は typed actor + move の一本道に確定。**

---

## 10. 未解決・次(Stage C へ)

- **Stage C**: 上 §8 の最小例を独立した小 crate で実際にコンパイルさせ、
  「壊れる版はコンパイルが通る/通らない」の対を実証する(型保証の裏取り)。
- `trn`/viewpoint adaptation を将来どうしても使いたくなった場合の退避先(`RefCell` 局所化など)は、
  必要になった時に別途検討(現時点では non-goal)。
- §6 の `Actor` trait は API のたたき台。SPSC mailbox・コアピン留めランタイムと接続する N=1 実装(着手 3)で確定させる。

---

## 11. 関連文書
- `concepts-explained.md` — **本書の噛み砕き版(用語集)**。高尚で読みづらいと感じたらまずこちら。
- `design.md` — 技術的 Thesis(§2.2 Pony、§4 最優先リスク)
- `direction-and-roadmap.md` — 方向性と道筋(本書は着手 1 = 理論詰め B の成果物)
- `actor-model-theoretical-concepts.md` — Hewitt actor モデルの形式化
- 原典: Clebsch et al., *"Deny Capabilities for Safe, Fast Actors"* (AGERE 2015)
