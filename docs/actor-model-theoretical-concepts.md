# Actor Model Theoretical Concepts

## Actor as a Fundamental Unit of Computation

Key properties:

Identity: Every actor is distinct from other actors.
State: Each actor maintains a private state that other actors cannot directly access.
Behavior: An actor's behavior defines how it processes messages and how it reacts to them, which may change over time.

## Messages and Message Passing

Key points:

**Messages are asynchronous**
Actors communicate **exclusively by sending messages**.
**No shared memory** between actors—everything is done via message passing.
Messages can **trigger behaviors**, change the actor's state, or result in sending messages to other actors.

## Formalization of the Actor World

### Actor Set and Behavior

A = {a_1, a_2, a_3, ... }

B_i_t: State_i_t x Message -> (State_i_t+1, Message Set, B_i_t+1)

This behavior can:

Send messages to other actors.
Create new actors.
Change its internal state.
Adopt a new behavior (using the become mechanism).

### Message Set

Each message has:

A sender (the actor who sends the message).
A receiver (the actor to whom the message is addressed).
A content (the data or action that the message is carrying).

M = {(a_sender, a_receiver, content)}

## Actor Creation and Dynamic Networks

Create(a_new, Initial State) -> A_t-1 + {a_new} = A_t

## Formal Notation for Actor Systems

the time t: (Actors, Messages, States)\_t

## Actor State details

Identity: 必須！
ID は固定！不変
name, address は可変

Address book: 必須！登録・削除可能にしておく

State:

## Thread, Thread Pool, Scheduler, Runtime

Actor Model の理論とは別にランタイムの概念が必要。
標準には、Thread Pool, Scheduler, Runtime の機能はないため外部クレートが必要。

### Thread Pool

rayon
tokio(サイズ変更など)

### Scheduler

tokio
async-std
rayon
