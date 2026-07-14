- Actor
- Superviser
- ActorSystem
- Akka extensions
- ActorRef: address
- message -> ActorRef -> mailbox -> Actor
- Dispatcher: call Actor, through actor mailbox to push Actor. use thread model
- Akka remote module
- ActorSystem: { RestApi actor, BoxOffice actor, TicketSeller actor }, RestApi <- akka-http
- BoxOffice create TicketSeller name RHCP
- HTTP Server launch flow
  - ConfigFactory.load()
  - get host + port
  - system = ActorSystem()
  - ec = system.dispatcher (ExecutionContext)
  - api = new RestApi(system, requestTimeout(config)).routes
  - am = ActorMaterializer()
  - bindFuture = Http().bindAndHandle(ap, host, port)
- Test
  - TestActorRef
  - TestKit
  - SilentActor: check notExceptionThrow, stopActor, changeInnerState
  - SendingActor: check responseMessage
  - SideEffectingActor: check reactionOfObject
- Props
  - generate, regenerate actor
- Superviser
  - parent actor = made actor = superviser
  - restart from Props,
  - resume (ignore crash),
  - stop
  - escalate throw parant(superviser)
- LifeCycle
  - event
    - start ActorContext.actorOf -> generate child actor
    - restart
    - stop ActorContext.stop or send PoisonPill message
  - hooks
    - preStart
    - postStop
    - preRestart
    - postRestart
  - state
    - Started
    - Terminated
- ActorContext owns
- deadLettersActorRef
- Future, Promise
- pipe pattern
- Network
  - Node
  - 4 category
    - latency
    - crash
    - memory access
    - parallel
  - RemoteRef
    - path
    - deploy
  - ActorRefProvider
  - akka.remote.netty.tcp
  - enabled-transports
  - hostname = "0.0.0.0"
  - port = 2552
  - Guardian actor = user
  - actorSelection
    - frontend = ActorSystem("frontend", config)
    - path = "akka.tcp://backend@0.0.0.0:2551/user/simple"
    - simple = frontend.actorSelection(path) # search query
  - FrontendMain extends App with Startup
    - api = new RestApi()
    - def createBoxOffice: ActorRef = {
      - val path = createPath()
      - system.actorOf(Props(new RemoteLookupProxy(path)), "lookupBoxOffice")
    - }
    - startup(api.routes)
  - BackendMain extends App with RequestTimeout
    - api = new RestApi()
    - system.actorOf(BoxOffice.pops, BoxOffice.name)
  - RemoteLookupProxy actor: trans message and connect error handling
    - state
      - identify
      - active
    - method
      - become
  - RemoteLookupProxy(path:String) extends Actor with ActorLoggin
    - selection = context.actorSelection(path)
    - def identify: Receive
      - case ActorIdentity(`path`, Some(actor)) # <- actor = remote actor, get ActorRef
        - context.become(active(actor))
        - contect.watch(actor)
  - config
    - actor {
      - provider = "akka.remote.RemoteActorRefProvider"
      -
      - deployment {
        - /boxOffice {
          - remote = "akka.tcp://backend@0.0.0.0:2552"
        - }
      - }
    - }
  - akka-cluster module: dynamic remote deploy
    - uri = "akka.tcp://backend@0.0.0.0:2552"
    - backendAddress = AddressFromURIString(uri)
    - props = Props[BoxOffice].withDeploy(
      - Deploy(scope = RemoteScope(backendAddress))
    - )
    - context.actorOf(props, "boxOffice")
- Config
  - application.conf etc
- ConfigFactory
  - load
    - application.conf
    - application.json
    - application.properties
    - reference.conf = fallback config
  - getInt
  - getString
- application.conf
  - version
  - name
  - description
  - database
    - connect
    - user
    - password
    - ..
  - variable
    - hostname=XXXX -> ${hostname}
    - ?HOST_NAME = environment variable
- ActorSystem(name, config)
  - if not to set config, then ConfigFactory.load()
- Pipe
- Filter
- ScatterGather

akka バイブルキーワード抽出

- actor operation
  - sendcreate
  - become
  - supervise
- actor system -> actor service
- actor_ref
- mailbox
- dispatcher
- test
  - silent_actor
  - sending_actor
  - side_effecting_actor
- actor life_cycle
  - start
  - stop
  - restart
- supervision
- Future
- scale_out
  - remote
    - remote ref
    - remote deploy
- config
  - default
- logging
- deploy
- pipe & filter
- enterprise integration patterns
  - scatter_gather
    - receiver_list pattern scatter
    - aggregate pattern gather
  - routing slip
  - router pattern
    - pool router
      - create
      - remote routy
      - dynamic router
      - supervise
    - group router
      - create
      - router group dynamic size change
    - consistent hashing router
    - internal base routing
    - state base routing
    - router implement
    - routing strategy
      - round robin pool
      - random pool
      - smallest mailbox pool
      - round robin group
      - random group
      - smallest mailbox group
- message channel
  - point to point channel
  - publish subscribe channel
    - event stream
    - custom event bus
  - special channel
    - dead letter channel
    - Guaranteed Delivery Channel
- finite state machine
- agent
- streaming
  - source
  - flow
    - bidi flow
  - sink
  - back pressure
  - materialize
  - optimize
    - buffering
    - parallel
    - slottoring
  - consumer
  - producer
    - buffer
  - graph speed
  - streaming http
- system integration

---

Akka の概念と機能を以下のように分類しました：

| 主要項目                                    | サブ項目                                                         | 説明                                                   |
| ------------------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------------ |
| 1. akka.actor                               | default-dispatcher                                               | アクターシステムのデフォルトディスパッチャー設定       |
|                                             | deployment                                                       | アクターのデプロイメント設定                           |
|                                             | serialize-messages                                               | メッセージのシリアル化を有効にするかどうか             |
|                                             | serialize-creators                                               | クリエーターのシリアル化を有効にするかどうか           |
|                                             | allow-java-serialization                                         | Java シリアル化を許可するかどうか                      |
|                                             | creation-timeout                                                 | アクター作成のタイムアウト時間                         |
|                                             | unstarted-push-timeout                                           | 未開始アクターへのメッセージプッシュのタイムアウト時間 |
|                                             | typed.timeout                                                    | typed actor のタイムアウト設定                         |
|                                             | warn-about-java-serializer-usage                                 | Java シリアライザー使用時の警告を有効にするかどうか    |
|                                             | serializers                                                      | カスタムシリアライザーの定義                           |
|                                             | serialization-bindings                                           | クラスとシリアライザーのバインディング                 |
|                                             | router.type-mapping                                              | ルーターの種類とクラスのマッピング                     |
|                                             | guardian-supervisor-strategy                                     | ルートガーディアンの監視戦略                           |
|                                             | debug                                                            | デバッグ関連の設定                                     |
|                                             | mailbox                                                          | メールボックスの設定                                   |
|                                             | default-mailbox                                                  | デフォルトメールボックスの設定                         |
|                                             | default-dispatcher                                               | デフォルトディスパッチャーの設定                       |
|                                             | default-blocking-io-dispatcher                                   | ブロッキング I/O 用のデフォルトディスパッチャー設定    |
|                                             | default-pinned-dispatcher                                        | ピン留めされたディスパッチャーのデフォルト設定         |
| 2. akka.remote                              | enabled-transports                                               | 有効にするトランスポートの種類                         |
|                                             | transport-failure-detector                                       | トランスポート失敗検出の設定                           |
|                                             | watch-failure-detector                                           | ウォッチ失敗検出の設定                                 |
|                                             | transport                                                        | リモート通信のトランスポート設定                       |
|                                             | log-remote-lifecycle-events                                      | リモートライフサイクルイベントのログ記録               |
|                                             | retry-gate-closed-for                                            | ゲートクローズ後の再試行時間                           |
|                                             | prune-quarantine-marker-after                                    | 隔離マーカーの削除時間                                 |
|                                             | startup-timeout                                                  | スタートアップのタイムアウト時間                       |
|                                             | shutdown-timeout                                                 | シャットダウンのタイムアウト時間                       |
|                                             | flush-wait-on-shutdown                                           | シャットダウン時のフラッシュ待機時間                   |
|                                             | use-passive-connections                                          | パッシブ接続の使用                                     |
|                                             | backoff                                                          | バックオフ設定                                         |
|                                             | command-ack-timeout                                              | コマンド確認応答のタイムアウト                         |
|                                             | use-unsafe-remote-features-outside-cluster                       | クラスター外での安全でないリモート機能の使用           |
|                                             | trusted-selection-paths                                          | 信頼されたセレクションパス                             |
|                                             | untrusted-mode                                                   | 信頼されていないモード                                 |
|                                             | max-frame-size                                                   | 最大フレームサイズ                                     |
|                                             | system-message-buffer-size                                       | システムメッセージバッファサイズ                       |
|                                             | system-message-ack-piggyback-timeout                             | システムメッセージ確認応答のピギーバックタイムアウト   |
|                                             | resend-interval                                                  | 再送間隔                                               |
| 3. akka.cluster                             | min-nr-of-members                                                | クラスターの最小メンバー数                             |
|                                             | seed-nodes                                                       | シードノードのリスト                                   |
|                                             | seed-node-timeout                                                | シードノードのタイムアウト時間                         |
|                                             | retry-unsuccessful-join-after                                    | 参加失敗後の再試行間隔                                 |
|                                             | shutdown-after-unsuccessful-join-seed-nodes                      | シードノード参加失敗後のシャットダウン時間             |
|                                             | periodic-tasks-initial-delay                                     | 定期タスクの初期遅延                                   |
|                                             | gossip-interval                                                  | ゴシッププロトコルの間隔                               |
|                                             | gossip-time-to-live                                              | ゴシップの生存時間                                     |
|                                             | leader-actions-interval                                          | リーダーアクションの間隔                               |
|                                             | unreachable-nodes-reaper-interval                                | 到達不能ノードの刈り取り間隔                           |
|                                             | publish-stats-interval                                           | 統計情報の公開間隔                                     |
|                                             | failure-detector                                                 | 失敗検出の設定                                         |
|                                             | monitored-by-nr-of-members                                       | 監視するメンバー数                                     |
|                                             | max-sample-size                                                  | サンプルサイズの最大値                                 |
|                                             | allow-weakly-up-members                                          | 弱い状態のメンバーを許可するかどうか                   |
|                                             | roles                                                            | ノードのロール                                         |
|                                             | role                                                             | ノードのデフォルトロール                               |
|                                             | run-coordinated-shutdown-when-down                               | ダウン時に調整されたシャットダウンを実行するかどうか   |
|                                             | coordinated-shutdown-phases                                      | 調整されたシャットダウンのフェーズ                     |
| 4. akka.persistence                         | journal                                                          | ジャーナルの設定                                       |
|                                             | snapshot-store                                                   | スナップショットストアの設定                           |
|                                             | at-least-once-delivery                                           | 最低 1 回配信の設定                                    |
|                                             | max-concurrent-recoveries                                        | 同時リカバリーの最大数                                 |
|                                             | internal-stash-overflow-strategy                                 | 内部スタッシュオーバーフロー戦略                       |
|                                             | journal.plugin                                                   | 使用するジャーナルプラグイン                           |
|                                             | snapshot-store.plugin                                            | 使用するスナップショットストアプラグイン               |
|                                             | max-message-batch-size                                           | メッセージバッチの最大サイズ                           |
|                                             | persistence-id-separator                                         | 永続化 ID のセパレーター                               |
|                                             | snapshot-after                                                   | スナップショット作成のイベント数しきい値               |
|                                             | snapshot-retention-policy                                        | スナップショット保持ポリシー                           |
|                                             | fsm.snapshot-after                                               | FSM のスナップショット作成のイベント数しきい値         |
|                                             | view.auto-update                                                 | ビューの自動更新設定                                   |
|                                             | view.auto-update-interval                                        | ビューの自動更新間隔                                   |
|                                             | view.auto-update-replay-max                                      | ビューの自動更新時の最大リプレイ数                     |
|                                             | at-least-once-delivery.redeliver-interval                        | 再配信の間隔                                           |
|                                             | at-least-once-delivery.warn-after-number-of-unconfirmed-attempts | 未確認試行回数の警告しきい値                           |
|                                             | at-least-once-delivery.max-unconfirmed-messages                  | 未確認メッセージの最大数                               |
|                                             | internal-stash-overflow-strategy                                 | 内部スタッシュオーバーフロー戦略                       |
| 5. akka.stream                              | materializer                                                     | マテリアライザーの設定                                 |
|                                             | blocking-io-dispatcher                                           | ブロッキング I/O 用ディスパッチャー                    |
|                                             | subscription-timeout                                             | サブスクリプションのタイムアウト                       |
|                                             | buffer-size                                                      | バッファサイズ                                         |
|                                             | max-fixed-buffer-size                                            | 固定バッファの最大サイズ                               |
|                                             | dispatcher                                                       | ストリーム用のディスパッチャー                         |
|                                             | debug-logging                                                    | デバッグログの有効化                                   |
|                                             | output-burst-limit                                               | 出力バーストの制限                                     |
|                                             | sync-processing-limit                                            | 同期処理の制限                                         |
|                                             | debug.fuzzing-mode                                               | ファジングモードの有効化                               |
|                                             | io.tcp                                                           | TCP の設定                                             |
|                                             | io.file                                                          | ファイル I/O の設定                                    |
|                                             | materializer.debug.fuzzing-mode                                  | マテリアライザーのファジングモード                     |
|                                             | materializer.initial-input-buffer-size                           | 初期入力バッファサイズ                                 |
|                                             | materializer.max-input-buffer-size                               | 最大入力バッファサイズ                                 |
|                                             | materializer.dispatcher                                          | マテリアライザーのディスパッチャー                     |
|                                             | materializer.subscription-timeout                                | マテリアライザーのサブスクリプションタイムアウト       |
|                                             | materializer.blocking-io-dispatcher                              | マテリアライザーのブロッキング I/O ディスパッチャー    |
|                                             | materializer.output-burst-limit                                  | マテリアライザーの出力バースト制限                     |
|                                             | materializer.sync-processing-limit                               | マテリアライザーの同期処理制限                         |
| 6. akka.http                                | server                                                           | HTTP サーバーの設定                                    |
|                                             | client                                                           | HTTP クライアントの設定                                |
|                                             | host-connection-pool                                             | ホスト接続プールの設定                                 |
|                                             | parsing                                                          | パース関連の設定                                       |
|                                             | server.server-header                                             | サーバーヘッダーの設定                                 |
|                                             | server.preview.enable-http2                                      | HTTP/2 の有効化（プレビュー）                          |
|                                             | server.idle-timeout                                              | アイドルタイムアウト                                   |
|                                             | server.request-timeout                                           | リクエストタイムアウト                                 |
|                                             | server.bind-timeout                                              | バインドタイムアウト                                   |
|                                             | server.linger-timeout                                            | リンガータイムアウト                                   |
|                                             | server.max-connections                                           | 最大接続数                                             |
|                                             | server.pipelining-limit                                          | パイプライニング制限                                   |
|                                             | server.verbose-error-messages                                    | 詳細なエラーメッセージの有効化                         |
|                                             | client.user-agent-header                                         | ユーザーエージェントヘッダー                           |
|                                             | client.connecting-timeout                                        | 接続タイムアウト                                       |
|                                             | client.idle-timeout                                              | クライアントアイドルタイムアウト                       |
|                                             | client.request-header-size-hint                                  | リクエストヘッダーサイズのヒント                       |
|                                             | host-connection-pool.max-connections                             | プールの最大接続数                                     |
|                                             | host-connection-pool.min-connections                             | プールの最小接続数                                     |
|                                             | host-connection-pool.max-retries                                 | 最大リトライ回数                                       |
|                                             | host-connection-pool.max-open-requests                           | 最大オープンリクエスト数                               |
|                                             | parsing.max-content-length                                       | 最大コンテンツ長                                       |
|                                             | parsing.max-header-count                                         | 最大ヘッダー数                                         |
| 7. akka.discovery                           | method                                                           | サービス検出メソッド                                   |
|                                             | kubernetes-api                                                   | Kubernetes API 設定                                    |
|                                             | config                                                           | 設定ベースの検出設定                                   |
|                                             | aggregate                                                        | 集約検出設定                                           |
|                                             | dns                                                              | DNS 検出設定                                           |
|                                             | kubernetes-api.pod-label-selector                                | Kubernetes ポッドラベルセレクター                      |
|                                             | kubernetes-api.pod-namespace                                     | Kubernetes ポッド名前空間                              |
|                                             | kubernetes-api.request-timeout                                   | Kubernetes API リクエストタイムアウト                  |
|                                             | config.services                                                  | 設定ベースのサービス定義                               |
|                                             | aggregate.discovery-methods                                      | 集約する検出メソッド                                   |
|                                             | dns.protocol                                                     | DNS 検出プロトコル                                     |
|                                             | dns.resolv-conf                                                  | resolv.conf ファイルのパス                             |
|                                             | dns.resolve-srv                                                  | SRV レコードの解決                                     |
|                                             | dns.use-ipv6                                                     | IPv6 の使用                                            |
|                                             | dns.resolve-timeout                                              | DNS 解決タイムアウト                                   |
|                                             | dns.async-dns                                                    | 非同期 DNS 解決の使用                                  |
|                                             | method                                                           | デフォルトの検出メソッド                               |
|                                             | kubernetes-api.api-ca-path                                       | Kubernetes API CA 証明書パス                           |
|                                             | kubernetes-api.api-token-path                                    | Kubernetes API トークンパス                            |
|                                             | kubernetes-api.api-service-host                                  | Kubernetes API サービスホスト                          |
|                                             | kubernetes-api.api-service-port                                  | Kubernetes API サービスポート                          |
| 8. akka.coordinated-shutdown                | phases                                                           | シャットダウンフェーズの定義                           |
|                                             | phase-timeout                                                    | 各フェーズのタイムアウト                               |
|                                             | timeout                                                          | 全体のタイムアウト                                     |
|                                             | exit-jvm                                                         | JVM 終了の有効化                                       |
|                                             | run-by-jvm-shutdown-hook                                         | JVM シャットダウンフックでの実行                       |
|                                             | coordinated-shutdown-phases                                      | カスタムシャットダウンフェーズの定義                   |
|                                             | default-phase-timeout                                            | デフォルトのフェーズタイムアウト                       |
|                                             | terminate-actor-system                                           | アクターシステム終了フェーズのタイムアウト             |
|                                             | exit-jvm-failure-reason                                          | JVM 終了時の失敗理由                                   |
|                                             | abort-timeout                                                    | 中断タイムアウト                                       |
|                                             | reason-overrides                                                 | 理由に基づくオーバーライド                             |
|                                             | lease-linger                                                     | リースの延長時間                                       |
|                                             | cooperative-shutdown-timeout                                     | 協調シャットダウンのタイムアウト                       |
|                                             | force-abort-timeout                                              | 強制中断タイムアウト                                   |
|                                             | coordinated-shutdown-phases.before-service-unbind                | サービスアンバインド前のフェーズ                       |
|                                             | coordinated-shutdown-phases.service-unbind                       | サービスアンバインドフェーズ                           |
|                                             | coordinated-shutdown-phases.service-requests-done                | サービスリクエスト完了フェーズ                         |
|                                             | coordinated-shutdown-phases.service-stop                         | サービス停止フェーズ                                   |
|                                             | coordinated-shutdown-phases.before-cluster-shutdown              | クラスターシャットダウン前フェーズ                     |
|                                             | coordinated-shutdown-phases.cluster-sharding-shutdown-region     | クラスターシャーディング領域シャットダウンフェーズ     |
|                                             | coordinated-shutdown-phases.cluster-leave                        | クラスター離脱フェーズ                                 |
|                                             | coordinated-shutdown-phases.cluster-exiting                      | クラスター退出中フェーズ                               |
|                                             | coordinated-shutdown-phases.cluster-exiting-done                 | クラスター退出完了フェーズ                             |
|                                             | coordinated-shutdown-phases.cluster-shutdown                     | クラスターシャットダウンフェーズ                       |
|                                             | coordinated-shutdown-phases.before-actor-system-terminate        | アクターシステム終了前フェーズ                         |
|                                             | coordinated-shutdown-phases.actor-system-terminate               | アクターシステム終了フェーズ                           |
| 9. akka.scheduler                           | tick-duration                                                    | スケジューラーのティック間隔                           |
|                                             | ticks-per-wheel                                                  | 一つのホイールあたりのティック数                       |
|                                             | implementation                                                   | スケジューラーの実装クラス                             |
|                                             | shutdown-timeout                                                 | シャットダウンタイムアウト                             |
|                                             | priority-levels                                                  | 優先度レベル数                                         |
|                                             | tick-duration-target                                             | 目標ティック間隔                                       |
|                                             | tick-duration-warning-threshold                                  | ティック間隔警告しきい値                               |
|                                             | shutdown-timeout                                                 | シャットダウンタイムアウト                             |
|                                             | jdk-timer                                                        | JDK タイマーの使用                                     |
|                                             | ticks-per-wheel                                                  | ティックホイールあたりのティック数                     |
|                                             | millis-per-tick                                                  | 1 ティックあたりのミリ秒                               |
|                                             | timers-default-timeout                                           | タイマーのデフォルトタイムアウト                       |
|                                             | instance-name                                                    | スケジューラーインスタンス名                           |
|                                             | tick-duration-target                                             | 目標ティック間隔                                       |
|                                             | tick-duration-error-margin                                       | ティック間隔エラーマージン                             |
|                                             | tick-duration-error-ratio                                        | ティック間隔エラー比率                                 |
|                                             | tick-duration-warn-ratio                                         | ティック間隔警告比率                                   |
|                                             | shutdown-timeout                                                 | シャットダウンタイムアウト                             |
| 10. akka.io                                 | tcp                                                              | TCP 設定                                               |
|                                             | udp                                                              | UDP 設定                                               |
|                                             | dns                                                              | DNS 設定                                               |
|                                             | tcp.register-timeout                                             | TCP レジスター待機時間                                 |
|                                             | tcp.max-channels                                                 | 最大 TCP チャンネル数                                  |
|                                             | tcp.select-timeout                                               | TCP 選択タイムアウト                                   |
|                                             | tcp.maximum-frame-size                                           | 最大フレームサイズ                                     |
|                                             | tcp.trace-logging                                                | トレースロギングの有効化                               |
|                                             | udp.receive-throughput                                           | UDP 受信スループット                                   |
|                                             | udp.send-throughput                                              | UDP 送信スループット                                   |
|                                             | udp.maximum-frame-size                                           | UDP の最大フレームサイズ                               |
|                                             | dns.resolver                                                     | DNS 解決プロバイダー                                   |
|                                             | dns.async-dns.resolve-timeout                                    | 非同期 DNS 解決タイムアウト                            |
|                                             | dns.async-dns.resolv-conf                                        | resolv.conf ファイルのパス                             |
|                                             | dns.async-dns.name-servers                                       | 名前サーバーのリスト                                   |
|                                             | tcp.batch-accept-limit                                           | バッチ受け入れ制限                                     |
|                                             | tcp.file-io-transferTo-limit                                     | ファイル IO 転送制限                                   |
|                                             | tcp.file-io-write-chunk-size                                     | ファイル IO 書き込みチャンクサイズ                     |
|                                             | tcp.socket-options                                               | ソケットオプション                                     |
| 11. akka.serialization                      | serializers                                                      | シリアライザーの定義                                   |
|                                             | serialization-bindings                                           | シリアル化バインディング                               |
|                                             | verify-serializability                                           | シリアル化可能性の検証                                 |
|                                             | warn-about-java-serializer-usage                                 | Java シリアライザー使用の警告                          |
|                                             | serializers.java                                                 | Java シリアライザーの設定                              |
|                                             | serializers.bytes                                                | バイトアレイシリアライザーの設定                       |
|                                             | serializers.primitive-long                                       | プリミティブ Long シリアライザーの設定                 |
|                                             | serializers.primitive-int                                        | プリミティブ Int シリアライザーの設定                  |
|                                             | serializers.primitive-string                                     | プリミティブ String シリアライザーの設定               |
|                                             | serializers.primitive-double                                     | プリミティブ Double シリアライザーの設定               |
|                                             | serializers.primitive-float                                      | プリミティブ Float シリアライザーの設定                |
|                                             | serializers.primitive-byte                                       | プリミティブ Byte シリアライザーの設定                 |
|                                             | serializers.primitive-boolean                                    | プリミティブ Boolean シリアライザーの設定              |
|                                             | serializers.primitive-char                                       | プリミティブ Char シリアライザーの設定                 |
|                                             | serializers.primitive-short                                      | プリミティブ Short シリアライザーの設定                |
|                                             | serializers.primitive-unit                                       | プリミティブ Unit シリアライザーの設定                 |
|                                             | serialization-bindings                                           | クラスとシリアライザーのバインディング                 |
|                                             | enable-additional-serialization-bindings                         | 追加のシリアル化バインディングの有効化                 |
|                                             | verify-serializable-messages                                     | シリアル化可能メッセージの検証                         |
| 12. akka.ssl-config                         | ssl                                                              | SSL 設定                                               |
|                                             | ssl.protocol                                                     | SSL プロトコル                                         |
|                                             | ssl.enabled-algorithms                                           | 有効なアルゴリズム                                     |
|                                             | ssl.keyManager                                                   | キーマネージャー                                       |
|                                             | ssl.trustManager                                                 | トラストマネージャー                                   |
|                                             | ssl.enabledCipherSuites                                          | 有効な暗号スイート                                     |
|                                             | ssl.hostnameVerifier                                             | ホスト名検証                                           |
|                                             | ssl.keystore                                                     | キーストア設定                                         |
|                                             | ssl.truststore                                                   | トラストストア設定                                     |
|                                             | ssl.debug                                                        | SSL デバッグオプション                                 |
|                                             | ssl.loose                                                        | ゆるい設定オプション                                   |
|                                             | ssl.default-context                                              | デフォルト SSL コンテキスト                            |
|                                             | ssl.cert-chain-file                                              | 証明書チェーンファイル                                 |
|                                             | ssl.private-key-file                                             | 秘密鍵ファイル                                         |
|                                             | ssl.key-password                                                 | 秘密鍵パスワード                                       |
|                                             | ssl.loose.allowWeakCiphers                                       | 弱い暗号の許可                                         |
|                                             | ssl.loose.allowWeakProtocols                                     | 弱いプロトコルの許可                                   |
|                                             | ssl.loose.allowLegacyHelloMessages                               | レガシーな Hello メッセージの許可                      |
|                                             | ssl.loose.allowUnsafeRenegotiation                               | 安全でない再ネゴシエーションの許可                     |
| 13. akka.cluster.sharding                   | state-store-mode                                                 | 状態ストアモード                                       |
|                                             | remember-entities                                                | エンティティの記憶                                     |
|                                             | least-shard-allocation-strategy                                  | 最小シャード割り当て戦略                               |
|                                             | rebalance-interval                                               | リバランス間隔                                         |
|                                             | coordinator-singleton                                            | コーディネーターシングルトン設定                       |
|                                             | retry-interval                                                   | 再試行間隔                                             |
|                                             | handoff-timeout                                                  | ハンドオフタイムアウト                                 |
|                                             | shard-start-timeout                                              | シャード開始タイムアウト                               |
|                                             | shard-failure-backoff                                            | シャード失敗バックオフ                                 |
|                                             | entity-restart-backoff                                           | エンティティ再起動バックオフ                           |
|                                             | rebalance-absolute-limit                                         | リバランスの絶対制限                                   |
|                                             | rebalance-relative-limit                                         | リバランスの相対制限                                   |
|                                             | least-shard-allocation-max-simultaneous-rebalance                | 最大同時リバランス数                                   |
|                                             | waiting-for-state-timeout                                        | 状態待機タイムアウト                                   |
|                                             | updating-state-timeout                                           | 状態更新タイムアウト                                   |
|                                             | version-vector                                                   | バージョンベクトル設定                                 |
|                                             | passivate-idle-entity-after                                      | アイドルエンティティの非アクティブ化時間               |
|                                             | sharding-region-name                                             | シャーディング領域名                                   |
|                                             | journal-plugin-id                                                | ジャーナルプラグイン ID                                |
|                                             | snapshot-plugin-id                                               | スナップショットプラグイン ID                          |
| 14. akka.cluster.singleton                  | singleton-name                                                   | シングルトン名                                         |
|                                             | role                                                             | シングルトンのロール                                   |
|                                             | hand-over-retry-interval                                         | ハンドオーバー再試行間隔                               |
|                                             | min-number-of-hand-over-retries                                  | 最小ハンドオーバー再試行回数                           |
|                                             | use-lease                                                        | リースの使用                                           |
|                                             | lease-implementation                                             | リース実装                                             |
|                                             | lease-retry-interval                                             | リース再試行間隔                                       |
|                                             | remove-internal-cluster-singleton-messages-after                 | 内部クラスターシングルトンメッセージの削除時間         |
|                                             | buffer-size                                                      | バッファサイズ                                         |
|                                             | singleton-proxy.singleton-name                                   | シングルトンプロキシ名                                 |
|                                             | singleton-proxy.role                                             | シングルトンプロキシのロール                           |
|                                             | singleton-proxy.singleton-identification-interval                | シングルトン識別間隔                                   |
|                                             | singleton-proxy.buffer-size                                      | プロキシバッファサイズ                                 |
|                                             | allow-multiple-oldest-nodes                                      | 複数の最古ノードの許可                                 |
|                                             | stable-after                                                     | 安定化時間                                             |
|                                             | jitter                                                           | ジッター                                               |
|                                             | lease-retry-interval                                             | リース再試行間隔                                       |
|                                             | lease-majority-check-interval                                    | リース多数チェック間隔                                 |
|                                             | lease-operation-timeout                                          | リース操作タイムアウト                                 |
| 15. akka.cluster.pubsub                     | gossip-interval                                                  | ゴシップ間隔                                           |
|                                             | removed-time-to-live                                             | 削除されたエントリーの生存時間                         |
|                                             | max-delta-elements                                               | 最大デルタ要素数                                       |
|                                             | routing-logic                                                    | ルーティングロジック                                   |
|                                             | send-to-dead-letters-when-no-subscribers                         | サブスクライバーがいない場合のデッドレター送信         |
|                                             | shard-size                                                       | シャードサイズ                                         |
|                                             | max-shard-number                                                 | 最大シャード数                                         |
|                                             | distributed-pubsub-mediator                                      | 分散 pubsub メディエーター設定                         |
|                                             | use-dispatcher                                                   | 使用するディスパッチャー                               |
|                                             | gossip-different-view-probability                                | 異なるビューのゴシップ確率                             |
|                                             | keep-removed-time-to-live                                        | 削除されたエントリーの保持時間                         |
|                                             | tombstone-time-to-live                                           | トゥームストーンの生存時間                             |
|                                             | max-delta-size                                                   | 最大デルタサイズ                                       |
|                                             | pruning-interval                                                 | 刈り込み間隔                                           |
|                                             | log-restoration-on-recovery                                      | リカバリー時のログ復元                                 |
|                                             | publish-local                                                    | ローカル公開の有効化                                   |
|                                             | send-local-first                                                 | ローカル優先送信                                       |
|                                             | shard-redistribution-interval                                    | シャード再配布間隔                                     |
|                                             | expected-update-delay                                            | 予想更新遅延                                           |
| 16. akka.cluster.metrics                    | collector                                                        | メトリクスコレクター                                   |
|                                             | collector-sample-interval                                        | コレクターサンプル間隔                                 |
|                                             | gossip-interval                                                  | ゴシップ間隔                                           |
|                                             | moving-average-half-life                                         | 移動平均の半減期                                       |
|                                             | metrics-gossip-interval                                          | メトリクスゴシップ間隔                                 |
|                                             | periodic-tasks-initial-delay                                     | 定期タスクの初期遅延                                   |
|                                             | sigar-native-library                                             | Sigar ネイティブライブラリパス                         |
|                                             | adaptive-load-balancing                                          | 適応型負荷分散設定                                     |
|                                             | collector-provider                                               | コレクタープロバイダー                                 |
|                                             | collector-class                                                  | コレクタークラス                                       |
|                                             | metric-filters                                                   | メトリクスフィルター                                   |
|                                             | gossip-time-to-live                                              | ゴシップの生存時間                                     |
|                                             | failure-detector                                                 | 失敗検出設定                                           |
|                                             | metrics-selector                                                 | メトリクスセレクター                                   |
|                                             | load-balancing-selector                                          | 負荷分散セレクター                                     |
|                                             | periodic-sample-interval                                         | 定期サンプル間隔                                       |
|                                             | initial-gossip-timeout                                           | 初期ゴシップタイムアウト                               |
|                                             | retry-gossip-timeout                                             | ゴシップ再試行タイムアウト                             |
|                                             | capacity-threshold                                               | 容量しきい値                                           |
| 17. akka.cluster.tools                      | singleton                                                        | シングルトン設定                                       |
|                                             | pub-sub                                                          | パブサブ設定                                           |
|                                             | client                                                           | クライアント設定                                       |
|                                             | singleton.singleton-name                                         | シングルトン名                                         |
|                                             | singleton.role                                                   | シングルトンのロール                                   |
|                                             | singleton.hand-over-retry-interval                               | ハンドオーバー再試行間隔                               |
|                                             | pub-sub.name                                                     | パブサブ名                                             |
|                                             | pub-sub.role                                                     | パブサブのロール                                       |
|                                             | pub-sub.routing-logic                                            | ルーティングロジック                                   |
|                                             | client.receptionist                                              | レセプショニスト設定                                   |
|                                             | client.receptionist.name                                         | レセプショニスト名                                     |
|                                             | client.receptionist.role                                         | レセプショニストのロール                               |
|                                             | client.receptionist.number-of-contacts                           | コンタクト数                                           |
|                                             | client.receptionist.responsible-nodes                            | 責任ノード                                             |
|                                             | client.receptionist.gossip-interval                              | ゴシップ間隔                                           |
|                                             | client.receptionist.notify-subscribers-interval                  | サブスクライバー通知間隔                               |
|                                             | client.receptionist.max-buffer-size                              | 最大バッファサイズ                                     |
|                                             | client.receptionist.pruning-interval                             | プルーニング間隔                                       |
|                                             | client.receptionist.serialize-messages                           | メッセージのシリアル化                                 |
|                                             | client.receptionist.use-dispatcher                               | 使用するディスパッチャー                               |
|                                             | client.receptionist.distributed-key                              | 分散キー                                               |
|                                             | client.receptionist.shard-size                                   | シャードサイズ                                         |
|                                             | client.receptionist.max-delta-elements                           | 最大デルタ要素数                                       |
|                                             | client.receptionist.gossip-different-view-probability            | 異なるビューのゴシップ確率                             |
|                                             | client.receptionist.retain-removed-time-to-live                  | 削除されたエントリーの保持時間                         |
|                                             | client.receptionist.max-pruning-dissemination                    | 最大プルーニング伝播                                   |
|                                             | client.receptionist.pruning-marker-time-to-live                  | プルーニングマーカーの生存時間                         |
|                                             | client.receptionist.log-restoration-on-recovery                  | リカバリー時のログ復元                                 |
| 18. akka.cluster.ddata                      | replicator-name                                                  | レプリケーター名                                       |
|                                             | use-dispatcher                                                   | 使用するディスパッチャー                               |
|                                             | gossip-interval                                                  | ゴシップ間隔                                           |
|                                             | notify-subscribers-interval                                      | サブスクライバー通知間隔                               |
|                                             | max-delta-elements                                               | 最大デルタ要素数                                       |
|                                             | serializer-bindings                                              | シリアライザーバインディング                           |
|                                             | durable                                                          | 永続性設定                                             |
|                                             | keys                                                             | キー設定                                               |
|                                             | prefer-oldest                                                    | 最古のデータを優先                                     |
|                                             | distributed-data                                                 | 分散データ設定                                         |
|                                             | role                                                             | レプリケーターのロール                                 |
|                                             | gossip-time-to-live                                              | ゴシップの生存時間                                     |
|                                             | read-majority-plus                                               | 読み取り多数プラス設定                                 |
|                                             | write-majority-plus                                              | 書き込み多数プラス設定                                 |
|                                             | read-timeout                                                     | 読み取りタイムアウト                                   |
|                                             | write-timeout                                                    | 書き込みタイムアウト                                   |
|                                             | delta-crdt.enabled                                               | Delta CRDT の有効化                                    |
|                                             | delta-crdt.max-delta-size                                        | 最大デルタサイズ                                       |
|                                             | delta-crdt.propagation-interval                                  | 伝播間隔                                               |
| 19. akka.remote.artery                      | enabled                                                          | Artery の有効化                                        |
|                                             | transport                                                        | トランスポート設定                                     |
|                                             | canonical.hostname                                               | 正規ホスト名                                           |
|                                             | canonical.port                                                   | 正規ポート                                             |
|                                             | bind.hostname                                                    | バインドホスト名                                       |
|                                             | bind.port                                                        | バインドポート                                         |
|                                             | ssl                                                              | SSL 設定                                               |
|                                             | large-message-destinations                                       | 大きなメッセージの宛先                                 |
|                                             | advanced                                                         | 高度な設定                                             |
|                                             | advanced.inbound-lanes                                           | 受信レーン数                                           |
|                                             | advanced.outbound-lanes                                          | 送信レーン数                                           |
|                                             | advanced.buffer-pool-size                                        | バッファプールサイズ                                   |
|                                             | advanced.maximum-frame-size                                      | 最大フレームサイズ                                     |
|                                             | advanced.idle-cpu-level                                          | アイドル CPU レベル                                    |
|                                             | advanced.compression                                             | 圧縮設定                                               |
|                                             | advanced.aeron                                                   | Aeron 設定                                             |
|                                             | advanced.aeron.embedded-media-driver                             | 組み込みメディアドライバー                             |
|                                             | advanced.aeron.aeron-dir                                         | Aeron ディレクトリ                                     |
|                                             | advanced.aeron.delete-aeron-dir                                  | Aeron ディレクトリの削除                               |
| 20. akka.typed                              | extension-id                                                     | 拡張 ID                                                |
|                                             | scheduler                                                        | スケジューラー設定                                     |
|                                             | receptionist                                                     | レセプショニスト設定                                   |
|                                             | coordinated-shutdown                                             | 調整されたシャットダウン設定                           |
|                                             | logger-class                                                     | ロガークラス                                           |
|                                             | log-level                                                        | ログレベル                                             |
|                                             | mailbox-pool-size                                                | メールボックスプールサイズ                             |
|                                             | mailbox-push-timeout-time                                        | メールボックスプッシュタイムアウト時間                 |
|                                             | timeout-service                                                  | タイムアウトサービス設定                               |
|                                             | timeout-service.slow-call-timeout                                | 遅いコールのタイムアウト                               |
|                                             | timeout-service.default-timeout                                  | デフォルトタイムアウト                                 |
|                                             | timeout-service.check-interval                                   | チェック間隔                                           |
|                                             | stash-capacity                                                   | スタッシュ容量                                         |
|                                             | stash-overflow-strategy                                          | スタッシュオーバーフロー戦略                           |
|                                             | guardian-supervisor-strategy                                     | ガーディアン監視戦略                                   |
|                                             | default-mailbox                                                  | デフォルトメールボックス                               |
|                                             | debug                                                            | デバッグ設定                                           |
|                                             | fsm                                                              | 有限状態機械設定                                       |
|                                             | serialize-messages                                               | メッセージのシリアル化                                 |
|                                             | serializers                                                      | シリアライザー設定                                     |
| 21. akka.dispatcher                         | default-dispatcher                                               | デフォルトディスパッチャーの設定                       |
|                                             | default-dispatcher.type                                          | ディスパッチャーの種類                                 |
|                                             | default-dispatcher.executor                                      | 実行器の設定                                           |
|                                             | default-dispatcher.throughput                                    | スループット                                           |
|                                             | default-dispatcher.throughput-deadline-time                      | スループットデッドライン時間                           |
|                                             | default-dispatcher.mailbox-capacity                              | メールボックス容量                                     |
|                                             | default-dispatcher.mailbox-push-timeout-time                     | メールボックスプッシュタイムアウト時間                 |
|                                             | default-dispatcher.mailbox-type                                  | メールボックスの種類                                   |
|                                             | default-fork-join-dispatcher                                     | デフォルトフォークジョインディスパッチャー             |
|                                             | default-fork-join-dispatcher.parallelism-min                     | 最小並列度                                             |
|                                             | default-fork-join-dispatcher.parallelism-factor                  | 並列度係数                                             |
|                                             | default-fork-join-dispatcher.parallelism-max                     | 最大並列度                                             |
|                                             | internal-dispatcher                                              | 内部ディスパッチャー                                   |
|                                             | default-blocking-io-dispatcher                                   | デフォルトブロッキング I/O ディスパッチャー            |
|                                             | default-thread-pool-dispatcher                                   | デフォルトスレッドプールディスパッチャー               |
|                                             | default-resizer                                                  | デフォルトリサイザー                                   |
|                                             | default-resizer.lower-bound                                      | 下限                                                   |
|                                             | default-resizer.upper-bound                                      | 上限                                                   |
|                                             | default-resizer.pressure-threshold                               | 圧力しきい値                                           |
|                                             | default-resizer.rampup-rate                                      | ランプアップレート                                     |
| 22. akka.mailbox                            | mailbox-type                                                     | メールボックスの種類                                   |
|                                             | mailbox-capacity                                                 | メールボックス容量                                     |
|                                             | mailbox-push-timeout-time                                        | メールボックスプッシュタイムアウト時間                 |
|                                             | priority-mailbox                                                 | 優先度メールボックス                                   |
|                                             | bounded-mailbox                                                  | 制限付きメールボックス                                 |
|                                             | unbounded-mailbox                                                | 無制限メールボックス                                   |
|                                             | durable-mailbox                                                  | 永続メールボックス                                     |
|                                             | durable-mailbox.store-dir                                        | 永続メールボックスのストアディレクトリ                 |
|                                             | durable-mailbox.keep-journal                                     | ジャーナルの保持                                       |
|                                             | durable-mailbox.circuit-breaker                                  | サーキットブレーカー設定                               |
|                                             | mailbox-requirement-mapping                                      | メールボックス要件マッピング                           |
|                                             | stash-capacity                                                   | スタッシュ容量                                         |
|                                             | stash-overflow-strategy                                          | スタッシュオーバーフロー戦略                           |
|                                             | mailbox-selector                                                 | メールボックスセレクター                               |
|                                             | mailbox-implementation-mapping                                   | メールボックス実装マッピング                           |
|                                             | default-mailbox                                                  | デフォルトメールボックス                               |
|                                             | dead-letters                                                     | デッドレター設定                                       |
|                                             | event-stream                                                     | イベントストリーム設定                                 |
|                                             | reliable-delivery                                                | 信頼性のある配信設定                                   |
|                                             | bounded-deque-based                                              | 制限付きデックベースのメールボックス                   |
| 23. akka.extensions                         | enabled                                                          | 有効な拡張機能                                         |
|                                             | serialize-messages                                               | メッセージのシリアル化                                 |
|                                             | extension-id-mapping                                             | 拡張機能 ID マッピング                                 |
|                                             | extension-class-mapping                                          | 拡張機能クラスマッピング                               |
|                                             | auto-extension-loading                                           | 自動拡張機能ロード                                     |
|                                             | extension-init-timeout                                           | 拡張機能初期化タイムアウト                             |
|                                             | extension-load-timeout                                           | 拡張機能ロードタイムアウト                             |
|                                             | extension-create-timeout                                         | 拡張機能作成タイムアウト                               |
|                                             | extension-lookup-timeout                                         | 拡張機能ルックアップタイムアウト                       |
|                                             | extension-lifecycle                                              | 拡張機能ライフサイクル                                 |
|                                             | extension-dispatcher                                             | 拡張機能ディスパッチャー                               |
|                                             | extension-mailbox                                                | 拡張機能メールボックス                                 |
|                                             | extension-guardian                                               | 拡張機能ガーディアン                                   |
|                                             | extension-supervisor-strategy                                    | 拡張機能監視戦略                                       |
|                                             | extension-router                                                 | 拡張機能ルーター                                       |
|                                             | extension-deployment                                             | 拡張機能デプロイメント                                 |
|                                             | extension-configuration                                          | 拡張機能設定                                           |
|                                             | extension-serialization                                          | 拡張機能シリアル化                                     |
|                                             | extension-remoting                                               | 拡張機能リモーティング                                 |
|                                             | extension-cluster                                                | 拡張機能クラスター                                     |
| 24. akka.loggers                            | loggers                                                          | ロガーの設定                                           |
|                                             | logging-filter                                                   | ロギングフィルター                                     |
|                                             | stdout-loglevel                                                  | 標準出力ログレベル                                     |
|                                             | stdout-logger-class                                              | 標準出力ロガークラス                                   |
|                                             | log-dead-letters                                                 | デッドレターのログ                                     |
|                                             | log-dead-letters-during-shutdown                                 | シャットダウン中のデッドレターログ                     |
|                                             | log-config-on-start                                              | 起動時の設定ログ                                       |
|                                             | debug                                                            | デバッグログ設定                                       |
|                                             | event-handlers                                                   | イベントハンドラー                                     |
|                                             | filter-logger-name                                               | フィルターロガー名                                     |
|                                             | loggers.0                                                        | 最初のロガー設定                                       |
|                                             | loggers.1                                                        | 2 番目のロガー設定                                     |
|                                             | logging-context                                                  | ロギングコンテキスト                                   |
|                                             | logger-startup-timeout                                           | ロガー起動タイムアウト                                 |
|                                             | loglevel                                                         | ログレベル                                             |
|                                             | log-config-on-start                                              | 起動時の設定ログ                                       |
|                                             | log-dead-letters-suspend-duration                                | デッドレター中断期間のログ                             |
|                                             | jvm-exit-on-fatal-error                                          | 致命的エラー時の JVM 終了                              |
|                                             | log-marker-interval                                              | ログマーカー間隔                                       |
| 25. akka.test                               | timefactor                                                       | 時間係数                                               |
|                                             | filter-leeway                                                    | フィルターの余裕                                       |
|                                             | single-expect-default                                            | 単一期待のデフォルト                                   |
|                                             | default-timeout                                                  | デフォルトタイムアウト                                 |
|                                             | calling-thread-dispatcher                                        | 呼び出しスレッドディスパッチャー                       |
|                                             | test-actor-ref-mode                                              | テストアクター参照モード                               |
|                                             | test-event-listener                                              | テストイベントリスナー                                 |
|                                             | filter-durations                                                 | フィルター期間                                         |
|                                             | expect-no-message-default                                        | メッセージなしを期待するデフォルト                     |
|                                             | test-conductor                                                   | テストコンダクター設定                                 |
|                                             | test-conductor.barrier-timeout                                   | バリアタイムアウト                                     |
|                                             | test-conductor.tick-duration                                     | ティック期間                                           |
|                                             | test-conductor.travel-tick-adjustment                            | 移動ティック調整                                       |
|                                             | test-conductor.expected-response-after                           | 期待される応答後                                       |
|                                             | test-conductor.timeout-warning-after                             | タイムアウト警告後                                     |
|                                             | test-conductor.terminate-system-after                            | システム終了後                                         |
|                                             | test-conductor.fail-on-unexpected-messages                       | 予期しないメッセージでの失敗                           |
|                                             | test-conductor.debug                                             | テストコンダクターデバッグ                             |
|                                             | test-actor-systems                                               | テストアクターシステム設定                             |
| 26. akka.routing                            | router                                                           | ルーターの設定                                         |
|                                             | from-code                                                        | コードからのルーター設定                               |
|                                             | round-robin-pool                                                 | ラウンドロビンプール                                   |
|                                             | round-robin-group                                                | ラウンドロビングループ                                 |
|                                             | random-pool                                                      | ランダムプール                                         |
|                                             | random-group                                                     | ランダムグループ                                       |
|                                             | balancing-pool                                                   | バランシングプール                                     |
|                                             | smallest-mailbox-pool                                            | 最小メールボックスプール                               |
|                                             | broadcast-pool                                                   | ブロードキャストプール                                 |
|                                             | broadcast-group                                                  | ブロードキャストグループ                               |
|                                             | scatter-gather-pool                                              | スキャッターギャザープール                             |
|                                             | scatter-gather-group                                             | スキャッターギャザーグループ                           |
|                                             | tail-chopping-pool                                               | テールチョッピングプール                               |
|                                             | tail-chopping-group                                              | テールチョッピンググループ                             |
|                                             | consistent-hashing-pool                                          | 一貫性ハッシュプール                                   |
|                                             | consistent-hashing-group                                         | 一貫性ハッシュグループ                                 |
|                                             | resizer                                                          | リサイザー設定                                         |
|                                             | optimal-size-exploring-resizer                                   | 最適サイズ探索リサイザー                               |
|                                             | use-dispatcher                                                   | 使用するディスパッチャー                               |
|                                             | virtual-nodes-factor                                             | 仮想ノードファクター                                   |
| 27. akka.coordinated-shutdown.phases        | service-requests-done                                            | サービスリクエスト完了フェーズ                         |
|                                             | service-unbind                                                   | サービスアンバインドフェーズ                           |
|                                             | service-stop                                                     | サービス停止フェーズ                                   |
|                                             | before-cluster-shutdown                                          | クラスターシャットダウン前フェーズ                     |
|                                             | cluster-sharding-shutdown-region                                 | クラスターシャーディング領域シャットダウンフェーズ     |
|                                             | cluster-leave                                                    | クラスター離脱フェーズ                                 |
|                                             | cluster-exiting                                                  | クラスター退出中フェーズ                               |
|                                             | cluster-exiting-done                                             | クラスター退出完了フェーズ                             |
|                                             | cluster-shutdown                                                 | クラスターシャットダウンフェーズ                       |
|                                             | before-actor-system-terminate                                    | アクターシステム終了前フェーズ                         |
|                                             | actor-system-terminate                                           | アクターシステム終了フェーズ                           |
|                                             | phase-timeout                                                    | 各フェーズのタイムアウト                               |
|                                             | timeout                                                          | 全体のタイムアウト                                     |
|                                             | recover                                                          | リカバリー設定                                         |
|                                             | recovery-strategy                                                | リカバリー戦略                                         |
|                                             | reason-overrides                                                 | 理由に基づくオーバーライド                             |
|                                             | exit-jvm                                                         | JVM 終了の有効化                                       |
|                                             | abort-timeout                                                    | 中断タイムアウト                                       |
|                                             | terminate-actor-system                                           | アクターシステム終了設定                               |
|                                             | terminate-actor-system-timeout                                   | アクターシステム終了タイムアウト                       |
| 28. akka.cluster.downing-provider-class     | split-brain-resolver                                             | スプリットブレイン解決プロバイダー                     |
|                                             | keep-majority                                                    | 多数派維持設定                                         |
|                                             | static-quorum                                                    | 静的クォーラム設定                                     |
|                                             | keep-oldest                                                      | 最古ノード維持設定                                     |
|                                             | down-all-when-unstable                                           | 不安定時の全ノードダウン設定                           |
|                                             | stable-after                                                     | 安定化時間                                             |
|                                             | down-removal-margin                                              | ダウン削除マージン                                     |
|                                             | minority-aware-down-all                                          | 少数派認識全ノードダウン                               |
|                                             | shutdown-actor-system-on-resolution                              | 解決時のアクターシステムシャットダウン                 |
|                                             | coordination-timeout                                             | 調整タイムアウト                                       |
|                                             | lease-majority                                                   | リース多数派設定                                       |
|                                             | lease-implementation                                             | リース実装                                             |
|                                             | down-all-failures                                                | 全障害時のダウン                                       |
|                                             | stable-after                                                     | 安定化時間                                             |
|                                             | down-if-alone                                                    | 単独時のダウン                                         |
|                                             | coordinate-all-nodes                                             | 全ノードの調整                                         |
|                                             | cleanup-unstable-nodes                                           | 不安定ノードのクリーンアップ                           |
|                                             | auto-down-unreachable-after                                      | 到達不能ノードの自動ダウン時間                         |
| 29. akka.cluster.client                     | initial-contacts                                                 | 初期コンタクト                                         |
|                                             | establishing-get-contacts-interval                               | コンタクト取得間隔の確立                               |
|                                             | refresh-contacts-interval                                        | コンタクト更新間隔                                     |
|                                             | heartbeat-interval                                               | ハートビート間隔                                       |
|                                             | acceptable-heartbeat-pause                                       | 許容可能なハートビート停止時間                         |
|                                             | buffer-size                                                      | バッファサイズ                                         |
|                                             | reconnect-timeout                                                | 再接続タイムアウト                                     |
|                                             | receptionist                                                     | レセプショニスト設定                                   |
|                                             | receptionist.name                                                | レセプショニスト名                                     |
|                                             | receptionist.role                                                | レセプショニストのロール                               |
|                                             | receptionist.number-of-contacts                                  | コンタクト数                                           |
|                                             | use-dispatcher                                                   | 使用するディスパッチャー                               |
|                                             | gossip-interval                                                  | ゴシップ間隔                                           |
|                                             | warning-for-entered-quarantine                                   | 隔離エントリーの警告                                   |
|                                             | attempt-quarantine-first                                         | 最初の隔離試行                                         |
|                                             | max-failures-for-quarantine                                      | 隔離のための最大失敗数                                 |
|                                             | quarantine-duration                                              | 隔離期間                                               |
|                                             | quarantine-after                                                 | 隔離後の時間                                           |
| 30. akka.cluster.bootstrap                  | contact-point-discovery                                          | コンタクトポイント発見設定                             |
|                                             | contact-point                                                    | コンタクトポイント設定                                 |
|                                             | new-cluster-enabled                                              | 新しいクラスターの有効化                               |
|                                             | min-members                                                      | 最小メンバー数                                         |
|                                             | contact-point-discovery.service-name                             | サービス名                                             |
|                                             | contact-point-discovery.discovery-method                         | 発見方法                                               |
|                                             | contact-point-discovery.required-contact-point-nr                | 必要なコンタクトポイント数                             |
|                                             | contact-point-discovery.interval                                 | 発見間隔                                               |
|                                             | contact-point-discovery.exponential-backoff-random-factor        | 指数バックオフランダムファクター                       |
|                                             | contact-point-discovery.exponential-backoff-max                  | 最大指数バックオフ                                     |
|                                             | contact-point-discovery.config-contact-points                    | 設定コンタクトポイント                                 |
|                                             | contact-point.fallback-port                                      | フォールバックポート                                   |
|                                             | contact-point.probing-failure-timeout                            | プロービング失敗タイムアウト                           |
|                                             | contact-point.probe-interval                                     | プローブ間隔                                           |
|                                             | contact-point.probe-request-timeout                              | プローブリクエストタイムアウト                         |
|                                             | join-timeout                                                     | 参加タイムアウト                                       |
|                                             | abort-timeout                                                    | 中断タイムアウト                                       |
|                                             | stable-margin                                                    | 安定マージン                                           |
|                                             | discovery-attempt-interval                                       | 発見試行間隔                                           |
|                                             | manual-cleanup                                                   | 手動クリーンアップ                                     |
| 31. akka.persistence.journal                | plugin                                                           | ジャーナルプラグイン設定                               |
|                                             | auto-start-journals                                              | 自動起動ジャーナル                                     |
|                                             | circuit-breaker                                                  | サーキットブレーカー設定                               |
|                                             | max-message-batch-size                                           | 最大メッセージバッチサイズ                             |
|                                             | replay-filter                                                    | リプレイフィルター設定                                 |
|                                             | publish-plugin-commands                                          | プラグインコマンドの公開                               |
|                                             | publish-confirmation-timeout                                     | 確認公開のタイムアウト                                 |
|                                             | recovery-event-timeout                                           | リカバリーイベントタイムアウト                         |
|                                             | circuit-breaker.max-failures                                     | 最大失敗回数                                           |
|                                             | circuit-breaker.call-timeout                                     | 呼び出しタイムアウト                                   |
|                                             | circuit-breaker.reset-timeout                                    | リセットタイムアウト                                   |
|                                             | replay-filter.mode                                               | リプレイフィルターモード                               |
|                                             | replay-filter.window-size                                        | ウィンドウサイズ                                       |
|                                             | replay-filter.max-old-writers                                    | 最大古いライター数                                     |
|                                             | leveldb                                                          | LevelDB ジャーナル設定                                 |
|                                             | inmem                                                            | インメモリジャーナル設定                               |
|                                             | proxy                                                            | プロキシジャーナル設定                                 |
|                                             | event-adapter-bindings                                           | イベントアダプターバインディング                       |
|                                             | serialization-identifier-migration                               | シリアル化識別子マイグレーション                       |
| 32. akka.persistence.snapshot-store         | plugin                                                           | スナップショットストアプラグイン設定                   |
|                                             | auto-start-snapshot-stores                                       | 自動起動スナップショットストア                         |
|                                             | circuit-breaker                                                  | サーキットブレーカー設定                               |
|                                             | max-load-attempts                                                | 最大ロード試行回数                                     |
|                                             | local                                                            | ローカルスナップショットストア設定                     |
|                                             | proxy                                                            | プロキシスナップショットストア設定                     |
|                                             | no-snapshot-store                                                | スナップショットストアなし設定                         |
|                                             | circuit-breaker.max-failures                                     | 最大失敗回数                                           |
|                                             | circuit-breaker.call-timeout                                     | 呼び出しタイムアウト                                   |
|                                             | circuit-breaker.reset-timeout                                    | リセットタイムアウト                                   |
|                                             | local.dir                                                        | ローカルディレクトリ                                   |
|                                             | local.max-load-attempts                                          | 最大ロード試行回数                                     |
|                                             | snapshot-is-optional                                             | スナップショットのオプション化                         |
|                                             | snapshot-after                                                   | スナップショット作成条件                               |
|                                             | serialization-identifier-migration                               | シリアル化識別子マイグレーション                       |
|                                             | use-dispatcher                                                   | 使用するディスパッチャー                               |
|                                             | plugin-fallback-autocreate                                       | プラグインフォールバック自動作成                       |
|                                             | leveldb                                                          | LevelDB スナップショットストア設定                     |
|                                             | inmem                                                            | インメモリスナップショットストア設定                   |
| 33. akka.persistence.query                  | journal.plugin                                                   | ジャーナルプラグイン設定                               |
|                                             | max-buffer-size                                                  | 最大バッファサイズ                                     |
|                                             | refresh-interval                                                 | リフレッシュ間隔                                       |
|                                             | read-journal                                                     | 読み取りジャーナル設定                                 |
|                                             | write-journal                                                    | 書き込みジャーナル設定                                 |
|                                             | journal.leveldb                                                  | LevelDB 読み取りジャーナル設定                         |
|                                             | journal.inmem                                                    | インメモリ読み取りジャーナル設定                       |
|                                             | ask-timeout                                                      | 問い合わせタイムアウト                                 |
|                                             | max-buffered-orders                                              | 最大バッファ注文数                                     |
|                                             | replay-parallelism                                               | リプレイ並列度                                         |
|                                             | sequence-nr-gap                                                  | シーケンス番号ギャップ                                 |
|                                             | gap-free-sequence-nr                                             | ギャップフリーシーケンス番号                           |
|                                             | eventual-consistency-delay                                       | 最終的一貫性遅延                                       |
|                                             | delayed-event-timeout                                            | 遅延イベントタイムアウト                               |
|                                             | pubsub-minimum-interval                                          | PubSub の最小間隔                                      |
|                                             | timestamp-query                                                  | タイムスタンプクエリ設定                               |
|                                             | journal-sequence-retrieval                                       | ジャーナルシーケンス取得設定                           |
|                                             | max-concurrent-replays                                           | 最大同時リプレイ数                                     |
| 34. akka.persistence.typed                  | recovery-timeout                                                 | リカバリータイムアウト                                 |
|                                             | internal-stash-overflow-strategy                                 | 内部スタッシュオーバーフロー戦略                       |
|                                             | running-event-adapters                                           | 実行中のイベントアダプター                             |
|                                             | recovery                                                         | リカバリー設定                                         |
|                                             | snapshot-adapter                                                 | スナップショットアダプター                             |
|                                             | recovery-permit-timeout                                          | リカバリー許可タイムアウト                             |
|                                             | replay-filter                                                    | リプレイフィルター設定                                 |
|                                             | stash-capacity                                                   | スタッシュ容量                                         |
|                                             | stash-overflow-strategy                                          | スタッシュオーバーフロー戦略                           |
|                                             | recovery.disable-load-timers                                     | ロードタイマーの無効化                                 |
|                                             | recovery.max-number-of-events                                    | 最大イベント数                                         |
|                                             | recovery.recovery-event-timeout                                  | リカバリーイベントタイムアウト                         |
|                                             | replay-filter.mode                                               | リプレイフィルターモード                               |
|                                             | replay-filter.window-size                                        | ウィンドウサイズ                                       |
|                                             | replay-filter.max-old-writers                                    | 最大古いライター数                                     |
|                                             | passivation-strategy                                             | パッシベーション戦略                                   |
|                                             | default-snapshot-store                                           | デフォルトスナップショットストア                       |
|                                             | state-change-timeout                                             | 状態変更タイムアウト                                   |
|                                             | command-handler-warning-timeout                                  | コマンドハンドラー警告タイムアウト                     |
| 35. akka.remote.classic                     | enabled-transports                                               | 有効なトランスポート                                   |
|                                             | netty.tcp                                                        | Netty TCP 設定                                         |
|                                             | netty.udp                                                        | Netty UDP 設定                                         |
|                                             | netty.ssl                                                        | Netty SSL 設定                                         |
|                                             | netty.tcp.hostname                                               | TCP ホスト名                                           |
|                                             | netty.tcp.port                                                   | TCP ポート                                             |
|                                             | netty.tcp.bind-hostname                                          | TCP バインドホスト名                                   |
|                                             | netty.tcp.bind-port                                              | TCP バインドポート                                     |
|                                             | netty.tcp.connection-timeout                                     | TCP 接続タイムアウト                                   |
|                                             | netty.tcp.outbound-buffer-size                                   | TCP 送信バッファサイズ                                 |
|                                             | netty.tcp.send-buffer-size                                       | TCP 送信バッファサイズ                                 |
|                                             | netty.tcp.receive-buffer-size                                    | TCP 受信バッファサイズ                                 |
|                                             | netty.tcp.maximum-frame-size                                     | TCP 最大フレームサイズ                                 |
|                                             | netty.tcp.backlog                                                | TCP バックログ                                         |
|                                             | netty.ssl.security                                               | SSL セキュリティ設定                                   |
|                                             | netty.ssl.ssl-engine-provider                                    | SSL エンジンプロバイダー                               |
|                                             | netty.ssl.key-store                                              | SSL キーストア設定                                     |
|                                             | netty.ssl.trust-store                                            | SSL トラストストア設定                                 |
|                                             | netty.ssl.protocol                                               | SSL プロトコル                                         |
|                                             | transport-failure-detector                                       | トランスポート失敗検出器設定                           |
| 36. akka.stream.materializer                | initial-input-buffer-size                                        | 初期入力バッファサイズ                                 |
|                                             | max-input-buffer-size                                            | 最大入力バッファサイズ                                 |
|                                             | dispatcher                                                       | 使用するディスパッチャー                               |
|                                             | subscription-timeout                                             | サブスクリプションタイムアウト                         |
|                                             | debugging                                                        | デバッギング設定                                       |
|                                             | output-burst-limit                                               | 出力バーストリミット                                   |
|                                             | auto-fusing                                                      | 自動フュージング                                       |
|                                             | max-fixed-buffer-size                                            | 最大固定バッファサイズ                                 |
|                                             | sync-processing-limit                                            | 同期処理制限                                           |
|                                             | debug-logging                                                    | デバッグロギング                                       |
|                                             | stream-ref                                                       | ストリーム参照設定                                     |
|                                             | blocking-io-dispatcher                                           | ブロッキング I/O ディスパッチャー                      |
|                                             | max-active-streams                                               | 最大アクティブストリーム数                             |
|                                             | stream-materialization-timeout                                   | ストリームマテリアライゼーションタイムアウト           |
|                                             | unwrap-single-element-termination                                | 単一要素終了のアンラップ                               |
|                                             | buffer-pool                                                      | バッファプール設定                                     |
|                                             | io-parallelism                                                   | I/O 並列度                                             |
|                                             | stream-ref-buffer-capacity                                       | ストリーム参照バッファ容量                             |
|                                             | phase-timeout-multiplier                                         | フェーズタイムアウト乗数                               |
|                                             | early-stop-mark-peeked                                           | 早期停止マークピーク                                   |
| 37. akka.stream.secret-key                  | provider                                                         | 秘密鍵プロバイダー                                     |
|                                             | algorithm                                                        | 暗号化アルゴリズム                                     |
|                                             | key-size                                                         | 鍵サイズ                                               |
|                                             | rotation-interval                                                | 鍵ローテーション間隔                                   |
|                                             | rotation-checks                                                  | ローテーションチェック間隔                             |
|                                             | old-keys-retention-period                                        | 古い鍵の保持期間                                       |
|                                             | encryption-random                                                | 暗号化用乱数生成器                                     |
|                                             | decryption-parallelism                                           | 復号並列度                                             |
|                                             | key-rotation-check-interval                                      | 鍵ローテーションチェック間隔                           |
|                                             | key-ids-in-use                                                   | 使用中の鍵 ID                                          |
|                                             | key-provider-settings                                            | 鍵プロバイダー設定                                     |
|                                             | key-rotation-validation                                          | 鍵ローテーション検証                                   |
|                                             | key-validation-interval                                          | 鍵検証間隔                                             |
|                                             | max-key-length                                                   | 最大鍵長                                               |
|                                             | min-key-length                                                   | 最小鍵長                                               |
|                                             | key-derivation-algorithm                                         | 鍵導出アルゴリズム                                     |
|                                             | key-derivation-iterations                                        | 鍵導出反復回数                                         |
|                                             | salt-size                                                        | ソルトサイズ                                           |
|                                             | iv-size                                                          | 初期化ベクトルサイズ                                   |
|                                             | mac-algorithm                                                    | MAC アルゴリズム                                       |
| 38. akka.stream.alpakka                     | file                                                             | ファイル関連設定                                       |
|                                             | ftp                                                              | FTP 関連設定                                           |
|                                             | s3                                                               | S3 関連設定                                            |
|                                             | slick                                                            | Slick 関連設定                                         |
|                                             | mqtt                                                             | MQTT 関連設定                                          |
|                                             | dynamodb                                                         | DynamoDB 関連設定                                      |
|                                             | cassandra                                                        | Cassandra 関連設定                                     |
|                                             | elasticsearch                                                    | Elasticsearch 関連設定                                 |
|                                             | mongodb                                                          | MongoDB 関連設定                                       |
|                                             | google-cloud-pub-sub                                             | Google Cloud Pub/Sub 関連設定                          |
|                                             | jms                                                              | JMS 関連設定                                           |
|                                             | kinesis                                                          | Kinesis 関連設定                                       |
|                                             | couchbase                                                        | Couchbase 関連設定                                     |
|                                             | sns                                                              | SNS 関連設定                                           |
|                                             | sqs                                                              | SQS 関連設定                                           |
|                                             | csv                                                              | CSV 関連設定                                           |
|                                             | xml                                                              | XML 関連設定                                           |
|                                             | udp                                                              | UDP 関連設定                                           |
|                                             | unix-domain-socket                                               | Unix ドメインソケット関連設定                          |
|                                             | avroparquet                                                      | Avro/Parquet 関連設定                                  |
| 39. akka.stream.testkit                     | test-timefactor                                                  | テスト時間係数                                         |
|                                             | filter-leeway                                                    | フィルターの余裕                                       |
|                                             | default-timeout                                                  | デフォルトタイムアウト                                 |
|                                             | stream-materialization-timeout                                   | ストリームマテリアライゼーションタイムアウト           |
|                                             | throw-on-early-termination                                       | 早期終了時の例外スロー                                 |
|                                             | debug-logging                                                    | デバッグロギング                                       |
|                                             | materializer                                                     | テスト用マテリアライザー設定                           |
|                                             | subscription-timeout                                             | サブスクリプションタイムアウト                         |
|                                             | tick-duration                                                    | ティック間隔                                           |
|                                             | default-test-sink-buffer-size                                    | デフォルトテストシンクバッファサイズ                   |
|                                             | default-test-source-buffer-size                                  | デフォルトテストソースバッファサイズ                   |
|                                             | stream-test-timeout                                              | ストリームテストタイムアウト                           |
|                                             | expect-no-message-default                                        | メッセージ非受信期待のデフォルト時間                   |
|                                             | single-expect-default                                            | 単一期待のデフォルト時間                               |
|                                             | test-sink-probe-settings                                         | テストシンクプローブ設定                               |
|                                             | test-source-probe-settings                                       | テストソースプローブ設定                               |
|                                             | test-publisher-settings                                          | テストパブリッシャー設定                               |
|                                             | test-subscriber-settings                                         | テストサブスクライバー設定                             |
|                                             | stream-test-timeout-factor                                       | ストリームテストタイムアウト係数                       |
|                                             | within-factor                                                    | 範囲内係数                                             |
| 40. akka.stream.kafka                       | consumer                                                         | Kafka コンシューマー設定                               |
|                                             | producer                                                         | Kafka プロデューサー設定                               |
|                                             | committer                                                        | コミッター設定                                         |
|                                             | connection-checker                                               | 接続チェッカー設定                                     |
|                                             | kafka-clients                                                    | Kafka クライアント設定                                 |
|                                             | consumer.poll-interval                                           | ポーリング間隔                                         |
|                                             | consumer.poll-timeout                                            | ポーリングタイムアウト                                 |
|                                             | consumer.stop-timeout                                            | 停止タイムアウト                                       |
|                                             | consumer.close-timeout                                           | クローズタイムアウト                                   |
|                                             | consumer.commit-timeout                                          | コミットタイムアウト                                   |
|                                             | consumer.wakeup-timeout                                          | ウェイクアップタイムアウト                             |
|                                             | consumer.max-wakeups                                             | 最大ウェイクアップ回数                                 |
|                                             | consumer.use-dispatcher                                          | 使用するディスパッチャー                               |
|                                             | consumer.wait-close-partition                                    | パーティションクローズ待機                             |
|                                             | consumer.position-timeout                                        | ポジション取得タイムアウト                             |
|                                             | consumer.offset-for-times-timeout                                | オフセット取得タイムアウト                             |
|                                             | consumer.metadata-request-timeout                                | メタデータリクエストタイムアウト                       |
|                                             | consumer.eos-draining-check-interval                             | EOS ドレイニングチェック間隔                           |
|                                             | consumer.partition-handler-warning                               | パーティションハンドラー警告                           |
|                                             | consumer.commit-warnings                                         | コミット警告                                           |
| 41. akka.http.host-connection-pool          | max-connections                                                  | 最大接続数                                             |
|                                             | min-connections                                                  | 最小接続数                                             |
|                                             | max-retries                                                      | 最大リトライ回数                                       |
|                                             | max-open-requests                                                | 最大オープンリクエスト数                               |
|                                             | pipelining-limit                                                 | パイプライニング制限                                   |
|                                             | idle-timeout                                                     | アイドルタイムアウト                                   |
|                                             | connection-lifetime                                              | 接続ライフタイム                                       |
|                                             | keep-alive-timeout                                               | キープアライブタイムアウト                             |
|                                             | base-connection-backoff                                          | 基本接続バックオフ                                     |
|                                             | max-connection-backoff                                           | 最大接続バックオフ                                     |
|                                             | idle-connection-test-period                                      | アイドル接続テスト期間                                 |
|                                             | max-connection-lifetime                                          | 最大接続ライフタイム                                   |
|                                             | client                                                           | クライアント設定                                       |
|                                             | pool-implementation                                              | プール実装                                             |
|                                             | response-entity-subscription-timeout                             | レスポンスエンティティサブスクリプションタイムアウト   |
|                                             | max-connection-lifetime-jitter                                   | 最大接続ライフタイムジッター                           |
|                                             | max-retries-per-request                                          | リクエストごとの最大リトライ回数                       |
|                                             | proxy                                                            | プロキシ設定                                           |
|                                             | decompression                                                    | 圧縮解除設定                                           |
| 42. akka.http.server                        | server-header                                                    | サーバーヘッダー                                       |
|                                             | preview                                                          | プレビュー設定                                         |
|                                             | idle-timeout                                                     | アイドルタイムアウト                                   |
|                                             | request-timeout                                                  | リクエストタイムアウト                                 |
|                                             | bind-timeout                                                     | バインドタイムアウト                                   |
|                                             | linger-timeout                                                   | リンガータイムアウト                                   |
|                                             | max-connections                                                  | 最大接続数                                             |
|                                             | pipelining-limit                                                 | パイプライニング制限                                   |
|                                             | remote-address-header                                            | リモートアドレスヘッダー                               |
|                                             | raw-request-uri-header                                           | 生のリクエスト URI ヘッダー                            |
|                                             | transparent-head-requests                                        | 透過的 HEAD リクエスト                                 |
|                                             | verbose-error-messages                                           | 詳細なエラーメッセージ                                 |
|                                             | response-header-size-hint                                        | レスポンスヘッダーサイズヒント                         |
|                                             | max-content-length                                               | 最大コンテンツ長                                       |
|                                             | parsing                                                          | パース設定                                             |
|                                             | timeouts                                                         | タイムアウト設定                                       |
|                                             | websocket                                                        | WebSocket 設定                                         |
|                                             | http2                                                            | HTTP/2 設定                                            |
|                                             | termination-deadline-exceeded-response                           | 終了期限超過レスポンス                                 |
|                                             | log-unencrypted-network-bytes                                    | 暗号化されていないネットワークバイトのログ             |
| 43. akka.http.client                        | user-agent-header                                                | ユーザーエージェントヘッダー                           |
|                                             | connecting-timeout                                               | 接続タイムアウト                                       |
|                                             | idle-timeout                                                     | アイドルタイムアウト                                   |
|                                             | request-header-size-hint                                         | リクエストヘッダーサイズヒント                         |
|                                             | socket-options                                                   | ソケットオプション                                     |
|                                             | proxy                                                            | プロキシ設定                                           |
|                                             | websocket                                                        | WebSocket 設定                                         |
|                                             | parsing                                                          | パース設定                                             |
|                                             | log-unencrypted-network-bytes                                    | 暗号化されていないネットワークバイトのログ             |
|                                             | max-redirects                                                    | 最大リダイレクト数                                     |
|                                             | max-retries                                                      | 最大リトライ回数                                       |
|                                             | max-uri-length                                                   | 最大 URI 長                                            |
|                                             | max-response-reason-length                                       | 最大レスポンス理由長                                   |
|                                             | max-header-count                                                 | 最大ヘッダー数                                         |
|                                             | max-header-value-length                                          | 最大ヘッダー値長                                       |
|                                             | max-content-length                                               | 最大コンテンツ長                                       |
|                                             | response-chunk-aggregation-limit                                 | レスポンスチャンク集約制限                             |
|                                             | server-header                                                    | サーバーヘッダー                                       |
|                                             | ssl-session-establishment-timeout                                | SSL セッション確立タイムアウト                         |
| 44. akka.http.routing                       | verbose-error-messages                                           | 詳細なエラーメッセージ                                 |
|                                             | file-get-conditional                                             | 条件付きファイル取得                                   |
|                                             | render-vanity-footer                                             | フッター表示                                           |
|                                             | range-coalescing-threshold                                       | 範囲結合しきい値                                       |
|                                             | range-count-limit                                                | 範囲カウント制限                                       |
|                                             | decode-max-bytes-per-chunk                                       | チャンクごとの最大デコードバイト数                     |
|                                             | file-io-dispatcher                                               | ファイル I/O ディスパッチャー                          |
|                                             | max-content-length                                               | 最大コンテンツ長                                       |
|                                             | max-content-length-setting                                       | 最大コンテンツ長設定                                   |
|                                             | uri-parsing-mode                                                 | URI パースモード                                       |
|                                             | uri-parsing-mode-setting                                         | URI パースモード設定                                   |
|                                             | default-host-header                                              | デフォルトホストヘッダー                               |
|                                             | default-host-header-setting                                      | デフォルトホストヘッダー設定                           |
|                                             | render-status-code-as-int                                        | ステータスコードを整数として表示                       |
|                                             | render-status-code-as-int-setting                                | ステータスコードを整数として表示設定                   |
|                                             | file-get-conditional-setting                                     | 条件付きファイル取得設定                               |
|                                             | render-vanity-footer-setting                                     | フッター表示設定                                       |
|                                             | range-coalescing-threshold-setting                               | 範囲結合しきい値設定                                   |
|                                             | range-count-limit-setting                                        | 範囲カウント制限設定                                   |
|                                             | decode-max-bytes-per-chunk-setting                               | チャンクごとの最大デコードバイト数設定                 |
| 45. akka.http.parsing                       | max-uri-length                                                   | 最大 URI 長                                            |
|                                             | max-method-length                                                | 最大メソッド長                                         |
|                                             | max-response-reason-length                                       | 最大レスポンス理由長                                   |
|                                             | max-header-name-length                                           | 最大ヘッダー名長                                       |
|                                             | max-header-value-length                                          | 最大ヘッダー値長                                       |
|                                             | max-header-count                                                 | 最大ヘッダー数                                         |
|                                             | max-content-length                                               | 最大コンテンツ長                                       |
|                                             | max-chunk-ext-length                                             | 最大チャンク拡張長                                     |
|                                             | max-chunk-size                                                   | 最大チャンクサイズ                                     |
|                                             | uri-parsing-mode                                                 | URI パースモード                                       |
|                                             | cookie-parsing-mode                                              | クッキーパースモード                                   |
|                                             | illegal-header-warnings                                          | 不正ヘッダー警告                                       |
|                                             | error-logging-verbosity                                          | エラーログ詳細度                                       |
|                                             | illegal-response-header-name-processing                          | 不正レスポンスヘッダー名処理                           |
|                                             | ignore-illegal-header-for                                        | 特定ヘッダーの不正無視                                 |
|                                             | headers-with-underscore-parsing-mode                             | アンダースコア付きヘッダーパースモード                 |
|                                             | header-cache                                                     | ヘッダーキャッシュ                                     |
|                                             | header-value-cache-limit                                         | ヘッダー値キャッシュ制限                               |
|                                             | modeled-header-parsing                                           | モデル化されたヘッダーパース                           |
|                                             | tls-session-info-header                                          | TLS セッション情報ヘッダー                             |
|                                             | user-agent-header                                                | ユーザーエージェントヘッダー                           |
|                                             | server-header                                                    | サーバーヘッダー                                       |
|                                             | cookie-header-parsing                                            | クッキーヘッダーパース                                 |
| 46. akka.http.websocket                     | periodic-keep-alive-mode                                         | 定期的キープアライブモード                             |
|                                             | periodic-keep-alive-max-idle                                     | 最大アイドル時間                                       |
|                                             | default-masking                                                  | デフォルトマスキング                                   |
|                                             | random-factory                                                   | ランダムファクトリー                                   |
|                                             | log-frames                                                       | フレームログ                                           |
|                                             | payload-size-threshold                                           | ペイロードサイズしきい値                               |
|                                             | max-frame-payload-length                                         | 最大フレームペイロード長                               |
|                                             | frame-compression                                                | フレーム圧縮                                           |
|                                             | frame-compression-mode                                           | フレーム圧縮モード                                     |
|                                             | message-compression                                              | メッセージ圧縮                                         |
|                                             | message-compression-mode                                         | メッセージ圧縮モード                                   |
|                                             | send-buffer                                                      | 送信バッファ                                           |
|                                             | receive-buffer                                                   | 受信バッファ                                           |
|                                             | receive-frame-logger                                             | 受信フレームロガー                                     |
|                                             | send-frame-logger                                                | 送信フレームロガー                                     |
|                                             | log-configuration                                                | ログ設定                                               |
|                                             | close-timeout                                                    | クローズタイムアウト                                   |
|                                             | auto-ping-pong                                                   | 自動 Ping-Pong                                         |
|                                             | ping-interval                                                    | Ping 間隔                                              |
|                                             | ping-timeout                                                     | Ping タイムアウト                                      |
| 47. akka.http.cors                          | allowed-origins                                                  | 許可されたオリジン                                     |
|                                             | allowed-methods                                                  | 許可されたメソッド                                     |
|                                             | allowed-headers                                                  | 許可されたヘッダー                                     |
|                                             | exposed-headers                                                  | 公開ヘッダー                                           |
|                                             | allow-credentials                                                | 認証情報の許可                                         |
|                                             | max-age                                                          | 最大有効期間                                           |
|                                             | allow-generic-http-requests                                      | 一般的な HTTP リクエストの許可                         |
|                                             | allow-subdomains                                                 | サブドメインの許可                                     |
|                                             | short-circuit-forbidden                                          | 禁止の短絡                                             |
|                                             | log-level                                                        | ログレベル                                             |
|                                             | log-rejections                                                   | 拒否のログ                                             |
|                                             | cors-settings                                                    | CORS 設定                                              |
|                                             | cors-filter                                                      | CORS フィルター                                        |
|                                             | cors-support                                                     | CORS サポート                                          |
|                                             | cors-handler                                                     | CORS ハンドラー                                        |
|                                             | cors-directive                                                   | CORS ディレクティブ                                    |
|                                             | cors-allow-origin-header                                         | CORS アロウオリジンヘッダー                            |
|                                             | cors-allow-methods-header                                        | CORS アロウメソッドヘッダー                            |
|                                             | cors-allow-headers-header                                        | CORS アロウヘッダーヘッダー                            |
| 48. akka.http.caching                       | lfu-cache                                                        | LFU キャッシュ設定                                     |
|                                             | lfuCache                                                         | LFU キャッシュ設定（別名）                             |
|                                             | lru-cache                                                        | LRU キャッシュ設定                                     |
|                                             | lruCache                                                         | LRU キャッシュ設定（別名）                             |
|                                             | cache-config                                                     | キャッシュ設定                                         |
|                                             | cache-key-generator                                              | キャッシュキー生成器                                   |
|                                             | keyer                                                            | キーヤー                                               |
|                                             | always-cache                                                     | 常にキャッシュ                                         |
|                                             | never-cache                                                      | 決してキャッシュしない                                 |
|                                             | cache                                                            | キャッシュ                                             |
|                                             | cache-or-pass                                                    | キャッシュまたはパス                                   |
|                                             | pass-through                                                     | パススルー                                             |
|                                             | expire-after-access                                              | アクセス後の有効期限                                   |
|                                             | expire-after-write                                               | 書き込み後の有効期限                                   |
|                                             | max-capacity                                                     | 最大容量                                               |
|                                             | initial-capacity                                                 | 初期容量                                               |
|                                             | time-to-idle                                                     | アイドル時間                                           |
|                                             | time-to-live                                                     | 生存時間                                               |
|                                             | use-cache-for-all-cacheable                                      | キャッシュ可能な全てに使用                             |
| 49. akka.http.server.preview                | enable-http2                                                     | HTTP/2 の有効化                                        |
|                                             | http2                                                            | HTTP/2 設定                                            |
|                                             | http2.max-concurrent-streams                                     | 最大同時ストリーム数                                   |
|                                             | http2.max-header-list-size                                       | 最大ヘッダーリストサイズ                               |
|                                             | http2.max-frame-size                                             | 最大フレームサイズ                                     |
|                                             | http2.max-concurrent-streams-to-root-actor                       | ルートアクターへの最大同時ストリーム数                 |
|                                             | http2.max-header-list-size-to-root-actor                         | ルートアクターへの最大ヘッダーリストサイズ             |
|                                             | http2.max-frame-size-to-root-actor                               | ルートアクターへの最大フレームサイズ                   |
|                                             | http2.min-concurrent-streams                                     | 最小同時ストリーム数                                   |
|                                             | http2.incoming-connection-level-buffer-size                      | 受信接続レベルバッファサイズ                           |
|                                             | http2.incoming-stream-level-buffer-size                          | 受信ストリームレベルバッファサイズ                     |
|                                             | http2.outgoing-control-frame-buffer-size                         | 送信制御フレームバッファサイズ                         |
|                                             | http2.max-header-compression-table-size                          | 最大ヘッダー圧縮テーブルサイズ                         |
|                                             | http2.header-table-size                                          | ヘッダーテーブルサイズ                                 |
|                                             | http2.enable-push                                                | プッシュの有効化                                       |
|                                             | http2.ping-interval                                              | Ping 間隔                                              |
|                                             | http2.ping-timeout                                               | Ping タイムアウト                                      |
|                                             | http2.idle-timeout                                               | アイドルタイムアウト                                   |
|                                             | http2.max-concurrent-pings                                       | 最大同時 Ping 数                                       |
|                                             | http2.max-settings-per-frame                                     | フレームごとの最大設定数                               |
| 50. akka.http.client.http2                  | max-concurrent-streams                                           | 最大同時ストリーム数                                   |
|                                             | max-header-list-size                                             | 最大ヘッダーリストサイズ                               |
|                                             | max-frame-size                                                   | 最大フレームサイズ                                     |
|                                             | max-concurrent-streams-to-root-actor                             | ルートアクターへの最大同時ストリーム数                 |
|                                             | max-header-list-size-to-root-actor                               | ルートアクターへの最大ヘッダーリストサイズ             |
|                                             | max-frame-size-to-root-actor                                     | ルートアクターへの最大フレームサイズ                   |
|                                             | min-concurrent-streams                                           | 最小同時ストリーム数                                   |
|                                             | incoming-connection-level-buffer-size                            | 受信接続レベルバッファサイズ                           |
|                                             | incoming-stream-level-buffer-size                                | 受信ストリームレベルバッファサイズ                     |
|                                             | outgoing-control-frame-buffer-size                               | 送信制御フレームバッファサイズ                         |
|                                             | max-header-compression-table-size                                | 最大ヘッダー圧縮テーブルサイズ                         |
|                                             | header-table-size                                                | ヘッダーテーブルサイズ                                 |
|                                             | enable-push                                                      | プッシュの有効化                                       |
|                                             | ping-interval                                                    | Ping 間隔                                              |
|                                             | ping-timeout                                                     | Ping タイムアウト                                      |
|                                             | idle-timeout                                                     | アイドルタイムアウト                                   |
|                                             | max-concurrent-pings                                             | 最大同時 Ping 数                                       |
|                                             | max-settings-per-frame                                           | フレームごとの最大設定数                               |
|                                             | connection-settings                                              | 接続設定                                               |
|                                             | client-settings                                                  | クライアント設定                                       |
| 51. akka.cluster.sharding.passivation       | strategy                                                         | パッシベーション戦略                                   |
|                                             | idle-entity.timeout                                              | アイドルエンティティのタイムアウト                     |
|                                             | idle-entity.interval                                             | アイドルエンティティのチェック間隔                     |
|                                             | idle-entity.predicate                                            | アイドルエンティティの判定条件                         |
|                                             | active-entity.timeout                                            | アクティブエンティティのタイムアウト                   |
|                                             | active-entity.interval                                           | アクティブエンティティのチェック間隔                   |
|                                             | active-entity.predicate                                          | アクティブエンティティの判定条件                       |
|                                             | strategy-decision-handler                                        | 戦略決定ハンドラー                                     |
|                                             | passivation-decision-handler                                     | パッシベーション決定ハンドラー                         |
|                                             | rebalance-interval                                               | リバランス間隔                                         |
|                                             | least-shard-allocation-strategy                                  | 最小シャード割り当て戦略                               |
|                                             | rebalance-absolute-limit                                         | リバランスの絶対制限                                   |
|                                             | rebalance-relative-limit                                         | リバランスの相対制限                                   |
|                                             | lease-retry-interval                                             | リース再試行間隔                                       |
|                                             | state-store-mode                                                 | 状態ストアモード                                       |
|                                             | snapshot-after                                                   | スナップショット作成条件                               |
|                                             | keep-n-snapshots                                                 | 保持するスナップショット数                             |
|                                             | snapshot-every                                                   | スナップショット作成間隔                               |
|                                             | journal-plugin-id                                                | ジャーナルプラグイン ID                                |
|                                             | snapshot-plugin-id                                               | スナップショットプラグイン ID                          |
| 52. akka.cluster.metrics                    | collector                                                        | メトリクスコレクター                                   |
|                                             | collector-class                                                  | コレクタークラス                                       |
|                                             | collector-sample-interval                                        | コレクターサンプル間隔                                 |
|                                             | gossip-interval                                                  | ゴシップ間隔                                           |
|                                             | moving-average-half-life                                         | 移動平均の半減期                                       |
|                                             | metrics-gossip-interval                                          | メトリクスゴシップ間隔                                 |
|                                             | periodic-tasks-initial-delay                                     | 定期タスクの初期遅延                                   |
|                                             | sigar-native-library                                             | Sigar ネイティブライブラリパス                         |
|                                             | metrics-selector                                                 | メトリクスセレクター                                   |
|                                             | load-balancing-selector                                          | 負荷分散セレクター                                     |
|                                             | adaptive-load-balancing                                          | 適応型負荷分散設定                                     |
|                                             | standard-deviation-factor                                        | 標準偏差因子                                           |
|                                             | cpu-threshold                                                    | CPU 使用率しきい値                                     |
|                                             | heap-threshold                                                   | ヒープ使用率しきい値                                   |
|                                             | memory-threshold                                                 | メモリ使用率しきい値                                   |
|                                             | mixed-threshold                                                  | 混合使用率しきい値                                     |
|                                             | cpu-load-cpus                                                    | CPU 負荷 CPU 数                                        |
|                                             | cpu-load-average                                                 | CPU 負荷平均                                           |
|                                             | heap-memory-used                                                 | 使用ヒープメモリ                                       |
|                                             | metric-filters                                                   | メトリクスフィルター                                   |
| 53. akka.cluster.ddata                      | max-delta-elements                                               | 最大デルタ要素数                                       |
|                                             | durable-lmdb                                                     | 永続 LMDB 設定                                         |
|                                             | durable-keys                                                     | 永続キー設定                                           |
|                                             | durable-pruning                                                  | 永続プルーニング設定                                   |
|                                             | delta-crdt.enabled                                               | Delta CRDT の有効化                                    |
|                                             | delta-crdt.max-delta-size                                        | 最大デルタサイズ                                       |
|                                             | delta-crdt.propagation-interval                                  | 伝播間隔                                               |
|                                             | delta-crdt.compact-after-inactive                                | 非アクティブ後の圧縮                                   |
|                                             | delta-crdt.reset-after-interval                                  | リセット間隔                                           |
|                                             | delta-crdt.time-to-live                                          | 生存時間                                               |
|                                             | delta-crdt.write-majority                                        | 書き込み多数                                           |
|                                             | delta-crdt.read-majority                                         | 読み取り多数                                           |
|                                             | delta-crdt.stable-after                                          | 安定化時間                                             |
|                                             | delta-crdt.max-pruning-dissemination                             | 最大プルーニング伝播                                   |
|                                             | delta-crdt.pruning-marker-time-to-live                           | プルーニングマーカーの生存時間                         |
|                                             | delta-crdt.pruning-interval                                      | プルーニング間隔                                       |
|                                             | delta-crdt.gossip-interval                                       | ゴシップ間隔                                           |
|                                             | delta-crdt.notify-subscribers-interval                           | サブスクライバー通知間隔                               |
|                                             | delta-crdt.max-delta-size                                        | 最大デルタサイズ                                       |
| 54. akka.cluster.singleton                  | singleton-name                                                   | シングルトン名                                         |
|                                             | role                                                             | シングルトンのロール                                   |
|                                             | hand-over-retry-interval                                         | ハンドオーバー再試行間隔                               |
|                                             | min-number-of-hand-over-retries                                  | 最小ハンドオーバー再試行回数                           |
|                                             | use-lease                                                        | リースの使用                                           |
|                                             | lease-implementation                                             | リース実装                                             |
|                                             | lease-retry-interval                                             | リース再試行間隔                                       |
|                                             | removal-margin                                                   | 削除マージン                                           |
|                                             | buffer-size                                                      | バッファサイズ                                         |
|                                             | singleton-proxy                                                  | シングルトンプロキシ設定                               |
|                                             | singleton-proxy.singleton-name                                   | プロキシのシングルトン名                               |
|                                             | singleton-proxy.role                                             | プロキシのロール                                       |
|                                             | singleton-proxy.singleton-identification-interval                | シングルトン識別間隔                                   |
|                                             | singleton-proxy.buffer-size                                      | プロキシバッファサイズ                                 |
|                                             | singleton-proxy.use-dispatcher                                   | プロキシの使用ディスパッチャー                         |
|                                             | allow-multiple-oldest-nodes                                      | 複数の最古ノードの許可                                 |
|                                             | stable-after                                                     | 安定化時間                                             |
|                                             | jitter                                                           | ジッター                                               |
|                                             | lease-majority-check-interval                                    | リース多数チェック間隔                                 |
|                                             | lease-operation-timeout                                          | リース操作タイムアウト                                 |
| 55. akka.cluster.pubsub                     | gossip-interval                                                  | ゴシップ間隔                                           |
|                                             | removed-time-to-live                                             | 削除されたエントリーの生存時間                         |
|                                             | max-delta-elements                                               | 最大デルタ要素数                                       |
|                                             | routing-logic                                                    | ルーティングロジック                                   |
|                                             | send-to-dead-letters-when-no-subscribers                         | サブスクライバーがいない場合のデッドレター送信         |
|                                             | shard-size                                                       | シャードサイズ                                         |
|                                             | max-shard-number                                                 | 最大シャード数                                         |
|                                             | distributed-pubsub-mediator                                      | 分散 pubsub メディエーター設定                         |
|                                             | use-dispatcher                                                   | 使用するディスパッチャー                               |
|                                             | gossip-different-view-probability                                | 異なるビューのゴシップ確率                             |
|                                             | keep-removed-time-to-live                                        | 削除されたエントリーの保持時間                         |
|                                             | tombstone-time-to-live                                           | トゥームストーンの生存時間                             |
|                                             | max-delta-size                                                   | 最大デルタサイズ                                       |
|                                             | pruning-interval                                                 | 刈り込み間隔                                           |
|                                             | log-restoration-on-recovery                                      | リカバリー時のログ復元                                 |
|                                             | publish-local                                                    | ローカル公開の有効化                                   |
|                                             | send-local-first                                                 | ローカル優先送信                                       |
|                                             | shard-redistribution-interval                                    | シャード再配布間隔                                     |
|                                             | expected-update-delay                                            | 予想更新遅延                                           |
|                                             | delta-propagation-interval                                       | デルタ伝播間隔                                         |
| 56. akka.cluster.client                     | initial-contacts                                                 | 初期コンタクト                                         |
|                                             | establishing-get-contacts-interval                               | コンタクト取得間隔の確立                               |
|                                             | refresh-contacts-interval                                        | コンタクト更新間隔                                     |
|                                             | heartbeat-interval                                               | ハートビート間隔                                       |
|                                             | acceptable-heartbeat-pause                                       | 許容可能なハートビート停止時間                         |
|                                             | buffer-size                                                      | バッファサイズ                                         |
|                                             | reconnect-timeout                                                | 再接続タイムアウト                                     |
|                                             | receptionist                                                     | レセプショニスト設定                                   |
|                                             | receptionist.name                                                | レセプショニスト名                                     |
|                                             | receptionist.role                                                | レセプショニストのロール                               |
|                                             | receptionist.number-of-contacts                                  | コンタクト数                                           |
|                                             | use-dispatcher                                                   | 使用するディスパッチャー                               |
|                                             | gossip-interval                                                  | ゴシップ間隔                                           |
|                                             | warning-for-entered-quarantine                                   | 隔離エントリーの警告                                   |
|                                             | attempt-quarantine-first                                         | 最初の隔離試行                                         |
|                                             | max-failures-for-quarantine                                      | 隔離のための最大失敗数                                 |
|                                             | quarantine-duration                                              | 隔離期間                                               |
|                                             | quarantine-after                                                 | 隔離後の時間                                           |
|                                             | retry-interval                                                   | 再試行間隔                                             |
|                                             | max-retries                                                      | 最大再試行回数                                         |
| 57. akka.cluster.typed                      | gossip-interval                                                  | ゴシップ間隔                                           |
|                                             | gossip-different-view-probability                                | 異なるビューのゴシップ確率                             |
|                                             | reduced-gossip-key-size                                          | 縮小されたゴシップキーサイズ                           |
|                                             | periodic-tasks-initial-delay                                     | 定期タスクの初期遅延                                   |
|                                             | publish-stats-interval                                           | 統計情報の公開間隔                                     |
|                                             | use-dispatcher                                                   | 使用するディスパッチャー                               |
|                                             | gossip-time-to-live                                              | ゴシップの生存時間                                     |
|                                             | leader-actions-interval                                          | リーダーアクションの間隔                               |
|                                             | unreachable-nodes-reaper-interval                                | 到達不能ノードの刈り取り間隔                           |
|                                             | down-removal-margin                                              | ダウン削除マージン                                     |
|                                             | allow-weakly-up-members                                          | 弱い状態のメンバーを許可するかどうか                   |
|                                             | number-of-pruning-markers                                        | プルーニングマーカーの数                               |
|                                             | shutdown-after-unsuccessful-join-seed-nodes                      | シードノード参加失敗後のシャットダウン時間             |
|                                             | periodic-tasks-initial-delay                                     | 定期タスクの初期遅延                                   |
|                                             | retry-unsuccessful-join-after                                    | 参加失敗後の再試行間隔                                 |
|                                             | adapter-name                                                     | アダプター名                                           |
|                                             | min-nr-of-members                                                | クラスターの最小メンバー数                             |
|                                             | log-info-verbose                                                 | 詳細な情報ログ                                         |
|                                             | failure-detector                                                 | 失敗検出器設定                                         |
| 58. akka.cluster.multi-data-center          | self-data-center                                                 | 自データセンター                                       |
|                                             | cross-data-center-connections                                    | クロスデータセンター接続数                             |
|                                             | gossip-interval                                                  | ゴシップ間隔                                           |
|                                             | failure-detector                                                 | 失敗検出器設定                                         |
|                                             | heartbeat-interval                                               | ハートビート間隔                                       |
|                                             | acceptable-heartbeat-pause                                       | 許容可能なハートビート停止時間                         |
|                                             | number-of-end-points-to-probe                                    | プローブするエンドポイント数                           |
|                                             | expected-response-after                                          | 期待される応答後の時間                                 |
|                                             | unreachable-nodes-reaper-interval                                | 到達不能ノードの刈り取り間隔                           |
|                                             | down-all-nodes-after                                             | 全ノードダウン後の時間                                 |
|                                             | stable-after                                                     | 安定化時間                                             |
|                                             | down-removal-margin                                              | ダウン削除マージン                                     |
|                                             | cross-data-center-gossip-probability                             | クロスデータセンターゴシップ確率                       |
|                                             | reduced-gossip-key-size                                          | 縮小されたゴシップキーサイズ                           |
|                                             | gossip-different-view-probability                                | 異なるビューのゴシップ確率                             |
|                                             | failure-detector-implementation-class                            | 失敗検出器実装クラス                                   |
|                                             | metrics-collector                                                | メトリクスコレクター設定                               |
|                                             | metrics-gossip-interval                                          | メトリクスゴシップ間隔                                 |
|                                             | periodic-tasks-initial-delay                                     | 定期タスクの初期遅延                                   |
|                                             | publish-stats-interval                                           | 統計情報の公開間隔                                     |
| 59. akka.cluster.split-brain-resolver       | active-strategy                                                  | アクティブな戦略                                       |
|                                             | stable-after                                                     | 安定化時間                                             |
|                                             | down-all-when-unstable                                           | 不安定時の全ノードダウン                               |
|                                             | keep-majority                                                    | 多数派維持設定                                         |
|                                             | keep-oldest                                                      | 最古ノード維持設定                                     |
|                                             | keep-referee                                                     | レフェリー維持設定                                     |
|                                             | static-quorum                                                    | 静的クォーラム設定                                     |
|                                             | lease-majority                                                   | リース多数派設定                                       |
|                                             | down-all                                                         | 全ノードダウン設定                                     |
|                                             | keep-majority.role                                               | 多数派維持のロール                                     |
|                                             | keep-oldest.down-if-alone                                        | 単独時のダウン                                         |
|                                             | keep-referee.address                                             | レフェリーアドレス                                     |
|                                             | static-quorum.quorum-size                                        | クォーラムサイズ                                       |
|                                             | static-quorum.role                                               | 静的クォーラムのロール                                 |
|                                             | lease-majority.lease-implementation                              | リース実装                                             |
|                                             | down-all.stable-after                                            | 全ノードダウン後の安定化時間                           |
|                                             | lease-majority.lease-name                                        | リース名                                               |
|                                             | lease-majority.acquire-lease-delay-for-minority                  | 少数派のリース取得遅延                                 |
|                                             | lease-majority.release-after                                     | リース解放時間                                         |
|                                             | lease-majority.lease-implementation-class                        | リース実装クラス                                       |
| 60. akka.cluster.bootstrap                  | contact-point-discovery                                          | コンタクトポイント発見設定                             |
|                                             | contact-point                                                    | コンタクトポイント設定                                 |
|                                             | new-cluster-enabled                                              | 新しいクラスターの有効化                               |
|                                             | min-members                                                      | 最小メンバー数                                         |
|                                             | contact-point-discovery.service-name                             | サービス名                                             |
|                                             | contact-point-discovery.discovery-method                         | 発見方法                                               |
|                                             | contact-point-discovery.required-contact-point-nr                | 必要なコンタクトポイント数                             |
|                                             | contact-point-discovery.interval                                 | 発見間隔                                               |
|                                             | contact-point-discovery.exponential-backoff-random-factor        | 指数バックオフランダムファクター                       |
|                                             | contact-point-discovery.exponential-backoff-max                  | 最大指数バックオフ                                     |
|                                             | contact-point-discovery.config-contact-points                    | 設定コンタクトポイント                                 |
|                                             | contact-point.fallback-port                                      | フォールバックポート                                   |
|                                             | contact-point.probing-failure-timeout                            | プロービング失敗タイムアウト                           |
|                                             | contact-point.probe-interval                                     | プローブ間隔                                           |
|                                             | contact-point.probe-request-timeout                              | プローブリクエストタイムアウト                         |
|                                             | join-timeout                                                     | 参加タイムアウト                                       |
|                                             | abort-timeout                                                    | 中断タイムアウト                                       |
|                                             | stable-margin                                                    | 安定マージン                                           |
|                                             | discovery-attempt-interval                                       | 発見試行間隔                                           |
|                                             | manual-cleanup                                                   | 手動クリーンアップ                                     |
| 61. akka.persistence.journal.leveldb        | dir                                                              | ディレクトリパス                                       |
|                                             | native                                                           | ネイティブ LevelDB 使用                                |
|                                             | compaction-intervals                                             | コンパクション間隔                                     |
|                                             | fsync                                                            | fsync 有効化                                           |
|                                             | write-batch-size                                                 | 書き込みバッチサイズ                                   |
|                                             | use-direct-buffer                                                | ダイレクトバッファ使用                                 |
|                                             | target-io-operations-per-second                                  | 目標 IO 操作/秒                                        |
|                                             | replay-filter                                                    | リプレイフィルター設定                                 |
|                                             | checksum                                                         | チェックサム有効化                                     |
|                                             | compaction-trigger-interval                                      | コンパクショントリガー間隔                             |
|                                             | compaction-threshold                                             | コンパクションしきい値                                 |
|                                             | recovery-batch-size                                              | リカバリーバッチサイズ                                 |
|                                             | recovery-parallelism                                             | リカバリー並列度                                       |
|                                             | recovery-dispatcher                                              | リカバリーディスパッチャー                             |
|                                             | replay-dispatcher                                                | リプレイディスパッチャー                               |
|                                             | snapshot-store                                                   | スナップショットストア設定                             |
|                                             | delete-old-entries                                               | 古いエントリーの削除                                   |
|                                             | delete-replica-entries                                           | レプリカエントリーの削除                               |
|                                             | event-adapters                                                   | イベントアダプター                                     |
| 62. akka.persistence.snapshot-store.local   | dir                                                              | ディレクトリパス                                       |
|                                             | max-load-attempts                                                | 最大ロード試行回数                                     |
|                                             | stream-io                                                        | ストリーム IO 設定                                     |
|                                             | write-batch-size                                                 | 書き込みバッチサイズ                                   |
|                                             | use-direct-buffer                                                | ダイレクトバッファ使用                                 |
|                                             | fsync                                                            | fsync 有効化                                           |
|                                             | checksum                                                         | チェックサム有効化                                     |
|                                             | snapshot-is-optional                                             | スナップショットのオプション化                         |
|                                             | load-attempt-interval                                            | ロード試行間隔                                         |
|                                             | recovery-dispatcher                                              | リカバリーディスパッチャー                             |
|                                             | replay-dispatcher                                                | リプレイディスパッチャー                               |
|                                             | stream-dispatcher                                                | ストリームディスパッチャー                             |
|                                             | compression                                                      | 圧縮設定                                               |
|                                             | metadata-table-name                                              | メタデータテーブル名                                   |
|                                             | snapshot-table-name                                              | スナップショットテーブル名                             |
|                                             | max-concurrent-recoveries                                        | 最大同時リカバリー数                                   |
|                                             | recovery-timeout                                                 | リカバリータイムアウト                                 |
|                                             | replay-filter                                                    | リプレイフィルター設定                                 |
|                                             | event-adapters                                                   | イベントアダプター                                     |
|                                             | serialization-identifier-migration                               | シリアル化識別子マイグレーション                       |
| 63. akka.persistence.query                  | journal.plugin                                                   | ジャーナルプラグイン設定                               |
|                                             | max-buffer-size                                                  | 最大バッファサイズ                                     |
|                                             | refresh-interval                                                 | リフレッシュ間隔                                       |
|                                             | read-journal                                                     | 読み取りジャーナル設定                                 |
|                                             | write-journal                                                    | 書き込みジャーナル設定                                 |
|                                             | journal.leveldb                                                  | LevelDB 読み取りジャーナル設定                         |
|                                             | journal.inmem                                                    | インメモリ読み取りジャーナル設定                       |
|                                             | ask-timeout                                                      | 問い合わせタイムアウト                                 |
|                                             | max-buffered-orders                                              | 最大バッファ注文数                                     |
|                                             | replay-parallelism                                               | リプレイ並列度                                         |
|                                             | sequence-nr-gap                                                  | シーケンス番号ギャップ                                 |
|                                             | gap-free-sequence-nr                                             | ギャップフリーシーケンス番号                           |
|                                             | eventual-consistency-delay                                       | 最終的一貫性遅延                                       |
|                                             | delayed-event-timeout                                            | 遅延イベントタイムアウト                               |
|                                             | pubsub-minimum-interval                                          | PubSub の最小間隔                                      |
|                                             | timestamp-query                                                  | タイムスタンプクエリ設定                               |
|                                             | journal-sequence-retrieval                                       | ジャーナルシーケンス取得設定                           |
|                                             | max-concurrent-replays                                           | 最大同時リプレイ数                                     |
|                                             | use-dispatcher                                                   | 使用するディスパッチャー                               |
| 64. akka.persistence.typed                  | recovery-timeout                                                 | リカバリータイムアウト                                 |
|                                             | internal-stash-overflow-strategy                                 | 内部スタッシュオーバーフロー戦略                       |
|                                             | running-event-adapters                                           | 実行中のイベントアダプター                             |
|                                             | recovery                                                         | リカバリー設定                                         |
|                                             | snapshot-adapter                                                 | スナップショットアダプター                             |
|                                             | recovery-permit-timeout                                          | リカバリー許可タイムアウト                             |
|                                             | replay-filter                                                    | リプレイフィルター設定                                 |
|                                             | stash-capacity                                                   | スタッシュ容量                                         |
|                                             | stash-overflow-strategy                                          | スタッシュオーバーフロー戦略                           |
|                                             | recovery.disable-load-timers                                     | ロードタイマーの無効化                                 |
|                                             | recovery.max-number-of-events                                    | 最大イベント数                                         |
|                                             | recovery.recovery-event-timeout                                  | リカバリーイベントタイムアウト                         |
|                                             | replay-filter.mode                                               | リプレイフィルターモード                               |
|                                             | replay-filter.window-size                                        | ウィンドウサイズ                                       |
|                                             | replay-filter.max-old-writers                                    | 最大古いライター数                                     |
|                                             | passivation-strategy                                             | パッシベーション戦略                                   |
|                                             | default-snapshot-store                                           | デフォルトスナップショットストア                       |
|                                             | state-change-timeout                                             | 状態変更タイムアウト                                   |
|                                             | command-handler-warning-timeout                                  | コマンドハンドラー警告タイムアウト                     |
| 65. akka.remote.artery                      | enabled                                                          | Artery の有効化                                        |
|                                             | transport                                                        | トランスポート設定                                     |
|                                             | canonical.hostname                                               | 正規ホスト名                                           |
|                                             | canonical.port                                                   | 正規ポート                                             |
|                                             | bind.hostname                                                    | バインドホスト名                                       |
|                                             | bind.port                                                        | バインドポート                                         |
|                                             | ssl                                                              | SSL 設定                                               |
|                                             | large-message-destinations                                       | 大きなメッセージの宛先                                 |
|                                             | advanced                                                         | 高度な設定                                             |
|                                             | advanced.inbound-lanes                                           | 受信レーン数                                           |
|                                             | advanced.outbound-lanes                                          | 送信レーン数                                           |
|                                             | advanced.buffer-pool-size                                        | バッファプールサイズ                                   |
|                                             | advanced.maximum-frame-size                                      | 最大フレームサイズ                                     |
|                                             | advanced.idle-cpu-level                                          | アイドル CPU レベル                                    |
|                                             | advanced.compression                                             | 圧縮設定                                               |
|                                             | advanced.aeron                                                   | Aeron 設定                                             |
|                                             | advanced.aeron.embedded-media-driver                             | 組み込みメディアドライバー                             |
|                                             | advanced.aeron.aeron-dir                                         | Aeron ディレクトリ                                     |
|                                             | advanced.aeron.delete-aeron-dir                                  | Aeron ディレクトリの削除                               |
| 66. akka.remote.classic                     | enabled-transports                                               | 有効なトランスポート                                   |
|                                             | netty.tcp                                                        | Netty TCP 設定                                         |
|                                             | netty.udp                                                        | Netty UDP 設定                                         |
|                                             | netty.ssl                                                        | Netty SSL 設定                                         |
|                                             | netty.tcp.hostname                                               | TCP ホスト名                                           |
|                                             | netty.tcp.port                                                   | TCP ポート                                             |
|                                             | netty.tcp.bind-hostname                                          | TCP バインドホスト名                                   |
|                                             | netty.tcp.bind-port                                              | TCP バインドポート                                     |
|                                             | netty.tcp.connection-timeout                                     | TCP 接続タイムアウト                                   |
|                                             | netty.tcp.outbound-buffer-size                                   | TCP 送信バッファサイズ                                 |
|                                             | netty.tcp.send-buffer-size                                       | TCP 送信バッファサイズ                                 |
|                                             | netty.tcp.receive-buffer-size                                    | TCP 受信バッファサイズ                                 |
|                                             | netty.tcp.maximum-frame-size                                     | TCP 最大フレームサイズ                                 |
|                                             | netty.tcp.backlog                                                | TCP バックログ                                         |
|                                             | netty.ssl.security                                               | SSL セキュリティ設定                                   |
|                                             | netty.ssl.ssl-engine-provider                                    | SSL エンジンプロバイダー                               |
|                                             | netty.ssl.key-store                                              | SSL キーストア設定                                     |
|                                             | netty.ssl.trust-store                                            | SSL トラストストア設定                                 |
|                                             | netty.ssl.protocol                                               | SSL プロトコル                                         |
| 67. akka.discovery                          | method                                                           | サービス発見メソッド                                   |
|                                             | kubernetes-api                                                   | Kubernetes API 設定                                    |
|                                             | config                                                           | 設定ベースの発見設定                                   |
|                                             | aggregate                                                        | 集約発見設定                                           |
|                                             | dns                                                              | DNS 発見設定                                           |
|                                             | kubernetes-api.pod-label-selector                                | Kubernetes ポッドラベルセレクター                      |
|                                             | kubernetes-api.pod-namespace                                     | Kubernetes ポッド名前空間                              |
|                                             | kubernetes-api.request-timeout                                   | Kubernetes API リクエストタイムアウト                  |
|                                             | config.services                                                  | 設定ベースのサービス定義                               |
|                                             | aggregate.discovery-methods                                      | 集約する発見メソッド                                   |
|                                             | dns.protocol                                                     | DNS 発見プロトコル                                     |
|                                             | dns.resolv-conf                                                  | resolv.conf ファイルのパス                             |
|                                             | dns.resolve-srv                                                  | SRV レコードの解決                                     |
|                                             | dns.use-ipv6                                                     | IPv6 の使用                                            |
|                                             | dns.resolve-timeout                                              | DNS 解決タイムアウト                                   |
|                                             | dns.async-dns                                                    | 非同期 DNS 解決の使用                                  |
|                                             | method                                                           | デフォルトの発見メソッド                               |
|                                             | kubernetes-api.api-ca-path                                       | Kubernetes API CA 証明書パス                           |
|                                             | kubernetes-api.api-token-path                                    | Kubernetes API トークンパス                            |
|                                             | kubernetes-api.api-service-host                                  | Kubernetes API サービスホスト                          |
| 68. akka.coordinated-shutdown               | phases                                                           | シャットダウンフェーズの定義                           |
|                                             | phase-timeout                                                    | 各フェーズのタイムアウト                               |
|                                             | timeout                                                          | 全体のタイムアウト                                     |
|                                             | exit-jvm                                                         | JVM 終了の有効化                                       |
|                                             | run-by-jvm-shutdown-hook                                         | JVM シャットダウンフックでの実行                       |
|                                             | coordinated-shutdown-phases                                      | カスタムシャットダウンフェーズの定義                   |
|                                             | default-phase-timeout                                            | デフォルトのフェーズタイムアウト                       |
|                                             | terminate-actor-system                                           | アクターシステム終了フェーズのタイムアウト             |
|                                             | exit-jvm-failure-reason                                          | JVM 終了時の失敗理由                                   |
|                                             | abort-timeout                                                    | 中断タイムアウト                                       |
|                                             | reason-overrides                                                 | 理由に基づくオーバーライド                             |
|                                             | lease-linger                                                     | リースの延長時間                                       |
|                                             | cooperative-shutdown-timeout                                     | 協調シャットダウンのタイムアウト                       |
|                                             | force-abort-timeout                                              | 強制中断タイムアウト                                   |
|                                             | coordinated-shutdown-phases.before-service-unbind                | サービスアンバインド前のフェーズ                       |
|                                             | coordinated-shutdown-phases.service-unbind                       | サービスアンバインドフェーズ                           |
|                                             | coordinated-shutdown-phases.service-requests-done                | サービスリクエスト完了フェーズ                         |
|                                             | coordinated-shutdown-phases.service-stop                         | サービス停止フェーズ                                   |
|                                             | coordinated-shutdown-phases.before-cluster-shutdown              | クラスターシャットダウン前フェーズ                     |
|                                             | coordinated-shutdown-phases.cluster-sharding-shutdown-region     | クラスターシャーディング領域シャットダウンフェーズ     |
| 69. akka.stream.alpakka                     | file                                                             | ファイル関連設定                                       |
|                                             | ftp                                                              | FTP 関連設定                                           |
|                                             | s3                                                               | S3 関連設定                                            |
|                                             | slick                                                            | Slick 関連設定                                         |
|                                             | mqtt                                                             | MQTT 関連設定                                          |
|                                             | dynamodb                                                         | DynamoDB 関連設定                                      |
|                                             | cassandra                                                        | Cassandra 関連設定                                     |
|                                             | elasticsearch                                                    | Elasticsearch 関連設定                                 |
|                                             | mongodb                                                          | MongoDB 関連設定                                       |
|                                             | google-cloud-pub-sub                                             | Google Cloud Pub/Sub 関連設定                          |
|                                             | jms                                                              | JMS 関連設定                                           |
|                                             | kinesis                                                          | Kinesis 関連設定                                       |
|                                             | couchbase                                                        | Couchbase 関連設定                                     |
|                                             | sns                                                              | SNS 関連設定                                           |
|                                             | sqs                                                              | SQS 関連設定                                           |
|                                             | csv                                                              | CSV 関連設定                                           |
|                                             | xml                                                              | XML 関連設定                                           |
|                                             | udp                                                              | UDP 関連設定                                           |
|                                             | unix-domain-socket                                               | Unix ドメインソケット関連設定                          |
|                                             | avroparquet                                                      | Avro/Parquet 関連設定                                  |
| 70. akka.stream.testkit                     | test-timefactor                                                  | テスト時間係数                                         |
|                                             | filter-leeway                                                    | フィルターの余裕                                       |
|                                             | default-timeout                                                  | デフォルトタイムアウト                                 |
|                                             | stream-materialization-timeout                                   | ストリームマテリアライゼーションタイムアウト           |
|                                             | throw-on-early-termination                                       | 早期終了時の例外スロー                                 |
|                                             | debug-logging                                                    | デバッグロギング                                       |
|                                             | materializer                                                     | テスト用マテリアライザー設定                           |
|                                             | subscription-timeout                                             | サブスクリプションタイムアウト                         |
|                                             | tick-duration                                                    | ティック間隔                                           |
|                                             | default-test-sink-buffer-size                                    | デフォルトテストシンクバッファサイズ                   |
|                                             | default-test-source-buffer-size                                  | デフォルトテストソースバッファサイズ                   |
|                                             | stream-test-timeout                                              | ストリームテストタイムアウト                           |
|                                             | expect-no-message-default                                        | メッセージ非受信期待のデフォルト時間                   |
|                                             | single-expect-default                                            | 単一期待のデフォルト時間                               |
|                                             | test-sink-probe-settings                                         | テストシンクプローブ設定                               |
|                                             | test-source-probe-settings                                       | テストソースプローブ設定                               |
|                                             | test-publisher-settings                                          | テストパブリッシャー設定                               |
|                                             | test-subscriber-settings                                         | テストサブスクライバー設定                             |
|                                             | stream-test-timeout-factor                                       | ストリームテストタイムアウト係数                       |
| 71. akka.kafka.consumer                     | poll-interval                                                    | ポーリング間隔                                         |
|                                             | poll-timeout                                                     | ポーリングタイムアウト                                 |
|                                             | stop-timeout                                                     | 停止タイムアウト                                       |
|                                             | close-timeout                                                    | クローズタイムアウト                                   |
|                                             | commit-timeout                                                   | コミットタイムアウト                                   |
|                                             | wakeup-timeout                                                   | ウェイクアップタイムアウト                             |
|                                             | max-wakeups                                                      | 最大ウェイクアップ回数                                 |
|                                             | use-dispatcher                                                   | 使用するディスパッチャー                               |
|                                             | wait-close-partition                                             | パーティションクローズ待機                             |
|                                             | position-timeout                                                 | ポジション取得タイムアウト                             |
|                                             | offset-for-times-timeout                                         | オフセット取得タイムアウト                             |
|                                             | metadata-request-timeout                                         | メタデータリクエストタイムアウト                       |
|                                             | eos-draining-check-interval                                      | EOS ドレイニングチェック間隔                           |
|                                             | partition-handler-warning                                        | パーティションハンドラー警告                           |
|                                             | commit-warnings                                                  | コミット警告                                           |
|                                             | kafka-clients                                                    | Kafka クライアント設定                                 |
|                                             | connection-checker                                               | 接続チェッカー設定                                     |
|                                             | commit-retry                                                     | コミット再試行設定                                     |
|                                             | commit-time-warning                                              | コミット時間警告                                       |
|                                             | reset-offset-on-invalid-messages                                 | 無効メッセージ時のオフセットリセット                   |
| 72. akka.kafka.producer                     | close-timeout                                                    | クローズタイムアウト                                   |
|                                             | use-dispatcher                                                   | 使用するディスパッチャー                               |
|                                             | eos-commit-interval                                              | EOS コミット間隔                                       |
|                                             | kafka-clients                                                    | Kafka クライアント設定                                 |
|                                             | resolve-timeout                                                  | 解決タイムアウト                                       |
|                                             | parallelism                                                      | 並列度                                                 |
|                                             | close-on-producer-stop                                           | プロデューサー停止時のクローズ                         |
|                                             | delivery-timeout                                                 | 配信タイムアウト                                       |
|                                             | flush-on-close                                                   | クローズ時のフラッシュ                                 |
|                                             | flush-timeout                                                    | フラッシュタイムアウト                                 |
|                                             | send-buffer                                                      | 送信バッファ                                           |
|                                             | max-batch-size                                                   | 最大バッチサイズ                                       |
|                                             | linger-ms                                                        | 待機時間                                               |
|                                             | max-request-size                                                 | 最大リクエストサイズ                                   |
|                                             | acks                                                             | 確認応答設定                                           |
|                                             | compression-type                                                 | 圧縮タイプ                                             |
|                                             | max-in-flight-requests-per-connection                            | 接続ごとの最大処理中リクエスト数                       |
|                                             | retries                                                          | 再試行回数                                             |
|                                             | batch-size                                                       | バッチサイズ                                           |
| 73. akka.projection                         | restart-backoff                                                  | 再起動バックオフ                                       |
|                                             | group-after-envelopes                                            | エンベロープ後のグループ化                             |
|                                             | group-after-duration                                             | 期間後のグループ化                                     |
|                                             | recovery-strategy                                                | リカバリー戦略                                         |
|                                             | management                                                       | 管理設定                                               |
|                                             | restart-max-attempts                                             | 最大再起動試行回数                                     |
|                                             | save-offset-after-envelopes                                      | エンベロープ後のオフセット保存                         |
|                                             | save-offset-after-duration                                       | 期間後のオフセット保存                                 |
|                                             | status-obser-interval                                            | ステータス観測間隔                                     |
|                                             | number-of-offsets-retained                                       | 保持するオフセット数                                   |
|                                             | atLeastOnce                                                      | 少なくとも 1 回の処理設定                              |
|                                             | exactlyOnce                                                      | 厳密に 1 回の処理設定                                  |
|                                             | groupedWithin                                                    | グループ化設定                                         |
|                                             | retryAndFail                                                     | 再試行と失敗設定                                       |
|                                             | retryAndSkip                                                     | 再試行とスキップ設定                                   |
|                                             | orElse                                                           | その他の設定                                           |
|                                             | slick                                                            | Slick 設定                                             |
|                                             | cassandra                                                        | Cassandra 設定                                         |
|                                             | elasticsearch                                                    | Elasticsearch 設定                                     |
|                                             | jdbc                                                             | JDBC 設定                                              |
| 74. akka.management                         | http                                                             | HTTP 管理設定                                          |
|                                             | health-checks                                                    | ヘルスチェック設定                                     |
|                                             | cluster-bootstrap                                                | クラスターブートストラップ設定                         |
|                                             | cluster-http                                                     | クラスター HTTP 設定                                   |
|                                             | http.hostname                                                    | HTTP ホスト名                                          |
|                                             | http.port                                                        | HTTP ポート                                            |
|                                             | http.bind-hostname                                               | HTTP バインドホスト名                                  |
|                                             | http.bind-port                                                   | HTTP バインドポート                                    |
|                                             | http.route-providers-read-only                                   | 読み取り専用ルートプロバイダー                         |
|                                             | http.routes                                                      | カスタムルート                                         |
|                                             | health-checks.readiness-checks                                   | レディネスチェック                                     |
|                                             | health-checks.liveness-checks                                    | ライブネスチェック                                     |
|                                             | cluster-bootstrap.contact-point-discovery                        | コンタクトポイント発見設定                             |
|                                             | cluster-bootstrap.contact-point                                  | コンタクトポイント設定                                 |
|                                             | cluster-http.route-providers                                     | ルートプロバイダー                                     |
|                                             | cluster-http.path-prefix                                         | パスプレフィックス                                     |
|                                             | cluster-http.port                                                | クラスター HTTP ポート                                 |
|                                             | cluster-http.host                                                | クラスター HTTP ホスト                                 |
|                                             | cluster-http.protocol                                            | クラスター HTTP プロトコル                             |
| 75. akka.persistence.cassandra              | journal                                                          | ジャーナル設定                                         |
|                                             | snapshot                                                         | スナップショット設定                                   |
|                                             | query                                                            | クエリ設定                                             |
|                                             | events-by-tag                                                    | タグ別イベント設定                                     |
|                                             | journal.keyspace                                                 | ジャーナルキースペース                                 |
|                                             | journal.table                                                    | ジャーナルテーブル                                     |
|                                             | journal.keyspace-autocreate                                      | キースペース自動作成                                   |
|                                             | journal.tables-autocreate                                        | テーブル自動作成                                       |
|                                             | journal.replication-factor                                       | レプリケーション係数                                   |
|                                             | journal.write-consistency                                        | 書き込み一貫性                                         |
|                                             | journal.read-consistency                                         | 読み取り一貫性                                         |
|                                             | snapshot.keyspace                                                | スナップショットキースペース                           |
|                                             | snapshot.table                                                   | スナップショットテーブル                               |
|                                             | query.refresh-interval                                           | クエリリフレッシュ間隔                                 |
|                                             | query.max-buffer-size                                            | 最大バッファサイズ                                     |
|                                             | events-by-tag.first-time-bucket                                  | 最初のタイムバケット                                   |
|                                             | events-by-tag.bucket-size                                        | バケットサイズ                                         |
|                                             | events-by-tag.max-message-batch-size                             | 最大メッセージバッチサイズ                             |
|                                             | events-by-tag.offset-tracking-page-size                          | オフセットトラッキングページサイズ                     |
| 76. akka.persistence.jdbc                   | journal                                                          | ジャーナル設定                                         |
|                                             | snapshot                                                         | スナップショット設定                                   |
|                                             | query                                                            | クエリ設定                                             |
|                                             | journal.class                                                    | ジャーナルクラス                                       |
|                                             | journal.plugin-dispatcher                                        | プラグインディスパッチャー                             |
|                                             | journal.circuit-breaker                                          | サーキットブレーカー設定                               |
|                                             | journal.dao                                                      | DAO クラス                                             |
|                                             | journal.table-configuration                                      | テーブル設定                                           |
|                                             | snapshot.class                                                   | スナップショットクラス                                 |
|                                             | snapshot.plugin-dispatcher                                       | プラグインディスパッチャー                             |
|                                             | snapshot.circuit-breaker                                         | サーキットブレーカー設定                               |
|                                             | snapshot.dao                                                     | DAO クラス                                             |
|                                             | snapshot.table-configuration                                     | テーブル設定                                           |
|                                             | query.class                                                      | クエリクラス                                           |
|                                             | query.refresh-interval                                           | クエリリフレッシュ間隔                                 |
|                                             | query.max-buffer-size                                            | 最大バッファサイズ                                     |
|                                             | slick                                                            | Slick 設定                                             |
|                                             | slick.profile                                                    | Slick プロファイル                                     |
|                                             | slick.db                                                         | Slick データベース設定                                 |
| 77. akka.persistence.mongodb                | mongo-journal                                                    | MongoDB ジャーナル設定                                 |
|                                             | mongo-snapshot-store                                             | MongoDB スナップショットストア設定                     |
|                                             | mongo-read-journal                                               | MongoDB 読み取りジャーナル設定                         |
|                                             | mongo-journal.mongo-uri                                          | MongoDB ジャーナル URI                                 |
|                                             | mongo-journal.mongo-collection                                   | MongoDB ジャーナルコレクション                         |
|                                             | mongo-journal.write-concern                                      | 書き込み懸念                                           |
|                                             | mongo-journal.write-journal-plugin                               | 書き込みジャーナルプラグイン                           |
|                                             | mongo-snapshot-store.mongo-uri                                   | MongoDB スナップショットストア URI                     |
|                                             | mongo-snapshot-store.mongo-collection                            | MongoDB スナップショットストアコレクション             |
|                                             | mongo-snapshot-store.write-concern                               | 書き込み懸念                                           |
|                                             | mongo-read-journal.mongo-uri                                     | MongoDB 読み取りジャーナル URI                         |
|                                             | mongo-read-journal.mongo-collection                              | MongoDB 読み取りジャーナルコレクション                 |
|                                             | mongo-read-journal.refresh-interval                              | リフレッシュ間隔                                       |
|                                             | mongo-read-journal.max-buffer-size                               | 最大バッファサイズ                                     |
|                                             | mongo-read-journal.read-concern                                  | 読み取り懸念                                           |
|                                             | mongo-read-journal.use-legacy-serialization                      | レガシーシリアル化の使用                               |
|                                             | mongo-read-journal.retry-reads                                   | 読み取り再試行                                         |
|                                             | mongo-read-journal.retry-writes                                  | 書き込み再試行                                         |
|                                             | mongo-read-journal.circuit-breaker                               | サーキットブレーカー設定                               |
| 78. akka.persistence.dynamodb               | journal                                                          | DynamoDB ジャーナル設定                                |
|                                             | snapshot                                                         | DynamoDB スナップショット設定                          |
|                                             | query                                                            | DynamoDB クエリ設定                                    |
|                                             | journal.table                                                    | ジャーナルテーブル名                                   |
|                                             | journal.aws-client-config                                        | AWS クライアント設定                                   |
|                                             | journal.dispatcher                                               | ジャーナルディスパッチャー                             |
|                                             | journal.circuit-breaker                                          | サーキットブレーカー設定                               |
|                                             | journal.write-batch-size                                         | 書き込みバッチサイズ                                   |
|                                             | journal.read-batch-size                                          | 読み取りバッチサイズ                                   |
|                                             | journal.sequence-shards                                          | シーケンスシャード数                                   |
|                                             | snapshot.table                                                   | スナップショットテーブル名                             |
|                                             | snapshot.aws-client-config                                       | AWS クライアント設定                                   |
|                                             | snapshot.dispatcher                                              | スナップショットディスパッチャー                       |
|                                             | snapshot.circuit-breaker                                         | サーキットブレーカー設定                               |
|                                             | query.refresh-interval                                           | クエリリフレッシュ間隔                                 |
|                                             | query.max-buffer-size                                            | 最大バッファサイズ                                     |
|                                             | query.read-batch-size                                            | 読み取りバッチサイズ                                   |
|                                             | query.parallelism                                                | 並列度                                                 |
|                                             | query.aws-client-config                                          | AWS クライアント設定                                   |
| 79. akka.persistence.query.journal.leveldb  | class                                                            | LevelDB ジャーナルクエリクラス                         |
|                                             | write-plugin                                                     | 書き込みプラグイン                                     |
|                                             | refresh-interval                                                 | リフレッシュ間隔                                       |
|                                             | max-buffer-size                                                  | 最大バッファサイズ                                     |
|                                             | include-deletions                                                | 削除の含有                                             |
|                                             | batch-size                                                       | バッチサイズ                                           |
|                                             | persistence-id-separator                                         | 永続化 ID 区切り文字                                   |
|                                             | persist-all-events                                               | 全イベントの永続化                                     |
|                                             | use-dispatcher                                                   | 使用するディスパッチャー                               |
|                                             | replay-dispatcher                                                | リプレイディスパッチャー                               |
|                                             | ask-timeout                                                      | 問い合わせタイムアウト                                 |
|                                             | sequence-nr-gap                                                  | シーケンス番号ギャップ                                 |
|                                             | gap-free-sequence-nr                                             | ギャップフリーシーケンス番号                           |
|                                             | eventual-consistency-delay                                       | 最終的一貫性遅延                                       |
|                                             | delayed-event-timeout                                            | 遅延イベントタイムアウト                               |
|                                             | pubsub-minimum-interval                                          | PubSub 最小間隔                                        |
|                                             | timestamp-query                                                  | タイムスタンプクエリ                                   |
|                                             | journal-sequence-retrieval                                       | ジャーナルシーケンス取得                               |
|                                             | max-concurrent-replays                                           | 最大同時リプレイ数                                     |
|                                             | read-journal-plugin-config                                       | 読み取りジャーナルプラグイン設定                       |
| 80. akka.persistence.snapshot-store.plugin  | inmem                                                            | インメモリスナップショットストア                       |
|                                             | local                                                            | ローカルスナップショットストア                         |
|                                             | no-snapshot-store                                                | スナップショットストアなし                             |
|                                             | proxy                                                            | プロキシスナップショットストア                         |
|                                             | inmem.class                                                      | インメモリクラス                                       |
|                                             | local.class                                                      | ローカルクラス                                         |
|                                             | local.dir                                                        | ローカルディレクトリ                                   |
|                                             | local.snapshot-is-optional                                       | スナップショットのオプション化                         |
|                                             | proxy.class                                                      | プロキシクラス                                         |
|                                             | proxy.start-timeout                                              | 開始タイムアウト                                       |
|                                             | proxy.init-timeout                                               | 初期化タイムアウト                                     |
|                                             | proxy.stop-timeout                                               | 停止タイムアウト                                       |
|                                             | plugin-dispatcher                                                | プラグインディスパッチャー                             |
|                                             | plugin-stash-capacity                                            | プラグインスタッシュ容量                               |
|                                             | use-dispatcher                                                   | 使用するディスパッチャー                               |
|                                             | stream-dispatcher                                                | ストリームディスパッチャー                             |
|                                             | max-concurrent-recoveries                                        | 最大同時リカバリー数                                   |
|                                             | recovery-timeout                                                 | リカバリータイムアウト                                 |
|                                             | serialization-identifier-migration                               | シリアル化識別子マイグレーション                       |
| 81. akka.persistence.journal.leveldb        | dir                                                              | ディレクトリパス                                       |
|                                             | native                                                           | ネイティブ LevelDB 使用                                |
|                                             | fsync                                                            | fsync 有効化                                           |
|                                             | compaction-intervals                                             | コンパクション間隔                                     |
|                                             | checksum                                                         | チェックサム有効化                                     |
|                                             | class                                                            | LevelDB ジャーナルクラス                               |
|                                             | plugin-dispatcher                                                | プラグインディスパッチャー                             |
|                                             | replay-dispatcher                                                | リプレイディスパッチャー                               |
|                                             | recovery-dispatcher                                              | リカバリーディスパッチャー                             |
|                                             | use-direct-buffer                                                | ダイレクトバッファ使用                                 |
|                                             | target-io-operations-per-second                                  | 目標 IO 操作/秒                                        |
|                                             | write-batch-size                                                 | 書き込みバッチサイズ                                   |
|                                             | replay-filter                                                    | リプレイフィルター設定                                 |
|                                             | compaction-trigger-interval                                      | コンパクショントリガー間隔                             |
|                                             | compaction-threshold                                             | コンパクションしきい値                                 |
|                                             | recovery-batch-size                                              | リカバリーバッチサイズ                                 |
|                                             | recovery-parallelism                                             | リカバリー並列度                                       |
|                                             | delete-old-entries                                               | 古いエントリーの削除                                   |
|                                             | delete-replica-entries                                           | レプリカエントリーの削除                               |
|                                             | event-adapters                                                   | イベントアダプター                                     |
| 82. akka.actor.allow-java-serialization     | on                                                               | Java シリアル化の許可                                  |
|                                             | off                                                              | Java シリアル化の禁止                                  |
|                                             | warn                                                             | Java シリアル化の警告                                  |
|                                             | warn-on-first-use                                                | 初回使用時の警告                                       |
|                                             | warn-on-first-use-only                                           | 初回使用時のみ警告                                     |
|                                             | error                                                            | Java シリアル化時のエラー                              |
|                                             | custom                                                           | カスタム設定                                           |
|                                             | custom.class                                                     | カスタムクラス                                         |
|                                             | custom.allowed-classes                                           | 許可されたクラス                                       |
|                                             | custom.disallowed-classes                                        | 禁止されたクラス                                       |
|                                             | custom.allow-list                                                | 許可リスト                                             |
|                                             | custom.deny-list                                                 | 拒否リスト                                             |
|                                             | custom.serialization-bindings                                    | シリアル化バインディング                               |
|                                             | custom.serializers                                               | カスタムシリアライザー                                 |
|                                             | custom.allow-list-class-prefix                                   | 許可リストクラスプレフィックス                         |
|                                             | custom.deny-list-class-prefix                                    | 拒否リストクラスプレフィックス                         |
|                                             | custom.use-manifests                                             | マニフェストの使用                                     |
|                                             | custom.impl                                                      | カスタム実装                                           |
| 83. akka.cluster.sharding.external-sharding | proxy-name                                                       | プロキシ名                                             |
|                                             | shard-extraction-strategy                                        | シャード抽出戦略                                       |
|                                             | buffer-size                                                      | バッファサイズ                                         |
|                                             | use-dispatcher                                                   | 使用するディスパッチャー                               |
|                                             | entity-props-creator                                             | エンティティプロパティクリエーター                     |
|                                             | sharding-region-name                                             | シャーディング領域名                                   |
|                                             | shard-allocation-strategy                                        | シャード割り当て戦略                                   |
|                                             | least-shard-allocation-strategy                                  | 最小シャード割り当て戦略                               |
|                                             | rebalance-interval                                               | リバランス間隔                                         |
|                                             | snapshot-after                                                   | スナップショット作成条件                               |
|                                             | keep-n-snapshots                                                 | 保持するスナップショット数                             |
|                                             | snapshot-every                                                   | スナップショット作成間隔                               |
|                                             | journal-plugin-id                                                | ジャーナルプラグイン ID                                |
|                                             | snapshot-plugin-id                                               | スナップショットプラグイン ID                          |
|                                             | passivation-strategy                                             | パッシベーション戦略                                   |
|                                             | use-lease                                                        | リースの使用                                           |
|                                             | lease-retry-interval                                             | リース再試行間隔                                       |
|                                             | verbose-debug-logging                                            | 詳細なデバッグロギング                                 |
|                                             | coordinator-singleton                                            | コーディネーターシングルトン設定                       |
|                                             | coordinator-failure-backoff                                      | コーディネーター失敗バックオフ                         |
| 84. akka.cluster.client                     | initial-contacts                                                 | 初期コンタクト                                         |
|                                             | establishing-get-contacts-interval                               | コンタクト取得間隔の確立                               |
|                                             | refresh-contacts-interval                                        | コンタクト更新間隔                                     |
|                                             | heartbeat-interval                                               | ハートビート間隔                                       |
|                                             | acceptable-heartbeat-pause                                       | 許容可能なハートビート停止時間                         |
|                                             | buffer-size                                                      | バッファサイズ                                         |
|                                             | reconnect-timeout                                                | 再接続タイムアウト                                     |
|                                             | receptionist                                                     | レセプショニスト設定                                   |
|                                             | receptionist.name                                                | レセプショニスト名                                     |
|                                             | receptionist.role                                                | レセプショニストのロール                               |
|                                             | receptionist.number-of-contacts                                  | コンタクト数                                           |
|                                             | use-dispatcher                                                   | 使用するディスパッチャー                               |
|                                             | gossip-interval                                                  | ゴシップ間隔                                           |
|                                             | warning-for-entered-quarantine                                   | 隔離エントリーの警告                                   |
|                                             | attempt-quarantine-first                                         | 最初の隔離試行                                         |
|                                             | max-failures-for-quarantine                                      | 隔離のための最大失敗数                                 |
|                                             | quarantine-duration                                              | 隔離期間                                               |
|                                             | quarantine-after                                                 | 隔離後の時間                                           |

---

| 機能                             | 機能説明                       | Akka | Actix | Bastion | Riker | Axiom  | Kompact | Crayfish | Stakker | Aquatic | Acto   | Theatre | Acteur | 他のクレート          |
| -------------------------------- | ------------------------------ | ---- | ----- | ------- | ----- | ------ | ------- | -------- | ------- | ------- | ------ | ------- | ------ | --------------------- |
| メッセージ送受信                 | アクター間のメッセージ交換     | ✓    | ✓     | ✓       | ✓     | ✓      | ✓       | ✓        | ✓       | ✓       | ✓      | ✓       | ✓      | ✓ (Tokio-actors)      |
| メッセージディスパッチャー       | メッセージの効率的な配布       | ✓    | ✓     | ✓       | ✓     | ✓      | ✓       | ✓        | ✓       | ✓       | ✓      | ✓       | ✓      | ✓ (Tokio)             |
| フォールトトレランス             | エラー発生時の回復機能         | ✓    | ✓     | ✓       | ✓     | 限定的 | ✓       | 限定的   | 限定的  | 限定的  | 限定的 | 限定的  | ✓      | 限定的 (Failure)      |
| ロケーショントランスペアレンス   | アクターの位置を隠蔽           | ✓    | ❌    | ❌      | ❌    | ❌     | ✓       | ❌       | ❌      | ❌      | ❌     | ❌      | ❌     | ❌                    |
| リモートアクター                 | 異なるノード上のアクター通信   | ✓    | ✓     | ✓       | ✓     | ❌     | ✓       | ❌       | ❌      | ❌      | ❌     | ❌      | ✓      | ✓ (Tonic/gRPC)        |
| パーシステンス                   | アクター状態の永続化           | ✓    | ❌    | ❌      | ✓     | ❌     | ✓       | ❌       | ❌      | ❌      | ❌     | ❌      | ❌     | ❌                    |
| スケジューリング                 | タスクのスケジュール実行       | ✓    | ✓     | ✓       | ✓     | ❌     | ✓       | ❌       | ✓       | ❌      | ❌     | ❌      | ✓      | ✓ (Tokio)             |
| ストリーミング                   | 連続したデータの処理           | ✓    | ❌    | ❌      | ❌    | ❌     | ✓       | ❌       | ❌      | ❌      | ❌     | ❌      | ❌     | ✓ (Tokio-stream)      |
| トランザクショナルメッセージング | 一貫性のある複数メッセージ処理 | ✓    | ❌    | ❌      | ❌    | ❌     | ❌      | ❌       | ❌      | ❌      | ❌     | ❌      | ❌     | ❌                    |
| クラスタリング                   | 複数ノードでの協調動作         | ✓    | ❌    | ✓       | ✓     | ❌     | ✓       | ❌       | ❌      | ❌      | ❌     | ❌      | ✓      | ❌                    |
| 分散データ                       | ノード間でのデータ共有         | ✓    | ❌    | ❌      | ❌    | ❌     | ✓       | ❌       | ❌      | ❌      | ❌     | ❌      | ❌     | ❌                    |
| セキュリティ                     | 通信の暗号化とアクセス制御     | ✓    | ❌    | ❌      | ❌    | ❌     | ❌      | ❌       | ❌      | ❌      | ❌     | ❌      | ❌     | ✓ (Rustls)            |
| メトリクスとモニタリング         | パフォーマンスと健全性の監視   | ✓    | ✓     | ❌      | ❌    | ❌     | ✓       | ❌       | ❌      | ❌      | ❌     | ❌      | ✓      | ✓ (RillRate)          |
| 動的なインスタンス作成           | 実行時のアクター生成           | ✓    | ✓     | ✓       | ✓     | ❌     | ✓       | ✓        | ✓       | ✓       | ✓      | ✓       | ✓      | ✓ (Tokio-actors)      |
| アクター階層とスーパービジョン   | 親子関係と監視機能             | ✓    | ✓     | ✓       | ✓     | ❌     | ✓       | ✓        | ✓       | ✓       | ✓      | ✓       | ✓      | 限定的 (Tokio-actors) |
| シリアライズとデシリアライズ     | データの変換と復元             | ✓    | ❌    | ❌      | ❌    | ❌     | ✓       | ❌       | ❌      | ❌      | ❌     | ❌      | ✓      | ✓ (Serde)             |
| 高可用性クラスタ                 | 障害に強いクラスター構成       | ✓    | ❌    | ✓       | ✓     | ❌     | ✓       | ❌       | ❌      | ❌      | ❌     | ❌      | ✓      | ❌                    |
| バックプレッシャー管理           | 過負荷時のフロー制御           | ✓    | ❌    | ❌      | ❌    | ❌     | ✓       | ❌       | ❌      | ❌      | ❌     | ❌      | ❌     | ✓ (Tokio-stream)      |
| サービスディスカバリー           | 動的なサービス検出             | ✓    | ❌    | ❌      | ❌    | ❌     | ✓       | ❌       | ❌      | ❌      | ❌     | ❌      | ✓      | ✓ (Consul)            |
| クロスノードシャーディング       | ノード間でのデータ分散         | ✓    | ❌    | ❌      | ❌    | ❌     | ✓       | ❌       | ❌      | ❌      | ❌     | ❌      | ❌     | ❌                    |

注意: この表は一般的な比較であり、各クレートの具体的な実装や機能の詳細は異なる場合があります。また、Rust のエコシステムは急速に発展しているため、新しい機能が追加されたり、別のクレートが登場したりする可能性があります。

---

1. ロケーショントランスペアレンス

ロケーショントランスペアレンスは、アクターシステムにおいて非常に重要な概念です。
これにより、アクターの物理的な位置（どのノードやマシンで実行されているか）を隠蔽し、アプリケーションコードがアクターの場所を意識せずにメッセージを送信できるようになります。

詳細:

- アクターの参照（アドレス）は、そのアクターの物理的な位置に関係なく一貫しています。
- システムは自動的にメッセージをルーティングし、適切なノードに転送します。
- アクターの移動や再配置が発生しても、アプリケーションコードを変更する必要がありません。
- スケーラビリティと柔軟性が向上し、システムの拡張が容易になります。

2. トランザクショナルメッセージング

トランザクショナルメッセージングは、複数のメッセージ操作を一つの原子的な単位として扱うことができる機能です。これにより、分散システムにおける一貫性と信頼性が向上します。

詳細:

- 複数のメッセージ送信を一つのトランザクションとしてグループ化できます。
- すべてのメッセージが成功して処理されるか、全てが失敗して元の状態に戻ります（ロールバック）。
- 部分的な更新や不整合な状態を防ぎます。
- 複雑なビジネスロジックや分散トランザクションの実装に役立ちます。

3. 分散データ

分散データ機能は、クラスター内の複数のノードにわたってデータを共有・管理する能力を提供します。これにより、高可用性と一貫性のあるデータ管理が可能になります。

詳細:

- データは複数のノードに複製され、単一障害点を排除します。
- 読み取りと書き込みの一貫性レベルを設定できます（強一貫性、結果整合性など）。
- CRDTs（Conflict-free Replicated Data Types）などの技術を使用して、競合解決を自動化します。
- キャッシュ、セッション状態、設定データなどの管理に適しています。

4. バックプレッシャー管理

バックプレッシャー管理は、システムが過負荷状態に陥らないようにするための重要な機能です。メッセージの生成速度が処理速度を上回った場合に、フロー制御を行います。

詳細:

- 受信側アクターが処理できる以上のメッセージを送信側が生成しないようにします。
- バッファオーバーフローやメモリ不足などの問題を防ぎます。
- リアクティブストリーミングの原則に基づいて、需要主導のメッセージングを実現します。
- システムの安定性と応答性を向上させます。

5. サービスディスカバリー

サービスディスカバリーは、動的な環境でサービス（アクター）の位置を自動的に検出・管理する機能です。クラウドや大規模分散システムで特に重要です。

詳細:

- 新しいサービスが追加されたり、既存のサービスが移動したりした場合に自動的に検出します。
- サービスのヘルスチェックとロードバランシングを提供します。
- 設定の複雑さを軽減し、システムの柔軟性を高めます。
- Kubernetes、Consul、ZooKeeper などの外部システムと統合できます。

6. クロスノードシャーディング

クロスノードシャーディングは、大規模なデータセットや処理を複数のノードに分散する技術です。これにより、システムのスケーラビリティと性能が向上します。

詳細:

- データやアクターを複数のノードに分散させ、負荷を分散します。
- シャーディング戦略（ハッシュベース、範囲ベースなど）を設定できます。
- ノードの追加・削除時に自動的にデータを再分散します。
- クロスシャード操作（複数のシャードにまたがるクエリや更新）をサポートします。
- 大規模なデータ処理や高トラフィックのアプリケーションに適しています。

---

## akka, kompact 比較

以下の理解で大体合ってる？

1. kompact は system との連携を想定。akka は親 actor との連携を想定。
2. kompact は構造体で定義されてるので実装が固定。akka は trait なので開発者による改変についても柔軟。
3. kompact は uninitialised()を用意しているのでもしかしたら空コンテキストで実装できるので、結局アプローチは違うが akka と同じ柔軟性が提供できてる？
4. akka は log フィールドなので、DI ができる柔軟性を提供している
5. 同等機能を提供してるかな
6. 恐らく同等機能を提供していると思われるが、akka の方がより明確に定義しやすそう。
7. 同等
8. akka のみ子アクターの管理機能を提供
9. kompact の方が柔軟性がありそう。ただ akka は恐らく隠蔽しても問題ないほどしっかり検討した内部状態を持っていそう。
10. ブロッキングって、メッセージの終了を待つかまたないかってことだよね。akka は待たないことに拘っていてただ、待つことも想定している設計思想ってことかな。
11. akka は内部的にバッファ管理してるんだ。。。kompact もそうできないのかなぁ。。。やっぱりバッファ管理って煩わされるからしっかりしたバッファ管理を提供できるならしてもらった方がありがたいよね。
12. 同等。

以下のようにすれば akka のアプローチに近づけると思うけどどうかな。kompact の実装を改造して実装できそうかな？

1. ComponentContext の trait 化し uninitialised はなくす
2. log フィールドの用意、もしくは DI 用メソッド trait の用意
3. scheduleOnce や schedule メソッドを trait に用意。命名は変えても良さそう(batch, schedule とか)
4. actorOf メソッド, children フィールドを trait に用意。命名は変えても良さそう。なんで actorOf なんだろう。。。
5. akka の状態遷移を enum で定義。akka の詳細を知ってたら提示して。
6. 基本的にノンブロッキングであることを trait で示せないかな。。そして Future, ask パターンを使用するのも trait 化したい。
7. akka の思想を反映させてバッファ管理を内部隠蔽した実装にしたい。trait の中で実装するイメージで。
8. 自殺機能はそのままでいいのかな。要は kompact は context.suicide() , akka は context.stop(self) で実現してるってことだよね？

いや、上記では本当に trait しか提供できないじゃん。自分としてはより具体的な実装を trait の基本実装でも提供したいんだよ。

1. 他のメソッドも提示して！
2. いいと思う。
3. batch より schedule_once のがわかりやすいってことかな。実装を変えなくてもいいから解説だけして。
4. actorOf の意図を推測して！
5. akka の状態遷移について詳細に漏らさず解説して。その上でもし trait の基本実装で定義できるなら実装してみて。それから enum を簡略化してるんだったら完全に詳細化して！
6. ネーミングは ComponentContext のままにして標準 trait をノンブロッキングにして。それから 既存の実装を利用して、 ask, pipe の基本実装を提供して！
7. 新たに buffer or buffers 用のフィールドを設けて、trait の基本実装の中で with_buffer, init_buffers を実装するようにできない？
8. いいと思う。

いや、フォルダ構成としては以下にしたい。要は aetherflow をルートとした workspace 構成にしたい。

- aetherflow/
  - core/
  - derive/
  - remote/
  - cluster/
  - persistence/
  - streams/
  - examples/
  - tests/
  - benches/
  - docs/
  - scripts/
    - setup_project.sh
  - .github/
    - workflows/
      - rust.yml
  - Cargo.toml
  - CONTRIBUTE.md
  - LICENSE
  - README.md
  - pull_request_template.md
  - rustfmt.toml

---

akka の actor trait の scala 実装を忠実に再現して。省略せず、全て網羅して。

---

````Scala
package akka.actor

import akka.dispatch.{Dispatcher, MessageDispatcher}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.Duration

trait Actor {
  import Actor._

  /**
   * The ActorContext belonging to this actor.
   * Exposes contextual information for the actor and the current message.
   */
  implicit val context: ActorContext

  /**
   * The only abstract method that needs to be implemented by the concrete actor.
   * Defines the behavior of the actor.
   */
  def receive: Receive

  /**
   * User overridable callback.
   * Is called when an Actor is started by invoking 'actor ! Start'.
   */
  def preStart(): Unit = ()

  /**
   * User overridable callback.
   * Is called asynchronously after 'actor.stop()' is invoked.
   */
  def postStop(): Unit = ()

  /**
   * User overridable callback.
   * Is called on a crashed Actor right BEFORE it is restarted to allow clean up of resources before Actor is terminated.
   */
  def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    context.children.foreach(context.stop(_))
    postStop()
  }

  /**
   * User overridable callback.
   * Is called on the crashed Actor when it is restarted by its Supervisor.
   * By default it calls 'preStart()'.
   */
  def postRestart(reason: Throwable): Unit = preStart()

  /**
   * Is called when a message is received.
   * Calls the 'receive' method defined by the user.
   */
  final def apply(msg: Any): Unit = receive.applyOrElse(msg, unhandled)

  /**
   * User overridable callback.
   * Is called when a message wasn't handled by the current behavior of the actor.
   */
  def unhandled(message: Any): Unit = context.system.eventStream.publish(UnhandledMessage(message, context.sender(), self))

  /**
   * The reference to this actor.
   */
  final def self: ActorRef = context.self

  /**
   * The reference to the sender of the last received message.
   * Is defined only in the scope of a message processing.
   * Do NOT keep a reference to it outside of this scope.
   */
  final def sender(): ActorRef = context.sender()

  /**
   * Allows to schedule a message to be sent later.
   */
  final def scheduleOnce(delay: FiniteDuration, target: ActorRef, message: Any)(implicit executor: ExecutionContextExecutor, sender: ActorRef = Actor.noSender): Cancellable =
    context.system.scheduler.scheduleOnce(delay, target, message)

  /**
   * Allows to schedule a message to be sent repeatedly.
   */
  final def schedule(initialDelay: FiniteDuration, interval: FiniteDuration, target: ActorRef, message: Any)(implicit executor: ExecutionContextExecutor, sender: ActorRef = Actor.noSender): Cancellable =
    context.system.scheduler.schedule(initialDelay, interval, target, message)
}

object Actor {
  /**
   * Type alias for the receive method
   */
  type Receive = PartialFunction[Any, Unit]

  /**
   * Marker trait for ActorRefs that support sending messages via ! and !?
   */
  trait ActorRefFactory

  /**
   * Marker trait for ActorRefs that are local and can be watched
   */
  trait LocalRef extends ActorRefFactory

  /**
   * Marker trait for ActorRefs that are remote
   */
  trait RemoteRef extends ActorRefFactory

  /**
   * Marker trait for ActorRefs that are dead
   */
  trait DeadRef extends ActorRefFactory

  /**
   * Defines a special ActorRef that can be used as a placeholder for an actor that doesn't exist
   */
  val noSender: ActorRef = null

  /**
   * Accessor to the system's default dispatcher
   */
  def defaultDispatcher: Dispatcher = null

  /**
   * Accessor to the system's default timeout
   */
  implicit val defaultTimeout: Timeout = Timeout(Duration.create(5, "seconds"))

  /**
   * Accessor to the system's event stream
   */
  val eventStream: akka.event.EventStream = null

  /**
   * Accessor to the system's logging adapter
   */
  val log: LoggingAdapter = null

  /**
   * Accessor to the mailbox type
   */
  type Mailbox = akka.dispatch.Mailbox

  /**
   * Accessor to the message dispatcher
   */
  type MessageDispatcher = akka.dispatch.MessageDispatcher
}
```

---

上記をコメントも含めて全て rust で実装し直して。
また、命名規則は rust の推奨命名規則にしたがって。
もしこのスコープでエラー処理が必要なら thiserrorを利用して実装して。
しかもコメントは rustdoc で OSS として適切なレベルで英語でお願い。
くれぐれも省略はせず、また解説は日本語でお願い。

---

akka の ActorContext trait の scala 実装を忠実に再現して。省略せず、全て網羅して。

---

```scala
package akka.actor

import akka.actor.dungeon.ChildrenContainer
import akka.dispatch.MessageDispatcher
import akka.pattern.{AskSupport, PipeToSupport}
import akka.util.Timeout

import scala.collection.immutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.reflect.ClassTag
import scala.util.Try

trait ActorContext extends ActorRefFactory with AskSupport with PipeToSupport {
  /**
   * The ActorRef representing this actor
   */
  def self: ActorRef

  /**
   * Retrieve the Props which were used to create this actor.
   */
  def props: Props

  /**
   * Actor's current behavior stack
   */
  def behavior: Behavior[Any]

  /**
   * The ActorSystem this actor is part of
   */
  def system: ActorSystem

  /**
   * Retrieve the sender which sent the currently processed message.
   * This reference is valid within the Actor itself, and may be stored to be used later.
   * Can be used inside of Futures, etc.
   */
  def sender(): ActorRef

  /**
   * The supervisor of this actor
   */
  def parent: ActorRef

  /**
   * Children of this actor
   */
  def children: immutable.Iterable[ActorRef]

  /**
   * Get the child with the given name if it exists
   */
  def child(name: String): Option[ActorRef]

  /**
   * Create a new child actor with the given name
   */
  def actorOf(props: Props, name: String = null): ActorRef

  /**
   * Stop the given child actor
   */
  def stop(actor: ActorRef): Unit

  /**
   * Register this actor to be notified of the termination of the given actor (child or not)
   */
  def watch(subject: ActorRef): ActorRef

  /**
   * Unregister this actor from being notified of the termination of the given actor
   */
  def unwatch(subject: ActorRef): ActorRef

  /**
   * Become a new behavior: replace only the top of the behavior stack
   */
  def become(behavior: Receive, discardOld: Boolean = true): Unit

  /**
   * Revert to the previous behavior (pop the behavior stack)
   */
  def unbecome(): Unit

  /**
   * Returns the dispatcher associated with this actor
   */
  implicit def dispatcher: ExecutionContextExecutor

  /**
   * Returns the default timeout for the actor system
   */
  implicit def timeout: Timeout

  /**
   * Schedule a message to be sent repeatedly to the given target actor with the given
   * initial delay and frequency.
   */
  def schedule(initialDelay: FiniteDuration, frequency: FiniteDuration, receiver: ActorRef, message: Any): Cancellable

  /**
   * Schedule a message to be sent once to the given target actor after the given delay.
   */
  def scheduleOnce(delay: FiniteDuration, receiver: ActorRef, message: Any): Cancellable

  /**
   * Set the restart strategy for the supervised child actors
   */
  def setRestartStrategy(strategy: SupervisorStrategy): Unit

  /**
   * Get the current restart strategy
   */
  def supervisorStrategy: SupervisorStrategy

  /**
   * Set the receive timeout for this actor
   */
  def setReceiveTimeout(timeout: Duration): Unit

  /**
   * Cancel the current receive timeout, if one is set
   */
  def cancelReceiveTimeout(): Unit

  /**
   * Get the current receive timeout
   */
  def receiveTimeout: Duration

  /**
   * Returns true if this actor will be restarted if it stops
   */
  def willBeRestarted: Boolean

  /**
   * Returns the path of this actor
   */
  def path: ActorPath

  /**
   * Returns the current mailbox size (approximate)
   */
  def mailboxSize: Int

  /**
   * Set the MDC (Mapped Diagnostic Context) for this actor
   */
  def setMdc(mdc: Map[String, String]): Unit

  /**
   * Clear the MDC (Mapped Diagnostic Context) for this actor
   */
  def clearMdc(): Unit

  /**
   * Get the current MDC (Mapped Diagnostic Context) for this actor
   */
  def mdc: Map[String, String]

  /**
   * Get a child selection with the given path elements
   */
  def actorSelection(path: String): ActorSelection

  /**
   * Get a child selection with the given path elements
   */
  def actorSelection(path: Iterable[String]): ActorSelection

  /**
   * Create a new actor as a child of this context and wrap it with a supervisor
   */
  def supervise(props: Props): ActorRef

  /**
   * Returns true if the actor is currently processing a message
   */
  def isProcessingMessage: Boolean

  /**
   * Get the current message (if any) being processed by the actor
   */
  def currentMessage: Option[Any]

  /**
   * Returns the ClassTag of the actor class
   */
  def classTag: ClassTag[_]

  /**
   * Returns the current behavior stack depth
   */
  def behaviorStackDepth: Int

  /**
   * Get a snapshot of the actor's internal state
   */
  def getState: Any

  /**
   * Set the actor's internal state
   */
  def setState(state: Any): Unit

  /**
   * Get the actor's StashBuffer
   */
  def stash: StashBuffer

  /**
   * Check if the given message is stashed
   */
  def isStashed(message: Any): Boolean

  /**
   * Unstash a single message (if any) and process it
   */
  def unstashOne(): Unit

  /**
   * Unstash all messages and process them
   */
  def unstashAll(): Unit

  /**
   * Get the number of messages currently stashed
   */
  def numStashed: Int

  /**
   * Check if this actor has any stashed messages
   */
  def hasStash: Boolean

  /**
   * Clear all stashed messages
   */
  def clearStash(): Unit

  /**
   * Get the actor's ChildrenContainer
   */
  def childrenContainer: ChildrenContainer

  /**
   * Check if the given child exists
   */
  def hasChild(child: ActorRef): Boolean

  /**
   * Get the number of children
   */
  def numChildren: Int

  /**
   * Retrieve metadata about this actor context
   */
  def getMetadata: Map[String, Any]

  /**
   * Set metadata for this actor context
   */
  def setMetadata(key: String, value: Any): Unit

  /**
   * Remove metadata for this actor context
   */
  def removeMetadata(key: String): Unit

  /**
   * Get the current receive function
   */
  def receive: PartialFunction[Any, Unit]

  /**
   * Execute the given function in the actor's thread and return the result
   */
  def executeInActorThread[T](f: => T): Try[T]

  /**
   * Get the actor's current message queue
   */
  def mailbox: MessageQueue

  /**
   * Check if this actor is currently suspended
   */
  def isSuspended: Boolean

  /**
   * Suspend this actor (stop processing messages)
   */
  def suspend(): Unit

  /**
   * Resume this actor (start processing messages again)
   */
  def resume(): Unit

  /**
   * Get the actor's current dispatcher
   */
  def getDispatcher: MessageDispatcher

  /**
   * Set a new dispatcher for this actor
   */
  def setDispatcher(dispatcher: MessageDispatcher): Unit

  /**
   * Check if this actor is currently shutting down
   */
  def isShuttingDown: Boolean

  /**
   * Start the shutdown process for this actor
   */
  def shutdown(): Unit

  /**
   * Get the actor's current incarnation (number of times it has been restarted)
   */
  def incarnation: Long

  /**
   * Get the actor's unique ID
   */
  def uid: Int

  /**
   * Get the actor's current lifecycle state
   */
  def lifecycleState: LifecycleState
}
```

---

上記をコメントも含めて全て rust で実装し直して。
ActorContext は Context に変更して。
それから、命名規則は rust の推奨命名規則にしたがって。
しかもコメントは rustdoc で英語でお願い。
くれぐれも省略はせず、また解説は日本語でお願い。
````
