//! Multi-core thread-per-core scheduler.
//!
//! [`System`] spins up as many core threads as requested, pins each thread to a core,
//! and runs the assigned actors to completion. Actors never migrate between cores (static placement).
//!
//! To host heterogeneous actors (differing `A::Message`) on the same core, they are stored
//! type-erased ([`ErasedActor`]). Sending stays typed via [`ActorRef`].

use crate::{mpsc, pinning, Actor, SendError, TrySendError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as ctrl; // control plane (main -> core): low-frequency, so a std channel is plenty
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

thread_local! {
    /// The logical core number if this thread is a core thread; None for non-core threads (main, etc.).
    static CURRENT_CORE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// The core number the current thread is running (None on a non-core thread).
pub(crate) fn current_core() -> Option<usize> {
    CURRENT_CORE.with(|c| c.get())
}

/// How a core thread behaves when there are no messages (a latency <-> CPU/tail-robustness tradeoff).
#[derive(Clone, Copy, Debug, Default)]
pub enum IdleStrategy {
    /// Always busy-spin. **Lowest latency, maximum CPU.** For isolated cores on dedicated bare metal.
    /// Under virtualization/oversubscription, a preempt mid-spin can blow up the tail.
    /// Latency-first; this is the default.
    #[default]
    BusySpin,
    /// Yield progressively: spin `spins` times -> `yield_now` `yields` times -> then sleep by `park`.
    /// Sacrifices a little median in exchange for lower CPU and a tamer tail under virtualization. For shared environments.
    Backoff {
        spins: u32,
        yields: u32,
        park: Duration,
    },
}

/// Default for the maximum number of messages processed consecutively per actor visit.
/// Amortizes the overhead of the outer scheduler loop (control-channel checks, etc.) to raise throughput.
const DEFAULT_BATCH_DRAIN: usize = 128;

/// Tuning knobs for the core scheduler. Defaults lean toward low latency and high throughput.
///
/// For simple cases `System::with_cores(n)` is enough; opt in only when you want to tune
/// (progressive disclosure):
///
/// ```
/// use aetherflow::{IdleStrategy, SchedulingPolicy, System};
///
/// // For shared environments: yielding idle + a smaller, fairer batch
/// let policy = SchedulingPolicy::default()
///     .idle(IdleStrategy::backoff())
///     .batch_drain(8);
/// let sys = System::with_policy(1, policy);
/// ```
#[derive(Clone, Copy, Debug)]
pub struct SchedulingPolicy {
    idle: IdleStrategy,
    batch_drain: usize,
}

impl Default for SchedulingPolicy {
    fn default() -> Self {
        SchedulingPolicy {
            idle: IdleStrategy::default(),
            batch_drain: DEFAULT_BATCH_DRAIN,
        }
    }
}

impl SchedulingPolicy {
    /// How a core thread behaves when there are no messages.
    pub fn idle(mut self, idle: IdleStrategy) -> Self {
        self.idle = idle;
        self
    }

    /// Maximum number of messages processed consecutively per actor visit (batch drain).
    ///
    /// Larger values amortize the outer loop's overhead (control-channel checks, walking the
    /// actor list), raising throughput. On the other hand a single actor can occupy the core for
    /// up to this many messages, so other actors on the same core may see worse latency ──
    /// **a throughput <-> fairness tradeoff**.
    ///
    /// `1` = "one message per visit," the fairest. The default is `128` (throughput-leaning).
    /// With only one actor per core, fairness is a non-issue, so a large value is fine.
    ///
    /// # Panics
    /// When `n == 0` (it would process no messages and make no progress).
    pub fn batch_drain(mut self, n: usize) -> Self {
        assert!(n >= 1, "batch_drain must be >= 1 (0 would make no progress)");
        self.batch_drain = n;
        self
    }
}

impl IdleStrategy {
    /// A safe, sensible backoff for shared/virtualized environments.
    pub fn backoff() -> Self {
        IdleStrategy::Backoff {
            spins: 128,
            yields: 128,
            park: Duration::from_micros(50),
        }
    }

    #[inline]
    fn idle(&self, count: u32) {
        match *self {
            IdleStrategy::BusySpin => std::hint::spin_loop(),
            IdleStrategy::Backoff {
                spins,
                yields,
                park,
            } => {
                if count < spins {
                    std::hint::spin_loop();
                } else if count < spins + yields {
                    std::thread::yield_now();
                } else {
                    std::thread::sleep(park);
                }
            }
        }
    }
}


/// The result of one polling pass.
enum PollOutcome {
    /// Processed one message.
    Worked,
    /// The mailbox was empty.
    Empty,
    /// `handle` panicked (this actor is detached).
    Panicked,
}

/// The result of a lifecycle hook (on_start / on_stop / restart).
/// A return value so that hook panics too stay confined to the actor boundary and don't take down the core thread.
enum LifecycleOutcome {
    Ok,
    Panicked,
}

/// A type-erased actor cell. The core thread drives actors through this trait.
trait ErasedActor: Send {
    fn start(&mut self) -> LifecycleOutcome;
    fn poll_one(&mut self) -> PollOutcome;
    /// All sending ends have dropped and the mailbox is empty (= no more work is coming).
    fn closed_and_empty(&mut self) -> bool;
    fn stop(&mut self) -> LifecycleOutcome;
}

/// Restart policy for a supervised actor. Unlimited restarts can become a runaway loop, so the default is capped.
#[derive(Clone, Copy, Debug)]
pub enum RestartPolicy {
    /// Never restart (on panic, stop and detach).
    Never,
    /// Restart up to `max_restarts` times within the `within` window. Stop once exceeded.
    Limited { max_restarts: u32, within: Duration },
    /// Restart without limit (beware runaway restart loops).
    Always,
}

impl RestartPolicy {
    /// The default for `.supervised()`. Conservative (5 times / 10 seconds) so it doesn't run wild on a BusySpin core.
    pub fn default_supervised() -> Self {
        RestartPolicy::Limited {
            max_restarts: 5,
            within: Duration::from_secs(10),
        }
    }
}

/// Restart-count tracker (fixed window + reset on elapse). A single panic after a long healthy run won't stop it.
#[derive(Default)]
struct RestartState {
    window_start: Option<Instant>,
    count: u32,
}

struct Cell<A: Actor> {
    actor: A,
    rx: mpsc::Receiver<A::Message>,
    /// `Some` means supervised: on panic, rebuild via the factory and continue (restart). `None` means stop.
    factory: Option<Box<dyn Fn() -> A + Send>>,
    /// Restart policy (only meaningful when supervised).
    restart_policy: RestartPolicy,
    /// Restart-count tracking (for the policy's cap check).
    restarts: RestartState,
    /// `Some` means instrumented: records handle durations (opt-in; default None = zero cost).
    metrics: Option<std::sync::Arc<crate::metrics::LatencyHistogram>>,
}

impl<A: Actor> Cell<A> {
    /// Records this panic (= a restart request) and returns whether the policy permits a restart.
    /// A failed rebuild (a factory / on_restart panic) also counts as one.
    fn allow_restart(&mut self) -> bool {
        match self.restart_policy {
            RestartPolicy::Never => false,
            RestartPolicy::Always => true,
            RestartPolicy::Limited { max_restarts, within } => {
                let now = Instant::now();
                match self.restarts.window_start {
                    Some(start) if now.duration_since(start) <= within => {
                        self.restarts.count += 1;
                    }
                    // Outside the window (or the first time) -> reset. A single panic after a long healthy run won't wrongly stop it.
                    _ => {
                        self.restarts.window_start = Some(now);
                        self.restarts.count = 1;
                    }
                }
                self.restarts.count <= max_restarts
            }
        }
    }
}

impl<A: Actor> ErasedActor for Cell<A> {
    fn start(&mut self) -> LifecycleOutcome {
        // Catch and contain on_start panics too (otherwise the whole core thread dies).
        let actor = &mut self.actor;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| actor.on_start())) {
            Ok(()) => LifecycleOutcome::Ok,
            Err(_) => LifecycleOutcome::Panicked,
        }
    }
    fn poll_one(&mut self) -> PollOutcome {
        match self.rx.try_recv() {
            Some(msg) => {
                // Measure only when instrumented (the hot cost of Instant is opt-in).
                let started = self.metrics.as_ref().map(|_| std::time::Instant::now());
                // Wrap handle in panic catching. Actor state is singly owned (the type guarantees isolation),
                // so a panic cannot corrupt any other actor's state.
                let actor = &mut self.actor;
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| actor.handle(msg)));
                if let (Some(m), Some(t0)) = (&self.metrics, started) {
                    m.record(t0.elapsed().as_nanos() as u64);
                }
                match result {
                    Ok(()) => PollOutcome::Worked,
                    // If supervised and the restart policy permits, restart; otherwise detach.
                    // Restart is sound thanks to isolation too (broken state is shared with no one and left behind nowhere).
                    // The offending message is already consumed = no poison loop. The factory/on_restart are also within the panic boundary.
                    Err(_) => {
                        if self.factory.is_some() && self.allow_restart() {
                            let make = self.factory.as_ref().unwrap();
                            let rebuilt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                                || {
                                    let mut a = make();
                                    a.on_restart();
                                    a
                                },
                            ));
                            match rebuilt {
                                Ok(a) => {
                                    self.actor = a;
                                    PollOutcome::Worked
                                }
                                // The factory or on_restart panicked -> give up and detach (already counted).
                                Err(_) => PollOutcome::Panicked,
                            }
                        } else {
                            // Not supervised, or the restart cap was reached -> stop and detach.
                            PollOutcome::Panicked
                        }
                    }
                }
            }
            None => PollOutcome::Empty,
        }
    }
    fn closed_and_empty(&mut self) -> bool {
        !self.rx.producers_alive() && self.rx.is_empty()
    }
    fn stop(&mut self) -> LifecycleOutcome {
        // Contain on_stop panics too.
        let actor = &mut self.actor;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| actor.on_stop())) {
            Ok(()) => LifecycleOutcome::Ok,
            Err(_) => LifecycleOutcome::Panicked,
        }
    }
}

enum Control {
    Add(Box<dyn ErasedActor>),
    Shutdown,
}

struct CoreHandle {
    control: ctrl::Sender<Control>,
    join: Option<thread::JoinHandle<()>>,
}

/// A multi-core actor runtime.
pub struct System {
    cores: Vec<CoreHandle>,
    next: AtomicUsize, // round-robin placement cursor
}

impl System {
    /// Spin up `n` core threads (busy-spin by default; lowest latency).
    pub fn with_cores(n: usize) -> System {
        System::with_cores_idle(n, IdleStrategy::default())
    }

    /// Spin up core threads with a specified idle strategy (other knobs stay at defaults).
    pub fn with_cores_idle(n: usize, idle: IdleStrategy) -> System {
        System::with_policy(n, SchedulingPolicy::default().idle(idle))
    }

    /// Spin up core threads with a specified scheduling policy
    /// (best-effort pinning each thread to logical cores 0..n).
    pub fn with_policy(n: usize, policy: SchedulingPolicy) -> System {
        assert!(n >= 1, "System needs at least 1 core");
        let mut cores = Vec::with_capacity(n);
        for core_index in 0..n {
            let (control, rx) = ctrl::channel::<Control>();
            let join = thread::Builder::new()
                .name(format!("aether-core-{core_index}"))
                .spawn(move || core_loop(core_index, rx, policy))
                .expect("spawn core thread");
            cores.push(CoreHandle {
                control,
                join: Some(join),
            });
        }
        System {
            cores,
            next: AtomicUsize::new(0),
        }
    }

    /// The number of cores.
    pub fn num_cores(&self) -> usize {
        self.cores.len()
    }

    /// Place an actor on the given core and return a typed send handle (default mailbox capacity).
    pub fn spawn_on<A: Actor>(&self, core: usize, actor: A) -> ActorRef<A> {
        self.spawn_on_with(core, actor, DEFAULT_MAILBOX_CAPACITY)
    }

    /// Place an actor on the given core (with a specified mailbox capacity).
    pub fn spawn_on_with<A: Actor>(
        &self,
        core: usize,
        actor: A,
        mailbox_capacity: usize,
    ) -> ActorRef<A> {
        self.install(core, mailbox_capacity, actor, None, RestartPolicy::Never, None)
    }

    /// Place an actor, choosing a core round-robin.
    pub fn spawn<A: Actor>(&self, actor: A) -> ActorRef<A> {
        let core = self.next.fetch_add(1, Ordering::Relaxed) % self.cores.len();
        self.spawn_on(core, actor)
    }

    /// Place a **supervised** actor on the given core. `make` is a factory used for both the initial
    /// instance and the **rebuild** on panic. On panic the mailbox is preserved and a fresh actor keeps
    /// processing the remaining messages (restart). Restart is sound because state is singly owned
    /// (the type guarantees isolation) and broken state is shared with no one and left behind nowhere.
    /// The offending message is already consumed, so there is no poison loop.
    pub fn spawn_on_supervised<A: Actor>(
        &self,
        core: usize,
        make: impl Fn() -> A + Send + 'static,
    ) -> ActorRef<A> {
        self.spawn_on_supervised_with(core, make, DEFAULT_MAILBOX_CAPACITY)
    }

    /// Supervised placement (with a specified mailbox capacity).
    pub fn spawn_on_supervised_with<A: Actor>(
        &self,
        core: usize,
        make: impl Fn() -> A + Send + 'static,
        mailbox_capacity: usize,
    ) -> ActorRef<A> {
        let actor = make();
        self.install(
            core,
            mailbox_capacity,
            actor,
            Some(Box::new(make)),
            RestartPolicy::default_supervised(),
            None,
        )
    }

    /// A builder for specifying the advanced knobs together (core / mailbox capacity / supervised / instrumented).
    /// For simple cases `spawn_on` and friends are enough ── opt in only for advanced tuning (progressive disclosure).
    ///
    /// Example: `sys.build(|| MyActor::new()).core(0).mailbox(4096).supervised().instrumented().spawn()`
    pub fn build<A: Actor, F: Fn() -> A + Send + 'static>(&self, make: F) -> SpawnBuilder<'_, A, F> {
        SpawnBuilder {
            sys: self,
            make,
            core: None,
            mailbox: DEFAULT_MAILBOX_CAPACITY,
            supervised: false,
            restart_policy: None,
            instrumented: false,
        }
    }

    /// The shared internal routine for installing an actor onto a core.
    fn install<A: Actor>(
        &self,
        core: usize,
        mailbox: usize,
        actor: A,
        factory: Option<Box<dyn Fn() -> A + Send>>,
        restart_policy: RestartPolicy,
        metrics: Option<std::sync::Arc<crate::metrics::LatencyHistogram>>,
    ) -> ActorRef<A> {
        assert!(core < self.cores.len(), "core index {core} out of range");
        let (tx, rx) = mpsc::channel::<A::Message>(mailbox);
        let cell: Box<dyn ErasedActor> = Box::new(Cell {
            actor,
            rx,
            factory,
            restart_policy,
            restarts: RestartState::default(),
            metrics: metrics.clone(),
        });
        self.cores[core]
            .control
            .send(Control::Add(cell))
            .unwrap_or_else(|_| panic!("core {core} thread is not running"));
        ActorRef { tx, metrics, core }
    }

    /// Stop and join all cores (equivalent to dropping the system; provided for explicitness).
    pub fn shutdown(self) {
        // drop(self) runs the Drop below.
    }
}

impl Drop for System {
    fn drop(&mut self) {
        for c in &self.cores {
            let _ = c.control.send(Control::Shutdown);
        }
        for c in &mut self.cores {
            if let Some(j) = c.join.take() {
                let _ = j.join();
            }
        }
    }
}

/// The body of a core thread. A run-to-completion loop that alternates between control messages and the actors.
fn core_loop(core_index: usize, control: ctrl::Receiver<Control>, policy: SchedulingPolicy) {
    pinning::pin_current_thread_to(core_index); // best-effort (a no-op on macOS)
    CURRENT_CORE.with(|c| c.set(Some(core_index))); // record this thread's assigned core (for the deadlock guard)

    let mut actors: Vec<Box<dyn ErasedActor>> = Vec::new();
    let mut shutting_down = false;
    let mut idle_count: u32 = 0;

    loop {
        // 1) Handle the control plane (actor additions / shutdown requests)
        let mut ctrl_activity = false;
        loop {
            match control.try_recv() {
                Ok(Control::Add(mut cell)) => {
                    // If on_start panics, drop just this actor (don't take the core down with it).
                    match cell.start() {
                        LifecycleOutcome::Ok => actors.push(cell),
                        LifecycleOutcome::Panicked => drop(cell),
                    }
                    ctrl_activity = true;
                }
                Ok(Control::Shutdown) => shutting_down = true,
                Err(ctrl::TryRecvError::Empty) => break,
                Err(ctrl::TryRecvError::Disconnected) => {
                    // The System is already dropped (a safety net for when even Shutdown doesn't arrive)
                    shutting_down = true;
                    break;
                }
            }
        }

        // 2) Schedule the actors for one pass
        let mut did_work = false;
        let mut i = 0;
        while i < actors.len() {
            match actors[i].poll_one() {
                PollOutcome::Worked => {
                    did_work = true;
                    // Batch drain: keep processing the same actor for up to batch_drain messages,
                    // amortizing the outer loop's overhead (control-channel try_recv, etc.) to 1/batch.
                    // One message is already processed, so batch_drain-1 remain (= no extra drain when 1).
                    // Panic isolation is preserved (a panic within the batch detaches as usual).
                    let mut panicked = false;
                    for _ in 1..policy.batch_drain {
                        match actors[i].poll_one() {
                            PollOutcome::Worked => {}
                            PollOutcome::Empty => break,
                            PollOutcome::Panicked => {
                                panicked = true;
                                break;
                            }
                        }
                    }
                    if panicked {
                        drop(actors.swap_remove(i)); // keep i (the tail is moved into place)
                    } else {
                        i += 1;
                    }
                }
                PollOutcome::Panicked => {
                    // Detach the panicked actor. For broken state, just drop it without calling on_stop.
                    // Thanks to type isolation it doesn't propagate to other actors / the core = safe to continue.
                    drop(actors.swap_remove(i));
                    did_work = true; // treated as progress (with i kept, we look at the swapped-in tail next)
                }
                PollOutcome::Empty => {
                    if actors[i].closed_and_empty() {
                        // All send ends dropped and empty -> stop and remove (on_stop is already panic-bounded)
                        let mut cell = actors.swap_remove(i);
                        let _ = cell.stop();
                        // swap_remove moves the tail into i, so keep i
                    } else {
                        i += 1;
                    }
                }
            }
        }

        // 3) If a shutdown was requested, drain and stop the rest, then break out of the loop
        if shutting_down {
            for mut cell in actors.drain(..) {
                // Don't call on_stop on an actor that panicked during the drain (consistent with the normal path).
                let mut panicked = false;
                loop {
                    match cell.poll_one() {
                        PollOutcome::Worked => {}
                        PollOutcome::Empty => break,
                        PollOutcome::Panicked => {
                            panicked = true;
                            break;
                        }
                    }
                }
                if !panicked {
                    let _ = cell.stop();
                }
            }
            break;
        }

        // 4) Yield per the idle strategy (reset the counter if there was work)
        if did_work || ctrl_activity {
            idle_count = 0;
        } else {
            policy.idle.idle(idle_count);
            idle_count = idle_count.saturating_add(1);
        }
    }
}

/// A send handle to an actor's mailbox (the MPSC producer). `Clone`-able = multiple senders OK.
///
/// `try_send` takes `A::Message` **by value**, so a use-after-send is a compile error.
pub struct ActorRef<A: Actor> {
    tx: mpsc::Sender<A::Message>,
    /// Some only for an instrumented spawn. Shares the processing-latency histogram.
    metrics: Option<std::sync::Arc<crate::metrics::LatencyHistogram>>,
    /// The core this actor lives on (for the deadlock guard).
    core: usize,
}

impl<A: Actor> ActorRef<A> {
    /// Whether a blocking call on this handle would self-block the calling core thread
    /// (= the situation where blocking from within a handler to a same-core actor deadlocks).
    pub(crate) fn would_deadlock_calling_core(&self) -> bool {
        current_core() == Some(self.core)
    }
}

impl<A: Actor> ActorRef<A> {
    /// Send a message by move. If full, `Err(TrySendError::Full)` (transient backpressure);
    /// if the receiving actor is gone, `Err(TrySendError::Closed)` (permanently unsendable). Both return the original message.
    pub fn try_send(&self, msg: A::Message) -> Result<(), TrySendError<A::Message>> {
        self.tx.try_send(msg)
    }

    /// Spin while full and always deliver.
    ///
    /// **Note**: calling `send_blocking` from within a handler to **another same-core actor** can deadlock
    /// (the sender keeps occupying this core thread, so the receiving actor can't drain).
    /// Use it for injecting from outside the runtime (the main thread, etc.). Within a handler use `try_send`
    /// and treat full as backpressure.
    pub fn send_blocking(&self, msg: A::Message) -> Result<(), SendError<A::Message>> {
        // Deadlock guard: blocking from within a handler to a same-core actor makes this core
        // thread block on itself and hang forever. This is a deterministic design mistake on the
        // caller's side, so prefer a clear panic over a silent hang (distinct from the Closed of a gone actor).
        assert!(
            !self.would_deadlock_calling_core(),
            "send_blocking from within a handler to a same-core actor would deadlock; use try_send"
        );
        let mut item = msg;
        loop {
            match self.tx.try_send(item) {
                Ok(()) => return Ok(()),
                // Full = transient backpressure. Wait until there's room.
                Err(TrySendError::Full(returned)) => {
                    item = returned;
                    std::hint::spin_loop();
                }
                // The receiving actor is gone = permanently unsendable. Instead of spinning forever, return the
                // original message and leave it to the caller (reroute/persist/log). Don't propagate an actor
                // failure as a panic into the sending thread.
                Err(TrySendError::Closed(returned)) => return Err(SendError::Closed(returned)),
            }
        }
    }

    /// The current mailbox backlog (approximate). For backpressure monitoring.
    ///
    /// **Zero added cost on the hot path**: just reads the enqueue/dequeue counters the single-consumer
    /// lock-free mailbox already keeps. In contrast to shared/work-stealing designs that require contended
    /// atomics (a consequence of the model).
    pub fn mailbox_depth(&self) -> usize {
        self.tx.depth()
    }
    /// Total number of messages sent to this actor.
    pub fn total_sent(&self) -> usize {
        self.tx.total_enqueued()
    }
    /// Total number of messages this actor has taken from the mailbox and processed (approximate).
    pub fn total_processed(&self) -> usize {
        self.tx.total_dequeued()
    }

    /// A snapshot of processing latency. `Some` only when spawned with `instrumented()`.
    pub fn latency(&self) -> Option<crate::metrics::LatencySnapshot> {
        self.metrics.as_ref().map(|m| m.snapshot())
    }
}

impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        ActorRef {
            tx: self.tx.clone(),
            metrics: self.metrics.clone(),
            core: self.core,
        }
    }
}

/// The spawn builder returned by [`System::build`]. Layer on advanced knobs opt-in (defaults are equivalent to a plain spawn).
pub struct SpawnBuilder<'s, A: Actor, F: Fn() -> A + Send + 'static> {
    sys: &'s System,
    make: F,
    core: Option<usize>,
    mailbox: usize,
    supervised: bool,
    restart_policy: Option<RestartPolicy>,
    instrumented: bool,
}

impl<'s, A: Actor, F: Fn() -> A + Send + 'static> SpawnBuilder<'s, A, F> {
    /// Pin the placement core (round-robin if unspecified).
    pub fn core(mut self, core: usize) -> Self {
        self.core = Some(core);
        self
    }
    /// Specify the mailbox capacity.
    pub fn mailbox(mut self, capacity: usize) -> Self {
        self.mailbox = capacity;
        self
    }
    /// Make it supervised (on panic, rebuild via the factory and continue = restart).
    /// The restart policy defaults to `RestartPolicy::default_supervised()` (stop after 5 times / 10 seconds).
    pub fn supervised(mut self) -> Self {
        self.supervised = true;
        self
    }
    /// Set the restart policy explicitly (overrides the `supervised()` default; calling it implies supervised).
    pub fn restart_policy(mut self, policy: RestartPolicy) -> Self {
        self.supervised = true;
        self.restart_policy = Some(policy);
        self
    }
    /// Enable the processing-latency histogram (readable via `ActorRef::latency()`; incurs the hot cost of Instant).
    pub fn instrumented(mut self) -> Self {
        self.instrumented = true;
        self
    }
    /// Install and return the send handle.
    pub fn spawn(self) -> ActorRef<A> {
        let core = self
            .core
            .unwrap_or_else(|| self.sys.next.fetch_add(1, Ordering::Relaxed) % self.sys.cores.len());
        let metrics = if self.instrumented {
            Some(std::sync::Arc::new(crate::metrics::LatencyHistogram::new()))
        } else {
            None
        };
        let actor = (self.make)();
        let (factory, restart_policy): (Option<Box<dyn Fn() -> A + Send>>, RestartPolicy) =
            if self.supervised {
                (
                    Some(Box::new(self.make)),
                    self.restart_policy
                        .unwrap_or_else(RestartPolicy::default_supervised),
                )
            } else {
                (None, RestartPolicy::Never)
            };
        self.sys
            .install(core, self.mailbox, actor, factory, restart_policy, metrics)
    }
}
