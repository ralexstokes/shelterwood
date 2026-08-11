use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{ReleaseGate, assert_quiet};
use shelterwood::{Actor, ActorOnceDef, Context, ExitError, ExitResult, Tree};

#[derive(Clone, Copy, Debug)]
enum PriorityMessage {
    Start,
    FirstContinuation,
    SecondContinuation,
    External,
}

struct PriorityActor {
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for PriorityActor {
    type Msg = PriorityMessage;
    type Args = (ReleaseGate, Arc<Mutex<Vec<&'static str>>>);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        args.0.wait().await;
        Ok(Self { log: args.1 })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        let entry = match message {
            PriorityMessage::Start => {
                context
                    .continue_with(PriorityMessage::FirstContinuation)
                    .expect("live continuation accepted");
                context
                    .continue_with(PriorityMessage::SecondContinuation)
                    .expect("live continuation accepted");
                "start"
            }
            PriorityMessage::FirstContinuation => "continuation-1",
            PriorityMessage::SecondContinuation => {
                context.stop();
                "continuation-2"
            }
            PriorityMessage::External => "external",
        };
        self.log.lock().expect("log mutex poisoned").push(entry);
        Ok(())
    }
}

#[tokio::test]
async fn continuations_are_fifo_and_yield_one_turn_to_ready_external_input() {
    let gate = ReleaseGate::default();
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "priority",
            ActorOnceDef::<PriorityActor>::new((gate.clone(), Arc::clone(&log))),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");

    actor
        .send(PriorityMessage::Start)
        .await
        .expect("mailbox accepts during init");
    actor
        .send(PriorityMessage::External)
        .await
        .expect("mailbox accepts during init");
    gate.release();
    system.wait_started().await.expect("actor initializes");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(
        *log.lock().expect("log mutex poisoned"),
        ["start", "continuation-1", "external", "continuation-2"]
    );
}

#[derive(Clone, Copy, Debug)]
enum BatchMessage {
    Start,
    FirstContinuation,
    SecondContinuation,
    Mailbox,
    Offload,
    Timer,
}

struct BatchTraceActor {
    fire_timer: bool,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for BatchTraceActor {
    type Msg = BatchMessage;
    type Args = (ReleaseGate, bool, Arc<Mutex<Vec<&'static str>>>);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        args.0.wait().await;
        Ok(Self {
            fire_timer: args.1,
            log: args.2,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        let entry = match message {
            BatchMessage::Start => {
                context
                    .continue_with(BatchMessage::FirstContinuation)
                    .expect("first continuation accepted");
                context
                    .continue_with(BatchMessage::SecondContinuation)
                    .expect("second continuation accepted");
                context
                    .offload(
                        async {},
                        |result| {
                            assert!(result.is_err(), "zero-budget work reports its deadline");
                            BatchMessage::Offload
                        },
                        Duration::ZERO,
                    )
                    .expect("offload accepted");
                if self.fire_timer {
                    context
                        .set_timeout("batch", BatchMessage::Timer, Duration::ZERO)
                        .expect("timer accepted");
                }
                "start"
            }
            BatchMessage::FirstContinuation => "continuation-1",
            BatchMessage::SecondContinuation => "continuation-2",
            BatchMessage::Mailbox => "mailbox",
            BatchMessage::Offload => {
                if !self.fire_timer {
                    context.stop();
                }
                "offload"
            }
            BatchMessage::Timer => {
                context.stop();
                "timer"
            }
        };
        self.log.lock().expect("log mutex poisoned").push(entry);
        Ok(())
    }
}

fn assert_subsequence(trace: &[&str], expected: &[&str]) {
    let mut cursor = 0;
    for entry in trace {
        if expected.get(cursor).is_some_and(|next| next == entry) {
            cursor += 1;
        }
    }
    assert_eq!(
        cursor,
        expected.len(),
        "missing ordered subsequence {expected:?} in {trace:?}"
    );
}

fn assert_exact_entries(trace: &[&str], expected: &[&str]) {
    assert_eq!(trace.len(), expected.len());
    for entry in expected {
        assert_eq!(
            trace.iter().filter(|observed| observed == &entry).count(),
            1,
            "expected one {entry:?} entry in {trace:?}"
        );
    }
}

async fn assert_batch_trace(fire_timer: bool) {
    let gate = ReleaseGate::default();
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "batch-trace",
            ActorOnceDef::<BatchTraceActor>::new((gate.clone(), fire_timer, Arc::clone(&log))),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    actor
        .send(BatchMessage::Start)
        .await
        .expect("start accepted during init");
    actor
        .send(BatchMessage::Mailbox)
        .await
        .expect("mailbox message accepted during init");
    gate.release();

    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    let trace = log.lock().expect("log mutex poisoned");
    let expected = if fire_timer {
        &[
            "start",
            "continuation-1",
            "continuation-2",
            "mailbox",
            "offload",
            "timer",
        ][..]
    } else {
        &[
            "start",
            "continuation-1",
            "continuation-2",
            "mailbox",
            "offload",
        ][..]
    };
    assert_exact_entries(&trace, expected);
    // SPEC preserves FIFO within each source and the causal start edge, but
    // intentionally does not order mailbox delivery against offload delivery.
    assert_subsequence(&trace, &["start", "continuation-1", "continuation-2"]);
    assert_subsequence(&trace, &["start", "mailbox"]);
    assert_subsequence(&trace, &["start", "offload"]);
    if fire_timer {
        assert_subsequence(&trace, &["start", "timer"]);
    }
}

#[tokio::test]
async fn steady_state_uses_the_shared_bounded_batch_trace() {
    assert_batch_trace(false).await;
}

#[tokio::test]
async fn due_timer_promotes_the_steady_batch_without_changing_source_priority() {
    assert_batch_trace(true).await;
}

#[derive(Clone, Copy, Debug)]
enum LiveContinuationMessage {
    Start,
    FirstOffload,
    SecondOffload,
    Continuation,
}

struct LiveContinuationActor {
    handled: usize,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for LiveContinuationActor {
    type Msg = LiveContinuationMessage;
    type Args = Arc<Mutex<Vec<&'static str>>>;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            handled: 0,
            log: args,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        let entry = match message {
            LiveContinuationMessage::Start => {
                context
                    .offload(
                        async {},
                        |_| LiveContinuationMessage::FirstOffload,
                        Duration::ZERO,
                    )
                    .expect("first completion accepted");
                context
                    .offload(
                        async {},
                        |_| LiveContinuationMessage::SecondOffload,
                        Duration::ZERO,
                    )
                    .expect("second completion accepted");
                "start"
            }
            LiveContinuationMessage::FirstOffload => {
                context
                    .continue_with(LiveContinuationMessage::Continuation)
                    .expect("continuation accepted");
                "offload-1"
            }
            LiveContinuationMessage::SecondOffload => "offload-2",
            LiveContinuationMessage::Continuation => "continuation",
        };
        self.handled += 1;
        if self.handled == 4 {
            context.stop();
        }
        self.log.lock().expect("log mutex poisoned").push(entry);
        Ok(())
    }
}

#[tokio::test]
async fn continuation_queued_by_external_handler_leads_remaining_steady_batch_work() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "live-continuation",
            ActorOnceDef::<LiveContinuationActor>::new(Arc::clone(&log)),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor
        .send(LiveContinuationMessage::Start)
        .await
        .expect("actor accepts start");

    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    let trace = log.lock().expect("log mutex poisoned");
    assert_exact_entries(&trace, &["start", "offload-1", "offload-2", "continuation"]);
    assert_subsequence(&trace, &["start", "offload-1", "offload-2"]);
    assert_subsequence(&trace, &["offload-1", "continuation"]);
}

#[derive(Clone, Copy, Debug)]
enum MidContinuationExternalMessage {
    Start,
    FirstContinuation,
    SecondContinuation,
    External,
}

struct MidContinuationExternalActor {
    handled: usize,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for MidContinuationExternalActor {
    type Msg = MidContinuationExternalMessage;
    type Args = Arc<Mutex<Vec<&'static str>>>;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            handled: 0,
            log: args,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        let entry = match message {
            MidContinuationExternalMessage::Start => {
                context
                    .continue_with(MidContinuationExternalMessage::FirstContinuation)
                    .expect("first continuation accepted");
                context
                    .continue_with(MidContinuationExternalMessage::SecondContinuation)
                    .expect("second continuation accepted");
                "start"
            }
            MidContinuationExternalMessage::FirstContinuation => {
                context
                    .myself()
                    .try_send(MidContinuationExternalMessage::External)
                    .expect("external message accepted during continuation");
                "continuation-1"
            }
            MidContinuationExternalMessage::SecondContinuation => "continuation-2",
            MidContinuationExternalMessage::External => "external",
        };
        self.handled += 1;
        if self.handled == 4 {
            context.stop();
        }
        self.log.lock().expect("log mutex poisoned").push(entry);
        Ok(())
    }
}

#[tokio::test]
async fn external_work_added_during_a_continuation_gets_the_fairness_turn() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "mid-continuation-external",
            ActorOnceDef::<MidContinuationExternalActor>::new(Arc::clone(&log)),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor
        .send(MidContinuationExternalMessage::Start)
        .await
        .expect("actor accepts start");

    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(
        *log.lock().expect("log mutex poisoned"),
        ["start", "continuation-1", "external", "continuation-2"]
    );
}

#[derive(Clone, Copy, Debug)]
enum NestedTimerMessage {
    Start,
    First,
    Second,
    AddedDuringBatch,
}

struct NestedTimerActor {
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for NestedTimerActor {
    type Msg = NestedTimerMessage;
    type Args = Arc<Mutex<Vec<&'static str>>>;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { log: args })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        let entry = match message {
            NestedTimerMessage::Start => {
                context
                    .set_timeout("first", NestedTimerMessage::First, Duration::ZERO)
                    .expect("first timer accepted");
                context
                    .set_timeout("second", NestedTimerMessage::Second, Duration::ZERO)
                    .expect("second timer accepted");
                "start"
            }
            NestedTimerMessage::First => {
                context
                    .set_timeout(
                        "added",
                        NestedTimerMessage::AddedDuringBatch,
                        Duration::ZERO,
                    )
                    .expect("later timer accepted");
                "first"
            }
            NestedTimerMessage::Second => "second",
            NestedTimerMessage::AddedDuringBatch => {
                context.stop();
                "added"
            }
        };
        self.log.lock().expect("log mutex poisoned").push(entry);
        Ok(())
    }
}

#[tokio::test]
async fn a_newly_due_timer_cannot_replace_remaining_fired_batch_armings() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "nested-timer",
            ActorOnceDef::<NestedTimerActor>::new(Arc::clone(&log)),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor
        .send(NestedTimerMessage::Start)
        .await
        .expect("actor accepts start");

    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(
        *log.lock().expect("log mutex poisoned"),
        ["start", "first", "second", "added"]
    );
}

#[derive(Clone, Copy, Debug)]
enum RetractionMessage {
    Arm,
    Cancel,
    Fired,
}

struct RetractionActor {
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for RetractionActor {
    type Msg = RetractionMessage;
    type Args = (ReleaseGate, Arc<Mutex<Vec<&'static str>>>);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        args.0.wait().await;
        Ok(Self { log: args.1 })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            RetractionMessage::Arm => {
                self.log.lock().expect("log mutex poisoned").push("arm");
                context
                    .set_timeout("deadline", RetractionMessage::Fired, Duration::ZERO)
                    .expect("zero timeout arms normally");
            }
            RetractionMessage::Cancel => {
                self.log.lock().expect("log mutex poisoned").push("cancel");
                assert_eq!(context.clear_timer(&"deadline"), Ok(true));
                context.stop();
            }
            RetractionMessage::Fired => {
                self.log.lock().expect("log mutex poisoned").push("fired");
                context.stop();
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn mailbox_work_queued_at_fire_can_retract_an_elapsed_timer() {
    let gate = ReleaseGate::default();
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "retraction",
            ActorOnceDef::<RetractionActor>::new((gate.clone(), Arc::clone(&log))),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    actor
        .send(RetractionMessage::Arm)
        .await
        .expect("arm accepted");
    actor
        .send(RetractionMessage::Cancel)
        .await
        .expect("cancel accepted");
    gate.release();
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(*log.lock().expect("log mutex poisoned"), ["arm", "cancel"]);
}

#[derive(Clone, Copy, Debug)]
enum BoundMessage {
    Arm,
    BeforeFire,
    Timer,
    AddedAfterFire,
}

struct BoundedTimerActor {
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for BoundedTimerActor {
    type Msg = BoundMessage;
    type Args = Arc<Mutex<Vec<&'static str>>>;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { log: args })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        let entry = match message {
            BoundMessage::Arm => {
                context
                    .set_timeout("timer", BoundMessage::Timer, Duration::ZERO)
                    .expect("timer accepted");
                context
                    .continue_with(BoundMessage::BeforeFire)
                    .expect("continuation accepted");
                "arm"
            }
            BoundMessage::BeforeFire => {
                context
                    .continue_with(BoundMessage::AddedAfterFire)
                    .expect("continuation accepted");
                "before-fire"
            }
            BoundMessage::Timer => "timer",
            BoundMessage::AddedAfterFire => {
                context.stop();
                "added-after-fire"
            }
        };
        self.log.lock().expect("log mutex poisoned").push(entry);
        Ok(())
    }
}

#[tokio::test]
async fn work_created_during_the_retraction_turn_does_not_preempt_fired_timer() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "bounded",
            ActorOnceDef::<BoundedTimerActor>::new(Arc::clone(&log)),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.send(BoundMessage::Arm).await.expect("actor live");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(
        *log.lock().expect("log mutex poisoned"),
        ["arm", "before-fire", "timer", "added-after-fire"]
    );
}

#[derive(Clone, Copy, Debug)]
enum TimerBatchFairnessMessage {
    Arm,
    DuringBatch,
    SeedCompletion,
    Timer,
    AfterBatch,
    DuringBatchCompletion,
}

struct TimerBatchFairnessActor {
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for TimerBatchFairnessActor {
    type Msg = TimerBatchFairnessMessage;
    type Args = (ReleaseGate, Arc<Mutex<Vec<&'static str>>>);

    async fn init(args: Self::Args, context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        context
            .offload(
                async {},
                |_| TimerBatchFairnessMessage::SeedCompletion,
                Duration::ZERO,
            )
            .expect("seed completion accepted");
        args.0.wait().await;
        Ok(Self { log: args.1 })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        let entry = match message {
            TimerBatchFairnessMessage::Arm => {
                context
                    .set_timeout("timer", TimerBatchFairnessMessage::Timer, Duration::ZERO)
                    .expect("timer accepted");
                "arm"
            }
            TimerBatchFairnessMessage::DuringBatch => {
                context
                    .offload(
                        async {},
                        |_| TimerBatchFairnessMessage::DuringBatchCompletion,
                        Duration::ZERO,
                    )
                    .expect("completion accepted during the timer batch");
                context
                    .myself()
                    .try_send(TimerBatchFairnessMessage::AfterBatch)
                    .expect("post-snapshot mailbox input accepted");
                "during-batch"
            }
            TimerBatchFairnessMessage::SeedCompletion => "seed-completion",
            TimerBatchFairnessMessage::Timer => "timer",
            TimerBatchFairnessMessage::AfterBatch => "after-batch",
            TimerBatchFairnessMessage::DuringBatchCompletion => {
                context.stop();
                "during-batch-completion"
            }
        };
        self.log.lock().expect("log mutex poisoned").push(entry);
        Ok(())
    }
}

/// A fired timer batch supersedes the steady-state completion credit captured
/// by the mailbox delivery that armed it. Completions created during the batch
/// therefore cannot use that stale credit to jump post-snapshot mailbox input.
#[tokio::test]
async fn timer_batch_resets_steady_state_completion_fairness() {
    let gate = ReleaseGate::default();
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "timer-batch-fairness",
            ActorOnceDef::<TimerBatchFairnessActor>::new((gate.clone(), Arc::clone(&log))),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    actor
        .send(TimerBatchFairnessMessage::Arm)
        .await
        .expect("arm accepted during init");
    actor
        .send(TimerBatchFairnessMessage::DuringBatch)
        .await
        .expect("batch work accepted during init");
    gate.release();

    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(
        *log.lock().expect("log mutex poisoned"),
        [
            "arm",
            "during-batch",
            "seed-completion",
            "timer",
            "after-batch",
            "during-batch-completion",
        ]
    );
}

#[derive(Clone, Copy, Debug)]
enum KeyedMessage {
    Arm,
    FirstType,
    SecondType,
    ReplacedFirst,
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct FirstKey(u8);

#[derive(Debug, Eq, Hash, PartialEq)]
struct SecondKey(u8);

struct KeyedActor {
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for KeyedActor {
    type Msg = KeyedMessage;
    type Args = Arc<Mutex<Vec<&'static str>>>;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { log: args })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            KeyedMessage::Arm => {
                context
                    .set_timeout(FirstKey(1), KeyedMessage::FirstType, Duration::ZERO)
                    .expect("first key accepted");
                context
                    .set_timeout(SecondKey(1), KeyedMessage::SecondType, Duration::ZERO)
                    .expect("distinct key type accepted");
                context
                    .set_timeout(FirstKey(1), KeyedMessage::ReplacedFirst, Duration::ZERO)
                    .expect("same key replaces");
            }
            KeyedMessage::FirstType => panic!("replaced timer must not fire"),
            KeyedMessage::SecondType => self
                .log
                .lock()
                .expect("log mutex poisoned")
                .push("second-type"),
            KeyedMessage::ReplacedFirst => {
                self.log
                    .lock()
                    .expect("log mutex poisoned")
                    .push("replaced-first");
                context.stop();
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn timer_keys_are_heterogeneous_and_rearming_takes_a_new_order_position() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once("keyed", ActorOnceDef::<KeyedActor>::new(Arc::clone(&log)))
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.send(KeyedMessage::Arm).await.expect("actor live");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(
        *log.lock().expect("log mutex poisoned"),
        ["second-type", "replaced-first"]
    );
}

#[derive(Clone, Copy, Debug)]
enum IntervalMessage {
    Arm,
    Tick,
}

struct IntervalActor {
    ticks: Arc<AtomicUsize>,
    armed: ReleaseGate,
    ticked: ReleaseGate,
}

impl Actor for IntervalActor {
    type Msg = IntervalMessage;
    type Args = (Arc<AtomicUsize>, ReleaseGate, ReleaseGate);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            ticks: args.0,
            armed: args.1,
            ticked: args.2,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            IntervalMessage::Arm => {
                context
                    .set_interval("interval", IntervalMessage::Tick, Duration::from_secs(10))
                    .expect("interval accepted");
                self.armed.release();
            }
            IntervalMessage::Tick => {
                let prior = self.ticks.fetch_add(1, Ordering::SeqCst);
                self.ticked.release();
                if prior == 1 {
                    assert_eq!(context.clear_timer(&"interval"), Ok(true));
                    context.stop();
                }
            }
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn intervals_start_after_one_period_and_skip_missed_ticks() {
    let ticks = Arc::new(AtomicUsize::new(0));
    let armed = ReleaseGate::default();
    let ticked = ReleaseGate::default();
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "interval",
            ActorOnceDef::<IntervalActor>::new((Arc::clone(&ticks), armed.clone(), ticked.clone())),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.send(IntervalMessage::Arm).await.expect("actor live");
    armed.wait().await;

    tokio::time::advance(Duration::from_secs(35)).await;
    ticked.wait().await;
    assert_eq!(ticks.load(Ordering::SeqCst), 1);
    assert_quiet(Duration::from_secs(9), || ticks.load(Ordering::SeqCst) != 1).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(ticks.load(Ordering::SeqCst), 2);
}

#[derive(Clone, Copy, Debug)]
enum ClearedIntervalMessage {
    Arm,
    Tick,
    Probe,
}

struct ClearedIntervalActor {
    ticks: Arc<AtomicUsize>,
    armed: ReleaseGate,
}

impl Actor for ClearedIntervalActor {
    type Msg = ClearedIntervalMessage;
    type Args = (Arc<AtomicUsize>, ReleaseGate);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            ticks: args.0,
            armed: args.1,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            ClearedIntervalMessage::Arm => {
                assert_eq!(
                    context.clear_timer(&"interval"),
                    Ok(false),
                    "a never-armed key has nothing to retract"
                );
                context
                    .set_interval(
                        "interval",
                        ClearedIntervalMessage::Tick,
                        Duration::from_secs(10),
                    )
                    .expect("interval accepted");
                self.armed.release();
            }
            ClearedIntervalMessage::Tick => {
                self.ticks.fetch_add(1, Ordering::SeqCst);
                context
                    .set_interval("interval", ClearedIntervalMessage::Tick, Duration::ZERO)
                    .expect("a zero period clears the key");
                assert_eq!(
                    context.clear_timer(&"interval"),
                    Ok(false),
                    "the zero-period arm already cleared the key"
                );
                // The probe fires long after every deadline the cleared
                // interval would have had, so a surviving interval must
                // deliver again before the probe stops the actor.
                context
                    .set_timeout(
                        "probe",
                        ClearedIntervalMessage::Probe,
                        Duration::from_secs(120),
                    )
                    .expect("probe timeout accepted");
            }
            ClearedIntervalMessage::Probe => context.stop(),
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn a_zero_period_interval_arming_clears_the_key_and_stops_ticks() {
    let ticks = Arc::new(AtomicUsize::new(0));
    let armed = ReleaseGate::default();
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "cleared-interval",
            ActorOnceDef::<ClearedIntervalActor>::new((Arc::clone(&ticks), armed.clone())),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor
        .send(ClearedIntervalMessage::Arm)
        .await
        .expect("actor live");
    armed.wait().await;

    tokio::time::advance(Duration::from_secs(10)).await;
    // A surviving interval would tick again inside this window, strictly
    // before the probe deadline arrives.
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::time::advance(Duration::from_secs(100)).await;
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    // The complete post-stop history is exact: one tick, then silence.
    assert_eq!(
        ticks.load(Ordering::SeqCst),
        1,
        "a cleared interval must never tick again"
    );
}

#[derive(Clone)]
enum OverflowMessage {
    Fired,
}

struct OverflowTimerActor {
    fired: Arc<AtomicUsize>,
    interval: bool,
}

impl Actor for OverflowTimerActor {
    type Msg = OverflowMessage;
    type Args = (Arc<AtomicUsize>, bool);

    async fn init(args: Self::Args, context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        let (fired, interval) = args;
        if interval {
            context
                .set_interval("never", OverflowMessage::Fired, Duration::MAX)
                .expect("live actor arms its interval");
        } else {
            context
                .set_timeout("never", OverflowMessage::Fired, Duration::MAX)
                .expect("live actor arms its timeout");
        }
        Ok(Self { fired, interval })
    }

    async fn handle(
        &mut self,
        OverflowMessage::Fired: Self::Msg,
        _: &mut Context<'_, Self>,
    ) -> ExitResult {
        self.fired.fetch_add(1, Ordering::SeqCst);
        let _ = self.interval;
        Ok(())
    }
}

/// A delay too large for the clock is a deadline that never arrives — not an
/// immediate fire (and for intervals, not an immediate-fire loop).
#[tokio::test(start_paused = true)]
async fn overflowing_timer_deadlines_never_fire() {
    for interval in [false, true] {
        let fired = Arc::new(AtomicUsize::new(0));
        let mut tree = Tree::new();
        tree.add_actor_once(
            "overflow",
            ActorOnceDef::<OverflowTimerActor>::new((Arc::clone(&fired), interval)),
        )
        .expect("valid actor");
        let system = tree.spawn().expect("runtime is available");
        system.wait_started().await.expect("actor starts");
        assert_quiet(Duration::from_secs(1), || fired.load(Ordering::SeqCst) != 0).await;
        system
            .shutdown(Duration::from_secs(1))
            .await
            .expect("tree shuts down");
        // The complete post-shutdown history seals the negative: the
        // overflowed deadline never fired at all.
        assert_eq!(
            fired.load(Ordering::SeqCst),
            0,
            "an overflowed timer deadline must never fire"
        );
    }
}
