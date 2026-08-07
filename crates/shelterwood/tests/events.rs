use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use shelterwood::{Actor, ActorOnceDef, Context, ExitError, ExitResult, Tree};
use shelterwood_test_support::ReleaseGate;

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
}

impl Actor for IntervalActor {
    type Msg = IntervalMessage;
    type Args = Arc<AtomicUsize>;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { ticks: args })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            IntervalMessage::Arm => context
                .set_interval("interval", IntervalMessage::Tick, Duration::from_secs(10))
                .expect("interval accepted"),
            IntervalMessage::Tick => {
                if self.ticks.fetch_add(1, Ordering::SeqCst) == 1 {
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
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "interval",
            ActorOnceDef::<IntervalActor>::new(Arc::clone(&ticks)),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.send(IntervalMessage::Arm).await.expect("actor live");
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_secs(35)).await;
    tokio::task::yield_now().await;
    assert_eq!(ticks.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(9)).await;
    tokio::task::yield_now().await;
    assert_eq!(ticks.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(ticks.load(Ordering::SeqCst), 2);
}
