use std::{
    fmt::Debug,
    future::Future,
    num::NonZeroUsize,
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use shelterwood::{
    ActorRef, ExitError, ExitResult, Guard, Intensity, KeyedCapacity, MonitorEvent,
    MonitorEventKind, MonitorMemberKind, RawActor, RawContext, RawDef, RawOnceDef, ScopeRef,
    SubtreeOnceDef, TaskDef, TaskRef, Tree, WatchTarget,
};
use shelterwood_test_support::{ReleaseGate, poll_until};

fn assert_copy_eq_debug_send_sync<T: Copy + Eq + Debug + Send + Sync>() {}
fn assert_clone_eq_debug_send_sync<T: Clone + Eq + Debug + Send + Sync>() {}

#[test]
fn milestone_eight_public_data_types_keep_their_trait_contracts() {
    assert_copy_eq_debug_send_sync::<KeyedCapacity>();
    assert_copy_eq_debug_send_sync::<MonitorMemberKind>();
    assert_clone_eq_debug_send_sync::<MonitorEvent>();
    assert_clone_eq_debug_send_sync::<MonitorEventKind>();
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct KeyedMessage {
    key: &'static str,
    value: usize,
}

struct KeyedActor {
    start: ReleaseGate,
    received: Arc<Mutex<Vec<KeyedMessage>>>,
}

impl RawActor for KeyedActor {
    type Msg = KeyedMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.start.wait().await;
        while let Some(message) = context.recv().await {
            self.received
                .lock()
                .expect("keyed receive log mutex poisoned")
                .push(message);
        }
        Ok(())
    }
}

#[tokio::test]
async fn latest_by_key_replaces_in_place_and_evicts_the_oldest_distinct_key() {
    let start = ReleaseGate::default();
    let received = Arc::new(Mutex::new(Vec::new()));
    let key_calls = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "keyed",
            RawOnceDef::new(KeyedActor {
                start: start.clone(),
                received: Arc::clone(&received),
            })
            .latest_by_key(
                KeyedCapacity::Explicit(NonZeroUsize::new(2).expect("non-zero capacity")),
                {
                    let key_calls = Arc::clone(&key_calls);
                    move |message: &KeyedMessage| {
                        key_calls.fetch_add(1, Ordering::SeqCst);
                        assert_ne!(message.key, "panic", "key panic remains on sender");
                        message.key
                    }
                },
            ),
        )
        .expect("valid keyed actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("keyed actor starts");

    actor
        .try_send(KeyedMessage { key: "a", value: 1 })
        .expect("first key accepted");
    actor
        .try_send(KeyedMessage { key: "b", value: 2 })
        .expect("second key accepted");
    actor
        .try_send(KeyedMessage { key: "a", value: 3 })
        .expect("same key replaces without refreshing age");
    actor
        .try_send(KeyedMessage { key: "c", value: 4 })
        .expect("new key evicts instead of backpressuring");
    assert_eq!(key_calls.load(Ordering::SeqCst), 4);

    let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = actor.try_send(KeyedMessage {
            key: "panic",
            value: 5,
        });
    }));
    assert!(panic.is_err());
    actor
        .try_send(KeyedMessage { key: "d", value: 6 })
        .expect("extractor panic did not poison the mailbox");

    start.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            received
                .lock()
                .expect("keyed receive log mutex poisoned")
                .as_slice()
                == [
                    KeyedMessage { key: "c", value: 4 },
                    KeyedMessage { key: "d", value: 6 },
                ]
        })
        .await
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("keyed actor stops");
}

#[tokio::test]
async fn split_definition_keeps_key_panics_on_the_parked_sender() {
    let mut tree = Tree::new();
    let slot = tree
        .reserve_actor::<KeyedMessage>("split-keyed")
        .expect("keyed slot reserved");
    let actor = slot.actor_ref();
    let mut send = Box::pin(actor.send(KeyedMessage {
        key: "panic",
        value: 1,
    }));
    let first_poll = std::future::poll_fn(|context| Poll::Ready(send.as_mut().poll(context))).await;
    assert!(first_poll.is_pending(), "undefined actor parks the send");

    let _defined = slot.define_once_raw(
        RawOnceDef::new(KeyedActor {
            start: ReleaseGate::default(),
            received: Arc::new(Mutex::new(Vec::new())),
        })
        .latest_by_key(KeyedCapacity::Inherit, |message: &KeyedMessage| {
            assert_ne!(message.key, "panic", "key panic remains on sender");
            message.key
        }),
    );

    let sender = tokio::spawn(send);
    let panic = sender
        .await
        .expect_err("key extraction panics on sender task");
    assert!(panic.is_panic());
}

enum TargetMessage {
    Crash,
    Stop,
}

struct TargetActor;

impl RawActor for TargetActor {
    type Msg = TargetMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        match context.recv().await {
            Some(TargetMessage::Crash) => Err(ExitError::message("restart target")),
            Some(TargetMessage::Stop) | None => Ok(()),
        }
    }
}

enum WatchMessage {
    Event(u8, MonitorEvent),
    ReregisterActor,
    UnwatchTask,
    CancelScope,
    WatchTerminalTask,
    WatchTerminalScope,
}

struct WatchAllActor {
    actor: ActorRef<TargetMessage>,
    task: TaskRef,
    scope: ScopeRef,
    events: Arc<Mutex<Vec<(u8, MonitorEvent)>>>,
    reregistered: Arc<AtomicUsize>,
    task_unwatched: Arc<AtomicUsize>,
    scope_cancelled: Arc<AtomicUsize>,
}

impl RawActor for WatchAllActor {
    type Msg = WatchMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        context
            .watch(&self.actor, |event| WatchMessage::Event(1, event))
            .expect("actor watch accepted");
        context
            .watch(&self.task, |event| WatchMessage::Event(1, event))
            .expect("task watch accepted");
        let mut scope_guard: Option<Guard> = Some(
            context
                .watch_scoped(&self.scope, |event| WatchMessage::Event(1, event))
                .expect("scope watch accepted"),
        );
        while let Some(message) = context.recv().await {
            match message {
                WatchMessage::Event(generation, event) => self
                    .events
                    .lock()
                    .expect("monitor event log mutex poisoned")
                    .push((generation, event)),
                WatchMessage::ReregisterActor => {
                    context
                        .watch(&self.actor, |event| WatchMessage::Event(2, event))
                        .expect("actor watch replacement accepted");
                    self.reregistered.store(1, Ordering::SeqCst);
                }
                WatchMessage::UnwatchTask => {
                    let removed = context.unwatch(&self.task);
                    self.task_unwatched
                        .store(if removed { 2 } else { 1 }, Ordering::SeqCst);
                }
                WatchMessage::CancelScope => {
                    drop(scope_guard.take());
                    self.scope_cancelled.store(1, Ordering::SeqCst);
                }
                WatchMessage::WatchTerminalTask => {
                    context
                        .watch(&self.task, |event| WatchMessage::Event(3, event))
                        .expect("terminal task watch accepted");
                }
                WatchMessage::WatchTerminalScope => {
                    context
                        .watch(&self.scope, |event| WatchMessage::Event(4, event))
                        .expect("terminal scope watch accepted");
                }
            }
        }
        Ok(())
    }
}

fn assert_watch_target<T: WatchTarget>() {}

#[tokio::test]
async fn watches_cover_all_handle_kinds_replace_cancel_and_report_terminal_targets() {
    assert_watch_target::<ActorRef<TargetMessage>>();
    assert_watch_target::<TaskRef>();
    assert_watch_target::<ScopeRef>();

    let task_release = ReleaseGate::default();
    let mut nested = Tree::new();
    nested
        .add_task(
            "hold",
            TaskDef::new(|context| async move {
                context.shutdown_token().cancelled().await;
                Ok(())
            }),
        )
        .expect("valid nested hold task");

    let events = Arc::new(Mutex::new(Vec::new()));
    let reregistered = Arc::new(AtomicUsize::new(0));
    let task_unwatched = Arc::new(AtomicUsize::new(0));
    let scope_cancelled = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let target = tree
        .add_raw("target", RawDef::factory(|| TargetActor))
        .expect("valid target actor");
    let task = tree
        .add_task(
            "task",
            TaskDef::new({
                let task_release = task_release.clone();
                move |_| {
                    let task_release = task_release.clone();
                    async move {
                        task_release.wait().await;
                        Ok(())
                    }
                }
            }),
        )
        .expect("valid task target");
    let scope = tree
        .add_subtree_once("scope", SubtreeOnceDef::new(nested))
        .expect("valid scope target");
    let watcher = tree
        .add_raw_once(
            "watcher",
            RawOnceDef::new(WatchAllActor {
                actor: target.clone(),
                task: task.clone(),
                scope: scope.clone(),
                events: Arc::clone(&events),
                reregistered: Arc::clone(&reregistered),
                task_unwatched: Arc::clone(&task_unwatched),
                scope_cancelled: Arc::clone(&scope_cancelled),
            }),
        )
        .expect("valid watcher actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("watch tree starts");

    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            events
                .lock()
                .expect("monitor event log mutex poisoned")
                .len()
                >= 3
        })
        .await
    );
    let first = events
        .lock()
        .expect("monitor event log mutex poisoned")
        .clone();
    for (kind, membership) in [
        (MonitorMemberKind::Actor, target.membership()),
        (MonitorMemberKind::Task, task.membership()),
        (MonitorMemberKind::Scope, scope.membership()),
    ] {
        assert!(first.iter().any(|(generation, event)| {
            *generation == 1
                && event.member_kind == kind
                && event.membership == membership
                && matches!(event.kind, MonitorEventKind::Started { .. })
        }));
    }
    let first_actor = first
        .iter()
        .find_map(|(_, event)| match event.kind {
            MonitorEventKind::Started { incarnation }
                if event.membership == target.membership() =>
            {
                Some(incarnation)
            }
            _ => None,
        })
        .expect("initial actor edge exists");
    events
        .lock()
        .expect("monitor event log mutex poisoned")
        .clear();

    watcher
        .send(WatchMessage::ReregisterActor)
        .await
        .expect("watch replacement command accepted");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            reregistered.load(Ordering::SeqCst) == 1
        })
        .await
    );
    target
        .send(TargetMessage::Crash)
        .await
        .expect("crash command accepted");
    let second_actor = target
        .next_incarnation(first_actor, Duration::from_secs(1))
        .await
        .expect("target restarts");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            events
                .lock()
                .expect("monitor event log mutex poisoned")
                .iter()
                .filter(|(generation, event)| {
                    *generation == 2 && event.membership == target.membership()
                })
                .count()
                == 2
        })
        .await
    );
    let actor_edges: Vec<_> = events
        .lock()
        .expect("monitor event log mutex poisoned")
        .iter()
        .filter(|(generation, event)| *generation == 2 && event.membership == target.membership())
        .map(|(_, event)| event.kind.clone())
        .collect();
    assert!(matches!(
        &actor_edges[0],
        MonitorEventKind::Exited { incarnation, .. } if *incarnation == first_actor
    ));
    assert_eq!(
        actor_edges[1],
        MonitorEventKind::Started {
            incarnation: second_actor,
        }
    );

    watcher
        .send(WatchMessage::UnwatchTask)
        .await
        .expect("unwatch command accepted");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            task_unwatched.load(Ordering::SeqCst) != 0
        })
        .await
    );
    assert_eq!(task_unwatched.load(Ordering::SeqCst), 2);
    task_release.release();
    let _ = task.wait().await;
    watcher
        .send(WatchMessage::WatchTerminalTask)
        .await
        .expect("terminal-task watch command accepted");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            events
                .lock()
                .expect("monitor event log mutex poisoned")
                .iter()
                .any(|(generation, event)| {
                    *generation == 3
                        && event.membership == task.membership()
                        && matches!(event.kind, MonitorEventKind::Removed { .. })
                })
        })
        .await
    );
    assert!(
        !events
            .lock()
            .expect("monitor event log mutex poisoned")
            .iter()
            .any(|(generation, event)| {
                *generation == 1
                    && event.membership == task.membership()
                    && !matches!(event.kind, MonitorEventKind::Started { .. })
            })
    );

    watcher
        .send(WatchMessage::CancelScope)
        .await
        .expect("scope-cancel command accepted");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            scope_cancelled.load(Ordering::SeqCst) == 1
        })
        .await
    );
    scope
        .shutdown_and_wait(Duration::from_secs(1))
        .await
        .expect("nested scope stops");
    watcher
        .send(WatchMessage::WatchTerminalScope)
        .await
        .expect("terminal-scope watch command accepted");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            events
                .lock()
                .expect("monitor event log mutex poisoned")
                .iter()
                .any(|(generation, event)| {
                    *generation == 4
                        && event.membership == scope.membership()
                        && matches!(event.kind, MonitorEventKind::Removed { .. })
                })
        })
        .await
    );
    assert!(
        !events
            .lock()
            .expect("monitor event log mutex poisoned")
            .iter()
            .any(|(generation, event)| {
                *generation == 1
                    && event.membership == scope.membership()
                    && !matches!(event.kind, MonitorEventKind::Started { .. })
            })
    );

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("watch tree stops");
}

struct PreSpawnWatcher {
    target: ActorRef<TargetMessage>,
    events: Arc<Mutex<Vec<MonitorEvent>>>,
}

impl RawActor for PreSpawnWatcher {
    type Msg = MonitorEvent;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        context
            .watch(&self.target, std::convert::identity)
            .expect("pre-spawn watch accepted");
        while let Some(event) = context.recv().await {
            self.events
                .lock()
                .expect("pre-spawn event log mutex poisoned")
                .push(event);
        }
        Ok(())
    }
}

#[tokio::test]
async fn pre_spawn_watch_waits_for_the_first_real_started_edge() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let watcher_slot = tree
        .reserve_actor::<MonitorEvent>("watcher")
        .expect("watcher slot reserved first");
    let target = tree
        .add_raw("target", RawDef::factory(|| TargetActor))
        .expect("target is declared after watcher");
    let _watcher = watcher_slot.define_once_raw(RawOnceDef::new(PreSpawnWatcher {
        target: target.clone(),
        events: Arc::clone(&events),
    }));
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("pre-spawn tree starts");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            events
                .lock()
                .expect("pre-spawn event log mutex poisoned")
                .len()
                == 1
        })
        .await
    );
    {
        let events = events.lock().expect("pre-spawn event log mutex poisoned");
        assert_eq!(events[0].membership, target.membership());
        assert!(matches!(events[0].kind, MonitorEventKind::Started { .. }));
    }
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("pre-spawn tree stops");
}

struct BlockedWatcher {
    target: ActorRef<TargetMessage>,
    registered: ReleaseGate,
    deliver: ReleaseGate,
    events: Arc<Mutex<Vec<MonitorEvent>>>,
}

impl RawActor for BlockedWatcher {
    type Msg = MonitorEvent;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        context
            .watch(&self.target, std::convert::identity)
            .expect("overflow watch accepted");
        self.registered.release();
        self.deliver.wait().await;
        while let Some(event) = context.recv().await {
            self.events
                .lock()
                .expect("overflow event log mutex poisoned")
                .push(event);
        }
        Ok(())
    }
}

#[tokio::test]
async fn monitor_overflow_coalesces_lag_and_never_drops_terminal_removed() {
    const RESTARTS: usize = 65;
    let registered = ReleaseGate::default();
    let deliver = ReleaseGate::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    tree.intensity(
        Intensity::new(100, Duration::from_secs(3600)).expect("valid restart intensity"),
    );
    let target = tree
        .add_raw("target", RawDef::factory(|| TargetActor))
        .expect("valid overflow target");
    tree.add_raw_once(
        "watcher",
        RawOnceDef::new(BlockedWatcher {
            target: target.clone(),
            registered: registered.clone(),
            deliver: deliver.clone(),
            events: Arc::clone(&events),
        }),
    )
    .expect("valid blocked watcher");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("overflow tree starts");
    registered.wait().await;

    let mut incarnation = target
        .try_send(TargetMessage::Crash)
        .expect("first crash accepted");
    for _ in 1..RESTARTS {
        incarnation = target
            .next_incarnation(incarnation, Duration::from_secs(1))
            .await
            .expect("next overflow incarnation starts");
        target
            .try_send(TargetMessage::Crash)
            .expect("next crash accepted");
    }
    incarnation = target
        .next_incarnation(incarnation, Duration::from_secs(1))
        .await
        .expect("final overflow incarnation starts");
    assert_eq!(
        target
            .try_send(TargetMessage::Stop)
            .expect("terminal completion accepted"),
        incarnation
    );
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            system
                .scope()
                .child("target")
                .is_some_and(|child| child.incarnation.is_none())
        })
        .await
    );

    deliver.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            events
                .lock()
                .expect("overflow event log mutex poisoned")
                .last()
                .is_some_and(|event| matches!(event.kind, MonitorEventKind::Removed { .. }))
        })
        .await
    );
    {
        let events = events.lock().expect("overflow event log mutex poisoned");
        assert_eq!(
            events.first().map(|event| &event.kind),
            Some(&MonitorEventKind::Lagged { dropped: 5 })
        );
        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(MonitorEventKind::Removed {
                last_incarnation: Some(last),
            }) if *last == incarnation
        ));
    }
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("overflow tree stops");
}
