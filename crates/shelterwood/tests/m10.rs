use std::{
    fmt::Debug,
    future::Future,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use shelterwood::{
    Actor, ActorKind, ActorOnceDef, ActorStats, ActorStatsSnapshot, Backoff, ChildObservation,
    ChildObserver, Context, DynamicTree, ExitError, ExitResult, KeyedCapacity, RawActor,
    RawContext, RawDef, RawOnceDef, RecursiveActorStats, RestartCondition, RestartCount,
    RestartCounter, RestartPolicy, ScopeState, StopContext, SubtreeOnceDef, TaskDef, Tree,
};
use shelterwood_test_support::{ReleaseGate, poll_until};

fn assert_clone_eq_debug_send_sync<T: Clone + Eq + Debug + Send + Sync>() {}
fn assert_copy_eq_debug_send_sync<T: Copy + Eq + Debug + Send + Sync>() {}
fn assert_send<T: Send>() {}

#[test]
fn milestone_ten_public_types_keep_their_trait_contracts() {
    assert_clone_eq_debug_send_sync::<ActorStats>();
    assert_clone_eq_debug_send_sync::<ActorStatsSnapshot>();
    assert_copy_eq_debug_send_sync::<ActorKind>();
    assert_clone_eq_debug_send_sync::<RecursiveActorStats>();
    assert_clone_eq_debug_send_sync::<ChildObservation>();
    assert_copy_eq_debug_send_sync::<RestartCount>();
    assert_send::<ChildObserver>();
    assert_send::<RestartCounter>();
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SizedMessage {
    key: &'static str,
    body: &'static str,
}

struct ParkedSizedActor;

impl RawActor for ParkedSizedActor {
    type Msg = SizedMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        context.shutdown_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn actor_stats_count_replacement_eviction_bytes_and_rejection_exactly() {
    let measurements = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "sized",
            RawOnceDef::new(ParkedSizedActor)
                .latest_by_key(
                    KeyedCapacity::Explicit(NonZeroUsize::new(2).expect("non-zero capacity")),
                    |message: &SizedMessage| message.key,
                )
                .message_size({
                    let measurements = Arc::clone(&measurements);
                    move |message| {
                        measurements.fetch_add(1, Ordering::SeqCst);
                        message.body.len()
                    }
                }),
        )
        .expect("valid sized actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("sized actor starts");

    actor
        .try_send(SizedMessage {
            key: "a",
            body: "aa",
        })
        .expect("a accepted");
    actor
        .try_send(SizedMessage {
            key: "b",
            body: "bbb",
        })
        .expect("b accepted");
    actor
        .try_send(SizedMessage {
            key: "a",
            body: "cccc",
        })
        .expect("same key replaced");
    actor
        .try_send(SizedMessage {
            key: "c",
            body: "d",
        })
        .expect("new key evicted oldest");
    actor
        .send_timeout(
            SizedMessage {
                key: "timeout",
                body: "not measured",
            },
            Duration::ZERO,
        )
        .await
        .expect_err("zero-budget ingress rejects before measurement");

    let live = actor.stats();
    assert_eq!(live.membership, actor.membership());
    assert!(live.observed_incarnation.is_some());
    assert_eq!(live.stats.messages_accepted, 4);
    assert_eq!(live.stats.messages_received, 0);
    assert_eq!(live.stats.messages_conflated, 1);
    assert_eq!(live.stats.messages_evicted, 1);
    assert_eq!(live.stats.message_bytes_accepted, Some(10));
    assert_eq!(live.stats.sends_rejected, 1);
    assert_eq!(live.stats.mailbox_depth, 2);
    assert_eq!(live.stats.mailbox_capacity, 2);
    assert_eq!(measurements.load(Ordering::SeqCst), 4);

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("sized actor stops");
    actor
        .try_send(SizedMessage {
            key: "terminal",
            body: "not measured",
        })
        .expect_err("terminal ingress rejects");
    let terminal = actor.stats();
    assert_eq!(terminal.observed_incarnation, None);
    assert_eq!(terminal.stats.messages_accepted, 4);
    assert_eq!(terminal.stats.message_bytes_accepted, Some(10));
    assert_eq!(terminal.stats.sends_rejected, 2);
    assert_eq!(terminal.stats.mailbox_depth, 0);
    assert_eq!(measurements.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn split_definition_keeps_size_panics_on_the_parked_sender() {
    let measurements = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let slot = tree
        .reserve_actor::<SizedMessage>("split-sized")
        .expect("sized slot reserved");
    let actor = slot.actor_ref();
    let mut send = Box::pin(actor.send(SizedMessage {
        key: "panic",
        body: "panic",
    }));
    let first_poll = std::future::poll_fn(|context| Poll::Ready(send.as_mut().poll(context))).await;
    assert!(first_poll.is_pending(), "undefined actor parks the send");

    let actor = slot.define_once_raw(RawOnceDef::new(ParkedSizedActor).message_size({
        let measurements = Arc::clone(&measurements);
        move |message| {
            measurements.fetch_add(1, Ordering::SeqCst);
            assert_ne!(message.body, "panic", "size panic remains on sender");
            message.body.len()
        }
    }));
    let sender = tokio::spawn(send);
    assert!(
        sender
            .await
            .expect_err("measurement panics on sender task")
            .is_panic()
    );

    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("sized actor starts");
    actor
        .send(SizedMessage {
            key: "ok",
            body: "good",
        })
        .await
        .expect("mailbox remains usable after measurement panic");
    assert_eq!(actor.stats().stats.message_bytes_accepted, Some(4));
    assert_eq!(measurements.load(Ordering::SeqCst), 2);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("sized actor stops");
}

struct CallbackActor;

impl Actor for CallbackActor {
    type Msg = ();
    type Args = ();

    async fn init(_: (), _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self)
    }

    async fn handle(&mut self, (): (), context: &mut Context<'_, Self>) -> ExitResult {
        context.stop();
        Ok(())
    }

    async fn on_stop(&mut self, _: &mut StopContext<'_, Self>) {}
}

struct IdleRaw;

impl RawActor for IdleRaw {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        while context.recv().await.is_some() {}
        Ok(())
    }
}

#[tokio::test]
async fn recursive_stats_use_typed_metadata_snapshot_order_and_scope_paths() {
    let mut nested = Tree::new();
    let nested_raw = nested
        .add_raw_once("raw", RawOnceDef::new(IdleRaw))
        .expect("nested raw actor valid");

    let mut tree = Tree::new();
    let callback = tree
        .add_actor_once("callback", ActorOnceDef::<CallbackActor>::new(()))
        .expect("callback actor valid");
    tree.add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("nested tree valid");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");

    let rows = system.scope().stats_recursive();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].path, []);
    assert_eq!(rows[0].id.as_str(), "callback");
    assert_eq!(rows[0].kind, ActorKind::Actor);
    assert_eq!(rows[0].membership, callback.membership());
    assert_eq!(rows[0].stats, callback.stats().stats);
    assert_eq!(rows[1].path.len(), 1);
    assert_eq!(rows[1].path[0].as_str(), "nested");
    assert_eq!(rows[1].id.as_str(), "raw");
    assert_eq!(rows[1].kind, ActorKind::RawActor);
    assert_eq!(rows[1].membership, nested_raw.membership());

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree stops");
}

#[tokio::test]
async fn child_and_restart_reducers_reset_after_loss_and_close_after_final_state() {
    let tree = DynamicTree::new();
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();
    let mut children = scope.observe_children();
    let mut restarts = scope.restart_counts();

    assert!(matches!(
        children.next().await,
        Some(ChildObservation::Reset { dropped: 0, .. })
    ));
    assert_eq!(
        restarts.next().await,
        Some(RestartCount {
            total: 0,
            delta: 0,
            resynced: true,
        })
    );

    for index in 0..129 {
        let id = format!("task-{index}");
        scope
            .add_task(
                id,
                TaskDef::new(|context| async move {
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }),
            )
            .await
            .expect("dynamic task admitted");
    }

    let Some(ChildObservation::Reset { snapshot, dropped }) = children.next().await else {
        panic!("child reducer must replace state after lag");
    };
    assert!(dropped > 0);
    assert_eq!(snapshot.children.len(), 129);
    assert_eq!(
        restarts.next().await,
        Some(RestartCount {
            total: 0,
            delta: 0,
            resynced: true,
        })
    );

    system
        .shutdown(Duration::from_secs(2))
        .await
        .expect("dynamic tree stops");
    let saw_final = tokio::time::timeout(Duration::from_secs(2), async {
        let mut saw_final = false;
        while let Some(observation) = children.next().await {
            let snapshot = match observation {
                ChildObservation::Reset { snapshot, .. }
                | ChildObservation::Changed { snapshot, .. } => snapshot,
                _ => continue,
            };
            saw_final |= matches!(snapshot.state, ScopeState::Stopped { .. });
        }
        saw_final
    })
    .await
    .expect("child observer closes");
    assert!(
        saw_final,
        "final scope state precedes child-observer closure"
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while restarts.next().await.is_some() {}
    })
    .await
    .expect("restart counter closes");
}

enum CrashMessage {
    Crash,
}

struct CrashActor;

impl RawActor for CrashActor {
    type Msg = CrashMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        match context.recv().await {
            Some(CrashMessage::Crash) => Err(ExitError::message("restart counter trigger")),
            None => Ok(()),
        }
    }
}

#[tokio::test]
async fn restart_counter_reports_authoritative_deltas_without_event_double_charge() {
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "crasher",
            RawDef::factory(|| CrashActor).restart(RestartPolicy::new(
                RestartCondition::Always,
                Backoff::Immediate,
            )),
        )
        .expect("crash actor valid");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("crash actor starts");
    let first = actor
        .stats()
        .observed_incarnation
        .expect("first incarnation bound");
    let mut counter = system.scope().restart_counts();
    assert_eq!(
        counter.next().await,
        Some(RestartCount {
            total: 0,
            delta: 0,
            resynced: true,
        })
    );

    actor.try_send(CrashMessage::Crash).expect("crash accepted");
    assert_eq!(
        counter.next().await,
        Some(RestartCount {
            total: 1,
            delta: 1,
            resynced: false,
        })
    );
    actor
        .next_incarnation(first, Duration::from_secs(1))
        .await
        .expect("actor restarts");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("crash actor stops");
}

#[derive(Clone, Copy)]
enum LocalMessage {
    Start,
    Continuation,
    Timer,
    Offload,
}

struct LocalSourceActor {
    release: ReleaseGate,
    delivered: Arc<AtomicUsize>,
}

impl RawActor for LocalSourceActor {
    type Msg = LocalMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        while let Some(message) = context.recv().await {
            match message {
                LocalMessage::Start => {
                    context
                        .continue_with(LocalMessage::Continuation)
                        .expect("continuation accepted");
                    context
                        .set_timeout("timer", LocalMessage::Timer, Duration::ZERO)
                        .expect("timer accepted");
                    let release = self.release.clone();
                    context
                        .offload(
                            async move { release.wait().await },
                            |result| {
                                result.expect("offload completes before deadline");
                                LocalMessage::Offload
                            },
                            Duration::from_secs(1),
                        )
                        .expect("offload accepted");
                }
                LocalMessage::Continuation | LocalMessage::Timer | LocalMessage::Offload => {
                    self.delivered.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn local_delivery_counters_and_outstanding_offload_gauge_are_exact() {
    let release = ReleaseGate::default();
    let delivered = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "local",
            RawOnceDef::new(LocalSourceActor {
                release: release.clone(),
                delivered: Arc::clone(&delivered),
            }),
        )
        .expect("local source actor valid");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("local actor starts");
    actor.try_send(LocalMessage::Start).expect("start accepted");

    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            actor.stats().stats.outstanding_offloads == 1 && delivered.load(Ordering::SeqCst) >= 2
        })
        .await
    );
    release.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            delivered.load(Ordering::SeqCst) == 3 && actor.stats().stats.outstanding_offloads == 0
        })
        .await
    );
    let stats = actor.stats().stats;
    assert_eq!(stats.messages_accepted, 1);
    assert_eq!(stats.messages_received, 3);
    assert_eq!(stats.messages_conflated, 0);
    assert_eq!(stats.messages_evicted, 0);
    assert_eq!(stats.message_bytes_accepted, None);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("local actor stops");
}
