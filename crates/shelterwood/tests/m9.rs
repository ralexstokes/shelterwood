use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use shelterwood::{
    Backoff, ExitError, ExitResult, Intensity, Jitter, LifecycleEventKind, LifecycleItem, RawActor,
    RawContext, RawDef, Readiness, ReadinessDeadline, RestartCondition, RestartPolicy, StopReason,
    Strategy, Tree,
};
use shelterwood_test_support::{ReleaseGate, poll_until};

enum GroupMessage {
    Crash,
}

struct GroupActor {
    generation: usize,
    delay_second_ready: bool,
    second_ready: ReleaseGate,
}

struct HoldingActor {
    generation: usize,
    shutdown_entered: ReleaseGate,
    shutdown_release: ReleaseGate,
}

struct ReadinessFailureActor {
    generation: usize,
}

impl RawActor for ReadinessFailureActor {
    type Msg = GroupMessage;

    fn readiness(&self) -> Readiness {
        Readiness::Manual
    }

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        if self.generation != 2 {
            context.mark_ready();
        }
        match context.recv().await {
            Some(GroupMessage::Crash) => Err(ExitError::message("readiness trigger")),
            None => Ok(()),
        }
    }
}

fn readiness_failure_actor(constructions: &Arc<AtomicUsize>) -> RawDef<ReadinessFailureActor> {
    RawDef::factory({
        let constructions = Arc::clone(constructions);
        move || ReadinessFailureActor {
            generation: constructions.fetch_add(1, Ordering::SeqCst) + 1,
        }
    })
    .readiness_deadline(
        ReadinessDeadline::bounded(Duration::from_millis(1)).expect("valid readiness deadline"),
    )
}

impl RawActor for HoldingActor {
    type Msg = GroupMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        match context.recv().await {
            Some(GroupMessage::Crash) => Err(ExitError::message("holding trigger")),
            None => {
                if self.generation == 1 {
                    self.shutdown_entered.release();
                    self.shutdown_release.wait().await;
                }
                Ok(())
            }
        }
    }
}

fn holding_actor(
    constructions: &Arc<AtomicUsize>,
    shutdown_entered: &ReleaseGate,
    shutdown_release: &ReleaseGate,
) -> RawDef<HoldingActor> {
    RawDef::factory({
        let constructions = Arc::clone(constructions);
        let shutdown_entered = shutdown_entered.clone();
        let shutdown_release = shutdown_release.clone();
        move || HoldingActor {
            generation: constructions.fetch_add(1, Ordering::SeqCst) + 1,
            shutdown_entered: shutdown_entered.clone(),
            shutdown_release: shutdown_release.clone(),
        }
    })
}

impl RawActor for GroupActor {
    type Msg = GroupMessage;

    fn readiness(&self) -> Readiness {
        Readiness::Manual
    }

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        if self.generation == 2 && self.delay_second_ready {
            self.second_ready.wait().await;
        }
        context.mark_ready();
        match context.recv().await {
            Some(GroupMessage::Crash) => Err(ExitError::message("group trigger")),
            None => Ok(()),
        }
    }
}

fn group_actor(
    constructions: &Arc<AtomicUsize>,
    second_ready: &ReleaseGate,
    delay_second_ready: bool,
) -> RawDef<GroupActor> {
    RawDef::factory({
        let constructions = Arc::clone(constructions);
        let second_ready = second_ready.clone();
        move || GroupActor {
            generation: constructions.fetch_add(1, Ordering::SeqCst) + 1,
            delay_second_ready,
            second_ready: second_ready.clone(),
        }
    })
}

async fn wait_for_count(counter: &AtomicUsize, expected: usize) {
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            counter.load(Ordering::SeqCst) == expected
        })
        .await,
        "construction count did not reach {expected}"
    );
}

#[tokio::test]
async fn one_for_all_drains_restartable_residents_and_respawns_in_ready_order() {
    let first = Arc::new(AtomicUsize::new(0));
    let trigger_count = Arc::new(AtomicUsize::new(0));
    let last = Arc::new(AtomicUsize::new(0));
    let never = Arc::new(AtomicUsize::new(0));
    let release_first = ReleaseGate::default();
    let ready = ReleaseGate::default();

    let mut tree = Tree::new();
    tree.strategy(Strategy::OneForAll);
    let _first = tree
        .add_raw("first", group_actor(&first, &release_first, true))
        .expect("valid first actor");
    let trigger = tree
        .add_raw("trigger", group_actor(&trigger_count, &ready, false))
        .expect("valid trigger actor");
    let _last = tree
        .add_raw("last", group_actor(&last, &ready, false))
        .expect("valid last actor");
    let _never_ref = tree
        .add_raw(
            "never",
            group_actor(&never, &ready, false).restart(RestartPolicy::new(
                RestartCondition::Never,
                Backoff::Immediate,
            )),
        )
        .expect("valid never actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("group starts");
    let never_incarnation = system
        .scope()
        .snapshot()
        .child("never")
        .expect("never resident exists")
        .incarnation
        .expect("never resident is live");

    trigger
        .send(GroupMessage::Crash)
        .await
        .expect("trigger crash accepted");
    wait_for_count(&first, 2).await;
    assert_eq!(trigger_count.load(Ordering::SeqCst), 1);
    assert_eq!(last.load(Ordering::SeqCst), 1);
    assert_eq!(never.load(Ordering::SeqCst), 1);

    release_first.release();
    wait_for_count(&trigger_count, 2).await;
    wait_for_count(&last, 2).await;
    assert_eq!(never.load(Ordering::SeqCst), 1);

    let snapshot = system.scope().snapshot();
    assert_eq!(snapshot.strategy, Some(Strategy::OneForAll));
    assert_eq!(
        snapshot
            .child("never")
            .expect("never member retained")
            .incarnation,
        Some(never_incarnation)
    );
    assert_eq!(snapshot.total_restarts, 3);
    for id in ["first", "trigger", "last"] {
        assert_eq!(
            snapshot
                .child(id)
                .expect("group member retained")
                .restart_count,
            1
        );
    }
    assert_eq!(
        snapshot
            .child("never")
            .expect("never member retained")
            .restart_count,
        0
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("group stops");
}

#[tokio::test]
async fn rest_for_one_keeps_earlier_members_and_restarts_the_declared_suffix() {
    let first = Arc::new(AtomicUsize::new(0));
    let trigger_count = Arc::new(AtomicUsize::new(0));
    let last = Arc::new(AtomicUsize::new(0));
    let ready = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.strategy(Strategy::RestForOne);
    tree.add_raw("first", group_actor(&first, &ready, false))
        .expect("valid first actor");
    let trigger = tree
        .add_raw("trigger", group_actor(&trigger_count, &ready, false))
        .expect("valid trigger actor");
    tree.add_raw("last", group_actor(&last, &ready, false))
        .expect("valid last actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("suffix group starts");

    trigger
        .send(GroupMessage::Crash)
        .await
        .expect("suffix trigger accepted");
    wait_for_count(&trigger_count, 2).await;
    wait_for_count(&last, 2).await;
    assert_eq!(first.load(Ordering::SeqCst), 1);
    let snapshot = system.scope().snapshot();
    assert_eq!(snapshot.strategy, Some(Strategy::RestForOne));
    assert_eq!(snapshot.total_restarts, 2);

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("suffix group stops");
}

#[tokio::test]
async fn an_outside_exit_waits_for_the_frozen_cycle_and_never_widens_it() {
    let outside_count = Arc::new(AtomicUsize::new(0));
    let trigger_count = Arc::new(AtomicUsize::new(0));
    let last_count = Arc::new(AtomicUsize::new(0));
    let ready = ReleaseGate::default();
    let shutdown_entered = ReleaseGate::default();
    let shutdown_release = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.strategy(Strategy::RestForOne);
    let outside = tree
        .add_raw("outside", group_actor(&outside_count, &ready, false))
        .expect("valid outside actor");
    let trigger = tree
        .add_raw("trigger", group_actor(&trigger_count, &ready, false))
        .expect("valid trigger actor");
    tree.add_raw(
        "last",
        holding_actor(&last_count, &shutdown_entered, &shutdown_release),
    )
    .expect("valid holding actor");
    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("deferred-exit group starts");

    trigger
        .send(GroupMessage::Crash)
        .await
        .expect("first group trigger accepted");
    shutdown_entered.wait().await;
    outside
        .send(GroupMessage::Crash)
        .await
        .expect("outside exit accepted during group drain");
    shutdown_release.release();

    wait_for_count(&outside_count, 2).await;
    wait_for_count(&trigger_count, 3).await;
    wait_for_count(&last_count, 3).await;
    assert_eq!(system.scope().snapshot().total_restarts, 5);

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("deferred-exit group stops");
}

#[tokio::test(start_paused = true)]
async fn the_trigger_backoff_delays_the_entire_group() {
    let first_count = Arc::new(AtomicUsize::new(0));
    let trigger_count = Arc::new(AtomicUsize::new(0));
    let ready = ReleaseGate::default();
    let shutdown_entered = ReleaseGate::default();
    let shutdown_release = ReleaseGate::default();
    let backoff = Duration::from_secs(10);
    let mut tree = Tree::new();
    tree.strategy(Strategy::OneForAll);
    tree.add_raw(
        "first",
        holding_actor(&first_count, &shutdown_entered, &shutdown_release),
    )
    .expect("valid first actor");
    let trigger = tree
        .add_raw(
            "trigger",
            group_actor(&trigger_count, &ready, false).restart(RestartPolicy::new(
                RestartCondition::OnFailure,
                Backoff::fixed(backoff, Jitter::None).expect("valid fixed backoff"),
            )),
        )
        .expect("valid delayed trigger");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("delayed group starts");
    let mut events = system.scope().subscribe_lifecycle();

    let first_incarnation = trigger
        .send(GroupMessage::Crash)
        .await
        .expect("delayed trigger accepted");
    shutdown_entered.wait().await;
    shutdown_release.release();
    loop {
        let Some(item) = events.recv().await else {
            panic!("lifecycle remains open during group restart");
        };
        if matches!(
            item,
            LifecycleItem::Event(event)
                if matches!(event.kind, LifecycleEventKind::Exited { ref id, .. } if id.as_str() == "first")
        ) {
            break;
        }
    }

    assert_eq!(first_count.load(Ordering::SeqCst), 1);
    assert_eq!(trigger_count.load(Ordering::SeqCst), 1);
    tokio::time::advance(backoff - Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(first_count.load(Ordering::SeqCst), 1);
    assert_eq!(trigger_count.load(Ordering::SeqCst), 1);

    tokio::time::advance(Duration::from_secs(1)).await;
    trigger
        .next_incarnation(first_incarnation, Duration::from_secs(1))
        .await
        .expect("whole group restarts after trigger backoff");
    wait_for_count(&first_count, 2).await;
    wait_for_count(&trigger_count, 2).await;

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("delayed group stops");
}

#[tokio::test(start_paused = true)]
async fn readiness_failure_reenters_the_funnel_after_the_current_cycle() {
    let first_count = Arc::new(AtomicUsize::new(0));
    let trigger_count = Arc::new(AtomicUsize::new(0));
    let ready = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.strategy(Strategy::OneForAll);
    tree.add_raw("first", readiness_failure_actor(&first_count))
        .expect("valid readiness-failure actor");
    let trigger = tree
        .add_raw("trigger", group_actor(&trigger_count, &ready, false))
        .expect("valid readiness trigger");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("readiness group starts");

    trigger
        .send(GroupMessage::Crash)
        .await
        .expect("readiness group trigger accepted");
    wait_for_count(&first_count, 3).await;
    wait_for_count(&trigger_count, 3).await;
    assert_eq!(system.scope().snapshot().total_restarts, 4);

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("readiness group stops");
}

#[tokio::test]
async fn group_intensity_is_one_atomic_batch_with_no_partial_respawn() {
    let first = Arc::new(AtomicUsize::new(0));
    let trigger_count = Arc::new(AtomicUsize::new(0));
    let last = Arc::new(AtomicUsize::new(0));
    let ready = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.strategy(Strategy::OneForAll);
    tree.intensity(Intensity::new(2, Duration::from_secs(60)).expect("valid intensity"));
    tree.add_raw("first", group_actor(&first, &ready, false))
        .expect("valid first actor");
    let trigger = tree
        .add_raw("trigger", group_actor(&trigger_count, &ready, false))
        .expect("valid trigger actor");
    tree.add_raw("last", group_actor(&last, &ready, false))
        .expect("valid last actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("intensity group starts");
    let scope = system.scope().clone();
    let mut events = scope.subscribe_lifecycle();

    trigger
        .send(GroupMessage::Crash)
        .await
        .expect("intensity trigger accepted");
    let reason = system.wait().await;
    let StopReason::IntensityTripped(trip) = reason else {
        panic!("expected group intensity trip");
    };
    assert_eq!(trip.max_restarts, 2);
    assert_eq!(trip.observed_restarts, 3);
    assert_eq!(scope.snapshot().total_restarts, 3);
    assert_eq!(first.load(Ordering::SeqCst), 1);
    assert_eq!(trigger_count.load(Ordering::SeqCst), 1);
    assert_eq!(last.load(Ordering::SeqCst), 1);

    let mut order = Vec::new();
    while let Some(item) = events.recv().await {
        let LifecycleItem::Event(event) = item else {
            continue;
        };
        match event.kind {
            LifecycleEventKind::Exited { id, .. } => order.push((id, "exited")),
            LifecycleEventKind::RestartScheduled { id, .. } => {
                order.push((id, "scheduled"));
            }
            _ => {}
        }
    }
    for id in ["first", "trigger", "last"] {
        let exited = order
            .iter()
            .position(|(event_id, kind)| event_id.as_str() == id && *kind == "exited")
            .expect("charged group member publishes exit");
        let scheduled = order
            .iter()
            .position(|(event_id, kind)| event_id.as_str() == id && *kind == "scheduled")
            .expect("charged group member publishes schedule");
        assert!(exited < scheduled, "{id} schedules only after its exit");
    }
}
