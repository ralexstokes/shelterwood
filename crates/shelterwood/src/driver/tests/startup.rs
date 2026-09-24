use super::support::*;

#[test]
fn refused_root_running_transition_panics_after_the_observation_transaction() {
    let plan = Tree::new().lower_for_test();
    let root = Arc::clone(&plan.root);
    root.member
        .update(|record| record.stage = MemberStage::Running);

    let mut driver = Box::pin(run_scope(plan, ScopeRole::Root));
    let refusal = catch_unwind(AssertUnwindSafe(|| {
        let mut context = Context::from_waker(Waker::noop());
        let _ = driver.as_mut().poll(&mut context);
    }));

    assert!(
        refusal.is_err(),
        "a refused root Running transition fails closed in every profile"
    );
    assert!(
        !root.observation_gate().is_poisoned(),
        "the transition verdict is asserted after ObservationTxn releases the gate"
    );
    drop(driver);
}

#[test]
fn empty_control_peeks_skip_the_tree_observation_gate() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let epoch = root
        .begin_incarnation(ScopeState::Starting)
        .expect("the scope has a live epoch");
    let captures = root.probe_gate_captures();

    assert!(!root.take_shutdown_request(epoch));
    assert!(!root.take_force_request(epoch));
    assert!(root.take_control_events().is_empty());
    assert!(
        matches!(
            captures.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ),
        "an idle driver wake only peeks under the control mutex"
    );
}

#[test]
fn pre_admission_restart_shutdown_is_published_when_the_scope_gets_a_parent() {
    let mut tree = Tree::new();
    tree.add_subtree("nested", SubtreeDef::factory(Tree::new))
        .expect("valid subtree");
    let plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let nested = plan.children[0]
        .slot
        .scope
        .as_ref()
        .expect("nested scope cell");

    let target = nested.request_shutdown();
    assert!(root.take_control_events().is_empty());
    assert!(
        root.set_admitted_children(
            plan.children
                .iter()
                .map(|child| resident_projection(&child.slot))
                .collect(),
        )
    );

    assert_eq!(
        root.take_control_events(),
        vec![ScopeControlEvent::WindowStop {
            membership: nested.member.membership(),
            target,
        }]
    );
}

#[test]
fn consumed_pre_admission_shutdown_is_not_published_when_the_scope_gets_a_parent() {
    let mut tree = Tree::new();
    tree.add_subtree("nested", SubtreeDef::factory(Tree::new))
        .expect("valid subtree");
    let plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let nested = plan.children[0]
        .slot
        .scope
        .as_ref()
        .expect("nested scope cell");

    let target = nested.request_shutdown();
    assert!(nested.take_shutdown_request(target));
    assert!(
        root.set_admitted_children(
            plan.children
                .iter()
                .map(|child| resident_projection(&child.slot))
                .collect(),
        )
    );

    assert!(
        root.take_control_events().is_empty(),
        "parent wiring cannot replay a shutdown request the child already consumed"
    );
}

fn nested_with_backoff(condition: RestartCondition) -> SubtreeDef<Tree> {
    SubtreeDef::factory(pending_tree).restart(RestartPolicy::new(
        condition,
        Backoff::fixed(Duration::from_secs(60), crate::Jitter::None)
            .expect("non-zero restart backoff"),
    ))
}

fn failed_exit(message: &'static str) -> Option<Retained<RecordedOutcome>> {
    Some(Retained::new(RecordedOutcome::returned(Err(
        ExitError::message(message),
    ))))
}

/// Starts the child's incarnation and aborts its task before it is ever
/// polled, so no user construction runs and the nested epoch plane stays
/// idle: a request accepted now is a pending-incarnation request.
fn spawn_unpolled(scope: &mut ScopeRuntime, key: ChildKey) -> Incarnation {
    scope.spawn_child(key);
    let active = scope.children[&key]
        .active
        .as_ref()
        .expect("the spawned incarnation is active");
    active.abort_handle.abort();
    active.incarnation
}

/// SPEC §11, pre-spawn under `Always`: the construction site vacates the
/// targeted first epoch and then constructs the policy's own incarnation,
/// which mints the following epoch with a clear latch.
#[crate::runtime::test]
async fn pre_spawn_always_stop_is_vacated_at_the_construction_site() {
    let mut tree = Tree::new();
    tree.add_subtree("nested", nested_with_backoff(RestartCondition::Always))
        .expect("valid subtree");
    let fixture = OrderedScopeFixture::new(tree);
    let key = fixture.children.keys().next().expect("one child plan");
    let nested = Arc::clone(
        fixture.children[key]
            .slot
            .scope
            .as_ref()
            .expect("nested scope cell"),
    );
    let target = nested.request_shutdown();
    let (mut scope, _event_receiver) = fixture.with_lifecycle(ScopeLifecycle::running()).build();

    spawn_unpolled(&mut scope, key);

    assert_eq!(nested.pending_incarnation_shutdown(), None);
    assert!(
        nested.settled(Some(target)),
        "the vacated target settles every wait on it"
    );
    let begun = nested
        .begin_incarnation(ScopeState::Starting)
        .expect("vacating leaves the epoch plane idle");
    assert_ne!(
        begun, target,
        "the incarnation mints past the vacated target"
    );
    assert!(
        !nested.has_stop_request(begun),
        "the policy's own incarnation starts with a clear latch"
    );
}

/// A window stop whose event beats the incarnation's exit finds the child
/// still active and leaves the request pending. The exit then re-checks the
/// level. Returns the scope, the child key and the nested cell.
async fn window_stop_before_exit(
    condition: RestartCondition,
) -> (ScopeRuntime, ChildKey, Arc<ScopeCell>, Epoch) {
    let mut tree = Tree::new();
    tree.add_subtree("nested", nested_with_backoff(condition))
        .expect("valid subtree");
    let fixture = OrderedScopeFixture::new(tree);
    let key = fixture.children.keys().next().expect("one child plan");
    let (mut scope, _event_receiver) = fixture.with_lifecycle(ScopeLifecycle::running()).build();
    let nested = Arc::clone(
        scope.children[&key]
            .slot
            .scope
            .as_ref()
            .expect("nested scope cell"),
    );
    let first = spawn_unpolled(&mut scope, key);
    let target = nested.request_shutdown();

    scope.resolve_window_stop(key, target);
    assert_eq!(
        nested.pending_incarnation_shutdown(),
        Some(target),
        "an active incarnation leaves the request to its exit"
    );

    scope.handle_exit(
        key,
        first,
        failed_exit("restart the nested scope"),
        crate::runtime::JoinOutcome::Ok { value: () },
        Cancellation::NotObserved,
        false,
    );
    assert!(scope.children[&key].active.is_none());
    (scope, key, nested, target)
}

#[crate::runtime::test]
async fn always_window_stop_arriving_before_exit_is_spent_when_the_window_opens() {
    let (scope, key, nested, target) = window_stop_before_exit(RestartCondition::Always).await;

    assert_eq!(nested.pending_incarnation_shutdown(), None);
    assert!(nested.settled(Some(target)));
    assert!(
        scope.children[&key].restart_deadline.is_some(),
        "the pending restart keeps its schedule"
    );
    assert_eq!(
        scope.children[&key].slot.member.record().stage,
        MemberStage::Restarting
    );
}

#[crate::runtime::test]
async fn on_failure_window_stop_arriving_before_exit_cancels_the_opened_window() {
    let (scope, key, nested, _target) = window_stop_before_exit(RestartCondition::OnFailure).await;

    assert!(
        scope.children[&key].restart_deadline.is_none(),
        "the pending restart is cancelled"
    );
    let record = scope.children[&key].slot.member.record();
    let MemberStage::Terminal(exit) = &record.stage else {
        panic!("the window member terminalizes in place")
    };
    assert!(
        matches!(exit.kind(), ExitKind::Failed(_)),
        "the terminal carries the last exit, not a cancelled completion"
    );
    assert!(!record.startup_aborted);
    assert!(
        nested.settled(None),
        "membership terminality settles the pending request"
    );
}

/// A restart deadline executing ahead of a window stop's control event must
/// not construct on the request's behalf: `spawn_child` re-checks the level at
/// the construction site. Returns the scope, the child key and the nested
/// cell, with the deadline already handled.
async fn restart_deadline_ahead_of_window_stop(
    condition: RestartCondition,
) -> (ScopeRuntime, ChildKey, Arc<ScopeCell>, Epoch, Incarnation) {
    let mut tree = Tree::new();
    tree.add_subtree("nested", nested_with_backoff(condition))
        .expect("valid subtree");
    let fixture = OrderedScopeFixture::new(tree);
    let key = fixture.children.keys().next().expect("one child plan");
    let (mut scope, _event_receiver) = fixture.with_lifecycle(ScopeLifecycle::running()).build();
    let nested = Arc::clone(
        scope.children[&key]
            .slot
            .scope
            .as_ref()
            .expect("nested scope cell"),
    );
    let first = spawn_unpolled(&mut scope, key);
    scope.handle_exit(
        key,
        first,
        failed_exit("open the restart window"),
        crate::runtime::JoinOutcome::Ok { value: () },
        Cancellation::NotObserved,
        false,
    );
    assert!(scope.children[&key].restart_deadline.is_some());
    let target = nested.request_shutdown();

    scope.handle_deadline(super::super::DeadlineKind::Restart { child: key });
    (scope, key, nested, target, first)
}

#[crate::runtime::test]
async fn on_failure_restart_deadline_ahead_of_a_window_stop_constructs_nothing() {
    let (scope, key, _nested, _target, _first) =
        restart_deadline_ahead_of_window_stop(RestartCondition::OnFailure).await;

    assert!(
        scope.children[&key].active.is_none(),
        "the due restart constructs no incarnation for a stopped window"
    );
    assert!(scope.children[&key].restart_deadline.is_none());
    let MemberStage::Terminal(exit) = scope.children[&key].slot.member.record().stage.clone()
    else {
        panic!("the construction site terminalizes the window member")
    };
    assert!(matches!(exit.kind(), ExitKind::Failed(_)));
}

#[crate::runtime::test]
async fn always_restart_deadline_ahead_of_a_window_stop_runs_the_policys_own_restart() {
    let (scope, key, nested, target, first) =
        restart_deadline_ahead_of_window_stop(RestartCondition::Always).await;

    let active = scope.children[&key]
        .active
        .as_ref()
        .expect("the scheduled restart still constructs");
    assert_ne!(active.incarnation, first);
    active.abort_handle.abort();
    assert_eq!(nested.pending_incarnation_shutdown(), None);
    assert!(nested.settled(Some(target)), "the request was spent first");
    let begun = nested
        .begin_incarnation(ScopeState::Starting)
        .expect("the vacated plane stays idle until the restart begins");
    assert!(
        !nested.has_stop_request(begun),
        "the restarted incarnation starts with a clear latch"
    );
}

/// Vacating an Always request wakes user code. That code must already see
/// Starting, so a reentrant stop addresses the committed body instead of
/// slipping into the idle gap between the stop decision and publication.
#[crate::runtime::test]
async fn vacated_stop_wakes_only_after_the_construction_boundary() {
    struct StopOnWake {
        scope: Arc<ScopeCell>,
        starting: AtomicBool,
    }
    impl Wake for StopOnWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.starting.store(
                matches!(self.scope.member.record().stage, MemberStage::Starting),
                Ordering::SeqCst,
            );
            self.scope.request_shutdown();
        }
    }
    let mut tree = Tree::new();
    tree.add_subtree("nested", nested_with_backoff(RestartCondition::Always))
        .expect("valid subtree");
    let fixture = OrderedScopeFixture::new(tree);
    let key = fixture.children.keys().next().expect("one child");
    let nested = Arc::clone(
        fixture.children[key]
            .slot
            .scope
            .as_ref()
            .expect("nested scope"),
    );
    let (mut scope, _events) = fixture.with_lifecycle(ScopeLifecycle::running()).build();
    let target = nested.request_shutdown();
    let mut watcher = nested.member.record_watcher();
    watcher.borrow_and_update_cloned();
    let wake = Arc::new(StopOnWake {
        scope: Arc::clone(&nested),
        starting: AtomicBool::new(false),
    });
    let waker = Waker::from(Arc::clone(&wake));
    let mut changed = Box::pin(watcher.changed());
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    spawn_unpolled(&mut scope, key);

    assert!(
        wake.starting.load(Ordering::SeqCst),
        "a settled idle request cannot wake before Starting commits"
    );
    assert!(
        nested.settled(Some(target)),
        "the original idle request is vacated"
    );
    let live = nested
        .begin_incarnation(ScopeState::Starting)
        .expect("the committed body begins");
    assert!(
        nested.has_stop_request(live),
        "the reentrant request addresses that body"
    );
    nested.finish_incarnation(live, StopReason::ShutdownRequested);
}

/// A window stop and an intensity-tripping sibling exit collected in one
/// batch: the exit sorts first and drains the scope, so the drain owns the
/// window member's terminal and the resolver leaves it alone.
#[crate::runtime::test]
async fn same_batch_intensity_trip_owns_a_window_stop_terminal() {
    let mut tree = Tree::new();
    tree.intensity(Intensity::new(1, Duration::from_secs(10)).expect("valid intensity"));
    tree.add_subtree("nested", nested_with_backoff(RestartCondition::OnFailure))
        .expect("valid subtree");
    tree.add_task("trip", TaskDef::new(|_| future::pending()))
        .expect("valid task");
    let fixture = OrderedScopeFixture::new(tree);
    let root = Arc::clone(&fixture.root);
    let find = |id: &str| {
        fixture
            .children
            .keys()
            .find(|key| fixture.children[*key].slot.member.id().as_str() == id)
            .expect("fixture child key")
    };
    let nested = find("nested");
    let trip = find("trip");
    let next_ordered_start = fixture.children.keys().next();
    let (mut scope, _event_receiver) = fixture
        .with_lifecycle(ScopeLifecycle::running())
        .with_next_ordered_start(next_ordered_start)
        .build();

    let nested_first = spawn_unpolled(&mut scope, nested);
    scope.handle_exit(
        nested,
        nested_first,
        failed_exit("open the restart window"),
        crate::runtime::JoinOutcome::Ok { value: () },
        Cancellation::NotObserved,
        false,
    );
    assert!(scope.children[&nested].restart_deadline.is_some());
    let target = scope.children[&nested]
        .slot
        .scope
        .as_ref()
        .expect("nested scope cell")
        .request_shutdown();
    let event = root
        .take_control_events()
        .pop()
        .expect("the window request publishes one control event");
    let work = scope
        .control_event_work(event)
        .expect("the event resolves to its member");
    assert!(matches!(
        work.1,
        Pending::WindowStop { child, target: event_target }
            if child == nested && event_target == target
    ));

    let trip_incarnation = spawn_unpolled(&mut scope, trip);
    let trip_exit = DriverEvent::Child(ChildEvent::Exited {
        child: trip,
        incarnation: trip_incarnation,
        recorded: failed_exit("trip intensity"),
        join: crate::runtime::JoinOutcome::Ok { value: () },
        cancellation: Cancellation::NotObserved,
        readiness_signal_seen: false,
    });
    let mut pending = [work, Pending::from(trip_exit).classified()];
    arbitrate(&mut pending);
    let mut order = Vec::new();
    for (_, event) in pending {
        match event {
            Pending::WindowStop { child, target } => {
                order.push("window-stop");
                scope.resolve_window_stop(child, target);
            }
            Pending::Child(ChildEvent::Exited {
                child,
                incarnation,
                recorded,
                join,
                cancellation,
                readiness_signal_seen,
            }) => {
                order.push("exit");
                scope.handle_exit(
                    child,
                    incarnation,
                    recorded,
                    join,
                    cancellation,
                    readiness_signal_seen,
                );
            }
            _ => unreachable!("the fixture queues only exit and window-stop work"),
        }
    }

    assert_eq!(order, ["exit", "window-stop"]);
    assert!(matches!(
        scope.supervisor.lifecycle().draining_reason(),
        Some(StopReason::IntensityTripped(_))
    ));
    // Ordered teardown reaches `nested` only after `trip` joins. The resolver
    // must leave the member to that turn rather than terminalize it early.
    assert!(!scope.supervisor.is_disposing(nested));
    assert!(scope.children[&nested].active.is_none());
    assert_eq!(
        scope.children[&nested].slot.member.record().stage,
        MemberStage::Restarting,
        "the drain, not the window stop, owns the member's terminal"
    );
}

/// A pre-spawn request on an ordered child that would not restart is left
/// alone by its control event and resolved only at the child's ordered
/// construction site, which terminalizes it as `NeverStarted` and routes the
/// scope's startup failure through it.
#[crate::runtime::test]
async fn pre_spawn_stop_on_an_ordered_child_resolves_at_its_turn_without_constructing() {
    let factories = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    tree.add_task(
        "a",
        TaskDef::new(|_| future::pending::<crate::ExitResult>())
            .readiness(Readiness::Manual)
            .expect("manual readiness is valid")
            .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid task");
    tree.add_subtree(
        "nested",
        SubtreeDef::factory({
            let factories = Arc::clone(&factories);
            move || {
                factories.fetch_add(1, Ordering::SeqCst);
                Tree::new()
            }
        })
        .restart(RestartPolicy::new(
            RestartCondition::OnFailure,
            Backoff::Immediate,
        )),
    )
    .expect("valid subtree");

    let fixture = OrderedScopeFixture::new(tree);
    let root = Arc::clone(&fixture.root);
    let first = fixture.children.keys().next().expect("first ordered child");
    let nested = fixture
        .children
        .keys()
        .find(|key| fixture.children[*key].slot.member.id().as_str() == "nested")
        .expect("nested child key");
    let nested_cell = Arc::clone(
        fixture.children[nested]
            .slot
            .scope
            .as_ref()
            .expect("nested scope cell"),
    );
    let target = nested_cell.request_shutdown();
    let (mut scope, _event_receiver) = fixture.with_next_ordered_start(Some(first)).build();

    // Ordered startup spawns "a" and parks on its never-fired readiness.
    scope.settle_supervisor();
    let a_incarnation = scope.children[&first]
        .active
        .as_ref()
        .expect("the first ordered child is active")
        .incarnation;

    let event = root
        .take_control_events()
        .pop()
        .expect("parent adoption publishes the pre-admission request");
    let Some((
        _,
        Pending::WindowStop {
            child,
            target: event_target,
        },
    )) = scope.control_event_work(event)
    else {
        panic!("the control event resolves to window-stop work");
    };
    assert_eq!((child, event_target), (nested, target));
    scope.resolve_window_stop(child, event_target);

    assert!(
        !matches!(
            scope.children[&nested].slot.member.record().stage,
            MemberStage::Terminal(_)
        ),
        "the member is not terminalized ahead of its ordered turn"
    );
    assert_eq!(nested_cell.pending_incarnation_shutdown(), Some(target));

    scope.handle_ready(first, a_incarnation);
    crate::runtime::yield_now().await;
    crate::runtime::yield_now().await;

    assert_eq!(
        factories.load(Ordering::SeqCst),
        0,
        "the ordered turn constructs nothing on the request's behalf"
    );
    assert!(scope.children[&nested].active.is_none());
    assert!(!scope.supervisor.spawned_once(nested));
    let record = scope.children[&nested].slot.member.record();
    let MemberStage::Terminal(exit) = &record.stage else {
        panic!("the ordered turn terminalizes the member")
    };
    assert!(matches!(exit.kind(), ExitKind::NeverStarted));
    assert!(
        !record.startup_aborted,
        "a never-spawned member publishes plain NeverStarted"
    );
    assert_eq!(
        nested_cell.record().state,
        ScopeState::Stopped {
            reason: StopReason::NeverStarted
        }
    );
    let Some(Err(StartupError::StartupFailed(failure))) = root.record().startup.clone() else {
        panic!("the never-started initial member fails the scope's startup")
    };
    assert!(matches!(
        failure.cause,
        StartupFailureCause::Child { ref id, .. } if id.as_str() == "nested"
    ));
}

/// A `mark_ready(); stop()` child reports its local stop and exit on
/// helper tasks, so one driver wake can collect both while the fired
/// readiness latch's Ready event is still undrained — and arbitration
/// orders the stop ahead of the readiness signal. `handle_self_stop`
/// must consult the fired latch before `begin_stop_child`'s Shutdown
/// step disarms the gate, or the clean post-ready exit is misread as a
/// pre-ready stop and spuriously aborts startup.
#[crate::runtime::test]
async fn same_batch_self_stop_preserves_fired_readiness_for_startup() {
    let mut tree = Tree::new();
    tree.add_task(
        "ready-then-stop",
        TaskDef::new(|_| future::pending::<crate::ExitResult>())
            .readiness(Readiness::Manual)
            .expect("manual readiness is valid")
            .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid task");
    let fixture = OrderedScopeFixture::new(tree);
    let root = Arc::clone(&fixture.root);
    let key = fixture.children.keys().next().expect("one child plan");
    let (mut scope, _event_receiver) = fixture.with_next_ordered_start(Some(key)).build();

    scope.spawn_child(key);
    let active = scope.children[&key]
        .active
        .as_ref()
        .expect("spawned child is active");
    let incarnation = active.incarnation;
    // The application task fired its readiness latch before stopping;
    // the driver has not yet drained the corresponding Ready event.
    assert!(active.ready_signal.fire());
    active.abort_handle.abort();

    let mut pending = [
        Pending::Child(ChildEvent::Exited {
            child: key,
            incarnation,
            recorded: Some(Retained::new(RecordedOutcome::returned(Ok(())))),
            join: crate::runtime::JoinOutcome::Ok { value: () },
            cancellation: Cancellation::NotObserved,
            readiness_signal_seen: true,
        })
        .classified(),
        Pending::Child(ChildEvent::SelfStop {
            child: key,
            incarnation,
        })
        .classified(),
    ];
    arbitrate(&mut pending);
    assert!(
        matches!(pending[0].1, Pending::Child(ChildEvent::SelfStop { .. })),
        "the regression premise: arbitration orders the stop ahead of the exit"
    );
    for (_, event) in pending {
        match event {
            Pending::Child(ChildEvent::SelfStop { child, incarnation }) => {
                scope.handle_self_stop(child, incarnation)
            }
            Pending::Child(ChildEvent::Exited {
                child,
                incarnation,
                recorded,
                join,
                cancellation,
                readiness_signal_seen,
            }) => scope.handle_exit(
                child,
                incarnation,
                recorded,
                join,
                cancellation,
                readiness_signal_seen,
            ),
            _ => unreachable!("the fixture queues only the stop and the exit"),
        }
    }

    let child = recv_construction_disposed(
        &mut scope.disposal_event_receiver,
        DRIVER_PROGRESS_WAIT,
        "the construction disposal completion",
    )
    .await;
    scope.handle_construction_disposed(child);

    assert!(
        scope.supervisor.lifecycle().startup_complete(),
        "the ready-before-stop child completes startup"
    );
    assert!(
        matches!(root.record().startup, Some(Ok(()))),
        "a fired readiness latch must survive a same-batch local stop: {:?}",
        root.record().startup
    );
    assert_eq!(root.record().state, ScopeState::Running);
    assert!(matches!(
        scope.children[&key].slot.member.record().stage,
        MemberStage::Terminal(ref exit) if matches!(exit.kind(), ExitKind::Completed)
    ));
    assert!(
        !scope.children[&key].slot.member.record().startup_aborted,
        "a post-ready clean self-stop is not a startup abort"
    );
}

/// Shutdown outranks a queued readiness signal (§13). Once drain begins,
/// even an already-fired latch cannot publish readiness (B.1/B.2).
#[crate::runtime::test]
async fn drain_stop_suppresses_an_already_fired_readiness_latch() {
    let mut tree = Tree::new();
    tree.add_task(
        "gate",
        TaskDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok(())
        })
        .readiness(Readiness::Manual)
        .expect("manual readiness is valid")
        .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid task");
    let fixture = OrderedScopeFixture::new(tree);
    let root = Arc::clone(&fixture.root);
    let key = fixture.children.keys().next().expect("one child plan");
    let (mut scope, _event_receiver) = fixture.with_next_ordered_start(Some(key)).build();

    scope.spawn_child(key);
    let active = scope.children[&key]
        .active
        .as_ref()
        .expect("spawned child is active");
    // The application task marked ready; the driver has not yet drained the
    // corresponding Ready event.
    let incarnation = active.incarnation;
    assert!(active.ready_signal.fire());
    let mut lifecycle = root.subscribe_lifecycle();

    scope.begin_drain(StopReason::ShutdownRequested);
    // The queued signal must remain inert when arbitration delivers it later.
    scope.handle_ready(key, incarnation);

    assert!(
        matches!(
            scope.children[&key].slot.member.record().stage,
            MemberStage::Stopping
        ),
        "the regression premise: the drain stopped the gated child"
    );
    assert!(
        !scope.supervisor.initial_ready(key),
        "shutdown wins before the queued readiness can be credited"
    );
    let mut published = Vec::new();
    while let Ok(crate::cells::LifecycleItem::Event(event)) = lifecycle.try_recv() {
        published.push(event.kind);
    }
    assert!(
        !published
            .iter()
            .any(|kind| matches!(kind, LifecycleEventKind::Ready { .. })),
        "readiness must not be published after drain begins: {published:?}"
    );
}

/// A readiness effect carrying a superseded incarnation is inert: it credits
/// no startup and promotes no member, even though the key is still resident.
#[crate::runtime::test]
async fn stale_incarnation_readiness_effect_does_not_credit_startup() {
    let mut tree = Tree::new();
    tree.add_task(
        "gate",
        TaskDef::new(|_| future::pending::<crate::ExitResult>())
            .readiness(Readiness::Manual)
            .expect("manual readiness is valid")
            .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid task");
    let fixture = OrderedScopeFixture::new(tree);
    let key = fixture.children.keys().next().expect("one child plan");
    let (mut scope, _event_receiver) = fixture.with_next_ordered_start(Some(key)).build();

    scope.spawn_child(key);
    let live = scope.children[&key]
        .active
        .as_ref()
        .expect("spawned child is active")
        .incarnation;
    let mut incarnations =
        IncarnationCounter::fixture(scope.children[&key].slot.member.membership());
    let stale = std::iter::repeat_with(|| incarnations.mint())
        .find(|incarnation| *incarnation != live)
        .expect("a second incarnation is mintable");

    let promoted =
        scope.apply_readiness_effect(key, stale, crate::engine::ReadinessEffect::BecameReady);

    assert!(!promoted, "a stale incarnation promotes nothing");
    assert!(
        !scope.supervisor.initial_ready(key),
        "a stale readiness effect must not credit startup"
    );
    assert!(!scope.supervisor.lifecycle().startup_complete());
    assert!(matches!(
        scope.children[&key].slot.member.record().stage,
        MemberStage::Starting
    ));
}

/// `next_ordered_start` is held across `spawn_child` and is never cleared by
/// `reclaim_child`, so `settle_supervisor` must treat a reclaimed key the way
/// `stop_next_ordered` treats its own cursor: already gone, advance past it.
/// Ordered scopes carry no dynamic control today and so never reclaim, which
/// is exactly why this is pinned here — the arena is a monotonic key domain,
/// so `keys_after` still ranges correctly over a removed key.
#[crate::runtime::test]
async fn ordered_startup_advances_past_a_reclaimed_cursor() {
    let mut tree = Tree::new();
    tree.add_task(
        "gone",
        TaskDef::new(|_| future::pending::<crate::ExitResult>())
            .readiness(Readiness::Manual)
            .expect("manual readiness is valid")
            .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid task");
    tree.add_task(
        "next",
        TaskDef::new(|_| future::pending::<crate::ExitResult>())
            .readiness(Readiness::Manual)
            .expect("manual readiness is valid")
            .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid task");

    let fixture = OrderedScopeFixture::new(tree);
    let gone = fixture.children.keys().next().expect("first ordered child");
    let next = fixture
        .children
        .keys()
        .nth(1)
        .expect("second ordered child");
    let (mut scope, _event_receiver) = fixture.with_next_ordered_start(Some(gone)).build();

    // Retire the authoritative record and runtime resource together, exactly
    // as production reclaim does, without disturbing the child that follows.
    scope.reduce(SupervisorEvent::DisposalStarted { child: gone });
    scope.reduce(SupervisorEvent::Terminalized { child: gone });
    scope
        .children
        .remove(&gone)
        .expect("the cursor's child is live before the reclaim");
    scope.reduce(SupervisorEvent::Reclaim { child: gone });

    scope.settle_supervisor();

    assert_eq!(
        scope.supervisor.next_ordered_start(),
        Some(next),
        "a vacated cursor advances to the next live child instead of panicking"
    );
    assert!(
        scope.children[&next].active.is_some(),
        "the following child starts at its ordered turn"
    );
    assert!(
        !scope.supervisor.lifecycle().startup_complete(),
        "the live successor still gates the aggregate"
    );
}

/// A removal response is an observation boundary, not merely a wake. Keep it
/// pending after membership commit until startup has been recomputed over the
/// shrunken declared set, and make the synchronous driver epilogue explicitly
/// discharge the retained completion if unwind bypasses the batch epilogue.
#[crate::runtime::test]
async fn startup_removal_response_follows_recomputation_and_drop_discharges_it() {
    let mut tree = DynamicTree::new();
    tree.add_task(
        "gate",
        TaskDef::new(|_| future::pending::<crate::ExitResult>())
            .readiness(Readiness::Manual)
            .expect("manual readiness is valid")
            .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid initial member");

    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = ScopeEpochGuard::begin(&root).expect("test scope epoch is available");
    root.member
        .update(|record| record.stage = MemberStage::Running);
    let (events, _event_receiver) = crate::runtime::unbounded_mpsc();
    let (control_events, _control_event_receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(control_events);
    let mut children = ChildArena::default();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let key = children.insert(child);
    let member = Arc::clone(&children[key].slot.member);
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events)
        .with_defaults(plan.defaults.clone())
        .with_children(children)
        .with_dynamic(Some(control))
        .with_transferred_plan(plan)
        .build();

    assert!(scope.terminalize_child(
        key,
        Exit::never_started(),
        None,
        StartupDisposition::NotAborted,
    ));
    let mut removal = super::super::remove_dynamic(&root, member.id(), Some(member.membership()));
    scope.finalize_removal(key);

    assert_eq!(
        removal.try_receive(),
        None,
        "membership commit alone cannot publish Removed before settlement"
    );
    assert!(!scope.supervisor.lifecycle().startup_complete());

    scope.settle_supervisor();
    assert!(scope.supervisor.lifecycle().startup_complete());
    assert_eq!(root.record().startup, Some(Ok(())));
    assert_eq!(
        removal.try_receive(),
        None,
        "the fixture retains the completion until the explicit drop epilogue"
    );
    drop(scope);
    assert_eq!(removal.try_receive(), Some(RemoveOutcome::Removed));
}

/// A latched removal outranks readiness structurally, and arbitration alone
/// cannot deliver that: `SelfStop` shares `MembershipRemoval` with `Removal`
/// and the primary lane is collected first, so the queued removal can never
/// preempt the readiness `handle_self_stop` replays. Publication consults the
/// removal sources at execution time instead — without that, a `Ready` is
/// published for a membership the owner already observes as `Removing`, and
/// it completes scope startup on a member that is leaving.
#[crate::runtime::test]
async fn queued_removal_suppresses_replayed_self_stop_readiness() {
    let mut tree = DynamicTree::new();
    tree.add_task(
        "gate",
        TaskDef::new(|_| future::pending::<crate::ExitResult>())
            .readiness(Readiness::Manual)
            .expect("manual readiness is valid")
            .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid initial member");

    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = ScopeEpochGuard::begin(&root).expect("test scope epoch is available");
    root.member
        .update(|record| record.stage = MemberStage::Running);
    let (events, _event_receiver) = crate::runtime::unbounded_mpsc();
    let (control_events, mut control_event_receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(control_events);
    let mut children = ChildArena::default();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let key = children.insert(child);
    let member = Arc::clone(&children[key].slot.member);
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events)
        .with_defaults(plan.defaults.clone())
        .with_children(children)
        .with_dynamic(Some(control))
        .with_transferred_plan(plan)
        .build();

    scope.spawn_child(key);
    let active = scope.children[&key]
        .active
        .as_ref()
        .expect("the spawned child is active");
    let incarnation = active.incarnation;
    // The application task fired readiness and then stopped; the driver has
    // not drained the corresponding Ready event yet.
    assert!(active.ready_signal.fire());

    let mut lifecycle = root.subscribe_lifecycle();
    let _removal = super::super::remove_dynamic(&root, member.id(), Some(member.membership()));
    assert_eq!(
        member.record().membership_status,
        MembershipStatus::Removing,
        "remove() latches Removing in the caller's observation gate"
    );

    // The premise: full-drain collection leads with the primary lane, so the
    // self-stop sorts ahead of the removal it shares a class with.
    let mut pending = vec![
        Pending::Child(ChildEvent::SelfStop {
            child: key,
            incarnation,
        })
        .classified(),
    ];
    while let Some(event) = control_event_receiver.try_recv() {
        pending.push(Pending::from(event).classified());
    }
    arbitrate(&mut pending);
    assert_eq!(
        pending.len(),
        2,
        "the removal request reached the control lane"
    );
    assert!(
        matches!(pending[0].1, Pending::Child(ChildEvent::SelfStop { .. })),
        "the premise: arbitration cannot order the removal ahead of the self-stop"
    );
    assert!(matches!(pending[1].1, Pending::Removal(_)));

    scope.handle_self_stop(key, incarnation);

    let mut published = Vec::new();
    while let Ok(crate::cells::LifecycleItem::Event(event)) = lifecycle.try_recv() {
        published.push(event.kind);
    }
    assert!(
        !published
            .iter()
            .any(|kind| matches!(kind, LifecycleEventKind::Ready { .. })),
        "no readiness edge is published for an already-Removing membership: {published:?}"
    );
    assert!(
        !matches!(member.record().stage, MemberStage::Running),
        "the suppressed edge does not project Running: {:?}",
        member.record().stage
    );
    assert!(
        !scope.supervisor.initial_ready(key),
        "a leaving membership is not credited to the startup aggregate"
    );
    assert!(!scope.supervisor.lifecycle().startup_complete());
    scope.settle_supervisor();
    assert!(
        !scope.supervisor.lifecycle().startup_complete(),
        "startup waits for the removal to shrink the declared set"
    );
    assert_eq!(root.record().state, ScopeState::Starting);
}

/// A removal mark wins even before its control event reaches the driver.
/// The terminal record must agree with the successful startup recomputation:
/// withdrawing a pre-ready initial member is not a startup failure (§7/B.6).
#[crate::runtime::test]
async fn removal_before_pre_ready_exit_does_not_publish_startup_abort() {
    let mut tree = DynamicTree::new();
    let _ = tree
        .add_task_once(
            "gate",
            TaskOnceDef::new(|_| future::pending::<crate::ExitResult>())
                .readiness(Readiness::Manual)
                .expect("manual readiness is valid")
                .readiness_deadline(ReadinessDeadline::Unbounded),
        )
        .expect("valid initial member");

    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = ScopeEpochGuard::begin(&root).expect("test scope epoch is available");
    root.member
        .update(|record| record.stage = MemberStage::Running);
    let (events, _event_receiver) = crate::runtime::unbounded_mpsc();
    let (control_events, _control_event_receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(control_events);
    let mut children = ChildArena::default();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let key = children.insert(child);
    let member = Arc::clone(&children[key].slot.member);
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events)
        .with_defaults(plan.defaults.clone())
        .with_children(children)
        .with_dynamic(Some(control))
        .with_transferred_plan(plan)
        .build();

    scope.spawn_child(key);
    let active = scope.children[&key].active.as_ref().expect("active child");
    let incarnation = active.incarnation;
    active.abort_handle.abort();
    let mut removal = super::super::remove_dynamic(&root, member.id(), Some(member.membership()));
    assert_eq!(
        member.record().membership_status,
        MembershipStatus::Removing
    );
    assert_eq!(
        scope.supervisor.membership_status(key),
        MembershipStatus::Active
    );

    scope.handle_exit(
        key,
        incarnation,
        Some(Retained::new(RecordedOutcome::returned(Err(
            ExitError::message("pre-ready failure racing removal"),
        )))),
        crate::runtime::JoinOutcome::Ok { value: () },
        Cancellation::NotObserved,
        false,
    );

    assert!(matches!(member.record().stage, MemberStage::Terminal(_)));
    assert!(
        !member.record().startup_aborted,
        "removal is not a startup abort"
    );
    scope.handle_removal(RemovalRequest { key });
    scope.settle_supervisor();
    scope.publish_startup_removals();
    assert_eq!(removal.try_receive(), Some(RemoveOutcome::Removed));
    assert!(scope.supervisor.lifecycle().startup_complete());
}

/// Pins main's linearization for a scope stop latched cross-batch: a
/// restartable initial child failing pre-ready still dispatches
/// `ScheduleRestart`, and the latched stop's own follow-up event owns
/// the startup verdict (`ShutdownRequested`). Exit dispatch must not
/// consult latched-but-unprocessed scope-stop sources for its membership
/// classification, or the failure would be rerouted into
/// `StartupFailed` while restart suppression claims the stop was first.
#[crate::runtime::test]
async fn latched_shutdown_keeps_the_startup_verdict_for_its_follow_up_event() {
    let mut tree = Tree::new();
    tree.add_task(
        "worker",
        TaskDef::new(|_| async { Err(ExitError::message("failed before readiness")) })
            .readiness(Readiness::Manual)
            .expect("manual readiness is valid"),
    )
    .expect("valid task");
    let fixture = OrderedScopeFixture::new(tree);
    let root = Arc::clone(&fixture.root);
    let key = fixture.children.keys().next().expect("one child plan");
    let (mut scope, mut event_receiver) = fixture.with_next_ordered_start(Some(key)).build();

    assert!(scope.supervisor.is_initial(key));
    scope.spawn_child(key);
    assert!(!scope.supervisor.initial_ready(key));
    let exit = recv_child_exit(
        &mut event_receiver,
        Duration::from_secs(2),
        "the pre-ready failure exit",
    )
    .await;

    // The stop request latches after this batch was collected: it is
    // visible to `has_stop_request`, but its `Pending::Shutdown` follow-up
    // event belongs to the next batch.
    root.request_shutdown();
    assert!(root.has_stop_request(scope.epoch));

    exit.dispatch(&mut scope);
    assert!(
        scope.children[&key].restart_deadline.is_some(),
        "a latched scope stop does not reclassify exit dispatch"
    );
    assert!(matches!(
        scope.children[&key].slot.member.record().stage,
        MemberStage::Restarting
    ));
    assert!(
        root.record().startup.is_none(),
        "the pre-ready failure must not claim the startup verdict: {:?}",
        root.record().startup
    );

    // The latched stop's guaranteed follow-up event runs in the next
    // batch and owns the verdict, exactly as an unlatched scope would.
    assert!(root.take_shutdown_request(scope.epoch));
    scope.begin_drain(StopReason::ShutdownRequested);
    assert!(
        matches!(
            root.record().startup,
            Some(Err(StartupError::ShutdownRequested))
        ),
        "the latched stop owns the startup verdict: {:?}",
        root.record().startup
    );
    assert!(scope.children[&key].restart_deadline.is_none());

    let child = recv_construction_disposed(
        &mut scope.disposal_event_receiver,
        DRIVER_PROGRESS_WAIT,
        "the construction disposal completion",
    )
    .await;
    scope.handle_construction_disposed(child);
    assert!(matches!(
        scope.children[&key].slot.member.record().stage,
        MemberStage::Terminal(_)
    ));
    assert!(
        !scope.children[&key].slot.member.record().startup_aborted,
        "shutdown-first linearization publishes no startup abort"
    );
    assert!(matches!(
        root.record().startup,
        Some(Err(StartupError::ShutdownRequested))
    ));
}

#[derive(Clone, Copy)]
enum PreLoopStopSource {
    ScopeShutdown,
    ScopeForce,
    AncestorShutdown,
}

async fn assert_pre_loop_stop_upgrades_a_nested_lowering_failure(source: PreLoopStopSource) {
    let nested_id = ChildId::from("nested");
    let mut parent_identity = ScopeIdentity::new();
    let nested_membership = parent_identity.mint_membership(&nested_id);
    let nested_member = MemberCell::new(nested_membership);

    let scope = ScopeCell::new(nested_member, ScopeFlavor::Ordered, ScopeIdentity::new());

    let mut tree = Tree::new();
    let worker = tree
        .add_task(
            "worker",
            TaskDef::new(|_| future::pending::<crate::ExitResult>()),
        )
        .expect("valid task");
    let _undefined = tree
        .reserve_task("missing")
        .expect("an undefined reservation fails lowering");
    let epoch = ScopeEpochGuard::begin(&scope).expect("the first nested epoch is available");
    let ancestor_shutdown = Latch::default();
    let child_shutdown = Latch::default();
    match source {
        PreLoopStopSource::ScopeShutdown => {
            let target = scope.request_shutdown();
            assert_eq!(target, epoch.epoch());
        }
        PreLoopStopSource::ScopeForce => scope.force_shutdown(epoch.epoch()),
        PreLoopStopSource::AncestorShutdown => {
            // Parent cancellation publishes the user-facing edge separately
            // from the nested driver's framework-only observation edge. The
            // fixture deliberately withholds that separate parent-published
            // edge so the post-call assertion observes only what this path
            // fires: the ancestor arm must not couple its framework observer
            // back to user-installable cancellation waiters.
            ancestor_shutdown.fire();
        }
    }
    let ready = CompletionGatedLatch::default();
    let result = super::super::run_nested_tree_with_epoch(
        tree.into_core_for_test(),
        Arc::clone(&scope),
        crate::policy::ResolvedDefaults::default(),
        NestedScopeLatches {
            parent_ready: ready.clone(),
            child_shutdown: child_shutdown.clone(),
            ancestor: AncestorCommandLatches {
                framework_shutdown: ancestor_shutdown.clone(),
                abort: Latch::default(),
                abort_ack: Latch::default(),
            },
        },
        epoch,
    )
    .await;

    assert!(result.is_ok());
    match source {
        PreLoopStopSource::ScopeShutdown | PreLoopStopSource::ScopeForce => assert!(
            child_shutdown.is_fired(),
            "a self-requested stop fires the user-facing latch that classifies the exit as cancelled"
        ),
        PreLoopStopSource::AncestorShutdown => assert!(
            !child_shutdown.is_fired(),
            "an ancestor-driven stop leaves the user-facing latch to the parent that published it"
        ),
    }
    assert!(matches!(
        scope.record().startup,
        Some(Err(StartupError::ShutdownRequested))
    ));
    assert!(matches!(
        scope.record().state,
        ScopeState::Stopped {
            reason: StopReason::ShutdownRequested
        }
    ));
    assert!(!ready.is_fired());
    assert!(matches!(worker.wait().await.kind(), ExitKind::NeverStarted));
}

#[crate::runtime::test]
async fn pre_loop_shutdown_upgrades_a_nested_lowering_failure() {
    assert_pre_loop_stop_upgrades_a_nested_lowering_failure(PreLoopStopSource::ScopeShutdown).await;
}

#[crate::runtime::test]
async fn pre_loop_force_upgrades_a_nested_lowering_failure() {
    assert_pre_loop_stop_upgrades_a_nested_lowering_failure(PreLoopStopSource::ScopeForce).await;
}

#[crate::runtime::test]
async fn pre_loop_ancestor_shutdown_upgrades_a_nested_lowering_failure() {
    assert_pre_loop_stop_upgrades_a_nested_lowering_failure(PreLoopStopSource::AncestorShutdown)
        .await;
}
