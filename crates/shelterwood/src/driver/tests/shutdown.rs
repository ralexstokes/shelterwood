use super::support::*;

struct BlockingFactoryDrop(Arc<FactoryGate>);

impl Drop for BlockingFactoryDrop {
    fn drop(&mut self) {
        self.0.block();
    }
}

const ARRIVED_FACTORY_DISPOSAL_PANIC: &str = "arrived factory disposal panic";

struct PanickingFactoryDrop;

impl Drop for PanickingFactoryDrop {
    fn drop(&mut self) {
        panic!("{ARRIVED_FACTORY_DISPOSAL_PANIC}");
    }
}

async fn scope_with_arrived_factory_disposal_panic() -> (ScopeRuntime, ChildKey, Arc<MemberCell>) {
    let mut tree = Tree::new();
    tree.add_task(
        "worker",
        TaskDef::new({
            let capture = PanickingFactoryDrop;
            move |_| {
                let _ = &capture;
                future::pending::<crate::ExitResult>()
            }
        })
        .restart(RestartPolicy::new(
            RestartCondition::Never,
            Backoff::Immediate,
        ))
        .retention(Retention::Retain),
    )
    .expect("valid task");
    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = root
        .begin_incarnation(ScopeState::Starting)
        .expect("test scope epoch is available");
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_state_and_startup(ScopeState::Running, Ok(()));
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    let (events, mut event_receiver) = crate::runtime::unbounded_mpsc();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let member = Arc::clone(&child.slot.member);
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntimeBuilder::new(root, epoch, events)
        .with_defaults(plan.defaults.clone())
        .with_children(children)
        .with_lifecycle(ScopeLifecycle::running())
        .build();
    plan.finish_transfer();

    scope.spawn_child(key);
    let active = scope.children[key]
        .active
        .as_ref()
        .expect("worker is active");
    let incarnation = active.incarnation;
    active.abort_handle.abort();
    let _joined = recv_child_exit(
        &mut event_receiver,
        DRIVER_PROGRESS_WAIT,
        "the aborted fixture task to join",
    )
    .await;
    // `handle_exit` is a post-join operation in production. Wait for that
    // boundary before injecting the synthetic application verdict so the
    // spawned body has released its factory clone and retained-construction
    // disposal is the sole owner whose panic this fixture observes.
    scope.handle_exit(
        key,
        incarnation,
        Some(RecordedOutcome::returned(Err(ExitError::message(
            "application failure",
        )))),
        crate::runtime::JoinOutcome::Ok { value: () },
        Cancellation::NotObserved,
        false,
    );
    assert!(scope.children[key].pending_terminal.is_some());

    let arrived = crate::runtime::timeout(DRIVER_PROGRESS_WAIT, async {
        while crate::runtime::unbounded_mpsc_is_empty(&scope.disposal_event_receiver) {
            crate::runtime::yield_now().await;
        }
    })
    .await;
    assert!(matches!(arrived, crate::runtime::Timeout::Completed(())));
    (scope, key, member)
}

#[crate::runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_teardown_finishes_the_cancelled_root_driver() {
    let plan = DynamicTree::new().lower_for_test();
    let root = Arc::clone(&plan.root);
    let (hosted, driver) = DedicatedRuntime::spawn(run_scope(plan, ScopeRole::Root));
    let monitor = monitor_root_driver(Arc::clone(&root), driver);
    root.wait_started().await.expect("root starts");
    let wakes = Arc::new(AtomicUsize::new(0));
    let stopped_waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
    let mut stopped = Box::pin(root.wait_stopped());
    assert!(
        stopped
            .as_mut()
            .poll(&mut Context::from_waker(&stopped_waker))
            .is_pending(),
        "the live root observer parks before runtime teardown"
    );

    hosted.shutdown().await;

    let monitored =
        crate::runtime::timeout(DRIVER_PROGRESS_WAIT, crate::runtime::join(monitor)).await;
    assert!(matches!(
        monitored,
        crate::runtime::Timeout::Completed(crate::runtime::JoinOutcome::Ok { ref value })
            if value.as_reason() == &StopReason::ShutdownRequested
    ));
    assert!(
        wakes.load(Ordering::SeqCst) > 0,
        "the join monitor wakes an observer parked before cancellation"
    );
    assert!(matches!(
        stopped
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(StopReason::ShutdownRequested)
    ));
    assert!(matches!(
        root.member.record().stage,
        MemberStage::Terminal(ref exit) if exit.cancellation() == Cancellation::Observed
    ));
}

#[crate::runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_teardown_publishes_the_exit_awaiting_factory_disposal() {
    const FAILURE: &str = "distinctive application failure";

    let gate = Arc::new(FactoryGate::default());
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "worker",
            TaskDef::new({
                let capture = BlockingFactoryDrop(Arc::clone(&gate));
                move |_| {
                    let _ = &capture;
                    async { Err(ExitError::message(FAILURE)) }
                }
            })
            .restart(RestartPolicy::new(
                RestartCondition::Never,
                Backoff::Immediate,
            ))
            .retention(Retention::Retain),
        )
        .expect("valid task");
    let plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let mut events = root.subscribe_lifecycle();
    let (hosted, driver) = DedicatedRuntime::spawn(run_scope(plan, ScopeRole::Root));

    root.wait_started().await.expect("root starts");
    assert!(matches!(
        crate::runtime::timeout(DRIVER_PROGRESS_WAIT, gate.wait_entered()).await,
        crate::runtime::Timeout::Completed(())
    ));
    let pending = task.cell.record();

    // Dropping this dedicated runtime destroys the scope-driver future while
    // the blocking pool remains held in the retained factory's destructor.
    // Run teardown concurrently so the test can observe the synchronous
    // driver epilogue before allowing that destructor to finish.
    let teardown = crate::runtime::spawn(hosted.shutdown());
    let publication = crate::runtime::timeout(DRIVER_PROGRESS_WAIT, async {
        let terminal = task.wait().await;
        let lifecycle = loop {
            let Some(item) = events.recv().await else {
                panic!("driver teardown closed lifecycle without an Exited event")
            };
            if let LifecycleItem::Event(event) = item
                && let LifecycleEventKind::Exited { id, exit, .. } = event.kind
                && id.as_str() == "worker"
            {
                break exit;
            }
        };
        (terminal, lifecycle, task.cell.record())
    })
    .await;

    // Always unblock the runtime thread before asserting the publication, so
    // a regression fails promptly rather than waiting for the gate backstop.
    gate.release();
    let teardown =
        crate::runtime::timeout(DRIVER_PROGRESS_WAIT, crate::runtime::join(teardown)).await;
    let driver = crate::runtime::timeout(DRIVER_PROGRESS_WAIT, crate::runtime::join(driver)).await;

    assert!(matches!(
        teardown,
        crate::runtime::Timeout::Completed(crate::runtime::JoinOutcome::Ok { value: () })
    ));
    assert!(matches!(
        driver,
        crate::runtime::Timeout::Completed(crate::runtime::JoinOutcome::Cancelled)
    ));
    assert!(matches!(pending.stage, MemberStage::Running));
    assert_eq!(pending.last_exit, None);
    let crate::runtime::Timeout::Completed((terminal, lifecycle, record)) = publication else {
        panic!("driver teardown did not publish the pending terminal exit")
    };
    let MemberStage::Terminal(recorded) = record.stage else {
        panic!("driver teardown did not terminalize the child membership")
    };
    for exit in [&terminal, &lifecycle, &recorded] {
        assert!(matches!(
            exit.kind(),
            ExitKind::Failed(error) if error.to_string() == FAILURE
        ));
        assert_eq!(exit.cancellation(), Cancellation::NotObserved);
    }
    assert_eq!(lifecycle, terminal);
    assert_eq!(recorded, terminal);
    assert_eq!(record.last_exit, Some(terminal));
}

#[crate::runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_teardown_folds_an_arrived_factory_disposal_panic() {
    let (scope, _key, member) = scope_with_arrived_factory_disposal_panic().await;

    drop(scope);

    assert_arrived_disposal_panic_published(&member);
}

fn assert_arrived_disposal_panic_published(member: &Arc<MemberCell>) {
    assert!(matches!(
        member.record().stage,
        MemberStage::Terminal(ref exit)
            if matches!(
                exit.kind(),
                ExitKind::Panicked { message }
                    if message.as_deref() == Some(ARRIVED_FACTORY_DISPOSAL_PANIC)
            )
    ));
}

/// The lane is empty by the time a forced batch dispatches: collection
/// already moved the completion into the batch, where `Pending::Force`
/// (`ScopeShutdown`) outranks it (`ChildExit`). Staging is what keeps it
/// reachable from the hard-force fallback.
#[crate::runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn hard_force_folds_a_batch_collected_factory_disposal_panic() {
    let (mut scope, key, member) = scope_with_arrived_factory_disposal_panic().await;

    let mut pending = vec![Pending::Force.classified()];
    collect_driver_events(&mut scope.disposal_event_receiver, 8, &mut pending);
    scope.stage_batch_disposal_panics(&mut pending);
    arbitrate(&mut pending);
    let mut batch = pending.into_iter().map(|(_, event)| event);
    assert!(matches!(batch.next(), Some(Pending::Force)));

    scope.force_all();

    assert!(!scope.supervisor.is_disposing(key));
    assert_arrived_disposal_panic_published(&member);

    // The batch's own entry still dispatches, against an already-taken
    // `pending_terminal`, and must not disturb the published verdict.
    let Some(Pending::Child(ChildEvent::ConstructionDisposed { child, panic })) = batch.next()
    else {
        panic!("the batch collected the construction-disposal completion")
    };
    assert_eq!(child, key);
    let panic = panic.or_else(|| scope.take_arrived_disposal_panic(child));
    scope.handle_construction_disposed(child, panic);
    assert_arrived_disposal_panic_published(&member);
}

#[crate::runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn hard_force_folds_an_arrived_factory_disposal_panic() {
    let (mut scope, key, member) = scope_with_arrived_factory_disposal_panic().await;

    scope.force_all();

    assert!(!scope.supervisor.is_disposing(key));
    assert_arrived_disposal_panic_published(&member);
}

#[crate::runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn hard_force_preserves_the_first_terminal_observer_panic_across_its_fallback() {
    const FIRST_PANIC: &str = "arrived disposal terminal observer panic";
    const SECOND_PANIC: &str = "hard-force fallback terminal observer panic";

    let gate = Arc::new(FactoryGate::default());
    let mut tree = Tree::new();
    tree.add_task(
        "arrived",
        TaskDef::new({
            let capture = PanickingFactoryDrop;
            move |_| {
                let _ = &capture;
                future::pending::<crate::ExitResult>()
            }
        })
        .restart(RestartPolicy::new(
            RestartCondition::Never,
            Backoff::Immediate,
        ))
        .retention(Retention::Retain),
    )
    .expect("valid arrived child");
    tree.add_task(
        "fallback",
        TaskDef::new({
            let capture = BlockingFactoryDrop(Arc::clone(&gate));
            move |_| {
                let _ = &capture;
                future::pending::<crate::ExitResult>()
            }
        })
        .restart(RestartPolicy::new(
            RestartCondition::Never,
            Backoff::Immediate,
        ))
        .retention(Retention::Retain),
    )
    .expect("valid fallback child");

    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = root
        .begin_incarnation(ScopeState::Starting)
        .expect("test scope epoch is available");
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_state_and_startup(ScopeState::Running, Ok(()));
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    let (events, mut event_receiver) = crate::runtime::unbounded_mpsc();
    let mut children = ChildArena::default();
    let mut arrived = None;
    let mut fallback = None;
    for child in plan.children.drain(..) {
        let id = child.slot.member.id().as_str().to_owned();
        let member = Arc::clone(&child.slot.member);
        let key = children
            .insert(ChildRuntime::from_plan(child, &root))
            .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
        match id.as_str() {
            "arrived" => arrived = Some((key, member)),
            "fallback" => fallback = Some((key, member)),
            _ => unreachable!("the fixture declares exactly two known children"),
        }
    }
    let (arrived_key, arrived_member) = arrived.expect("arrived child is present");
    let (fallback_key, fallback_member) = fallback.expect("fallback child is present");
    let mut scope = ScopeRuntimeBuilder::new(root, epoch, events)
        .with_defaults(plan.defaults.clone())
        .with_children(children)
        .with_lifecycle(ScopeLifecycle::running())
        .build();
    plan.finish_transfer();

    for key in [arrived_key, fallback_key] {
        scope.spawn_child(key);
        scope.children[key]
            .active
            .as_ref()
            .expect("child is active")
            .abort_handle
            .abort();
    }
    for _ in 0..2 {
        recv_child_exit(
            &mut event_receiver,
            DRIVER_PROGRESS_WAIT,
            "an aborted fixture child to join",
        )
        .await
        .dispatch(&mut scope);
    }
    gate.wait_entered().await;
    let arrived_completion = crate::runtime::timeout(DRIVER_PROGRESS_WAIT, async {
        while crate::runtime::unbounded_mpsc_is_empty(&scope.disposal_event_receiver) {
            crate::runtime::yield_now().await;
        }
    })
    .await;
    assert!(matches!(
        arrived_completion,
        crate::runtime::Timeout::Completed(())
    ));

    let mut arrived_terminal = Box::pin(arrived_member.wait_terminal());
    let mut fallback_terminal = Box::pin(fallback_member.wait_terminal());
    assert!(
        arrived_terminal
            .as_mut()
            .poll(&mut Context::from_waker(&Waker::from(Arc::new(PanicWake(
                FIRST_PANIC
            )))))
            .is_pending()
    );
    assert!(
        fallback_terminal
            .as_mut()
            .poll(&mut Context::from_waker(&Waker::from(Arc::new(PanicWake(
                SECOND_PANIC
            )))))
            .is_pending()
    );

    let result = catch_unwind(AssertUnwindSafe(|| scope.force_child(fallback_key)));
    gate.release();

    let payload = result.expect_err("both hostile terminal wakes are contained");
    assert_eq!(
        payload.downcast_ref::<&'static str>().copied(),
        Some(FIRST_PANIC),
        "the earlier arrived-disposal panic remains primary"
    );
    for member in [&arrived_member, &fallback_member] {
        assert!(matches!(member.record().stage, MemberStage::Terminal(_)));
    }
}

#[crate::runtime::test]
async fn latched_shutdown_upgrades_an_intensity_drain() {
    let mut tree = Tree::new();
    tree.intensity(Intensity::new(0, Duration::from_secs(10)).expect("valid intensity"));
    tree.add_task(
        "worker",
        TaskDef::new(|_| future::pending::<crate::ExitResult>()),
    )
    .expect("valid task");
    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = root
        .begin_incarnation(ScopeState::Starting)
        .expect("test scope epoch is available");
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    root.set_state(ScopeState::Running);
    root.set_startup(Ok(()));
    let (events, _event_receiver) = crate::runtime::unbounded_mpsc();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events)
        .with_defaults(plan.defaults.clone())
        .with_intensity_policy(plan.intensity_policy())
        .with_children(children)
        .with_lifecycle(ScopeLifecycle::running())
        .build();
    plan.finish_transfer();

    scope.spawn_child(key);
    let active = scope.children[key]
        .active
        .as_ref()
        .expect("worker is active");
    let incarnation = active.incarnation;
    active.abort_handle.abort();

    // Model a shutdown request that latches after this pass sampled the
    // control plane but before the collected child exit is dispatched.
    assert!(root.request_shutdown().is_some());
    scope.handle_exit(
        key,
        incarnation,
        Some(RecordedOutcome::returned(Err(ExitError::message(
            "trip intensity",
        )))),
        crate::runtime::JoinOutcome::Ok { value: () },
        Cancellation::NotObserved,
        false,
    );
    assert!(matches!(
        scope.supervisor.lifecycle().draining_reason(),
        Some(StopReason::IntensityTripped(_))
    ));

    // The request's next-pass follow-up owns the stronger verdict even
    // though teardown was already started by the intensity trip.
    assert!(root.take_shutdown_request(scope.epoch));
    scope.begin_drain(StopReason::ShutdownRequested);
    assert_eq!(
        scope.supervisor.lifecycle().draining_reason(),
        Some(&StopReason::ShutdownRequested)
    );
}

#[crate::runtime::test]
async fn force_upgrades_an_intensity_drain_to_shutdown_requested() {
    let mut tree = Tree::new();
    tree.intensity(Intensity::new(0, Duration::from_secs(10)).expect("valid intensity"));
    tree.add_task(
        "worker",
        TaskDef::new(|_| future::pending::<crate::ExitResult>()),
    )
    .expect("valid task");
    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = root
        .begin_incarnation(ScopeState::Starting)
        .expect("test scope epoch is available");
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    root.set_state(ScopeState::Running);
    root.set_startup(Ok(()));
    let (events, _event_receiver) = crate::runtime::unbounded_mpsc();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events)
        .with_defaults(plan.defaults.clone())
        .with_intensity_policy(plan.intensity_policy())
        .with_children(children)
        .with_lifecycle(ScopeLifecycle::running())
        .build();
    plan.finish_transfer();

    scope.spawn_child(key);
    let active = scope.children[key]
        .active
        .as_ref()
        .expect("worker is active");
    let incarnation = active.incarnation;
    active.abort_handle.abort();
    scope.handle_exit(
        key,
        incarnation,
        Some(RecordedOutcome::returned(Err(ExitError::message(
            "trip intensity",
        )))),
        crate::runtime::JoinOutcome::Ok { value: () },
        Cancellation::NotObserved,
        false,
    );
    assert!(matches!(
        scope.supervisor.lifecycle().draining_reason(),
        Some(StopReason::IntensityTripped(_))
    ));

    // Model the child driver collecting only the ancestor abort latch: force
    // runs on a scope already draining for the trip, without a processed
    // shutdown request having upgraded the reason first.
    scope.force_all();
    assert_eq!(
        scope.supervisor.lifecycle().draining_reason(),
        Some(&StopReason::ShutdownRequested)
    );
    assert_eq!(
        scope.finish_if_ready(),
        Some(StopReason::ShutdownRequested),
        "the same pass terminalizes with the forced verdict, not the stale trip"
    );
}

#[crate::runtime::test(start_paused = true)]
async fn force_uses_the_stop_funnel_for_every_ordered_child() {
    let mut tree = Tree::new();
    let first = tree
        .add_raw("first", crate::RawDef::factory(|| PendingRaw))
        .expect("valid first actor");
    let second = tree
        .add_raw("second", crate::RawDef::factory(|| PendingRaw))
        .expect("valid second actor");
    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = root
        .begin_incarnation(ScopeState::Starting)
        .expect("test scope epoch is available");
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    let (events, _event_receiver) = crate::runtime::unbounded_mpsc();
    let mut children = ChildArena::default();
    plan.children.reverse();
    while let Some(child) = plan.children.pop() {
        children
            .insert(ChildRuntime::from_plan(child, &root))
            .unwrap_or_else(|_| panic!("the fixture fits in the child-key domain"));
    }
    let keys = children.keys().collect::<Vec<_>>();
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events)
        .with_defaults(plan.defaults.clone())
        .with_intensity_policy(plan.intensity_policy())
        .with_children(children)
        .with_lifecycle(ScopeLifecycle::running())
        .build();
    plan.finish_transfer();

    for key in &keys {
        scope.spawn_child(*key);
    }
    let incarnations = keys
        .iter()
        .map(|key| {
            scope.children[*key]
                .active
                .as_ref()
                .expect("child is active")
                .incarnation
        })
        .collect::<Vec<_>>();

    scope.force_all();

    for key in &keys {
        let active = scope.children[*key]
            .active
            .as_ref()
            .expect("forced child remains active through the tidy beat");
        assert!(active.shutdown.is_fired(), "force sends cancellation");
        assert!(active.abort.is_fired(), "force immediately escalates");
    }
    assert_eq!(
        first.try_send(1).expect_err("first mailbox freezes").kind,
        SendErrorKind::NotRunning
    );
    assert_eq!(
        second.try_send(2).expect_err("second mailbox freezes").kind,
        SendErrorKind::NotRunning
    );

    // Model readiness messages that shared the driver's wake with force.
    // The force boundary disarmed both gates before either can publish a
    // late Running transition.
    for (key, incarnation) in keys.iter().zip(incarnations) {
        scope.handle_ready(*key, incarnation);
        assert!(matches!(
            scope.children[*key].slot.member.record().stage,
            MemberStage::Stopping
        ));
    }

    let deadlines = keys
        .iter()
        .map(|key| {
            scope.children[*key]
                .active
                .as_ref()
                .and_then(|active| active.ladder)
                .and_then(StopLadder::deadline)
                .expect("each ladder retains its tidy deadline")
        })
        .collect::<Vec<_>>();
    scope.force_all();
    for (key, deadline) in keys.iter().zip(deadlines) {
        assert_eq!(
            scope.children[*key]
                .active
                .as_ref()
                .and_then(|active| active.ladder)
                .and_then(StopLadder::deadline),
            Some(deadline),
            "repeated force cannot rewind or skip the ladder"
        );
    }
}

#[test]
fn forced_ordered_drain_advances_an_inactive_suffix_iteratively() {
    const CHILDREN: usize = 1_024;

    let mut tree = Tree::new();
    for index in 0..CHILDREN {
        tree.add_task(
            format!("inactive-{index}"),
            TaskDef::new(|_| future::pending()),
        )
        .expect("unique child declaration");
    }
    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = root
        .begin_incarnation(ScopeState::Starting)
        .expect("test scope epoch is available");
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    let (events, _event_receiver) = crate::runtime::unbounded_mpsc();
    let mut children = ChildArena::default();
    plan.children.reverse();
    while let Some(child) = plan.children.pop() {
        children
            .insert(ChildRuntime::from_plan(child, &root))
            .unwrap_or_else(|_| panic!("the fixture fits in the child-key domain"));
    }
    // Model restart-window children: no incarnation and no retained
    // construction remains, so forced terminalization completes inline.
    // This used to re-enter `stop_next_ordered` once per child.
    for child in children.values_mut() {
        drop(child.construction.take());
    }
    let mut scope = ScopeRuntimeBuilder::new(root, epoch, events)
        .with_defaults(plan.defaults.clone())
        .with_intensity_policy(plan.intensity_policy())
        .with_children(children)
        .with_lifecycle(ScopeLifecycle::running())
        .with_hard_forced(true)
        .build();
    plan.finish_transfer();

    scope.begin_drain(StopReason::ShutdownRequested);

    assert!(scope.children.values().all(ChildRuntime::is_terminal));
    assert_eq!(scope.supervisor.ordered_stop_waiting(), None);
    assert_eq!(
        scope.supervisor.ordered_stop_inspections(),
        CHILDREN,
        "the reverse cursor inspects each ordered child exactly once"
    );
    assert!(
        scope.supervisor.all_children_joined(),
        "completion is derived from authoritative child states"
    );
    assert_eq!(
        scope.finish_if_ready(),
        Some(StopReason::ShutdownRequested),
        "shutdown completion is decided from the maintained count"
    );
}
#[crate::runtime::test]
async fn system_shutdown_joins_root_driver_teardown() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    let root = system.scope();
    system.wait_started().await.expect("dynamic root starts");
    let control = root
        .as_scope()
        .cell
        .dynamic_route()
        .expect("running dynamic root has a control");
    let weak = Arc::downgrade(&control);
    drop(control);

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("empty dynamic root shuts down");

    assert!(
        weak.upgrade().is_none(),
        "shutdown returns only after root driver teardown drops dynamic state"
    );
}
