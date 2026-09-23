use super::support::*;
use crate::cells::RetainedExit;

struct BlockingFactoryDrop(Arc<FactoryGate>);

impl Drop for BlockingFactoryDrop {
    fn drop(&mut self) {
        self.0.block();
    }
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

/// A child's exit publishes at dispatch, ahead of its retained factory's
/// destruction (SPEC §9). Driver death while that destruction is still
/// blocked therefore finds the verdict already published: teardown keeps it
/// and only stops waiting for the release edge (SPEC §11).
#[crate::runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_teardown_keeps_the_exit_published_ahead_of_factory_disposal() {
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
    // The factory's destructor is now blocked on the pool, and the driver
    // submitted it only after publishing the exit.
    let published = task.cell.record();
    let before_teardown = crate::runtime::timeout(DRIVER_PROGRESS_WAIT, task.wait()).await;

    // Dropping this dedicated runtime destroys the scope-driver future while
    // the blocking pool remains held in the retained factory's destructor.
    // Join the cancelled driver before releasing the gate below, so the
    // driver future is gone and the release edge can no longer be crossed.
    let teardown = crate::runtime::spawn(hosted.shutdown());
    let driver = crate::runtime::timeout(DRIVER_PROGRESS_WAIT, crate::runtime::join(driver)).await;
    let lifecycle = crate::runtime::timeout(DRIVER_PROGRESS_WAIT, async {
        loop {
            let Some(item) = events.recv().await else {
                panic!("lifecycle closed without the worker's Exited event")
            };
            if let LifecycleItem::Event(event) = item
                && let LifecycleEventKind::Exited { id, exit, .. } = event.kind
                && id.as_str() == "worker"
            {
                break exit;
            }
        }
    })
    .await;
    let after_teardown = task.cell.record();

    // Always unblock the runtime thread before asserting, so a regression
    // fails promptly rather than waiting for the gate backstop.
    gate.release();
    let teardown =
        crate::runtime::timeout(DRIVER_PROGRESS_WAIT, crate::runtime::join(teardown)).await;

    assert!(matches!(
        teardown,
        crate::runtime::Timeout::Completed(crate::runtime::JoinOutcome::Ok { value: () })
    ));
    assert!(matches!(
        driver,
        crate::runtime::Timeout::Completed(crate::runtime::JoinOutcome::Cancelled)
    ));
    let MemberStage::Terminal(recorded) = published.stage else {
        panic!("the exit must publish before the factory's destruction completes")
    };
    let crate::runtime::Timeout::Completed(terminal) = before_teardown else {
        panic!("wait() must resolve before the factory's destruction completes")
    };
    let crate::runtime::Timeout::Completed(lifecycle) = lifecycle else {
        panic!("the published exit's lifecycle event was not observed")
    };
    let MemberStage::Terminal(kept) = after_teardown.stage else {
        panic!("driver teardown must keep the membership terminal")
    };
    for exit in [&terminal, &lifecycle, &recorded, &kept] {
        assert!(matches!(
            exit.kind(),
            ExitKind::Failed(error) if error.to_string() == FAILURE
        ));
        assert_eq!(exit.cancellation(), Cancellation::NotObserved);
    }
    assert_eq!(lifecycle, terminal);
    assert_eq!(recorded, terminal);
    assert_eq!(kept, terminal, "teardown keeps the published verdict");
    assert_eq!(published.last_exit, Some(terminal));
}

#[crate::runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_terminal_disposal_retires_its_exit_off_the_driver() {
    let (mut scope, _events) = OrderedScopeFixture::new(Tree::new()).build();
    let (error, disposed) = thread_reporting_error();
    let driver_thread = std::thread::current().id();

    // The carrier makes retention structural; the destructor-thread probe
    // still pins the refusal's disposal artifact to the blocking pool.
    scope.begin_terminal_disposal(
        ChildKey::fixture(999),
        RetainedExit::new(Exit::failed(error, Cancellation::NotObserved)),
        None,
        StartupDisposition::NotAborted,
    );

    assert_ne!(
        disposed
            .recv_timeout(DRIVER_PROGRESS_WAIT)
            .expect("the refused terminal exit is disposed"),
        driver_thread,
        "a refusal cannot destroy the user error on the driver"
    );
}

#[crate::runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_terminal_resource_retires_its_exit_before_panicking() {
    let (mut scope, _events) = OrderedScopeFixture::new(Tree::new()).build();
    let (error, disposed) = thread_reporting_error();
    let driver_thread = std::thread::current().id();

    let panic = catch_unwind(AssertUnwindSafe(|| {
        scope.terminalize_child(
            ChildKey::fixture(999),
            Exit::failed(error, Cancellation::NotObserved),
            None,
            StartupDisposition::NotAborted,
        );
    }))
    .expect_err("a missing terminal resource remains a framework invariant");

    assert_eq!(
        panic.downcast_ref::<String>().map(String::as_str),
        Some("terminalized child remains registered"),
    );
    assert_ne!(
        disposed
            .recv_timeout(DRIVER_PROGRESS_WAIT)
            .expect("the rejected terminal exit is disposed"),
        driver_thread,
        "the framework panic cannot unwind the user error on the driver"
    );
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
    let fixture = OrderedScopeFixture::new(tree);
    let root = Arc::clone(&fixture.root);
    root.set_state(ScopeState::Running);
    root.set_startup(Ok(()));
    let key = fixture.children.keys().next().expect("one child plan");
    let (mut scope, _event_receiver) = fixture.with_lifecycle(ScopeLifecycle::running()).build();

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
        Some(RetainedRecordedOutcome::new(RecordedOutcome::returned(
            Err(ExitError::message("trip intensity")),
        ))),
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
    let fixture = OrderedScopeFixture::new(tree);
    let root = Arc::clone(&fixture.root);
    root.set_state(ScopeState::Running);
    root.set_startup(Ok(()));
    let key = fixture.children.keys().next().expect("one child plan");
    let (mut scope, _event_receiver) = fixture.with_lifecycle(ScopeLifecycle::running()).build();

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
        Some(RetainedRecordedOutcome::new(RecordedOutcome::returned(
            Err(ExitError::message("trip intensity")),
        ))),
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
    let fixture = OrderedScopeFixture::new(tree);
    let keys = fixture.children.keys().collect::<Vec<_>>();
    let (mut scope, _event_receiver) = fixture.with_lifecycle(ScopeLifecycle::running()).build();

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
    let mut fixture = OrderedScopeFixture::new(tree);
    // Model restart-window children: no incarnation and no retained
    // construction remains, so forced terminalization completes inline.
    // This used to re-enter `stop_next_ordered` once per child.
    for child in fixture.children.values_mut() {
        drop(child.construction.take());
    }
    let (mut scope, _event_receiver) = fixture
        .with_lifecycle(ScopeLifecycle::running())
        .with_hard_forced(true)
        .build();

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

#[test]
fn epoch_guard_unwind_retires_the_epoch_despite_poisoned_control() {
    let root = Arc::clone(&Tree::new().lower_for_test().root);
    let epoch = ScopeEpochGuard::begin(&root).expect("test scope epoch is available");
    root.poison_control();

    // A strict control lock in the guard's drop would panic inside this
    // unwind and abort the process instead of returning `Err`.
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _epoch = epoch;
        panic!("unwind through the scope epoch guard");
    }))
    .expect_err("the fixture unwinds through the guard");

    assert!(matches!(
        root.snapshot().state,
        ScopeState::Stopped {
            reason: StopReason::ShutdownRequested
        }
    ));
}
