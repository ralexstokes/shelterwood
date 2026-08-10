use super::support::*;

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
    let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_intensity_policy(plan.config.intensity)
        .with_children(children)
        .with_lifecycle(ScopeLifecycle::running())
        .build();
    plan.armed = false;
    drop(plan);

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
        scope.lifecycle.draining_reason(),
        Some(StopReason::IntensityTripped(_))
    ));

    // The request's next-pass follow-up owns the stronger verdict even
    // though teardown was already started by the intensity trip.
    assert!(root.take_shutdown_request(scope.epoch));
    scope.begin_drain(StopReason::ShutdownRequested);
    assert_eq!(
        scope.lifecycle.draining_reason(),
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
    let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_intensity_policy(plan.config.intensity)
        .with_children(children)
        .with_lifecycle(ScopeLifecycle::running())
        .build();
    plan.armed = false;
    drop(plan);

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
        scope.lifecycle.draining_reason(),
        Some(StopReason::IntensityTripped(_))
    ));

    // Model the child driver collecting only the ancestor abort latch: force
    // runs on a scope already draining for the trip, without a processed
    // shutdown request having upgraded the reason first.
    scope.force_all();
    assert_eq!(
        scope.lifecycle.draining_reason(),
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
    let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let mut children = ChildArena::default();
    plan.children.reverse();
    while let Some(child) = plan.children.pop() {
        children
            .insert(ChildRuntime::from_plan(child, &root))
            .unwrap_or_else(|_| panic!("the fixture fits in the child-key domain"));
    }
    let keys = children.keys().collect::<Vec<_>>();
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_intensity_policy(plan.config.intensity)
        .with_children(children)
        .with_lifecycle(ScopeLifecycle::running())
        .build();
    plan.armed = false;
    drop(plan);

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
    let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
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
    let mut scope = ScopeRuntimeBuilder::new(root, epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_intensity_policy(plan.config.intensity)
        .with_children(children)
        .with_lifecycle(ScopeLifecycle::running())
        .with_hard_forced(true)
        .build();
    plan.armed = false;
    drop(plan);

    scope.begin_drain(StopReason::ShutdownRequested);

    assert!(scope.children.values().all(ChildRuntime::is_terminal));
    assert!(!scope.ordered_stop_progressing);
    assert_eq!(scope.ordered_stop_waiting, None);
    assert_eq!(
        scope.ordered_stop_inspections, CHILDREN,
        "the reverse cursor inspects each ordered child exactly once"
    );
    assert_eq!(
        scope.incomplete_children, 0,
        "terminal completion decrements the incremental count exactly once"
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
