use super::support::*;

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

    let target = nested
        .request_shutdown()
        .expect("the pre-admission shutdown targets the first epoch");
    assert!(root.take_control_events().is_empty());
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );

    assert_eq!(
        root.take_control_events(),
        vec![ScopeControlEvent::RestartShutdown {
            membership: nested.member.membership(),
            target,
        }]
    );
}

#[crate::runtime::test]
async fn pre_admission_restart_shutdown_does_not_expedite_the_following_incarnation() {
    let mut tree = Tree::new();
    tree.add_subtree(
        "nested",
        SubtreeDef::factory(pending_tree).restart(RestartPolicy::new(
            RestartCondition::Always,
            Backoff::fixed(Duration::from_secs(60), crate::Jitter::None)
                .expect("non-zero restart backoff"),
        )),
    )
    .expect("valid subtree");
    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let nested = Arc::clone(
        plan.children[0]
            .slot
            .scope
            .as_ref()
            .expect("nested scope cell"),
    );
    let target = nested
        .request_shutdown()
        .expect("the pre-admission shutdown targets the first epoch");
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
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_lifecycle(ScopeLifecycle::running())
        .with_children(children)
        .build();
    plan.finish_transfer();

    scope.spawn_child(key);
    let first = scope.children[key]
        .active
        .as_ref()
        .expect("the first incarnation is active")
        .incarnation;
    assert_eq!(
        nested.begin_incarnation(ScopeState::Starting),
        Some(target),
        "the first nested incarnation claims the pre-admission target"
    );
    assert!(nested.take_shutdown_request(target));
    let event = root
        .take_control_events()
        .pop()
        .expect("parent adoption publishes the pre-admission request");
    let Some((_, Pending::RestartShutdown { child, target })) = scope.control_event_work(event)
    else {
        panic!("the control event resolves to restart-shutdown work");
    };
    assert_eq!(child, key);
    scope.expedite_restart_shutdown(child, target);
    nested.finish_incarnation(target, StopReason::ShutdownRequested);
    scope.children[key]
        .active
        .as_ref()
        .expect("the first incarnation is active")
        .abort_handle
        .abort();

    scope.handle_exit(
        key,
        first,
        Some(RecordedOutcome::returned(Err(ExitError::message(
            "restart the nested scope",
        )))),
        crate::runtime::JoinOutcome::Ok { value: () },
        Cancellation::NotObserved,
        false,
    );

    // Drive the deferred retry the exit queued: the consumed target must not
    // expedite the following incarnation.
    for (child, target) in std::mem::take(&mut scope.restart_shutdown_retries) {
        scope.expedite_restart_shutdown(child, target);
    }

    assert!(
        scope.children[key].active.is_none(),
        "the consumed request must not bypass the following incarnation's backoff"
    );
    assert!(scope.children[key].restart_deadline.is_some());
    assert!(scope.children[key].restart_shutdown_pending.is_none());
}

#[crate::runtime::test]
async fn expedited_restart_progresses_synchronous_readiness() {
    let mut tree = Tree::new();
    tree.add_subtree("nested", SubtreeDef::factory(pending_tree))
        .expect("valid subtree");
    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let nested_cell = Arc::clone(
        plan.children[0]
            .slot
            .scope
            .as_ref()
            .expect("nested scope cell"),
    );
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
    let mut child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    // Scope definitions currently resolve to Manual and expose no public
    // readiness setter. Model that API invariant changing: the expedited
    // restart must still release ordered startup when configuration produces
    // an immediate readiness effect.
    child.options.readiness = Readiness::Immediate;
    child.spawned_once = true;
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_children(children)
        .with_next_ordered_start(Some(key))
        .build();
    plan.finish_transfer();
    let target = nested_cell
        .request_shutdown()
        .expect("the shutdown targets the pending nested incarnation");

    scope.expedite_restart_shutdown(key, target);

    assert!(scope.children[key].initial_ready);
    assert!(
        scope.lifecycle.startup_complete(),
        "synchronous readiness from an expedited spawn advances aggregate startup"
    );
    assert_eq!(root.record().startup, Some(Ok(())));
}

/// A shutdown request against a nested scope's *first* (still pending)
/// incarnation, published before the ordered start reaches that child, must
/// not expedite-spawn it: only a member in the restart gap may bypass
/// `progress_startup`'s in-order gating. The request stays latched on the
/// nested cell and is claimed when the child starts at its ordered turn.
#[crate::runtime::test]
async fn early_restart_shutdown_does_not_expedite_a_never_started_ordered_child() {
    let factories = Arc::new(std::sync::atomic::AtomicUsize::new(0));
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
                factories.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Tree::new()
            }
        }),
    )
    .expect("valid subtree");

    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let nested_cell = Arc::clone(
        plan.children[1]
            .slot
            .scope
            .as_ref()
            .expect("nested scope cell"),
    );
    let target = nested_cell
        .request_shutdown()
        .expect("the pre-admission shutdown targets the first epoch");
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
            .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    }
    let first = children.keys().next().expect("first ordered child");
    let nested = children
        .keys()
        .find(|key| children[*key].slot.member.id().as_str() == "nested")
        .expect("nested child key");
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_children(children)
        .with_next_ordered_start(Some(first))
        .build();
    plan.finish_transfer();

    // Ordered startup spawns "a" and parks on its (never-fired) readiness.
    scope.progress_startup();
    assert!(scope.children[first].active.is_some());
    assert!(!scope.children[first].initial_ready);

    let event = root
        .take_control_events()
        .pop()
        .expect("parent adoption publishes the pre-admission request");
    let Some((
        _,
        Pending::RestartShutdown {
            child,
            target: event_target,
        },
    )) = scope.control_event_work(event)
    else {
        panic!("the control event resolves to restart-shutdown work");
    };
    assert_eq!(child, nested);
    assert_eq!(event_target, target);
    scope.expedite_restart_shutdown(child, event_target);

    crate::runtime::yield_now().await;
    crate::runtime::yield_now().await;

    assert_eq!(
        factories.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a never-started ordered child must not be expedite-spawned before its turn"
    );
    assert!(scope.children[nested].active.is_none());
    assert!(!scope.children[nested].spawned_once);
    assert!(scope.children[nested].restart_shutdown_pending.is_none());
    assert_eq!(
        nested_cell.begin_incarnation(ScopeState::Starting),
        Some(target),
        "the request stays latched for the first incarnation's ordered turn"
    );
}

#[crate::runtime::test]
async fn restart_shutdown_arriving_before_exit_is_retried_after_the_child_becomes_inactive() {
    let mut tree = Tree::new();
    tree.add_subtree(
        "nested",
        SubtreeDef::factory(pending_tree).restart(RestartPolicy::new(
            RestartCondition::Always,
            Backoff::fixed(Duration::from_secs(60), crate::Jitter::None)
                .expect("non-zero restart backoff"),
        )),
    )
    .expect("valid subtree");
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
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_lifecycle(ScopeLifecycle::running())
        .with_children(children)
        .build();
    plan.finish_transfer();

    let target = scope.children[key]
        .slot
        .scope
        .as_ref()
        .expect("nested scope cell")
        .request_shutdown()
        .expect("the shutdown targets the pending nested incarnation");
    scope.spawn_child(key);
    let first = scope.children[key]
        .active
        .as_ref()
        .expect("the first incarnation is active")
        .incarnation;
    scope.expedite_restart_shutdown(key, target);
    assert_eq!(
        scope.children[key].restart_shutdown_pending,
        Some(target),
        "the early event remains owned until the active incarnation exits"
    );
    scope.children[key]
        .active
        .as_ref()
        .expect("the first incarnation is active")
        .abort_handle
        .abort();

    scope.handle_exit(
        key,
        first,
        Some(RecordedOutcome::returned(Err(ExitError::message(
            "restart the nested scope",
        )))),
        crate::runtime::JoinOutcome::Ok { value: () },
        Cancellation::NotObserved,
        false,
    );

    // Exit handling queues the retry rather than expediting mid-batch; the
    // driver loop drains it into the next wake's arbitration.
    let retries = std::mem::take(&mut scope.restart_shutdown_retries);
    assert_eq!(retries, vec![(key, target)]);
    for (child, target) in retries {
        scope.expedite_restart_shutdown(child, target);
    }

    let restarted = scope.children[key]
        .active
        .as_ref()
        .expect("the retained event starts the next incarnation on the following wake");
    assert_ne!(restarted.incarnation, first);
    assert!(scope.children[key].restart_deadline.is_none());
    assert!(scope.children[key].restart_shutdown_pending.is_none());
}

#[crate::runtime::test]
async fn same_batch_intensity_exit_suppresses_real_expedited_factory() {
    let factories = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tree = Tree::new();
    tree.intensity(Intensity::new(0, Duration::from_secs(10)).expect("valid intensity"));
    tree.add_subtree(
        "nested",
        SubtreeDef::factory({
            let factories = Arc::clone(&factories);
            move || {
                factories.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Tree::new()
            }
        }),
    )
    .expect("valid subtree");
    tree.add_task("trip", TaskDef::new(|_| future::pending()))
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
    let (events, _event_receiver) = crate::runtime::unbounded_mpsc();
    let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let mut children = ChildArena::default();
    plan.children.reverse();
    while let Some(child) = plan.children.pop() {
        children
            .insert(ChildRuntime::from_plan(child, &root))
            .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    }
    let nested = children
        .keys()
        .find(|key| children[*key].slot.member.id().as_str() == "nested")
        .expect("nested child key");
    let trip = children
        .keys()
        .find(|key| children[*key].slot.member.id().as_str() == "trip")
        .expect("tripping child key");
    let next_ordered_start = children.keys().next();
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_intensity_policy(plan.config.intensity)
        .with_children(children)
        .with_lifecycle(ScopeLifecycle::running())
        .with_next_ordered_start(next_ordered_start)
        .build();
    plan.finish_transfer();

    root.transition_child(
        &scope.children[nested].slot.member,
        |record| {
            record.incarnation = None;
            record.stage = MemberStage::Restarting;
        },
        None,
    );
    let target = scope.children[nested]
        .slot
        .scope
        .as_ref()
        .expect("nested scope cell")
        .request_shutdown()
        .expect("the shutdown targets the pending nested incarnation");
    let event = root
        .take_control_events()
        .pop()
        .expect("the request publishes one subject-carrying control event");
    let Some((
        _,
        Pending::RestartShutdown {
            child: subject,
            target: event_target,
        },
    )) = scope.control_event_work(event)
    else {
        panic!("the control event resolves to restart-shutdown work");
    };
    assert_eq!(subject, nested);
    assert_eq!(event_target, target);

    scope.spawn_child(trip);
    let incarnation = scope.children[trip]
        .active
        .as_ref()
        .expect("tripping child is active")
        .incarnation;
    scope.children[trip]
        .active
        .as_ref()
        .expect("tripping child is active")
        .abort_handle
        .abort();
    let exit = DriverEvent::Child(ChildEvent::Exited {
        child: trip,
        incarnation,
        recorded: Some(RecordedOutcome::returned(Err(ExitError::message(
            "trip intensity",
        )))),
        join: crate::runtime::JoinOutcome::Ok { value: () },
        cancellation: Cancellation::NotObserved,
        readiness_signal_seen: false,
    });
    let mut pending = [
        restart_shutdown_work(nested, target),
        Pending::from(exit).classified(),
    ];
    arbitrate(&mut pending);
    for (_, event) in pending {
        match event {
            Pending::RestartShutdown { child, target } => {
                scope.expedite_restart_shutdown(child, target);
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
            _ => unreachable!("the fixture queues only exit and restart work"),
        }
    }

    crate::runtime::yield_now().await;
    crate::runtime::yield_now().await;

    assert!(scope.lifecycle.is_draining());
    assert_eq!(
        factories.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the production guard must suppress the expedited factory after intensity drain"
    );
}

/// The retained-fact twin of the test above: a restart-shutdown fact retained
/// while its subject was active is retried when the subject's exit is handled
/// — but the retry must re-enter arbitration rather than expedite mid-batch,
/// so an intensity-tripping exit collected in the same wake drains the scope
/// before the retry can start a doomed incarnation.
#[crate::runtime::test]
async fn same_batch_intensity_exit_suppresses_retained_expedite_retry() {
    let factories = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tree = Tree::new();
    tree.intensity(Intensity::new(1, Duration::from_secs(10)).expect("valid intensity"));
    tree.add_subtree(
        "nested",
        SubtreeDef::factory({
            let factories = Arc::clone(&factories);
            move || {
                factories.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Tree::new()
            }
        })
        .restart(RestartPolicy::new(
            RestartCondition::Always,
            Backoff::fixed(Duration::from_secs(60), crate::Jitter::None)
                .expect("non-zero restart backoff"),
        )),
    )
    .expect("valid subtree");
    tree.add_task("trip", TaskDef::new(|_| future::pending()))
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
    let (events, _event_receiver) = crate::runtime::unbounded_mpsc();
    let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let mut children = ChildArena::default();
    plan.children.reverse();
    while let Some(child) = plan.children.pop() {
        children
            .insert(ChildRuntime::from_plan(child, &root))
            .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    }
    let nested = children
        .keys()
        .find(|key| children[*key].slot.member.id().as_str() == "nested")
        .expect("nested child key");
    let trip = children
        .keys()
        .find(|key| children[*key].slot.member.id().as_str() == "trip")
        .expect("tripping child key");
    let next_ordered_start = children.keys().next();
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_intensity_policy(plan.config.intensity)
        .with_children(children)
        .with_lifecycle(ScopeLifecycle::running())
        .with_next_ordered_start(next_ordered_start)
        .build();
    plan.finish_transfer();

    let target = scope.children[nested]
        .slot
        .scope
        .as_ref()
        .expect("nested scope cell")
        .request_shutdown()
        .expect("the shutdown targets the pending nested incarnation");
    scope.spawn_child(nested);
    let nested_first = scope.children[nested]
        .active
        .as_ref()
        .expect("the first nested incarnation is active")
        .incarnation;
    // The subject-carrying event runs while the child is still active, so the
    // fact is retained for the exit-time retry.
    scope.expedite_restart_shutdown(nested, target);
    assert_eq!(
        scope.children[nested].restart_shutdown_pending,
        Some(target)
    );
    scope.children[nested]
        .active
        .as_ref()
        .expect("the first nested incarnation is active")
        .abort_handle
        .abort();

    scope.spawn_child(trip);
    let trip_incarnation = scope.children[trip]
        .active
        .as_ref()
        .expect("tripping child is active")
        .incarnation;
    scope.children[trip]
        .active
        .as_ref()
        .expect("tripping child is active")
        .abort_handle
        .abort();

    let nested_exit = DriverEvent::Child(ChildEvent::Exited {
        child: nested,
        incarnation: nested_first,
        recorded: Some(RecordedOutcome::returned(Err(ExitError::message(
            "restart the nested scope",
        )))),
        join: crate::runtime::JoinOutcome::Ok { value: () },
        cancellation: Cancellation::NotObserved,
        readiness_signal_seen: false,
    });
    let trip_exit = DriverEvent::Child(ChildEvent::Exited {
        child: trip,
        incarnation: trip_incarnation,
        recorded: Some(RecordedOutcome::returned(Err(ExitError::message(
            "trip intensity",
        )))),
        join: crate::runtime::JoinOutcome::Ok { value: () },
        cancellation: Cancellation::NotObserved,
        readiness_signal_seen: false,
    });
    let mut pending = [
        Pending::from(nested_exit).classified(),
        Pending::from(trip_exit).classified(),
    ];
    arbitrate(&mut pending);
    for (_, event) in pending {
        match event {
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
            _ => unreachable!("the fixture queues only the two exits"),
        }
    }

    // The nested exit deferred its retry through arbitration, so the trip
    // exit from the same batch drained the scope first.
    assert!(scope.lifecycle.is_draining());
    let retries = std::mem::take(&mut scope.restart_shutdown_retries);
    assert_eq!(retries, vec![(nested, target)]);
    for (child, target) in retries {
        scope.expedite_restart_shutdown(child, target);
    }

    crate::runtime::yield_now().await;
    crate::runtime::yield_now().await;

    assert_eq!(
        factories.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the deferred retry must not start a doomed incarnation after intensity drain"
    );
    assert!(scope.children[nested].active.is_none());
    assert!(scope.children[nested].restart_shutdown_pending.is_none());
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
    let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_intensity_policy(plan.config.intensity)
        .with_children(children)
        .with_next_ordered_start(Some(key))
        .build();
    plan.finish_transfer();

    scope.spawn_child(key);
    let active = scope.children[key]
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
            recorded: Some(RecordedOutcome::returned(Ok(()))),
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

    let DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic }) =
        disposal_event_receiver
            .recv()
            .await
            .expect("disposal reports completion")
    else {
        panic!("only construction disposal was armed")
    };
    scope.handle_construction_disposed(child, panic);

    assert!(
        scope.lifecycle.startup_complete(),
        "the ready-before-stop child completes startup"
    );
    assert!(
        matches!(root.record().startup, Some(Ok(()))),
        "a fired readiness latch must survive a same-batch local stop: {:?}",
        root.record().startup
    );
    assert_eq!(root.record().state, ScopeState::Running);
    assert!(matches!(
        scope.children[key].slot.member.record().stage,
        MemberStage::Terminal(ref exit) if matches!(exit.kind(), ExitKind::Completed)
    ));
    assert!(
        !scope.children[key].slot.member.record().startup_aborted,
        "a post-ready clean self-stop is not a startup abort"
    );
}

/// `next_ordered_start` is held across `spawn_child` and is never cleared by
/// `reclaim_child`, so `progress_startup` must treat a vacated slot the way
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
            .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    }
    let gone = children.keys().next().expect("first ordered child");
    let next = children.keys().nth(1).expect("second ordered child");
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_children(children)
        .with_next_ordered_start(Some(gone))
        .build();
    plan.finish_transfer();

    // Vacate the slot the cursor points at, exactly as a reclaim would leave
    // it, without disturbing the child that follows.
    scope
        .children
        .remove(gone)
        .expect("the cursor's child is live before the reclaim");

    scope.progress_startup();

    assert_eq!(
        scope.next_ordered_start,
        Some(next),
        "a vacated cursor advances to the next live child instead of panicking"
    );
    assert!(
        scope.children[next].active.is_some(),
        "the following child starts at its ordered turn"
    );
    assert!(
        !scope.lifecycle.startup_complete(),
        "the live successor still gates the aggregate"
    );
}

/// A removal response is an observation boundary, not merely a wake. Keep it
/// pending after membership commit until the batch epilogue has recomputed
/// startup over the shrunken declared set.
#[crate::runtime::test]
async fn startup_removal_response_follows_aggregate_recomputation() {
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
    let epoch = root
        .begin_incarnation(ScopeState::Starting)
        .expect("test scope epoch is available");
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    let (events, _event_receiver) = crate::runtime::unbounded_mpsc();
    let (control_events, _control_event_receiver) = crate::runtime::unbounded_mpsc();
    let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(control_events);
    let mut children = ChildArena::default();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the fixture fits in the child-key domain"));
    root.with_observation_gate(|txn| {
        control.register_initial(children.iter().map(|(key, child)| (&child.slot, key)), txn);
    });
    root.set_dynamic_route(Some(control.clone()));
    let member = Arc::clone(&children[key].slot.member);
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_children(children)
        .with_dynamic(Some(control))
        .build();
    plan.finish_transfer();

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
    assert!(!scope.lifecycle.startup_complete());

    scope.progress_startup();
    assert!(scope.lifecycle.startup_complete());
    assert_eq!(root.record().startup, Some(Ok(())));
    scope.publish_startup_removals();
    assert_eq!(removal.try_receive(), Some(RemoveOutcome::Removed));
}
