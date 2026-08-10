use super::support::*;

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
    plan.armed = false;
    drop(plan);

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

    assert!(
        scope.children[key].active.is_none(),
        "the consumed request must not bypass the following incarnation's backoff"
    );
    assert!(scope.children[key].restart_deadline.is_some());
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
    plan.armed = false;
    drop(plan);

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

    let restarted = scope.children[key]
        .active
        .as_ref()
        .expect("the retained event starts the next incarnation immediately");
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
    plan.armed = false;
    drop(plan);

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
        Pending::Driver(exit).classified(),
    ];
    arbitrate(&mut pending);
    for (_, event) in pending {
        match event {
            Pending::RestartShutdown { child, target } => {
                scope.expedite_restart_shutdown(child, target);
            }
            Pending::Driver(DriverEvent::Child(ChildEvent::Exited {
                child,
                incarnation,
                recorded,
                join,
                cancellation,
                readiness_signal_seen,
            })) => scope.handle_exit(
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
    plan.armed = false;
    drop(plan);

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
        Pending::Driver(DriverEvent::Child(ChildEvent::Exited {
            child: key,
            incarnation,
            recorded: Some(RecordedOutcome::returned(Ok(()))),
            join: crate::runtime::JoinOutcome::Ok { value: () },
            cancellation: Cancellation::NotObserved,
            readiness_signal_seen: true,
        }))
        .classified(),
        Pending::Driver(DriverEvent::Child(ChildEvent::SelfStop {
            child: key,
            incarnation,
        }))
        .classified(),
    ];
    arbitrate(&mut pending);
    assert!(
        matches!(
            pending[0].1,
            Pending::Driver(DriverEvent::Child(ChildEvent::SelfStop { .. }))
        ),
        "the regression premise: arbitration orders the stop ahead of the exit"
    );
    for (_, event) in pending {
        match event {
            Pending::Driver(DriverEvent::Child(ChildEvent::SelfStop { child, incarnation })) => {
                scope.handle_self_stop(child, incarnation)
            }
            Pending::Driver(DriverEvent::Child(ChildEvent::Exited {
                child,
                incarnation,
                recorded,
                join,
                cancellation,
                readiness_signal_seen,
            })) => scope.handle_exit(
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
