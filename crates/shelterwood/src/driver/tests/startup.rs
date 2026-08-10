use super::support::*;

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
    let _ = scope.children[nested]
        .slot
        .scope
        .as_ref()
        .expect("nested scope cell")
        .request_shutdown();
    assert_eq!(scope.pending_restart_shutdowns(), vec![nested]);

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
        restart_shutdown_work(nested),
        Pending::Driver(exit).classified(),
    ];
    arbitrate(&mut pending);
    for (_, event) in pending {
        match event {
            Pending::RestartShutdown(child) => scope.expedite_restart_shutdown(child),
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
