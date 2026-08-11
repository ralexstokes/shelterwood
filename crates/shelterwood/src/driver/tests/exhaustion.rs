use super::support::*;

#[test]
fn member_transitions_own_their_complete_record_projection() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let membership = identity.mint_membership(&id).expect("membership available");
    let member = MemberCell::new(id, membership);
    let mut incarnations = member.take_incarnation_counter();
    let incarnation = incarnations.mint().expect("incarnation available");

    member.transition(MemberTransition::Admitted);
    assert_eq!(member.record().stage, MemberStage::Admitted);

    member.transition(MemberTransition::Starting { incarnation });
    let record = member.record();
    assert_eq!(record.stage, MemberStage::Starting);
    assert_eq!(record.incarnation, Some(incarnation));
    assert_eq!(record.last_incarnation, Some(incarnation));
    assert_eq!(record.restart_at, None);

    member.transition(MemberTransition::Running);
    assert_eq!(member.record().stage, MemberStage::Running);
    member.transition(MemberTransition::Stopping);
    assert_eq!(member.record().stage, MemberStage::Stopping);

    let exit = Exit::new(ExitKind::Completed, Cancellation::NotObserved);
    let restart_count = RestartCount::ZERO.bump();
    let restart_at = crate::runtime::now();
    member.transition(MemberTransition::RestartScheduled {
        exit: exit.clone(),
        restart_count,
        restart_at: Some(restart_at),
    });
    let record = member.record();
    assert_eq!(record.stage, MemberStage::Restarting);
    assert_eq!(record.incarnation, None);
    assert_eq!(record.last_incarnation, Some(incarnation));
    assert_eq!(record.last_exit, Some(exit));
    assert_eq!(record.restart_count, restart_count);
    assert_eq!(record.restart_at, Some(restart_at));

    let second = incarnations.mint().expect("restart incarnation available");
    member.transition(MemberTransition::Starting {
        incarnation: second,
    });
    let record = member.record();
    assert_eq!(record.stage, MemberStage::Starting);
    assert_eq!(record.incarnation, Some(second));
    assert_eq!(record.last_incarnation, Some(second));
    assert_eq!(record.restart_at, None);
    assert_eq!(
        record.last_exit,
        Some(Exit::new(ExitKind::Completed, Cancellation::NotObserved))
    );
    assert_eq!(record.restart_count, restart_count);
}

#[test]
fn incarnation_mint_exhaustion_has_no_terminal_side_effects() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let membership = identity.mint_membership(&id).expect("membership available");
    let member = MemberCell::new(id, membership);
    let previous = Exit::new(
        ExitKind::Failed(ExitError::message("last completed incarnation")),
        Cancellation::NotObserved,
    );
    // Walk the record along the production path so the transition-source
    // assertions in `apply_transition` cover this setup too.
    let mut setup_incarnations = member.take_incarnation_counter();
    let spent = setup_incarnations
        .mint()
        .expect("setup incarnation available");
    member.transition(MemberTransition::Admitted);
    member.transition(MemberTransition::Starting { incarnation: spent });
    member.transition(MemberTransition::RestartScheduled {
        exit: previous.clone(),
        restart_count: RestartCount::ZERO,
        restart_at: None,
    });
    let mut counter = IncarnationCounter::near_exhaustion(member.membership());

    assert!(counter.mint().is_some());
    assert!(counter.mint().is_none());
    assert!(matches!(member.record().stage, MemberStage::Restarting));
    assert_eq!(member.record().last_exit, Some(previous));
    assert!(counter.mint().is_none());
}

#[crate::runtime::test]
async fn incarnation_exhaustion_uses_post_disposal_retention_routing() {
    let mut tree = Tree::new();
    tree.add_task(
        "worker",
        TaskDef::new(|_| future::pending::<crate::ExitResult>()).retention(Retention::Remove),
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
    let (events, mut event_receiver) = crate::runtime::unbounded_mpsc();
    let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_intensity_policy(plan.intensity_policy())
        .with_children(children)
        .with_lifecycle(ScopeLifecycle::running())
        .with_next_ordered_start(Some(key))
        .build();
    plan.finish_transfer();

    scope.children[key].incarnations =
        IncarnationCounter::near_exhaustion(scope.children[key].slot.member.membership());
    let first = scope.children[key]
        .incarnations
        .mint()
        .expect("the last usable incarnation mints");
    let previous = Exit::new(
        ExitKind::Failed(ExitError::message("last completed incarnation")),
        Cancellation::NotObserved,
    );
    root.transition_child(
        &scope.children[key].slot.member,
        |record| {
            record.incarnation = None;
            record.last_incarnation = Some(first);
            record.last_exit = Some(previous.clone());
            record.stage = MemberStage::Restarting;
        },
        None,
    );

    assert!(
        crate::runtime::unbounded_mpsc_send(
            &scope.events,
            DriverEvent::Child(ChildEvent::Ready {
                child: key,
                incarnation: first,
            }),
        )
        .is_ok(),
        "the driver lane remains open"
    );

    scope.spawn_child(key);
    assert!(scope.children[key].is_disposing());
    assert!(matches!(
        scope.children[key].slot.member.record().stage,
        MemberStage::Restarting
    ));
    assert_eq!(root.snapshot().children.len(), 1);

    let DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic }) =
        disposal_event_receiver
            .recv()
            .await
            .expect("disposal reports completion")
    else {
        panic!("only construction disposal was armed")
    };
    assert!(matches!(
        event_receiver.try_recv(),
        Ok(DriverEvent::Child(ChildEvent::Ready { .. }))
    ));
    scope.handle_construction_disposed(child, panic);

    assert!(!scope.children[key].is_disposing());
    assert!(matches!(
        scope.children[key].slot.member.record().stage,
        MemberStage::Terminal(ref exit) if exit == &previous
    ));
    assert!(
        root.snapshot().children.is_empty(),
        "retention pruning follows joined disposal"
    );
}

/// Exhaustion terminalizes a membership that never spawned. B.6 makes
/// that the plain `Stopped { NeverStarted }` verdict; §6's
/// `StartupAborted` stays reserved for a membership that ran and failed
/// before its initial readiness edge. The pre-readiness position still
/// has to route the scope's startup failure.
#[crate::runtime::test]
async fn first_spawn_exhaustion_stops_without_reporting_a_startup_abort() {
    let mut tree = Tree::new();
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
    let (events, _event_receiver) = crate::runtime::unbounded_mpsc();
    let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_intensity_policy(plan.intensity_policy())
        .with_children(children)
        .with_next_ordered_start(Some(key))
        .build();
    plan.finish_transfer();

    // Burn the counter's last usable generation without touching the
    // member record: the child is still an unspawned initial member, so
    // its very first `spawn_child` exhausts before any incarnation runs.
    scope.children[key].incarnations =
        IncarnationCounter::near_exhaustion(scope.children[key].slot.member.membership());
    assert!(scope.children[key].incarnations.mint().is_some());
    assert!(scope.supervisor.is_initial(key));
    assert!(!scope.supervisor.initial_ready(key));
    assert_eq!(
        scope.children[key].slot.member.record().last_incarnation,
        None
    );

    scope.spawn_child(key);
    assert!(scope.children[key].is_disposing());

    let DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic }) =
        disposal_event_receiver
            .recv()
            .await
            .expect("disposal reports completion")
    else {
        panic!("only construction disposal was armed")
    };
    scope.handle_construction_disposed(child, panic);

    assert!(matches!(
        scope.children[key].slot.member.record().stage,
        MemberStage::Terminal(ref exit) if matches!(exit.kind(), ExitKind::NeverStarted)
    ));
    let snapshot = root.snapshot();
    let published = snapshot
        .child("worker")
        .expect("a retained exhausted child stays resident");
    assert!(
        matches!(
            published.state,
            ChildState::Stopped { ref exit } if matches!(exit.kind(), ExitKind::NeverStarted)
        ),
        "an unspawned exhausted membership is Stopped, not StartupAborted: {:?}",
        published.state
    );
    assert!(
        !scope.children[key].slot.member.record().startup_aborted,
        "§6's startup-abort flag belongs to a membership that ran"
    );
    assert!(
        matches!(
            root.record().startup,
            Some(Err(StartupError::StartupFailed(_)))
        ),
        "the pre-readiness position still routes the scope's startup failure"
    );
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
    let (events, mut event_receiver) = crate::runtime::unbounded_mpsc();
    let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_defaults(plan.defaults.clone())
        .with_intensity_policy(plan.intensity_policy())
        .with_children(children)
        .with_next_ordered_start(Some(key))
        .build();
    plan.finish_transfer();

    assert!(scope.supervisor.is_initial(key));
    scope.spawn_child(key);
    assert!(!scope.supervisor.initial_ready(key));
    let exit = match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv()).await {
        crate::runtime::Timeout::Completed(Some(DriverEvent::Child(ChildEvent::Exited {
            child,
            incarnation,
            recorded,
            join,
            cancellation,
            readiness_signal_seen,
        }))) => (
            child,
            incarnation,
            recorded,
            join,
            cancellation,
            readiness_signal_seen,
        ),
        crate::runtime::Timeout::Completed(_) => panic!("the pre-ready failure reports exit"),
        crate::runtime::Timeout::Elapsed => panic!("the pre-ready failure exit must arrive"),
    };

    // The stop request latches after this batch was collected: it is
    // visible to `has_stop_request`, but its `Pending::Shutdown` follow-up
    // event belongs to the next batch.
    assert!(root.request_shutdown().is_some());
    assert!(root.has_stop_request(scope.epoch));

    scope.handle_exit(exit.0, exit.1, exit.2, exit.3, exit.4, exit.5);
    assert!(
        scope.children[key].restart_deadline.is_some(),
        "a latched scope stop does not reclassify exit dispatch"
    );
    assert!(matches!(
        scope.children[key].slot.member.record().stage,
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
    assert!(scope.children[key].restart_deadline.is_none());

    let Some(DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic })) =
        disposal_event_receiver.recv().await
    else {
        panic!("only construction disposal was armed")
    };
    scope.handle_construction_disposed(child, panic);
    assert!(matches!(
        scope.children[key].slot.member.record().stage,
        MemberStage::Terminal(_)
    ));
    assert!(
        !scope.children[key].slot.member.record().startup_aborted,
        "shutdown-first linearization publishes no startup abort"
    );
    assert!(matches!(
        root.record().startup,
        Some(Err(StartupError::ShutdownRequested))
    ));
}

#[crate::runtime::test]
async fn nested_membership_exhaustion_is_structured_and_fail_closed() {
    let nested_id = ChildId::from("nested");
    let mut parent_identity = ScopeIdentity::new();
    let nested_membership = parent_identity
        .mint_membership(&nested_id)
        .expect("nested membership available");
    let nested_member = MemberCell::new(nested_id, nested_membership);

    let worker_id = ChildId::from("worker");
    let mut child_identity = ScopeIdentity::near_exhaustion(worker_id.clone(), 7);
    child_identity
        .mint_membership(&worker_id)
        .expect("last usable membership is minted before the rebuild");
    let scope = ScopeCell::new(nested_member, ScopeFlavor::Ordered, child_identity);

    let mut tree = Tree::new();
    let worker = tree
        .add_task(
            worker_id.clone(),
            TaskDef::new(|_| future::pending::<crate::ExitResult>()),
        )
        .expect("provisional declaration succeeds");
    let ready = CompletionGatedLatch::default();
    let error = run_nested_tree(
        tree.into_core_for_test(),
        Arc::clone(&scope),
        crate::policy::ResolvedDefaults::default(),
        NestedScopeLatches {
            parent_ready: ready.clone(),
            ancestor: AncestorCommandLatches {
                shutdown: Latch::default(),
                abort: Latch::default(),
                abort_ack: Latch::default(),
            },
        },
    )
    .await
    .expect_err("the stable child-id domain is exhausted");

    let failure = error
        .startup_failure()
        .expect("framework provenance is retained");
    assert!(matches!(
        failure.cause,
        StartupFailureCause::IdentityExhausted { ref id } if id == &worker_id
    ));
    assert!(matches!(
        scope.record().startup,
        Some(Err(StartupError::StartupFailed(ref failure)))
            if matches!(failure.cause, StartupFailureCause::IdentityExhausted { ref id } if id == &worker_id)
    ));
    assert!(matches!(
        scope.record().state,
        ScopeState::Stopped {
            reason: StopReason::StartupFailed(_)
        }
    ));
    assert!(!ready.is_fired());
    assert!(matches!(worker.wait().await.kind(), ExitKind::NeverStarted));
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
    let nested_membership = parent_identity
        .mint_membership(&nested_id)
        .expect("nested membership available");
    let nested_member = MemberCell::new(nested_id, nested_membership);

    let worker_id = ChildId::from("worker");
    let mut child_identity = ScopeIdentity::near_exhaustion(worker_id.clone(), 7);
    child_identity
        .mint_membership(&worker_id)
        .expect("last usable membership is minted before the rebuild");
    let scope = ScopeCell::new(nested_member, ScopeFlavor::Ordered, child_identity);

    let mut tree = Tree::new();
    let worker = tree
        .add_task(
            worker_id,
            TaskDef::new(|_| future::pending::<crate::ExitResult>()),
        )
        .expect("provisional declaration succeeds");
    let epoch = ScopeEpochGuard::begin(&scope).expect("the first nested epoch is available");
    let ancestor_shutdown = Latch::default();
    match source {
        PreLoopStopSource::ScopeShutdown => {
            let target = scope
                .request_shutdown()
                .expect("declaration-time shutdown targets the live epoch");
            assert_eq!(target, epoch.epoch());
        }
        PreLoopStopSource::ScopeForce => scope.force_shutdown(epoch.epoch()),
        PreLoopStopSource::AncestorShutdown => {
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
            ancestor: AncestorCommandLatches {
                shutdown: ancestor_shutdown.clone(),
                abort: Latch::default(),
                abort_ack: Latch::default(),
            },
        },
        epoch,
    )
    .await;

    assert!(result.is_ok());
    assert!(
        ancestor_shutdown.is_fired(),
        "the upgraded verdict fires the latch that reports the exit as cancelled"
    );
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

#[crate::runtime::test]
async fn scope_incarnation_exhaustion_closes_nested_observation() {
    let parent = isolated_scope("parent", ScopeFlavor::Ordered);
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("nested");
    let membership = identity.mint_membership(&id).expect("membership available");
    let member = MemberCell::new(id, membership);
    let scope = ScopeCell::new(
        Arc::clone(&member),
        ScopeFlavor::Ordered,
        ScopeIdentity::new(),
    );
    let slot = SlotCell::new(Arc::clone(&member), Some(Arc::clone(&scope)));
    parent.set_admitted_children(vec![resident_projection(&slot)]);
    let mut snapshots = scope.subscribe_snapshots();
    let mut events = scope.subscribe_lifecycle();
    let mut counter = IncarnationCounter::near_exhaustion(member.membership());
    let first = counter.mint().expect("last incarnation mints");
    member.update(|record| {
        record.stage = MemberStage::Restarting;
        record.last_incarnation = Some(first);
        record.last_exit = Some(Exit::new(ExitKind::Completed, Cancellation::NotObserved));
    });
    scope.set_state(ScopeState::Stopped {
        reason: StopReason::Finished,
    });

    assert!(counter.mint().is_none());
    assert!(matches!(member.record().stage, MemberStage::Restarting));
    assert!(matches!(
        events.try_recv(),
        Ok(LifecycleItem::Event(crate::LifecycleEvent {
            kind: LifecycleEventKind::ScopeState {
                state: ScopeState::Stopped {
                    reason: StopReason::Finished
                }
            },
            ..
        }))
    ));
    assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Empty));
    assert!(parent.terminalize_child(
        &member,
        Exit::new(ExitKind::Completed, Cancellation::NotObserved),
        None,
        StartupDisposition::NotAborted,
    ));
    assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Closed));
    snapshots
        .changed()
        .await
        .expect("the prior incarnation's final snapshot remains observable");
    assert!(matches!(
        snapshots.borrow_latest().state,
        ScopeState::Stopped {
            reason: StopReason::Finished
        }
    ));
    assert!(snapshots.changed().await.is_err());
}

#[crate::runtime::test]
async fn never_started_nested_terminal_publishes_one_final_parent_snapshot() {
    let parent = isolated_scope("parent", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Ordered);
    resolve_fixture_options(&nested.member);
    let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
    parent.set_admitted_children(vec![resident_projection(&slot)]);
    let mut snapshots = parent.subscribe_snapshots();

    assert!(parent.terminalize_child(
        &nested.member,
        Exit::never_started(),
        None,
        StartupDisposition::NotAborted,
    ));
    snapshots
        .changed()
        .await
        .expect("no-incarnation terminal publishes the parent projection");

    assert!(matches!(
        snapshots
            .borrow_latest()
            .child("nested")
            .expect("retained nested child remains resident")
            .state,
        ChildState::Stopped { ref exit } if matches!(exit.kind(), ExitKind::NeverStarted)
    ));
    assert!(matches!(
        nested.record().state,
        ScopeState::Stopped {
            reason: StopReason::NeverStarted
        }
    ));
}

#[test]
fn lifecycle_sequence_exhaustion_poison_is_never_minted_and_becomes_lag() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("scope");
    let membership = identity.mint_membership(&id).expect("membership available");
    let member = MemberCell::new(id, membership);
    let scope = ScopeCell::new(member, ScopeFlavor::Ordered, ScopeIdentity::new());
    scope.set_lifecycle_sequence(u64::MAX - 2);
    let mut events = scope.subscribe_lifecycle();

    scope.emit(LifecycleEventKind::ScopeState {
        state: ScopeState::Starting,
    });
    scope.emit(LifecycleEventKind::ScopeState {
        state: ScopeState::Running,
    });
    scope.emit(LifecycleEventKind::ScopeState {
        state: ScopeState::Draining,
    });

    assert_eq!(events.try_recv(), Ok(LifecycleItem::Lagged { dropped: 2 }));
    let LifecycleItem::Event(event) = events.try_recv().expect("last mintable event remains")
    else {
        panic!("expected the final mintable event");
    };
    assert_eq!(event.seq.get(), u64::MAX - 1);
    assert_eq!(
        scope.snapshot().lifecycle_seq,
        crate::LifecycleSeq::EXHAUSTED
    );
}
