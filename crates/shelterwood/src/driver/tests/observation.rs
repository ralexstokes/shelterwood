use super::support::*;

#[test]
#[should_panic(expected = "resident member options are resolved before snapshot publication")]
fn snapshot_rejects_a_resident_whose_options_were_never_resolved() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(
        child_id.clone(),
        root.child_identity
            .lock()
            .expect("scope identity mutex poisoned")
            .mint_membership(&child_id)
            .expect("child membership available"),
    );

    root.admit_child(ResidentProjection::new(member, None));
    let _ = root.snapshot();
}

#[test]
fn independent_systems_do_not_share_an_observation_critical_section() {
    let first = isolated_scope("first", ScopeFlavor::Ordered);
    let second = isolated_scope("second", ScopeFlavor::Ordered);
    let first_gate = first.observation_gate();
    let second_gate = second.observation_gate();
    assert!(!first_gate.same_gate(&second_gate));

    let held = first_gate.lock();
    let (completed, receiver) = std::sync::mpsc::sync_channel(0);
    let worker = std::thread::spawn(move || {
        second.set_state(ScopeState::Starting);
        completed.send(()).expect("test receiver remains available");
    });
    let result = receiver.recv_timeout(Duration::from_secs(2));
    drop(held);
    worker.join().expect("independent transition succeeds");
    assert_eq!(
        result,
        Ok(()),
        "holding one system's gate must not stall another system"
    );
}

#[test]
fn snapshot_subscription_waker_can_reenter_snapshot() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let handle = ScopeRef {
        cell: Arc::clone(&scope),
    };
    let mut snapshots = handle.subscribe_snapshots();
    let (waker, observed) = snapshot_reentry_waker(&scope);
    let mut changed = Box::pin(snapshots.changed());
    assert!(matches!(
        changed.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    ));

    let publisher = std::thread::spawn(move || scope.set_state(ScopeState::Starting));
    assert_eq!(
        observed.recv_timeout(Duration::from_secs(2)),
        Ok(ScopeState::Starting),
        "the watch waker must run only after snapshot can reacquire the gate"
    );
    publisher.join().expect("snapshot publication completes");
    assert!(matches!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(_))
    ));
}

#[test]
fn lifecycle_subscription_waker_can_reenter_snapshot() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let handle = ScopeRef {
        cell: Arc::clone(&scope),
    };
    let mut events = handle.subscribe_lifecycle();
    let (waker, observed) = snapshot_reentry_waker(&scope);
    let mut next = Box::pin(events.recv());
    assert!(matches!(
        next.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    ));

    let publisher = std::thread::spawn(move || scope.set_state(ScopeState::Starting));
    assert_eq!(
        observed.recv_timeout(Duration::from_secs(2)),
        Ok(ScopeState::Starting),
        "the lifecycle waker must run only after snapshot can reacquire the gate"
    );
    publisher.join().expect("lifecycle publication completes");
    assert!(matches!(
        next.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Some(_))
    ));
}

#[test]
fn scope_wait_waker_can_reenter_snapshot_at_terminality() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let handle = ScopeRef {
        cell: Arc::clone(&scope),
    };
    let mut stopped = Box::pin(handle.wait_stopped());
    let (stopped_waker, stopped_observed) = snapshot_reentry_waker(&scope);
    assert!(matches!(
        stopped
            .as_mut()
            .poll(&mut Context::from_waker(&stopped_waker)),
        Poll::Pending
    ));

    let terminalizer = std::thread::spawn(move || scope.terminalize_never_started());
    assert!(matches!(
        stopped_observed.recv_timeout(Duration::from_secs(2)),
        Ok(ScopeState::Stopped { .. })
    ));
    terminalizer.join().expect("terminal publication completes");
    assert!(matches!(
        stopped
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(_)
    ));
}

#[test]
fn observation_gate_poison_does_not_wedge_later_observation() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let gate = scope.observation_gate();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _held = gate.lock();
            panic!("inject observation failure");
        }))
        .is_err()
    );

    scope.set_state(ScopeState::Starting);
    assert_eq!(scope.record().state, ScopeState::Starting);
}

#[test]
fn stale_scope_driver_cannot_stop_a_newer_live_incarnation_projection() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let first = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("first scope epoch is available");
    scope.finish_incarnation(first, StopReason::Finished);
    let second = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("second scope epoch is available");
    scope.set_state(ScopeState::Running);

    scope.finish_incarnation(first, StopReason::Finished);
    assert_eq!(scope.record().state, ScopeState::Running);
    assert!(!scope.incarnation_finished(second));

    scope.finish_incarnation(second, StopReason::Finished);
    assert_eq!(
        scope.record().state,
        ScopeState::Stopped {
            reason: StopReason::Finished,
        }
    );
    assert!(scope.incarnation_finished(second));
}

#[test]
fn a_new_incarnation_owns_an_unpublished_startup_verdict() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let first = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("first scope epoch is available");
    scope.set_startup(Ok(()));
    assert!(matches!(scope.record().startup, Some(Ok(()))));
    scope.finish_incarnation(first, StopReason::Finished);

    let second = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("second scope epoch is available");
    assert!(
        scope.record().startup.is_none(),
        "the first incarnation's verdict does not outlive its epoch"
    );
    scope.set_startup(Err(StartupError::ShutdownRequested));
    assert!(
        matches!(scope.record().startup, Some(Err(_))),
        "the write-once startup latch reopens per incarnation"
    );
    scope.finish_incarnation(second, StopReason::Finished);
}

#[crate::runtime::test]
async fn terminal_scope_waits_for_its_live_incarnation_to_stop() {
    let parent = isolated_scope("parent", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Ordered);
    let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
    parent.set_admitted_children(vec![resident_projection(&slot)]);

    let epoch = nested
        .begin_incarnation(ScopeState::Starting)
        .expect("nested scope epoch is available");
    let mut incarnations = IncarnationCounter::fixture(nested.member.membership());
    let incarnation = incarnations.mint().expect("child incarnation is available");
    nested.member.update(|record| {
        record.stage = MemberStage::Running;
        record.incarnation = Some(incarnation);
        record.last_incarnation = Some(incarnation);
    });
    nested.set_state(ScopeState::Running);

    assert!(parent.terminalize_child(
        &nested.member,
        Exit::new(
            ExitKind::Aborted {
                phase: GracePhase::WithinGrace,
            },
            Cancellation::Observed,
        ),
        Some(incarnation),
        StartupDisposition::NotAborted,
    ));

    let nested_ref = ScopeRef {
        cell: Arc::clone(&nested),
    };
    let mut snapshot_waiter =
        Box::pin(nested_ref.wait_for_child("missing", |_| false, Duration::from_secs(10)));
    let first_snapshot_poll =
        std::future::poll_fn(|context| Poll::Ready(snapshot_waiter.as_mut().poll(context))).await;
    assert!(
        first_snapshot_poll.is_pending(),
        "membership terminality must not close a live scope's snapshot stream"
    );

    let mut waiter = Box::pin(nested.wait_stopped());
    let first_poll =
        std::future::poll_fn(|context| Poll::Ready(waiter.as_mut().poll(context))).await;
    assert!(
        first_poll.is_pending(),
        "membership terminality does not imply that its live scope incarnation stopped"
    );

    nested.finish_incarnation(epoch, StopReason::ShutdownRequested);
    assert_eq!(waiter.await, StopReason::ShutdownRequested);
    assert!(matches!(
        snapshot_waiter.await,
        Err(crate::WaitError::ScopeTerminated {
            state: ScopeState::Stopped {
                reason: StopReason::ShutdownRequested
            }
        })
    ));
}

#[crate::runtime::test]
async fn wait_for_child_reloads_after_its_predicate_closes_the_snapshot_stream() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let epoch = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("scope epoch is available");
    scope.set_state(ScopeState::Running);
    let child_id = ChildId::from("child");
    let membership = scope
        .child_identity
        .lock()
        .expect("scope identity mutex is healthy")
        .mint_membership(&child_id)
        .expect("child membership is available");
    let child = MemberCell::new(child_id, membership);
    resolve_fixture_options(&child);
    let slot = SlotCell::new(Arc::clone(&child), None);
    scope.set_admitted_children(vec![resident_projection(&slot)]);
    let scope_ref = ScopeRef {
        cell: Arc::clone(&scope),
    };
    let closing_scope = Arc::clone(&scope);
    let mut first = true;

    let result = scope_ref
        .wait_for_child(
            "child",
            move |_| {
                if std::mem::take(&mut first) {
                    closing_scope.finish_root_incarnation(
                        epoch,
                        StopReason::Finished,
                        Exit::new(ExitKind::Completed, Cancellation::NotObserved),
                    );
                }
                false
            },
            Duration::from_secs(1),
        )
        .await;

    assert!(matches!(
        result,
        Err(crate::WaitError::ScopeTerminated {
            state: ScopeState::Stopped {
                reason: StopReason::Finished
            }
        })
    ));
}

#[crate::runtime::test]
async fn terminality_fallback_preserves_restart_window_scope_reason() {
    let parent = isolated_scope("parent", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Ordered);
    let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
    parent.set_admitted_children(vec![resident_projection(&slot)]);

    let mut incarnations = IncarnationCounter::fixture(nested.member.membership());
    let last_incarnation = incarnations.mint().expect("child incarnation is available");
    nested.member.update(|record| {
        record.stage = MemberStage::Restarting;
        record.incarnation = None;
        record.last_incarnation = Some(last_incarnation);
        record.last_exit = Some(Exit::new(ExitKind::Completed, Cancellation::NotObserved));
    });
    nested.set_state(ScopeState::Stopped {
        reason: StopReason::Finished,
    });
    let mut snapshots = nested.subscribe_snapshots();

    let mut terminality = Obligation::new(
        ChildTerminality {
            root: Arc::clone(&parent),
            slot,
        },
        discharge_child_terminality,
    );
    terminality.discharge();

    assert_eq!(nested.wait_stopped().await, StopReason::Finished);
    let MemberStage::Terminal(exit) = nested.member.record().stage else {
        panic!("the fallback must terminalize the nested membership");
    };
    assert!(matches!(
        exit.kind(),
        ExitKind::Aborted {
            phase: GracePhase::WithinGrace
        }
    ));
    assert_eq!(exit.cancellation(), Cancellation::Observed);
    assert_eq!(
        snapshots.borrow_latest().state,
        ScopeState::Stopped {
            reason: StopReason::Finished
        }
    );
    assert!(
        snapshots.changed().await.is_err(),
        "the fallback closes observation after retaining the final stopped snapshot"
    );
}

#[crate::runtime::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_initial_scope_factory_owns_its_stop_epilogue() {
    let gate = Arc::new(FactoryGate::default());
    let mut tree = Tree::new();
    let nested = tree
        .add_subtree(
            "nested",
            SubtreeDef::factory({
                let gate = Arc::clone(&gate);
                move || {
                    gate.block();
                    pending_tree()
                }
            }),
        )
        .expect("nested scope is valid");
    let plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = ScopeEpochGuard::begin(&root).expect("parent epoch is available");
    let driver = crate::runtime::spawn(run_scope_incarnation(
        plan.take_for_runtime(),
        ScopeRole::Root,
        epoch,
    ));
    let abort = driver.abort_handle();

    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(2), gate.wait_entered()).await,
        crate::runtime::Timeout::Completed(())
    ));
    let factory_state = nested.snapshot().state.clone();

    abort.abort();
    let parent_join = crate::runtime::join(driver).await;
    let mut waiter = Box::pin(nested.wait_stopped());
    let before_release =
        std::future::poll_fn(|context| Poll::Ready(waiter.as_mut().poll(context))).await;
    gate.release();
    assert_eq!(factory_state, ScopeState::Starting);
    assert!(matches!(
        parent_join,
        crate::runtime::JoinOutcome::Cancelled
    ));
    assert!(
        before_release.is_pending(),
        "an executing initial factory still owns the final scope epilogue"
    );
    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(2), waiter).await,
        crate::runtime::Timeout::Completed(StopReason::ShutdownRequested)
    ));
}

#[crate::runtime::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_restart_scope_factory_supersedes_the_stale_stopped_projection() {
    let gate = Arc::new(FactoryGate::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let nested = tree
        .add_subtree(
            "nested",
            SubtreeDef::factory({
                let gate = Arc::clone(&gate);
                let calls = Arc::clone(&calls);
                move || {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        finished_tree()
                    } else {
                        gate.block();
                        pending_tree()
                    }
                }
            })
            .restart(RestartPolicy::new(
                RestartCondition::Always,
                Backoff::Immediate,
            )),
        )
        .expect("restartable nested scope is valid");
    let plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = ScopeEpochGuard::begin(&root).expect("parent epoch is available");
    let driver = crate::runtime::spawn(run_scope_incarnation(
        plan.take_for_runtime(),
        ScopeRole::Root,
        epoch,
    ));
    let abort = driver.abort_handle();

    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(2), gate.wait_entered()).await,
        crate::runtime::Timeout::Completed(())
    ));
    let factory_calls = calls.load(Ordering::SeqCst);
    let factory_state = nested.snapshot().state.clone();

    abort.abort();
    let parent_join = crate::runtime::join(driver).await;
    let mut waiter = Box::pin(nested.wait_stopped());
    let before_release =
        std::future::poll_fn(|context| Poll::Ready(waiter.as_mut().poll(context))).await;
    gate.release();
    assert_eq!(factory_calls, 2);
    assert_eq!(
        factory_state,
        ScopeState::Starting,
        "the second epoch supersedes the first incarnation's Stopped projection before its factory runs"
    );
    assert!(matches!(
        parent_join,
        crate::runtime::JoinOutcome::Cancelled
    ));
    assert!(
        before_release.is_pending(),
        "an executing restart factory still owns the final scope epilogue"
    );
    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(2), waiter).await,
        crate::runtime::Timeout::Completed(StopReason::ShutdownRequested)
    ));
}

#[crate::runtime::test]
async fn panicking_nested_factory_releases_its_pre_driver_epoch() {
    let scope = isolated_scope("nested", ScopeFlavor::Ordered);
    let driver_scope = Arc::clone(&scope);
    let driver = crate::runtime::spawn(async move {
        let factory = Arc::new(|| -> crate::plan::BuilderCore {
            panic!("injected nested factory panic");
        });
        run_nested_factory(
            factory,
            driver_scope,
            crate::policy::ResolvedDefaults::default(),
            NestedScopeLatches {
                parent_ready: CompletionGatedLatch::default(),
                ancestor: AncestorCommandLatches {
                    shutdown: Latch::default(),
                    abort: Latch::default(),
                    abort_ack: Latch::default(),
                },
            },
        )
        .await
    });

    assert!(matches!(
        crate::runtime::join(driver).await,
        crate::runtime::JoinOutcome::Panic { .. }
    ));
    assert_eq!(
        scope.record().state,
        ScopeState::Stopped {
            reason: StopReason::ShutdownRequested,
        }
    );
    let successor =
        ScopeEpochGuard::begin(&scope).expect("factory unwind retires the reserved scope epoch");
    successor.finish(StopReason::NeverStarted);
}

#[test]
fn pre_driver_epoch_guard_releases_on_cancellation_and_unwind() {
    let cancelled = isolated_scope("cancelled", ScopeFlavor::Ordered);
    let guard = ScopeEpochGuard::begin(&cancelled).expect("first epoch is available");
    let mut setup = Box::pin(async move {
        let _guard = guard;
        future::pending::<()>().await;
    });
    assert!(
        setup
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(setup);
    let successor =
        ScopeEpochGuard::begin(&cancelled).expect("cancelling pre-driver setup retires its epoch");
    successor.finish(StopReason::NeverStarted);

    let unwound = isolated_scope("unwound", ScopeFlavor::Ordered);
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _guard = ScopeEpochGuard::begin(&unwound).expect("unwind epoch is available");
            panic!("injected pre-driver unwind");
        }))
        .is_err()
    );
    let successor =
        ScopeEpochGuard::begin(&unwound).expect("unwinding pre-driver setup retires its epoch");
    successor.finish(StopReason::NeverStarted);
}

#[test]
fn a_declined_epoch_still_publishes_its_owned_terminal_exit() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let epoch = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("scope epoch is available");
    // The orderly finisher retires the epoch without a terminal exit, so
    // a second owner still holds the only membership verdict.
    scope.finish_incarnation(epoch, StopReason::Finished);
    assert!(!matches!(
        scope.member.record().stage,
        MemberStage::Terminal(_)
    ));

    scope.finish_root_incarnation(
        epoch,
        StopReason::ShutdownRequested,
        Exit::new(
            ExitKind::Aborted {
                phase: GracePhase::WithinGrace,
            },
            Cancellation::Observed,
        ),
    );
    assert!(matches!(
        scope.member.record().stage,
        MemberStage::Terminal(_)
    ));
    assert_eq!(
        scope.record().state,
        ScopeState::Stopped {
            reason: StopReason::Finished,
        },
        "a declined epoch must not rewrite the retired stop reason"
    );
}

#[test]
fn admitted_subtrees_share_their_parent_observation_gate() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));

    root.set_admitted_children(vec![resident_projection(&slot)]);

    assert!(
        root.observation_gate()
            .same_gate(&nested.observation_gate())
    );
}

#[test]
fn admitted_subtree_rehomes_existing_descendants_to_one_gate() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    let leaf = isolated_scope("leaf", ScopeFlavor::Ordered);
    let raw_leaf_id = ChildId::from("raw-leaf");
    let raw_leaf = MemberCell::new(
        raw_leaf_id.clone(),
        nested
            .child_identity
            .lock()
            .expect("scope identity mutex poisoned")
            .mint_membership(&raw_leaf_id)
            .expect("raw leaf membership is available"),
    );
    let leaf_slot = SlotCell::new(Arc::clone(&leaf.member), Some(Arc::clone(&leaf)));
    let raw_leaf_slot = SlotCell::new(Arc::clone(&raw_leaf), None);
    nested.set_admitted_children(vec![
        resident_projection(&leaf_slot),
        resident_projection(&raw_leaf_slot),
    ]);
    assert!(
        nested
            .observation_gate()
            .same_gate(&leaf.observation_gate())
    );
    assert!(
        nested
            .observation_gate()
            .same_gate(&raw_leaf.observation_gate())
    );

    let nested_slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
    root.set_admitted_children(vec![resident_projection(&nested_slot)]);

    let root_gate = root.observation_gate();
    assert!(root_gate.same_gate(&nested.observation_gate()));
    assert!(root_gate.same_gate(&leaf.observation_gate()));
    assert!(root_gate.same_gate(&raw_leaf.observation_gate()));
}

#[test]
fn receiverless_config_state_is_atomic_under_concurrent_snapshots() {
    const UPDATES: usize = 2_000;

    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let first = Intensity::new(1, Duration::from_secs(1)).expect("valid first intensity");
    let second = Intensity::new(2, Duration::from_secs(2)).expect("valid second intensity");
    scope.set_observation_config(Default::default(), first);

    let start = Arc::new(Barrier::new(2));
    let writer_scope = Arc::clone(&scope);
    let writer_start = Arc::clone(&start);
    let writer = std::thread::spawn(move || {
        writer_start.wait();
        for update in 0..UPDATES {
            let intensity = if update % 2 == 0 { second } else { first };
            writer_scope.set_observation_config(Default::default(), intensity);
        }
    });

    start.wait();
    for _ in 0..UPDATES {
        let intensity = scope.snapshot().intensity;
        assert!(
            intensity == first || intensity == second,
            "a snapshot observes one complete configuration update"
        );
    }
    writer.join().expect("config writer completes");
}

#[test]
fn plain_resident_state_is_released_before_recursive_removed_publication() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let first = isolated_scope("first", ScopeFlavor::Dynamic);
    let second = isolated_scope("second", ScopeFlavor::Dynamic);
    resolve_fixture_options(&first.member);
    resolve_fixture_options(&second.member);
    let first_slot = SlotCell::new(Arc::clone(&first.member), Some(first));
    let second_slot = SlotCell::new(Arc::clone(&second.member), Some(second));
    let mut events = root.subscribe_lifecycle();
    let snapshots = root.subscribe_snapshots();

    root.set_admitted_children(vec![
        resident_projection(&first_slot),
        resident_projection(&second_slot),
    ]);
    root.clear_residents();

    assert!(root.resident_projections().is_empty());
    assert!(snapshots.borrow_latest().children.is_empty());
    let mut added = 0;
    let mut removed = 0;
    while let Ok(LifecycleItem::Event(event)) = events.try_recv() {
        match event.kind {
            LifecycleEventKind::Added { .. } => added += 1,
            LifecycleEventKind::Removed { .. } => removed += 1,
            _ => {}
        }
    }
    assert_eq!((added, removed), (2, 2));
}

#[test]
fn plain_parent_state_preserves_nested_snapshot_propagation() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    let mut incarnations = IncarnationCounter::near_exhaustion(nested.member.membership());
    resolve_fixture_options(&nested.member);
    let nested_slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
    root.set_admitted_children(vec![resident_projection(&nested_slot)]);
    // Start the nested member along the production admit-then-spawn order so
    // the transition-source assertions in `apply_transition` hold here too.
    nested.member.transition(MemberTransition::Starting {
        incarnation: incarnations.mint().expect("incarnation available"),
    });
    let snapshots = root.subscribe_snapshots();
    let intensity = Intensity::new(7, Duration::from_secs(11)).expect("valid intensity");

    nested.set_observation_config(Default::default(), intensity);

    assert_eq!(
        snapshots
            .borrow_latest()
            .child("nested")
            .and_then(|child| child.nested.as_deref())
            .map(|snapshot| snapshot.intensity),
        Some(intensity)
    );
}

#[test]
fn lifecycle_emit_resolves_the_ancestor_chain_once() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    let nested_slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
    root.set_admitted_children(vec![resident_projection(&nested_slot)]);
    let mut root_events = root.subscribe_lifecycle();

    let _ = nested.take_ancestor_parent_reads();
    let _ = root.take_ancestor_parent_reads();
    nested.emit(LifecycleEventKind::ScopeState {
        state: ScopeState::Running,
    });

    assert_eq!(
        nested.take_ancestor_parent_reads(),
        1,
        "the emitting scope reads its parent link once"
    );
    assert_eq!(
        root.take_ancestor_parent_reads(),
        1,
        "each ancestor reads its parent link once while building the chain"
    );
    assert!(matches!(
        root_events.try_recv(),
        Ok(LifecycleItem::Event(event))
            if matches!(event.kind, LifecycleEventKind::ScopeState { state: ScopeState::Running })
    ));
}

#[test]
fn pre_admission_observer_retries_after_gate_handoff() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    let captures = nested.probe_gate_captures();
    let prior_gate = nested.observation_gate();
    let held = prior_gate.lock();
    let observer = Arc::clone(&nested);
    let worker = std::thread::spawn(move || observer.set_state(ScopeState::Starting));

    // The capture report proves the observer committed to the
    // pre-admission gate, which the held guard keeps it from acquiring.
    assert_eq!(
        captures
            .recv_timeout(CAPTURE_PROBE_WAIT)
            .expect("the observer reports its capture within the bound"),
        GateCapture::Observation
    );

    // Model the instant at which adoption owns the old gate and publishes
    // the replacement. The waiting observer must acquire the old gate,
    // detect this handoff, and retry on the root gate.
    nested.replace_observation_gate(root.observation_gate());
    drop(held);
    worker.join().expect("observer follows the gate handoff");

    assert_eq!(
        captures
            .recv_timeout(CAPTURE_PROBE_WAIT)
            .expect("the observer reports its retry within the bound"),
        GateCapture::Observation,
        "the handoff forces one retry capture on the root gate"
    );
    assert_eq!(nested.record().state, ScopeState::Starting);
    assert!(
        root.observation_gate()
            .same_gate(&nested.observation_gate())
    );
}
#[test]
fn gate_handoff_waits_for_an_in_flight_observation_edge() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    let captures = nested.probe_gate_captures();
    let (entered, entered_receiver) = std::sync::mpsc::sync_channel(0);
    let (release, release_receiver) = std::sync::mpsc::sync_channel(0);
    let observer = Arc::clone(&nested);
    let observation = std::thread::spawn(move || {
        observer.with_observation_gate(|_| {
            entered.send(()).expect("test receiver remains available");
            release_receiver
                .recv()
                .expect("test sender releases the observation edge");
        });
    });
    entered_receiver
        .recv()
        .expect("observer enters the pre-admission edge");
    assert_eq!(
        captures
            .recv_timeout(CAPTURE_PROBE_WAIT)
            .expect("the observation edge reports its capture within the bound"),
        GateCapture::Observation
    );

    let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
    let adopting_root = Arc::clone(&root);
    let (adopted, adopted_receiver) = std::sync::mpsc::sync_channel(0);
    let adoption = std::thread::spawn(move || {
        adopting_root.set_admitted_children(vec![resident_projection(&slot)]);
        adopted.send(()).expect("test receiver remains available");
    });

    // The adoption capture proves handoff committed to the prior gate and
    // is blocked behind the complete observation edge rather than
    // replacing it concurrently, so adoption cannot yet have completed.
    assert_eq!(
        captures
            .recv_timeout(CAPTURE_PROBE_WAIT)
            .expect("adoption reports its capture within the bound"),
        GateCapture::Adoption
    );
    assert!(matches!(
        adopted_receiver.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));

    release
        .send(())
        .expect("active observation remains available");
    observation.join().expect("observation edge completes");
    adopted_receiver
        .recv()
        .expect("adoption reports completion after the edge");
    adoption.join().expect("gate handoff completes");
    assert!(
        root.observation_gate()
            .same_gate(&nested.observation_gate())
    );
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "a scope with a live dynamic route is never re-homed")]
fn gate_handoff_rejects_a_scope_with_an_unadmitted_dynamic_reservation() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    nested
        .member
        .update(|record| record.stage = MemberStage::Running);
    nested.set_state(ScopeState::Running);
    let (events, _receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(events);
    nested.set_dynamic_route(Some(control));
    let _reservation = nested
        .with_observation_gate(|txn| {
            super::super::admission_control::reserve_dynamic_in(
                &nested,
                ChildId::from("reserved"),
                None,
                txn,
            )
        })
        .expect("the synthetic running scope reserves a child");
    let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));

    // Public layering cannot reach this adoption: a reservation requires a
    // started driver, while a scope is parented before its driver starts.
    root.set_admitted_children(vec![resident_projection(&slot)]);
}
