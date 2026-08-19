use super::support::*;

#[test]
#[should_panic(expected = "resident member options are resolved before snapshot publication")]
fn snapshot_rejects_a_resident_whose_options_were_never_resolved() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(
        child_id.clone(),
        root.mint_membership(&child_id)
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
    let result = receiver.recv_timeout(CAPTURE_PROBE_WAIT);
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
        observed.recv_timeout(CAPTURE_PROBE_WAIT),
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
        observed.recv_timeout(CAPTURE_PROBE_WAIT),
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
        stopped_observed.recv_timeout(CAPTURE_PROBE_WAIT),
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
    assert!(!scope.settled(Some(second)));

    scope.finish_incarnation(second, StopReason::Finished);
    assert_eq!(
        scope.record().state,
        ScopeState::Stopped {
            reason: StopReason::Finished,
        }
    );
    assert!(scope.settled(Some(second)));
}

#[test]
fn a_stale_scope_verdict_destroys_its_nested_exit_outside_the_gate() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let first = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("first scope epoch is available");
    scope.finish_incarnation(first, StopReason::Finished);
    let second = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("second scope epoch is available");
    scope.set_state(ScopeState::Running);

    let (exit, held_at_drop) = gate_probe_exit(&scope);
    scope.finish_incarnation(
        first,
        StopReason::StartupFailed(StartupFailure {
            cause: StartupFailureCause::Child {
                id: ChildId::from("worker"),
                membership: scope.member.membership(),
                exit,
            },
        }),
    );

    assert!(
        !wait_for_gate_probe(&held_at_drop),
        "a stale structured stop reason must outlive the observation gate"
    );
    assert_eq!(
        scope.record().state,
        ScopeState::Running,
        "the stale verdict must not rewrite the newer incarnation"
    );
    scope.finish_incarnation(second, StopReason::Finished);
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

    let nested_ref = ScopeRef {
        cell: Arc::clone(&nested),
    };
    let mut parked_shutdown = Box::pin(nested_ref.shutdown_and_wait(Duration::from_secs(10)));
    let first_shutdown_poll =
        std::future::poll_fn(|context| Poll::Ready(parked_shutdown.as_mut().poll(context))).await;
    assert!(
        first_shutdown_poll.is_pending(),
        "the live incarnation accepts its shutdown request before terminality"
    );

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

    let before_epilogue =
        std::future::poll_fn(|context| Poll::Ready(parked_shutdown.as_mut().poll(context))).await;
    assert!(
        before_epilogue.is_pending(),
        "a parked shutdown wait must not treat published membership terminality as scope settlement"
    );
    let mut fresh_shutdown = Box::pin(nested_ref.shutdown_and_wait(Duration::from_secs(10)));
    let fresh_before_epilogue =
        std::future::poll_fn(|context| Poll::Ready(fresh_shutdown.as_mut().poll(context))).await;
    assert!(
        fresh_before_epilogue.is_pending(),
        "a fresh shutdown wait must not short-circuit during the terminal-before-epilogue window"
    );

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
    parked_shutdown
        .await
        .expect("the target incarnation settled");
    fresh_shutdown
        .await
        .expect("the target incarnation settled");
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
async fn terminal_scope_in_drain_waits_for_its_live_incarnation_to_stop() {
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

    let nested_ref = ScopeRef {
        cell: Arc::clone(&nested),
    };
    let mut shutdown = Box::pin(nested_ref.shutdown_and_wait(Duration::from_secs(10)));
    let accepted =
        std::future::poll_fn(|context| Poll::Ready(shutdown.as_mut().poll(context))).await;
    assert!(accepted.is_pending(), "the live shutdown request parks");

    nested.set_state(ScopeState::Draining);
    let draining =
        std::future::poll_fn(|context| Poll::Ready(shutdown.as_mut().poll(context))).await;
    assert!(
        draining.is_pending(),
        "the shutdown wait enters its incarnation-completion phase"
    );

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
    let before_epilogue =
        std::future::poll_fn(|context| Poll::Ready(shutdown.as_mut().poll(context))).await;
    assert!(
        before_epilogue.is_pending(),
        "the completion wait must not treat published membership terminality as scope settlement"
    );

    nested.finish_incarnation(epoch, StopReason::ShutdownRequested);
    shutdown.await.expect("the target incarnation settled");
}

/// The `#201` published state with a *real* driver behind it: a nested
/// `ScopeRuntime` whose task the ancestor hard-aborted, so the settlement the
/// fence waits on is the one the production epilogue actually performs.
struct AbortedNestedDriverFixture {
    parent: Arc<ScopeCell>,
    nested: Arc<ScopeCell>,
    incarnation: crate::identity::Incarnation,
    driver: ScopeRuntime,
    _events: crate::runtime::UnboundedMpscReceiver<DriverEvent>,
}

impl AbortedNestedDriverFixture {
    fn new() -> Self {
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
        nested.set_admitted_children(Vec::new());

        let (events, events_receiver) = crate::runtime::unbounded_mpsc();
        let driver = ScopeRuntimeBuilder::new(Arc::clone(&nested), epoch, events)
            .with_lifecycle(ScopeLifecycle::running())
            .build();
        Self {
            parent,
            nested,
            incarnation,
            driver,
            _events: events_receiver,
        }
    }

    /// The ancestor's destruction edge: membership terminality is published
    /// while the nested driver still owns its unfinished incarnation.
    fn terminalize_from_parent(&self) {
        assert!(self.parent.terminalize_child(
            &self.nested.member,
            Exit::new(
                ExitKind::Aborted {
                    phase: GracePhase::WithinGrace,
                },
                Cancellation::Observed,
            ),
            Some(self.incarnation),
            StartupDisposition::NotAborted,
        ));
    }

    fn scope_ref(&self) -> ScopeRef {
        ScopeRef {
            cell: Arc::clone(&self.nested),
        }
    }
}

#[crate::runtime::test]
async fn aborted_nested_driver_epilogue_settles_a_shutdown_wait() {
    let fixture = AbortedNestedDriverFixture::new();
    let scope = fixture.scope_ref();
    let mut shutdown = Box::pin(scope.shutdown_and_wait(Duration::from_secs(10)));
    assert!(
        std::future::poll_fn(|context| Poll::Ready(shutdown.as_mut().poll(context)))
            .await
            .is_pending(),
        "the live incarnation accepts its shutdown request"
    );

    fixture.terminalize_from_parent();
    assert!(
        std::future::poll_fn(|context| Poll::Ready(shutdown.as_mut().poll(context)))
            .await
            .is_pending(),
        "published membership terminality is not the nested epilogue"
    );

    // Tokio finally drops the hard-aborted driver's frame. Its synchronous
    // epilogue is the whole settlement: nothing else can finish this epoch.
    drop(fixture.driver);
    assert!(
        matches!(
            std::future::poll_fn(|context| Poll::Ready(shutdown.as_mut().poll(context))).await,
            Poll::Ready(Ok(()))
        ),
        "the aborted driver's epilogue settles the fence"
    );
    assert!(matches!(
        fixture.nested.record().state,
        ScopeState::Stopped { .. }
    ));
}

#[crate::runtime::test]
async fn aborted_nested_driver_epilogue_wakes_a_parked_shutdown_task() {
    // Manual polling would hide a missing pulse, so this waiter is a real
    // task: it can only resolve if the epilogue actually wakes it.
    let fixture = AbortedNestedDriverFixture::new();
    let scope = fixture.scope_ref();
    let waiter =
        crate::runtime::spawn(
            async move { scope.shutdown_and_wait(Duration::from_secs(30)).await },
        );
    crate::runtime::yield_now().await;
    fixture.terminalize_from_parent();
    crate::runtime::yield_now().await;
    drop(fixture.driver);

    match crate::runtime::timeout(DRIVER_PROGRESS_WAIT, crate::runtime::join(waiter)).await {
        crate::runtime::Timeout::Completed(crate::runtime::JoinOutcome::Ok { value }) => {
            value.expect("the target incarnation settled");
        }
        crate::runtime::Timeout::Completed(_) => {
            panic!("the shutdown waiter task did not run to completion")
        }
        crate::runtime::Timeout::Elapsed => {
            panic!("the scope epilogue never woke the parked shutdown waiter")
        }
    }
}

#[crate::runtime::test]
async fn terminal_unstarted_scope_is_already_settled() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    scope
        .member
        .terminalize(Exit::never_started(), StartupDisposition::Unchanged);
    assert_eq!(scope.record().state, ScopeState::Unstarted);

    let scope_ref = ScopeRef { cell: scope };
    let mut shutdown = Box::pin(scope_ref.shutdown_and_wait(Duration::from_secs(10)));
    let first_poll =
        std::future::poll_fn(|context| Poll::Ready(shutdown.as_mut().poll(context))).await;
    assert!(
        matches!(first_poll, Poll::Ready(Ok(()))),
        "terminal membership with no spawned incarnation settles at entry"
    );
}

#[crate::runtime::test]
async fn shutdown_wait_settles_its_epoch_after_a_newer_incarnation_starts() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let first = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("first scope epoch is available");
    scope.set_state(ScopeState::Running);
    let scope_ref = ScopeRef {
        cell: Arc::clone(&scope),
    };
    let mut shutdown = Box::pin(scope_ref.shutdown_and_wait(Duration::from_secs(10)));
    let accepted =
        std::future::poll_fn(|context| Poll::Ready(shutdown.as_mut().poll(context))).await;
    assert!(
        accepted.is_pending(),
        "the first live epoch accepts shutdown"
    );

    scope.set_state(ScopeState::Draining);
    let draining =
        std::future::poll_fn(|context| Poll::Ready(shutdown.as_mut().poll(context))).await;
    assert!(draining.is_pending(), "the waiter targets the first epoch");

    scope.finish_incarnation(first, StopReason::ShutdownRequested);
    let second = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("second scope epoch is available");
    scope.set_state(ScopeState::Running);
    let superseded =
        std::future::poll_fn(|context| Poll::Ready(shutdown.as_mut().poll(context))).await;
    assert!(
        matches!(superseded, Poll::Ready(Ok(()))),
        "a newer live incarnation must not extend the captured epoch's wait"
    );
    assert!(!scope.settled(Some(second)));

    scope.finish_incarnation(second, StopReason::Finished);
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
    let driver = crate::runtime::spawn(run_scope_incarnation(plan, ScopeRole::Root, epoch));
    let abort = driver.abort_handle();

    assert!(matches!(
        crate::runtime::timeout(DRIVER_PROGRESS_WAIT, gate.wait_entered()).await,
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
        crate::runtime::timeout(DRIVER_PROGRESS_WAIT, waiter).await,
        crate::runtime::Timeout::Completed(StopReason::ShutdownRequested)
    ));
}

/// The public-surface form of the `#201` window, plus B.9's arming edge: the
/// ancestor's hard abort publishes terminal membership while the nested
/// incarnation is still pre-drain, so `shutdown_and_wait` has no cooperative
/// phase to bound and waits on the epilogue instead of expiring.
#[crate::runtime::test(flavor = "multi_thread", worker_threads = 4)]
async fn hard_aborted_incarnation_fences_shutdown_and_wait_without_arming_its_budget() {
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
    let driver = crate::runtime::spawn(run_scope_incarnation(plan, ScopeRole::Root, epoch));
    let abort = driver.abort_handle();
    assert!(matches!(
        crate::runtime::timeout(DRIVER_PROGRESS_WAIT, gate.wait_entered()).await,
        crate::runtime::Timeout::Completed(())
    ));

    abort.abort();
    assert!(matches!(
        crate::runtime::join(driver).await,
        crate::runtime::JoinOutcome::Cancelled
    ));

    // A budget far shorter than the blocked epilogue. It never arms: the
    // incarnation was hard-aborted before drain entry, so there is no
    // cooperative phase to escalate and no straggler report to make.
    let mut shutdown = Box::pin(nested.shutdown_and_wait(Duration::from_millis(10)));
    let inside_window =
        crate::runtime::timeout(Duration::from_millis(250), shutdown.as_mut()).await;
    gate.release();
    assert!(
        matches!(inside_window, crate::runtime::Timeout::Elapsed),
        "shutdown_and_wait resolved inside the terminal-before-epilogue window"
    );
    assert!(matches!(
        crate::runtime::timeout(DRIVER_PROGRESS_WAIT, shutdown).await,
        crate::runtime::Timeout::Completed(Ok(()))
    ));
    assert!(matches!(
        nested.snapshot().state,
        ScopeState::Stopped { .. }
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
    let driver = crate::runtime::spawn(run_scope_incarnation(plan, ScopeRole::Root, epoch));
    let abort = driver.abort_handle();

    assert!(matches!(
        crate::runtime::timeout(DRIVER_PROGRESS_WAIT, gate.wait_entered()).await,
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
        crate::runtime::timeout(DRIVER_PROGRESS_WAIT, waiter).await,
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
                child_shutdown: Latch::default(),
                ancestor: AncestorCommandLatches {
                    framework_shutdown: Latch::default(),
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
    scope.set_observation_config(first);

    let start = Arc::new(Barrier::new(2));
    let (first_update, first_update_seen) = std::sync::mpsc::sync_channel(0);
    let (first_snapshot, first_snapshot_seen) = std::sync::mpsc::sync_channel(0);
    let writer_scope = Arc::clone(&scope);
    let writer_start = Arc::clone(&start);
    let writer = std::thread::spawn(move || {
        writer_start.wait();
        for update in 0..UPDATES {
            let intensity = if update % 2 == 0 { second } else { first };
            writer_scope.set_observation_config(intensity);
            if update == 0 {
                first_update
                    .send(())
                    .expect("the snapshot reader remains available");
                first_snapshot_seen
                    .recv()
                    .expect("the snapshot reader acknowledges its observation");
            }
        }
    });

    start.wait();
    first_update_seen
        .recv()
        .expect("the writer publishes its first update");
    for snapshot in 0..UPDATES {
        let intensity = scope.snapshot().intensity;
        assert!(
            intensity == first || intensity == second,
            "a snapshot observes one complete configuration update"
        );
        if snapshot == 0 {
            first_snapshot
                .send(())
                .expect("the config writer remains available");
        }
    }
    writer.join().expect("config writer completes");
}

#[test]
fn snapshot_watch_batch_admission_is_one_committed_cut() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let first = isolated_scope("first", ScopeFlavor::Dynamic);
    let second = isolated_scope("second", ScopeFlavor::Dynamic);
    resolve_fixture_options(&first.member);
    resolve_fixture_options(&second.member);
    let first_slot = SlotCell::new(Arc::clone(&first.member), Some(first));
    let second_slot = SlotCell::new(Arc::clone(&second.member), Some(second));
    let snapshots = root.subscribe_snapshots();

    root.with_observation_gate(|txn| {
        root.clear_residents_locked(txn);
        root.admit_child_locked(resident_projection(&first_slot), txn);
        assert!(
            snapshots.borrow_latest().children.is_empty(),
            "the first admission must remain staged until the batch commits"
        );
        root.admit_child_locked(resident_projection(&second_slot), txn);
        assert!(
            snapshots.borrow_latest().children.is_empty(),
            "the complete batch must remain staged until its transaction commits"
        );
    });

    let committed = snapshots.borrow_latest();
    assert_eq!(committed.children.len(), 2);
    assert!(committed.child("first").is_some());
    assert!(committed.child("second").is_some());
}

#[test]
fn snapshot_watch_batch_admission_mints_one_generation_per_hub() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let branch = isolated_scope("branch", ScopeFlavor::Dynamic);
    resolve_fixture_options(&branch.member);
    let branch_slot = SlotCell::new(Arc::clone(&branch.member), Some(Arc::clone(&branch)));
    root.with_observation_gate(|txn| {
        root.admit_child_locked(resident_projection(&branch_slot), txn);
    });

    let first = isolated_scope("first", ScopeFlavor::Dynamic);
    let second = isolated_scope("second", ScopeFlavor::Dynamic);
    resolve_fixture_options(&first.member);
    resolve_fixture_options(&second.member);
    let first_slot = SlotCell::new(Arc::clone(&first.member), Some(first));
    let second_slot = SlotCell::new(Arc::clone(&second.member), Some(second));

    // Two hubs, because a batch publishes each admission to the admitting
    // scope and to every ancestor: coalescing must group publications by hub,
    // neither leaving one hub with several edges nor folding a descendant's
    // cut into its ancestor's.
    let root_snapshots = root.subscribe_snapshots();
    let mut branch_snapshots = branch.subscribe_snapshots();
    let root_generation = root_snapshots.current_generation();
    let branch_generation = branch_snapshots.current_generation();

    branch.with_observation_gate(|txn| {
        branch.admit_child_locked(resident_projection(&first_slot), txn);
        branch.admit_child_locked(resident_projection(&second_slot), txn);
    });

    assert_eq!(
        branch_snapshots.current_generation(),
        branch_generation + 1,
        "a batch admission mints exactly one generation edge on its own hub"
    );
    assert_eq!(
        root_snapshots.current_generation(),
        root_generation + 1,
        "and exactly one on the ancestor it propagates to"
    );

    let mut changed = Box::pin(branch_snapshots.changed());
    let Poll::Ready(Ok(committed)) = changed
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("the committed batch resolves the pending change");
    };
    drop(changed);
    assert_eq!(
        committed.children.len(),
        2,
        "the one delivered value carries the whole batch"
    );
    let mut changed = Box::pin(branch_snapshots.changed());
    assert!(
        matches!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ),
        "the batch owes no second delivery"
    );
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

    nested.set_observation_config(intensity);

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

/// SPEC §1's lock rule: nothing user-owned is destroyed inside a framework
/// critical section. `Exit`'s type-erased application error is the cell
/// layer's only user-owned value, and the tests below cover the paths that
/// retire one under the resident-tree observation gate: a losing
/// terminalization, a superseded snapshot (on subscription and on closure),
/// and the member record's `last_exit` slot (on a restart schedule and on
/// terminalization), lifecycle-ring eviction and residency retirement.
#[test]
fn a_losing_terminal_exit_payload_is_destroyed_outside_the_gate() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    scope.member.terminalize(
        Exit::new(ExitKind::Completed, Cancellation::NotObserved),
        StartupDisposition::Unchanged,
    );

    let (losing, held_at_drop) = gate_probe_exit(&scope);
    scope
        .member
        .terminalize(losing, StartupDisposition::Unchanged);

    assert!(
        !wait_for_gate_probe(&held_at_drop),
        "a competing terminalizer's exit payload must outlive the gate"
    );
}

#[test]
fn a_losing_supervised_terminal_exit_is_a_complete_noop_outside_the_gate() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(
        child_id.clone(),
        root.mint_membership(&child_id)
            .expect("child membership available"),
    );
    resolve_fixture_options(&member);
    root.admit_child(ResidentProjection::new(Arc::clone(&member), None));
    member.terminalize(
        Exit::new(ExitKind::Completed, Cancellation::NotObserved),
        StartupDisposition::NotAborted,
    );
    let winning_record = member.record();
    let mut events = root.subscribe_lifecycle();

    let (losing, held_at_drop) = gate_probe_exit(&root);
    assert!(
        !root.terminalize_child(&member, losing, None, StartupDisposition::Aborted),
        "the outer guard rejects a losing supervised terminalizer"
    );

    assert!(
        !wait_for_gate_probe(&held_at_drop),
        "the rejected exit payload must leave through post-gate disposal"
    );
    assert_eq!(
        member.record(),
        winning_record,
        "the rejected edge cannot reclassify the winning terminal record"
    );
    assert_eq!(
        events.try_recv(),
        Err(LifecycleTryRecvError::Empty),
        "the rejected edge publishes no Exited event"
    );
}

#[test]
fn a_retired_snapshot_payload_is_destroyed_outside_the_gate() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(
        child_id.clone(),
        root.mint_membership(&child_id)
            .expect("child membership available"),
    );
    resolve_fixture_options(&member);
    root.admit_child(ResidentProjection::new(Arc::clone(&member), None));
    let subscription = root.subscribe_snapshots();

    let (exit, held_at_drop) = gate_probe_exit(&root);
    root.terminalize_child(&member, exit, None, StartupDisposition::Unchanged);
    // Publication is skipped while receiverless, so the retained projection
    // goes stale and outlives both residency and the member cell whose record
    // holds the other clone. It is then the payload's last owner, and the next
    // subscription's refresh is what destroys it.
    drop(subscription);
    root.prune_child(&member);
    drop(member);
    assert_eq!(
        gate_probe_verdict(&held_at_drop),
        None,
        "the stale retained snapshot still owns the payload"
    );

    let _resubscribed = root.subscribe_snapshots();
    assert!(
        !wait_for_gate_probe(&held_at_drop),
        "a superseded snapshot's payload must outlive the gate"
    );
}

/// The same rule on the member record's `last_exit` slot: a restart schedule
/// overwrites the previous incarnation's exit while the record's watch lock
/// and the observation gate are both held.
#[test]
fn a_superseded_restart_exit_payload_is_destroyed_outside_the_gate() {
    let (root, member, mut incarnations) = restarting_member_fixture();

    let (probe, held_at_drop) = gate_probe_exit(&root);
    member.transition(MemberTransition::RestartScheduled {
        exit: probe,
        restart_count: RestartCount::ZERO.bump(),
        restart_at: None,
    });
    assert_eq!(
        gate_probe_verdict(&held_at_drop),
        None,
        "the record still owns the scheduled restart's exit"
    );

    let second = incarnations.mint().expect("restart incarnation available");
    member.transition(MemberTransition::Starting {
        incarnation: second,
    });
    member.transition(MemberTransition::RestartScheduled {
        exit: Exit::new(ExitKind::Completed, Cancellation::NotObserved),
        restart_count: RestartCount::ZERO.bump().bump(),
        restart_at: None,
    });
    assert!(
        !wait_for_gate_probe(&held_at_drop),
        "a superseded restart exit payload must outlive the gate"
    );
}

/// Terminalization writes the same slot, so it retires a prior restart's
/// payload on a path distinct from the losing-terminalizer one above.
#[test]
fn a_restart_exit_payload_superseded_by_terminalization_outlives_the_gate() {
    let (root, member, _incarnations) = restarting_member_fixture();

    let (probe, held_at_drop) = gate_probe_exit(&root);
    member.transition(MemberTransition::RestartScheduled {
        exit: probe,
        restart_count: RestartCount::ZERO.bump(),
        restart_at: None,
    });
    assert_eq!(
        gate_probe_verdict(&held_at_drop),
        None,
        "the record still owns the scheduled restart's exit"
    );

    member.terminalize(
        Exit::new(ExitKind::Completed, Cancellation::NotObserved),
        StartupDisposition::Unchanged,
    );
    assert!(
        !wait_for_gate_probe(&held_at_drop),
        "a terminalized member's superseded exit payload must outlive the gate"
    );
}

/// `SnapshotHub::close` retires the same slot as `subscribe`, on the path a
/// scope takes when it terminalizes after its last observer left.
#[test]
fn a_snapshot_retired_by_observation_closure_is_destroyed_outside_the_gate() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(
        child_id.clone(),
        root.mint_membership(&child_id)
            .expect("child membership available"),
    );
    resolve_fixture_options(&member);
    root.admit_child(ResidentProjection::new(Arc::clone(&member), None));
    let subscription = root.subscribe_snapshots();

    let (exit, held_at_drop) = gate_probe_exit(&root);
    root.terminalize_child(&member, exit, None, StartupDisposition::Unchanged);
    drop(subscription);
    root.prune_child(&member);
    drop(member);
    assert_eq!(
        gate_probe_verdict(&held_at_drop),
        None,
        "the stale retained snapshot still owns the payload"
    );

    root.terminalize_never_started();
    assert!(
        !wait_for_gate_probe(&held_at_drop),
        "the projection a closing hub supersedes must outlive the gate"
    );
}

#[test]
fn lifecycle_ring_eviction_isolates_a_failed_exit_payload() {
    let (root, member, _incarnations) = restarting_member_fixture();
    let incarnation = member
        .record()
        .incarnation
        .expect("the fixture starts one incarnation");
    let _events = root.subscribe_lifecycle();
    let (exit, held_at_drop) = gate_probe_exit(&root);

    root.emit(LifecycleEventKind::Exited {
        id: member.id().clone(),
        membership: member.membership(),
        incarnation,
        exit,
    });
    for _ in 0..LIFECYCLE_EVENT_CAPACITY {
        root.emit(LifecycleEventKind::ScopeState {
            state: ScopeState::Running,
        });
    }

    assert!(
        !wait_for_gate_probe(&held_at_drop),
        "an evicted lifecycle exit must not run its payload under the observation gate"
    );
}

#[test]
fn lifecycle_ring_eviction_isolates_an_exit_nested_in_scope_state() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let _events = root.subscribe_lifecycle();
    let (exit, held_at_drop) = gate_probe_exit(&root);

    root.emit(LifecycleEventKind::ScopeState {
        state: ScopeState::Stopped {
            reason: StopReason::StartupFailed(StartupFailure {
                cause: StartupFailureCause::Child {
                    id: ChildId::from("worker"),
                    membership: root.member.membership(),
                    exit,
                },
            }),
        },
    });
    for _ in 0..LIFECYCLE_EVENT_CAPACITY {
        root.emit(LifecycleEventKind::ScopeState {
            state: ScopeState::Running,
        });
    }

    assert!(
        !wait_for_gate_probe(&held_at_drop),
        "an evicted structured startup exit must not run under the observation gate"
    );
}

#[test]
fn scope_record_retirement_isolates_an_exit_nested_in_startup_result() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let (exit, held_at_drop) = gate_probe_exit(&root);
    root.set_startup(Err(StartupError::StartupFailed(StartupFailure {
        cause: StartupFailureCause::Child {
            id: ChildId::from("worker"),
            membership: root.member.membership(),
            exit,
        },
    })));

    let epoch = root
        .begin_incarnation(ScopeState::Starting)
        .expect("the fixture has an unused scope epoch");

    assert!(
        !wait_for_gate_probe(&held_at_drop),
        "clearing the retained startup result must isolate its nested exit"
    );
    root.finish_incarnation(epoch, StopReason::ShutdownRequested);
}

#[test]
fn snapshot_retirement_isolates_an_exit_nested_in_scope_state() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let _snapshots = root.subscribe_snapshots();
    let (exit, held_at_drop) = gate_probe_exit(&root);
    root.set_state(ScopeState::Stopped {
        reason: StopReason::StartupFailed(StartupFailure {
            cause: StartupFailureCause::Child {
                id: ChildId::from("worker"),
                membership: root.member.membership(),
                exit,
            },
        }),
    });

    let epoch = root
        .begin_incarnation(ScopeState::Starting)
        .expect("the fixture has an unused scope epoch");

    assert!(
        !wait_for_gate_probe(&held_at_drop),
        "retiring a structured startup snapshot must isolate its nested exit"
    );
    root.finish_incarnation(epoch, StopReason::ShutdownRequested);
}

#[test]
fn residency_can_release_the_last_member_arc_with_a_failed_exit() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(
        child_id.clone(),
        root.mint_membership(&child_id)
            .expect("child membership available"),
    );
    resolve_fixture_options(&member);
    root.admit_child(ResidentProjection::new(Arc::clone(&member), None));
    let weak = Arc::downgrade(&member);
    let (exit, held_at_drop) = gate_probe_exit(&root);
    member.terminalize(exit, StartupDisposition::Unchanged);
    drop(member);

    root.clear_residents();

    assert!(
        weak.upgrade().is_none(),
        "residency held the final member Arc"
    );
    assert!(
        !wait_for_gate_probe(&held_at_drop),
        "retiring the member record must not run its payload under the observation gate"
    );
}
