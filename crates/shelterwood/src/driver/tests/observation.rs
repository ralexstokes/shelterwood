use super::support::*;

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

#[crate::runtime::test]
async fn terminal_scope_waits_for_its_live_incarnation_to_stop() {
    let parent = isolated_scope("parent", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Ordered);
    let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
    assert!(parent.set_admitted_children(vec![resident_projection(&slot)]));

    let epoch = nested
        .begin_incarnation(ScopeState::Starting)
        .expect("nested scope epoch is available");
    let mut incarnations = IncarnationCounter::fixture(nested.member.membership());
    let incarnation = incarnations.mint();
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
        Exit::aborted(GracePhase::WithinGrace, Cancellation::Observed),
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
    assert!(parent.set_admitted_children(vec![resident_projection(&slot)]));

    let epoch = nested
        .begin_incarnation(ScopeState::Starting)
        .expect("nested scope epoch is available");
    let mut incarnations = IncarnationCounter::fixture(nested.member.membership());
    let incarnation = incarnations.mint();
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
        Exit::aborted(GracePhase::WithinGrace, Cancellation::Observed),
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
        assert!(parent.set_admitted_children(vec![resident_projection(&slot)]));

        let epoch = ScopeEpochGuard::begin(&nested).expect("nested scope epoch is available");
        let mut incarnations = IncarnationCounter::fixture(nested.member.membership());
        let incarnation = incarnations.mint();
        nested.member.update(|record| {
            record.stage = MemberStage::Running;
            record.incarnation = Some(incarnation);
            record.last_incarnation = Some(incarnation);
        });
        nested.set_state(ScopeState::Running);
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
            Exit::aborted(GracePhase::WithinGrace, Cancellation::Observed),
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
        crate::runtime::Timeout::Completed(JoinOutcome::Ok { value }) => {
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
    let membership = scope.mint_membership(&child_id);
    let child = MemberCell::new(membership);
    resolve_fixture_options(&child);
    let slot = SlotCell::new(Arc::clone(&child), None);
    assert!(scope.set_admitted_children(vec![resident_projection(&slot)]));
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
                        Exit::completed(Cancellation::NotObserved),
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
    assert!(parent.set_admitted_children(vec![resident_projection(&slot)]));

    let mut incarnations = IncarnationCounter::fixture(nested.member.membership());
    let last_incarnation = incarnations.mint();
    nested.member.update(|record| {
        record.stage = MemberStage::Restarting;
        record.incarnation = None;
        record.last_incarnation = Some(last_incarnation);
        record.last_exit = Some(Exit::completed(Cancellation::NotObserved));
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
    let MemberStage::Terminal(exit) = nested.member.record().stage.clone() else {
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
    assert!(matches!(parent_join, JoinOutcome::Cancelled));
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
        JoinOutcome::Cancelled
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
    assert!(matches!(parent_join, JoinOutcome::Cancelled));
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
        let start = nested_scope_start(&driver_scope);
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
            start,
        )
        .await
    });

    assert!(matches!(
        crate::runtime::join(driver).await,
        JoinOutcome::Panic { .. }
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
fn gate_handoff_rejects_a_scope_with_an_unadmitted_dynamic_reservation_without_rehoming() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    let original_gate = nested.observation_gate();
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
    assert!(
        !root.set_admitted_children(vec![resident_projection(&slot)]),
        "the reducer refuses an already-running member's admission"
    );
    assert!(
        root.resident_projections().is_empty(),
        "the illegal running-to-admitted transition publishes no residency"
    );
    assert!(
        original_gate.same_gate(&nested.observation_gate()),
        "a rejected admission leaves the live subtree on its original gate"
    );
}
