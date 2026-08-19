use super::support::*;

struct TrySendOnWake {
    actor: ActorRef<u8>,
    observed: Mutex<Option<SendErrorKind>>,
}

impl TrySendOnWake {
    fn observe(&self) {
        let error = self
            .actor
            .try_send(1)
            .expect_err("a terminality-derived wake observes a closed mailbox");
        *self.observed.lock().expect("observation mutex poisoned") = Some(error.kind);
    }
}

impl Wake for TrySendOnWake {
    fn wake(self: Arc<Self>) {
        self.observe();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.observe();
    }
}

struct ObserveMemberOnMailboxWake {
    member: Arc<MemberCell>,
    competing_exit: Exit,
    observed: Mutex<Option<(MemberStage, MemberStage)>>,
}

impl ObserveMemberOnMailboxWake {
    fn observe(&self) {
        let before = self.member.record().stage;
        self.member
            .terminalize(self.competing_exit.clone(), StartupDisposition::Unchanged);
        let after = self.member.record().stage;
        *self.observed.lock().expect("observation mutex poisoned") = Some((before, after));
    }
}

impl Wake for ObserveMemberOnMailboxWake {
    fn wake(self: Arc<Self>) {
        self.observe();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.observe();
    }
}

struct ObserveScopeOnStartupWake {
    scope: Arc<ScopeCell>,
    epoch: Option<Epoch>,
    observed: Mutex<Option<(MemberStage, Option<bool>)>>,
}

impl ObserveScopeOnStartupWake {
    fn observe(&self) {
        let member = self.scope.member.record().stage;
        let settled = self.epoch.map(|epoch| self.scope.settled(Some(epoch)));
        *self.observed.lock().expect("observation mutex poisoned") = Some((member, settled));
    }
}

#[derive(Clone, Copy, Debug)]
enum TerminalStopPath {
    LiveEpoch,
    NoLiveEpoch,
    NeverStarted,
}

#[crate::runtime::test]
async fn terminal_stop_paths_share_one_complete_observation_transition() {
    for (path, reason) in [
        (TerminalStopPath::LiveEpoch, StopReason::ShutdownRequested),
        (TerminalStopPath::NoLiveEpoch, StopReason::ShutdownRequested),
        (TerminalStopPath::NeverStarted, StopReason::NeverStarted),
    ] {
        let scope = isolated_scope("root", ScopeFlavor::Ordered);
        let epoch = matches!(path, TerminalStopPath::LiveEpoch).then(|| {
            scope
                .begin_incarnation(ScopeState::Starting)
                .expect("test scope epoch is available")
        });
        let handle = ScopeRef {
            cell: Arc::clone(&scope),
        };
        let mut snapshots = handle.subscribe_snapshots();
        let mut events = handle.subscribe_lifecycle();

        match path {
            TerminalStopPath::LiveEpoch => scope.finish_root_incarnation(
                epoch.expect("live path owns an epoch"),
                reason.clone(),
                Exit::never_started(),
            ),
            TerminalStopPath::NoLiveEpoch => {
                scope.finish_live_root_incarnation(reason.clone(), Exit::never_started())
            }
            TerminalStopPath::NeverStarted => scope.terminalize_never_started(),
        }

        let expected_state = ScopeState::Stopped {
            reason: reason.clone(),
        };
        assert!(matches!(
            scope.member.record().stage,
            MemberStage::Terminal(_)
        ));
        assert_eq!(scope.record().state, expected_state);
        assert_eq!(
            epoch.map(|epoch| scope.settled(Some(epoch))),
            epoch.map(|_| true)
        );
        assert_eq!(snapshots.borrow_latest().state, expected_state);
        assert!(matches!(
            events.try_recv(),
            Ok(LifecycleItem::Event(crate::LifecycleEvent {
                kind: LifecycleEventKind::ScopeState { state },
                ..
            })) if state == expected_state
        ));
        assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Closed));

        snapshots
            .changed()
            .await
            .expect("the final snapshot precedes observation closure");
        assert!(snapshots.changed().await.is_err());
    }
}

#[crate::runtime::test]
async fn root_driver_panic_mid_drain_upgrades_to_the_join_monitor_verdict() {
    let scope = isolated_scope("root", ScopeFlavor::Ordered);
    let epoch = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("test scope epoch is available");
    scope
        .member
        .update(|record| record.stage = MemberStage::Running);
    scope.set_state_and_startup(ScopeState::Running, Ok(()));

    let mut lifecycle = ScopeLifecycle::running();
    assert!(
        lifecycle.begin_drain(StopReason::Finished).is_some(),
        "the driver enters its orderly drain before panicking"
    );
    scope.set_state(ScopeState::Draining);

    let handle = ScopeRef {
        cell: Arc::clone(&scope),
    };
    let mut snapshots = handle.subscribe_snapshots();
    let mut events = handle.subscribe_lifecycle();

    let (events_tx, _events_rx) = crate::runtime::unbounded_mpsc();
    let driver = crate::runtime::spawn({
        let scope = Arc::clone(&scope);
        async move {
            let _runtime = ScopeRuntimeBuilder::new(scope, epoch, events_tx)
                .with_lifecycle(lifecycle)
                .build();
            panic!("root driver panicked mid-drain");
            #[allow(unreachable_code)]
            crate::cells::RetainedStopReason::new(StopReason::Finished)
        }
    });
    let panic_exit =
        super::super::classify_retained_root_driver_join(crate::runtime::join(driver).await)
            .expect_err("the injected root-driver panic reaches its join monitor");

    let drained = ScopeState::Stopped {
        reason: StopReason::Finished,
    };
    let upgraded = ScopeState::Stopped {
        reason: StopReason::ShutdownRequested,
    };
    assert_eq!(
        scope.record().state,
        drained,
        "ScopeRuntime's unwind epilogue publishes its established drain reason"
    );
    assert!(
        !matches!(scope.member.record().stage, MemberStage::Terminal(_)),
        "the unwind epilogue leaves root terminality to the join monitor"
    );
    assert!(
        scope.settled(Some(epoch)),
        "the unwind epilogue retires its epoch, so the join monitor must take \
         the no-live-epoch fallback this test is named for"
    );

    scope.finish_live_root_incarnation(StopReason::ShutdownRequested, panic_exit.clone());

    assert_eq!(
        scope.record().state,
        upgraded,
        "ShutdownRequested strictly outranks the abandoned drain's Finished"
    );
    assert!(matches!(
        scope.member.record().stage,
        MemberStage::Terminal(ref exit) if exit == &panic_exit
    ));
    assert!(matches!(
        events.try_recv(),
        Ok(LifecycleItem::Event(crate::LifecycleEvent {
            kind: LifecycleEventKind::ScopeState { state },
            ..
        })) if state == drained
    ));
    assert!(
        matches!(
            events.try_recv(),
            Ok(LifecycleItem::Event(crate::LifecycleEvent {
                kind: LifecycleEventKind::ScopeState { state },
                ..
            })) if state == upgraded
        ),
        "the corrected verdict reaches the stream before it closes"
    );
    assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Closed));

    assert_eq!(snapshots.borrow_latest().state, upgraded);
    snapshots
        .changed()
        .await
        .expect("the upgraded stopped snapshot precedes fallback closure");
    assert!(snapshots.changed().await.is_err());
}

/// Drives one pair of competing stopped publications through the shared
/// publisher and reports the settled record plus every `ScopeState` edge the
/// lifecycle stream carried.
async fn resolve_competing_stops(
    first: StopReason,
    second: StopReason,
) -> (ScopeState, Vec<ScopeState>) {
    let scope = isolated_scope("root", ScopeFlavor::Ordered);
    let epoch = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("test scope epoch is available");
    let handle = ScopeRef {
        cell: Arc::clone(&scope),
    };
    let mut events = handle.subscribe_lifecycle();

    scope.finish_incarnation(epoch, first);
    scope.finish_live_root_incarnation(second, Exit::never_started());

    let mut states = Vec::new();
    while let Ok(LifecycleItem::Event(event)) = events.try_recv() {
        if let LifecycleEventKind::ScopeState { state } = event.kind {
            states.push(state);
        }
    }
    assert_eq!(
        events.try_recv(),
        Err(LifecycleTryRecvError::Closed),
        "the fallback closes observation once its verdict is published"
    );
    let settled = scope.record().state;
    assert_eq!(
        settled,
        ScopeState::Stopped {
            reason: scope.wait_stopped().await
        },
        "wait_stopped reports exactly the settled record"
    );
    (settled, states)
}

#[crate::runtime::test]
async fn a_weaker_first_stop_is_upgraded_by_a_stronger_second() {
    let (settled, states) =
        resolve_competing_stops(StopReason::Finished, StopReason::ShutdownRequested).await;
    assert_eq!(
        settled,
        ScopeState::Stopped {
            reason: StopReason::ShutdownRequested
        }
    );
    assert_eq!(
        states,
        vec![
            ScopeState::Stopped {
                reason: StopReason::Finished
            },
            ScopeState::Stopped {
                reason: StopReason::ShutdownRequested
            },
        ],
        "an upgrade corrects the stream instead of ending it on a stale verdict"
    );
}

#[crate::runtime::test]
async fn never_started_outranks_an_earlier_shutdown_requested() {
    let (settled, states) =
        resolve_competing_stops(StopReason::ShutdownRequested, StopReason::NeverStarted).await;
    assert_eq!(
        settled,
        ScopeState::Stopped {
            reason: StopReason::NeverStarted
        },
        "the scope state must agree with a never-started membership exit"
    );
    assert_eq!(
        states,
        vec![
            ScopeState::Stopped {
                reason: StopReason::ShutdownRequested
            },
            ScopeState::Stopped {
                reason: StopReason::NeverStarted
            },
        ]
    );
}

#[crate::runtime::test]
async fn never_started_is_retained_against_a_later_shutdown_requested() {
    let (settled, states) =
        resolve_competing_stops(StopReason::NeverStarted, StopReason::ShutdownRequested).await;
    assert_eq!(
        settled,
        ScopeState::Stopped {
            reason: StopReason::NeverStarted
        },
        "the lattice resolves this pair the same way in either arrival order"
    );
    assert_eq!(
        states,
        vec![ScopeState::Stopped {
            reason: StopReason::NeverStarted
        }],
        "a weaker repeat emits nothing"
    );
}

#[crate::runtime::test]
async fn an_exact_duplicate_stop_publishes_once() {
    let (settled, states) =
        resolve_competing_stops(StopReason::ShutdownRequested, StopReason::ShutdownRequested).await;
    assert_eq!(
        settled,
        ScopeState::Stopped {
            reason: StopReason::ShutdownRequested
        }
    );
    assert_eq!(
        states,
        vec![ScopeState::Stopped {
            reason: StopReason::ShutdownRequested
        }],
        "equal verdicts are idempotent repeats, not upgrades"
    );
}

impl Wake for ObserveScopeOnStartupWake {
    fn wake(self: Arc<Self>) {
        self.observe();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.observe();
    }
}

#[test]
fn stopped_publication_keeps_mailbox_panic_primary_and_finishes_observation() {
    let scope = isolated_scope("root", ScopeFlavor::Ordered);
    let epoch = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("test scope epoch is available");
    let handle = ScopeRef {
        cell: Arc::clone(&scope),
    };
    let mut events = handle.subscribe_lifecycle();
    let mailbox = MailboxCell::new(scope.member.id().clone(), crate::runtime::mailbox_runtime());
    let actor: ActorRef<u8> = actor_ref_from_parts(Arc::clone(&scope.member), Arc::clone(&mailbox));
    scope.member.attach_mailbox(mailbox);

    let mailbox_payload_dropped = Arc::new(AtomicUsize::new(0));
    let mailbox_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
        &mailbox_payload_dropped,
    ))));
    let mut parked_send = Box::pin(actor.send(1));
    assert!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(&mailbox_waker))
            .is_pending()
    );

    let terminal_payload_dropped = Arc::new(AtomicUsize::new(0));
    let terminal_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
        &terminal_payload_dropped,
    ))));
    let mut terminal = Box::pin(scope.member.wait_terminal());
    assert!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(&terminal_waker))
            .is_pending()
    );

    let payload = catch_unwind(AssertUnwindSafe(|| {
        scope.finish_root_incarnation(epoch, StopReason::ShutdownRequested, Exit::never_started());
    }))
    .expect_err("the primary mailbox panic still surfaces");
    assert_eq!(
        mailbox_payload_dropped.load(Ordering::SeqCst),
        0,
        "the mailbox panic payload remains owned by the caller"
    );
    assert!(
        terminal_payload_dropped.load(Ordering::SeqCst) > 0,
        "later member-pulse panics are contained as cleanup"
    );
    assert!(matches!(
        scope.record().state,
        ScopeState::Stopped {
            reason: StopReason::ShutdownRequested
        }
    ));
    assert!(scope.settled(Some(epoch)));
    assert!(matches!(
        events.try_recv(),
        Ok(LifecycleItem::Event(crate::LifecycleEvent {
            kind: LifecycleEventKind::ScopeState {
                state: ScopeState::Stopped {
                    reason: StopReason::ShutdownRequested
                }
            },
            ..
        }))
    ));
    assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Closed));
    assert!(matches!(
        scope.member.record().stage,
        MemberStage::Terminal(ref exit) if matches!(exit.kind(), ExitKind::NeverStarted)
    ));
    drop(payload);
    assert_eq!(mailbox_payload_dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn panicking_mailbox_waker_cannot_skip_the_terminal_pulse() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let mailbox = MailboxCell::new(member.id().clone(), crate::runtime::mailbox_runtime());
    let actor = actor_ref_from_parts(Arc::clone(&member), Arc::clone(&mailbox));
    member.attach_mailbox(mailbox);

    let mut parked_send = Box::pin(actor.send(1));
    let panicking_waker = Waker::from(Arc::new(PanicWake("injected mailbox waker panic")));
    assert!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(&panicking_waker))
            .is_pending()
    );

    let wakes = Arc::new(AtomicUsize::new(0));
    let terminal_waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
    let mut terminal = Box::pin(member.wait_terminal());
    assert!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(&terminal_waker))
            .is_pending()
    );

    catch_unwind(AssertUnwindSafe(|| {
        member.terminalize(Exit::never_started(), StartupDisposition::Unchanged);
    }))
    .expect_err("the hostile mailbox waker still surfaces its panic");
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        1,
        "membership terminality is pulsed before the mailbox panic resumes"
    );
    assert!(matches!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
    ));
    assert!(matches!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(error)) if error.kind == SendErrorKind::Terminated
    ));
}

#[test]
fn mailbox_teardown_panic_precedes_a_terminal_pulse_panic() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let mailbox = MailboxCell::new(member.id().clone(), crate::runtime::mailbox_runtime());
    let actor = actor_ref_from_parts(Arc::clone(&member), Arc::clone(&mailbox));
    member.attach_mailbox(mailbox);

    let mut parked_send = Box::pin(actor.send(1));
    let mailbox_payload_dropped = Arc::new(AtomicUsize::new(0));
    let mailbox_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
        &mailbox_payload_dropped,
    ))));
    assert!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(&mailbox_waker))
            .is_pending()
    );

    let mut terminal = Box::pin(member.wait_terminal());
    let pulse_payload_dropped = Arc::new(AtomicUsize::new(0));
    let terminal_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
        &pulse_payload_dropped,
    ))));
    assert!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(&terminal_waker))
            .is_pending()
    );

    let payload = catch_unwind(AssertUnwindSafe(|| {
        member.terminalize(Exit::never_started(), StartupDisposition::Unchanged);
    }))
    .expect_err("the primary mailbox panic still surfaces");
    assert_eq!(
        mailbox_payload_dropped.load(Ordering::SeqCst),
        0,
        "the primary mailbox payload is retained for the caller"
    );
    assert_eq!(
        pulse_payload_dropped.load(Ordering::SeqCst),
        1,
        "the membership-pulse panic is cleanup and is contained"
    );
    drop(payload);
    assert_eq!(mailbox_payload_dropped.load(Ordering::SeqCst), 1);
    assert!(matches!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
    ));
    assert!(matches!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(error)) if error.kind == SendErrorKind::Terminated
    ));
}

#[crate::runtime::test]
async fn mailbox_waker_panic_is_contained_without_wedging_system_completion() {
    let exit = Latch::default();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "worker",
            RawOnceDef::new(ExitOnSignalRaw { exit: exit.clone() })
                .mailbox(Mailbox::queue(1).expect("non-zero capacity")),
        )
        .expect("valid actor");
    let plan = tree.lower_for_test();
    let member = Arc::clone(&plan.children[0].slot.member);
    let root = Arc::clone(&plan.root);
    let mut events = root.subscribe_lifecycle();
    let mut system = super::super::spawn_system(plan);
    root.wait_started().await.expect("actor starts");
    actor
        .try_send(1)
        .expect("the first message fills the queue");

    let mut parked_send = Box::pin(actor.send(2));
    let panicking_waker = Waker::from(Arc::new(PanicWake("injected mailbox waker panic")));
    assert!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(&panicking_waker))
            .is_pending()
    );

    let waiter_started = Latch::default();
    let terminal_waiter = crate::runtime::spawn({
        let member = Arc::clone(&member);
        let waiter_started = waiter_started.clone();
        async move {
            waiter_started.fire();
            member.wait_terminal().await
        }
    });
    waiter_started.fired().await;
    exit.fire();

    let member_exit =
        match crate::runtime::timeout(DRIVER_PROGRESS_WAIT, crate::runtime::join(terminal_waiter))
            .await
        {
            crate::runtime::Timeout::Completed(crate::runtime::JoinOutcome::Ok { value }) => value,
            crate::runtime::Timeout::Completed(crate::runtime::JoinOutcome::Panic { message }) => {
                panic!("the terminal waiter panicked: {message:?}")
            }
            crate::runtime::Timeout::Completed(crate::runtime::JoinOutcome::Cancelled) => {
                panic!("the terminal waiter was cancelled")
            }
            crate::runtime::Timeout::Elapsed => {
                panic!("the terminal waiter was not pulsed after the mailbox panic")
            }
        };
    assert!(matches!(member_exit.kind(), ExitKind::Completed));

    let reason = match crate::runtime::timeout(DRIVER_PROGRESS_WAIT, system.wait()).await {
        crate::runtime::Timeout::Completed(reason) => reason,
        crate::runtime::Timeout::Elapsed => {
            panic!("the system monitor did not contain the driver unwind")
        }
    };
    assert_eq!(reason, StopReason::ShutdownRequested);
    let root_exit = root.member.wait_terminal().await;
    assert!(matches!(
        root_exit.kind(),
        ExitKind::Panicked { message }
            if message.as_deref() == Some("injected mailbox waker panic")
    ));
    let mut terminal_trace = Vec::new();
    while let Some(item) = events.recv().await {
        let LifecycleItem::Event(event) = item else {
            panic!("the single-child terminal trace cannot lag");
        };
        match event.kind {
            LifecycleEventKind::Exited { .. } => terminal_trace.push("exited"),
            LifecycleEventKind::Removed { .. } => terminal_trace.push("removed"),
            _ => {}
        }
    }
    assert_eq!(
        terminal_trace,
        ["exited", "removed"],
        "mailbox panic resumes only after the terminal event, pruning edge, and stream closure"
    );
}

#[test]
fn terminality_signal_follows_mailbox_termination() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let mailbox = MailboxCell::new(member.id().clone(), crate::runtime::mailbox_runtime());
    let actor = actor_ref_from_parts(Arc::clone(&member), Arc::clone(&mailbox));
    member.attach_mailbox(mailbox);

    let probe = Arc::new(TrySendOnWake {
        actor,
        observed: Mutex::new(None),
    });
    let waker = Waker::from(Arc::clone(&probe));
    let mut context = Context::from_waker(&waker);
    let mut watcher = member.record_watcher();
    let mut changed = Box::pin(watcher.changed());
    assert!(changed.as_mut().poll(&mut context).is_pending());

    member.terminalize(Exit::never_started(), StartupDisposition::Unchanged);

    assert_eq!(
        *probe.observed.lock().expect("observation mutex poisoned"),
        Some(SendErrorKind::Terminated)
    );
    assert!(changed.as_mut().poll(&mut context).is_ready());
}

#[test]
fn supervised_terminality_pulse_follows_mailbox_termination() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    root.admit_child(ResidentProjection::new(Arc::clone(&member), None));
    let mailbox = MailboxCell::new(member.id().clone(), crate::runtime::mailbox_runtime());
    let actor = actor_ref_from_parts(Arc::clone(&member), Arc::clone(&mailbox));
    member.attach_mailbox(mailbox);

    let mailbox_wakes = Arc::new(AtomicUsize::new(0));
    let mailbox_waker = Waker::from(Arc::new(CountWake(Arc::clone(&mailbox_wakes))));
    let mut parked_send = Box::pin(actor.send(1));
    assert!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(&mailbox_waker))
            .is_pending()
    );

    let probe = Arc::new(ObserveWakeCount {
        wakes: Arc::clone(&mailbox_wakes),
        observed: Mutex::new(None),
    });
    let terminal_waker = Waker::from(Arc::clone(&probe));
    let mut terminal = Box::pin(member.wait_terminal());
    assert!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(&terminal_waker))
            .is_pending()
    );

    assert!(root.terminalize_child(
        &member,
        Exit::never_started(),
        None,
        StartupDisposition::Aborted,
    ));

    assert_eq!(
        *probe.observed.lock().expect("observation mutex poisoned"),
        Some(1),
        "the supervised terminal pulse must follow parked-sender discharge"
    );
    assert!(member.record().startup_aborted);
    assert!(matches!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
    ));
    assert!(matches!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(error)) if error.kind == SendErrorKind::Terminated
    ));
}

#[test]
fn supervised_mailbox_teardown_panic_precedes_terminal_pulse_panic() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    root.admit_child(ResidentProjection::new(Arc::clone(&member), None));
    let mailbox = MailboxCell::new(member.id().clone(), crate::runtime::mailbox_runtime());
    let actor = actor_ref_from_parts(Arc::clone(&member), Arc::clone(&mailbox));
    member.attach_mailbox(mailbox);

    let mailbox_payload_dropped = Arc::new(AtomicUsize::new(0));
    let mailbox_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
        &mailbox_payload_dropped,
    ))));
    let mut parked_send = Box::pin(actor.send(1));
    assert!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(&mailbox_waker))
            .is_pending()
    );

    let terminal_payload_dropped = Arc::new(AtomicUsize::new(0));
    let terminal_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
        &terminal_payload_dropped,
    ))));
    let mut terminal = Box::pin(member.wait_terminal());
    assert!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(&terminal_waker))
            .is_pending()
    );

    let payload = catch_unwind(AssertUnwindSafe(|| {
        root.terminalize_child(
            &member,
            Exit::never_started(),
            None,
            StartupDisposition::Aborted,
        );
    }))
    .expect_err("the primary mailbox panic still surfaces");
    assert_eq!(
        mailbox_payload_dropped.load(Ordering::SeqCst),
        0,
        "the primary mailbox payload is retained for the caller"
    );
    assert_eq!(
        terminal_payload_dropped.load(Ordering::SeqCst),
        1,
        "the later terminal-pulse panic is contained as cleanup"
    );
    drop(payload);
    assert_eq!(mailbox_payload_dropped.load(Ordering::SeqCst), 1);
    assert!(matches!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
    ));
    assert!(matches!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(error)) if error.kind == SendErrorKind::Terminated
    ));
}

#[test]
fn mailbox_wake_observes_terminal_record_and_reentrant_terminality_is_idempotent() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let mailbox = MailboxCell::new(member.id().clone(), crate::runtime::mailbox_runtime());
    let actor = actor_ref_from_parts(Arc::clone(&member), Arc::clone(&mailbox));
    member.attach_mailbox(mailbox);
    let first_exit = Exit::never_started();
    let probe = Arc::new(ObserveMemberOnMailboxWake {
        member: Arc::clone(&member),
        competing_exit: Exit::new(ExitKind::Completed, Cancellation::NotObserved),
        observed: Mutex::new(None),
    });
    let waker = Waker::from(Arc::clone(&probe));
    let mut parked = Box::pin(actor.send(1));
    assert!(
        parked
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    member.terminalize(first_exit.clone(), StartupDisposition::Unchanged);

    assert_eq!(
        *probe.observed.lock().expect("observation mutex poisoned"),
        Some((
            MemberStage::Terminal(first_exit.clone()),
            MemberStage::Terminal(first_exit.clone())
        ))
    );
    assert!(matches!(
        member.record().stage,
        MemberStage::Terminal(exit) if exit == first_exit
    ));
}

#[test]
fn attach_during_terminal_publication_finishes_record_before_mailbox_wake() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let mailbox = MailboxCell::new(member.id().clone(), crate::runtime::mailbox_runtime());
    let actor = actor_ref_from_parts(Arc::clone(&member), Arc::clone(&mailbox));
    let first_exit = Exit::never_started();
    member.stage_terminal_before_mailbox(first_exit.clone());
    let probe = Arc::new(ObserveMemberOnMailboxWake {
        member: Arc::clone(&member),
        competing_exit: Exit::new(ExitKind::Completed, Cancellation::NotObserved),
        observed: Mutex::new(None),
    });
    let waker = Waker::from(Arc::clone(&probe));
    let mut parked = Box::pin(actor.send(1));
    assert!(
        parked
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    member.attach_mailbox(mailbox);

    assert_eq!(
        *probe.observed.lock().expect("observation mutex poisoned"),
        Some((
            MemberStage::Terminal(first_exit.clone()),
            MemberStage::Terminal(first_exit.clone())
        ))
    );
    assert!(matches!(
        member.record().stage,
        MemberStage::Terminal(exit) if exit == first_exit
    ));
}

#[test]
fn concurrent_terminalizers_return_after_one_consistent_record_is_visible() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let start = Arc::new(Barrier::new(3));
    let workers = [
        Exit::never_started(),
        Exit::new(ExitKind::Completed, Cancellation::NotObserved),
    ]
    .into_iter()
    .map(|exit| {
        let member = Arc::clone(&member);
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            start.wait();
            member.terminalize(exit, StartupDisposition::Unchanged);
            assert!(matches!(member.record().stage, MemberStage::Terminal(_)));
        })
    })
    .collect::<Vec<_>>();
    start.wait();
    for worker in workers {
        worker.join().expect("terminalizer thread succeeds");
    }

    let record = member.record();
    let MemberStage::Terminal(exit) = record.stage else {
        panic!("one terminal record must be visible");
    };
    assert_eq!(record.last_exit, Some(exit));
}

#[test]
fn terminal_startup_wake_follows_member_and_incarnation_publication() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("root");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let scope = ScopeCell::new(member, ScopeFlavor::Ordered, ScopeIdentity::new());
    let epoch = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("test scope epoch is available");
    let probe = Arc::new(ObserveScopeOnStartupWake {
        scope: Arc::clone(&scope),
        epoch: Some(epoch),
        observed: Mutex::new(None),
    });
    let waker = Waker::from(Arc::clone(&probe));
    let mut watcher = scope.record_watcher();
    let mut changed = Box::pin(watcher.changed());
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    scope.finish_root_incarnation(epoch, StopReason::ShutdownRequested, Exit::never_started());

    let observed = probe
        .observed
        .lock()
        .expect("observation mutex poisoned")
        .clone()
        .expect("terminal startup wakes the scope watcher");
    assert!(matches!(observed.0, MemberStage::Terminal(_)));
    assert_eq!(observed.1, Some(true));
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
}

#[test]
fn no_live_root_startup_wake_follows_member_publication() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("root");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let scope = ScopeCell::new(member, ScopeFlavor::Ordered, ScopeIdentity::new());
    let probe = Arc::new(ObserveScopeOnStartupWake {
        scope: Arc::clone(&scope),
        epoch: None,
        observed: Mutex::new(None),
    });
    let waker = Waker::from(Arc::clone(&probe));
    let mut watcher = scope.record_watcher();
    let mut changed = Box::pin(watcher.changed());
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    scope.finish_live_root_incarnation(StopReason::ShutdownRequested, Exit::never_started());

    let observed = probe
        .observed
        .lock()
        .expect("observation mutex poisoned")
        .clone()
        .expect("terminal startup wakes the scope watcher");
    assert!(matches!(observed.0, MemberStage::Terminal(_)));
    assert_eq!(observed.1, None);
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
}
