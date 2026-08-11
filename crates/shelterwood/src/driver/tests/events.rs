use super::support::*;

fn disposed(child: ChildKey) -> DriverEvent {
    DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic: None })
}

fn disposed_child(pending: &Pending) -> ChildKey {
    match pending {
        Pending::Child(ChildEvent::ConstructionDisposed { child, .. }) => *child,
        _ => panic!("this fixture only queues construction disposals"),
    }
}

/// Pins the post-blocking-wake gap: removal is queued on the control lane
/// before readiness reaches the primary lane, but the biased wait returns the
/// primary head. Retaining that head and re-entering the common collection
/// site must put both events in one arbitration batch, where removal wins.
#[crate::runtime::test]
async fn blocking_primary_wake_recollects_control_removal_before_arbitration() {
    let identity = isolated_scope("identity", ScopeFlavor::Dynamic);
    let membership = identity.member.membership();
    let key = ChildKey(1);
    let incarnation = IncarnationCounter::fixture(membership)
        .mint()
        .expect("fixture incarnation is available");
    let (primary, mut primary_receiver) = crate::runtime::unbounded_mpsc();
    let (control, mut control_receiver) = crate::runtime::unbounded_mpsc();
    let (_disposal, mut disposal_receiver) = crate::runtime::unbounded_mpsc();

    let wait = crate::runtime::wait_scope(
        crate::runtime::ScopeWait {
            signal: future::pending::<()>(),
            parent_shutdown: future::pending::<()>(),
        },
        &mut primary_receiver,
        Some(&mut control_receiver),
        None,
    );
    let publisher = crate::runtime::spawn(async move {
        crate::runtime::yield_now().await;
        control
            .send(DriverEvent::Removal(RemovalRequest { membership, key }))
            .expect("the control lane remains open");
        primary
            .send(DriverEvent::Child(ChildEvent::Ready {
                child: key,
                incarnation,
            }))
            .expect("the primary lane remains open");
    });
    let wake = wait.await;
    assert!(matches!(
        crate::runtime::join(publisher).await,
        crate::runtime::JoinOutcome::Ok { value: () }
    ));
    let crate::runtime::ScopeWake::Message(Some(event)) = wake else {
        panic!("the biased blocking wait returns the later primary head");
    };

    let mut pending = Vec::new();
    super::super::retain_woken_event(event, &mut pending);
    assert!(!super::super::collect_event_lanes(
        super::super::EventLanes {
            primary: &mut primary_receiver,
            control: Some(&mut control_receiver),
            disposal: &mut disposal_receiver,
        },
        super::super::MIN_EVENT_BATCH_LIMIT,
        &mut pending,
    ));
    arbitrate(&mut pending);

    assert_eq!(pending.len(), 2);
    assert!(matches!(pending[0].1, Pending::Removal(_)));
    assert!(matches!(
        pending[1].1,
        Pending::Child(ChildEvent::Ready { .. })
    ));
}

/// Pins the wiring the driver's per-wake collection depends on: the disposal
/// lane is collected last, through the same cap as the other two, and its
/// saturation reaches the caller so the loop yields a scheduler turn instead
/// of returning to a lane that still has a queued suffix. Without the cap a
/// disposal flood monopolizes the wake, deferring the shutdown check at the
/// top of the loop — and with it the start of the shutdown timeout.
#[test]
fn every_event_lane_is_capped_and_a_saturated_lane_forces_a_yield() {
    let limit = super::super::MIN_EVENT_BATCH_LIMIT;
    let primary_key = ChildKey(1);
    let control_key = ChildKey(2);
    let disposal_key = ChildKey(3);
    let (primary, mut primary_receiver) = crate::runtime::unbounded_mpsc();
    let (control, mut control_receiver) = crate::runtime::unbounded_mpsc();
    let (disposal, mut disposal_receiver) = crate::runtime::unbounded_mpsc();
    primary
        .send(disposed(primary_key))
        .expect("the primary lane remains open");
    control
        .send(disposed(control_key))
        .expect("the control lane remains open");
    for _ in 0..limit * 2 {
        disposal
            .send(disposed(disposal_key))
            .expect("the disposal lane remains open");
    }

    let mut pending = Vec::new();
    let saturated = super::super::collect_event_lanes(
        super::super::EventLanes {
            primary: &mut primary_receiver,
            control: Some(&mut control_receiver),
            disposal: &mut disposal_receiver,
        },
        limit,
        &mut pending,
    );

    assert!(
        saturated,
        "a saturated disposal lane reports through to the driver's yield"
    );
    assert_eq!(
        pending.len(),
        limit + 3,
        "the disposal lane contributes one bounded batch plus its saturation probe"
    );
    assert_eq!(
        disposed_child(&pending[0].1),
        primary_key,
        "child lifecycle events lead the wake"
    );
    assert_eq!(
        disposed_child(&pending[1].1),
        control_key,
        "dynamic control traffic collects after the primary lane"
    );
    assert!(
        pending[2..]
            .iter()
            .all(|entry| disposed_child(&entry.1) == disposal_key),
        "disposal completions trail both lifecycle lanes so they stay batch-tail events"
    );
    assert!(
        crate::runtime::unbounded_mpsc_try_recv(&mut disposal_receiver).is_some(),
        "the disposal suffix remains for a later scheduler turn"
    );
}

/// Exercises the `DeadlineKind::Restart` suppression gate on its own:
/// the restart deadline is scheduled first (no stop source latched at
/// exit time), then the fused cancellation lands before the deadline's
/// batch runs. The gate must clear the stale backoff edge without
/// invoking user construction.
#[crate::runtime::test]
async fn restart_deadline_gate_suppresses_a_fused_cancel_landing_after_scheduling() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let epoch = root
        .begin_incarnation(ScopeState::Starting)
        .expect("test scope epoch is available");
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_state(ScopeState::Running);
    root.set_startup(Ok(()));

    let (events, mut event_receiver) = crate::runtime::unbounded_mpsc();
    let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(events.clone());
    root.set_dynamic_route(Some(control.clone()));
    root.set_admitted_children(Vec::new());
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_lifecycle(ScopeLifecycle::running())
        .with_dynamic(Some(control.clone()))
        .build();

    let starts = Arc::new(AtomicUsize::new(0));
    let reservation = super::super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("running dynamic scope reserves the child");
    let member = Arc::clone(&reservation.slot.member);
    reservation
        .slot
        .define(ChildConstruction::Task(TaskDef::new({
            let starts = Arc::clone(&starts);
            move |_| {
                let invocation = starts.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if invocation == 1 {
                        Err(ExitError::message("first incarnation failed"))
                    } else {
                        future::pending().await
                    }
                }
            }
        })));
    let membership = member.membership();
    let fused_cancel = Latch::default();
    let mut response = super::super::start_admission(
        Arc::clone(&reservation.control),
        Arc::clone(&reservation.slot),
        Some(fused_cancel.clone()),
    )
    .expect("the running scope accepts the admission");
    let Some(DriverEvent::Admission(request)) = event_receiver.recv().await else {
        panic!("admission enqueueing submits the request")
    };
    scope.handle_admission(request);
    assert!(matches!(response.try_receive(), Some(Ok(()))));

    let exit = match crate::runtime::timeout(DRIVER_PROGRESS_WAIT, event_receiver.recv()).await {
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
        crate::runtime::Timeout::Completed(_) => panic!("the first incarnation reports exit"),
        crate::runtime::Timeout::Elapsed => panic!("the first incarnation exit must arrive"),
    };
    let key = exit.0;
    scope.handle_exit(exit.0, exit.1, exit.2, exit.3, exit.4, exit.5);
    assert!(
        scope.children[key].restart_deadline.is_some(),
        "a live fused admission does not suppress the restart at exit dispatch"
    );
    assert!(matches!(
        scope.children[key].slot.member.record().stage,
        MemberStage::Restarting
    ));

    // The fused admission handle drops only now: the cancellation latch
    // fires after the backoff was scheduled, and its Removal edge queues
    // behind the already-due restart deadline.
    super::super::signal_fused_cancel(
        &reservation.scope,
        control.as_ref(),
        &reservation.slot,
        &fused_cancel,
    );
    assert!(fused_cancel.is_fired());

    let deadline = scope
        .deadlines
        .pop_due(crate::runtime::now() + Duration::from_secs(60 * 60))
        .expect("the immediate-backoff restart deadline is registered");
    assert!(matches!(
        deadline,
        super::super::DeadlineKind::Restart { .. }
    ));
    scope.handle_deadline(deadline);

    assert!(
        scope.children[key].restart_deadline.is_none(),
        "the gate clears the stale backoff edge"
    );
    assert!(scope.children[key].active.is_none());
    for _ in 0..16 {
        crate::runtime::yield_now().await;
    }
    assert_eq!(
        starts.load(Ordering::SeqCst),
        1,
        "the restart deadline arm rechecks level-triggered stop sources"
    );

    let forwarded = match crate::runtime::timeout(DRIVER_PROGRESS_WAIT, event_receiver.recv()).await
    {
        crate::runtime::Timeout::Completed(Some(event)) => event,
        crate::runtime::Timeout::Completed(None) => panic!("the driver lane remains open"),
        crate::runtime::Timeout::Elapsed => panic!("the fused removal edge is forwarded"),
    };
    let DriverEvent::Removal(removal) = forwarded else {
        panic!("the queued event is the fused removal");
    };
    assert_eq!(removal.membership, membership);
    assert_eq!(removal.key, key);
    scope.handle_removal(removal);
    let Some(DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic })) =
        disposal_event_receiver.recv().await
    else {
        panic!("removal joins retained construction disposal")
    };
    scope.handle_construction_disposed(child, panic);
    assert!(scope.children.get(key).is_none());
}

#[crate::runtime::test]
async fn queued_admissions_yield_to_shutdown_without_forwarder_tasks() {
    // The backlog is sized in whole batches because the ordering asserted
    // below is only observable while it lasts: the driver yields once per
    // batch, and each yield is a window in which the cancelled task can run
    // and publish its exit. One batch would make the assertion a coin flip on
    // whether that task happens to be scheduled in the single window; a
    // starved machine can miss many consecutive windows, so the count buys
    // margin rather than certainty.
    const ADMISSIONS: usize = super::super::MIN_EVENT_BATCH_LIMIT * 64 + 1;

    let release_holder = Latch::default();
    let mut tree = DynamicTree::new();
    let exiting = tree
        .add_task(
            "exiting",
            TaskDef::new(|context| async move {
                context.shutdown_token().cancelled().await;
                Ok(())
            }),
        )
        .expect("exiting task is valid");
    tree.add_task(
        "holder",
        TaskDef::new({
            let release_holder = release_holder.clone();
            move |context| {
                let release_holder = release_holder.clone();
                async move {
                    context.shutdown_token().cancelled().await;
                    release_holder.fired().await;
                    Ok(())
                }
            }
        }),
    )
    .expect("holding task is valid");

    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();
    let baseline = crate::runtime::alive_task_count();
    let mut admissions = Vec::with_capacity(ADMISSIONS);
    for index in 0..ADMISSIONS {
        let mut admission = scope.add_task(
            format!("queued-{index}"),
            TaskDef::new(|_| future::pending()),
        );
        assert!(
            Pin::new(&mut admission)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending(),
            "first poll synchronously queues admission {index}"
        );
        admissions.push(admission);
    }
    assert_eq!(
        crate::runtime::alive_task_count(),
        baseline,
        "synchronous admission enqueueing spawns no transport tasks"
    );

    // Latch shutdown without yielding to the already-woken driver. Its first
    // batch begins draining and rejects one control prefix; the full-batch
    // yield must then let `exiting` run and publish its primary-lane exit.
    let mut shutdown = Box::pin(scope.shutdown_and_wait(DRIVER_PROGRESS_WAIT));
    assert!(
        shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "shutdown waits for both declared children"
    );
    // Step the runtime rather than awaiting a timer for the exit. A timed
    // await hands the driver every turn it wants before this task is
    // rescheduled, and on a loaded machine the whole control suffix can drain
    // inside that one window. Yielding samples once per scheduler turn
    // instead, and the suffix is read in the same synchronous step that sees
    // the exit — no await between the two polls — so the pair is one
    // consistent snapshot rather than two readings of a moving target.
    let mut exit = Box::pin(exiting.wait());
    let give_up = crate::runtime::now() + DRIVER_PROGRESS_WAIT;
    let suffix_pending = loop {
        let exited = exit
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready();
        let suffix_pending = Pin::new(admissions.last_mut().expect("an admission exists"))
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending();
        if exited {
            break suffix_pending;
        }
        // Wall clock appears only as the hang backstop, never as the sampling
        // interval: the loop still samples once per scheduler turn.
        assert!(
            crate::runtime::now() < give_up,
            "the cancelled task publishes its exit"
        );
        crate::runtime::yield_now().await;
    };
    assert!(
        suffix_pending,
        "the primary exit is handled before the control suffix is drained"
    );

    release_holder.fire();
    assert!(matches!(
        crate::runtime::timeout(DRIVER_PROGRESS_WAIT, shutdown.as_mut()).await,
        crate::runtime::Timeout::Completed(Ok(()))
    ));
    drop(shutdown);
    assert_eq!(system.wait().await, StopReason::ShutdownRequested);

    // The driver tore down with a control backlog still queued, so every
    // stranded admission's completion obligation must resolve rather than
    // hang its caller. Pin both ends of the queue: the head is rejected
    // while the drain runs, the tail at latest by the obligation's
    // receiver-drop fallback once the driver's lane closes.
    let tail = admissions.pop().expect("the backlog has a tail");
    let head = admissions.swap_remove(0);
    for (position, admission) in [("head", head), ("tail", tail)] {
        let outcome = match crate::runtime::timeout(DRIVER_PROGRESS_WAIT, admission).await {
            crate::runtime::Timeout::Completed(outcome) => outcome,
            crate::runtime::Timeout::Elapsed => {
                panic!("the {position}-of-queue admission obligation completes at teardown")
            }
        };
        assert!(
            matches!(
                outcome,
                Err(ReserveError::NotAdmitting(
                    crate::NotAdmittingCause::Draining | crate::NotAdmittingCause::Terminal
                ))
            ),
            "the {position}-of-queue admission reports the stopped scope"
        );
    }
}
