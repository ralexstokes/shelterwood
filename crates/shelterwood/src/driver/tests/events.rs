use super::support::*;

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
    super::super::signal_fused_cancel(control.as_ref(), &reservation.slot, &fused_cancel);
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

    let forwarded =
        match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv()).await {
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
    const ADMISSIONS: usize = super::super::MIN_EVENT_BATCH_LIMIT * 8 + 1;

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
    let mut shutdown = Box::pin(scope.shutdown_and_wait(Duration::from_secs(2)));
    assert!(
        shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "shutdown waits for both declared children"
    );
    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(1), exiting.wait()).await,
        crate::runtime::Timeout::Completed(_)
    ));
    assert!(
        Pin::new(admissions.last_mut().expect("an admission exists"))
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "the primary exit is handled before the control suffix is drained"
    );

    release_holder.fire();
    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(1), shutdown.as_mut()).await,
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
        let outcome = match crate::runtime::timeout(Duration::from_secs(2), admission).await {
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
