use super::support::*;

#[crate::runtime::test(start_paused = true)]
async fn dynamic_high_cycle_add_remove_keeps_only_live_runtime_storage() {
    const CYCLES: usize = 1_000;

    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();
    let cell = Arc::clone(&scope.as_scope().cell);

    for cycle in 0..CYCLES {
        let task = scope
            .add_task(
                "worker",
                TaskDef::new(|_| future::pending())
                    .readiness(Readiness::Manual)
                    .expect("manual readiness is valid")
                    .readiness_deadline(
                        ReadinessDeadline::bounded(Duration::from_secs(60 * 60))
                            .expect("non-zero readiness deadline"),
                    )
                    .shutdown(crate::Shutdown::Abort),
            )
            .await
            .expect("task admission");
        assert_eq!(
            cell.runtime_storage(),
            RuntimeStorage {
                children: 1,
                child_slots: 1,
                deadlines: 1,
                deadline_slots: 1,
            },
            "cycle {cycle} stores only the live child"
        );

        assert_eq!(scope.remove_task(&task).await, RemoveOutcome::Removed);
        assert_eq!(
            cell.runtime_storage(),
            RuntimeStorage {
                children: 0,
                child_slots: 0,
                deadlines: 0,
                deadline_slots: 0,
            },
            "cycle {cycle} must release removed-child storage"
        );

        let automatic = scope
            .add_task(
                "worker",
                TaskDef::new(|_| async { Ok(()) }).retention(Retention::Remove),
            )
            .await
            .expect("auto-removing task admission");
        automatic.wait().await;
        assert_eq!(
            cell.runtime_storage(),
            RuntimeStorage {
                children: 0,
                child_slots: 0,
                deadlines: 0,
                deadline_slots: 0,
            },
            "cycle {cycle} must release Retention::Remove storage"
        );
    }

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("empty dynamic scope shuts down");
}
#[test]
fn dynamic_close_holds_removal_completion_through_observation_cleanup() {
    let mut identity = ScopeIdentity::new();
    let root_id = ChildId::from("root");
    let root_member = MemberCell::new(
        root_id.clone(),
        identity
            .mint_membership(&root_id)
            .expect("root membership available"),
    );
    let root = ScopeCell::new(root_member, ScopeFlavor::Dynamic, ScopeIdentity::new());
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(
        child_id.clone(),
        root.child_identity
            .lock()
            .expect("scope identity mutex poisoned")
            .mint_membership(&child_id)
            .expect("child membership available"),
    );
    let slot = SlotCell::new(Arc::clone(&member), None);
    root.set_admitted_children(vec![resident_projection(&slot)]);
    let (events, _receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(events);
    let (sender, mut response) = crate::runtime::oneshot();
    let mut responses = RemovalResponses::default();
    responses.0.push(sender);
    control
        .state
        .lock()
        .expect("dynamic-state mutex poisoned")
        .entries
        .insert(
            ChildId::from("worker"),
            DynamicEntry::removing(slot, ChildKey(1), responses),
        );

    let entries = control.close();
    assert!(
        response.try_receive().is_none(),
        "closing admission must not complete removal before teardown"
    );
    member.terminalize(Exit::never_started());
    assert!(root.prune_child(&member));
    assert!(
        response.try_receive().is_none(),
        "terminality and Removed precede removal completion"
    );
    drop(entries);
    assert_eq!(response.try_receive(), Some(RemoveOutcome::Removed));
}

#[crate::runtime::test]
async fn retained_unadmitted_slot_does_not_block_driver_teardown() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let slot = scope
        .reserve_task("retained")
        .expect("unadmitted reservation is retained");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("driver teardown completes");

    drop(slot);
}

#[test]
fn reserve_dynamic_rejects_an_empty_id_at_the_driver_boundary() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);

    assert!(matches!(
        super::super::reserve_dynamic(&root, ChildId::from(""), None),
        Err(crate::ReserveError::EmptyId)
    ));
}

#[test]
fn dynamic_removal_releases_state_before_waiting_for_the_observation_gate() {
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
    let slot = SlotCell::new(Arc::clone(&member), None);
    root.set_admitted_children(vec![resident_projection(&slot)]);
    let (events, _receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(events);
    let key = ChildKey(1);
    control
        .state
        .lock()
        .expect("dynamic-state mutex poisoned")
        .entries
        .insert(child_id.clone(), DynamicEntry::resident(slot, key, None));
    root.set_dynamic_route(Some(control.clone()));

    let captures = root.probe_gate_captures();
    let gate = root.observation_gate();
    let held_gate = gate.lock();
    let removal_root = Arc::clone(&root);
    let removal_id = child_id.clone();
    let worker =
        std::thread::spawn(move || super::super::remove_dynamic(&removal_root, &removal_id, None));

    // The capture report proves removal committed to the held observation
    // gate for its removing transition. Dynamic state cannot change again
    // until that gate is released, so a single acquisition attempt decides
    // whether removal reached the gate while still holding the state.
    assert_eq!(
        captures
            .recv_timeout(CAPTURE_PROBE_WAIT)
            .expect("removal reports its gate capture within the bound"),
        GateCapture::Observation
    );
    let state = control
        .state
        .try_lock()
        .expect("a removal waiting on observation must release dynamic state");
    let entry = state
        .entries
        .get(&child_id)
        .expect("the removal keeps its resident registration");
    assert!(entry.is_removing());
    assert!(entry.matches_key(key));
    drop(state);

    let route = root
        .dynamic_route()
        .expect("the fixture exposes its dynamic route");
    assert!(matches!(
        route.reserve(&root, child_id.clone(), None),
        Err(crate::ReserveError::RemovalInProgress(id)) if id == child_id
    ));

    drop(held_gate);
    let response = worker.join().expect("removal transition completes");
    drop(response);
}

#[crate::runtime::test]
async fn removal_from_a_foreign_thread_reaches_the_driver() {
    let mut identity = ScopeIdentity::new();
    let child_id = ChildId::from("worker");
    let membership = identity
        .mint_membership(&child_id)
        .expect("membership available");
    let member = MemberCell::new(child_id.clone(), membership);
    let slot = SlotCell::new(Arc::clone(&member), None);
    let key = ChildKey(1);
    let (events, mut event_receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(events);
    control
        .state
        .lock()
        .expect("dynamic-state mutex poisoned")
        .entries
        .insert(
            child_id,
            DynamicEntry::resident(Arc::clone(&slot), key, Some(Latch::default())),
        );
    let foreign_control = Arc::clone(&control);
    let foreign_slot = Arc::clone(&slot);
    std::thread::spawn(move || {
        assert!(
            !crate::runtime::is_available(),
            "Tokio context is not inherited by a foreign thread"
        );
        super::super::signal_fused_cancel(
            foreign_control.as_ref(),
            &foreign_slot,
            &Latch::default(),
        );
    })
    .join()
    .expect("foreign-thread removal signaling succeeds");

    let event = match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv()).await {
        crate::runtime::Timeout::Completed(event) => event.expect("the driver lane remains open"),
        crate::runtime::Timeout::Elapsed => {
            panic!("the off-runtime removal edge must not be lost")
        }
    };
    let DriverEvent::Removal(observed) = event else {
        panic!("the queued event is the requested removal");
    };
    assert_eq!(observed.membership, membership);
    assert_eq!(observed.key, key);
}

#[crate::runtime::test]
async fn admission_conversion_panic_does_not_poison_dynamic_cleanup() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let epoch = root
        .begin_incarnation(ScopeState::Starting)
        .expect("test scope epoch is available");
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_state(ScopeState::Running);
    root.set_startup(Ok(()));

    let (events, mut event_receiver) = crate::runtime::unbounded_mpsc();
    let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(events.clone());
    root.set_dynamic_route(Some(control.clone()));
    root.set_admitted_children(Vec::new());
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events, disposal_events)
        .with_lifecycle(ScopeLifecycle::running())
        .with_dynamic(Some(control.clone()))
        .build();

    let reservation = super::super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("running dynamic scope reserves the child");
    let member = Arc::clone(&reservation.slot.member);
    reservation
        .slot
        .define(ChildConstruction::Task(TaskDef::new(|_| future::pending())));
    let response = super::super::start_admission(
        Arc::clone(&reservation.control),
        Arc::clone(&reservation.slot),
        None,
    )
    .expect("admission starts inside the runtime");
    let Some(DriverEvent::Admission(request)) = event_receiver.recv().await else {
        panic!("admission enqueueing submits the request")
    };

    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _identity = root
                .child_identity
                .lock()
                .expect("scope identity mutex starts healthy");
            panic!("inject admission conversion failure");
        }))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| scope.handle_admission(request))).is_err(),
        "the poisoned child identity injects the conversion panic"
    );
    assert!(matches!(
        response.receive().await,
        Some(Err(ReserveError::NotAdmitting(
            crate::NotAdmittingCause::Terminal
        )))
    ));
    assert!(matches!(member.record().stage, MemberStage::Terminal(_)));

    let cleanup = catch_unwind(AssertUnwindSafe(|| drop(scope)));
    assert!(
        cleanup.is_ok(),
        "conversion failure must not poison dynamic cleanup"
    );
    assert!(
        control
            .state
            .lock()
            .expect("dynamic-state mutex remains healthy")
            .entries
            .is_empty(),
        "cleanup discharges the stranded reservation"
    );
}

#[crate::runtime::test]
async fn fused_cancellation_overtaking_admission_rejects_before_conversion() {
    let (mut scope, _event_receiver, mut dynamic_event_receiver, _disposal_event_receiver, control) =
        running_dynamic_fixture();
    let root = Arc::clone(&scope.root);
    let reservation = super::super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("running dynamic scope reserves the child");
    let member = Arc::clone(&reservation.slot.member);
    reservation
        .slot
        .define(ChildConstruction::Task(TaskDef::new(|_| future::pending())));
    let fused_cancel = Latch::default();
    let response = super::super::start_admission(
        Arc::clone(&reservation.control),
        Arc::clone(&reservation.slot),
        Some(fused_cancel.clone()),
    )
    .expect("admission starts inside the runtime");
    let Some(DriverEvent::Admission(request)) = dynamic_event_receiver.recv().await else {
        panic!("admission enqueueing submits the request")
    };

    // Fused cancellation linearizes after Admission is queued but before
    // the driver handles it. A reserved entry has no arena key, so
    // `signal_fused_cancel` cannot queue a Removal for this membership;
    // the fired latch is the authoritative rejection evidence until the
    // queued Admission reaches the driver.
    super::super::signal_fused_cancel(control.as_ref(), &reservation.slot, &fused_cancel);
    assert!(fused_cancel.is_fired());
    assert!(
        crate::runtime::unbounded_mpsc_try_recv(&mut dynamic_event_receiver).is_none(),
        "a reserved membership cannot emit a key-addressed Removal"
    );

    // If handle_admission loses its first latch re-check, conversion now
    // reaches this poisoned identity mutex and unwinds before the later
    // under-lock check. The real guard rejects without touching it.
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _identity = root
                .child_identity
                .lock()
                .expect("scope identity mutex starts healthy");
            panic!("poison conversion after the overtaking fused cancellation");
        }))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| scope.handle_admission(request))).is_ok(),
        "overtaking fused cancellation rejects before fallible child conversion"
    );
    assert!(matches!(
        response.receive().await,
        Some(Err(ReserveError::NotAdmitting(
            crate::NotAdmittingCause::ReservationEnded
        )))
    ));
    assert!(scope.children.is_empty());
    assert!(
        control
            .state
            .lock()
            .expect("dynamic-state mutex remains healthy")
            .entries
            .is_empty()
    );
    assert!(matches!(member.record().stage, MemberStage::Terminal(_)));
}

#[crate::runtime::test]
async fn fused_cancellation_during_conversion_is_rejected_by_the_under_lock_recheck() {
    let (mut scope, _event_receiver, mut dynamic_event_receiver, _disposal_event_receiver, control) =
        running_dynamic_fixture();
    let root = Arc::clone(&scope.root);
    let reservation = super::super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("running dynamic scope reserves the child");
    let member = Arc::clone(&reservation.slot.member);
    reservation
        .slot
        .define(ChildConstruction::Task(TaskDef::new(|_| future::pending())));
    let fused_cancel = Latch::default();
    let response = super::super::start_admission(
        Arc::clone(&reservation.control),
        Arc::clone(&reservation.slot),
        Some(fused_cancel.clone()),
    )
    .expect("admission starts inside the runtime");
    let Some(DriverEvent::Admission(request)) = dynamic_event_receiver.recv().await else {
        panic!("admission enqueueing submits the request")
    };

    // Fused cancellation fires while the driver is already inside child
    // conversion: past the pre-conversion latch check, parked on the child
    // identity mutex held below. Only the re-check under the control-plane
    // lock can observe this firing, so this interleaving pins that disjunct
    // specifically. The entry stays Reserved throughout — a fired latch
    // cannot mark a keyless reservation Removing — which keeps the
    // stale-reservation disjunct of the same re-check false.
    let defaults = ResolvedDefaults::default();
    let mut definition_claimed = false;
    std::thread::scope(|threads| {
        let identity = root
            .child_identity
            .lock()
            .expect("scope identity mutex starts healthy");
        let driver_scope = &mut scope;
        let driver = threads.spawn(move || driver_scope.handle_admission(request));
        // The definition leaves the slot after the pre-conversion latch
        // check and before conversion parks on the held identity mutex, so
        // observing the claim proves the driver passed that first check
        // while the latch was still unfired.
        let deadline = std::time::Instant::now() + CAPTURE_PROBE_WAIT;
        while std::time::Instant::now() < deadline {
            if matches!(reservation.slot.resolve_policy(&defaults), Ok(None)) {
                definition_claimed = true;
                break;
            }
            std::thread::yield_now();
        }
        super::super::signal_fused_cancel(control.as_ref(), &reservation.slot, &fused_cancel);
        // Release without unwinding: conversion must proceed into the
        // under-lock re-check, not observe a poisoned identity mutex.
        drop(identity);
        driver
            .join()
            .expect("conversion proceeds after the identity mutex is released");
    });
    assert!(
        definition_claimed,
        "the parked admission claims its definition within the bound"
    );

    assert!(matches!(
        response.receive().await,
        Some(Err(ReserveError::NotAdmitting(
            crate::NotAdmittingCause::ReservationEnded
        )))
    ));
    assert!(
        scope.children.is_empty(),
        "the under-lock rejection admits no resident"
    );
    assert!(
        crate::runtime::unbounded_mpsc_try_recv(&mut dynamic_event_receiver).is_none(),
        "a reservation-time cancellation emits no key-addressed Removal"
    );
    assert!(
        control
            .state
            .lock()
            .expect("dynamic-state mutex remains healthy")
            .entries
            .is_empty()
    );
    assert!(matches!(member.record().stage, MemberStage::Terminal(_)));
}

#[derive(Clone, Copy)]
enum DuplicateRemovalDelivery {
    WhileActive,
    DuringDisposal,
}

async fn exercise_double_removal(delivery: DuplicateRemovalDelivery) {
    let (
        mut scope,
        mut event_receiver,
        mut dynamic_event_receiver,
        mut disposal_event_receiver,
        control,
    ) = running_dynamic_fixture();
    let root = Arc::clone(&scope.root);
    let mut lifecycle = root.subscribe_lifecycle();
    let started = Latch::default();
    let reservation = super::super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("running dynamic scope reserves the child");
    let member = Arc::clone(&reservation.slot.member);
    let membership = member.membership();
    reservation
        .slot
        .define(ChildConstruction::Task(TaskDef::new({
            let started = started.clone();
            move |context| {
                let started = started.clone();
                async move {
                    started.fire();
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        })));
    let fused_cancel = Latch::default();
    let mut admission_response = super::super::start_admission(
        Arc::clone(&reservation.control),
        Arc::clone(&reservation.slot),
        Some(fused_cancel.clone()),
    )
    .expect("admission starts inside the runtime");
    let Some(DriverEvent::Admission(request)) = dynamic_event_receiver.recv().await else {
        panic!("admission enqueueing submits the request")
    };
    scope.handle_admission(request);
    assert!(matches!(admission_response.try_receive(), Some(Ok(()))));
    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(2), started.fired()).await,
        crate::runtime::Timeout::Completed(())
    ));

    let key = control
        .state
        .lock()
        .expect("dynamic-state mutex poisoned")
        .entries
        .get(member.id())
        .and_then(DynamicEntry::key)
        .expect("the admission installs its child key");
    super::super::signal_fused_cancel(control.as_ref(), &reservation.slot, &fused_cancel);
    let mut removal_response =
        super::super::remove_dynamic(&root, member.id(), Some(member.membership()));

    let mut removals = Vec::new();
    while removals.len() < 2 {
        let event =
            match crate::runtime::timeout(Duration::from_secs(2), dynamic_event_receiver.recv())
                .await
            {
                crate::runtime::Timeout::Completed(Some(event)) => event,
                crate::runtime::Timeout::Completed(None) => {
                    panic!("the dynamic-control lane remains open")
                }
                crate::runtime::Timeout::Elapsed => {
                    panic!("both removal sources reach the driver")
                }
            };
        let DriverEvent::Removal(removal) = event else {
            panic!("only removal requests reach the dynamic-control lane here")
        };
        removals.push(removal);
    }
    assert!(
        removals
            .iter()
            .all(|removal| { removal.membership == membership && removal.key == key })
    );

    scope.handle_removal(removals.remove(0));
    let active = scope.children[key]
        .active
        .as_ref()
        .expect("the first removal begins the live stop ladder");
    let ladder = active.ladder.expect("the stop ladder is armed");
    let stop_deadline = active.stop_deadline;
    let deadline_count = scope.deadlines.len();

    if matches!(delivery, DuplicateRemovalDelivery::WhileActive) {
        scope.handle_removal(removals.remove(0));
        let active = scope.children[key]
            .active
            .as_ref()
            .expect("the duplicate leaves the incarnation active");
        assert_eq!(active.ladder, Some(ladder));
        assert_eq!(active.stop_deadline, stop_deadline);
        assert_eq!(scope.deadlines.len(), deadline_count);
    }

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
        crate::runtime::Timeout::Completed(_) => panic!("the stopped child reports exit"),
        crate::runtime::Timeout::Elapsed => panic!("the stopped child exit must arrive"),
    };
    scope.handle_exit(exit.0, exit.1, exit.2, exit.3, exit.4, exit.5);
    assert!(scope.children[key].pending_terminal.is_some());

    if matches!(delivery, DuplicateRemovalDelivery::DuringDisposal) {
        // Defense-in-depth, pinned jointly rather than individually: the
        // duplicate is dropped by `begin_stop_child`'s terminal/disposing
        // guard, and even without that guard `begin_terminal_disposal`'s
        // pending-terminal guard returns before disturbing disposal. Either
        // check alone suffices to protect this observable, so this test
        // fails only when both are removed together.
        scope.handle_removal(removals.remove(0));
        assert!(
            scope
                .children
                .get(key)
                .is_some_and(|child| child.pending_terminal.is_some()),
            "the duplicate cannot bypass retained-construction disposal"
        );
    }
    assert!(removals.is_empty());
    assert_eq!(
        removal_response.try_receive(),
        None,
        "removal completion waits for terminality, disposal, and pruning"
    );

    let disposal =
        match crate::runtime::timeout(Duration::from_secs(2), disposal_event_receiver.recv()).await
        {
            crate::runtime::Timeout::Completed(Some(event)) => event,
            crate::runtime::Timeout::Completed(None) => {
                panic!("the disposal lane remains open")
            }
            crate::runtime::Timeout::Elapsed => {
                panic!("retained construction disposal completes")
            }
        };
    let DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic }) = disposal else {
        panic!("the stop path reports retained construction disposal")
    };
    assert_eq!(child, key);
    scope.handle_construction_disposed(child, panic);

    assert!(scope.children.get(key).is_none());
    assert!(root.snapshot().child("worker").is_none());
    assert!(
        !control
            .state
            .lock()
            .expect("dynamic-state mutex poisoned")
            .entries
            .contains_key(member.id())
    );
    assert_eq!(
        removal_response.receive().await,
        Some(RemoveOutcome::Removed)
    );
    let mut added = 0;
    let mut removed = 0;
    while let Ok(LifecycleItem::Event(event)) = lifecycle.try_recv() {
        match event.kind {
            LifecycleEventKind::Added { .. } => added += 1,
            LifecycleEventKind::Removed { .. } => removed += 1,
            _ => {}
        }
    }
    assert_eq!((added, removed), (1, 1));
}

#[crate::runtime::test]
async fn double_removal_is_idempotent_while_the_child_is_active() {
    exercise_double_removal(DuplicateRemovalDelivery::WhileActive).await;
}

#[crate::runtime::test]
async fn double_removal_is_idempotent_during_construction_disposal() {
    exercise_double_removal(DuplicateRemovalDelivery::DuringDisposal).await;
}

#[crate::runtime::test]
async fn fused_only_removal_publishes_removing_from_the_driver() {
    let (
        mut scope,
        mut event_receiver,
        mut dynamic_event_receiver,
        mut disposal_event_receiver,
        control,
    ) = running_dynamic_fixture();
    let root = Arc::clone(&scope.root);
    let mut lifecycle = root.subscribe_lifecycle();
    let started = Latch::default();
    let reservation = super::super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("running dynamic scope reserves the child");
    let member = Arc::clone(&reservation.slot.member);
    let membership = member.membership();
    reservation
        .slot
        .define(ChildConstruction::Task(TaskDef::new({
            let started = started.clone();
            move |context| {
                let started = started.clone();
                async move {
                    started.fire();
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        })));
    let fused_cancel = Latch::default();
    let mut admission_response = super::super::start_admission(
        Arc::clone(&reservation.control),
        Arc::clone(&reservation.slot),
        Some(fused_cancel.clone()),
    )
    .expect("admission starts inside the runtime");
    let Some(DriverEvent::Admission(request)) = dynamic_event_receiver.recv().await else {
        panic!("admission enqueueing submits the request")
    };
    scope.handle_admission(request);
    assert!(matches!(admission_response.try_receive(), Some(Ok(()))));
    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(2), started.fired()).await,
        crate::runtime::Timeout::Completed(())
    ));
    let key = control
        .state
        .lock()
        .expect("dynamic-state mutex poisoned")
        .entries
        .get(member.id())
        .and_then(DynamicEntry::key)
        .expect("the admission installs its child key");

    // A fused drop is the only removal source in this variant. Unlike an
    // explicit `remove`, `signal_fused_cancel` marks the control-plane entry
    // Removing and queues the Removal without touching the member record,
    // so the driver-side `publish_dynamic_removal` call in `handle_removal`
    // is the only writer of the public Removing projection on this path.
    // The projection asserts after `handle_removal` pin that call; the
    // explicit-remove variants cannot, because `remove_dynamic_impl`
    // publishes the projection before its request reaches the driver.
    assert_eq!(member.record().membership_status, MembershipStatus::Active);
    super::super::signal_fused_cancel(control.as_ref(), &reservation.slot, &fused_cancel);
    assert_eq!(
        member.record().membership_status,
        MembershipStatus::Active,
        "the fused source leaves the Removing projection to the driver"
    );
    assert!(matches!(
        root.snapshot()
            .child("worker")
            .map(|child| child.membership_status),
        Some(MembershipStatus::Active)
    ));
    let removal = match crate::runtime::timeout(
        Duration::from_secs(2),
        dynamic_event_receiver.recv(),
    )
    .await
    {
        crate::runtime::Timeout::Completed(Some(DriverEvent::Removal(removal))) => removal,
        crate::runtime::Timeout::Completed(_) => {
            panic!("the fused source queues exactly one removal")
        }
        crate::runtime::Timeout::Elapsed => panic!("the fused removal reaches the driver"),
    };
    assert_eq!(removal.membership, membership);
    assert_eq!(removal.key, key);

    scope.handle_removal(removal);
    assert_eq!(
        member.record().membership_status,
        MembershipStatus::Removing,
        "handle_removal publishes the Removing projection"
    );
    assert!(matches!(
        root.snapshot()
            .child("worker")
            .map(|child| child.membership_status),
        Some(MembershipStatus::Removing)
    ));

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
        crate::runtime::Timeout::Completed(_) => panic!("the stopped child reports exit"),
        crate::runtime::Timeout::Elapsed => panic!("the stopped child exit must arrive"),
    };
    scope.handle_exit(exit.0, exit.1, exit.2, exit.3, exit.4, exit.5);
    assert!(scope.children[key].pending_terminal.is_some());

    let disposal =
        match crate::runtime::timeout(Duration::from_secs(2), disposal_event_receiver.recv()).await
        {
            crate::runtime::Timeout::Completed(Some(event)) => event,
            crate::runtime::Timeout::Completed(None) => {
                panic!("the disposal lane remains open")
            }
            crate::runtime::Timeout::Elapsed => {
                panic!("retained construction disposal completes")
            }
        };
    let DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic }) = disposal else {
        panic!("the stop path reports retained construction disposal")
    };
    assert_eq!(child, key);
    scope.handle_construction_disposed(child, panic);

    assert!(scope.children.get(key).is_none());
    assert!(root.snapshot().child("worker").is_none());
    assert!(
        !control
            .state
            .lock()
            .expect("dynamic-state mutex poisoned")
            .entries
            .contains_key(member.id())
    );
    let mut added = 0;
    let mut removed = 0;
    while let Ok(LifecycleItem::Event(event)) = lifecycle.try_recv() {
        match event.kind {
            LifecycleEventKind::Added { .. } => added += 1,
            LifecycleEventKind::Removed { .. } => removed += 1,
            _ => {}
        }
    }
    assert_eq!((added, removed), (1, 1));
}

pub(crate) async fn exercise_queued_fused_drop_before_exit_dispatch<A>(
    make_admission: impl FnOnce(super::super::DynamicReservation) -> A,
) where
    A: Future,
{
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

    let release_failure = Latch::default();
    let starts = Arc::new(AtomicUsize::new(0));
    let reservation = super::super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("running dynamic scope reserves the child");
    let member = Arc::clone(&reservation.slot.member);
    reservation
        .slot
        .define(ChildConstruction::Task(TaskDef::new({
            let release_failure = release_failure.clone();
            let starts = Arc::clone(&starts);
            move |_| {
                let release_failure = release_failure.clone();
                let invocation = starts.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if invocation == 1 {
                        release_failure.fired().await;
                        Err(ExitError::message("first incarnation failed"))
                    } else {
                        future::pending().await
                    }
                }
            }
        })));
    let membership = member.membership();
    let mut admission = Box::pin(make_admission(reservation));
    assert!(
        admission
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "first poll submits the fused admission"
    );
    let Some(DriverEvent::Admission(request)) = event_receiver.recv().await else {
        panic!("admission enqueueing submits the request")
    };
    scope.handle_admission(request);

    for _ in 0..64 {
        if starts.load(Ordering::SeqCst) == 1 {
            break;
        }
        crate::runtime::yield_now().await;
    }
    assert_eq!(starts.load(Ordering::SeqCst), 1);

    release_failure.fire();
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
    assert!(
        crate::runtime::unbounded_mpsc_send(
            &scope.events,
            DriverEvent::Removal(RemovalRequest {
                membership: root.member.membership(),
                key: ChildKey(u64::MAX - 1),
            }),
        )
        .is_ok(),
        "the open lane queues a predecessor edge ahead of the fused removal"
    );
    drop(admission);
    assert!(
        control
            .state
            .lock()
            .expect("dynamic-state mutex poisoned")
            .entries
            .get(member.id())
            .is_some_and(|entry| entry.is_removing() && entry.matches_key(key)),
        "fused drop marks the indexed membership removing before its queued edge advances"
    );
    assert!(matches!(
        root.snapshot()
            .child("worker")
            .map(|child| child.membership_status),
        Some(MembershipStatus::Active)
    ));
    assert_eq!(
        scope.dispatch_membership_status(key),
        MembershipStatus::Removing,
        "exit dispatch follows the fused-cancel control plane before the public projection"
    );

    scope.handle_exit(exit.0, exit.1, exit.2, exit.3, exit.4, exit.5);
    assert!(scope.children[key].restart_deadline.is_none());
    assert_eq!(
        root.snapshot().total_restarts,
        crate::TotalRestarts::ZERO,
        "cancellation that linearized before exit incurs no restart charge"
    );
    for _ in 0..16 {
        crate::runtime::yield_now().await;
    }
    assert_eq!(
        starts.load(Ordering::SeqCst),
        1,
        "exit dispatch consults fused cancellation before restart construction"
    );

    assert!(matches!(
        event_receiver.recv().await,
        Some(DriverEvent::Removal(queued))
            if queued.membership == root.member.membership()
    ));
    let forwarded =
        match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv()).await {
            crate::runtime::Timeout::Completed(Some(event)) => event,
            crate::runtime::Timeout::Completed(None) => panic!("the driver lane remains open"),
            crate::runtime::Timeout::Elapsed => panic!("the fused removal edge is forwarded"),
        };
    let DriverEvent::Removal(removal) = forwarded else {
        panic!("the queued event is the fused removal")
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
