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

    let entries = root.with_observation_gate(|txn| control.close(&root, txn));
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

#[test]
fn dynamic_close_evicts_a_terminal_reservation_before_readd() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_state(ScopeState::Running);
    let child_id = ChildId::from("worker");
    let (events, _receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(events);
    root.set_dynamic_route(Some(control.clone()));

    let first = root
        .with_observation_gate(|txn| {
            super::super::admission_control::reserve_dynamic_in(&root, child_id.clone(), None, txn)
        })
        .expect("the first incarnation reserves the child");
    let first_membership = first.slot.member.membership();
    let retained = root.with_observation_gate(|txn| control.close(&root, txn));
    assert!(
        retained.is_empty(),
        "reservations are terminalized on close"
    );

    let (restart_events, _restart_receiver) = crate::runtime::unbounded_mpsc();
    let restart_control = DynamicControl::new(restart_events);
    root.set_dynamic_route(Some(restart_control));
    let replacement = root
        .with_observation_gate(|txn| {
            super::super::admission_control::reserve_dynamic_in(&root, child_id, None, txn)
        })
        .expect("the restarted scope can reserve the id again");
    let replacement_membership = replacement.slot.member.membership();
    assert!(!first_membership.supersedes(replacement_membership));
    assert!(!replacement_membership.supersedes(first_membership));
    cancel_dynamic_reservation(
        &replacement.scope,
        replacement.control.as_ref(),
        &replacement.slot,
    );
}

#[test]
fn removing_a_reservation_evicts_its_identity_before_readd() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_state(ScopeState::Running);
    let child_id = ChildId::from("worker");
    let (events, _receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(events);
    root.set_dynamic_route(Some(control));

    let first = root
        .with_observation_gate(|txn| {
            super::super::admission_control::reserve_dynamic_in(&root, child_id.clone(), None, txn)
        })
        .expect("the first reservation succeeds");
    let first_membership = first.slot.member.membership();
    let mut removal = super::super::remove_dynamic(&root, &child_id, Some(first_membership));
    assert_eq!(removal.try_receive(), Some(RemoveOutcome::Removed));

    let replacement = root
        .with_observation_gate(|txn| {
            super::super::admission_control::reserve_dynamic_in(&root, child_id, None, txn)
        })
        .expect("the removed reservation releases the id");
    let replacement_membership = replacement.slot.member.membership();
    assert!(!first_membership.supersedes(replacement_membership));
    assert!(!replacement_membership.supersedes(first_membership));
    cancel_dynamic_reservation(
        &replacement.scope,
        replacement.control.as_ref(),
        &replacement.slot,
    );
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
fn dynamic_removal_waits_for_the_observation_gate_before_mutating_state() {
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

    // Capturing the gate attempt proves the remover has reached its commit
    // boundary. No retained control-plane state may change until that one
    // observation transaction begins.
    assert_eq!(
        captures
            .recv_timeout(CAPTURE_PROBE_WAIT)
            .expect("removal reports its gate capture within the bound"),
        GateCapture::Observation
    );
    let state = control
        .state
        .try_lock()
        .expect("a removal waiting on observation has not acquired or mutated dynamic state");
    let entry = state
        .entries
        .get(&child_id)
        .expect("the removal keeps its resident registration");
    assert!(!entry.is_removing());
    assert!(entry.matches_key(key));
    drop(state);

    drop(held_gate);
    let response = worker.join().expect("removal transition completes");
    drop(response);

    let route = root
        .dynamic_route()
        .expect("the fixture exposes its dynamic route");
    assert!(matches!(
        root.with_observation_gate(|txn| route.reserve(&root, child_id.clone(), None, txn)),
        Err(crate::ReserveError::RemovalInProgress(id)) if id == child_id
    ));
}

#[crate::runtime::test]
async fn final_removal_holds_the_id_until_removed_publication_commits() {
    let (mut scope, _events, _dynamic_events, _disposal_events, control) =
        running_dynamic_fixture();
    let root = Arc::clone(&scope.root);
    let reservation = super::super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("running dynamic scope reserves the child");
    let member = Arc::clone(&reservation.slot.member);
    reservation
        .slot
        .define(ChildConstruction::Task(TaskDef::new(|_| future::pending())));
    let (definition, resolved) = reservation
        .slot
        .resolve_and_take_defined(&scope.defaults)
        .expect("the slot is defined");
    let plan =
        crate::plan::ChildPlan::with_options(Arc::clone(&reservation.slot), definition, resolved);
    let mut child = ChildRuntime::from_plan(plan, &root);
    child.initial = false;
    let key = root.with_observation_gate(|txn| {
        let key = match scope.insert_child(child) {
            Ok(key) => key,
            Err(_) => panic!("the empty arena accepts its first child"),
        };
        control
            .state
            .lock()
            .expect("dynamic-state mutex poisoned")
            .entries
            .get_mut(member.id())
            .expect("the reservation remains registered")
            .promote(key, None, txn);
        root.admit_child_locked(resident_projection(&reservation.slot), txn);
        key
    });
    assert!(scope.terminalize_child(
        key,
        Exit::never_started(),
        None,
        StartupDisposition::NotAborted,
    ));
    let mut removal = super::super::remove_dynamic(&root, member.id(), Some(member.membership()));
    assert!(root.snapshot().child("worker").is_some());

    let captures = root.probe_gate_captures();
    let gate = root.observation_gate();
    let held_gate = gate.lock();
    std::thread::scope(|threads| {
        let finalizer = threads.spawn(|| scope.finalize_removal(key));
        assert_eq!(
            captures
                .recv_timeout(CAPTURE_PROBE_WAIT)
                .expect("finalization reaches its commit gate within the bound"),
            GateCapture::Observation
        );
        assert!(
            control
                .state
                .lock()
                .expect("dynamic-state mutex remains available")
                .entries
                .contains_key(member.id()),
            "the old id remains claimed until residency withdrawal can commit"
        );
        drop(held_gate);
        finalizer.join().expect("finalization completes");
    });

    assert!(root.snapshot().child("worker").is_none());
    assert!(
        !control
            .state
            .lock()
            .expect("dynamic-state mutex remains healthy")
            .entries
            .contains_key(member.id()),
        "id release follows the Removed publication"
    );
    assert_eq!(removal.try_receive(), Some(RemoveOutcome::Removed));

    let replacement = super::super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("the id is reusable after the Removed commit");
    cancel_dynamic_reservation(
        &replacement.scope,
        replacement.control.as_ref(),
        &replacement.slot,
    );
}

#[test]
fn draining_cannot_publish_across_an_inflight_reservation_transaction() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_state(ScopeState::Running);
    let (events, _receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(events);
    root.set_dynamic_route(Some(control.clone()));

    let state = control.state.lock().expect("dynamic-state mutex poisoned");
    let (entered, observed) = std::sync::mpsc::channel();
    std::thread::scope(|threads| {
        let reserve_root = Arc::clone(&root);
        let reserver = threads.spawn(move || {
            reserve_root.with_observation_gate(|txn| {
                entered
                    .send(())
                    .expect("the test waits for the reservation transaction");
                super::super::admission_control::reserve_dynamic_in(
                    &reserve_root,
                    ChildId::from("worker"),
                    None,
                    txn,
                )
            })
        });
        observed
            .recv_timeout(CAPTURE_PROBE_WAIT)
            .expect("reservation acquires the observation gate");

        let captures = root.probe_gate_captures();
        let drain_root = Arc::clone(&root);
        let drainer = threads.spawn(move || drain_root.set_state(ScopeState::Draining));
        assert_eq!(
            captures
                .recv_timeout(CAPTURE_PROBE_WAIT)
                .expect("draining reaches the occupied gate within the bound"),
            GateCapture::Observation
        );
        assert_eq!(
            root.record().state,
            ScopeState::Running,
            "draining cannot publish after the reservation has read the live phase"
        );

        drop(state);
        let reservation = reserver
            .join()
            .expect("reservation transaction does not panic")
            .expect("the earlier transaction reserves before draining");
        drainer
            .join()
            .expect("draining completes after reservation");
        assert_eq!(root.record().state, ScopeState::Draining);
        cancel_dynamic_reservation(
            &reservation.scope,
            reservation.control.as_ref(),
            &reservation.slot,
        );
    });
}

#[crate::runtime::test]
async fn removal_from_a_foreign_thread_reaches_the_driver() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let child_id = ChildId::from("worker");
    let membership = root
        .child_identity
        .lock()
        .expect("scope identity mutex poisoned")
        .mint_membership(&child_id)
        .expect("membership available");
    let member = MemberCell::new(child_id.clone(), membership);
    let membership = member.membership();
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
    let foreign_root = Arc::clone(&root);
    std::thread::spawn(move || {
        assert!(
            !crate::runtime::is_available(),
            "Tokio context is not inherited by a foreign thread"
        );
        super::super::signal_fused_cancel(
            &foreign_root,
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
            let _incarnations = member.lock_incarnation_counter();
            panic!("inject admission conversion failure");
        }))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| scope.handle_admission(request))).is_err(),
        "the poisoned issued counter injects the conversion panic"
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

// Gate-free never-started terminalization (an `Admission` drop's annul) and
// gated supervised terminalization (`terminalize_child`) are mutually
// excluded per member by the reserved→resident transition: the annul decides
// against `is_reserved` under the dynamic-state mutex, and promotion happens
// under that same mutex before the member is admitted to residency. The three
// tests below pin both serialization orders and the racing window between
// them, so a competing terminalizer can never publish an exit that diverges
// from the one the lifecycle stream carries.

#[crate::runtime::test]
async fn annulment_before_admission_owns_never_started_terminality() {
    let (mut scope, _event_receiver, mut dynamic_event_receiver, _disposal_event_receiver, control) =
        running_dynamic_fixture();
    let root = Arc::clone(&scope.root);
    let mut lifecycle = root.subscribe_lifecycle();
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
    let Some(DriverEvent::Admission(request)) = dynamic_event_receiver.recv().await else {
        panic!("admission enqueueing submits the request")
    };

    cancel_dynamic_reservation(
        &reservation.scope,
        reservation.control.as_ref(),
        &reservation.slot,
    );
    let annulled_stage = member.record().stage;
    assert!(matches!(
        &annulled_stage,
        MemberStage::Terminal(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
    ));

    scope.handle_admission(request);

    assert!(matches!(
        response.receive().await,
        Some(Err(ReserveError::NotAdmitting(
            crate::NotAdmittingCause::ReservationEnded
        )))
    ));
    assert!(
        scope.children.is_empty(),
        "an annulled reservation is never admitted"
    );
    assert!(
        control
            .state
            .lock()
            .expect("dynamic-state mutex remains healthy")
            .entries
            .is_empty()
    );
    assert_eq!(
        member.record().stage,
        annulled_stage,
        "the driver's rejection cannot displace the annul's terminal exit"
    );
    assert!(root.snapshot().child("worker").is_none());
    while let Ok(LifecycleItem::Event(event)) = lifecycle.try_recv() {
        assert!(
            !matches!(
                event.kind,
                LifecycleEventKind::Added { .. } | LifecycleEventKind::Exited { .. }
            ),
            "a never-resident member has no supervised lifecycle edges"
        );
    }
}

#[crate::runtime::test]
async fn annulment_after_promotion_is_inert_and_supervision_owns_the_exit() {
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
    let mut response = super::super::start_admission(
        Arc::clone(&reservation.control),
        Arc::clone(&reservation.slot),
        None,
    )
    .expect("admission starts inside the runtime");
    let Some(DriverEvent::Admission(request)) = dynamic_event_receiver.recv().await else {
        panic!("admission enqueueing submits the request")
    };
    scope.handle_admission(request);
    assert!(matches!(response.try_receive(), Some(Ok(()))));
    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(2), started.fired()).await,
        crate::runtime::Timeout::Completed(())
    ));

    // The promoted entry is no longer reserved, so a late annul (a dropped
    // `Admission` future racing its own completion) must not terminalize the
    // now-supervised member.
    cancel_dynamic_reservation(
        &reservation.scope,
        reservation.control.as_ref(),
        &reservation.slot,
    );
    assert!(
        !matches!(member.record().stage, MemberStage::Terminal(_)),
        "a late annul cannot compete with supervised terminalization"
    );
    assert!(
        control
            .state
            .lock()
            .expect("dynamic-state mutex remains healthy")
            .entries
            .get(member.id())
            .is_some_and(|entry| !entry.is_reserved()),
        "the resident registration survives the inert annul"
    );

    let removal_response =
        super::super::remove_dynamic(&root, member.id(), Some(member.membership()));
    let removal = match crate::runtime::timeout(
        Duration::from_secs(2),
        dynamic_event_receiver.recv(),
    )
    .await
    {
        crate::runtime::Timeout::Completed(Some(DriverEvent::Removal(removal))) => removal,
        crate::runtime::Timeout::Completed(_) => panic!("the removal reaches the driver"),
        crate::runtime::Timeout::Elapsed => panic!("the removal must reach the driver"),
    };
    scope.handle_removal(removal);
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
    let disposal =
        match crate::runtime::timeout(Duration::from_secs(2), disposal_event_receiver.recv()).await
        {
            crate::runtime::Timeout::Completed(Some(event)) => event,
            crate::runtime::Timeout::Completed(None) => panic!("the disposal lane remains open"),
            crate::runtime::Timeout::Elapsed => panic!("retained construction disposal completes"),
        };
    let DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic }) = disposal else {
        panic!("the stop path reports retained construction disposal")
    };
    scope.handle_construction_disposed(child, panic);
    assert_eq!(
        removal_response.receive().await,
        Some(RemoveOutcome::Removed)
    );

    // The adjudicated consistency property: the terminal record and the
    // lifecycle stream publish the same exit, because supervision was the
    // only terminalizer.
    let MemberStage::Terminal(record_exit) = member.record().stage else {
        panic!("the removed member publishes a terminal record");
    };
    let mut emitted_exit = None;
    while let Ok(LifecycleItem::Event(event)) = lifecycle.try_recv() {
        if let LifecycleEventKind::Exited { exit, .. } = event.kind {
            assert!(
                emitted_exit.replace(exit).is_none(),
                "exactly one supervised exit is emitted"
            );
        }
    }
    assert_eq!(
        emitted_exit.as_ref(),
        Some(&record_exit),
        "the lifecycle stream and the terminal record agree on the exit"
    );
}

#[crate::runtime::test]
async fn annulment_racing_admission_resolves_to_one_terminalization_owner() {
    for _ in 0..64 {
        let (
            mut scope,
            _event_receiver,
            mut dynamic_event_receiver,
            _disposal_event_receiver,
            control,
        ) = running_dynamic_fixture();
        let root = Arc::clone(&scope.root);
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
        let Some(DriverEvent::Admission(request)) = dynamic_event_receiver.recv().await else {
            panic!("admission enqueueing submits the request")
        };

        let barrier = Arc::new(Barrier::new(2));
        let annul = {
            let barrier = Arc::clone(&barrier);
            let annul_control = Arc::clone(&reservation.control);
            let annul_slot = Arc::clone(&reservation.slot);
            let annul_scope = Arc::clone(&reservation.scope);
            std::thread::spawn(move || {
                barrier.wait();
                cancel_dynamic_reservation(&annul_scope, annul_control.as_ref(), &annul_slot);
            })
        };
        barrier.wait();
        scope.handle_admission(request);
        annul.join().expect("the annul thread completes");

        // Whichever side won the dynamic-state mutex, exactly one owner
        // published terminality (or none, if the admission won): the record,
        // the arena, and the control-plane entry always agree.
        match response.receive().await {
            Some(Ok(())) => {
                assert!(
                    !matches!(member.record().stage, MemberStage::Terminal(_)),
                    "an admitted member is live; the losing annul is inert"
                );
                assert_eq!(scope.children.len(), 1);
                assert!(
                    control
                        .state
                        .lock()
                        .expect("dynamic-state mutex remains healthy")
                        .entries
                        .get(member.id())
                        .is_some_and(|entry| !entry.is_reserved())
                );
            }
            Some(Err(ReserveError::NotAdmitting(crate::NotAdmittingCause::ReservationEnded))) => {
                assert!(matches!(
                    member.record().stage,
                    MemberStage::Terminal(exit)
                        if matches!(exit.kind(), ExitKind::NeverStarted)
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
            }
            other => panic!("the race admits exactly two outcomes, got {other:?}"),
        }
    }
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
    super::super::signal_fused_cancel(
        &reservation.scope,
        control.as_ref(),
        &reservation.slot,
        &fused_cancel,
    );
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
        let incarnations = member.lock_incarnation_counter();
        let driver_scope = &mut scope;
        let driver = threads.spawn(move || driver_scope.handle_admission(request));
        // The definition leaves the slot after the pre-conversion latch
        // check and before conversion parks on the held identity mutex, so
        // observing the claim proves the driver passed that first check
        // while the latch was still unfired.
        let deadline = std::time::Instant::now() + CAPTURE_PROBE_WAIT;
        while std::time::Instant::now() < deadline {
            if reservation.slot.resolve_policy(&defaults).is_none() {
                definition_claimed = true;
                break;
            }
            std::thread::yield_now();
        }
        super::super::signal_fused_cancel(
            &reservation.scope,
            control.as_ref(),
            &reservation.slot,
            &fused_cancel,
        );
        // Release without unwinding: conversion must proceed into the
        // under-lock re-check, not observe a poisoned identity mutex.
        drop(incarnations);
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
    super::super::signal_fused_cancel(
        &reservation.scope,
        control.as_ref(),
        &reservation.slot,
        &fused_cancel,
    );
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
async fn fused_only_removal_commits_phase_and_projection_together() {
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

    // A fused drop is the only removal source in this variant. Its dynamic
    // phase and public projection commit under one observation transaction,
    // before the deferred driver request is delivered.
    assert_eq!(member.record().membership_status, MembershipStatus::Active);
    super::super::signal_fused_cancel(
        &reservation.scope,
        control.as_ref(),
        &reservation.slot,
        &fused_cancel,
    );
    assert_eq!(
        member.record().membership_status,
        MembershipStatus::Removing,
        "the fused source commits the Removing projection with its phase"
    );
    assert!(matches!(
        root.snapshot()
            .child("worker")
            .map(|child| child.membership_status),
        Some(MembershipStatus::Removing)
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
        Some(MembershipStatus::Removing)
    ));
    assert_eq!(
        scope.dispatch_membership_status(key),
        MembershipStatus::Removing,
        "exit dispatch and the public projection share the fused-cancel commit"
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
