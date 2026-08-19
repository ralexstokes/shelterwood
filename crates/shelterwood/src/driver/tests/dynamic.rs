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
        // `deadline_slots` is exact, not a band. The removal below pins the
        // queue empty, and arming one readiness deadline pushes exactly one
        // heap entry, so two slots here means the arm leaked a second,
        // never-registered entry — the leak this cycle exists to catch, and
        // one that `deadlines..=deadlines * 2` would admit unnoticed.
        assert_eq!(
            cell.runtime_storage(),
            RuntimeStorage {
                children: 1,
                child_slots: 1,
                deadlines: 1,
                deadline_slots: 1,
            },
            "cycle {cycle} stores exactly the live child and readiness deadline"
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

/// The reducer can emit an initial/admission `StartChild` before a concurrent
/// caller commits removal.  Construction must re-sample the synchronous latch
/// when that effect is executed: the forwarded `Removal` event is deliberately
/// left queued here, so arbitration cannot be what suppresses the start.
#[crate::runtime::test]
async fn latched_removal_suppresses_a_queued_start_effect() {
    let (mut scope, _events, mut dynamic_events, control) = running_dynamic_fixture();
    let root = Arc::clone(&scope.root);
    let starts = Arc::new(AtomicUsize::new(0));
    let (reservation, key) = insert_dynamic_fixture(
        &mut scope,
        &control,
        "worker",
        ChildConstruction::Task(TaskDef::new({
            let starts = Arc::clone(&starts);
            move |_| {
                starts.fetch_add(1, Ordering::SeqCst);
                future::pending()
            }
        })),
        |_| {},
        DynamicFixtureState::Resident,
    );

    let _removal = super::super::remove_dynamic(
        &root,
        reservation.slot.member.id(),
        Some(reservation.slot.member.membership()),
    );
    assert_eq!(
        reservation.slot.member.record().membership_status,
        MembershipStatus::Removing
    );

    // Model execution of a reducer effect that was emitted before removal
    // won.  The removal event is still in the control lane at this point.
    scope.spawn_child(key);
    for _ in 0..8 {
        crate::runtime::yield_now().await;
    }
    assert!(scope.children[key].active.is_none());
    assert_eq!(
        scope.supervisor.membership_status(key),
        MembershipStatus::Removing
    );
    assert_eq!(
        starts.load(Ordering::SeqCst),
        0,
        "a latch that wins before effect execution suppresses user construction"
    );
    assert!(matches!(
        crate::runtime::unbounded_mpsc_try_recv(&mut dynamic_events),
        Some(DriverEvent::Removal(RemovalRequest { key: queued })) if queued == key
    ));
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
        root.mint_membership(&child_id)
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
            DynamicEntry::removing(slot, ChildKey::fixture(1), responses),
        );

    let entries = root.with_observation_gate(|txn| control.close(&root, txn));
    assert!(
        response.try_receive().is_none(),
        "closing admission must not complete removal before teardown"
    );
    member.terminalize(Exit::never_started(), StartupDisposition::Unchanged);
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
    assert!(matches!(
        first.slot.member.record().stage,
        MemberStage::Terminal(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
    ));
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
        root.mint_membership(&child_id)
            .expect("child membership available"),
    );
    let slot = SlotCell::new(Arc::clone(&member), None);
    root.set_admitted_children(vec![resident_projection(&slot)]);
    let (events, _receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(events);
    let key = ChildKey::fixture(1);
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
    let (mut scope, _events, _dynamic_events, control) = running_dynamic_fixture();
    let root = Arc::clone(&scope.root);
    let (reservation, key) = insert_dynamic_fixture(
        &mut scope,
        &control,
        "worker",
        ChildConstruction::Task(TaskDef::new(|_| future::pending())),
        |_| {},
        DynamicFixtureState::Resident,
    );
    let member = Arc::clone(&reservation.slot.member);
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

#[crate::runtime::test]
async fn removal_tolerates_synchronous_reclaim_from_the_stop_funnel() {
    let (mut scope, _events, _dynamic_events, control) = running_dynamic_fixture();
    let root = Arc::clone(&scope.root);
    let (_reservation, key) = insert_dynamic_fixture(
        &mut scope,
        &control,
        "worker",
        ChildConstruction::Task(TaskDef::new(|_| future::pending())),
        |child| {
            // Model the latent restart-gap shape: there is no active
            // incarnation and no retained construction, so a hard-force stop
            // can terminalize and reclaim this Removing member synchronously.
            drop(child.construction.take());
        },
        DynamicFixtureState::Removing,
    );
    scope.supervisor.set_hard_forced_for_test(true);

    scope.handle_removal(RemovalRequest { key });

    assert!(
        scope.children.get(key).is_none(),
        "the stop funnel may reclaim the member before handle_removal returns"
    );
    assert!(root.snapshot().child("worker").is_none());
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
        .mint_membership(&child_id)
        .expect("membership available");
    let member = MemberCell::new(child_id.clone(), membership);
    let slot = SlotCell::new(Arc::clone(&member), None);
    let key = ChildKey::fixture(1);
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

    let observed = recv_removal(
        &mut event_receiver,
        DRIVER_PROGRESS_WAIT,
        "the off-runtime removal edge",
    )
    .await;
    assert_eq!(observed.key, key);
}

#[crate::runtime::test]
async fn admission_conversion_panic_does_not_poison_dynamic_cleanup() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let epoch = ScopeEpochGuard::begin(&root).expect("test scope epoch is available");
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_state(ScopeState::Running);
    root.set_startup(Ok(()));

    let (events, mut event_receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(events.clone());
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events)
        .with_lifecycle(ScopeLifecycle::running())
        .with_dynamic(Some(control.clone()))
        .build();

    let reservation = super::super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("running dynamic scope reserves the child");
    let member = Arc::clone(&reservation.slot.member);
    reservation
        .slot
        .define(ChildConstruction::Task(TaskDef::new(|_| future::pending())));
    let (response, request) = begin_admission(&reservation, &mut event_receiver, None).await;

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
    let (mut scope, _event_receiver, mut dynamic_event_receiver, control) =
        running_dynamic_fixture();
    let root = Arc::clone(&scope.root);
    let mut lifecycle = root.subscribe_lifecycle();
    let reservation = super::super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("running dynamic scope reserves the child");
    let member = Arc::clone(&reservation.slot.member);
    reservation
        .slot
        .define(ChildConstruction::Task(TaskDef::new(|_| future::pending())));
    let (response, request) =
        begin_admission(&reservation, &mut dynamic_event_receiver, None).await;

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
    let (mut scope, mut event_receiver, mut dynamic_event_receiver, control) =
        running_dynamic_fixture();
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
    let (mut response, request) =
        begin_admission(&reservation, &mut dynamic_event_receiver, None).await;
    scope.handle_admission(request);
    assert!(matches!(response.try_receive(), Some(Ok(()))));
    assert!(matches!(
        crate::runtime::timeout(DRIVER_PROGRESS_WAIT, started.fired()).await,
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
    let removal = recv_removal(
        &mut dynamic_event_receiver,
        DRIVER_PROGRESS_WAIT,
        "the removal reaches the driver",
    )
    .await;
    scope.handle_removal(removal);
    recv_child_exit(
        &mut event_receiver,
        DRIVER_PROGRESS_WAIT,
        "the stopped child exit",
    )
    .await
    .dispatch(&mut scope);
    let (child, panic) = recv_construction_disposed(
        &mut scope.disposal_event_receiver,
        DRIVER_PROGRESS_WAIT,
        "retained construction disposal completes",
    )
    .await;
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
        let (mut scope, _event_receiver, mut dynamic_event_receiver, control) =
            running_dynamic_fixture();
        let root = Arc::clone(&scope.root);
        let reservation = super::super::reserve_dynamic(&root, ChildId::from("worker"), None)
            .expect("running dynamic scope reserves the child");
        let member = Arc::clone(&reservation.slot.member);
        reservation
            .slot
            .define(ChildConstruction::Task(TaskDef::new(|_| future::pending())));
        let (response, request) =
            begin_admission(&reservation, &mut dynamic_event_receiver, None).await;

        let (contender_ready, contenders_ready) = std::sync::mpsc::sync_channel(2);
        let state_control = Arc::clone(&control);
        let state = state_control
            .state
            .lock()
            .expect("dynamic-state mutex remains healthy");
        let annul_ready = contender_ready.clone();
        let annul_control = Arc::clone(&reservation.control);
        let annul_slot = Arc::clone(&reservation.slot);
        let annul_scope = Arc::clone(&reservation.scope);
        let annul = std::thread::spawn(move || {
            annul_ready
                .send(())
                .expect("the race coordinator remains available");
            cancel_dynamic_reservation(&annul_scope, annul_control.as_ref(), &annul_slot);
        });
        let (admission_runtime, admission) = DedicatedRuntime::spawn(async move {
            contender_ready
                .send(())
                .expect("the race coordinator remains available");
            scope.handle_admission(request);
            scope
        });
        // Both contenders are running before the dynamic-state mutex is
        // released, so neither can finish ahead of the other: the guard,
        // not the rendezvous, is what forces them to overlap. The
        // rendezvous only proves each contender reached its send — the
        // channel is shared, so neither receive names a contender.
        contenders_ready
            .recv()
            .expect("a contender starts before the mutex is released");
        contenders_ready
            .recv()
            .expect("both contenders start before the mutex is released");
        drop(state);
        annul.join().expect("the annul contender completes");
        let scope = match crate::runtime::join(admission).await {
            crate::runtime::JoinOutcome::Ok { value } => value,
            crate::runtime::JoinOutcome::Panic { message } => {
                panic!("the admission contender panicked: {message:?}")
            }
            crate::runtime::JoinOutcome::Cancelled => {
                panic!("the admission contender was cancelled")
            }
        };
        admission_runtime.shutdown().await;

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
    let (mut scope, _event_receiver, mut dynamic_event_receiver, control) =
        running_dynamic_fixture();
    let root = Arc::clone(&scope.root);
    let reservation = super::super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("running dynamic scope reserves the child");
    let member = Arc::clone(&reservation.slot.member);
    reservation
        .slot
        .define(ChildConstruction::Task(TaskDef::new(|_| future::pending())));
    let fused_cancel = Latch::default();
    let (response, request) = begin_admission(
        &reservation,
        &mut dynamic_event_receiver,
        Some(fused_cancel.clone()),
    )
    .await;

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

    // Conversion issues the membership's incarnation counter to the child
    // runtime and never returns it, so an unclaimed counter afterwards is
    // evidence that the first latch re-check rejected before conversion ran.
    // That is what the sibling under-lock test parks on to reach the later
    // disjunct.
    assert!(
        catch_unwind(AssertUnwindSafe(|| scope.handle_admission(request))).is_ok(),
        "overtaking fused cancellation rejects before fallible child conversion"
    );
    assert!(
        member.lock_incarnation_counter().is_some(),
        "the pre-conversion latch check rejects before the counter is issued"
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
    let (mut scope, _event_receiver, mut dynamic_event_receiver, control) =
        running_dynamic_fixture();
    let root = Arc::clone(&scope.root);
    let reservation = super::super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("running dynamic scope reserves the child");
    let member = Arc::clone(&reservation.slot.member);
    reservation
        .slot
        .define(ChildConstruction::Task(TaskDef::new(|_| future::pending())));
    let fused_cancel = Latch::default();
    let (response, request) = begin_admission(
        &reservation,
        &mut dynamic_event_receiver,
        Some(fused_cancel.clone()),
    )
    .await;

    // Fused cancellation fires while the driver is already inside child
    // conversion: past the pre-conversion latch check, parked on the
    // incarnation-counter mutex held below. Only the re-check under the control-plane
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum RemovalSource {
    FusedAndExplicit,
    FusedOnly,
}

async fn exercise_coalesced_removal(source: RemovalSource) {
    let (mut scope, mut event_receiver, mut dynamic_event_receiver, control) =
        running_dynamic_fixture();
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
    let fused_cancel = Latch::default();
    let (mut admission_response, request) = begin_admission(
        &reservation,
        &mut dynamic_event_receiver,
        Some(fused_cancel.clone()),
    )
    .await;
    scope.handle_admission(request);
    assert!(matches!(admission_response.try_receive(), Some(Ok(()))));
    assert!(matches!(
        crate::runtime::timeout(DRIVER_PROGRESS_WAIT, started.fired()).await,
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

    if source == RemovalSource::FusedOnly {
        assert_eq!(member.record().membership_status, MembershipStatus::Active);
    }
    super::super::signal_fused_cancel(
        &reservation.scope,
        control.as_ref(),
        &reservation.slot,
        &fused_cancel,
    );
    let mut removal_response = (source == RemovalSource::FusedAndExplicit)
        .then(|| super::super::remove_dynamic(&root, member.id(), Some(member.membership())));

    if source == RemovalSource::FusedOnly {
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
    }

    let removal = recv_removal(
        &mut dynamic_event_receiver,
        DRIVER_PROGRESS_WAIT,
        "one removal request reaches the driver",
    )
    .await;
    assert_eq!(removal.key, key);
    if source == RemovalSource::FusedAndExplicit {
        assert!(
            crate::runtime::unbounded_mpsc_try_recv(&mut dynamic_event_receiver).is_none(),
            "the state transition coalesces fused and explicit removal sources"
        );
    }

    let duplicate = removal;
    scope.handle_removal(removal);
    if source == RemovalSource::FusedOnly {
        assert_eq!(
            member.record().membership_status,
            MembershipStatus::Removing,
            "driver handling preserves the already-published Removing projection"
        );
        assert!(matches!(
            root.snapshot()
                .child("worker")
                .map(|child| child.membership_status),
            Some(MembershipStatus::Removing)
        ));

        let active = scope.children[key]
            .active
            .as_ref()
            .expect("the removal begins the live stop ladder");
        let ladder = active.ladder.expect("the stop ladder is armed");
        let stop_deadline = active.stop_deadline;
        let deadline_count = scope.deadlines.len();
        scope.handle_removal(duplicate);
        let active = scope.children[key]
            .active
            .as_ref()
            .expect("the duplicate leaves the incarnation active");
        assert_eq!(active.ladder, Some(ladder));
        assert_eq!(active.stop_deadline, stop_deadline);
        assert_eq!(scope.deadlines.len(), deadline_count);
    }

    recv_child_exit(
        &mut event_receiver,
        DRIVER_PROGRESS_WAIT,
        "the stopped child exit",
    )
    .await
    .dispatch(&mut scope);
    assert!(scope.children[key].pending_terminal.is_some());

    if let Some(response) = &mut removal_response {
        assert_eq!(
            response.try_receive(),
            None,
            "removal completion waits for terminality, disposal, and pruning"
        );
    }

    let (child, panic) = recv_construction_disposed(
        &mut scope.disposal_event_receiver,
        DRIVER_PROGRESS_WAIT,
        "retained construction disposal completes",
    )
    .await;
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
    if let Some(response) = removal_response {
        assert_eq!(response.receive().await, Some(RemoveOutcome::Removed));
    }
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
async fn fused_and_explicit_removal_queue_once_at_transition() {
    exercise_coalesced_removal(RemovalSource::FusedAndExplicit).await;
}

#[crate::runtime::test]
async fn fused_only_removal_commits_phase_and_projection_together() {
    exercise_coalesced_removal(RemovalSource::FusedOnly).await;
}

pub(crate) async fn exercise_queued_fused_drop_before_exit_dispatch<A>(
    make_admission: impl FnOnce(super::super::DynamicReservation) -> A,
) where
    A: Future,
{
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let epoch = ScopeEpochGuard::begin(&root).expect("test scope epoch is available");
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_state(ScopeState::Running);
    root.set_startup(Ok(()));

    let (events, mut event_receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(events.clone());
    let mut scope = ScopeRuntimeBuilder::new(Arc::clone(&root), epoch, events)
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
    let exit = recv_child_exit(
        &mut event_receiver,
        DRIVER_PROGRESS_WAIT,
        "the first incarnation exit",
    )
    .await;
    let key = exit.child;
    assert!(
        crate::runtime::unbounded_mpsc_send(
            &scope.events,
            DriverEvent::Removal(RemovalRequest {
                key: ChildKey::fixture(u64::MAX - 1),
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

    exit.dispatch(&mut scope);
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
            if queued.key == ChildKey::fixture(u64::MAX - 1)
    ));
    let removal = recv_removal(
        &mut event_receiver,
        DRIVER_PROGRESS_WAIT,
        "the fused removal edge",
    )
    .await;
    assert_eq!(removal.key, key);
    scope.handle_removal(removal);
    let (child, panic) = recv_construction_disposed(
        &mut scope.disposal_event_receiver,
        DRIVER_PROGRESS_WAIT,
        "removal joins retained construction disposal",
    )
    .await;
    scope.handle_construction_disposed(child, panic);
    assert!(scope.children.get(key).is_none());
}
