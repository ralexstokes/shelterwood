//! Scope-cell observation tests: the per-tree gate, epoch bookkeeping, and
//! the lock rule's post-gate destruction of every retained `Exit` payload.
//!
//! Each test drives a bare `ScopeCell` graph built from
//! [`unresolved_isolated_scope`], with no scope driver behind it.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Barrier,
    time::Duration,
};

use shelterwood_core::{
    exit::{
        Cancellation, Exit, ExitError, GracePhase, StartupError, StartupFailure,
        StartupFailureCause, StopReason,
    },
    identity::IncarnationCounter,
    policy::RestartCount,
};

use super::*;
use crate::cells::{
    LIFECYCLE_EVENT_CAPACITY, LifecycleTryRecvError, MemberStage, MemberTransition,
    StartupDisposition,
    observe::LifecycleEventKind,
    test_support::{TEST_WAIT, resolve_options, unresolved_isolated_scope},
};

/// A user error payload that records whether its destructor ran on the
/// retiring thread while the observation gate was held.
///
/// The lock rule's probe for `Exit`: an `ExitKind::Failed` carries a
/// type-erased application error, so wherever the cell layer destroys an exit
/// it is running caller code. The retiring thread identity plus
/// `ObservationGate::is_held` answers from inside that destructor without the
/// reentrant acquisition that would deadlock.
struct GateProbeError {
    gate: crate::cells::ObservationGate,
    retiring_thread: std::thread::ThreadId,
    held_at_drop: Arc<Mutex<Option<bool>>>,
}

/// Builds a failed exit whose payload reports where it was destroyed.
fn gate_probe_exit(scope: &Arc<ScopeCell>) -> (Exit, Arc<Mutex<Option<bool>>>) {
    let held_at_drop = Arc::new(Mutex::new(None));
    let exit = Exit::failed(
        ExitError::from(GateProbeError {
            gate: scope.observation_gate(),
            retiring_thread: std::thread::current().id(),
            held_at_drop: Arc::clone(&held_at_drop),
        }),
        Cancellation::NotObserved,
    );
    (exit, held_at_drop)
}

/// A dynamic root with one admitted, started child, plus the child's
/// incarnation counter — the shape a restart schedule needs.
fn restarting_member_fixture() -> (Arc<ScopeCell>, Arc<MemberCell>, IncarnationCounter) {
    let root = unresolved_isolated_scope("root", ScopeFlavor::Dynamic);
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(root.mint_membership(&child_id));
    resolve_options(&member);
    let mut incarnations = member.take_incarnation_counter();
    assert!(root.admit_child(ResidentProjection::new(Arc::clone(&member), None)));
    let first = incarnations.mint();
    assert!(member.transition(MemberTransition::Starting { incarnation: first }));
    (root, member, incarnations)
}

fn gate_probe_verdict(held_at_drop: &Arc<Mutex<Option<bool>>>) -> Option<bool> {
    *held_at_drop.lock().expect("gate probe mutex poisoned")
}

fn wait_for_gate_probe(held_at_drop: &Arc<Mutex<Option<bool>>>) -> bool {
    let deadline = std::time::Instant::now() + TEST_WAIT;
    loop {
        if let Some(verdict) = gate_probe_verdict(held_at_drop) {
            return verdict;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for retained exit disposal"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

impl std::fmt::Debug for GateProbeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("GateProbeError")
    }
}

impl std::fmt::Display for GateProbeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("gate probe")
    }
}

impl std::error::Error for GateProbeError {}

impl Drop for GateProbeError {
    fn drop(&mut self) {
        let ran_inline_under_gate =
            std::thread::current().id() == self.retiring_thread && self.gate.is_held();
        *self.held_at_drop.lock().expect("gate probe mutex poisoned") = Some(ran_inline_under_gate);
    }
}

#[test]
#[should_panic(expected = "resident member options are resolved before snapshot publication")]
fn snapshot_rejects_a_resident_whose_options_were_never_resolved() {
    let root = unresolved_isolated_scope("root", ScopeFlavor::Dynamic);
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(root.mint_membership(&child_id));

    assert!(root.admit_child(ResidentProjection::new(member, None)));
    let _ = root.snapshot();
}

#[test]
fn independent_systems_do_not_share_an_observation_critical_section() {
    let first = unresolved_isolated_scope("first", ScopeFlavor::Ordered);
    let second = unresolved_isolated_scope("second", ScopeFlavor::Ordered);
    let first_gate = first.observation_gate();
    let second_gate = second.observation_gate();
    assert!(!first_gate.same_gate(&second_gate));

    let held = first_gate.lock();
    let (completed, receiver) = std::sync::mpsc::sync_channel(0);
    let worker = std::thread::spawn(move || {
        second.set_state(ScopeState::Starting);
        completed.send(()).expect("test receiver remains available");
    });
    let result = receiver.recv_timeout(TEST_WAIT);
    drop(held);
    worker.join().expect("independent transition succeeds");
    assert_eq!(
        result,
        Ok(()),
        "holding one system's gate must not stall another system"
    );
}

#[test]
fn observation_gate_poison_does_not_wedge_later_observation() {
    let scope = unresolved_isolated_scope("scope", ScopeFlavor::Ordered);
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
    let scope = unresolved_isolated_scope("scope", ScopeFlavor::Ordered);
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
    let scope = unresolved_isolated_scope("scope", ScopeFlavor::Ordered);
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
    let scope = unresolved_isolated_scope("scope", ScopeFlavor::Ordered);
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

#[test]
fn a_declined_epoch_still_publishes_its_owned_terminal_exit() {
    let scope = unresolved_isolated_scope("scope", ScopeFlavor::Ordered);
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
        Exit::aborted(GracePhase::WithinGrace, Cancellation::Observed),
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
fn receiverless_config_state_is_atomic_under_concurrent_snapshots() {
    const UPDATES: usize = 2_000;

    let scope = unresolved_isolated_scope("scope", ScopeFlavor::Ordered);
    let first = Intensity::new(1, Duration::from_secs(1)).expect("valid first intensity");
    let second = Intensity::new(2, Duration::from_secs(2)).expect("valid second intensity");
    scope.set_intensity(first);

    let start = Arc::new(Barrier::new(2));
    let (first_update, first_update_seen) = std::sync::mpsc::sync_channel(0);
    let (first_snapshot, first_snapshot_seen) = std::sync::mpsc::sync_channel(0);
    let writer_scope = Arc::clone(&scope);
    let writer_start = Arc::clone(&start);
    let writer = std::thread::spawn(move || {
        writer_start.wait();
        for update in 0..UPDATES {
            let intensity = if update % 2 == 0 { second } else { first };
            writer_scope.set_intensity(intensity);
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
fn pre_admission_observer_retries_after_gate_handoff() {
    let root = unresolved_isolated_scope("root", ScopeFlavor::Ordered);
    let nested = unresolved_isolated_scope("nested", ScopeFlavor::Dynamic);
    let captures = nested.probe_gate_captures();
    let prior_gate = nested.observation_gate();
    let held = prior_gate.lock();
    let observer = Arc::clone(&nested);
    let worker = std::thread::spawn(move || observer.set_state(ScopeState::Starting));

    // The capture report proves the observer committed to the
    // pre-admission gate, which the held guard keeps it from acquiring.
    assert_eq!(
        captures
            .recv_timeout(TEST_WAIT)
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
            .recv_timeout(TEST_WAIT)
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

/// SPEC §15.4's lock rule: nothing user-owned is destroyed inside a framework
/// critical section. `Exit`'s type-erased application error is the cell
/// layer's only user-owned value, and the tests below cover the paths that
/// retire one under the resident-tree observation gate: a losing
/// terminalization, a superseded snapshot (on subscription and on closure),
/// and the member record's `last_exit` slot (on a restart schedule and on
/// terminalization), lifecycle-ring eviction and residency retirement.
#[test]
fn a_losing_terminal_exit_payload_is_destroyed_outside_the_gate() {
    let scope = unresolved_isolated_scope("scope", ScopeFlavor::Ordered);
    scope.member.terminalize(
        Exit::completed(Cancellation::NotObserved),
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
    let root = unresolved_isolated_scope("root", ScopeFlavor::Ordered);
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(root.mint_membership(&child_id));
    resolve_options(&member);
    assert!(root.admit_child(ResidentProjection::new(Arc::clone(&member), None)));
    member.terminalize(
        Exit::completed(Cancellation::NotObserved),
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
    let root = unresolved_isolated_scope("root", ScopeFlavor::Dynamic);
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(root.mint_membership(&child_id));
    resolve_options(&member);
    assert!(root.admit_child(ResidentProjection::new(Arc::clone(&member), None)));
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
    assert!(member.transition(MemberTransition::RestartScheduled {
        exit: probe,
        restart_count: RestartCount::ZERO.bump(),
        restart_at: None,
    }));
    assert_eq!(
        gate_probe_verdict(&held_at_drop),
        None,
        "the record still owns the scheduled restart's exit"
    );

    let second = incarnations.mint();
    assert!(member.transition(MemberTransition::Starting {
        incarnation: second,
    }));
    assert!(member.transition(MemberTransition::RestartScheduled {
        exit: Exit::completed(Cancellation::NotObserved),
        restart_count: RestartCount::ZERO.bump().bump(),
        restart_at: None,
    }));
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
    assert!(member.transition(MemberTransition::RestartScheduled {
        exit: probe,
        restart_count: RestartCount::ZERO.bump(),
        restart_at: None,
    }));
    assert_eq!(
        gate_probe_verdict(&held_at_drop),
        None,
        "the record still owns the scheduled restart's exit"
    );

    member.terminalize(
        Exit::completed(Cancellation::NotObserved),
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
    let root = unresolved_isolated_scope("root", ScopeFlavor::Dynamic);
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(root.mint_membership(&child_id));
    resolve_options(&member);
    assert!(root.admit_child(ResidentProjection::new(Arc::clone(&member), None)));
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
    let root = unresolved_isolated_scope("root", ScopeFlavor::Dynamic);
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
    let root = unresolved_isolated_scope("root", ScopeFlavor::Dynamic);
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
    let root = unresolved_isolated_scope("root", ScopeFlavor::Dynamic);
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
    let root = unresolved_isolated_scope("root", ScopeFlavor::Dynamic);
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(root.mint_membership(&child_id));
    resolve_options(&member);
    assert!(root.admit_child(ResidentProjection::new(Arc::clone(&member), None)));
    let weak = Arc::downgrade(&member);
    let (exit, held_at_drop) = gate_probe_exit(&root);
    member.terminalize(exit, StartupDisposition::Unchanged);
    drop(member);

    root.clear_residents();

    let deadline = std::time::Instant::now() + TEST_WAIT;
    while weak.upgrade().is_some() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for detached resident disposal"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        !wait_for_gate_probe(&held_at_drop),
        "retiring the member record must not run its payload under the observation gate"
    );
}
