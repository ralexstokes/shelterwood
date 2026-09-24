//! Unit tests for the scope cell.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use crate::mailbox::{
    MailboxCell, MailboxControl, MailboxEffectQueue, MailboxReceiver, actor_ref_from_parts,
};
use shelterwood_core::{
    Cancellation, Exit, ExitError, GracePhase, IntensityTrip, StartupFailure, StartupFailureCause,
    exit::{StartupError, StopReason},
    identity::ScopeIdentity,
    policy::{ResolvedMailbox, RestartAttempt, RestartCount, ScopeFlavor},
};

use super::{
    control::{ControlPoison, ScopeRequest},
    *,
};
use crate::cells::{
    LifecycleEvent, LifecycleItem, LifecycleTryRecvError, MemberStage, MemberTransition, Retained,
    StartupDisposition,
    observe::LifecycleEventKind,
    test_support::{TEST_WAIT, ThreadProbe, child_member, child_scope, isolated_scope},
};

struct GateCheckingWake {
    gate: super::ObservationGate,
    woke_after_unlock: Arc<AtomicBool>,
}

#[test]
fn never_started_body_publishes_before_membership_closes_observation() {
    let scope = isolated_scope("nested", ScopeFlavor::Ordered);
    let snapshots = scope.subscribe_snapshots();

    scope.close_never_started_body();

    let (snapshot, closed) = snapshots.borrow_latest_and_closed();
    assert!(!closed, "membership terminality owns observation closure");
    assert!(matches!(
        snapshot.state,
        ScopeState::Stopped {
            reason: StopReason::NeverStarted
        }
    ));
    assert_eq!(snapshot.total_restarts, TotalRestarts::ZERO);
    assert!(matches!(
        scope.record().startup,
        Some(Err(StartupError::ShutdownRequested))
    ));
    assert!(
        !matches!(scope.member.record().stage, MemberStage::Terminal(_)),
        "the parent driver still owns membership terminality"
    );

    scope.terminalize_never_started();
    let (_, closed) = snapshots.borrow_latest_and_closed();
    assert!(closed, "terminal membership closes observation");
}

/// A body dropped before its *restart* incarnation began keeps the prior
/// incarnation's published reason, whichever of the two owners of that
/// scope's terminality runs first.
///
/// The parent path (`terminalize_child`) and the body fallback race on a
/// current-thread runtime only by a few instructions, and on a
/// multi-thread runtime genuinely. Publishing `NeverStarted` here would
/// both depend on that order and falsely claim that no scope incarnation
/// ever began; SPEC B.6's stop-reason lattice instead preserves the real
/// prior-incarnation verdict within the scope plane in either order.
#[test]
fn never_polled_restart_body_keeps_its_prior_reason_in_either_arrival_order() {
    for parent_first in [true, false] {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let nested = child_scope(&root, "nested", ScopeFlavor::Ordered);
        assert!(root.admit_child(ResidentProjection::new(
            Arc::clone(&nested.member),
            Some(Arc::clone(&nested)),
        )));
        let mut incarnations = nested.member.take_incarnation_counter();
        let first = incarnations.mint();
        assert!(
            nested
                .member
                .transition(MemberTransition::Starting { incarnation: first })
        );
        let epoch = nested
            .begin_incarnation(ScopeState::Starting)
            .expect("first incarnation begins");
        nested.finish_incarnation(epoch, StopReason::Finished);
        assert!(
            nested
                .member
                .transition(MemberTransition::RestartScheduled {
                    exit: Exit::completed(Cancellation::NotObserved),
                    restart_count: RestartCount::ZERO.bump(),
                    restart_at: None,
                })
        );
        let restarted = incarnations.mint();
        assert!(nested.member.transition(MemberTransition::Starting {
            incarnation: restarted,
        }));
        let snapshots = nested.subscribe_snapshots();

        // The restart body is dropped before its first poll, so it never
        // reaches `begin_incarnation` and the parent aborts it.
        let terminalize = || {
            root.terminalize_child(
                &nested.member,
                Exit::aborted(GracePhase::WithinGrace, Cancellation::Observed),
                Some(restarted),
                StartupDisposition::NotAborted,
            )
        };
        if parent_first {
            terminalize();
            nested.close_never_started_body();
        } else {
            nested.close_never_started_body();
            terminalize();
        }

        let (snapshot, closed) = snapshots.borrow_latest_and_closed();
        assert_eq!(
            snapshot.state,
            ScopeState::Stopped {
                reason: StopReason::Finished
            },
            "a never-polled restart keeps its prior incarnation's reason \
             (parent_first={parent_first})"
        );
        assert!(
            closed,
            "membership terminality closes observation (parent_first={parent_first})"
        );
        let record = nested.member.record();
        assert!(
            matches!(record.stage, MemberStage::Terminal(_)),
            "the parent owns the shared membership's terminal exit"
        );
        assert!(
            record.last_incarnation.is_some(),
            "a restarted membership has spawned, so `ExitKind::NeverStarted` cannot describe its exit"
        );
    }
}

impl Wake for GateCheckingWake {
    fn wake(self: Arc<Self>) {
        self.woke_after_unlock
            .store(!self.gate.is_held(), Ordering::SeqCst);
    }
}

/// A user payload whose destructor panics, as SPEC §5.5's containment
/// clause anticipates.
struct HostileDropMessage {
    entered: mpsc::SyncSender<&'static str>,
    id: &'static str,
}

impl Drop for HostileDropMessage {
    fn drop(&mut self) {
        let _ = self.entered.send(self.id);
        panic!("hostile resident payload destructor");
    }
}

struct GateDropMessage {
    gate: super::ObservationGate,
    entered: mpsc::SyncSender<(bool, std::thread::ThreadId)>,
    release: mpsc::Receiver<()>,
}

impl Drop for GateDropMessage {
    fn drop(&mut self) {
        let _ = self
            .entered
            .send((!self.gate.is_held(), std::thread::current().id()));
        let _ = self.release.recv_timeout(TEST_WAIT);
    }
}

#[test]
fn nonresident_terminal_exit_is_retained_before_the_residency_assertion() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let member = child_member(&root, "missing");
    let (dropped, observed) = mpsc::sync_channel(1);
    let retiring_thread = std::thread::current().id();
    let exit = Exit::failed(
        ExitError::from(ThreadProbe(dropped)),
        Cancellation::NotObserved,
    );

    catch_unwind(AssertUnwindSafe(|| {
        root.terminalize_child(&member, exit, None, StartupDisposition::Unchanged);
    }))
    .expect_err("a supervised child must remain resident");

    assert_ne!(
        observed
            .recv_timeout(TEST_WAIT)
            .expect("failed exit disposal reports"),
        retiring_thread,
        "the incoming error cannot unwind through the observation gate"
    );
}

#[test]
fn foreign_gate_drain_retains_startup_exit_across_the_diagnostic() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let foreign = isolated_scope("foreign", ScopeFlavor::Ordered);
    let (dropped, observed) = mpsc::sync_channel(1);
    let retiring_thread = std::thread::current().id();
    let startup = Err(StartupError::StartupFailed(StartupFailure {
        cause: StartupFailureCause::Child {
            id: foreign.member.id().clone(),
            membership: foreign.member.membership(),
            exit: Exit::failed(
                ExitError::from(ThreadProbe(dropped)),
                Cancellation::NotObserved,
            ),
        },
    }));

    catch_unwind(AssertUnwindSafe(|| {
        root.publish_drain(
            ScopeState::Draining,
            Some(startup),
            &[Arc::clone(&foreign.member)],
        );
    }))
    .expect_err("a drain cannot mark a member from another observation gate");

    assert!(
        !root.observation_gate().is_poisoned(),
        "the foreign-gate diagnostic resumes only after the transaction unlocks"
    );
    assert_ne!(
        observed
            .recv_timeout(TEST_WAIT)
            .expect("the rejected startup exit is destroyed"),
        retiring_thread,
        "the diagnostic cannot destroy the nested user error on the unwinding driver thread"
    );
}

#[test]
fn rejected_resident_admission_detaches_its_last_mailbox_owner() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let mut lifecycle = root.subscribe_lifecycle();
    let member = child_member(&root, "invalid");
    let mut incarnations = member.take_incarnation_counter();
    let incarnation = incarnations.mint();
    let mailbox = MailboxCell::new(member.id().clone());
    member.attach_mailbox(mailbox.clone());
    let actor = actor_ref_from_parts(Arc::clone(&member), Arc::clone(&mailbox));
    let mut effects = MailboxEffectQueue::default();
    let token = MailboxControl::configure(&*mailbox, ResolvedMailbox::Latest, &mut effects);
    MailboxControl::bind(&*mailbox, token, incarnation, &mut effects);
    drop(effects);
    // Admission below is intentionally illegal, but the projection is
    // still the final owner of this mailbox-bearing member when it fails.
    assert!(member.transition(MemberTransition::Admitted));
    let (dropped, observed) = mpsc::sync_channel(1);
    actor
        .try_send(ThreadProbe(dropped))
        .expect("bound mailbox accepts the probe");
    let projection = ResidentProjection::new(member, None);
    drop(actor);
    drop(mailbox);
    let retiring_thread = std::thread::current().id();

    assert!(
        !root.admit_child(projection),
        "an admitted member cannot be admitted twice"
    );
    assert!(
        root.resident_projections().is_empty(),
        "a rejected admission publishes no residency"
    );
    assert_eq!(
        lifecycle.try_recv(),
        Err(LifecycleTryRecvError::Empty),
        "ordinary refusal emits neither Added nor Removed"
    );

    assert_ne!(
        observed
            .recv_timeout(TEST_WAIT)
            .expect("mailbox payload disposal reports"),
        retiring_thread,
        "the by-value projection cannot unwind its mailbox through the observation gate"
    );
}

#[test]
fn panicked_resident_admission_lingers_until_scope_clear() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let mut lifecycle = root.subscribe_lifecycle();
    let nested = child_scope(&root, "nested", ScopeFlavor::Dynamic);
    let mut incarnations = nested.member.take_incarnation_counter();
    let incarnation = incarnations.mint();
    let mailbox = MailboxCell::new(nested.member.id().clone());
    nested.member.attach_mailbox(mailbox.clone());
    let actor = actor_ref_from_parts(Arc::clone(&nested.member), Arc::clone(&mailbox));
    let mut effects = MailboxEffectQueue::default();
    let token = MailboxControl::configure(&*mailbox, ResolvedMailbox::Latest, &mut effects);
    MailboxControl::bind(&*mailbox, token, incarnation, &mut effects);
    drop(effects);
    let (dropped, observed) = mpsc::sync_channel(1);
    actor
        .try_send(ThreadProbe(dropped))
        .expect("bound mailbox accepts the probe");

    let poisoned = Arc::clone(&nested);
    assert!(
        catch_unwind(AssertUnwindSafe(move || {
            let _control = poisoned.control.lock().expect("control starts healthy");
            panic!("inject admission bookkeeping panic");
        }))
        .is_err(),
        "the fixture poisons the post-transition parent-wiring step"
    );

    let membership = nested.member.membership();
    let projection = ResidentProjection::new(Arc::clone(&nested.member), Some(nested));
    drop(actor);
    drop(mailbox);
    let retiring_thread = std::thread::current().id();
    catch_unwind(AssertUnwindSafe(|| root.admit_child(projection)))
        .expect_err("poisoned parent wiring unwinds admission");

    let residents = root.resident_projections();
    assert_eq!(residents.len(), 1, "the half-wired resident remains owned");
    assert_eq!(residents[0].member.membership(), membership);
    assert!(matches!(
        residents[0].member.record().stage,
        MemberStage::Admitted
    ));
    drop(residents);
    assert_eq!(
        observed.try_recv(),
        Err(mpsc::TryRecvError::Empty),
        "admission unwind does not destroy the resident mailbox"
    );
    assert_eq!(
        lifecycle.try_recv(),
        Err(LifecycleTryRecvError::Empty),
        "the panic precedes Added publication"
    );
    assert!(
        root.snapshot().children.is_empty(),
        "an unannounced resident is owned residency, not an observed child"
    );

    root.clear_residents();
    assert!(root.resident_projections().is_empty());
    assert_eq!(
        lifecycle.try_recv(),
        Err(LifecycleTryRecvError::Empty),
        "an unannounced resident publishes no Removed: SPEC §3.2 pairs both edges or neither"
    );
    assert_ne!(
        observed
            .recv_timeout(TEST_WAIT)
            .expect("scope clear disposes the lingering mailbox payload"),
        retiring_thread,
        "scope clear retains the detached disposal venue"
    );
}

/// The venues that withdraw a resident from its scope. Every one of them
/// severs, so each pin below runs against all three.
#[derive(Clone, Copy, Debug)]
enum Retirement {
    Prune,
    Clear,
    Replace,
}

impl Retirement {
    fn retire(self, root: &Arc<ScopeCell>, member: &MemberCell) {
        match self {
            Self::Prune => assert!(root.prune_child(member)),
            Self::Clear => root.clear_residents(),
            Self::Replace => assert!(root.set_admitted_children(Vec::new())),
        }
    }
}

#[test]
fn retiring_an_unannounced_nested_resident_severs_its_parent_link() {
    for retirement in [Retirement::Prune, Retirement::Clear, Retirement::Replace] {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let nested = child_scope(&root, "nested", ScopeFlavor::Dynamic);
        let mut root_lifecycle = root.subscribe_lifecycle();
        let mut nested_lifecycle = nested.subscribe_lifecycle();

        // `set_parent` installs the back-link before this poisoned control
        // lock unwinds the admission, leaving an unannounced resident behind.
        let poisoned = Arc::clone(&nested);
        assert!(
            catch_unwind(AssertUnwindSafe(move || {
                let _control = poisoned.control.lock().expect("control starts healthy");
                panic!("inject admission bookkeeping panic");
            }))
            .is_err()
        );
        catch_unwind(AssertUnwindSafe(|| {
            let _ = root.admit_child(ResidentProjection::new(
                Arc::clone(&nested.member),
                Some(Arc::clone(&nested)),
            ));
        }))
        .expect_err("poisoned parent wiring unwinds admission");

        assert!(Arc::ptr_eq(
            &nested
                .parent()
                .expect("the failed admission installed its link"),
            &root
        ));
        retirement.retire(&root, &nested.member);
        assert!(
            nested.parent().is_none(),
            "{retirement:?} severs an unannounced resident's parent link"
        );
        assert_eq!(
            root_lifecycle.try_recv(),
            Err(LifecycleTryRecvError::Empty),
            "the failed admission published neither membership edge"
        );

        nested.set_state(ScopeState::Stopped {
            reason: StopReason::Finished,
        });
        assert!(matches!(
            nested_lifecycle.try_recv(),
            Ok(LifecycleItem::Event(_))
        ));
        assert_eq!(
            root_lifecycle.try_recv(),
            Err(LifecycleTryRecvError::Empty),
            "post-retirement events stay local to the removed subtree ({retirement:?})"
        );
    }
}

/// Retirement severs; destroying a resident child does not.
///
/// A refused duplicate admission temporarily owns a second `ResidentChild`
/// for an already-resident subtree and then disposes it, so a sever in
/// `Drop` would detach a subtree that is still resident under its original
/// parent. `rejected_nested_admission_preserves_original_parent_and_gate`
/// walks that refusal path, but it retires the duplicate through *detached*
/// disposal — the assertion outruns the drop, so only this synchronous
/// destruction pins the venue.
#[test]
fn destroying_a_resident_child_leaves_the_subtree_parent_link_installed() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = child_scope(&root, "nested", ScopeFlavor::Dynamic);
    assert!(root.admit_child(ResidentProjection::new(
        Arc::clone(&nested.member),
        Some(Arc::clone(&nested)),
    )));

    drop(ResidentChild::new(ResidentProjection::new(
        Arc::clone(&nested.member),
        Some(Arc::clone(&nested)),
    )));

    assert!(
        Arc::ptr_eq(
            &nested
                .parent()
                .expect("residency still owns the upward observation edge"),
            &root
        ),
        "a duplicate resident's destruction must not sever the original"
    );
}

#[test]
fn announced_nested_retirement_makes_removed_the_last_parent_event() {
    for retirement in [Retirement::Prune, Retirement::Clear, Retirement::Replace] {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let nested = child_scope(&root, "nested", ScopeFlavor::Dynamic);
        let membership = nested.member.membership();
        let mut root_lifecycle = root.subscribe_lifecycle();
        let mut nested_lifecycle = nested.subscribe_lifecycle();

        assert!(root.admit_child(ResidentProjection::new(
            Arc::clone(&nested.member),
            Some(Arc::clone(&nested)),
        )));
        assert!(matches!(
            root_lifecycle.try_recv(),
            Ok(LifecycleItem::Event(LifecycleEvent {
                kind: LifecycleEventKind::Added {
                    membership: added,
                    ..
                },
                ..
            })) if added == membership
        ));

        retirement.retire(&root, &nested.member);
        assert!(matches!(
            root_lifecycle.try_recv(),
            Ok(LifecycleItem::Event(LifecycleEvent {
                kind: LifecycleEventKind::Removed {
                    membership: removed,
                    ..
                },
                ..
            })) if removed == membership
        ));
        assert!(
            nested.parent().is_none(),
            "{retirement:?} withdraws the resident's upward observation edge"
        );

        nested.set_state(ScopeState::Stopped {
            reason: StopReason::Finished,
        });
        assert!(matches!(
            nested_lifecycle.try_recv(),
            Ok(LifecycleItem::Event(LifecycleEvent {
                kind: LifecycleEventKind::ScopeState {
                    state: ScopeState::Stopped {
                        reason: StopReason::Finished,
                    },
                },
                ..
            }))
        ));
        assert_eq!(
            root_lifecycle.try_recv(),
            Err(LifecycleTryRecvError::Empty),
            "Removed is the parent stream's final word about the subtree ({retirement:?})"
        );
    }
}

#[test]
fn nested_retirement_tolerates_a_poisoned_parent_link() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = child_scope(&root, "nested", ScopeFlavor::Dynamic);
    assert!(root.admit_child(ResidentProjection::new(
        Arc::clone(&nested.member),
        Some(Arc::clone(&nested)),
    )));

    let poisoned = Arc::clone(&nested);
    assert!(
        catch_unwind(AssertUnwindSafe(move || {
            let _parent = poisoned
                .observation
                .parent
                .lock()
                .expect("parent link starts healthy");
            panic!("inject parent-link poison");
        }))
        .is_err()
    );

    root.clear_residents();
    assert!(
        nested.parent().is_none(),
        "retirement recovers the parent link and severs it"
    );
}

#[test]
fn resident_slot_announces_its_index_when_a_later_resident_exists() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let first = child_member(&root, "first");
    let second = child_member(&root, "second");

    let first_slot = root.push_unannounced(ResidentProjection::new(first, None));
    let _second_slot = root.push_unannounced(ResidentProjection::new(second, None));

    assert!(
        first_slot.announce(),
        "the slot still addresses the resident installed before the later push"
    );
    let children = root.current_children();
    assert!(children[0].announced, "the indexed resident is announced");
    assert!(
        !children[1].announced,
        "announcing an earlier slot cannot mark the last resident"
    );
}

#[test]
fn resident_slot_refuses_a_different_membership_at_its_index() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let first = child_member(&root, "first");
    let second = child_member(&root, "second");

    let first_slot = root.push_unannounced(ResidentProjection::new(first, None));
    let _second_slot = root.push_unannounced(ResidentProjection::new(second, None));
    root.current_children().swap(0, 1);

    assert!(
        !first_slot.announce(),
        "an index that now names another membership is not this admission's slot"
    );
    assert!(
        root.current_children()
            .iter()
            .all(|resident| !resident.announced),
        "a mismatched slot cannot announce either resident"
    );
}

#[test]
fn resident_slot_withdraws_the_resident_it_installed() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let only = child_member(&root, "only");
    let membership = only.membership();

    let slot = root.push_unannounced(ResidentProjection::new(only, None));
    let (rejected, is_admission) = slot.withdraw();

    assert!(
        is_admission,
        "the slot still holds this admission's resident"
    );
    assert_eq!(
        rejected
            .expect("withdrawal hands the displaced resident back")
            .projection()
            .member
            .membership(),
        membership,
        "withdrawal returns the resident the slot installed"
    );
    assert!(
        root.current_children().is_empty(),
        "withdrawal leaves no residency entry behind"
    );
}

#[test]
fn resident_slot_withdrawal_refuses_an_interposed_resident() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let first = child_member(&root, "first");
    let second = child_member(&root, "second");
    let second_membership = second.membership();

    let first_slot = root.push_unannounced(ResidentProjection::new(first, None));
    let _second_slot = root.push_unannounced(ResidentProjection::new(second, None));
    let (rejected, is_admission) = first_slot.withdraw();

    assert!(
        !is_admission,
        "a slot that is no longer the final entry is not this admission's resident"
    );
    assert_eq!(
        rejected
            .expect("withdrawal still displaces the final resident")
            .projection()
            .member
            .membership(),
        second_membership,
        "withdrawal displaces the last entry so residency cannot grow past a refusal"
    );
}

#[test]
fn resident_slot_withdrawal_refuses_a_different_membership_at_its_index() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let first = child_member(&root, "first");
    let second = child_member(&root, "second");
    let first_membership = first.membership();

    let _first_slot = root.push_unannounced(ResidentProjection::new(first, None));
    let second_slot = root.push_unannounced(ResidentProjection::new(second, None));
    root.current_children().swap(0, 1);
    let (rejected, is_admission) = second_slot.withdraw();

    assert!(
        !is_admission,
        "a final index that now names another membership is not this admission's slot"
    );
    assert_eq!(
        rejected
            .expect("withdrawal still displaces the final resident")
            .projection()
            .member
            .membership(),
        first_membership,
        "the displaced resident is whichever entry the swap left last"
    );
}

/// Withdrawal mirrors announcement at both removal sites, not just the
/// one `panicked_resident_admission_lingers_until_scope_clear` drives.
#[test]
fn pruning_an_unannounced_resident_publishes_no_removal_edge() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let member = child_member(&root, "half-wired");
    let mut lifecycle = root.subscribe_lifecycle();

    // Install residency exactly as an admission that unwound before its
    // `Added` publication leaves it: the production push, with its slot
    // dropped instead of announced.
    drop(root.push_unannounced(ResidentProjection::new(Arc::clone(&member), None)));
    assert_eq!(root.resident_projections().len(), 1);
    assert!(root.snapshot().children.is_empty());

    assert!(
        root.prune_child(&member),
        "an unannounced resident is still withdrawn"
    );
    assert!(root.resident_projections().is_empty());
    assert_eq!(
        lifecycle.try_recv(),
        Err(LifecycleTryRecvError::Empty),
        "neither edge is published for a membership that never announced"
    );
}

#[test]
fn rejected_nested_admission_preserves_original_parent_and_gate() {
    let original = isolated_scope("original", ScopeFlavor::Ordered);
    let destination = isolated_scope("destination", ScopeFlavor::Ordered);
    let nested = child_scope(&original, "nested", ScopeFlavor::Dynamic);
    let descendant = child_member(&nested, "descendant");
    assert!(
        nested.set_admitted_children(vec![ResidentProjection::new(Arc::clone(&descendant), None)])
    );
    assert!(original.set_admitted_children(vec![ResidentProjection::new(
        Arc::clone(&nested.member),
        Some(Arc::clone(&nested)),
    )]));
    let original_gate = original.observation_gate();
    assert!(original_gate.same_gate(&nested.observation_gate()));
    assert!(original_gate.same_gate(&descendant.observation_gate()));

    assert!(
        !destination.admit_child(ResidentProjection::new(
            Arc::clone(&nested.member),
            Some(Arc::clone(&nested)),
        )),
        "an already-admitted subtree cannot move to a second parent"
    );

    assert!(original.has_resident_child(&nested.member));
    assert!(destination.resident_projections().is_empty());
    assert!(Arc::ptr_eq(
        &nested
            .parent()
            .expect("the original parent remains installed"),
        &original
    ));
    assert!(original_gate.same_gate(&nested.observation_gate()));
    assert!(original_gate.same_gate(&descendant.observation_gate()));
    assert!(
        !destination
            .observation_gate()
            .same_gate(&nested.observation_gate()),
        "rejection leaves the subtree on its original observation gate"
    );
}

#[test]
fn adoption_does_not_forward_an_already_finished_shutdown_target() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Ordered);
    let target = nested
        .begin_incarnation(ScopeState::Starting)
        .expect("the nested scope begins its first epoch");
    nested.finish_incarnation(target, StopReason::Finished);
    nested
        .control
        .lock()
        .expect("scope control mutex remains healthy")
        .shutdown = Some(ScopeRequest {
        epoch: target,
        consumed: false,
    });

    assert!(root.admit_child(ResidentProjection::new(
        Arc::clone(&nested.member),
        Some(nested),
    )));
    assert!(
        root.take_control_events().is_empty(),
        "adoption cannot forward a target the nested epoch plane already finished"
    );
}

#[test]
fn rejected_stage_transition_disposes_its_lifecycle_exit_off_thread() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let member = child_member(&root, "invalid");
    // Residency is what puts the member on the root's observation gate, so
    // a projection transition reaches it the way production does.
    assert!(root.admit_child(ResidentProjection::new(Arc::clone(&member), None)));
    member.update(|record| record.stage = MemberStage::Reserved);
    let mut events = root.subscribe_lifecycle();
    let (dropped, observed) = mpsc::sync_channel(1);
    let retiring_thread = std::thread::current().id();

    assert!(
        !root.transition_child_stage(
            &member,
            MemberTransition::RestartScheduled {
                exit: Exit::completed(Cancellation::NotObserved),
                restart_count: RestartCount::ZERO.bump(),
                restart_at: None,
            },
            Some(LifecycleEventKind::Exited {
                id: member.id().clone(),
                membership: member.membership(),
                incarnation: member.take_incarnation_counter().mint(),
                exit: Exit::failed(
                    ExitError::from(ThreadProbe(dropped)),
                    Cancellation::NotObserved,
                ),
            }),
        ),
        "the Reserved-to-Restarting projection transition is illegal"
    );
    assert!(matches!(member.record().stage, MemberStage::Reserved));
    assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Empty));
    assert_ne!(
        observed
            .recv_timeout(TEST_WAIT)
            .expect("the refused lifecycle exit is destroyed"),
        retiring_thread,
        "a refused scope event keeps the detached disposal venue"
    );
}

#[test]
fn rejected_restart_publication_disposes_its_lifecycle_exit_off_thread() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let member = child_member(&root, "invalid");
    // Residency is what puts the member on the root's observation gate, so
    // a restart publication reaches it the way production does.
    assert!(root.admit_child(ResidentProjection::new(Arc::clone(&member), None)));
    member.update(|record| record.stage = MemberStage::Reserved);
    let mut events = root.subscribe_lifecycle();
    let incarnation = member.take_incarnation_counter().mint();
    let (dropped, observed) = mpsc::sync_channel(1);
    let retiring_thread = std::thread::current().id();
    let exit = Exit::failed(
        ExitError::from(ThreadProbe(dropped)),
        Cancellation::NotObserved,
    );

    assert!(
        !root.publish_child_restart(
            &member,
            TotalRestarts::ZERO,
            Retained::new(exit.clone()),
            MemberTransition::RestartScheduled {
                exit: Exit::completed(Cancellation::NotObserved),
                restart_count: RestartCount::ZERO.bump(),
                restart_at: None,
            },
            LifecycleEventKind::Exited {
                id: member.id().clone(),
                membership: member.membership(),
                incarnation,
                exit,
            },
            LifecycleEventKind::RestartScheduled {
                id: member.id().clone(),
                membership: member.membership(),
                attempt: RestartAttempt::ZERO.bump(),
                delay: Duration::ZERO,
            },
        ),
        "the Reserved-to-Restarting projection transition is illegal"
    );
    assert!(matches!(member.record().stage, MemberStage::Reserved));
    assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Empty));
    assert_ne!(
        observed
            .recv_timeout(TEST_WAIT)
            .expect("the refused restart exit is destroyed"),
        retiring_thread,
        "both refused restart events keep the detached disposal venue"
    );
}

struct InertRoute;

impl DynamicRoute for InertRoute {
    fn close_admission(&self, _txn: &mut ObservationTxn<'_>) {}
}

/// Coverage for the live-route re-homing assertion.
///
/// `admit_observation_gate` needs none: its legality probe
/// refuses every stage a started driver can present, so a re-homed live
/// route is unconstructible there. The reservation-time adoption path has
/// no such probe, so this test pins the assertion there.
#[test]
#[should_panic(expected = "a scope with a live dynamic route is never re-homed")]
fn plain_gate_adoption_rejects_a_scope_with_a_live_dynamic_route() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    nested.set_dynamic_route(Some(Arc::new(InertRoute)));

    root.with_observation_gate(|txn| {
        root.adopt_child_observation_gate(&nested.member, Some(&nested), txn);
    });
}

#[test]
fn adoption_retries_when_the_captured_child_gate_has_already_been_replaced() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    let replacement = isolated_scope("replacement", ScopeFlavor::Ordered);
    let captures = nested.probe_gate_captures();
    let prior_gate = nested.observation_gate();
    let held = prior_gate.lock();
    let adopting_root = Arc::clone(&root);
    let adopting_nested = Arc::clone(&nested);
    let adoption = std::thread::spawn(move || {
        assert!(adopting_root.admit_child(ResidentProjection::new(
            Arc::clone(&adopting_nested.member),
            Some(adopting_nested),
        )));
    });

    assert_eq!(
        captures
            .recv_timeout(TEST_WAIT)
            .expect("adoption captures the child's prior gate"),
        GateCapture::Adoption
    );
    nested.replace_observation_gate(replacement.observation_gate());
    drop(held);
    assert_eq!(
        captures
            .recv_timeout(TEST_WAIT)
            .expect("adoption retries after observing the replacement"),
        GateCapture::Adoption
    );
    adoption.join().expect("the retried adoption completes");

    assert!(
        root.observation_gate()
            .same_gate(&nested.observation_gate())
    );
    assert!(root.has_resident_child(&nested.member));
    assert_eq!(captures.try_recv(), Err(mpsc::TryRecvError::Empty));
}

#[test]
fn resident_readers_wait_out_a_half_wired_admission() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    let root_captures = root.probe_gate_captures();
    let nested_captures = nested.probe_gate_captures();
    let prior_gate = nested.observation_gate();
    let held = prior_gate.lock();
    let adopting_root = Arc::clone(&root);
    let adopting_nested = Arc::clone(&nested);
    let adoption = std::thread::spawn(move || {
        assert!(adopting_root.admit_child(ResidentProjection::new(
            Arc::clone(&adopting_nested.member),
            Some(adopting_nested),
        )));
    });

    assert_eq!(
        root_captures
            .recv_timeout(TEST_WAIT)
            .expect("admission captures the parent gate"),
        GateCapture::Observation
    );
    assert_eq!(
        nested_captures
            .recv_timeout(TEST_WAIT)
            .expect("admission reaches the child handoff after its residency push"),
        GateCapture::Adoption
    );
    assert!(matches!(
        nested.member.record().stage,
        MemberStage::Reserved
    ));

    let projecting_root = Arc::clone(&root);
    let (projected, projected_receiver) = mpsc::sync_channel(1);
    let projection_reader = std::thread::spawn(move || {
        let residents = projecting_root.resident_projections();
        projected
            .send((residents.len(), residents[0].member.record().stage.clone()))
            .expect("projection observation remains available");
    });
    let checking_root = Arc::clone(&root);
    let checking_member = Arc::clone(&nested.member);
    let (checked, checked_receiver) = mpsc::sync_channel(1);
    let membership_reader = std::thread::spawn(move || {
        checked
            .send((
                checking_root.has_resident_child(&checking_member),
                checking_member.record().stage.clone(),
            ))
            .expect("membership observation remains available");
    });
    for _ in 0..2 {
        assert_eq!(
            root_captures
                .recv_timeout(TEST_WAIT)
                .expect("each resident reader captures the parent gate"),
            GateCapture::Observation
        );
    }
    assert_eq!(
        projected_receiver.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    );
    assert_eq!(checked_receiver.try_recv(), Err(mpsc::TryRecvError::Empty));

    drop(held);
    adoption.join().expect("admission completes after handoff");
    assert!(matches!(
        projected_receiver
            .recv_timeout(TEST_WAIT)
            .expect("projection reader completes after admission"),
        (1, MemberStage::Admitted)
    ));
    assert!(matches!(
        checked_receiver
            .recv_timeout(TEST_WAIT)
            .expect("membership reader completes after admission"),
        (true, MemberStage::Admitted)
    ));
    projection_reader
        .join()
        .expect("projection reader completes without panic");
    membership_reader
        .join()
        .expect("membership reader completes without panic");
}

#[test]
fn adopting_a_resident_subtree_rehomes_every_descendant_gate() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = child_scope(&root, "nested", ScopeFlavor::Dynamic);
    let leaf_scope = child_scope(&nested, "leaf-scope", ScopeFlavor::Ordered);
    let leaf_member = child_member(&nested, "leaf-member");
    // Depth 2. The one-level loop in
    // `adopt_descendant_observation_gates_locked` re-homes `leaf_scope`'s
    // own member, and `ScopeCell::observation_gate` reads that member, so
    // a subtree of depth 1 cannot tell the loop from the recursion. Only
    // this grandchild is reachable exclusively through the recursive call.
    let grandchild = child_member(&leaf_scope, "grandchild");
    assert!(
        leaf_scope
            .set_admitted_children(vec![ResidentProjection::new(Arc::clone(&grandchild), None)])
    );
    assert!(nested.set_admitted_children(vec![
        ResidentProjection::new(
            Arc::clone(&leaf_scope.member),
            Some(Arc::clone(&leaf_scope)),
        ),
        ResidentProjection::new(Arc::clone(&leaf_member), None),
    ]));

    assert!(root.admit_child(ResidentProjection::new(
        Arc::clone(&nested.member),
        Some(Arc::clone(&nested)),
    )));

    let root_gate = root.observation_gate();
    for gate in [
        nested.observation_gate(),
        leaf_scope.observation_gate(),
        leaf_member.observation_gate(),
        grandchild.observation_gate(),
    ] {
        assert!(
            root_gate.same_gate(&gate),
            "every descendant joins the adopting parent's observation gate"
        );
    }
    assert!(Arc::ptr_eq(
        &nested.parent().expect("nested parent is installed"),
        &root
    ));
    assert!(Arc::ptr_eq(
        &leaf_scope.parent().expect("leaf parent is installed"),
        &nested
    ));
}

#[test]
fn drain_publication_installs_terminal_disposal_intent_with_the_state() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let child = child_member(&root, "child");
    assert!(root.set_admitted_children(vec![ResidentProjection::new(Arc::clone(&child), None)]));

    root.publish_drain(ScopeState::Draining, None, &[Arc::clone(&child)]);

    assert!(matches!(root.record().state, ScopeState::Draining));
    assert!(child.terminal_or_disposal_pending());
}

#[test]
fn residency_admits_prunes_and_clears_exact_memberships() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = child_scope(&root, "nested", ScopeFlavor::Dynamic);
    let leaf = child_member(&root, "leaf");
    let nested_membership = nested.member.membership();
    let leaf_membership = leaf.membership();
    let mut lifecycle = root.subscribe_lifecycle();

    assert!(root.set_admitted_children(vec![
        ResidentProjection::new(Arc::clone(&nested.member), Some(Arc::clone(&nested))),
        ResidentProjection::new(Arc::clone(&leaf), None),
    ]));
    assert_eq!(root.resident_projections().len(), 2);
    assert!(root.has_resident_child(&nested.member));
    assert!(root.has_resident_child(&leaf));
    assert!(matches!(
        nested.member.record().stage,
        MemberStage::Admitted
    ));
    assert!(matches!(leaf.record().stage, MemberStage::Admitted));

    assert!(root.prune_child(&nested.member));
    assert!(!root.prune_child(&nested.member));
    assert!(!root.has_resident_child(&nested.member));
    assert!(root.has_resident_child(&leaf));
    root.clear_residents();
    assert!(root.resident_projections().is_empty());

    let mut edges = Vec::new();
    while let Ok(LifecycleItem::Event(event)) = lifecycle.try_recv() {
        match event.kind {
            LifecycleEventKind::Added { membership, .. } => {
                edges.push(("added", membership));
            }
            LifecycleEventKind::Removed { membership, .. } => {
                edges.push(("removed", membership));
            }
            _ => panic!("residency mutation emitted an unrelated lifecycle edge"),
        }
    }
    assert_eq!(
        edges,
        [
            ("added", nested_membership),
            ("added", leaf_membership),
            ("removed", nested_membership),
            ("removed", leaf_membership),
        ]
    );
}

#[test]
fn stopped_publication_uses_the_full_precedence_lattice_and_strict_upgrades() {
    let scope = isolated_scope("root", ScopeFlavor::Ordered);
    let mut lifecycle = scope.subscribe_lifecycle();
    let ascending = vec![
        StopReason::Finished,
        StopReason::IntensityTripped(IntensityTrip {
            max_restarts: 1,
            observed_restarts: 2,
            within: Duration::from_secs(1),
        }),
        StopReason::StartupFailed(StartupFailure {
            cause: StartupFailureCause::Lowering {
                undefined: vec![ChildId::from("missing")],
            },
        }),
        StopReason::ShutdownRequested,
        StopReason::NeverStarted,
    ];

    for reason in &ascending {
        scope.with_observation_gate(|txn| {
            scope.publish_stopped_locked(txn, reason.clone(), None, None);
        });
        assert_eq!(
            scope.record().state,
            ScopeState::Stopped {
                reason: reason.clone()
            }
        );
    }
    for reason in ascending.iter().rev() {
        scope.with_observation_gate(|txn| {
            scope.publish_stopped_locked(txn, reason.clone(), None, None);
        });
    }

    let mut published = Vec::new();
    while let Ok(LifecycleItem::Event(event)) = lifecycle.try_recv() {
        if let LifecycleEventKind::ScopeState {
            state: ScopeState::Stopped { reason },
        } = event.kind
        {
            published.push(reason);
        }
    }
    assert_eq!(
        published, ascending,
        "only strict precedence upgrades publish a stopped-state edge"
    );
    assert_eq!(
        scope.record().state,
        ScopeState::Stopped {
            reason: StopReason::NeverStarted
        }
    );
}

#[test]
fn shutdown_and_force_requests_are_exactly_once_and_epoch_scoped() {
    let scope = isolated_scope("root", ScopeFlavor::Ordered);
    let first = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("the first epoch is available");
    assert_eq!(scope.request_shutdown(), first);
    assert!(scope.take_shutdown_request(first));
    assert!(!scope.take_shutdown_request(first));
    scope.force_shutdown(first);
    assert!(scope.take_force_request(first));
    assert!(!scope.take_force_request(first));
    scope.finish_incarnation(first, StopReason::Finished);

    let second = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("the next epoch is available");
    assert_ne!(first, second);
    scope.force_shutdown(first);
    assert!(!scope.take_force_request(first));
    assert!(!scope.take_force_request(second));
    assert_eq!(scope.request_shutdown(), second);
    assert!(!scope.take_shutdown_request(first));
    assert!(scope.take_shutdown_request(second));
    scope.force_shutdown(second);
    assert!(scope.take_force_request(second));
    assert!(!scope.take_force_request(second));
    scope.finish_incarnation(second, StopReason::ShutdownRequested);
}

#[test]
fn a_stale_scope_verdict_disposes_its_nested_exit_off_the_finishing_thread() {
    let finishing_thread = std::thread::current().id();
    let mut identity = ScopeIdentity::new();
    let root_id = ChildId::from("root");
    let root = MemberCell::new(identity.mint_membership(&root_id));
    let scope = ScopeCell::new(root, ScopeFlavor::Ordered, ScopeIdentity::new());
    let child_id = ChildId::from("worker");
    let child = MemberCell::new(identity.mint_membership(&child_id));

    let stale = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("first scope epoch is available");
    scope.finish_incarnation(stale, StopReason::Finished);
    let live = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("second scope epoch is available");

    // The stale epoch is declined, so this structured verdict is never
    // published — but the framework still owns the failed child `Exit` it
    // recursively carries, and must not run that user destructor inline.
    let (dropped, observed) = mpsc::sync_channel(1);
    scope.finish_incarnation(
        stale,
        StopReason::StartupFailed(StartupFailure {
            cause: StartupFailureCause::Child {
                id: child_id,
                membership: child.membership(),
                exit: Exit::failed(
                    ExitError::from(ThreadProbe(dropped)),
                    Cancellation::NotObserved,
                ),
            },
        }),
    );

    let disposal_thread = observed
        .recv_timeout(Duration::from_secs(10))
        .expect("the stale verdict's nested exit is destroyed");
    assert_ne!(
        disposal_thread, finishing_thread,
        "a declined stop reason must not run its nested user destructor on the \
         finishing thread"
    );
    assert_eq!(
        scope.record().state,
        ScopeState::Starting,
        "the stale verdict must not rewrite the newer incarnation"
    );
    scope.finish_incarnation(live, StopReason::Finished);
}

#[test]
fn a_shutdown_winning_the_start_publication_gate_prevents_starting() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let nested = child_scope(&root, "nested", ScopeFlavor::Dynamic);
    assert!(root.admit_child(ResidentProjection::new(
        Arc::clone(&nested.member),
        Some(Arc::clone(&nested)),
    )));
    let incarnation =
        crate::identity::IncarnationCounter::fixture(nested.member.membership()).mint();
    let captures = root.probe_gate_captures();
    let start = root.with_observation_gate(|txn| {
        captures
            .recv_timeout(TEST_WAIT)
            .expect("our transaction captured the gate");
        let starting_root = Arc::clone(&root);
        let starting_child = Arc::clone(&nested);
        let start = std::thread::spawn(move || {
            starting_root.start_scope_child(&starting_child, incarnation, false)
        });
        captures
            .recv_timeout(TEST_WAIT)
            .expect("the start is waiting for publication");
        // Exercise the same shutdown transition as the public request,
        // using the transaction we already hold to order it ahead of start.
        let control = nested.lock_control(ControlPoison::Reject);
        nested.request_shutdown_locked(control, txn, ControlPoison::Reject);
        start
    });
    assert!(!start.join().expect("start decision completes"));
    assert!(matches!(
        nested.member.record().stage,
        MemberStage::Admitted
    ));
    assert!(nested.pending_incarnation_shutdown().is_some());
}

#[test]
fn destructor_shutdown_tolerates_a_poisoned_control_mutex() {
    let id = ChildId::from("root");
    let mut identity = ScopeIdentity::new();
    let member = MemberCell::new(identity.mint_membership(&id));
    let scope = ScopeCell::new(member, ScopeFlavor::Dynamic, ScopeIdentity::new());

    let poison = Arc::clone(&scope);
    assert!(
        catch_unwind(AssertUnwindSafe(move || {
            let _control = poison.control.lock().expect("control starts healthy");
            panic!("inject control poison");
        }))
        .is_err()
    );

    scope.request_shutdown_ignoring_poison();
}

#[test]
fn destructor_shutdown_tolerates_a_poisoned_parent_control_mutex() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let nested = child_scope(&root, "nested", ScopeFlavor::Dynamic);
    assert!(root.admit_child(ResidentProjection::new(
        Arc::clone(&nested.member),
        Some(Arc::clone(&nested)),
    )));
    root.poison_control();

    // No incarnation has begun, so the request targets a pending one and
    // publishes a restart-shutdown event into the parent's control.
    let target = nested.request_shutdown_ignoring_poison();

    root.control.clear_poison();
    assert_eq!(
        root.take_control_events(),
        [ScopeControlEvent::WindowStop {
            membership: nested.member.membership(),
            target,
        }],
        "the tolerant request still reaches the poisoned parent"
    );
}

#[test]
fn destructor_finish_tolerates_a_poisoned_control_mutex() {
    let scope = isolated_scope("root", ScopeFlavor::Ordered);
    let epoch = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("the fixture begins one incarnation");
    scope.poison_control();

    scope.finish_incarnation_ignoring_poison(epoch, StopReason::ShutdownRequested);

    assert!(matches!(
        scope.snapshot().state,
        ScopeState::Stopped {
            reason: StopReason::ShutdownRequested
        }
    ));
    scope.control.clear_poison();
    assert!(
        scope.begin_incarnation(ScopeState::Starting).is_some(),
        "the tolerant finish retired the epoch"
    );
}

#[test]
fn poisoned_finish_bookkeeping_retires_user_inputs_off_thread() {
    let scope = isolated_scope("root", ScopeFlavor::Ordered);
    let epoch = scope
        .begin_incarnation(ScopeState::Starting)
        .expect("the fixture begins one incarnation");
    let poison = Arc::clone(&scope);
    assert!(
        catch_unwind(AssertUnwindSafe(move || {
            let _control = poison.control.lock().expect("control starts healthy");
            panic!("inject control poison");
        }))
        .is_err()
    );

    let retiring_thread = std::thread::current().id();
    let (reason_dropped, reason_observed) = mpsc::sync_channel(1);
    let reason_exit = Exit::failed(
        ExitError::from(ThreadProbe(reason_dropped)),
        Cancellation::NotObserved,
    );
    let reason = StopReason::StartupFailed(StartupFailure {
        cause: StartupFailureCause::Child {
            id: ChildId::from("failed-child"),
            membership: scope.member.membership(),
            exit: reason_exit,
        },
    });
    let (terminal_dropped, terminal_observed) = mpsc::sync_channel(1);
    let terminal_exit = Exit::failed(
        ExitError::from(ThreadProbe(terminal_dropped)),
        Cancellation::NotObserved,
    );

    catch_unwind(AssertUnwindSafe(|| {
        scope.finish_root_incarnation(epoch, reason, terminal_exit);
    }))
    .expect_err("the poisoned control mutex rejects finish bookkeeping");

    for observed in [reason_observed, terminal_observed] {
        assert_ne!(
            observed
                .recv_timeout(TEST_WAIT)
                .expect("failed exit disposal reports"),
            retiring_thread,
            "scope-finish inputs cannot unwind through the observation gate"
        );
    }
}

#[test]
fn mailbox_control_wakes_are_deferred_past_the_observation_gate() {
    let id = ChildId::from("root");
    let mut identity = ScopeIdentity::new();
    let member = MemberCell::new(identity.mint_membership(&id));
    let mut incarnations = member.take_incarnation_counter();
    let incarnation = incarnations.mint();
    let scope = ScopeCell::new(member, ScopeFlavor::Dynamic, ScopeIdentity::new());
    let gate = scope.observation_gate();
    let mailbox = MailboxCell::<u8>::new(id);
    let mut effects = MailboxEffectQueue::default();
    let token = MailboxControl::configure(&*mailbox, ResolvedMailbox::Latest, &mut effects);
    MailboxControl::bind(&*mailbox, token, incarnation, &mut effects);
    drop(effects);
    let mut receiver = MailboxReceiver::new(Arc::clone(&mailbox), incarnation);
    let woke_after_unlock = Arc::new(AtomicBool::new(false));
    let waker = Waker::from(Arc::new(GateCheckingWake {
        gate: gate.clone(),
        woke_after_unlock: Arc::clone(&woke_after_unlock),
    }));
    let mut changed = Box::pin(receiver.changed());
    assert!(matches!(
        changed.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    ));

    scope.with_observation_gate(|txn| {
        MailboxControl::freeze(&*mailbox, incarnation, txn);
    });

    assert!(woke_after_unlock.load(Ordering::SeqCst));
}

#[test]
fn clearing_residents_detaches_the_last_mailbox_owner_after_unlock() {
    let root_id = ChildId::from("root");
    let mut root_identity = ScopeIdentity::new();
    let root_member = MemberCell::new(root_identity.mint_membership(&root_id));
    let scope = ScopeCell::new(root_member, ScopeFlavor::Dynamic, ScopeIdentity::new());
    let gate = scope.observation_gate();

    let child_id = ChildId::from("child");
    let mut child_identity = ScopeIdentity::new();
    let child = MemberCell::new(child_identity.mint_membership(&child_id));
    let mut incarnations = child.take_incarnation_counter();
    let incarnation = incarnations.mint();
    let mailbox = MailboxCell::new(child_id);
    child.attach_mailbox(mailbox.clone());
    let actor = actor_ref_from_parts(Arc::clone(&child), Arc::clone(&mailbox));
    let mut effects = MailboxEffectQueue::default();
    let token = MailboxControl::configure(&*mailbox, ResolvedMailbox::Latest, &mut effects);
    MailboxControl::bind(&*mailbox, token, incarnation, &mut effects);
    drop(effects);
    let (entered, observed) = mpsc::sync_channel(1);
    let (release, release_drop) = mpsc::sync_channel(1);
    actor
        .try_send(GateDropMessage {
            gate,
            entered,
            release: release_drop,
        })
        .expect("bound mailbox accepts the probe");
    assert!(scope.admit_child(ResidentProjection::new(Arc::clone(&child), None)));
    drop(actor);
    drop(mailbox);
    drop(child);

    let (cleared, clear_observed) = mpsc::sync_channel(1);
    let clearing = std::thread::spawn(move || {
        let thread = std::thread::current().id();
        scope.clear_residents();
        cleared
            .send(thread)
            .expect("clear observer remains available");
    });
    let (unlocked, drop_thread) = observed
        .recv_timeout(TEST_WAIT)
        .expect("resident mailbox payload destructor reports");
    let clear_before_release = clear_observed.recv_timeout(Duration::from_millis(100)).ok();
    let returned_before_release = clear_before_release.is_some();
    release
        .send(())
        .expect("the blocking destructor remains parked");
    let clear_thread = clear_before_release.unwrap_or_else(|| {
        clear_observed
            .recv_timeout(TEST_WAIT)
            .expect("resident clearing eventually returns")
    });
    clearing.join().expect("resident clearing thread joins");

    assert!(
        unlocked,
        "the displaced resident owner is released after the gate unlocks"
    );
    assert!(
        returned_before_release,
        "resident clearing must not wait for a blocking user destructor"
    );
    assert_ne!(
        drop_thread, clear_thread,
        "last-owner resident disposal runs on the detached lane"
    );
}

#[test]
fn clearing_residents_contains_every_hostile_payload_destructor() {
    let scope = isolated_scope("root", ScopeFlavor::Dynamic);
    let (entered, observed) = mpsc::sync_channel(2);
    let mut counters = Vec::new();
    for id in ["first", "second"] {
        let child = child_member(&scope, id);
        let mut incarnations = child.take_incarnation_counter();
        let incarnation = incarnations.mint();
        counters.push(incarnations);
        let mailbox = MailboxCell::new(child.id().clone());
        child.attach_mailbox(mailbox.clone());
        let actor = actor_ref_from_parts(Arc::clone(&child), Arc::clone(&mailbox));
        let mut effects = MailboxEffectQueue::default();
        let token = MailboxControl::configure(&*mailbox, ResolvedMailbox::Latest, &mut effects);
        MailboxControl::bind(&*mailbox, token, incarnation, &mut effects);
        drop(effects);
        actor
            .try_send(HostileDropMessage {
                entered: entered.clone(),
                id,
            })
            .expect("bound mailbox accepts the hostile payload");
        assert!(scope.admit_child(ResidentProjection::new(Arc::clone(&child), None)));
        drop(actor);
        drop(mailbox);
        drop(child);
    }
    drop(entered);

    scope.clear_residents();

    // Park behind a job queued after the displaced set. Without a
    // per-resident boundary the second destructor panics inside the first
    // one's unwind through `Vec`'s slice drop glue, and the disposal
    // worker aborts the process before this sentinel can run -- the
    // sequencing is what makes the regression deterministic rather than a
    // race against the test's own return.
    let (sentinel, sequenced) = mpsc::sync_channel(1);
    runtime::dispose_detached(ThreadProbe(sentinel));
    sequenced
        .recv_timeout(TEST_WAIT)
        .expect("the disposal worker survives every hostile resident");

    let mut reported = Vec::new();
    for _ in 0..2 {
        reported.push(
            observed
                .recv_timeout(TEST_WAIT)
                .expect("every hostile resident destructor runs"),
        );
    }
    reported.sort_unstable();
    assert_eq!(reported, ["first", "second"]);
}
