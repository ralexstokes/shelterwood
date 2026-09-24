use std::{
    sync::{
        Arc, Mutex, OnceLock, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use crate::{mailbox::MailboxControl, runtime};
use shelterwood_core::{
    ChildId, Exit, Incarnation, Membership, RestartCount,
    engine::MembershipStatus,
    identity::{IncarnationCounter, MintedMembership, ProvisionalMembership},
    policy::ResolvedCommonOptions,
};

use super::{Guarded, ObservationGate, ObservationTxn, RetainGuards, Retained};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MemberStage {
    Reserved,
    Admitted,
    Starting,
    Running,
    Restarting,
    Stopping,
    Terminal(Exit),
}

/// One non-terminal member-record transition owned by the cell layer.
pub(crate) enum MemberTransition {
    Admitted,
    Starting {
        incarnation: Incarnation,
    },
    Running,
    Stopping,
    RestartScheduled {
        exit: Exit,
        restart_count: RestartCount,
        restart_at: Option<Instant>,
    },
}

impl MemberTransition {
    /// The legality matrix for driver-requested stage transitions.
    ///
    /// This is the single definition of which source stages each event may
    /// consume. [`MemberRecord::apply_transition`] enforces it, and
    /// [`MemberCell::would_accept`] probes it ahead of an operation that must
    /// commit other state before the transition itself lands.
    fn is_legal_from(&self, stage: &MemberStage) -> bool {
        matches!(
            (stage, self),
            (MemberStage::Reserved, MemberTransition::Admitted)
                | (
                    MemberStage::Admitted | MemberStage::Restarting,
                    MemberTransition::Starting { .. }
                )
                | (
                    MemberStage::Starting | MemberStage::Reserved,
                    MemberTransition::Running
                )
                | (
                    MemberStage::Starting | MemberStage::Running,
                    MemberTransition::Stopping
                )
                | (
                    MemberStage::Starting | MemberStage::Running | MemberStage::Stopping,
                    MemberTransition::RestartScheduled { .. }
                )
        )
    }
}

/// Whether a terminal child incarnation failed during aggregate startup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartupDisposition {
    /// Terminalization is outside the supervised startup decision.
    Unchanged,
    /// The supervised exit did not abort aggregate startup.
    NotAborted,
    /// The supervised exit aborted aggregate startup.
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MemberRecord {
    pub stage: MemberStage,
    pub incarnation: Option<Incarnation>,
    pub last_incarnation: Option<Incarnation>,
    pub last_exit: Option<Exit>,
    pub restart_count: RestartCount,
    pub restart_at: Option<Instant>,
    pub membership_status: MembershipStatus,
    pub startup_aborted: bool,
}

impl RetainGuards for MemberRecord {
    fn retain_guards(&self, guards: &mut Vec<Retained<Exit>>) {
        if let MemberStage::Terminal(exit) = &self.stage {
            exit.retain_guards(guards);
        }
        self.last_exit.retain_guards(guards);
    }
}

impl MemberRecord {
    /// Applies one driver-requested transition.
    ///
    /// Every watch-channel writer routes stage changes through here (see
    /// [`MemberCell::transition`] for the wake-bus contract). The reducer
    /// rejects an event whose source stage is not one its driver call sites
    /// can present, including in release builds.
    ///
    /// Exits are safe to retire inside the watch mutation: it runs under
    /// [`Guarded::update`], whose previous guard set still covers the
    /// displaced value.
    fn apply_transition(&mut self, transition: MemberTransition) -> Result<(), MemberTransition> {
        if !transition.is_legal_from(&self.stage) {
            return Err(transition);
        }

        match transition {
            MemberTransition::Admitted => {
                self.stage = MemberStage::Admitted;
            }
            MemberTransition::Starting { incarnation } => {
                self.stage = MemberStage::Starting;
                self.incarnation = Some(incarnation);
                self.last_incarnation = Some(incarnation);
                self.restart_at = None;
            }
            MemberTransition::Running => {
                self.stage = MemberStage::Running;
            }
            MemberTransition::Stopping => {
                self.stage = MemberStage::Stopping;
            }
            MemberTransition::RestartScheduled {
                exit,
                restart_count,
                restart_at,
            } => {
                self.stage = MemberStage::Restarting;
                self.incarnation = None;
                self.last_exit = Some(exit);
                self.restart_count = restart_count;
                self.restart_at = restart_at;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct MemberCell {
    id: ChildId,
    membership: Membership,
    rebased_membership: OnceLock<Membership>,
    provisional_membership: Mutex<Option<ProvisionalMembership>>,
    incarnations: Mutex<Option<IncarnationCounter>>,
    pub(super) record: runtime::WatchSender<Guarded<MemberRecord>>,
    // Guards only a gate-pointer swap, so no torn state is possible; every
    // access deliberately tolerates poisoning (mirroring
    // `ObservationGate::lock`) so drop-path shutdown after a panicked assert
    // cannot itself panic.
    pub(super) observation_gate: RwLock<ObservationGate>,
    terminal_disposal_pending: AtomicBool,
    mailbox: Mutex<MemberMailbox>,
    // Lowering resolves this before residency in both production routes:
    // `ChildPlan::with_options` runs ahead of the planned
    // `set_admitted_children` and ahead of the dynamic `admit_child_locked`.
    // The enforcement point is snapshot construction rather than admission —
    // that is the only read, and admitting an unresolved member is a useful
    // fixture shape — so a missing value surfaces there as an internal
    // admission-order bug.
    options: OnceLock<ResolvedCommonOptions>,
}

#[derive(Debug, Default)]
enum MemberMailbox {
    #[default]
    Unattached,
    Attached(Arc<dyn MailboxControl>),
    Terminal {
        control: Option<Arc<dyn MailboxControl>>,
    },
}

/// Queues the committed record edge after mailbox discharge, including when
/// preparation unwinds. The transaction still owns the post-unlock wake.
struct TerminalRecordPulse<'a, 'gate> {
    txn: &'a mut ObservationTxn<'gate>,
    record: Option<&'a runtime::WatchSender<Guarded<MemberRecord>>>,
}

impl Drop for TerminalRecordPulse<'_, '_> {
    fn drop(&mut self) {
        if let Some(record) = self.record {
            self.txn.pulse(record);
        }
    }
}

impl MemberCell {
    // The id is read from the grant rather than accepted beside it: the
    // reconciliation token carries the id its lineage was minted for, so a
    // member whose `id()` disagrees with the id its membership was minted
    // under — and would therefore be adopted under — is unconstructible.
    pub(crate) fn new(identity: MintedMembership) -> Arc<Self> {
        let id = identity.id().clone();
        let (membership, provisional_membership, incarnations) = identity.into_provisional_parts();
        let (record, _) = runtime::watch(Guarded::new(MemberRecord {
            stage: MemberStage::Reserved,
            incarnation: None,
            last_incarnation: None,
            last_exit: None,
            restart_count: RestartCount::ZERO,
            restart_at: None,
            membership_status: MembershipStatus::Active,
            startup_aborted: false,
        }));
        Arc::new(Self {
            id,
            membership,
            rebased_membership: OnceLock::new(),
            provisional_membership: Mutex::new(Some(provisional_membership)),
            incarnations: Mutex::new(Some(incarnations)),
            record,
            observation_gate: RwLock::new(ObservationGate::new()),
            terminal_disposal_pending: AtomicBool::new(false),
            mailbox: Mutex::new(MemberMailbox::default()),
            options: OnceLock::new(),
        })
    }

    pub(crate) fn id(&self) -> &ChildId {
        &self.id
    }

    pub(crate) fn membership(&self) -> Membership {
        self.rebased_membership
            .get()
            .copied()
            .unwrap_or(self.membership)
    }

    pub(crate) fn take_provisional_membership(&self) -> ProvisionalMembership {
        // Two statements, not one chain: the guard must be released before
        // the verdict can panic, or the failing assertion would poison the
        // mutex for every later caller.
        let provisional = self
            .provisional_membership
            .lock()
            .expect("provisional membership mutex poisoned")
            .take();
        provisional.expect("a declaration membership can be reconciled at most once")
    }

    pub(crate) fn rebase_membership(&self, identity: MintedMembership) {
        let (membership, incarnations) = identity.into_pair();
        let record = self.record();
        assert!(
            matches!(record.stage, MemberStage::Reserved)
                && record.incarnation.is_none()
                && record.last_incarnation.is_none(),
            "only an unstarted reservation can be rebased"
        );
        self.rebased_membership
            .set(membership)
            .expect("a reservation can be rebased at most once");
        *self
            .incarnations
            .lock()
            .expect("incarnation counter mutex poisoned") = Some(incarnations);
    }

    pub(crate) fn take_incarnation_counter(&self) -> IncarnationCounter {
        self.incarnations
            .lock()
            .expect("incarnation counter mutex poisoned")
            .take()
            .expect("a membership's incarnation counter is issued to one runtime")
    }

    #[cfg(test)]
    pub(crate) fn lock_incarnation_counter(
        &self,
    ) -> std::sync::MutexGuard<'_, Option<IncarnationCounter>> {
        self.incarnations
            .lock()
            .expect("incarnation counter mutex starts healthy")
    }

    pub(crate) fn record(&self) -> Guarded<MemberRecord> {
        self.record.read_cloned()
    }

    #[cfg(test)]
    pub(crate) fn record_watcher(&self) -> runtime::WatchReceiver<Guarded<MemberRecord>> {
        self.record.watcher()
    }

    /// Reports terminality or drain-entry terminal-disposal intent.
    ///
    /// `Acquire` pairs with the `Release` store in
    /// [`Self::set_terminal_disposal_pending`]. Two separate edges make a
    /// zero-budget straggler sample (the driver's `collect_stragglers`)
    /// correct. [`ScopeCell::resident_projections`] serializes the
    /// residency clone with `publish_drain` through the observation gate, but
    /// releases that gate before the caller samples this marker and the member
    /// record. Those following reads therefore still rest on the explicit
    /// ordering below rather than incidental mutex overlap.
    ///
    /// This method owns the marker-before-record order so a caller cannot
    /// accidentally recreate the trailing gap by reversing two independent
    /// reads.
    ///
    /// * A marker load that observes the *clear* synchronizes with the store the
    ///   driver issues after `publish_terminal` published
    ///   `MemberStage::Terminal`, so the sampler's following record read is
    ///   ordered after that publication. It cannot pair a stale nonterminal
    ///   projection with an already-cleared marker.
    /// * A load cannot return a stale `false` from *before* the marker was
    ///   set. A sampler reaches this call only after reading
    ///   `ScopeState::Draining` out of the scope record, and
    ///   [`Self::set_terminal_disposal_pending`] stores every drain-entry
    ///   marker *before* `publish_drain` writes that record. The record
    ///   write/read pair supplies happens-before, and coherence then forbids
    ///   this load from returning a value preceding `true` in the marker's
    ///   modification order.
    pub(crate) fn terminal_or_disposal_pending(&self) -> bool {
        self.terminal_disposal_pending.load(Ordering::Acquire)
            || matches!(self.record().stage, MemberStage::Terminal(_))
    }

    /// Installs or clears the drain-entry terminal-disposal marker.
    ///
    /// `Release` publishes everything the caller sequenced before it. The
    /// clearing store at terminal publication therefore carries
    /// `MemberStage::Terminal` to any sampler whose `Acquire` load reads it;
    /// the setting store is made visible by the `Draining` record write that
    /// `publish_drain` performs afterwards. See
    /// [`Self::terminal_or_disposal_pending`] for the full argument.
    pub(crate) fn set_terminal_disposal_pending(&self, pending: bool) {
        self.terminal_disposal_pending
            .store(pending, Ordering::Release);
    }

    /// Mutates a member record and pulses the watch channel.
    ///
    /// Test-only escape hatch around [`Self::transition`]; the wake-bus
    /// contract documented there binds this path too.
    #[cfg(test)]
    pub(crate) fn update(&self, update: impl FnOnce(&mut MemberRecord)) {
        self.with_observation_txn(|txn| self.update_locked(txn, update));
    }

    /// Applies a member transition and pulses the watch channel.
    ///
    /// The driver also treats this channel as its control-plane wake bus: any
    /// field read by a loop precondition must be changed through a pulsing path
    /// like this one, never by a silent write outside an observation gate.
    ///
    /// Returns whether the reducer accepted the event; see
    /// [`Self::transition_locked`].
    #[cfg(test)]
    #[must_use = "an illegal member transition is rejected, not applied"]
    pub(crate) fn transition(&self, transition: MemberTransition) -> bool {
        self.with_observation_txn(|txn| self.transition_locked(txn, transition))
    }

    fn with_observation_txn<R>(&self, operation: impl FnOnce(&mut ObservationTxn<'_>) -> R) -> R {
        self.with_observation_txn_probed(|| {}, operation)
    }

    pub(super) fn with_observation_txn_probed<R>(
        &self,
        mut report_capture: impl FnMut(),
        operation: impl FnOnce(&mut ObservationTxn<'_>) -> R,
    ) -> R {
        loop {
            let gate = self.current_observation_gate();
            report_capture();
            let guard = gate.lock();
            if gate.shares_gate(&self.current_observation_gate()) {
                let mut txn = ObservationTxn::new(&gate, guard);
                return operation(&mut txn);
            }
            drop(guard);
        }
    }

    pub(super) fn current_observation_gate(&self) -> ObservationGate {
        self.observation_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[cfg(test)]
    pub(crate) fn observation_gate(&self) -> ObservationGate {
        self.current_observation_gate()
    }

    pub(super) fn install_observation_gate_locked(
        &self,
        previous: &ObservationGate,
        gate: &ObservationGate,
    ) {
        let mut installed = self
            .observation_gate
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if installed.shares_gate(previous) {
            *installed = gate.clone();
        } else {
            assert!(
                installed.shares_gate(gate),
                "a resident member must share its tree observation gate"
            );
        }
    }

    pub(super) fn adopt_observation_gate(
        &self,
        gate: &ObservationGate,
        _txn: &mut ObservationTxn<'_>,
    ) {
        self.adopt_observation_gate_with(
            gate,
            || {},
            |previous| {
                self.install_observation_gate_locked(previous, gate);
            },
        );
    }

    /// Adopts `gate` unconditionally, running `install` when it differs from
    /// this member's current gate.
    pub(super) fn adopt_observation_gate_with(
        &self,
        gate: &ObservationGate,
        report_capture: impl FnMut(),
        install: impl FnOnce(&ObservationGate),
    ) {
        self.with_handoff_gate(gate, report_capture, |current| {
            if !current.shares_gate(gate) {
                install(current);
            }
            true
        });
    }

    /// Runs `attempt` with this member's current observation gate held.
    ///
    /// The caller already holds the destination `gate`. When the member is
    /// already on it, `attempt` runs directly under the caller's guard.
    /// Otherwise this takes the member's current gate in the one permitted
    /// parent-to-child direction and holds it across `attempt`, so validation,
    /// record mutation and the recursive handoff form a single cut; `attempt`
    /// owns the decision to install `gate` and returns whether it accepted.
    ///
    /// An operation that passed the pointer check may finish its complete edge
    /// before handoff. One that merely captured an obsolete gate retries after
    /// acquiring it and observing the replacement, so `report_capture` fires
    /// once per differing-gate iteration.
    pub(super) fn with_handoff_gate(
        &self,
        gate: &ObservationGate,
        mut report_capture: impl FnMut(),
        attempt: impl FnOnce(&ObservationGate) -> bool,
    ) -> bool {
        loop {
            let current = self.current_observation_gate();
            if current.shares_gate(gate) {
                return attempt(&current);
            }
            report_capture();
            let current_guard = current.lock();
            if current.shares_gate(&self.current_observation_gate()) {
                let accepted = attempt(&current);
                drop(current_guard);
                return accepted;
            }
            drop(current_guard);
        }
    }

    pub(super) fn update_locked(
        &self,
        txn: &mut ObservationTxn<'_>,
        update: impl FnOnce(&mut MemberRecord),
    ) {
        #[cfg(debug_assertions)]
        txn.debug_assert_gate(&self.current_observation_gate());
        // Guarding here rather than in each writer keeps the guard-set
        // invariant on every record mutation, including the test-only escape
        // hatch that writes fields directly.
        self.record
            .modify_silently(|record| record.update(txn, update));
        txn.pulse(&self.record);
    }

    /// Probes the legality matrix without mutating anything.
    ///
    /// The stage can only change through a gated writer, so a caller holding
    /// this member's observation gate across the probe and the matching
    /// [`Self::transition_locked`] sees no window between them. That lets an
    /// operation which must commit other state first refuse before it starts.
    pub(super) fn would_accept(&self, transition: &MemberTransition) -> bool {
        self.record
            .read_with(|record| transition.is_legal_from(&record.stage))
    }

    /// Applies `transition` and returns whether the reducer accepted it.
    ///
    /// A rejection is a total no-op *within the cell layer*: no record field
    /// changes, no watch version advances, and no lifecycle event is emitted.
    /// It says nothing about state the caller committed before calling — see
    /// the driver call sites, each of which asserts legality in debug builds.
    #[must_use = "an illegal member transition is rejected, not applied"]
    pub(crate) fn transition_locked(
        &self,
        txn: &mut ObservationTxn<'_>,
        transition: MemberTransition,
    ) -> bool {
        #[cfg(debug_assertions)]
        txn.debug_assert_gate(&self.current_observation_gate());
        // `RestartScheduled` carries a user error by value. Retain it before
        // the watch lock and reducer validation so an unrelated panic can
        // unwind the raw transition as refcount traffic only.
        let retained_exit = match &transition {
            MemberTransition::RestartScheduled { exit, .. } => Some(Retained::new(exit.clone())),
            MemberTransition::Admitted
            | MemberTransition::Starting { .. }
            | MemberTransition::Running
            | MemberTransition::Stopping => None,
        };
        let mut rejected = None;
        self.record.modify_silently(|record| {
            rejected = record
                .update(txn, |record| record.apply_transition(transition))
                .err();
        });
        if let Some(rejected) = rejected {
            // Rejection leaves this cell exactly as it was. The rejected event
            // may own a failed exit, so retire it with the transaction rather
            // than under the observation gate or watch lock.
            if let Some(retained_exit) = retained_exit {
                // The deferred transition proves this clone cannot be last.
                txn.surrender([retained_exit]);
            }
            txn.defer(move || runtime::dispose_detached(rejected));
            return false;
        }
        txn.pulse(&self.record);
        if let Some(retained_exit) = retained_exit {
            // The record now owns an equivalent retained copy.
            txn.surrender([retained_exit]);
        }
        true
    }

    pub(crate) fn set_options(&self, options: ResolvedCommonOptions) {
        self.options
            .set(options)
            .expect("member options are resolved exactly once");
    }

    pub(super) fn options(&self) -> ResolvedCommonOptions {
        self.options
            .get()
            .cloned()
            .expect("resident member options are resolved before snapshot publication")
    }

    pub(crate) fn attach_mailbox(&self, mailbox: Arc<dyn MailboxControl>) {
        self.with_observation_txn(|txn| {
            let mut rejected = None;
            let terminal_teardown = {
                let mut state = self.mailbox.lock().expect("member mailbox mutex poisoned");
                match &mut *state {
                    MemberMailbox::Unattached => {
                        *state = MemberMailbox::Attached(mailbox);
                        None
                    }
                    MemberMailbox::Attached(_)
                    | MemberMailbox::Terminal {
                        control: Some(_), ..
                    } => {
                        rejected = Some(mailbox);
                        None
                    }
                    MemberMailbox::Terminal { control, .. } => {
                        let drained = mailbox.prepare_termination(txn);
                        *control = Some(mailbox);
                        drained
                    }
                }
            };
            // Raise the contract failure as the first post-unlock effect so
            // it keeps precedence over a hostile rejected-mailbox destructor.
            // The transaction accumulator still runs the disposal effect
            // before it resumes that panic.
            if let Some(rejected) = rejected {
                txn.defer(|| panic!("a member can own only one mailbox"));
                txn.defer(move || runtime::dispose_detached(rejected));
                return;
            }
            if let Some(teardown) = terminal_teardown {
                txn.defer(move || {
                    runtime::dispose_detached(teardown.finish());
                });
            }
        });
    }

    pub(crate) fn mailbox(&self) -> Option<Arc<dyn MailboxControl>> {
        match &*self.mailbox.lock().expect("member mailbox mutex poisoned") {
            MemberMailbox::Unattached => None,
            MemberMailbox::Attached(control) => Some(Arc::clone(control)),
            MemberMailbox::Terminal { control, .. } => control.clone(),
        }
    }

    pub(crate) fn terminalize(&self, exit: Exit, startup: StartupDisposition) {
        self.with_observation_txn(|txn| {
            self.terminalize_locked(exit, startup, txn);
        });
    }

    pub(crate) fn terminalize_locked(
        &self,
        exit: Exit,
        startup: StartupDisposition,
        txn: &mut ObservationTxn<'_>,
    ) -> Exit {
        #[cfg(debug_assertions)]
        txn.debug_assert_gate(&self.current_observation_gate());
        let (lost, attached) = {
            let mut state = self.mailbox.lock().expect("member mailbox mutex poisoned");
            match &mut *state {
                MemberMailbox::Terminal { .. } => (true, None),
                MemberMailbox::Unattached => {
                    *state = MemberMailbox::Terminal { control: None };
                    (false, None)
                }
                MemberMailbox::Attached(control) => {
                    // Mark the mailbox terminal here, but leave it live: every
                    // writer of this state holds the observation gate, so no
                    // other terminalizer or attach can observe the window
                    // before `prepare_termination` below.
                    let control = Arc::clone(control);
                    *state = MemberMailbox::Terminal {
                        control: Some(Arc::clone(&control)),
                    };
                    (false, Some(control))
                }
            }
        };
        if lost {
            // The winner stored its exit in the record in the same gated
            // section that made the mailbox terminal.
            let winner = self.record.read_with(|record| match &record.stage {
                MemberStage::Terminal(exit) => Some(exit.clone()),
                _ => None,
            });
            debug_assert!(
                winner.is_some(),
                "a terminal mailbox implies a terminal record"
            );
            if let Some(winner) = winner {
                // The losing exit owns a type-erased user error whose
                // destructor may block, panic, or re-enter observation, so it
                // retires through the critical-disposal lane after unlock.
                let losing_exit = Retained::new(exit);
                txn.defer(move || drop(losing_exit));
                return winner;
            }
        }
        let terminal_exit = exit;
        let mut published = false;
        self.record.modify_silently(|record| {
            record.update(txn, |record| {
                if matches!(record.stage, MemberStage::Terminal(_)) {
                    return;
                }
                match startup {
                    StartupDisposition::Unchanged => {}
                    StartupDisposition::NotAborted => record.startup_aborted = false,
                    StartupDisposition::Aborted => record.startup_aborted = true,
                }
                record.incarnation = None;
                record.restart_at = None;
                record.last_exit = Some(terminal_exit.clone());
                record.stage = MemberStage::Terminal(terminal_exit.clone());
                published = true;
            });
        });
        // First terminalizer wins: only a newly stored terminal record owes
        // a pulse. Own that obligation before fallible mailbox preparation.
        let notification = TerminalRecordPulse {
            txn,
            record: published.then_some(&self.record),
        };
        let txn = &mut *notification.txn;
        // SPEC §3.2: store the terminal record, then terminalize the mailbox.
        // The phase flip inside `prepare_termination` is what makes a new
        // send fail `Terminated`, so a sender holding that verdict reads the
        // terminal record, and reentrant mailbox wakers observe the winning
        // exit. Parked operations are discharged by the deferred `finish`,
        // and notification-driven readers still see discharge-before-pulse;
        // tree-scoped publication defers both until the complete observation
        // transaction has released its gate. The member mailbox guard is not
        // needed here: the gate already serializes every writer of it.
        if let Some(teardown) = attached.and_then(|control| control.prepare_termination(txn)) {
            txn.defer(move || {
                runtime::dispose_detached(teardown.finish());
            });
        }
        terminal_exit
    }

    pub(crate) async fn wait_terminal(&self) -> Exit {
        let mut watcher = self.record.watcher();
        loop {
            if let MemberStage::Terminal(exit) = &watcher.borrow_cloned().stage {
                return exit.clone();
            }
            watcher.changed().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::mpsc,
        task::{Context, Waker},
        time::{Duration, Instant},
    };

    use crate::{
        SendErrorKind,
        mailbox::{MailboxCell, MailboxControl, actor_ref_from_parts},
    };
    use shelterwood_core::{Cancellation, ExitError, identity::ScopeIdentity, policy::ScopeFlavor};

    use super::*;
    use crate::cells::test_support::{ThreadProbe, isolated_scope};

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "a locked observation writer requires its current tree gate")]
    fn locked_member_writer_checks_transaction_gate_identity() {
        let scope = isolated_scope("root", ScopeFlavor::Ordered);
        let wrong_gate = ObservationGate::new();
        let mut txn = ObservationTxn::new(&wrong_gate, wrong_gate.lock());

        scope.member.update_locked(&mut txn, |_| {});
    }

    #[test]
    fn illegal_restart_transition_is_rejected_in_every_build() {
        let scope = isolated_scope("root", ScopeFlavor::Ordered);
        let mut watcher = scope.member.record_watcher();
        let (dropped, observed) = mpsc::sync_channel(1);
        let retiring_thread = std::thread::current().id();
        let exit = Exit::failed(
            ExitError::from(ThreadProbe(dropped)),
            Cancellation::NotObserved,
        );

        assert!(
            !scope.member.transition(MemberTransition::RestartScheduled {
                exit,
                restart_count: RestartCount::ZERO.bump(),
                restart_at: None,
            }),
            "a reserved member cannot schedule a restart"
        );
        assert!(matches!(scope.member.record().stage, MemberStage::Reserved));
        let mut changed = Box::pin(watcher.changed());
        assert!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending(),
            "a rejected transition publishes no record edge"
        );

        assert_ne!(
            observed
                .recv_timeout(Duration::from_secs(10))
                .expect("failed exit disposal reports"),
            retiring_thread,
            "a rejected user error retires on the detached lane"
        );
    }

    #[test]
    fn attaching_a_second_mailbox_panics_without_replacing_or_poisoning_the_first() {
        let scope = isolated_scope("root", ScopeFlavor::Ordered);
        let first = MailboxCell::<u8>::new(scope.member.id().clone());
        let second = MailboxCell::<u8>::new(scope.member.id().clone());
        let first_control: Arc<dyn MailboxControl> = first;
        let second_control: Arc<dyn MailboxControl> = second;
        scope.member.attach_mailbox(Arc::clone(&first_control));

        let payload = catch_unwind(AssertUnwindSafe(|| {
            scope.member.attach_mailbox(second_control);
        }))
        .expect_err("a member rejects a second mailbox");
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"a member can own only one mailbox")
        );
        assert!(Arc::ptr_eq(
            &scope
                .member
                .mailbox()
                .expect("the first mailbox remains attached"),
            &first_control
        ));
        scope.member.terminalize(
            Exit::completed(Cancellation::NotObserved),
            StartupDisposition::Unchanged,
        );
        assert!(
            scope.member.mailbox().is_some(),
            "the rejected attach did not poison the mailbox mutex"
        );
    }

    /// Attaching to a terminal member drains the mailbox without a record
    /// edge. The teardown half is pinned by
    /// `attach_to_a_terminal_member_finishes_record_before_mailbox_wake`.
    #[test]
    fn attaching_to_a_terminal_member_does_not_republish_its_record() {
        let scope = isolated_scope("root", ScopeFlavor::Ordered);
        scope.member.terminalize(
            Exit::completed(Cancellation::NotObserved),
            StartupDisposition::Unchanged,
        );
        let mut watcher = scope.member.record_watcher();
        watcher.borrow_and_update_cloned();
        let mailbox = MailboxCell::<u8>::new(scope.member.id().clone());

        scope.member.attach_mailbox(mailbox);

        let mut changed = Box::pin(watcher.changed());
        assert!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending(),
            "attaching to an already-terminal member publishes no second record edge"
        );
    }

    #[test]
    fn losing_terminalizer_returns_the_winner_without_republishing_or_reclassifying() {
        let scope = isolated_scope("root", ScopeFlavor::Ordered);
        let member = &scope.member;
        let winner = Exit::completed(Cancellation::NotObserved);
        member.terminalize(winner.clone(), StartupDisposition::NotAborted);
        let mut watcher = member.record_watcher();
        let winning_record = watcher.borrow_and_update_cloned();
        let losing = Exit::failed(ExitError::message("late failure"), Cancellation::Observed);

        let returned = member.with_observation_txn(|txn| {
            member.terminalize_locked(losing, StartupDisposition::Aborted, txn)
        });

        assert_eq!(returned, winner);
        assert_eq!(member.record(), winning_record);
        assert!(!member.record().startup_aborted);
        let mut changed = Box::pin(watcher.changed());
        assert!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending(),
            "a losing terminalizer publishes no second record edge"
        );
    }

    #[test]
    fn terminal_sample_accepts_pending_disposal_or_a_terminal_record() {
        let scope = isolated_scope("root", ScopeFlavor::Ordered);
        let member = &scope.member;
        assert!(!member.terminal_or_disposal_pending());

        member.set_terminal_disposal_pending(true);
        assert!(member.terminal_or_disposal_pending());

        member.set_terminal_disposal_pending(false);
        assert!(!member.terminal_or_disposal_pending());
        member.terminalize(
            Exit::completed(Cancellation::NotObserved),
            StartupDisposition::Unchanged,
        );
        assert!(member.terminal_or_disposal_pending());
    }

    #[test]
    fn a_losing_failed_terminal_exit_disposes_off_the_retiring_thread() {
        let retiring_thread = std::thread::current().id();
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let member = MemberCell::new(identity.mint_membership(&id));
        member.terminalize(
            Exit::completed(Cancellation::NotObserved),
            StartupDisposition::Unchanged,
        );
        let (dropped, observed) = mpsc::sync_channel(1);

        member.terminalize(
            Exit::failed(
                ExitError::from(ThreadProbe(dropped)),
                Cancellation::NotObserved,
            ),
            StartupDisposition::Unchanged,
        );

        let disposal_thread = observed
            .recv_timeout(Duration::from_secs(10))
            .expect("losing exit disposal completes");
        assert_ne!(
            disposal_thread, retiring_thread,
            "a losing failed exit must not run its user destructor on the committing thread"
        );
    }

    #[test]
    fn member_record_reads_share_one_guard_set_and_the_last_one_isolates() {
        let reading_thread = std::thread::current().id();
        let (dropped, observed) = mpsc::sync_channel(1);
        let id = ChildId::from("worker");
        let mut identity = ScopeIdentity::new();
        let member = MemberCell::new(identity.mint_membership(&id));
        member.terminalize(
            Exit::failed(
                ExitError::from(ThreadProbe(dropped)),
                Cancellation::NotObserved,
            ),
            StartupDisposition::Unchanged,
        );

        let first = member.record();
        let second = member.record();
        assert!(
            first.guard_count() > 0,
            "a terminal failed member retains its exit"
        );
        assert!(
            first.shares_guards_with(&second),
            "record reads must share one guard allocation instead of submitting \
             one disposal job per retained exit per read"
        );
        // Reading a record is refcount traffic all the way through: these
        // clones retire without any of them owning the last guard.
        drop(first);
        drop(second);
        assert_eq!(
            observed.try_recv(),
            Err(mpsc::TryRecvError::Empty),
            "a record read must not destroy the member's exit payload"
        );

        drop(member);

        assert_ne!(
            observed
                .recv_timeout(Duration::from_secs(10))
                .expect("the last guard set disposes the payload"),
            reading_thread,
            "the member record's final guard must isolate its failed payload"
        );
    }

    /// SPEC §3.2: terminal publication stores the cell record before it
    /// discharges the mailbox, so a sender holding a `Terminated` verdict can
    /// never read a nonterminal record. Holding the record's value guard
    /// stalls the terminalizer at exactly its record store; whatever the
    /// sender surface reports during that stall was published before the
    /// store. Wait for the terminalizer's mailbox-state transition before
    /// checking the verdict; a scheduling timeout fails instead of passing.
    #[test]
    fn a_terminated_send_verdict_is_never_observed_before_the_terminal_record() {
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let member = MemberCell::new(identity.mint_membership(&id));
        let mailbox = MailboxCell::<u8>::new(member.id().clone());
        let actor = actor_ref_from_parts(Arc::clone(&member), Arc::clone(&mailbox));
        member.attach_mailbox(mailbox);

        // Only plain evidence leaves the guarded section; every verdict is
        // judged below, after the watch guard has been released.
        let (transitioned, verdict, stage_under_guard, terminalizer) =
            member.record.read_with(|record| {
                let terminalizer = {
                    let member = Arc::clone(&member);
                    std::thread::spawn(move || {
                        member.terminalize(Exit::never_started(), StartupDisposition::Unchanged);
                    })
                };
                let deadline = Instant::now() + Duration::from_secs(10);
                let transitioned = loop {
                    let terminal = matches!(
                        *member.mailbox.lock().expect("member mailbox mutex healthy"),
                        MemberMailbox::Terminal { .. }
                    );
                    if terminal || Instant::now() >= deadline {
                        break terminal;
                    }
                    std::thread::yield_now();
                };
                let verdict = actor.try_send(1).map_err(|error| error.kind);
                (transitioned, verdict, record.stage.clone(), terminalizer)
            });
        terminalizer.join().expect("terminalizer thread succeeds");

        assert!(
            transitioned,
            "terminalizer must reach the held record guard"
        );
        assert_eq!(stage_under_guard, MemberStage::Reserved);
        assert_eq!(
            verdict,
            Err(SendErrorKind::NotRunning),
            "a sender observed `Terminated` while the member record was still {stage_under_guard:?}"
        );
        assert_eq!(
            actor.try_send(1).map_err(|error| error.kind),
            Err(SendErrorKind::Terminated)
        );
        assert_eq!(
            member.record().stage,
            MemberStage::Terminal(Exit::never_started())
        );
    }
}
