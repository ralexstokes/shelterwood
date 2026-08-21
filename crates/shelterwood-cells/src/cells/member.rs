use std::{
    fmt,
    sync::{
        Arc, Mutex, OnceLock, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use shelterwood_core::{
    ChildId, Exit, ExitKind, Incarnation, Membership, RestartCount,
    engine::MembershipStatus,
    identity::{IncarnationCounter, MintedMembership},
    policy::ResolvedCommonOptions,
};
use shelterwood_mailbox::{ActorIdentity, MailboxControl, MailboxTermination};
use shelterwood_runtime as runtime;

use super::{ObservationGate, ObservationTxn, RetainedExit};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MemberStage {
    Reserved,
    Admitted,
    Starting,
    Running,
    Restarting,
    Stopping,
    Terminal(Exit),
}

/// One non-terminal member-record transition owned by the cell layer.
pub enum MemberTransition {
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
pub enum StartupDisposition {
    /// Terminalization is outside the supervised startup decision.
    Unchanged,
    /// The supervised exit did not abort aggregate startup.
    NotAborted,
    /// The supervised exit aborted aggregate startup.
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberRecord {
    pub stage: MemberStage,
    pub incarnation: Option<Incarnation>,
    pub last_incarnation: Option<Incarnation>,
    pub last_exit: Option<Exit>,
    pub restart_count: RestartCount,
    pub restart_at: Option<Instant>,
    pub membership_status: MembershipStatus,
    pub startup_aborted: bool,
    // Keep this field after every exit-bearing projection above. Field order
    // makes a record clone's raw exits provably refcount-only on drop, and
    // clones share the guard allocation, so a boolean stage probe costs
    // refcount traffic rather than one disposal job per retained exit.
    pub(super) retained_exits: Arc<Vec<RetainedExit>>,
}

impl MemberRecord {
    /// Rebuilds the guard set covering this record's raw exits.
    ///
    /// Every mutation path calls this after writing, so the guards a mutation
    /// displaces always still cover the raw value it overwrites: the inline
    /// drop inside the watch mutation is refcount work, and the retired guard
    /// set transfers any failed payload to isolated disposal once its last
    /// record clone dies.
    pub(super) fn refresh_retained_exits(&mut self) {
        let mut retained = Vec::new();
        if let MemberStage::Terminal(exit) = &self.stage {
            RetainedExit::retain_exit(&mut retained, exit);
        }
        if let Some(exit) = &self.last_exit {
            RetainedExit::retain_exit(&mut retained, exit);
        }
        RetainedExit::install(&mut self.retained_exits, retained);
    }

    /// Applies one driver-requested transition.
    ///
    /// Every watch-channel writer routes stage changes through here (see
    /// [`MemberCell::transition`] for the wake-bus contract). The reducer
    /// rejects an event whose source stage is not one its driver call sites
    /// can present, including in release builds.
    ///
    /// Exits are safe to retire inside the watch mutation: the record's guard
    /// set still covers the displaced value, and [`Self::refresh_retained_exits`]
    /// re-establishes that cover before the mutation returns.
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
pub struct MemberCell {
    id: ChildId,
    membership: Membership,
    rebased_membership: OnceLock<Membership>,
    incarnations: Mutex<Option<IncarnationCounter>>,
    pub(super) record: runtime::WatchSender<MemberRecord>,
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

impl ActorIdentity for MemberCell {
    fn id(&self) -> &ChildId {
        self.id()
    }

    fn membership(&self) -> Membership {
        self.membership()
    }
}

#[derive(Default)]
enum MemberMailbox {
    #[default]
    Unattached,
    Attached(Arc<dyn MailboxControl>),
    Terminal {
        control: Option<Arc<dyn MailboxControl>>,
        exit: RetainedExit,
        teardown: Option<Box<dyn MailboxTermination>>,
    },
}

impl fmt::Debug for MemberMailbox {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unattached => formatter.write_str("Unattached"),
            Self::Attached(control) => formatter.debug_tuple("Attached").field(control).finish(),
            Self::Terminal {
                control,
                exit,
                teardown,
            } => {
                // Formatting `ExitKind::Failed` would invoke the user's error
                // formatter while `MemberCell::mailbox` is held by the
                // derived `MemberCell` Debug implementation. A static tag is
                // the only mailbox diagnostic needed here.
                let exit = match exit.as_exit().kind() {
                    ExitKind::Completed => "Completed",
                    ExitKind::Failed(_) => "Failed",
                    ExitKind::Panicked { .. } => "Panicked",
                    ExitKind::ReadinessTimedOut { .. } => "ReadinessTimedOut",
                    ExitKind::Aborted { .. } => "Aborted",
                    ExitKind::NeverStarted => "NeverStarted",
                };
                formatter
                    .debug_struct("Terminal")
                    .field("control", control)
                    .field("exit", &exit)
                    .field("teardown_pending", &teardown.is_some())
                    .finish()
            }
        }
    }
}

impl MemberCell {
    pub fn new(id: ChildId, identity: MintedMembership) -> Arc<Self> {
        let (membership, incarnations) = identity.into_pair();
        let (record, _) = runtime::watch(MemberRecord {
            stage: MemberStage::Reserved,
            incarnation: None,
            last_incarnation: None,
            last_exit: None,
            restart_count: RestartCount::ZERO,
            restart_at: None,
            membership_status: MembershipStatus::Active,
            startup_aborted: false,
            retained_exits: Arc::new(Vec::new()),
        });
        Arc::new(Self {
            id,
            membership,
            rebased_membership: OnceLock::new(),
            incarnations: Mutex::new(Some(incarnations)),
            record,
            observation_gate: RwLock::new(ObservationGate::new()),
            terminal_disposal_pending: AtomicBool::new(false),
            mailbox: Mutex::new(MemberMailbox::default()),
            options: OnceLock::new(),
        })
    }

    pub fn id(&self) -> &ChildId {
        &self.id
    }

    pub fn membership(&self) -> Membership {
        self.rebased_membership
            .get()
            .copied()
            .unwrap_or(self.membership)
    }

    pub fn rebase_membership(&self, identity: MintedMembership) {
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

    pub fn take_incarnation_counter(&self) -> IncarnationCounter {
        self.incarnations
            .lock()
            .expect("incarnation counter mutex poisoned")
            .take()
            .expect("a membership's incarnation counter is issued to one runtime")
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn lock_incarnation_counter(
        &self,
    ) -> std::sync::MutexGuard<'_, Option<IncarnationCounter>> {
        self.incarnations
            .lock()
            .expect("incarnation counter mutex starts healthy")
    }

    pub fn record(&self) -> MemberRecord {
        self.record.read_cloned()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn record_watcher(&self) -> runtime::WatchReceiver<MemberRecord> {
        self.record.watcher()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn stage_terminal_before_mailbox(&self, exit: Exit) {
        let mut mailbox = self.mailbox.lock().expect("member mailbox mutex poisoned");
        assert!(matches!(*mailbox, MemberMailbox::Unattached));
        *mailbox = MemberMailbox::Terminal {
            control: None,
            exit: RetainedExit::new(exit),
            teardown: None,
        };
    }

    /// Reports terminality or drain-entry terminal-disposal intent.
    ///
    /// `Acquire` pairs with the `Release` store in
    /// [`Self::set_terminal_disposal_pending`]. Two separate edges make a
    /// zero-budget straggler sample (the driver's `collect_stragglers`)
    /// correct, and both rest on these atomics alone: the sample is *not*
    /// incidentally serialized by the observation gate, because
    /// [`ScopeCell::resident_projections`] takes `observation.current_children`
    /// — a different mutex from the gate `publish_drain` holds.
    ///
    /// This method owns the marker-before-record order so a caller cannot
    /// accidentally recreate the trailing gap by reversing two independent
    /// reads.
    ///
    /// * A marker load that observes the *clear* synchronizes with the store the
    ///   driver issues after `terminalize_child` published
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
    pub fn terminal_or_disposal_pending(&self) -> bool {
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
    pub fn set_terminal_disposal_pending(&self, pending: bool) {
        self.terminal_disposal_pending
            .store(pending, Ordering::Release);
    }

    /// Mutates a member record and pulses the watch channel.
    ///
    /// Test-only escape hatch around [`Self::transition`]; the wake-bus
    /// contract documented there binds this path too.
    #[cfg(any(test, feature = "test-util"))]
    pub fn update(&self, update: impl FnOnce(&mut MemberRecord)) {
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
    #[cfg(any(test, feature = "test-util"))]
    #[must_use = "an illegal member transition is rejected, not applied"]
    pub fn transition(&self, transition: MemberTransition) -> bool {
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
        let mut operation = Some(operation);
        loop {
            let gate = self.current_observation_gate();
            report_capture();
            let guard = gate.lock();
            if gate.shares_gate(&self.current_observation_gate()) {
                let mut txn = ObservationTxn::new(guard);
                return operation
                    .take()
                    .expect("member observation operation runs exactly once")(
                    &mut txn
                );
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

    #[cfg(any(test, feature = "test-util"))]
    pub fn observation_gate(&self) -> ObservationGate {
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
        let adopted = self.with_handoff_gate(gate, report_capture, |current| {
            if !current.shares_gate(gate) {
                install(current);
            }
            true
        });
        debug_assert!(adopted, "unconditional adoption never refuses");
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
        let mut attempt = Some(attempt);
        loop {
            let current = self.current_observation_gate();
            if current.shares_gate(gate) {
                return attempt
                    .take()
                    .expect("an observation gate handoff attempt runs exactly once")(
                    &current
                );
            }
            report_capture();
            let current_guard = current.lock();
            if current.shares_gate(&self.current_observation_gate()) {
                let accepted = attempt
                    .take()
                    .expect("an observation gate handoff attempt runs exactly once")(
                    &current
                );
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
        // Refreshing here rather than in each writer keeps the guard-set
        // invariant on every record mutation, including the test-only escape
        // hatch that writes fields directly.
        self.record.modify_silently(|record| {
            update(record);
            record.refresh_retained_exits();
        });
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
    pub fn transition_locked(
        &self,
        txn: &mut ObservationTxn<'_>,
        transition: MemberTransition,
    ) -> bool {
        // `RestartScheduled` carries a user error by value. Retain it before
        // the watch lock and reducer validation so an unrelated panic can
        // unwind the raw transition as refcount traffic only.
        let retained_exit = match &transition {
            MemberTransition::RestartScheduled { exit, .. } => {
                Some(RetainedExit::new(exit.clone()))
            }
            MemberTransition::Admitted
            | MemberTransition::Starting { .. }
            | MemberTransition::Running
            | MemberTransition::Stopping => None,
        };
        let mut rejected = None;
        self.record
            .modify_silently(|record| match record.apply_transition(transition) {
                Ok(()) => record.refresh_retained_exits(),
                Err(transition) => rejected = Some(transition),
            });
        if let Some(rejected) = rejected {
            // Rejection leaves this cell exactly as it was. The rejected event
            // may own a failed exit, so retire it with the transaction rather
            // than under the observation gate or watch lock.
            txn.defer(move || runtime::dispose_detached(rejected));
            if let Some(retained_exit) = retained_exit {
                // The deferred transition proves this clone cannot be last.
                drop(retained_exit.into_exit());
            }
            return false;
        }
        txn.pulse(&self.record);
        if let Some(retained_exit) = retained_exit {
            // The record now owns an equivalent retained copy.
            drop(retained_exit.into_exit());
        }
        true
    }

    pub fn set_options(&self, options: ResolvedCommonOptions) {
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

    pub fn attach_mailbox(&self, mailbox: Arc<dyn MailboxControl>) {
        self.with_observation_txn(|txn| {
            let mut rejected = None;
            let terminal_exit = {
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
                    MemberMailbox::Terminal {
                        control,
                        exit,
                        teardown,
                    } => {
                        debug_assert!(teardown.is_none());
                        *teardown = mailbox.prepare_termination(txn);
                        *control = Some(mailbox);
                        Some(exit.as_exit().clone())
                    }
                }
            };
            // Follow the mailbox layer's convention and release the lock before
            // panicking, so a driver-contract violation stays on this thread
            // instead of poisoning the mutex under every later mailbox lookup.
            // The rejected mailbox still owns unread user messages, so the
            // transaction destroys it after the gate is released -- during this
            // panic's unwind, which is exactly when an inline destructor would
            // be a double panic.
            if let Some(rejected) = rejected {
                txn.defer(move || drop(rejected));
                panic!("a member can own only one mailbox");
            }
            if let Some(terminal_exit) = terminal_exit {
                self.terminalize_locked(terminal_exit, StartupDisposition::Unchanged, txn);
            }
        });
    }

    pub fn mailbox(&self) -> Option<Arc<dyn MailboxControl>> {
        match &*self.mailbox.lock().expect("member mailbox mutex poisoned") {
            MemberMailbox::Unattached => None,
            MemberMailbox::Attached(control) => Some(Arc::clone(control)),
            MemberMailbox::Terminal { control, .. } => control.clone(),
        }
    }

    pub fn terminalize(&self, exit: Exit, startup: StartupDisposition) {
        self.with_observation_txn(|txn| {
            self.terminalize_locked(exit, startup, txn);
        });
    }

    pub fn terminalize_locked(
        &self,
        exit: Exit,
        startup: StartupDisposition,
        txn: &mut ObservationTxn<'_>,
    ) -> Exit {
        // A losing terminalizer's exit is destroyed here, and an
        // `ExitKind::Failed` payload owns a type-erased user error whose
        // destructor may block, panic, or re-enter observation. Hand it to the
        // transaction rather than dropping it under the gate.
        let mut losing_exit = None;
        let terminal_exit = {
            let mut state = self.mailbox.lock().expect("member mailbox mutex poisoned");
            match &*state {
                MemberMailbox::Terminal {
                    exit: terminal_exit,
                    ..
                } => {
                    let terminal_exit = terminal_exit.as_exit().clone();
                    losing_exit = Some(exit);
                    terminal_exit
                }
                MemberMailbox::Unattached => {
                    *state = MemberMailbox::Terminal {
                        control: None,
                        exit: RetainedExit::new(exit.clone()),
                        teardown: None,
                    };
                    exit
                }
                MemberMailbox::Attached(control) => {
                    let control = Arc::clone(control);
                    let teardown = control.prepare_termination(txn);
                    *state = MemberMailbox::Terminal {
                        control: Some(control),
                        exit: RetainedExit::new(exit.clone()),
                        teardown,
                    };
                    exit
                }
            }
        };
        if let Some(losing_exit) = losing_exit {
            // This is framework-retained ownership, not a value returned to a
            // user. Route a failed exit's possibly-blocking user destructor
            // through the critical-disposal lane after the gate is released.
            let losing_exit = RetainedExit::new(losing_exit);
            txn.defer(move || drop(losing_exit));
        }
        let mut published = false;
        self.record.modify_silently(|record| {
            if !matches!(record.stage, MemberStage::Terminal(_)) {
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
            }
            // The guard set displaced above still covers whatever this
            // mutation overwrote, so refreshing after the writes is what
            // keeps the inline drops refcount-only.
            record.refresh_retained_exits();
        });
        // Store before discharge so reentrant mailbox wakers observe the
        // winning exit. Notification-driven readers still see
        // discharge-before-pulse; tree-scoped publication defers both until
        // the complete observation transaction has released its gate.
        let mut nonterminal_mailbox = false;
        let teardown = match &mut *self.mailbox.lock().expect("member mailbox mutex poisoned") {
            MemberMailbox::Terminal { teardown, .. } => teardown.take(),
            MemberMailbox::Unattached | MemberMailbox::Attached(_) => {
                // Panicking here would poison the member mailbox and can
                // retire a retained user exit during unwind. Compute the
                // verdict under the lock and raise it below, once the guard
                // has been released; release builds keep the already-published
                // terminal state.
                nonterminal_mailbox = true;
                None
            }
        };
        debug_assert!(
            !nonterminal_mailbox,
            "terminal publication requires terminal mailbox state"
        );
        if let Some(teardown) = teardown {
            txn.defer(move || {
                runtime::dispose_detached(teardown.finish());
            });
        }
        // First terminalizer wins. A losing edge neither reclassifies startup
        // nor publishes a second record edge; a future caller that needs
        // different semantics must make that race explicit at its boundary.
        if published {
            txn.pulse(&self.record);
        }
        terminal_exit
    }

    pub async fn wait_terminal(&self) -> Exit {
        let mut watcher = self.record.watcher();
        loop {
            if let MemberStage::Terminal(exit) = watcher.borrow_cloned().stage {
                return exit;
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
        time::Duration,
    };

    use shelterwood_core::{Cancellation, ExitError, identity::ScopeIdentity, policy::ScopeFlavor};
    use shelterwood_mailbox::{MailboxCell, MailboxControl};
    use shelterwood_runtime as runtime;

    use super::*;
    use crate::cells::test_support::{ThreadProbe, isolated_scope};

    // The retention this pins only has an observable effect when the
    // framework invariant it guards is checked, and that check is a
    // `debug_assert!`. Release builds decline the panic entirely, so the test
    // is gated on the profile it can hold in rather than left to fail there.
    #[cfg(debug_assertions)]
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
        let first = MailboxCell::<u8>::new(scope.member.id().clone(), runtime::mailbox_runtime());
        let second = MailboxCell::<u8>::new(scope.member.id().clone(), runtime::mailbox_runtime());
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
        let member = MemberCell::new(
            id.clone(),
            identity
                .mint_membership(&id)
                .expect("membership is available"),
        );
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
        let member = MemberCell::new(
            id.clone(),
            identity
                .mint_membership(&id)
                .expect("membership is available"),
        );
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
            !first.retained_exits.is_empty(),
            "a terminal failed member retains its exit"
        );
        assert!(
            Arc::ptr_eq(&first.retained_exits, &second.retained_exits),
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
}
