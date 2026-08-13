//! Restart-stable shared member and scope state.
//!
//! These cells are the neutral synchronization and observation projection
//! layer shared by public handles, mailboxes, actor/task contexts, and the
//! mutable supervision driver. In particular, this module does not depend on
//! mutable driver state. Its dynamic-route interface names declaration slots
//! only as opaque capability payloads; their state remains owned elsewhere.

use std::{
    any::Any,
    collections::VecDeque,
    fmt,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, RwLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

#[cfg(any(test, feature = "test-util"))]
use std::sync::atomic::AtomicUsize;

use crate::{
    ChildId, Exit, Incarnation, Intensity, Membership, RestartCount, Strategy, TotalRestarts,
    admission::{RemoveOutcome, ReserveError},
    engine::{Epoch, MembershipStatus, RequestTarget, ScopeEpochs, ScopeState},
    exit::{StartupError, StopReason, stop_reason_precedence},
    identity::{AtomicPoisonedCounter, IncarnationCounter, MintedMembership, ScopeIdentity},
    mailbox::{ActorIdentity, MailboxControl, MailboxTermination},
    observe::{
        ChildSnapshot, ChildState, LifecycleEvent, LifecycleEventKind, LifecycleEvents,
        LifecycleHub, LifecycleSeq, ScopeKind, ScopeSnapshot, SnapshotHub, SnapshotReceiver,
    },
    policy::{ResolvedCommonOptions, ScopeFlavor},
    runtime::{self, Latch},
};

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
}

impl MemberRecord {
    /// Applies one driver-requested transition.
    ///
    /// Every watch-channel writer routes stage changes through here (see
    /// [`MemberCell::transition`] for the wake-bus contract), so each arm
    /// asserts the source stages its driver call sites can actually present.
    ///
    /// Returns the exit this transition displaced from `last_exit`, if any.
    /// The record is mutated under the observation gate and the watch
    /// channel's own lock, and an `ExitKind::Failed` payload owns a
    /// type-erased user error, so the displaced value is surrendered to the
    /// caller rather than destroyed here (§1's lock rule).
    #[must_use]
    fn apply_transition(&mut self, transition: MemberTransition) -> Option<Exit> {
        match transition {
            MemberTransition::Admitted => {
                debug_assert!(
                    matches!(self.stage, MemberStage::Reserved),
                    "admission must consume a fresh reservation, not {:?}",
                    self.stage
                );
                self.stage = MemberStage::Admitted;
                None
            }
            MemberTransition::Starting { incarnation } => {
                debug_assert!(
                    matches!(self.stage, MemberStage::Admitted | MemberStage::Restarting),
                    "a spawn must start an admitted or restarting member, not {:?}",
                    self.stage
                );
                self.stage = MemberStage::Starting;
                self.incarnation = Some(incarnation);
                self.last_incarnation = Some(incarnation);
                self.restart_at = None;
                None
            }
            MemberTransition::Running => {
                debug_assert!(
                    matches!(self.stage, MemberStage::Starting | MemberStage::Reserved),
                    "readiness must promote a starting member (or the root scope's own \
                     never-admitted reservation), not {:?}",
                    self.stage
                );
                self.stage = MemberStage::Running;
                None
            }
            MemberTransition::Stopping => {
                debug_assert!(
                    matches!(self.stage, MemberStage::Starting | MemberStage::Running),
                    "a stop ladder must begin on a starting or running member, not {:?}",
                    self.stage
                );
                self.stage = MemberStage::Stopping;
                None
            }
            MemberTransition::RestartScheduled {
                exit,
                restart_count,
                restart_at,
            } => {
                debug_assert!(
                    matches!(
                        self.stage,
                        MemberStage::Starting | MemberStage::Running | MemberStage::Stopping
                    ),
                    "a restart must be scheduled from an active incarnation's exit, not {:?}",
                    self.stage
                );
                self.stage = MemberStage::Restarting;
                self.incarnation = None;
                let displaced = self.last_exit.replace(exit);
                self.restart_count = restart_count;
                self.restart_at = restart_at;
                displaced
            }
        }
    }
}

#[derive(Debug)]
pub struct MemberCell {
    id: ChildId,
    membership: Membership,
    rebased_membership: OnceLock<Membership>,
    incarnations: Mutex<Option<IncarnationCounter>>,
    record: runtime::WatchSender<MemberRecord>,
    // Guards only a gate-pointer swap, so no torn state is possible; every
    // access deliberately tolerates poisoning (mirroring
    // `ObservationGate::lock`) so drop-path shutdown after a panicked assert
    // cannot itself panic.
    observation_gate: RwLock<ObservationGate>,
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
        exit: Exit,
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
            } => formatter
                .debug_struct("Terminal")
                .field("control", control)
                .field("exit", exit)
                .field("teardown_pending", &teardown.is_some())
                .finish(),
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
            exit,
            teardown: None,
        };
    }

    pub fn terminal_disposal_pending(&self) -> bool {
        self.terminal_disposal_pending.load(Ordering::Acquire)
    }

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
    #[cfg(any(test, feature = "test-util"))]
    pub fn transition(&self, transition: MemberTransition) {
        self.with_observation_txn(|txn| self.transition_locked(txn, transition));
    }

    fn with_observation_txn<R>(&self, operation: impl FnOnce(&mut ObservationTxn<'_>) -> R) -> R {
        let mut operation = Some(operation);
        loop {
            let gate = self.current_observation_gate();
            let guard = gate.lock();
            if gate.same_gate(&self.current_observation_gate()) {
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

    fn current_observation_gate(&self) -> ObservationGate {
        self.observation_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn observation_gate(&self) -> ObservationGate {
        self.current_observation_gate()
    }

    fn install_observation_gate_locked(&self, previous: &ObservationGate, gate: &ObservationGate) {
        let mut installed = self
            .observation_gate
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if installed.same_gate(previous) {
            *installed = gate.clone();
        } else {
            assert!(
                installed.same_gate(gate),
                "a resident member must share its tree observation gate"
            );
        }
    }

    fn adopt_observation_gate(&self, gate: &ObservationGate, _txn: &mut ObservationTxn<'_>) {
        loop {
            let current = self.current_observation_gate();
            if current.same_gate(gate) {
                return;
            }
            let current_guard = current.lock();
            if current.same_gate(&self.current_observation_gate()) {
                self.install_observation_gate_locked(&current, gate);
                drop(current_guard);
                return;
            }
            drop(current_guard);
        }
    }

    fn update_locked(&self, txn: &mut ObservationTxn<'_>, update: impl FnOnce(&mut MemberRecord)) {
        self.record.modify_silently(update);
        txn.pulse(&self.record);
    }

    pub fn transition_locked(&self, txn: &mut ObservationTxn<'_>, transition: MemberTransition) {
        // A restart schedule overwrites `last_exit`. The record is mutated
        // under the gate and the watch channel's own lock, so the exit it
        // displaces -- whose `ExitKind::Failed` payload owns a type-erased
        // user error -- leaves the critical section and is destroyed by the
        // transaction instead.
        let mut displaced_exit = None;
        self.update_locked(txn, |record| {
            displaced_exit = record.apply_transition(transition);
        });
        if let Some(displaced_exit) = displaced_exit {
            txn.defer(move || drop(displaced_exit));
        }
    }

    pub fn set_options(&self, options: ResolvedCommonOptions) {
        self.options
            .set(options)
            .expect("member options are resolved exactly once");
    }

    fn options(&self) -> ResolvedCommonOptions {
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
                        *teardown = mailbox.prepare_termination();
                        *control = Some(mailbox);
                        Some(exit.clone())
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
                    let terminal_exit = terminal_exit.clone();
                    losing_exit = Some(exit);
                    terminal_exit
                }
                MemberMailbox::Unattached => {
                    *state = MemberMailbox::Terminal {
                        control: None,
                        exit: exit.clone(),
                        teardown: None,
                    };
                    exit
                }
                MemberMailbox::Attached(control) => {
                    let control = Arc::clone(control);
                    let teardown = control.prepare_termination();
                    *state = MemberMailbox::Terminal {
                        control: Some(control),
                        exit: exit.clone(),
                        teardown,
                    };
                    exit
                }
            }
        };
        if let Some(losing_exit) = losing_exit {
            txn.defer(move || drop(losing_exit));
        }
        let mut published = false;
        // Terminalization also overwrites `last_exit`, which a prior restart
        // schedule may already have filled with a different user payload.
        let mut displaced_exit = None;
        self.record.modify_silently(|record| {
            match startup {
                StartupDisposition::Unchanged => {}
                StartupDisposition::NotAborted => record.startup_aborted = false,
                StartupDisposition::Aborted => record.startup_aborted = true,
            }
            if !matches!(record.stage, MemberStage::Terminal(_)) {
                record.incarnation = None;
                record.restart_at = None;
                displaced_exit = record.last_exit.replace(terminal_exit.clone());
                record.stage = MemberStage::Terminal(terminal_exit.clone());
                published = true;
            }
        });
        if let Some(displaced_exit) = displaced_exit {
            txn.defer(move || drop(displaced_exit));
        }
        let record_changed = published || startup != StartupDisposition::Unchanged;
        // Store before discharge so reentrant mailbox wakers observe the
        // winning exit. Notification-driven readers still see
        // discharge-before-pulse; tree-scoped publication defers both until
        // the complete observation transaction has released its gate.
        let teardown = match &mut *self.mailbox.lock().expect("member mailbox mutex poisoned") {
            MemberMailbox::Terminal { teardown, .. } => teardown.take(),
            MemberMailbox::Unattached | MemberMailbox::Attached(_) => {
                unreachable!("terminal publication requires terminal mailbox state")
            }
        };
        if let Some(teardown) = teardown {
            txn.defer(move || {
                if let Some(payload) = teardown.finish() {
                    runtime::dispose_detached(payload);
                }
            });
        }
        // A supervised terminal edge updates `startup_aborted` even if a
        // competing terminalizer won the stage transition. Its record pulse
        // remains required, but mailbox discharge must lead it.
        if record_changed {
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

/// Observation-only projection of the authoritative engine lifecycle.
/// Driver decisions never read this record back as liveness policy.
#[derive(Clone, Debug)]
pub struct ScopeRecord {
    pub state: ScopeState,
    pub startup: Option<Result<(), StartupError>>,
    /// Read only by this crate's snapshot publication; the driver takes its
    /// restart totals from the decision that produced them.
    pub(crate) total_restarts: TotalRestarts,
}

/// Type-erased declaration slot carried by the restart-stable cell layer.
///
/// The route implementation chooses the concrete slot allocation. Erasing it
/// here lets [`ScopeCell`] retain the route without depending on the plan
/// layer that owns declaration state.
pub type ErasedDynamicSlot = dyn Any + Send + Sync;

/// Object-safe dynamic route retained by a restart-stable scope cell.
pub type ErasedDynamicRoute = dyn DynamicRoute<Slot = ErasedDynamicSlot>;

pub trait DynamicRoute: Send + Sync {
    type Slot: ?Sized + Send + Sync;

    fn reserve(
        &self,
        scope: &Arc<ScopeCell>,
        id: ChildId,
        child_scope: Option<ScopeFlavor>,
        txn: &mut ObservationTxn<'_>,
    ) -> Result<Arc<Self::Slot>, ReserveError>;

    fn close_admission(&self, txn: &mut ObservationTxn<'_>);

    fn start_admission(
        self: Arc<Self>,
        slot: Arc<Self::Slot>,
        fused_cancel: Option<Latch>,
    ) -> Result<runtime::OneShotReceiver<Result<(), ReserveError>>, ReserveError>;

    fn cancel_reservation(
        &self,
        scope: &Arc<ScopeCell>,
        slot: &Self::Slot,
        txn: &mut ObservationTxn<'_>,
    );

    fn signal_fused_cancel(
        &self,
        scope: &Arc<ScopeCell>,
        slot: &Self::Slot,
        latch: &Latch,
        txn: &mut ObservationTxn<'_>,
    );

    fn remove(
        &self,
        scope: &Arc<ScopeCell>,
        id: &ChildId,
        exact: Option<Membership>,
        txn: &mut ObservationTxn<'_>,
    ) -> runtime::OneShotReceiver<RemoveOutcome>;
}

/// Shared critical section for one resident tree's observation projection.
///
/// Gate identity, rather than the lock payload, defines tree membership. The
/// lock deliberately tolerates poisoning: a panic in an observation path must
/// not permanently wedge later observation or a subtree handoff.
#[derive(Clone, Debug)]
pub struct ObservationGate(Arc<Mutex<()>>);

impl ObservationGate {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(())))
    }

    pub fn same_gate(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn lock(&self) -> MutexGuard<'_, ()> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether some thread — possibly this one — is inside the gate.
    ///
    /// The probe a lock-rule test needs: a value's destructor asks whether it
    /// is running inside the critical section, which a same-thread `try_lock`
    /// answers without the reentrant acquisition that would deadlock. A
    /// poisoned but unheld gate reports `false`, matching [`Self::lock`]'s
    /// deliberate poison tolerance.
    #[cfg(any(test, feature = "test-util"))]
    #[must_use]
    pub fn is_held(&self) -> bool {
        matches!(self.0.try_lock(), Err(std::sync::TryLockError::WouldBlock))
    }
}

/// Capability for one observation-gate transaction.
///
/// Every retained control-plane writer takes this token, making an
/// out-of-transaction mutation unavailable by construction. Tokio invokes
/// registered wakers synchronously, so pulses and disposal work accumulate on
/// the token and flush only after its gate guard has been released. The same
/// drop path runs during unwind, preventing a poisoned transaction from
/// stranding already-committed wakes.
pub struct ObservationTxn<'a> {
    guard: Option<MutexGuard<'a, ()>>,
    pulses: Vec<Box<dyn FnOnce()>>,
}

impl<'a> ObservationTxn<'a> {
    fn new(guard: MutexGuard<'a, ()>) -> Self {
        Self {
            guard: Some(guard),
            pulses: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn detached() -> Self {
        Self {
            guard: None,
            pulses: Vec::new(),
        }
    }

    pub fn defer(&mut self, operation: impl FnOnce() + 'static) {
        self.pulses.push(Box::new(operation));
    }

    /// Defers a watch-channel wake. The driver reaches its own senders
    /// through [`Self::defer`], so this stays inside the cell layer.
    pub(crate) fn pulse<T: 'static>(&mut self, sender: &runtime::WatchSender<T>) {
        let sender = sender.clone();
        self.defer(move || sender.pulse());
    }

    fn commit(&mut self) {
        drop(self.guard.take());
        let mut panics = runtime::PanicAccumulator::default();
        for pulse in self.pulses.drain(..) {
            // One hostile waker must not prevent the remaining committed
            // observation edges from notifying their waiters.
            panics.run(pulse);
        }
    }
}

impl Drop for ObservationTxn<'_> {
    fn drop(&mut self) {
        self.commit();
    }
}

/// Driver-independent view of one declaration slot while its membership is
/// resident in a running scope.
#[derive(Clone)]
pub struct ResidentProjection {
    pub member: Arc<MemberCell>,
    pub scope: Option<Arc<ScopeCell>>,
}

impl ResidentProjection {
    pub fn new(member: Arc<MemberCell>, scope: Option<Arc<ScopeCell>>) -> Self {
        Self { member, scope }
    }
}

struct ResidencyCompletion {
    parent: Weak<ScopeCell>,
    projection: ResidentProjection,
}

struct ResidentChild {
    projection: ResidentProjection,
    removal: Option<ResidencyCompletion>,
}

impl ResidentChild {
    fn new(parent: &Arc<ScopeCell>, projection: ResidentProjection) -> Self {
        Self {
            removal: Some(ResidencyCompletion {
                parent: Arc::downgrade(parent),
                projection: projection.clone(),
            }),
            projection,
        }
    }

    fn disarm_removal(&mut self) -> ResidencyCompletion {
        self.removal
            .take()
            .expect("a resident completes removal exactly once")
    }

    fn publish_removal(completion: ResidencyCompletion, txn: &mut ObservationTxn<'_>) {
        if let Some(parent) = completion.parent.upgrade() {
            parent.emit_locked(
                txn,
                LifecycleEventKind::Removed {
                    id: completion.projection.member.id().clone(),
                    membership: completion.projection.member.membership(),
                    last_incarnation: completion.projection.member.record().last_incarnation,
                },
            );
        }
    }

    fn complete_removal(mut self, txn: &mut ObservationTxn<'_>) {
        let completion = self.disarm_removal();
        Self::publish_removal(completion, txn);
    }
}

impl Drop for ResidentChild {
    fn drop(&mut self) {
        let Some(completion) = self.removal.take() else {
            return;
        };
        let Some(parent) = completion.parent.upgrade() else {
            return;
        };
        parent.with_observation_gate(|txn| Self::publish_removal(completion, txn));
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ObservationConfig {
    strategy: Strategy,
    intensity: Intensity,
}

#[derive(Debug, Default)]
struct ScopeControl {
    epochs: ScopeEpochs,
    shutdown: Option<ScopeRequest>,
    force: Option<ScopeRequest>,
    events: VecDeque<ScopeControlEvent>,
}

#[derive(Clone, Copy, Debug)]
struct ScopeRequest {
    epoch: Epoch,
    consumed: bool,
}

/// One fact published by a scope control-plane transaction for its driver.
///
/// The payload is restart-stable identity rather than a mutable driver key.
/// The driver resolves it through its incrementally maintained membership
/// index, so stale events miss instead of addressing a replacement child.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScopeControlEvent {
    RestartShutdown {
        membership: Membership,
        target: Epoch,
    },
}

/// Shared scope state follows two distinct synchronization regimes.
///
/// One gate per resident tree serializes compound observation-visible
/// transitions across configuration, records, resident children, and parent
/// links. Configuration, residency, and ancestry are plain synchronized state;
/// their individual locks do not make a multi-field transition atomic, so
/// every recursive observation path continues to hold that one tree gate.
///
/// Control requests, the dynamic route, records, residency, and hubs retain
/// their narrow storage locks, but every mutation is subordinate to the tree
/// gate and takes an [`ObservationTxn`] capability. Identity allocation and
/// lifecycle sequence minting remain independent driver-only counters. The
/// member-record watch is intentionally also the driver's wake bus.
struct ScopeObservation {
    config: Mutex<ObservationConfig>,
    record: runtime::WatchSender<ScopeRecord>,
    // `ResidentChild::drop` emits `Removed` by taking the observation gate
    // itself, so dropping a resident anywhere the gate is already held
    // self-deadlocks. In-gate removal paths must consume the resident through
    // `complete_removal(txn)`. Dropping a resident while holding this mutex
    // likewise self-deadlocks, so removal moves it into outer storage first.
    current_children: Mutex<Vec<ResidentChild>>,
    parent: Mutex<Option<Weak<ScopeCell>>>,
    lifecycle_seq: AtomicPoisonedCounter,
    lifecycle: LifecycleHub,
    snapshots: SnapshotHub,
    closed: AtomicBool,
}

pub struct ScopeCell {
    pub member: Arc<MemberCell>,
    pub flavor: ScopeFlavor,
    child_identity: Mutex<ScopeIdentity>,
    control: Mutex<ScopeControl>,
    dynamic_route: Mutex<Option<Arc<ErasedDynamicRoute>>>,
    observation: ScopeObservation,
    #[cfg(any(test, feature = "test-util"))]
    ancestor_parent_reads: AtomicUsize,
    #[cfg(any(test, feature = "test-util"))]
    runtime_storage: Mutex<RuntimeStorage>,
    #[cfg(any(test, feature = "test-util"))]
    gate_capture_probe: Mutex<Option<std::sync::mpsc::Sender<GateCapture>>>,
}

/// One observation-gate capture reported to a test probe.
///
/// A capture is reported after a thread has cloned the gate it is about to
/// acquire and before it blocks on that acquisition. Unit tests use these
/// reports as explicit barriers in place of scheduler or strong-count
/// polling: receiving a capture proves the reporting thread committed to the
/// gate that was current at that instant.
#[cfg(any(test, feature = "test-util"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateCapture {
    /// [`ScopeCell::with_observation_gate`] captured its current gate.
    Observation,
    /// Gate adoption captured an obsolete gate it must acquire to hand off.
    Adoption,
}

#[cfg(any(test, feature = "test-util"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeStorage {
    pub children: usize,
    pub child_slots: usize,
    pub deadlines: usize,
    pub deadline_slots: usize,
}

impl ScopeCell {
    pub fn mint_membership(&self, id: &ChildId) -> Option<MintedMembership> {
        self.child_identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .mint_membership(id)
    }

    pub fn adopt_or_mint_membership(
        &self,
        id: &ChildId,
        provisional: Membership,
    ) -> Option<Option<MintedMembership>> {
        self.child_identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .adopt_or_mint_membership(id, provisional)
    }

    pub fn evict_child_identity(&self, member: &MemberCell) {
        self.child_identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .evict(member.id(), member.membership());
    }

    pub fn new(
        member: Arc<MemberCell>,
        flavor: ScopeFlavor,
        child_identity: ScopeIdentity,
    ) -> Arc<Self> {
        let (record, _) = runtime::watch(ScopeRecord {
            state: ScopeState::Unstarted,
            startup: None,
            total_restarts: TotalRestarts::ZERO,
        });
        Arc::new(Self {
            member,
            flavor,
            child_identity: Mutex::new(child_identity),
            control: Mutex::new(ScopeControl::default()),
            dynamic_route: Mutex::new(None),
            observation: ScopeObservation {
                config: Mutex::new(ObservationConfig::default()),
                record,
                current_children: Mutex::new(Vec::new()),
                parent: Mutex::new(None),
                lifecycle_seq: AtomicPoisonedCounter::new(),
                lifecycle: LifecycleHub::default(),
                snapshots: SnapshotHub::default(),
                closed: AtomicBool::new(false),
            },
            #[cfg(any(test, feature = "test-util"))]
            ancestor_parent_reads: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-util"))]
            runtime_storage: Mutex::new(RuntimeStorage::default()),
            #[cfg(any(test, feature = "test-util"))]
            gate_capture_probe: Mutex::new(None),
        })
    }

    pub fn record(&self) -> ScopeRecord {
        self.observation.record.read_cloned()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn record_watcher(&self) -> runtime::WatchReceiver<ScopeRecord> {
        self.observation.record.watcher()
    }

    pub fn resident_projections(&self) -> Vec<ResidentProjection> {
        self.current_children()
            .iter()
            .map(|resident| resident.projection.clone())
            .collect()
    }

    pub fn has_resident_child(&self, member: &MemberCell) -> bool {
        self.current_children()
            .iter()
            .any(|resident| resident.projection.member.membership() == member.membership())
    }

    fn current_children(&self) -> MutexGuard<'_, Vec<ResidentChild>> {
        self.observation
            .current_children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn parent(&self) -> Option<Arc<ScopeCell>> {
        #[cfg(any(test, feature = "test-util"))]
        self.ancestor_parent_reads.fetch_add(1, Ordering::Relaxed);
        self.observation
            .parent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade)
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn take_ancestor_parent_reads(&self) -> usize {
        self.ancestor_parent_reads.swap(0, Ordering::Relaxed)
    }

    fn set_parent(&self, parent: &Arc<ScopeCell>, txn: &mut ObservationTxn<'_>) {
        *self
            .observation
            .parent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::downgrade(parent));
        let control = self.control.lock().expect("scope control mutex poisoned");
        let pending_shutdown = control.shutdown.filter(|request| {
            !request.consumed && control.epochs.request_is_pending(request.epoch)
        });
        drop(control);
        if let Some(request) = pending_shutdown {
            parent.publish_control_event_locked(
                ScopeControlEvent::RestartShutdown {
                    membership: self.member.membership(),
                    target: request.epoch,
                },
                txn,
            );
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn runtime_storage(&self) -> RuntimeStorage {
        *self
            .runtime_storage
            .lock()
            .expect("runtime-storage mutex poisoned")
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn record_runtime_storage(&self, storage: RuntimeStorage) {
        *self
            .runtime_storage
            .lock()
            .expect("runtime-storage mutex poisoned") = storage;
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn observation_gate(&self) -> ObservationGate {
        self.current_observation_gate()
    }

    /// Installs a probe reporting every gate capture made through this scope.
    #[cfg(any(test, feature = "test-util"))]
    pub fn probe_gate_captures(&self) -> std::sync::mpsc::Receiver<GateCapture> {
        let (sender, receiver) = std::sync::mpsc::channel();
        *self
            .gate_capture_probe
            .lock()
            .expect("gate capture probe mutex poisoned") = Some(sender);
        receiver
    }

    #[cfg(any(test, feature = "test-util"))]
    fn report_gate_capture(&self, capture: GateCapture) {
        if let Some(probe) = &*self
            .gate_capture_probe
            .lock()
            .expect("gate capture probe mutex poisoned")
        {
            // The probe channel is unbounded, so reporting never blocks and
            // cannot reorder the acquisition it announces.
            let _ = probe.send(capture);
        }
    }

    fn current_observation_gate(&self) -> ObservationGate {
        self.member.current_observation_gate()
    }

    fn adopt_observation_gate(
        &self,
        parent: &ScopeCell,
        gate: &ObservationGate,
        txn: &mut ObservationTxn<'_>,
    ) {
        debug_assert!(
            !std::ptr::eq(self, parent),
            "a scope cannot adopt from itself"
        );
        // The caller holds `gate` through `parent.with_observation_gate`.
        // Re-homing the parent would first have to acquire that same gate, so
        // rereading its installed pointer here cannot race a parent handoff.
        debug_assert!(
            gate.same_gate(&parent.current_observation_gate()),
            "observation gates are adopted only in the parent-to-child direction"
        );
        loop {
            let current = self.current_observation_gate();
            if current.same_gate(gate) {
                return;
            }
            debug_assert!(
                self.dynamic_route_in(txn).is_none(),
                "a scope with a live dynamic route is never re-homed"
            );

            #[cfg(any(test, feature = "test-util"))]
            self.report_gate_capture(GateCapture::Adoption);

            // An operation that passed `with_observation_gate`'s pointer
            // check may finish its complete edge before handoff. An operation
            // that merely captured this obsolete gate retries after acquiring
            // it and observing the replacement.
            let current_guard = current.lock();
            if current.same_gate(&self.current_observation_gate()) {
                self.member.install_observation_gate_locked(&current, gate);
                self.adopt_descendant_observation_gates_locked(&current, gate, txn);
                drop(current_guard);
                return;
            }
            drop(current_guard);
        }
    }

    pub fn adopt_child_observation_gate(
        self: &Arc<Self>,
        member: &MemberCell,
        child: Option<&ScopeCell>,
        txn: &mut ObservationTxn<'_>,
    ) {
        let gate = self.current_observation_gate();
        if let Some(child) = child {
            child.adopt_observation_gate(self, &gate, txn);
        } else {
            member.adopt_observation_gate(&gate, txn);
        }
    }

    /// Re-homes a resident subtree while its prior tree gate is held. The
    /// destination gate is also held, so observers cannot enter either tree
    /// while the handoff is installed recursively. Walking residents is
    /// exhaustive here: a reserved dynamic slot requires the live route that
    /// only a started driver installs, while gate adoption happens before
    /// that driver can run, and no running scope is subsequently re-homed.
    fn adopt_descendant_observation_gates_locked(
        &self,
        previous: &ObservationGate,
        gate: &ObservationGate,
        _txn: &mut ObservationTxn<'_>,
    ) {
        let descendants = self
            .current_children()
            .iter()
            .map(|resident| resident.projection.clone())
            .collect::<Vec<_>>();
        for descendant in descendants {
            descendant
                .member
                .install_observation_gate_locked(previous, gate);
            if let Some(scope) = descendant.scope {
                scope.adopt_descendant_observation_gates_locked(previous, gate, _txn);
            }
        }
    }

    /// Runs against the current resident-tree observation gate. Adoption can
    /// race an early pre-start observer, so an obsolete-gate acquisition is
    /// detected and retried before the operation enters its critical section.
    pub fn with_observation_gate<R>(
        &self,
        operation: impl FnOnce(&mut ObservationTxn<'_>) -> R,
    ) -> R {
        let mut operation = Some(operation);
        loop {
            let gate = self.current_observation_gate();
            #[cfg(any(test, feature = "test-util"))]
            self.report_gate_capture(GateCapture::Observation);
            let guard = gate.lock();
            if gate.same_gate(&self.current_observation_gate()) {
                let operation = operation
                    .take()
                    .expect("observation operation runs exactly once");
                let mut txn = ObservationTxn::new(guard);
                let result = operation(&mut txn);
                drop(txn);
                return result;
            }
            drop(guard);
        }
    }

    pub fn set_state(&self, state: ScopeState) {
        self.with_observation_gate(|txn| self.set_state_locked(state, txn));
    }

    pub fn set_state_and_startup(&self, state: ScopeState, startup: Result<(), StartupError>) {
        self.with_observation_gate(|txn| {
            self.set_startup_locked(startup, txn);
            self.set_state_locked(state, txn);
        });
    }

    fn set_state_locked(&self, state: ScopeState, txn: &mut ObservationTxn<'_>) {
        if matches!(state, ScopeState::Draining | ScopeState::StartupFailed)
            && let Some(route) = self.dynamic_route_in(txn)
        {
            route.close_admission(txn);
        }
        self.observation.record.modify_silently(|record| {
            record.state = state.clone();
        });
        txn.pulse(&self.observation.record);
        txn.pulse(&self.member.record);
        self.emit_locked(txn, LifecycleEventKind::ScopeState { state });
    }

    pub fn set_observation_config(&self, strategy: Strategy, intensity: Intensity) {
        self.with_observation_gate(|wakes| {
            *self
                .observation
                .config
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = ObservationConfig {
                strategy,
                intensity,
            };
            self.publish_snapshot_chain_locked(wakes);
        });
    }

    pub fn set_child_removing_locked(&self, member: &MemberCell, txn: &mut ObservationTxn<'_>) {
        if member.record().membership_status == MembershipStatus::Removing {
            return;
        }
        member.update_locked(txn, |record| {
            record.membership_status = MembershipStatus::Removing;
        });
        self.publish_snapshot_chain_locked(txn);
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn transition_child(
        &self,
        member: &MemberCell,
        update: impl FnOnce(&mut MemberRecord),
        event: Option<LifecycleEventKind>,
    ) {
        self.with_observation_gate(|wakes| {
            member.update_locked(wakes, update);
            if let Some(event) = event {
                self.emit_locked(wakes, event);
            } else {
                self.publish_snapshot_chain_locked(wakes);
            }
        });
    }

    pub fn transition_child_stage(
        &self,
        member: &MemberCell,
        transition: MemberTransition,
        event: Option<LifecycleEventKind>,
    ) {
        // Routed through `transition_locked` rather than a record-only update
        // so a restart schedule's displaced exit leaves the gate on this path
        // too.
        self.with_observation_gate(|wakes| {
            member.transition_locked(wakes, transition);
            if let Some(event) = event {
                self.emit_locked(wakes, event);
            } else {
                self.publish_snapshot_chain_locked(wakes);
            }
        });
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn emit(&self, event: LifecycleEventKind) {
        self.with_observation_gate(|wakes| self.emit_locked(wakes, event));
    }

    pub fn publish_child_restart(
        &self,
        member: &MemberCell,
        total_restarts: TotalRestarts,
        transition: MemberTransition,
        exited: LifecycleEventKind,
        scheduled: LifecycleEventKind,
    ) {
        self.with_observation_gate(|wakes| {
            self.observation.record.modify_silently(|scope| {
                scope.total_restarts = total_restarts;
            });
            wakes.pulse(&self.observation.record);
            member.transition_locked(wakes, transition);
            self.emit_locked(wakes, exited);
            self.emit_locked(wakes, scheduled);
        });
    }

    pub fn terminalize_child(
        &self,
        member: &MemberCell,
        exit: Exit,
        exited_incarnation: Option<Incarnation>,
        startup: StartupDisposition,
    ) -> bool {
        self.with_observation_gate(|wakes| {
            let record = member.record();
            if matches!(record.stage, MemberStage::Terminal(_)) {
                return false;
            }
            // Nested publication and closure are reached only through this
            // residency lookup, so a supervised child must still be resident
            // here when it is terminalized. That holds because residency is
            // installed before the child can be spawned (`set_admitted_children`
            // for a planned incarnation, `admit_child` before dynamic
            // `spawn_child`) and is only withdrawn by pruning, which always
            // follows terminality. A child that has already left residency
            // owns its nested scope through `SlotCell::terminalize_never_started`
            // instead.
            let resident = self
                .current_children()
                .iter()
                .find(|resident| resident.projection.member.membership() == member.membership())
                .map(|resident| resident.projection.clone());
            debug_assert!(
                resident.is_some(),
                "a supervised terminal child must remain in parent residency"
            );
            let nested = resident.and_then(|resident| resident.scope);
            let terminal_exit = member.terminalize_locked(exit, startup, wakes);
            self.evict_child_identity(member);
            if record.last_incarnation.is_none()
                && let Some(scope) = &nested
            {
                scope.publish_stopped_locked(wakes, StopReason::NeverStarted, None, None);
            }
            if let Some(incarnation) = exited_incarnation {
                self.emit_locked(
                    wakes,
                    LifecycleEventKind::Exited {
                        id: member.id().clone(),
                        membership: member.membership(),
                        incarnation,
                        exit: terminal_exit,
                    },
                );
            } else {
                // Terminals without a current incarnation have no Exited
                // event to carry snapshot publication. Publish the final
                // parent projection explicitly before nested observation
                // closes.
                self.publish_snapshot_chain_locked(wakes);
            }
            if let Some(scope) = nested
                && matches!(scope.record().state, ScopeState::Stopped { .. })
            {
                // A parent fallback can terminalize a live nested membership
                // before cancellation drops the nested driver. Keep that
                // scope's stream open until its own epilogue publishes the
                // final Stopped record; otherwise waiters would receive a
                // terminal stream carrying Starting/Running as its payload.
                scope.close_observation_locked(wakes);
            }
            true
        })
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn prune_child(&self, member: &MemberCell) -> bool {
        self.with_observation_gate(|wakes| self.prune_child_locked(member, wakes))
    }

    pub fn prune_child_locked(&self, member: &MemberCell, txn: &mut ObservationTxn<'_>) -> bool {
        let membership = member.membership();
        let resident = {
            let mut children = self.current_children();
            let index = children
                .iter()
                .position(|child| child.projection.member.membership() == membership);
            index.map(|index| children.remove(index))
        };
        let Some(resident) = resident else {
            return false;
        };
        debug_assert_eq!(resident.projection.member.membership(), membership);
        resident.complete_removal(txn);
        true
    }

    pub fn snapshot(&self) -> Arc<ScopeSnapshot> {
        self.with_observation_gate(|_| self.snapshot_locked())
    }

    pub fn subscribe_snapshots(&self) -> SnapshotReceiver {
        self.with_observation_gate(|wakes| {
            let receiver = self
                .observation
                .snapshots
                .subscribe(self.snapshot_locked(), wakes);
            debug_assert!(
                !self.observation.closed.load(Ordering::Acquire)
                    || receiver.borrow_latest_and_closed().1,
                "closed snapshot state is installed before later subscriptions"
            );
            receiver
        })
    }

    pub fn subscribe_lifecycle(&self) -> LifecycleEvents {
        self.with_observation_gate(|_txn| {
            let events = self.observation.lifecycle.subscribe();
            debug_assert!(
                !self.observation.closed.load(Ordering::Acquire)
                    || self.observation.lifecycle.is_closed(),
                "closed lifecycle state is installed before later subscriptions"
            );
            events
        })
    }

    fn snapshot_locked(&self) -> Arc<ScopeSnapshot> {
        let record = self.record();
        let config = *self
            .observation
            .config
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let children = self.current_children();
        let children = children
            .iter()
            .map(|resident| self.child_snapshot_locked(&resident.projection))
            .collect::<Vec<_>>();
        Arc::new(ScopeSnapshot {
            state: record.state,
            kind: match self.flavor {
                ScopeFlavor::Ordered => ScopeKind::Ordered,
                ScopeFlavor::Dynamic => ScopeKind::Dynamic,
            },
            strategy: (self.flavor == ScopeFlavor::Ordered).then_some(config.strategy),
            intensity: config.intensity,
            total_restarts: record.total_restarts,
            lifecycle_seq: LifecycleSeq::new(
                self.observation.lifecycle_seq.load(Ordering::Acquire),
            ),
            children: children.into(),
        })
    }

    fn child_snapshot_locked(&self, child: &ResidentProjection) -> ChildSnapshot {
        let record = child.member.record();
        let options = child.member.options();
        let terminal = matches!(record.stage, MemberStage::Terminal(_));
        let nested = child.scope.as_ref().and_then(|scope| {
            (record.incarnation.is_some() || terminal).then(|| scope.snapshot_locked())
        });
        ChildSnapshot {
            id: child.member.id().clone(),
            membership: child.member.membership(),
            incarnation: record.incarnation,
            state: match record.stage {
                MemberStage::Reserved | MemberStage::Admitted => ChildState::Admitted,
                MemberStage::Starting => ChildState::Starting,
                MemberStage::Running => ChildState::Running,
                MemberStage::Restarting => ChildState::Restarting,
                MemberStage::Stopping => ChildState::Stopping,
                MemberStage::Terminal(exit) if record.startup_aborted => {
                    ChildState::StartupAborted { exit }
                }
                MemberStage::Terminal(exit) => ChildState::Stopped { exit },
            },
            last_exit: record.last_exit,
            membership_status: record.membership_status,
            restart_count: record.restart_count,
            restart_policy: options.restart,
            retention: options.retention,
            restart_at: record.restart_at,
            nested,
            scope_seq: child.scope.as_ref().map(|scope| {
                LifecycleSeq::new(scope.observation.lifecycle_seq.load(Ordering::Acquire))
            }),
        }
    }

    fn ancestors_locked(&self) -> Vec<Arc<ScopeCell>> {
        let mut ancestors = Vec::new();
        let mut current = self.parent();
        while let Some(scope) = current {
            current = scope.parent();
            ancestors.push(scope);
        }
        ancestors
    }

    fn publish_snapshot_chain_through_locked(
        &self,
        wakes: &mut ObservationTxn<'_>,
        ancestors: &[Arc<ScopeCell>],
    ) {
        self.observation
            .snapshots
            .publish(wakes, || self.snapshot_locked());
        for ancestor in ancestors {
            ancestor
                .observation
                .snapshots
                .publish(wakes, || ancestor.snapshot_locked());
        }
    }

    fn publish_snapshot_chain_locked(&self, wakes: &mut ObservationTxn<'_>) {
        let ancestors = self.ancestors_locked();
        self.publish_snapshot_chain_through_locked(wakes, &ancestors);
    }

    fn emit_locked(&self, wakes: &mut ObservationTxn<'_>, kind: LifecycleEventKind) {
        // Parent links cannot change under the resident-tree observation gate.
        // Resolve them once for snapshot and lifecycle propagation so one leaf
        // edge does not repeatedly lock every ancestor's parent mutex.
        let ancestors = self.ancestors_locked();
        // The resident-tree observation gate serializes every mint; the
        // atomic is the published watermark as well as the counter, avoiding
        // a second, provably uncontended lock on every lifecycle edge. The
        // mint is still a compare-and-swap so an emit that ever escaped the
        // gate could reorder events but never duplicate a sequence value.
        let seq = self
            .observation
            .lifecycle_seq
            .mint(Ordering::Release, Ordering::Relaxed);
        let Some(seq) = seq.map(LifecycleSeq::new) else {
            self.publish_snapshot_chain_through_locked(wakes, &ancestors);
            self.observation.lifecycle.publish_lagged(wakes, 1);
            for ancestor in &ancestors {
                ancestor.observation.lifecycle.publish_lagged(wakes, 1);
            }
            return;
        };
        self.publish_snapshot_chain_through_locked(wakes, &ancestors);

        let scope = self.member.membership();
        let mut event = LifecycleEvent {
            scope_path: Vec::new(),
            scope,
            seq,
            kind,
        };
        self.observation.lifecycle.publish(wakes, event.clone());
        let mut child_id = self.member.id().clone();
        for ancestor in ancestors {
            event.scope_path.insert(0, child_id);
            child_id = ancestor.member.id().clone();
            ancestor.observation.lifecycle.publish(wakes, event.clone());
        }
    }

    fn close_observation_locked(&self, wakes: &mut ObservationTxn<'_>) {
        if self.observation.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        // Closure follows the final state/snapshot/event publication performed
        // by the caller while this same observation gate remains held.
        self.observation
            .snapshots
            .close(wakes, || self.snapshot_locked());
        self.observation.lifecycle.close(wakes);
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn replace_observation_gate(&self, gate: ObservationGate) {
        *self
            .member
            .observation_gate
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = gate;
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn set_lifecycle_sequence(&self, current: u64) {
        self.observation
            .lifecycle_seq
            .set(current, Ordering::Relaxed);
    }

    pub fn set_startup(&self, startup: Result<(), StartupError>) {
        self.with_observation_gate(|txn| self.set_startup_locked(startup, txn));
    }

    fn set_startup_locked(&self, startup: Result<(), StartupError>, txn: &mut ObservationTxn<'_>) {
        let mut published = false;
        self.observation.record.modify_silently(|record| {
            if record.startup.is_none() {
                record.startup = Some(startup);
                published = true;
            }
        });
        if published {
            txn.pulse(&self.member.record);
            txn.pulse(&self.observation.record);
        }
    }

    pub fn begin_incarnation(&self, state: ScopeState) -> Option<Epoch> {
        debug_assert!(
            matches!(state, ScopeState::Starting),
            "a fresh incarnation publishes its lifecycle machine's initial state"
        );
        self.with_observation_gate(|wakes| {
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            let epoch = control.epochs.begin()?;
            // The idle epoch plane pairs only with a settled projection:
            // `Unstarted` before any mint, `Stopped` after every finish. That
            // pairing is what lets `settled` treat terminal membership
            // plus a settled projection as final without stranding a scope
            // that still owns a live incarnation.
            debug_assert!(
                matches!(
                    self.record().state,
                    ScopeState::Unstarted | ScopeState::Stopped { .. }
                ),
                "an idle scope projection is Unstarted or Stopped before a fresh incarnation mints"
            );
            self.observation.record.modify_silently(|record| {
                record.total_restarts = TotalRestarts::ZERO;
                record.startup = None;
                record.state = state.clone();
            });
            // Hold epoch ownership through its observation projection. A
            // stale finish and a newer begin can no longer cross these two
            // state planes in opposite orders.
            drop(control);
            wakes.pulse(&self.observation.record);
            wakes.pulse(&self.member.record);
            self.emit_locked(wakes, LifecycleEventKind::ScopeState { state });
            Some(epoch)
        })
    }

    pub fn finish_incarnation(&self, epoch: Epoch, reason: StopReason) {
        self.finish_incarnation_with_terminal(epoch, reason, None);
    }

    pub fn finish_root_incarnation(&self, epoch: Epoch, reason: StopReason, exit: Exit) {
        self.finish_incarnation_with_terminal(epoch, reason, Some(exit));
    }

    fn finish_incarnation_with_terminal(
        &self,
        epoch: Epoch,
        reason: StopReason,
        terminal_exit: Option<Exit>,
    ) {
        self.with_observation_gate(|wakes| {
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            if !control.epochs.finish(epoch) {
                // A stale driver must not overwrite the observation
                // projection of a newer live incarnation. Membership
                // terminality is not part of that projection: whoever owns a
                // terminal exit still publishes it exactly once, so declining
                // the epoch can never strand `wait_terminal`.
                drop(control);
                if let Some(exit) = terminal_exit {
                    self.member
                        .terminalize_locked(exit, StartupDisposition::Unchanged, wakes);
                    wakes.pulse(&self.member.record);
                    wakes.pulse(&self.observation.record);
                    self.close_observation_locked(wakes);
                }
                return;
            }
            if control
                .shutdown
                .is_some_and(|request| request.epoch <= epoch)
            {
                control.shutdown = None;
            }
            if control.force.is_some_and(|request| request.epoch <= epoch) {
                control.force = None;
            }
            let terminal = terminal_exit.is_some();
            let membership_terminal =
                matches!(self.member.record().stage, MemberStage::Terminal(_));
            self.publish_stopped_locked(wakes, reason, terminal_exit, Some(control));
            if terminal || membership_terminal {
                // A parent-driver fallback may have terminalized this nested
                // membership while its live scope epilogue was still
                // pending. The epilogue owns the final Stopped projection and
                // closes observation only after publishing it.
                self.close_observation_locked(wakes);
            }
        });
    }

    pub fn finish_live_root_incarnation(&self, reason: StopReason, exit: Exit) {
        let epoch = {
            let control = self.control.lock().expect("scope control mutex poisoned");
            control.epochs.live_epoch()
        };
        if let Some(epoch) = epoch {
            self.finish_root_incarnation(epoch, reason, exit);
        } else {
            self.with_observation_gate(|wakes| {
                self.publish_stopped_locked(wakes, reason, Some(exit), None);
                self.close_observation_locked(wakes);
            });
        }
    }

    pub fn request_shutdown(&self) -> Option<Epoch> {
        self.with_observation_gate(|txn| {
            let control = self.control.lock().expect("scope control mutex poisoned");
            self.request_shutdown_locked(control, txn)
        })
    }

    /// [`Self::request_shutdown`] for destructors: tolerates a poisoned
    /// control mutex so a drop-path request cannot panic — and abort — on a
    /// thread that is already unwinding. Control holds plain request state,
    /// so overwriting a poisoner's partial update is no worse than any other
    /// racing request.
    pub fn request_shutdown_ignoring_poison(&self) -> Option<Epoch> {
        self.with_observation_gate(|txn| {
            let control = self
                .control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.request_shutdown_locked(control, txn)
        })
    }

    fn request_shutdown_locked(
        &self,
        mut control: MutexGuard<'_, ScopeControl>,
        txn: &mut ObservationTxn<'_>,
    ) -> Option<Epoch> {
        let RequestTarget {
            epoch: target,
            pending_incarnation,
        } = control.epochs.request_target()?;
        let published = control
            .shutdown
            .is_none_or(|request| request.epoch < target);
        if published {
            control.shutdown = Some(ScopeRequest {
                epoch: target,
                consumed: false,
            });
        }
        drop(control);
        if published {
            txn.pulse(&self.member.record);
            if pending_incarnation && let Some(parent) = self.parent() {
                parent.publish_control_event_locked(
                    ScopeControlEvent::RestartShutdown {
                        membership: self.member.membership(),
                        target,
                    },
                    txn,
                );
            }
        }
        Some(target)
    }

    fn publish_control_event_locked(&self, event: ScopeControlEvent, txn: &mut ObservationTxn<'_>) {
        self.control
            .lock()
            .expect("scope control mutex poisoned")
            .events
            .push_back(event);
        txn.pulse(&self.member.record);
    }

    pub fn take_control_events(&self) -> Vec<ScopeControlEvent> {
        if self
            .control
            .lock()
            .expect("scope control mutex poisoned")
            .events
            .is_empty()
        {
            return Vec::new();
        }
        self.with_observation_gate(|_txn| {
            self.control
                .lock()
                .expect("scope control mutex poisoned")
                .events
                .drain(..)
                .collect()
        })
    }

    pub fn has_pending_incarnation_shutdown(&self, target: Epoch) -> bool {
        let control = self.control.lock().expect("scope control mutex poisoned");
        control.shutdown.is_some_and(|request| {
            request.epoch == target
                && !request.consumed
                && control.epochs.request_is_pending(target)
        })
    }

    pub fn has_stop_request(&self, epoch: Epoch) -> bool {
        let control = self.control.lock().expect("scope control mutex poisoned");
        control
            .shutdown
            .is_some_and(|request| request.epoch == epoch)
            || control.force.is_some_and(|request| request.epoch == epoch)
    }

    pub fn take_shutdown_request(&self, epoch: Epoch) -> bool {
        let pending = self
            .control
            .lock()
            .expect("scope control mutex poisoned")
            .shutdown
            .is_some_and(|request| request.epoch == epoch && !request.consumed);
        if !pending {
            return false;
        }
        self.with_observation_gate(|_txn| {
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            match control.shutdown.as_mut() {
                Some(request) if request.epoch == epoch && !request.consumed => {
                    request.consumed = true;
                    true
                }
                _ => false,
            }
        })
    }

    pub fn force_shutdown(&self, epoch: Epoch) {
        self.with_observation_gate(|txn| {
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            if control.epochs.is_current(epoch) {
                control.force = Some(ScopeRequest {
                    epoch,
                    consumed: false,
                });
            }
            drop(control);
            txn.pulse(&self.member.record);
        });
    }

    pub fn take_force_request(&self, epoch: Epoch) -> bool {
        let pending = self
            .control
            .lock()
            .expect("scope control mutex poisoned")
            .force
            .is_some_and(|request| request.epoch == epoch && !request.consumed);
        if !pending {
            return false;
        }
        self.with_observation_gate(|_txn| {
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            match control.force.as_mut() {
                Some(request) if request.epoch == epoch && !request.consumed => {
                    request.consumed = true;
                    true
                }
                _ => false,
            }
        })
    }

    fn incarnation_complete(&self, epoch: Epoch) -> bool {
        let control = self.control.lock().expect("scope control mutex poisoned");
        control.epochs.finished(epoch)
    }

    fn membership_terminal(&self) -> bool {
        matches!(self.member.record().stage, MemberStage::Terminal(_))
    }

    fn joined(&self) -> bool {
        matches!(
            self.record().state,
            ScopeState::Stopped { .. } | ScopeState::Unstarted
        )
    }

    /// Whether a shutdown wait has crossed the finality fence for its target.
    ///
    /// Membership terminality alone is insufficient: parent-driver
    /// destruction publishes it before the aborted nested driver runs the
    /// scope epilogue that finishes the incarnation and publishes `Stopped`.
    /// `None` is used only by the entry check, before a target epoch exists.
    ///
    /// This predicate is strictly weaker than membership terminality, so
    /// shutdown liveness now rests on two structural invariants. First, every
    /// live epoch has exactly one owner — the pre-driver epoch guard before a
    /// scope runtime exists, the scope runtime itself afterwards — and both
    /// finish it from `Drop`, so an unsettled target always has a pending
    /// finisher. Second, an idle epoch plane implies a settled projection:
    /// `begin_incarnation` is the only mint and it publishes `Starting`
    /// under the control guard,
    /// while [`Self::finish_incarnation`] always publishes `Stopped` under
    /// that same guard, so `ScopeEpochs::Idle`/`Exhausted` can only pair with
    /// `Unstarted` (never begun) or `Stopped`. Together they mean the
    /// terminal-membership arm can never be the *only* reachable settlement
    /// for a scope that still owns work.
    pub fn settled(&self, epoch: Option<Epoch>) -> bool {
        epoch.is_some_and(|epoch| self.incarnation_complete(epoch))
            || (self.membership_terminal() && self.joined())
    }

    pub fn set_admitted_children(self: &Arc<Self>, children: Vec<ResidentProjection>) {
        self.with_observation_gate(|wakes| {
            self.clear_residents_locked(wakes);
            for child in children {
                self.admit_child_locked(child, wakes);
            }
        });
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn admit_child(self: &Arc<Self>, child: ResidentProjection) {
        self.with_observation_gate(|wakes| self.admit_child_locked(child, wakes));
    }

    pub fn admit_child_locked(
        self: &Arc<Self>,
        child: ResidentProjection,
        txn: &mut ObservationTxn<'_>,
    ) {
        let gate = self.current_observation_gate();
        if let Some(scope) = &child.scope {
            scope.adopt_observation_gate(self, &gate, txn);
            scope.set_parent(self, txn);
        } else {
            child.member.adopt_observation_gate(&gate, txn);
        }
        let id = child.member.id().clone();
        let membership = child.member.membership();
        child
            .member
            .transition_locked(txn, MemberTransition::Admitted);
        self.current_children()
            .push(ResidentChild::new(self, child));
        self.emit_locked(txn, LifecycleEventKind::Added { id, membership });
    }

    pub fn clear_residents(&self) {
        self.with_observation_gate(|wakes| self.clear_residents_locked(wakes));
    }

    pub fn clear_residents_locked(&self, wakes: &mut ObservationTxn<'_>) {
        let mut residents = {
            let mut children = self.current_children();
            std::mem::take(&mut *children)
        };
        // Disarm every drop fallback before publishing any edge. If an emit
        // unwinds, the untouched suffix no longer re-enters the non-reentrant
        // observation gate from ResidentChild::drop.
        let completions = residents
            .iter_mut()
            .map(ResidentChild::disarm_removal)
            .collect::<Vec<_>>();
        drop(residents);
        for completion in completions {
            ResidentChild::publish_removal(completion, wakes);
        }
    }

    pub fn set_dynamic_route(&self, route: Option<Arc<ErasedDynamicRoute>>) {
        self.with_observation_gate(|txn| {
            self.set_dynamic_route_locked(route, txn);
        });
    }

    pub fn set_dynamic_route_locked(
        &self,
        route: Option<Arc<ErasedDynamicRoute>>,
        txn: &mut ObservationTxn<'_>,
    ) {
        let previous = std::mem::replace(
            &mut *self
                .dynamic_route
                .lock()
                .expect("scope dynamic-route mutex poisoned"),
            route,
        );
        txn.defer(move || drop(previous));
        txn.pulse(&self.member.record);
    }

    pub fn dynamic_route_in(&self, _txn: &ObservationTxn<'_>) -> Option<Arc<ErasedDynamicRoute>> {
        self.dynamic_route
            .lock()
            .expect("scope dynamic-route mutex poisoned")
            .clone()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn dynamic_route(&self) -> Option<Arc<ErasedDynamicRoute>> {
        self.with_observation_gate(|txn| self.dynamic_route_in(txn))
    }

    pub fn signal(&self) -> &runtime::WatchSender<MemberRecord> {
        &self.member.record
    }

    pub async fn wait_started(&self) -> Result<(), StartupError> {
        let mut watcher = self.observation.record.watcher();
        loop {
            if let Some(result) = watcher.borrow_and_update_cloned().startup {
                return result;
            }
            watcher.changed().await;
        }
    }

    pub async fn wait_stopped(&self) -> StopReason {
        self.member.wait_terminal().await;
        // Parent-driver destruction can terminalize a nested membership
        // synchronously before the aborted nested driver runs its own scope
        // epilogue. Membership terminality is therefore the finality fence,
        // not proof that the scope record has already reached `Stopped`.
        let mut watcher = self.observation.record.watcher();
        loop {
            match watcher.borrow_and_update_cloned().state {
                ScopeState::Stopped { reason } => return reason,
                ScopeState::Unstarted => return StopReason::NeverStarted,
                ScopeState::Starting
                | ScopeState::Running
                | ScopeState::StartupFailed
                | ScopeState::Draining => watcher.changed().await,
            }
        }
    }

    /// Commits a stopped-scope projection monotonically and applies its
    /// optional member terminal edge under the resident-tree observation gate.
    ///
    /// Several owners can reach a stop verdict for one incarnation — a
    /// driver's drain epilogue, a join monitor's fallback, a never-started
    /// terminalization — so competing reasons resolve through
    /// `StopPrecedence`, never through arrival order. A publication commits
    /// only when it strictly outranks the recorded reason; equal or weaker
    /// verdicts are idempotent repeats that mutate nothing. An upgrade
    /// republishes the record, the snapshot and a corrected `ScopeState` edge,
    /// so the stream never ends on an event that disagrees with the final
    /// record (SPEC B.4's non-final-`Stopped` rule admits exactly this step).
    ///
    /// Member terminalization prepares mailbox teardown first; its deferred
    /// discharge is therefore queued before either member or scope pulses.
    /// `epoch_owner` carries a live incarnation's control ownership through
    /// both retained record mutations and is released before snapshot and
    /// lifecycle publication, preserving the stop transition's ownership
    /// boundary. A suppressed repeat still terminalizes the member and still
    /// releases the epoch: only the scope-record mutation, snapshot pulse and
    /// lifecycle edge are skipped. Because the ancestor snapshot chain is
    /// republished by `emit_locked`, a suppressed repeat publishes no ancestor
    /// snapshot either — a caller whose member terminal edge must reach an
    /// ancestor projection has to publish that itself. Observation closure
    /// remains a caller decision because a nested scope's final event must
    /// precede its parent's terminal event and only then close the nested
    /// streams; a subscriber attaching after the final event and before that
    /// closure therefore resolves by closure alone, as it already does on the
    /// stale-epoch path above.
    fn publish_stopped_locked(
        &self,
        wakes: &mut ObservationTxn<'_>,
        reason: StopReason,
        terminal_exit: Option<Exit>,
        epoch_owner: Option<MutexGuard<'_, ScopeControl>>,
    ) {
        let incoming = stop_reason_precedence(&reason);
        let state = ScopeState::Stopped { reason };
        let mut published = false;
        self.observation.record.modify_silently(|record| {
            if let ScopeState::Stopped { reason: recorded } = &record.state
                && incoming <= stop_reason_precedence(recorded)
            {
                return;
            }
            if record.startup.is_none() {
                record.startup = Some(Err(StartupError::ShutdownRequested));
            }
            record.state = state.clone();
            published = true;
        });
        if let Some(exit) = terminal_exit {
            self.member
                .terminalize_locked(exit, StartupDisposition::Unchanged, wakes);
        }
        drop(epoch_owner);
        wakes.pulse(&self.member.record);
        // `wait_started` must not observe terminal startup until the member
        // and incarnation-control planes are mutually consistent.
        if published {
            wakes.pulse(&self.observation.record);
            self.emit_locked(wakes, LifecycleEventKind::ScopeState { state });
        }
    }

    pub fn terminalize_never_started(&self) {
        self.with_observation_gate(|txn| self.terminalize_never_started_locked(txn));
    }

    pub fn terminalize_never_started_locked(&self, txn: &mut ObservationTxn<'_>) {
        if self.observation.closed.load(Ordering::Acquire) {
            return;
        }
        self.member
            .terminalize_locked(Exit::never_started(), StartupDisposition::Unchanged, txn);
        self.publish_stopped_locked(txn, StopReason::NeverStarted, None, None);
        self.close_observation_locked(txn);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::Arc,
    };

    use crate::{ChildId, MemberCell, ScopeCell, identity::ScopeIdentity, policy::ScopeFlavor};

    #[test]
    fn destructor_shutdown_tolerates_a_poisoned_control_mutex() {
        let id = ChildId::from("root");
        let mut identity = ScopeIdentity::new();
        let member = MemberCell::new(
            id.clone(),
            identity
                .mint_membership(&id)
                .expect("root membership is available"),
        );
        let scope = ScopeCell::new(member, ScopeFlavor::Dynamic, ScopeIdentity::new());

        let poison = Arc::clone(&scope);
        assert!(
            catch_unwind(AssertUnwindSafe(move || {
                let _control = poison.control.lock().expect("control starts healthy");
                panic!("inject control poison");
            }))
            .is_err()
        );

        assert!(scope.request_shutdown_ignoring_poison().is_some());
    }
}
