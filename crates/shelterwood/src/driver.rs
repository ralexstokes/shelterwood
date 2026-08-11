//! Mutable runtime shell and shared handle state.

mod admission_control;
mod child;
mod events;
mod removal;
mod shutdown;
mod startup;

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

mod storage;

use storage::{ChildArena, ChildKey, Obligation};

use child::ChildRuntime;
#[cfg(test)]
use child::{ChildTerminality, discharge_child_terminality, report_slot};
use events::{
    ChildEvent, DeadlineKind, DriverEvent, MIN_EVENT_BATCH_LIMIT, Pending, collect_driver_events,
    restart_shutdown_work,
};
use removal::RemovalRequest;
pub(crate) use shutdown::shutdown_scope;

use crate::{
    Cancellation, ChildId, Exit, GracePhase, Incarnation, IntensityTrip, JitterSample, Membership,
    Readiness, ScopeState, ShutdownStraggler, ShutdownTimeout, StartupFailure, StartupFailureCause,
    admission::{NotAdmittingCause, ReserveError},
    cells::{
        MailboxControl, MemberStage, MemberTransition, ResidentProjection, ScopeCell,
        ScopeControlEvent, StartupDisposition,
    },
    deadline::Deadline,
    engine::{
        ArbitrationClass, ChildCompletionState, DeadlineHandle, DeadlineQueue, Epoch, ExitDispatch,
        IncarnationRun, IntensityState, MembershipStatus, ReadinessEffect, ReadinessEvent,
        ReadinessGate, RestartState, ScopeLifecycle, ScopeMode, StopAction, StopLadder, arbitrate,
        dispatch_exit, schedule_restart,
    },
    exit::{
        RecordedOutcome, StartupError, StopReason, classify_disposal_panic, classify_exit,
        reconcile_recorded_outcomes,
    },
    identity::IncarnationCounter,
    observe::LifecycleEventKind,
    plan::{
        BuilderCore, ChildConstruction, ChildPlan, LowerError, RuntimeScopePlan, ScopeFactory,
        ScopePlan, SlotCell,
    },
    policy::{DefaultsInheritance, ResolvedDefaults, ScopeFlavor},
    raw::{CatchUnwindFuture, RawRunContext, RawSpawn},
    runtime::{self, CompletionGatedLatch, Latch},
    task::{TaskContext, TaskContextLatches, TaskFactory},
};

use admission_control::{
    AdmissionRequest, DynamicControl, DynamicEntry, cancel_dynamic_reservation_parts,
    reject_admission_after_disposal,
};
pub(crate) use admission_control::{
    DynamicReservation, RemovalResponse, cancel_dynamic_reservation, remove_dynamic,
    reserve_dynamic, signal_fused_cancel, start_admission,
};

#[cfg(test)]
use crate::cells::{GateCapture, MemberCell, RuntimeStorage};
#[cfg(test)]
use admission_control::RemovalResponses;

pub(crate) struct SystemRun {
    pub(crate) root: Arc<ScopeCell>,
    driver: Option<runtime::JoinHandle<StopReason>>,
}

fn resident_projection(slot: &SlotCell) -> ResidentProjection {
    ResidentProjection::new(Arc::clone(&slot.member), slot.scope.clone())
}

impl SystemRun {
    pub(crate) async fn shutdown(&mut self, timeout: Duration) -> Result<(), ShutdownTimeout> {
        let result = shutdown_scope(Arc::clone(&self.root), timeout).await;
        self.join_driver().await;
        result
    }

    pub(crate) async fn wait(&mut self) -> StopReason {
        let reason = self.root.wait_stopped().await;
        self.join_driver().await;
        reason
    }

    async fn join_driver(&mut self) {
        let Some(driver) = self.driver.take() else {
            return;
        };
        // Only a crashed or cancelled driver leaves a live root incarnation to
        // classify; cancellation of the driver itself is the one join verdict
        // that proves the root observed cancellation.
        let (join, cancellation) = match runtime::join(driver).await {
            runtime::JoinOutcome::Ok { .. } => return,
            runtime::JoinOutcome::Panic { message } => (
                runtime::JoinOutcome::Panic { message },
                Cancellation::NotObserved,
            ),
            runtime::JoinOutcome::Cancelled => {
                (runtime::JoinOutcome::Cancelled, Cancellation::Observed)
            }
        };
        self.root.finish_live_root_incarnation(
            StopReason::ShutdownRequested,
            classify_exit(None, join, None, cancellation),
        );
    }
}

impl Drop for SystemRun {
    fn drop(&mut self) {
        // After a clean shutdown the root epochs are `Idle`, not `Exhausted`,
        // so this writes a real `ScopeRequest` — targeting the pending next
        // incarnation — into dead control state and pulses the member record.
        // That stays harmless only while watchers tolerate spurious wakes and
        // the driver we already joined was the sole consumer of scope
        // requests; nothing may come to treat a post-shutdown request as
        // meaningful. The poison-tolerant entry point keeps this drop from
        // panicking — and aborting — on an already-unwinding thread.
        let _ = self.root.request_shutdown_ignoring_poison();
    }
}

pub(crate) fn spawn_system(plan: ScopePlan) -> SystemRun {
    let root = Arc::clone(&plan.root);
    let monitor_root = Arc::clone(&root);
    let driver = runtime::spawn(async move { run_scope(plan, ScopeRole::Root).await });
    let lifecycle = runtime::spawn(async move {
        let (join, cancellation) = match runtime::join(driver).await {
            runtime::JoinOutcome::Ok { value } => return value,
            runtime::JoinOutcome::Panic { message } => (
                runtime::JoinOutcome::Panic { message },
                Cancellation::NotObserved,
            ),
            runtime::JoinOutcome::Cancelled => {
                (runtime::JoinOutcome::Cancelled, Cancellation::Observed)
            }
        };
        monitor_root.finish_live_root_incarnation(
            StopReason::ShutdownRequested,
            classify_exit(None, join, None, cancellation),
        );
        StopReason::ShutdownRequested
    });
    SystemRun {
        root,
        driver: Some(lifecycle),
    }
}

struct AncestorCommandLatches {
    shutdown: Latch,
    abort: Latch,
    abort_ack: Latch,
}

struct NestedScopeLatches {
    parent_ready: CompletionGatedLatch,
    ancestor: AncestorCommandLatches,
}

enum ScopeRole {
    Root,
    Nested(NestedScopeLatches),
}

impl ScopeRole {
    fn is_root(&self) -> bool {
        matches!(self, Self::Root)
    }

    fn parent_ready(&self) -> Option<&CompletionGatedLatch> {
        match self {
            Self::Root => None,
            Self::Nested(latches) => Some(&latches.parent_ready),
        }
    }

    fn ancestor(&self) -> Option<&AncestorCommandLatches> {
        match self {
            Self::Root => None,
            Self::Nested(latches) => Some(&latches.ancestor),
        }
    }
}

struct ScopeRuntime {
    root: Arc<ScopeCell>,
    defaults: ResolvedDefaults,
    intensity_policy: crate::Intensity,
    intensity: IntensityState,
    children: ChildArena<ChildRuntime>,
    // The index and count are maintained only at child installation/completion.
    // Driver wakes resolve a subject directly and test completion in O(1).
    child_keys: HashMap<Membership, ChildKey>,
    incomplete_children: usize,
    // Retained restart-shutdown facts whose subjects became inactive mid-batch.
    // `handle_exit` queues them here instead of expediting synchronously so the
    // retry re-enters arbitration on the next wake: an exit collected in the
    // same batch must first get the chance to trip intensity or fail startup.
    // Duplicates are harmless — expediting is idempotent.
    restart_shutdown_retries: Vec<(ChildKey, Epoch)>,
    events: runtime::UnboundedMpscSender<DriverEvent>,
    disposal_events: runtime::UnboundedMpscSender<DriverEvent>,
    deadlines: DeadlineQueue<DeadlineKind>,
    jitter: runtime::JitterRng,
    lifecycle: ScopeLifecycle,
    next_ordered_start: Option<ChildKey>,
    role: ScopeRole,
    dynamic: Option<Arc<DynamicControl>>,
    epoch: Epoch,
    ancestor_shutdown_seen: bool,
    ancestor_abort_seen: bool,
    hard_forced: bool,
    ordered_stop_progressing: bool,
    ordered_stop_cursor: Option<ChildKey>,
    ordered_stop_waiting: Option<ChildKey>,
    #[cfg(test)]
    ordered_stop_inspections: usize,
    completion: Option<ScopeCompletion>,
}

struct ScopeCompletion {
    reason: StopReason,
    root_exit: Option<Exit>,
}

impl Drop for ScopeRuntime {
    fn drop(&mut self) {
        let dynamic_entries = if let Some(dynamic) = &self.dynamic {
            let entries = self.root.with_observation_gate(|txn| {
                let entries = dynamic.close(&self.root, txn);
                self.root.set_dynamic_route_locked(None, txn);
                entries
            });
            Some(entries)
        } else {
            None
        };
        for child in self.children.values_mut() {
            if let Some(active) = child.active.take() {
                if let Some(mailbox) = &child.mailbox {
                    mailbox.freeze(active.incarnation);
                    if let Some(teardown) = mailbox.close(active.incarnation) {
                        runtime::dispose_detached(teardown);
                    }
                }
                active.shutdown.fire();
                active.abort.fire();
                active.abort_handle.abort();
            }
            // Driver destruction consumes the same owned terminality
            // completion as the orderly path. Its fallback publishes the
            // coarse kill verdict synchronously.
            child.terminality.discharge();
        }
        // Residency owns the matching Removed edges. Clearing the set after
        // terminality discharges them all before the scope's final event.
        self.root.clear_residents();
        // Dynamic entries own removal completions. Keep them armed until the
        // corresponding members are terminal and no longer resident.
        drop(dynamic_entries);
        self.children.clear();
        if !matches!(self.root.record().state, ScopeState::Stopped { .. }) {
            let completion = self.completion.take();
            let reason = completion
                .as_ref()
                .map(|completion| completion.reason.clone())
                .or_else(|| self.lifecycle.draining_reason().cloned())
                .unwrap_or(StopReason::ShutdownRequested);
            if let Some(exit) = completion.and_then(|completion| completion.root_exit) {
                self.root.finish_root_incarnation(self.epoch, reason, exit);
            } else {
                self.root.finish_incarnation(self.epoch, reason);
            }
        }
    }
}

impl ScopeRuntime {
    pub(super) fn insert_child(
        &mut self,
        child: ChildRuntime,
    ) -> Result<ChildKey, Box<ChildRuntime>> {
        let membership = child.slot.member.membership();
        let incomplete = child.is_incomplete();
        let key = self.children.insert(child)?;
        let replaced = self.child_keys.insert(membership, key);
        assert!(
            replaced.is_none(),
            "one live membership maps to exactly one child key"
        );
        if incomplete {
            self.incomplete_children = self
                .incomplete_children
                .checked_add(1)
                .expect("an in-memory child count fits in usize");
        }
        Ok(key)
    }

    #[cfg(test)]
    fn record_storage(&self) {
        self.root.record_runtime_storage(RuntimeStorage {
            children: self.children.len(),
            child_slots: self.children.storage_len(),
            deadlines: self.deadlines.len(),
            deadline_slots: self.deadlines.storage_len(),
        });
    }

    fn handle_admission(&mut self, mut request: AdmissionRequest) {
        let Some(control) = request.control.upgrade() else {
            request.complete(Err(ReserveError::NotAdmitting(NotAdmittingCause::Terminal)));
            return;
        };
        if self
            .dynamic
            .as_ref()
            .is_none_or(|current| !Arc::ptr_eq(current, &control))
            || self.lifecycle.is_draining()
            || self.lifecycle.startup_failed()
        {
            let cause = if self.lifecycle.is_draining() {
                NotAdmittingCause::Draining
            } else if self.lifecycle.startup_failed() {
                NotAdmittingCause::StartupFailed
            } else {
                NotAdmittingCause::NoLiveIncarnation
            };
            let (definition, removed) = self.root.with_observation_gate(|txn| {
                cancel_dynamic_reservation_parts(&self.root, &control, &request.slot, txn)
            });
            reject_admission_after_disposal(
                request,
                definition,
                removed,
                ReserveError::NotAdmitting(cause),
            );
            return;
        }
        if request.fused_cancel.as_ref().is_some_and(Latch::is_fired) {
            let (definition, removed) = self.root.with_observation_gate(|txn| {
                cancel_dynamic_reservation_parts(&self.root, &control, &request.slot, txn)
            });
            reject_admission_after_disposal(
                request,
                definition,
                removed,
                ReserveError::NotAdmitting(NotAdmittingCause::ReservationEnded),
            );
            return;
        }

        let (definition, resolved) = match request.slot.resolve_and_take_defined(&self.defaults) {
            Ok(Some(claimed)) => claimed,
            Ok(None) => {
                let (_, removed) = self.root.with_observation_gate(|txn| {
                    cancel_dynamic_reservation_parts(&self.root, &control, &request.slot, txn)
                });
                reject_admission_after_disposal(
                    request,
                    None,
                    removed,
                    ReserveError::NotAdmitting(NotAdmittingCause::ReservationEnded),
                );
                return;
            }
            Err(invalid) => {
                let (definition, removed) = self.root.with_observation_gate(|txn| {
                    cancel_dynamic_reservation_parts(&self.root, &control, &request.slot, txn)
                });
                reject_admission_after_disposal(
                    request,
                    definition,
                    removed,
                    ReserveError::InvalidPolicy(invalid),
                );
                return;
            }
        };
        let plan = ChildPlan::with_options(Arc::clone(&request.slot), definition, resolved);
        // Conversion can unwind while acquiring child identity or configuring
        // the mailbox. Keep that fallible work outside the control-plane lock
        // so driver teardown can still close reservations and removals.
        let mut child = ChildRuntime::from_plan(plan, &self.root);
        child.initial = false;
        enum AdmissionInstall {
            Admitted(ChildKey),
            Rejected {
                child: Box<ChildRuntime>,
                removed: Option<DynamicEntry>,
                error: ReserveError,
            },
        }
        let root = Arc::clone(&self.root);
        let installed = root.with_observation_gate(|txn| {
            let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
            let id = request.slot.member.id();
            let matches_reservation = state.entry(id).is_some_and(|entry| {
                entry.slot.member.membership() == request.slot.member.membership()
                    && entry.is_reserved()
            });
            if !matches_reservation || request.fused_cancel.as_ref().is_some_and(Latch::is_fired) {
                let removed = matches_reservation.then(|| state.remove(id, txn)).flatten();
                drop(state);
                request.slot.terminalize_never_started_locked(&root, txn);
                return AdmissionInstall::Rejected {
                    child: Box::new(child),
                    removed,
                    error: ReserveError::NotAdmitting(NotAdmittingCause::ReservationEnded),
                };
            }
            // The control-plane lock makes arena insertion and promotion one
            // state transition: an exact remover sees either the reservation
            // or a resident carrying its live arena key, never an unindexed
            // admitted intermediate.
            let key = match self.insert_child(child) {
                Ok(key) => key,
                Err(child) => {
                    let removed = state.remove(id, txn);
                    drop(state);
                    request.slot.terminalize_never_started_locked(&root, txn);
                    return AdmissionInstall::Rejected {
                        child,
                        removed,
                        error: ReserveError::IdentityExhausted,
                    };
                }
            };
            let entry = state
                .entry_mut(id)
                .expect("the matching reservation was just resolved");
            entry.promote(key, request.fused_cancel.take(), txn);
            root.admit_child_locked(resident_projection(&request.slot), txn);
            AdmissionInstall::Admitted(key)
        });
        let key = match installed {
            AdmissionInstall::Admitted(key) => key,
            AdmissionInstall::Rejected {
                mut child,
                removed,
                error,
            } => {
                // The entry's drop completes its removal response; preserve
                // the same terminality-before-completion ordering as every
                // other reservation-cancellation path. Definition disposal
                // is also complete before either waiter regains ownership.
                child.complete_terminality();
                let ChildRuntime { construction, .. } = *child;
                reject_admission_after_disposal(request, Some(construction), removed, error);
                return;
            }
        };
        #[cfg(test)]
        self.record_storage();
        request.complete(Ok(()));
        self.spawn_child(key);
    }
}

/// Owns a scope epoch and its matching initial lifecycle until a
/// `ScopeRuntime` has taken over teardown.
///
/// Nested lowering can await isolated disposal before a driver exists. If
/// that setup future is cancelled or unwinds, dropping this guard retires the
/// epoch so a later restart cannot mistake the still-live reservation for
/// identity exhaustion.
struct ScopeEpochGuard {
    scope: Arc<ScopeCell>,
    epoch: Option<Epoch>,
    lifecycle: ScopeLifecycle,
}

impl ScopeEpochGuard {
    fn begin(scope: &Arc<ScopeCell>) -> Option<Self> {
        let lifecycle = ScopeLifecycle::starting();
        let epoch = scope.begin_incarnation(lifecycle.state())?;
        Some(Self {
            scope: Arc::clone(scope),
            epoch: Some(epoch),
            lifecycle,
        })
    }

    fn lifecycle(&self) -> ScopeLifecycle {
        self.lifecycle.clone()
    }

    fn epoch(&self) -> Epoch {
        self.epoch
            .expect("an owned scope epoch remains available until transfer or finish")
    }

    fn finish(mut self, reason: StopReason) {
        let epoch = self
            .epoch
            .take()
            .expect("an owned scope epoch finishes at most once");
        self.scope.finish_incarnation(epoch, reason);
    }

    fn transfer(mut self) -> Epoch {
        self.epoch
            .take()
            .expect("an owned scope epoch transfers at most once")
    }
}

impl Drop for ScopeEpochGuard {
    fn drop(&mut self) {
        if let Some(epoch) = self.epoch.take() {
            self.scope
                .finish_incarnation(epoch, StopReason::ShutdownRequested);
        }
    }
}

async fn run_nested_tree(
    tree: BuilderCore,
    scope: Arc<ScopeCell>,
    inherited: ResolvedDefaults,
    latches: NestedScopeLatches,
) -> crate::ExitResult {
    let epoch = begin_nested_incarnation(&scope)?;
    run_nested_tree_with_epoch(tree, scope, inherited, latches, epoch).await
}

/// Begins the nested epoch before invoking its synchronous restartable
/// factory. Tokio cancellation cannot interrupt one in-progress poll, so a
/// factory that overlaps parent-driver destruction must already own the
/// epilogue that makes `wait_stopped` final.
async fn run_nested_factory(
    factory: ScopeFactory,
    scope: Arc<ScopeCell>,
    inherited: ResolvedDefaults,
    latches: NestedScopeLatches,
) -> crate::ExitResult {
    let epoch = begin_nested_incarnation(&scope)?;
    let tree = factory();
    run_nested_tree_with_epoch(tree, scope, inherited, latches, epoch).await
}

fn begin_nested_incarnation(scope: &Arc<ScopeCell>) -> Result<ScopeEpochGuard, crate::ExitError> {
    ScopeEpochGuard::begin(scope).ok_or_else(|| {
        let failure = StartupFailure {
            cause: StartupFailureCause::IdentityExhausted {
                id: scope.member.id().clone(),
            },
        };
        scope.set_startup(Err(StartupError::StartupFailed(failure.clone())));
        crate::ExitError::from_startup_failure(failure)
    })
}

async fn run_nested_tree_with_epoch(
    tree: BuilderCore,
    scope: Arc<ScopeCell>,
    inherited: ResolvedDefaults,
    latches: NestedScopeLatches,
    mut epoch: ScopeEpochGuard,
) -> crate::ExitResult {
    let plan = match tree.lower(inherited, Some(Arc::clone(&scope))) {
        Ok(plan) => plan,
        Err(error) => {
            let (cause, disposal) = match error {
                LowerError::Undefined { paths, disposal } => {
                    (StartupFailureCause::Lowering { undefined: paths }, disposal)
                }
                LowerError::IdentityExhausted { id, disposal } => {
                    (StartupFailureCause::IdentityExhausted { id }, disposal)
                }
                LowerError::InvalidPolicy { invalid, disposal } => {
                    // `InvalidPolicy::path` is relative to the scope being
                    // lowered, which here is this nested scope itself. The
                    // owning child id is already carried by the enclosing
                    // `StartupFailureCause::Child`, so prepending it here
                    // would double-count this frame.
                    (StartupFailureCause::InvalidPolicy(invalid), disposal)
                }
            };
            // Lowering never created a nested driver to own teardown. Keep
            // its isolated definitions attached to this incarnation until
            // they finish; hard-aborting the incarnation still detaches the
            // cancellation-safe disposal jobs.
            disposal.fired().await;
            let failure = StartupFailure { cause };
            // A lowering failure occurs before the driver loop exists, but it
            // still belongs to a live incarnation. Resolve every stop source
            // through the same monotone verdict lattice as the loop path.
            // Peek rather than consume: `finish_incarnation` clears both
            // epoch-tagged request latches after publishing the verdict.
            // The ancestor *abort* latch needs no separate arm: it is the
            // framework-abort edge, fired only by `StopAction::AbortFramework`,
            // and the stop ladder unconditionally passes through
            // `StopAction::Cancel` — which fires this same ancestor shutdown
            // latch — before it can reach that phase.
            if scope.has_stop_request(epoch.epoch()) || latches.ancestor.shutdown.is_fired() {
                // Mirror the loop's `Pending::Shutdown` arm: firing the
                // ancestor shutdown latch is what makes this scope's exit read
                // `Cancellation::Observed` at its parent, as a requested stop
                // must (§11). The latch is level-triggered, so re-firing an
                // ancestor-driven stop is a no-op.
                latches.ancestor.shutdown.fire();
                epoch.lifecycle.begin_drain(StopReason::ShutdownRequested);
            }
            // Both drain effects are deliberately discarded: this path
            // publishes no `Draining` edge because nothing was ever started to
            // drain, matching the pre-lattice behaviour of the `StartupFailed`
            // verdict it generalizes.
            epoch
                .lifecycle
                .begin_drain(StopReason::StartupFailed(failure));
            let reason = epoch
                .lifecycle
                .draining_reason()
                .cloned()
                .expect("pre-loop verdicts enter the drain lattice");
            let startup = match &reason {
                StopReason::ShutdownRequested => Err(StartupError::ShutdownRequested),
                StopReason::StartupFailed(failure) => {
                    Err(StartupError::StartupFailed(failure.clone()))
                }
                StopReason::Finished
                | StopReason::IntensityTripped(_)
                | StopReason::NeverStarted => {
                    unreachable!("lowering resolves only failure or shutdown verdicts")
                }
            };
            scope.set_startup(startup);
            epoch.finish(reason.clone());
            return reason.into_nested_result();
        }
    };
    run_scope_incarnation(plan.take_for_runtime(), ScopeRole::Nested(latches), epoch)
        .await
        .into_nested_result()
}

async fn run_scope(plan: ScopePlan, role: ScopeRole) -> StopReason {
    let root = Arc::clone(&plan.root);
    let Some(epoch) = ScopeEpochGuard::begin(&root) else {
        // Dropping the still-owned plan terminalizes every never-started
        // declaration and the root; no aliased driver epoch is created.
        drop(plan);
        return StopReason::NeverStarted;
    };
    run_scope_incarnation(plan.take_for_runtime(), role, epoch).await
}

/// Derives the membership index and incompleteness count for a freshly
/// assembled child arena. Production construction and the test builder both
/// call this, so the derived state cannot drift between them.
fn index_children(children: &ChildArena<ChildRuntime>) -> (HashMap<Membership, ChildKey>, usize) {
    let child_keys = children
        .iter()
        .map(|(key, child)| (child.slot.member.membership(), key))
        .collect();
    let incomplete_children = children
        .values()
        .filter(|child| child.is_incomplete())
        .count();
    (child_keys, incomplete_children)
}

async fn run_scope_incarnation(
    mut plan: RuntimeScopePlan,
    role: ScopeRole,
    epoch: ScopeEpochGuard,
) -> StopReason {
    let root = Arc::clone(&plan.root);
    if role.is_root() {
        root.with_observation_gate(|txn| {
            root.member
                .transition_locked(txn, MemberTransition::Running);
        });
    }
    // Both lanes are unbounded so their producers can publish synchronously.
    // Keep child lifecycle events separate from externally generated dynamic
    // control traffic: a large admission prefix must not strand the exit that
    // completes shutdown. Bound each lane's per-wake collection so traffic
    // cannot defer signals or deadlines indefinitely. The cap adds one
    // ordering surface: when a wake finds more than a full batch of primary
    // events, the deferred suffix (an intensity-tripping exit, say) is
    // processed one wake after control-lane admissions enqueued earlier.
    // `arbitrate` promises order only within a batch, and cross-pass
    // wall-clock inversion was already reachable through the forwarder, so
    // no promised order is violated.
    let event_batch_limit = plan
        .children
        .len()
        .saturating_mul(3)
        .max(MIN_EVENT_BATCH_LIMIT);
    let (events, mut event_receiver) = runtime::unbounded_mpsc();
    let (disposal_events, mut disposal_event_receiver) = runtime::unbounded_mpsc();
    let (dynamic, mut dynamic_event_receiver) = if plan.root.flavor == ScopeFlavor::Dynamic {
        let (dynamic_events, receiver) = runtime::unbounded_mpsc();
        (Some(DynamicControl::new(dynamic_events)), Some(receiver))
    } else {
        (None, None)
    };
    // Transfer children one at a time. The not-yet-converted suffix remains
    // owned by RuntimeScopePlan, while ChildRuntime::from_plan arms the current
    // child's obligation before fallible setup. Thus a panic at any point has
    // exactly one terminality owner for every child.
    let mut children = ChildArena::default();
    plan.children.reverse();
    while let Some(child) = plan.children.pop() {
        let child = ChildRuntime::from_plan(child, &root);
        if children.insert(child).is_err() {
            unreachable!("a fresh child-key domain accommodates an in-memory child collection");
        }
    }
    if let Some(control) = &dynamic {
        root.with_observation_gate(|txn| {
            control.register_initial(children.iter().map(|(key, child)| (&child.slot, key)), txn);
        });
    }
    let next_ordered_start = children.keys().next();
    let (child_keys, incomplete_children) = index_children(&children);
    let mut scope = ScopeRuntime {
        root: Arc::clone(&root),
        defaults: plan.defaults.clone(),
        intensity_policy: plan.config.intensity,
        intensity: IntensityState::default(),
        children,
        child_keys,
        incomplete_children,
        restart_shutdown_retries: Vec::new(),
        events,
        disposal_events,
        deadlines: DeadlineQueue::default(),
        jitter: runtime::JitterRng::from_system_entropy(),
        lifecycle: epoch.lifecycle(),
        next_ordered_start,
        role,
        dynamic,
        ancestor_shutdown_seen: false,
        ancestor_abort_seen: false,
        hard_forced: false,
        ordered_stop_progressing: false,
        ordered_stop_cursor: None,
        ordered_stop_waiting: None,
        #[cfg(test)]
        ordered_stop_inspections: 0,
        completion: None,
        // Transfer last: every fallible setup expression above remains
        // covered by the pre-driver guard, and completed construction moves
        // the raw epoch directly into ScopeRuntime's synchronous epilogue.
        epoch: epoch.transfer(),
    };
    plan.finish_transfer();

    // ScopeRuntime owns teardown before the route becomes public. If either
    // route notification or initial-child publication unwinds, its epilogue
    // closes dynamic state, terminalizes every child, and clears any resident
    // prefix. Install the fully keyed route before publishing Added so a
    // synchronous observer never sees membership without its control plane.
    if let Some(control) = &scope.dynamic {
        scope.root.set_dynamic_route(Some(control.clone()));
    }
    scope.root.set_admitted_children(
        scope
            .children
            .values()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    #[cfg(test)]
    scope.record_storage();

    match scope.root.flavor {
        ScopeFlavor::Ordered => scope.progress_startup(),
        ScopeFlavor::Dynamic => {
            let children: Vec<_> = scope.children.keys().collect();
            for child in children {
                scope.spawn_child(child);
            }
            scope.progress_startup();
        }
    }

    let mut signal = root.signal().watcher();
    loop {
        let mut pending = Vec::new();
        if root.take_shutdown_request(scope.epoch) {
            pending.push(Pending::Shutdown.classified());
        }
        for event in root.take_control_events() {
            if let Some(work) = scope.control_event_work(event) {
                pending.push(work);
            }
        }
        // Retries queued at the tail of a previous batch's exit handling
        // re-enter arbitration here, so a same-wake exit sorts ahead of them
        // and the execution-time suppression re-check observes its drain.
        for (child, target) in std::mem::take(&mut scope.restart_shutdown_retries) {
            pending.push(restart_shutdown_work(child, target));
        }
        if !scope.ancestor_shutdown_seen
            && scope
                .role
                .ancestor()
                .is_some_and(|latches| latches.shutdown.is_fired())
        {
            scope.ancestor_shutdown_seen = true;
            pending.push(Pending::AncestorShutdown.classified());
        }
        if !scope.ancestor_abort_seen
            && scope
                .role
                .ancestor()
                .is_some_and(|latches| latches.abort.is_fired())
        {
            scope.ancestor_abort_seen = true;
            pending.push(Pending::AncestorAbort.classified());
        }
        if root.take_force_request(scope.epoch) {
            // Force owns shutdown arbitration: readiness from the same wake
            // cannot publish Running after the stop boundary.
            pending.push(Pending::Force.classified());
        }
        let primary_batch_full =
            collect_driver_events(&mut event_receiver, event_batch_limit, &mut pending);
        let control_batch_full = dynamic_event_receiver.as_mut().is_some_and(|receiver| {
            collect_driver_events(receiver, event_batch_limit, &mut pending)
        });
        // Disposal completions drain after the primary and dynamic lanes, and `arbitrate`
        // sorts stably, so a `ConstructionDisposed` always trails every
        // same-class `Exited` collected in the same wake — even one produced
        // later. A disposal is therefore a batch-tail event: the exit it
        // trails may begin a drain first, after which
        // `handle_construction_disposed` sees `is_draining` and routes the
        // disposed child through stop progression instead of `fail_startup`.
        // That is a widening of an order that was already reachable, not a new
        // one: disposal runs on the blocking pool, so its completion never had
        // a fixed position relative to concurrent exits.
        while let Some(event) = runtime::unbounded_mpsc_try_recv(&mut disposal_event_receiver) {
            pending.push(Pending::Driver(event).classified());
        }
        let now = runtime::now();
        while let Some(deadline) = scope.deadlines.pop_due(now) {
            pending.push(Pending::Deadline(deadline).classified());
        }

        if pending.is_empty() {
            let ancestor_shutdown = scope
                .role
                .ancestor()
                .filter(|_| !scope.ancestor_shutdown_seen)
                .map(|latches| latches.shutdown.clone());
            let ancestor_abort = scope
                .role
                .ancestor()
                .filter(|_| !scope.ancestor_abort_seen)
                .map(|latches| latches.abort.clone());
            let ancestor_command = async move {
                let shutdown = async move {
                    if let Some(shutdown) = ancestor_shutdown {
                        shutdown.fired().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                };
                let abort = async move {
                    if let Some(abort) = ancestor_abort {
                        abort.fired().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                };
                let _ = runtime::select_two(shutdown, abort).await;
            };
            match runtime::wait_scope(
                runtime::ScopeWait {
                    signal: signal.changed(),
                    parent_shutdown: ancestor_command,
                },
                &mut event_receiver,
                dynamic_event_receiver.as_mut(),
                scope.deadlines.next(),
            )
            .await
            {
                runtime::ScopeWake::Signal | runtime::ScopeWake::ParentShutdown => continue,
                runtime::ScopeWake::Deadline => {
                    // A producer becoming ready at the same instant owns the
                    // tie over its deadline. Give tasks woken by that clock
                    // edge one turn to publish their retained readiness
                    // latch before collecting due registrations.
                    runtime::yield_now().await;
                    continue;
                }
                runtime::ScopeWake::Message(Some(event)) => {
                    pending.push(Pending::Driver(event).classified());
                }
                runtime::ScopeWake::ControlMessage(Some(event)) => {
                    pending.push(Pending::Driver(event).classified());
                }
                runtime::ScopeWake::Message(None) | runtime::ScopeWake::ControlMessage(None) => {
                    continue;
                }
            }
        }

        arbitrate(&mut pending);
        for (_, event) in pending {
            match event {
                Pending::Shutdown => {
                    if let Some(latches) = scope.role.ancestor() {
                        latches.shutdown.fire();
                        scope.ancestor_shutdown_seen = true;
                    }
                    scope.begin_drain(StopReason::ShutdownRequested);
                }
                Pending::RestartShutdown { child, target } => {
                    scope.expedite_restart_shutdown(child, target);
                }
                Pending::AncestorShutdown => {
                    scope.begin_drain(StopReason::ShutdownRequested);
                }
                Pending::AncestorAbort => {
                    if let Some(latches) = scope.role.ancestor() {
                        latches.abort_ack.fire();
                    }
                    // A scheduled framework driver recursively hard-drains
                    // and joins its children. Its parent only task-aborts it
                    // at the tidy-beat backstop when this acknowledgement is
                    // never published.
                    scope.force_all();
                }
                Pending::Force => {
                    scope.force_all();
                }
                Pending::Driver(DriverEvent::Removal(removal)) => scope.handle_removal(removal),
                Pending::Driver(DriverEvent::Admission(request)) => {
                    scope.handle_admission(request);
                }
                Pending::Driver(DriverEvent::Child(ChildEvent::Ready { child, incarnation })) => {
                    scope.handle_ready(child, incarnation);
                }
                Pending::Driver(DriverEvent::Child(ChildEvent::SelfStop {
                    child,
                    incarnation,
                })) => scope.handle_self_stop(child, incarnation),
                Pending::Driver(DriverEvent::Child(ChildEvent::Exited {
                    child,
                    incarnation,
                    recorded,
                    join,
                    cancellation,
                    readiness_signal_seen,
                })) => scope.handle_exit(
                    child,
                    incarnation,
                    recorded,
                    join,
                    cancellation,
                    readiness_signal_seen,
                ),
                Pending::Driver(DriverEvent::Child(ChildEvent::ConstructionDisposed {
                    child,
                    panic,
                })) => scope.handle_construction_disposed(child, panic),
                Pending::Deadline(deadline) => scope.handle_deadline(deadline),
            }
        }

        if let Some(reason) = scope.finish_if_ready() {
            let root_exit = scope.role.is_root().then(|| reason.root_exit());
            // ScopeRuntime's synchronous epilogue clears dynamic state,
            // discharges child obligations and residency, and only then
            // publishes the scope's terminal state.
            scope.completion = Some(ScopeCompletion {
                reason: reason.clone(),
                root_exit,
            });
            return reason;
        }

        // A full lane may still have a queued suffix. On a current-thread
        // runtime, immediately collecting the next batch would prevent the
        // child, timer, and helper tasks whose events this loop prioritizes
        // from running at all. Give those producers one scheduler turn before
        // returning to either saturated lane.
        if primary_batch_full || control_batch_full {
            runtime::yield_now().await;
        }
    }
}

#[cfg(test)]
pub(crate) use tests::exercise_queued_fused_drop_before_exit_dispatch;

#[cfg(test)]
mod tests;
