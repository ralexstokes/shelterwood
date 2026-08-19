//! Mutable runtime shell and shared handle state.

mod admission_control;
mod child;
mod events;
mod removal;
mod shutdown;
mod startup;

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::{Index, IndexMut},
    sync::{Arc, OnceLock},
    time::Instant,
};

mod storage;

use storage::Obligation;

use child::ChildRuntime;
#[cfg(test)]
use child::{ChildTerminality, discharge_child_terminality, report_slot};
use events::{
    ChildEvent, DeadlineKind, DriverEvent, EventLanes, MIN_EVENT_BATCH_LIMIT, Pending,
    collect_event_lanes, restart_shutdown_work, retain_woken_event,
};
use removal::RemovalRequest;
pub(crate) use shutdown::shutdown_scope;

use crate::{
    Cancellation, ChildId, DeadlineBudget, Exit, GracePhase, Incarnation, JitterSample, Readiness,
    ScopeState, ShutdownStraggler, ShutdownTimeout, StartupFailure, StartupFailureCause,
    admission::{NotAdmittingCause, ReserveError},
    cells::{
        MemberCell, MemberStage, MemberTransition, ResidentProjection, RetainedExit,
        RetainedStopReason, ScopeCell, ScopeControlEvent, StartupDisposition,
    },
    deadline::Deadline,
    engine::{
        ArbitrationClass, ChildKey, DeadlineHandle, DeadlineQueue, Effect as SupervisorEffect,
        Epoch, Event as SupervisorEvent, ExitDispatch, IncarnationRun, IntensityState,
        MembershipStatus, ReadinessEffect, ReadinessEvent, ReadinessGate, RestartState,
        ScopeLifecycle, ScopeMode, StopAction, StopLadder, SupervisorState, arbitrate,
        dispatch_exit, schedule_restart, step as supervisor_step,
    },
    exit::{
        RecordedOutcome, StartupError, StopReason, classify_disposal_panic, classify_exit,
        reconcile_recorded_outcomes, stop_reason_into_nested_result, stop_reason_root_exit,
        structured_startup_failure_error,
    },
    identity::IncarnationCounter,
    mailbox::MailboxControl,
    observe::LifecycleEventKind,
    plan::{
        BuilderCore, ChildConstruction, ChildPlan, LowerError, ScopeFactory, ScopePlan, SlotCell,
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
use crate::cells::{GateCapture, RuntimeStorage};
#[cfg(test)]
use admission_control::RemovalResponses;

pub(crate) struct SystemRun {
    pub(crate) root: Arc<ScopeCell>,
    driver: Option<runtime::JoinHandle<RetainedStopReason>>,
}

fn resident_projection(slot: &SlotCell) -> ResidentProjection {
    ResidentProjection::new(Arc::clone(&slot.member), slot.scope.clone())
}

impl SystemRun {
    pub(crate) async fn shutdown(
        &mut self,
        timeout: DeadlineBudget,
    ) -> Result<(), ShutdownTimeout> {
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
        if let Err(exit) = classify_retained_root_driver_join(runtime::join(driver).await) {
            self.root
                .finish_live_root_incarnation(StopReason::ShutdownRequested, exit);
        }
    }
}

fn classify_retained_root_driver_join(
    outcome: runtime::JoinOutcome<RetainedStopReason>,
) -> Result<RetainedStopReason, Exit> {
    let (join, cancellation) = match outcome {
        runtime::JoinOutcome::Ok { value } => return Ok(value),
        runtime::JoinOutcome::Panic { message } => (
            runtime::JoinOutcome::Panic { message },
            Cancellation::NotObserved,
        ),
        runtime::JoinOutcome::Cancelled => {
            (runtime::JoinOutcome::Cancelled, Cancellation::Observed)
        }
    };
    Err(classify_exit(None, join, None, cancellation))
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
    let driver = runtime::spawn(async move { run_scope(plan, ScopeRole::Root).await });
    let lifecycle = monitor_root_driver(Arc::clone(&root), driver);
    SystemRun {
        root,
        driver: Some(lifecycle),
    }
}

fn monitor_root_driver(
    monitor_root: Arc<ScopeCell>,
    driver: runtime::JoinHandle<RetainedStopReason>,
) -> runtime::JoinHandle<RetainedStopReason> {
    runtime::spawn(async move {
        match classify_retained_root_driver_join(runtime::join(driver).await) {
            Ok(reason) => reason,
            Err(exit) => {
                monitor_root.finish_live_root_incarnation(StopReason::ShutdownRequested, exit);
                RetainedStopReason::new(StopReason::ShutdownRequested)
            }
        }
    })
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
    // Runtime resources are keyed by the arena owned by `supervisor`; this
    // map carries no lifecycle or membership decisions.
    children: ChildResources<ChildRuntime>,
    supervisor: SupervisorState,
    supervisor_effects: Vec<SupervisorEffect>,
    // Retained restart-shutdown facts whose subjects became inactive mid-batch.
    // `handle_exit` queues them here instead of expediting synchronously so the
    // retry re-enters arbitration on the next wake: an exit collected in the
    // same batch must first get the chance to trip intensity or fail startup.
    // Duplicates are harmless — expediting is idempotent.
    restart_shutdown_retries: Vec<(ChildKey, Epoch)>,
    events: runtime::UnboundedMpscSender<DriverEvent>,
    disposal_events: runtime::UnboundedMpscSender<DriverEvent>,
    disposal_event_receiver: runtime::UnboundedMpscReceiver<DriverEvent>,
    /// Construction-disposal payloads lifted off the lane but not yet
    /// folded. Collection empties the lane into the arbitrated batch, so
    /// this is the only place a teardown transition sorted ahead of that
    /// batch can still find them. Plain framework data — the completion
    /// carries a message, never a user value.
    arrived_disposal_panics: BTreeMap<ChildKey, Option<runtime::DisposalPanic>>,
    deadlines: DeadlineQueue<DeadlineKind>,
    jitter: runtime::JitterRng,
    role: ScopeRole,
    dynamic: Option<Arc<DynamicControl>>,
    // Removing an initial member can complete startup. Hold its response
    // obligation until the batch epilogue has recomputed that aggregate, so
    // observing `Removed` implies the declared set already reflects the
    // committed shrink.
    pending_startup_removals: Vec<DynamicEntry>,
    epoch: Epoch,
    ancestor_shutdown_seen: bool,
    ancestor_abort_seen: bool,
    completion: Option<ScopeCompletion>,
    finished: Option<StopReason>,
    // Last by design: the supervisor, queued effects, completion, and
    // finished result can all retain a structured startup reason containing
    // a child's raw Exit. Their fields retire before these guards detach the
    // corresponding failed payloads.
    retained_exits: Vec<RetainedExit>,
}

struct ChildResources<T>(BTreeMap<ChildKey, T>);

impl<T> Default for ChildResources<T> {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

trait ChildKeyArg {
    fn child_key(self) -> ChildKey;
}

impl ChildKeyArg for ChildKey {
    fn child_key(self) -> ChildKey {
        self
    }
}

impl ChildKeyArg for &ChildKey {
    fn child_key(self) -> ChildKey {
        *self
    }
}

impl<T> ChildResources<T> {
    fn insert(&mut self, key: ChildKey, child: T) -> Option<T> {
        self.0.insert(key, child)
    }

    fn get(&self, key: impl ChildKeyArg) -> Option<&T> {
        self.0.get(&key.child_key())
    }

    fn get_mut(&mut self, key: impl ChildKeyArg) -> Option<&mut T> {
        self.0.get_mut(&key.child_key())
    }

    fn remove(&mut self, key: impl ChildKeyArg) -> Option<T> {
        self.0.remove(&key.child_key())
    }

    fn iter(&self) -> impl Iterator<Item = (ChildKey, &T)> {
        self.0.iter().map(|(key, child)| (*key, child))
    }

    fn values(&self) -> impl Iterator<Item = &T> {
        self.0.values()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn clear(&mut self) {
        self.0.clear();
    }
}

impl<T> Index<ChildKey> for ChildResources<T> {
    type Output = T;

    fn index(&self, key: ChildKey) -> &Self::Output {
        self.get(key).expect("live child resource key")
    }
}

impl<T> Index<&ChildKey> for ChildResources<T> {
    type Output = T;

    fn index(&self, key: &ChildKey) -> &Self::Output {
        self.get(key).expect("live child resource key")
    }
}

impl<T> IndexMut<ChildKey> for ChildResources<T> {
    fn index_mut(&mut self, key: ChildKey) -> &mut Self::Output {
        self.get_mut(key).expect("live child resource key")
    }
}

impl<T> IndexMut<&ChildKey> for ChildResources<T> {
    fn index_mut(&mut self, key: &ChildKey) -> &mut Self::Output {
        self.get_mut(key).expect("live child resource key")
    }
}

struct ScopeCompletion {
    reason: RetainedStopReason,
    root_exit: Option<RetainedExit>,
}

impl Drop for ScopeRuntime {
    fn drop(&mut self) {
        let mut panics = runtime::PanicAccumulator::default();
        let mut dynamic_entries = None;
        if let Some(dynamic) = &self.dynamic {
            panics.run(|| {
                self.root.with_observation_gate(|txn| {
                    // Retain the entries before the transaction flushes its
                    // wakes. If one is hostile, removal completion must still
                    // remain ordered after terminality and residency cleanup.
                    dynamic_entries = Some(dynamic.close(&self.root, txn));
                    self.root.set_dynamic_route_locked(None, txn);
                });
            });
        }
        // An exited child can be waiting only for retained user construction
        // to finish disposal. Its exit is already classified, so driver death
        // must publish that verdict before the terminality fallback gets a
        // chance to synthesize a coarse cancellation. The disposal job stays
        // detached; as on hard escalation, teardown does not wait for it and
        // cannot incorporate a destructor panic that has not completed at
        // publication. A completion that has already been reported is
        // available without waiting, however, so fold everything reported
        // before falling back to the stored verdict.
        self.drain_arrived_disposal_events(&mut panics);
        let child_keys: Vec<_> = self.children.iter().map(|(key, _)| key).collect();
        for key in child_keys {
            if self
                .children
                .get(key)
                .is_some_and(|child| child.pending_terminal.is_some())
            {
                panics.run(|| self.handle_construction_disposed(key, None));
            }
            let Some(child) = self.children.get_mut(key) else {
                // Terminal publication can reclaim a remove-retained dynamic
                // child; its terminality obligation was completed in that
                // path, so there is no fallback left to discharge here.
                continue;
            };
            if let Some(active) = child.active.take() {
                if let Some(mailbox) = &child.mailbox {
                    panics.run(|| mailbox.freeze(active.incarnation));
                    let mut teardown = None;
                    panics.run(|| teardown = mailbox.close(active.incarnation));
                    if let Some(teardown) = teardown {
                        runtime::dispose_detached(teardown);
                    }
                }
                panics.run(|| {
                    active.shutdown.fire();
                });
                panics.run(|| {
                    active.abort.fire();
                });
                panics.run(|| active.abort_handle.abort());
            }
            // Driver destruction consumes the same owned terminality
            // completion as the orderly path. Its fallback publishes the
            // coarse kill verdict synchronously.
            panics.run(|| child.terminality.discharge());
        }
        // Residency owns the matching Removed edges. Clearing the set after
        // terminality discharges them all before the scope's final event.
        panics.run(|| self.root.clear_residents());
        // Dynamic entries own removal completions. Keep them armed until the
        // corresponding members are terminal and no longer resident.
        panics.run(|| drop(dynamic_entries.take()));
        panics.run(|| self.children.clear());
        // Unconditional: the publisher is the idempotence point, joining this
        // verdict into the stopped-reason lattice, but epoch retirement is not
        // idempotent and has no other owner. Skipping the call on an
        // already-`Stopped` record would strand this incarnation's epoch.
        let completion = self.completion.take();
        let reason = completion
            .as_ref()
            .map(|completion| completion.reason.as_reason().clone())
            .or_else(|| self.supervisor.lifecycle().draining_reason().cloned())
            .unwrap_or(StopReason::ShutdownRequested);
        panics.run(|| {
            if let Some(exit) = completion.and_then(|completion| completion.root_exit) {
                self.root
                    .finish_root_incarnation(self.epoch, reason, exit.into_exit());
            } else {
                self.root.finish_incarnation(self.epoch, reason);
            }
        });
    }
}

impl ScopeRuntime {
    /// Moves the payload of every construction-disposal completion in a
    /// collected batch onto the scope.
    ///
    /// Arbitration dispatches a scope-shutdown transition ahead of the
    /// `ChildExit` class this completion belongs to, so the fallback in
    /// `force_child` runs while the completion is still undispatched. Once
    /// staged here it is reachable from that fallback; the batch entry keeps
    /// its position and dispatches the staged payload in arbitration order
    /// when no teardown claimed it first.
    fn stage_batch_disposal_panics(&mut self, pending: &mut [(ArbitrationClass, Pending)]) {
        for (_, event) in pending {
            if let Pending::Child(ChildEvent::ConstructionDisposed { child, panic }) = event {
                let panic = panic.take();
                self.stage_disposal_panic(*child, panic);
            }
        }
    }

    fn stage_disposal_panic(&mut self, child: ChildKey, panic: Option<runtime::DisposalPanic>) {
        let replaced = self.arrived_disposal_panics.insert(child, panic);
        debug_assert!(
            replaced.is_none(),
            "a terminal disposes its retained construction once, under a never-reused key"
        );
    }

    fn take_arrived_disposal_panic(&mut self, child: ChildKey) -> Option<runtime::DisposalPanic> {
        self.arrived_disposal_panics.remove(&child).flatten()
    }

    /// Folds every construction-disposal completion already reported,
    /// whether it is still on the lane or was staged out of the current
    /// batch.
    ///
    /// Non-blocking by construction, so a disposal still running on the
    /// blocking pool stays detached and its unknowable result cannot delay
    /// the kill path.
    fn drain_arrived_disposal_events(&mut self, panics: &mut runtime::PanicAccumulator) {
        while let Some(event) = runtime::unbounded_mpsc_try_recv(&mut self.disposal_event_receiver)
        {
            // The lane has one producer, which sends exactly one variant.
            // Assert rather than let a pattern-matching loop condition drop
            // an unexpected event and silently abandon the rest of the
            // drain; teardown runs this during an unwind, where a panic
            // would abort.
            debug_assert!(
                matches!(
                    event,
                    DriverEvent::Child(ChildEvent::ConstructionDisposed { .. })
                ),
                "the disposal lane carries only construction-disposal completions"
            );
            if let DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic }) = event {
                self.stage_disposal_panic(child, panic);
            }
        }
        // Folding empties the staging map, so the batch entries these
        // completions came from dispatch as no-ops against an already-taken
        // `pending_terminal`.
        while let Some(child) = self.arrived_disposal_panics.keys().next().copied() {
            let panic = self.take_arrived_disposal_panic(child);
            panics.run(|| self.handle_construction_disposed(child, panic));
        }
    }

    fn reduce(&mut self, event: SupervisorEvent) {
        supervisor_step(&mut self.supervisor, event, &mut self.supervisor_effects);
    }

    fn flush_supervisor_effects(&mut self) {
        while !self.supervisor_effects.is_empty() {
            let effects = std::mem::take(&mut self.supervisor_effects);
            for effect in effects {
                match effect {
                    SupervisorEffect::Admitted { .. } => {
                        unreachable!("admission consumes its key synchronously")
                    }
                    SupervisorEffect::StartChild { child } => self.spawn_child(child),
                    SupervisorEffect::StopChild { child } => self.begin_stop_child(child, None),
                    SupervisorEffect::ForceChild { child } => self.force_child(child),
                    SupervisorEffect::FinalizeRemoval { child } => self.finalize_removal(child),
                    SupervisorEffect::StartupCompleted { state } => {
                        self.publish_startup_complete(state);
                    }
                    SupervisorEffect::Finished { reason } => {
                        self.finished.get_or_insert(reason);
                    }
                    SupervisorEffect::StartupFailed { .. }
                    | SupervisorEffect::DrainStarted { .. } => {
                        unreachable!("the transition owner publishes its contextual result")
                    }
                }
            }
        }
    }

    fn settle_supervisor(&mut self) {
        loop {
            self.reduce(SupervisorEvent::Settle);
            if self.supervisor_effects.is_empty() {
                break;
            }
            self.flush_supervisor_effects();
            if self.finished.is_some() {
                break;
            }
        }
    }

    pub(super) fn insert_child(
        &mut self,
        child: ChildRuntime,
        initial: bool,
    ) -> Result<ChildKey, Box<ChildRuntime>> {
        let membership = child.slot.member.membership();
        let before = self.supervisor_effects.len();
        self.reduce(SupervisorEvent::Admit {
            membership,
            initial,
            start_immediately: false,
        });
        let key = match self.supervisor_effects.get(before) {
            Some(SupervisorEffect::Admitted { child }) => *child,
            None => return Err(Box::new(child)),
            Some(effect) => unreachable!("admission emitted {effect:?} before its key"),
        };
        self.supervisor_effects.remove(before);
        let replaced = self.children.insert(key, child);
        debug_assert!(
            replaced.is_none(),
            "the reducer's monotonic child key is new to runtime resources"
        );
        Ok(key)
    }

    #[cfg(test)]
    fn record_storage(&self) {
        self.root.record_runtime_storage(RuntimeStorage {
            children: self.children.len(),
            child_slots: self.children.len(),
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
            || self.supervisor.lifecycle().is_draining()
            || self.supervisor.lifecycle().startup_failed()
        {
            let cause = if self.supervisor.lifecycle().is_draining() {
                NotAdmittingCause::Draining
            } else if self.supervisor.lifecycle().startup_failed() {
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
            Some(claimed) => claimed,
            None => {
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
        };
        let plan = ChildPlan::with_options(Arc::clone(&request.slot), definition, resolved);
        // Conversion can unwind while acquiring child identity or configuring
        // the mailbox. Keep that fallible work outside the control-plane lock
        // so driver teardown can still close reservations and removals.
        let child = ChildRuntime::from_plan(plan, &self.root);
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
            let key = match self.insert_child(child, false) {
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
    // The core lifecycle retains raw structured stop reasons. Keep the
    // cells-layer guards last so unwind retires the reasons before detaching
    // their nested failed exits.
    retained_exits: Vec<RetainedExit>,
}

impl ScopeEpochGuard {
    fn begin(scope: &Arc<ScopeCell>) -> Option<Self> {
        let lifecycle = ScopeLifecycle::starting();
        let epoch = scope.begin_incarnation(lifecycle.state())?;
        Some(Self {
            scope: Arc::clone(scope),
            epoch: Some(epoch),
            lifecycle,
            retained_exits: Vec::new(),
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
        structured_startup_failure_error(failure)
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
            epoch.lifecycle.begin_drain({
                let reason = StopReason::StartupFailed(failure);
                RetainedExit::retain_stop_reason(&mut epoch.retained_exits, &reason);
                reason
            });
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
            return stop_reason_into_nested_result(reason);
        }
    };
    stop_reason_into_nested_result(
        run_scope_incarnation(plan, ScopeRole::Nested(latches), epoch).await,
    )
}

async fn run_scope(plan: ScopePlan, role: ScopeRole) -> RetainedStopReason {
    let root = Arc::clone(&plan.root);
    let Some(epoch) = ScopeEpochGuard::begin(&root) else {
        // Dropping the still-owned plan terminalizes every never-started
        // declaration and the root; no aliased driver epoch is created.
        drop(plan);
        return RetainedStopReason::new(StopReason::NeverStarted);
    };
    RetainedStopReason::new(run_scope_incarnation(plan, role, epoch).await)
}

async fn run_scope_incarnation(
    mut plan: ScopePlan,
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
    let (disposal_events, disposal_event_receiver) = runtime::unbounded_mpsc();
    let (dynamic, mut dynamic_event_receiver) = if plan.root.flavor == ScopeFlavor::Dynamic {
        let (dynamic_events, receiver) = runtime::unbounded_mpsc();
        (Some(DynamicControl::new(dynamic_events)), Some(receiver))
    } else {
        (None, None)
    };
    // Transfer children one at a time. The not-yet-converted suffix remains
    // owned by ScopePlan, while ChildRuntime::from_plan arms the current
    // child's obligation before fallible setup. Thus a panic at any point has
    // exactly one terminality owner for every child.
    let mut supervisor = SupervisorState::new(root.flavor, epoch.lifecycle());
    let mut supervisor_effects = Vec::new();
    let mut children = ChildResources::default();
    plan.children.reverse();
    while let Some(child) = plan.children.pop() {
        let child = ChildRuntime::from_plan(child, &root);
        let membership = child.slot.member.membership();
        supervisor_step(
            &mut supervisor,
            SupervisorEvent::Admit {
                membership,
                initial: true,
                start_immediately: false,
            },
            &mut supervisor_effects,
        );
        let Some(SupervisorEffect::Admitted { child: key }) = supervisor_effects.pop() else {
            unreachable!("a fresh child-key domain accommodates an in-memory child collection")
        };
        let replaced = children.insert(key, child);
        debug_assert!(replaced.is_none());
    }
    if let Some(control) = &dynamic {
        root.with_observation_gate(|txn| {
            control.register_initial(children.iter().map(|(key, child)| (&child.slot, key)), txn);
        });
    }
    let mut scope = ScopeRuntime {
        root: Arc::clone(&root),
        defaults: plan.defaults.clone(),
        intensity_policy: plan.intensity_policy(),
        intensity: IntensityState::default(),
        children,
        supervisor,
        supervisor_effects,
        restart_shutdown_retries: Vec::new(),
        events,
        disposal_events,
        disposal_event_receiver,
        arrived_disposal_panics: BTreeMap::new(),
        deadlines: DeadlineQueue::default(),
        jitter: runtime::JitterRng::new(),
        role,
        dynamic,
        pending_startup_removals: Vec::new(),
        ancestor_shutdown_seen: false,
        ancestor_abort_seen: false,
        completion: None,
        finished: None,
        retained_exits: Vec::new(),
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

    scope.settle_supervisor();

    let mut signal = root.signal().watcher();
    let mut pending = Vec::new();
    loop {
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
        let lane_batch_full = collect_event_lanes(
            EventLanes {
                primary: &mut event_receiver,
                control: dynamic_event_receiver.as_mut(),
                disposal: &mut scope.disposal_event_receiver,
            },
            event_batch_limit,
            &mut pending,
        );
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
                scope.deadlines.next_deadline(),
            )
            .await
            {
                runtime::ScopeWake::Signal
                | runtime::ScopeWake::ParentShutdown
                | runtime::ScopeWake::Message(None)
                | runtime::ScopeWake::ControlMessage(None) => {}
                runtime::ScopeWake::Deadline => {
                    // A producer becoming ready at the same instant owns the
                    // tie over its deadline. Give tasks woken by that clock
                    // edge one turn to publish their retained readiness
                    // latch before collecting due registrations.
                    runtime::yield_now().await;
                }
                runtime::ScopeWake::Message(Some(event))
                | runtime::ScopeWake::ControlMessage(Some(event)) => {
                    retain_woken_event(event, &mut pending);
                }
            }
            // Every wake re-enters the collection site above. Nothing is
            // dispatched from this arm.
            continue;
        }

        // Collection emptied the disposal lane into this batch. Stage the
        // completion payloads on the scope before arbitration hands a
        // teardown transition the chance to publish first.
        scope.stage_batch_disposal_panics(&mut pending);
        arbitrate(&mut pending);
        for (_, event) in pending.drain(..) {
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
                Pending::Removal(removal) => scope.handle_removal(removal),
                Pending::Admission(request) => {
                    scope.handle_admission(request);
                }
                Pending::Child(ChildEvent::Ready { child, incarnation }) => {
                    scope.handle_ready(child, incarnation);
                }
                Pending::Child(ChildEvent::SelfStop { child, incarnation }) => {
                    scope.handle_self_stop(child, incarnation)
                }
                Pending::Child(ChildEvent::Exited {
                    child,
                    incarnation,
                    recorded,
                    join,
                    cancellation,
                    readiness_signal_seen,
                }) => scope.handle_exit(
                    child,
                    incarnation,
                    recorded,
                    join,
                    cancellation,
                    readiness_signal_seen,
                ),
                Pending::Child(ChildEvent::ConstructionDisposed { child, panic }) => {
                    // Staging emptied the event; a teardown that folded this
                    // completion first leaves nothing to take.
                    let panic = panic.or_else(|| scope.take_arrived_disposal_panic(child));
                    scope.handle_construction_disposed(child, panic);
                }
                Pending::Deadline(deadline) => scope.handle_deadline(deadline),
            }
        }

        // Settlement is level-triggered from authoritative state after every
        // batch. Any transition that changes the startup aggregate therefore
        // gets the same recomputation point as terminal completion.
        scope.settle_supervisor();
        // A removal response is also an observation edge: SPEC §6 promises
        // that a returned `Removed` has already been incorporated into the
        // startup aggregate. Finalization retains starting-phase obligations
        // until the recomputation above establishes that order.
        scope.publish_startup_removals();
        if let Some(reason) = scope.finished.take() {
            let root_exit = scope
                .role
                .is_root()
                .then(|| RetainedExit::new(stop_reason_root_exit(&reason)));
            // ScopeRuntime's synchronous epilogue clears dynamic state,
            // discharges child obligations and residency, and only then
            // publishes the scope's terminal state.
            scope.completion = Some(ScopeCompletion {
                reason: RetainedStopReason::new(reason.clone()),
                root_exit,
            });
            return reason;
        }

        // A full lane may still have a queued suffix. On a current-thread
        // runtime, immediately collecting the next batch would prevent the
        // child, timer, and helper tasks whose events this loop prioritizes
        // from running at all. Give those producers one scheduler turn before
        // returning to any saturated lane.
        if lane_batch_full {
            runtime::yield_now().await;
        }
    }
}

#[cfg(test)]
pub(crate) use tests::exercise_queued_fused_drop_before_exit_dispatch;

#[cfg(test)]
mod tests;
