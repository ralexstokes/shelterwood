//! Mutable runtime shell and shared handle state.

mod admission_control;
mod child;
mod events;
mod incarnation;
mod nested;
mod removal;
mod report;
mod shutdown;
mod spawn;
mod startup;
mod stop;
mod system;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, OnceLock},
    time::Instant,
};

mod storage;

use storage::Obligation;

use child::{ActiveChild, ChildRuntime};
#[cfg(test)]
use child::{ChildTerminality, discharge_child_terminality};
use events::{
    ChildEvent, DeadlineKind, DriverEvent, EventLanes, MIN_EVENT_BATCH_LIMIT, Pending,
    collect_event_lanes,
};
use removal::RemovalRequest;
pub(crate) use shutdown::shutdown_scope;

use crate::{
    Cancellation, ChildId, DeadlineBudget, Exit, GracePhase, Incarnation, JitterSample, Readiness,
    ScopeState, ShutdownStraggler, ShutdownTimeout, StartupFailure, StartupFailureCause,
    cells::{
        Guarded, LifecycleEventKind, MemberCell, MemberStage, MemberTransition, ResidentProjection,
        RetainGuards, Retained, ScopeCell, ScopeControlEvent, StartupDisposition,
        classify_exit_retaining, reconcile_recorded_outcomes_retaining,
    },
    deadline::Deadline,
    engine::{
        ArbitrationClass, DeadlineHandle, DeadlineQueue, Epoch, ExitDispatch, IncarnationRun,
        IntensityState, MembershipStatus, ReadinessEffect, ReadinessEvent, ReadinessGate,
        RestartState, ScopeLifecycle, StopAction, StopLadder, arbitrate, dispatch_exit,
        schedule_restart,
    },
    exit::{
        RecordedOutcome, StartupError, StopReason, stop_reason_into_nested_result,
        stop_reason_root_exit,
    },
    identity::IncarnationCounter,
    mailbox::{MailboxBindToken, MailboxControl, MailboxEffectQueue},
    plan::{
        BuilderCore, ChildConstruction, ChildPlan, LowerError, ScopeFactory, ScopePlan, SlotCell,
    },
    policy::{DefaultsInheritance, Intensity, ResolvedDefaults, ScopeFlavor},
    raw::{CatchUnwindFuture, RawRunContext, RawSpawn},
    runtime::{self, CompletionGatedLatch, Latch},
    task::{TaskContext, TaskContextLatches, TaskFactory},
};
use shelterwood_core::supervisor::{
    ChildKey, Effect as SupervisorEffect, Event as SupervisorEvent, SupervisorState,
    admit as supervisor_admit, begin_drain as supervisor_begin_drain,
    fail_startup as supervisor_fail_startup, force as supervisor_force, step as supervisor_step,
};

use admission_control::{AdmissionRequest, DynamicControl, DynamicEntry};
pub(crate) use admission_control::{
    DynamicReservation, RemovalResponse, remove_dynamic, reserve_dynamic, signal_fused_cancel,
};

pub(crate) use admission_control::{LATCHED_REMOVAL_OUTCOME, LOST_ADMISSION_RESPONSE_ERROR};

#[cfg(test)]
use crate::cells::{GateCapture, RuntimeStorage};
#[cfg(test)]
use admission_control::AdmissionInstall;
#[cfg(test)]
use admission_control::RemovalResponses;
#[cfg(test)]
use incarnation::wait_for_scope_wake;
use incarnation::{ScopeEpochGuard, run_scope, run_scope_incarnation};
#[cfg(test)]
use nested::run_nested_tree_with_epoch;
use nested::{
    AncestorCommandLatches, NestedScopeLatches, NestedScopeStart, ScopeRole, nested_scope_start,
    run_nested_factory, run_nested_tree,
};
use report::report_slot;
use spawn::fire_shutdown_edges;
pub(crate) use system::{SystemRun, spawn_system};
#[cfg(test)]
use system::{classify_retained_root_driver_join, monitor_root_driver};

fn resident_projection(slot: &SlotCell) -> ResidentProjection {
    ResidentProjection::new(Arc::clone(&slot.member), slot.scope.clone())
}

struct ScopeRuntime {
    root: Arc<ScopeCell>,
    defaults: ResolvedDefaults,
    intensity_policy: crate::Intensity,
    intensity: IntensityState,
    // Runtime resources are keyed by the arena owned by `supervisor`; this
    // map carries no lifecycle or membership decisions.
    children: BTreeMap<ChildKey, ChildRuntime>,
    supervisor: SupervisorState,
    supervisor_effects: Vec<SupervisorEffect>,
    events: runtime::UnboundedMpscSender<DriverEvent>,
    disposal_events: runtime::UnboundedMpscSender<DriverEvent>,
    disposal_event_receiver: runtime::UnboundedMpscReceiver<DriverEvent>,
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
    completion: Option<Guarded<StopReason>>,
    finished: Option<StopReason>,
    // Last by design: the supervisor, queued effects, completion, and
    // finished result can all retain a structured startup reason containing
    // a child's raw Exit. Their fields retire before these guards detach the
    // corresponding failed payloads.
    retained_exits: Vec<Retained<Exit>>,
}

struct ScopeRuntimeWiring {
    root: Arc<ScopeCell>,
    defaults: ResolvedDefaults,
    intensity_policy: Intensity,
    children: BTreeMap<ChildKey, ChildRuntime>,
    supervisor: SupervisorState,
    events: runtime::UnboundedMpscSender<DriverEvent>,
    disposal_events: runtime::UnboundedMpscSender<DriverEvent>,
    disposal_event_receiver: runtime::UnboundedMpscReceiver<DriverEvent>,
    role: ScopeRole,
    dynamic: Option<Arc<DynamicControl>>,
}

#[cfg(test)]
struct ScopeRuntimeTestWiring {
    root: Arc<ScopeCell>,
    defaults: ResolvedDefaults,
    intensity_policy: Intensity,
    children: Vec<(ChildKey, ChildRuntime)>,
    lifecycle: ScopeLifecycle,
    next_ordered_start: Option<Option<ChildKey>>,
    events: runtime::UnboundedMpscSender<DriverEvent>,
    dynamic: Option<Arc<DynamicControl>>,
    hard_forced: bool,
}

/// Runs the synchronous fail-closed scope epilogue.
///
/// `pending_startup_removals` can reach this path only when a resumed
/// user-code panic unwinds the driver: no await point exists between retaining
/// one of those completions and the orderly batch epilogue that publishes it.
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
        // A disposing child already published its exit at dispatch and is
        // waiting only for its retained construction's release edge (SPEC
        // §11). Teardown keeps that verdict and stops waiting: the disposal
        // job stays detached, and its later completion finds nothing to join.
        let child_keys: Vec<_> = self.children.keys().copied().collect();
        for key in child_keys {
            if self.supervisor.is_disposing(key) {
                panics.run(|| self.handle_construction_disposed(key));
            }
            let Some(child) = self.children.get_mut(&key) else {
                // Terminal publication can reclaim a remove-retained dynamic
                // child; its terminality obligation was completed in that
                // path, so there is no fallback left to discharge here.
                continue;
            };
            if let Some(active) = child.active.take() {
                if let Some(mailbox) = &child.mailbox {
                    // Both control calls defer their effects into one queue, so
                    // a panic from either still leaves the other to run and the
                    // flush below to publish everything already collected.
                    let mut effects = MailboxEffectQueue::default();
                    let mut closed = None;
                    panics.run(|| mailbox.freeze(active.incarnation, &mut effects));
                    panics.run(|| closed = mailbox.close(active.incarnation, &mut effects));
                    panics.run(move || drop(effects));
                    if let Some(closed) = closed {
                        let (_token, teardown) = closed.into_parts();
                        runtime::dispose_detached(teardown);
                    }
                }
                panics.run(|| {
                    fire_shutdown_edges(&active.shutdown, active.framework_shutdown.as_ref());
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
        panics.run(|| self.publish_startup_removals());
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
            .map(|completion| completion.get().clone())
            .or_else(|| self.supervisor.lifecycle().draining_reason().cloned())
            .unwrap_or(StopReason::ShutdownRequested);
        // Root membership terminality is join-gated: the monitor owns it on
        // both successful and failed driver joins. The scope epilogue only
        // retires this incarnation and publishes its stopped projection, just
        // as it already does while unwinding. That keeps `wait_stopped` behind
        // the last point at which the join can change the final verdict.
        panics.run(|| self.root.finish_incarnation(self.epoch, reason));
    }
}

impl ScopeRuntime {
    fn new(wiring: ScopeRuntimeWiring, epoch: ScopeEpochGuard) -> Self {
        Self {
            root: wiring.root,
            defaults: wiring.defaults,
            intensity_policy: wiring.intensity_policy,
            intensity: IntensityState::default(),
            children: wiring.children,
            supervisor: wiring.supervisor,
            supervisor_effects: Vec::new(),
            events: wiring.events,
            disposal_events: wiring.disposal_events,
            disposal_event_receiver: wiring.disposal_event_receiver,
            deadlines: DeadlineQueue::default(),
            jitter: runtime::JitterRng::new(),
            role: wiring.role,
            dynamic: wiring.dynamic,
            pending_startup_removals: Vec::new(),
            ancestor_shutdown_seen: false,
            ancestor_abort_seen: false,
            completion: None,
            finished: None,
            retained_exits: Vec::new(),
            // Transfer last: every fallible setup expression above remains
            // covered by the pre-driver guard, and completed construction
            // moves the raw epoch directly into ScopeRuntime's synchronous
            // epilogue.
            epoch: epoch.transfer(),
        }
    }

    #[cfg(test)]
    fn for_test(wiring: ScopeRuntimeTestWiring, epoch: ScopeEpochGuard) -> Self {
        let mut supervisor = SupervisorState::new(wiring.root.flavor, wiring.lifecycle);
        let mut children = BTreeMap::new();
        for (expected, child) in wiring.children {
            let actual = supervisor_admit(&mut supervisor, child.slot.member.membership(), true);
            assert_eq!(actual, expected);
            let replaced = children.insert(actual, child);
            assert!(replaced.is_none(), "fixture child keys are unique");
        }
        if let Some(next) = wiring.next_ordered_start {
            supervisor.set_next_ordered_start_for_test(next);
        }
        supervisor.set_hard_forced_for_test(wiring.hard_forced);
        if let Some(control) = &wiring.dynamic {
            wiring.root.with_observation_gate(|txn| {
                control
                    .register_initial(children.iter().map(|(key, child)| (&child.slot, *key)), txn);
            });
        }
        let (disposal_events, disposal_event_receiver) = runtime::unbounded_mpsc();
        Self::new(
            ScopeRuntimeWiring {
                root: wiring.root,
                defaults: wiring.defaults,
                intensity_policy: wiring.intensity_policy,
                children,
                supervisor,
                events: wiring.events,
                disposal_events,
                disposal_event_receiver,
                role: ScopeRole::Root,
                dynamic: wiring.dynamic,
            },
            epoch,
        )
    }

    fn publish_initial_children(&self) {
        // ScopeRuntime owns teardown before the route becomes public. If
        // either route notification or initial-child publication unwinds, its
        // epilogue closes dynamic state, terminalizes every child, and clears
        // any resident prefix. Install the fully keyed route before publishing
        // Added so a synchronous observer never sees membership without its
        // control plane.
        if let Some(control) = &self.dynamic {
            self.root.set_dynamic_route(Some(control.clone()));
        }
        // Every slot here is a fresh reservation, so the projection reducer
        // accepts each `Admitted` edge. A refusal would leave the child out of
        // the residency while its driver record still expects it.
        let admitted = self.root.set_admitted_children(
            self.children
                .values()
                .map(|child| resident_projection(&child.slot))
                .collect(),
        );
        assert!(
            admitted,
            "initial children are admitted from their reservation exactly once"
        );
        #[cfg(test)]
        self.record_storage();
    }

    /// Stages a finished incarnation's completion for the driver to return.
    ///
    /// ScopeRuntime's synchronous epilogue clears dynamic state, discharges
    /// child obligations and residency, and only then publishes the stopped
    /// projection. For the root, the join monitor owns the later
    /// membership-terminal fence.
    fn take_completion(&mut self) -> Option<StopReason> {
        let reason = self.finished.take()?;
        self.completion = Some(Guarded::new(reason.clone()));
        Some(reason)
    }

    /// Acts on this incarnation's own consumed shutdown request.
    fn accept_shutdown_request(&mut self) {
        if let ScopeRole::Nested(nested) = &self.role {
            // Firing the child-facing latch is what makes this incarnation's
            // exit read `Cancellation::Observed` at its parent (§12).
            nested.child_shutdown.fire();
            // The ancestor arm's sole action is the same
            // `begin_drain(ShutdownRequested)` call immediately below, so
            // consuming its observation here suppresses only a duplicate
            // idempotent transition. Construction suppression still reads the
            // ancestor latch itself rather than this consumption flag.
            self.ancestor_shutdown_seen = true;
        }
        self.begin_drain(StopReason::ShutdownRequested);
    }

    fn reduce(&mut self, event: SupervisorEvent) {
        supervisor_step(&mut self.supervisor, event, &mut self.supervisor_effects);
    }

    fn flush_supervisor_effects(&mut self) {
        while !self.supervisor_effects.is_empty() {
            let effects = std::mem::take(&mut self.supervisor_effects);
            for effect in effects {
                match effect {
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

    pub(super) fn insert_child(&mut self, child: ChildRuntime, initial: bool) -> ChildKey {
        let key = supervisor_admit(
            &mut self.supervisor,
            child.slot.member.membership(),
            initial,
        );
        // `supervisor_admit` mints every key from this scope's monotonic
        // counter, which never repeats a value, so the insert cannot displace.
        let _ = self.children.insert(key, child);
        key
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
}

#[cfg(test)]
pub(crate) use tests::exercise_queued_fused_drop_before_exit_dispatch;

#[cfg(test)]
mod tests;
