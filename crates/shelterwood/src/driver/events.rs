use super::*;

pub(super) enum DriverEvent {
    Child(ChildEvent),
    Admission(AdmissionRequest),
    Removal(RemovalRequest),
}

pub(super) const MIN_EVENT_BATCH_LIMIT: usize = 64;
pub(super) enum ChildEvent {
    Ready {
        child: ChildKey,
        incarnation: Incarnation,
    },
    SelfStop {
        child: ChildKey,
        incarnation: Incarnation,
    },
    Exited {
        child: ChildKey,
        incarnation: Incarnation,
        recorded: Option<Retained<RecordedOutcome>>,
        join: runtime::JoinOutcome<()>,
        cancellation: Cancellation,
        readiness_signal_seen: bool,
    },
    ConstructionDisposed {
        child: ChildKey,
    },
}

pub(super) enum DeadlineKind {
    Readiness {
        child: ChildKey,
        incarnation: Incarnation,
    },
    Restart {
        child: ChildKey,
    },
    Stop {
        child: ChildKey,
        incarnation: Incarnation,
    },
}
pub(super) enum Pending {
    Shutdown,
    WindowStop { child: ChildKey, target: Epoch },
    AncestorShutdown,
    AncestorAbort,
    Force,
    Child(ChildEvent),
    Admission(AdmissionRequest),
    Removal(RemovalRequest),
    Deadline(DeadlineKind),
}

impl Pending {
    pub(super) fn class(&self) -> ArbitrationClass {
        match self {
            Self::Shutdown | Self::AncestorShutdown | Self::AncestorAbort | Self::Force => {
                ArbitrationClass::ScopeShutdown
            }
            Self::WindowStop { .. } => ArbitrationClass::BackoffDue,
            Self::Child(ChildEvent::SelfStop { .. }) | Self::Removal(_) => {
                ArbitrationClass::MembershipRemoval
            }
            Self::Child(ChildEvent::Ready { .. }) => ArbitrationClass::ReadinessSignal,
            Self::Child(ChildEvent::Exited { .. } | ChildEvent::ConstructionDisposed { .. }) => {
                ArbitrationClass::ChildExit
            }
            Self::Admission(_) => ArbitrationClass::Admission,
            Self::Deadline(DeadlineKind::Readiness { .. }) => ArbitrationClass::ReadinessDeadline,
            Self::Deadline(DeadlineKind::Restart { .. }) => ArbitrationClass::BackoffDue,
            Self::Deadline(DeadlineKind::Stop { .. }) => ArbitrationClass::StopDeadline,
        }
    }

    pub(super) fn classified(self) -> (ArbitrationClass, Self) {
        (self.class(), self)
    }
}

impl From<DriverEvent> for Pending {
    fn from(event: DriverEvent) -> Self {
        match event {
            DriverEvent::Child(event) => Self::Child(event),
            DriverEvent::Admission(request) => Self::Admission(request),
            DriverEvent::Removal(request) => Self::Removal(request),
        }
    }
}

/// Retains the item that ended a blocking wait. The driver then returns to
/// its single collection site so this head is arbitrated with every other
/// input that became eligible before the wake was observed.
///
/// The retained head keeps its own lane's FIFO position — it was that
/// channel's head — but it sits ahead of the whole re-entered collection, so
/// a woken *control* head precedes the primary lane the collection order
/// otherwise puts first. `MembershipRemoval` is the only class both lanes
/// produce (`Removal` and `SelfStop`), and neither ordering of that pair
/// changes a verdict: readiness publication consults the removal sources at
/// execution time rather than relying on arbitration position.
pub(super) fn retain_woken_event(
    event: DriverEvent,
    pending: &mut Vec<(ArbitrationClass, Pending)>,
) {
    pending.push(Pending::from(event).classified());
}

/// The three unbounded lanes one driver wake collects from, in collection
/// order.
pub(super) struct EventLanes<'a> {
    pub(super) primary: &'a mut runtime::UnboundedMpscReceiver<DriverEvent>,
    pub(super) control: Option<&'a mut runtime::UnboundedMpscReceiver<DriverEvent>>,
    pub(super) disposal: &'a mut runtime::UnboundedMpscReceiver<DriverEvent>,
}

/// Collects one bounded batch from every unbounded lane and reports whether
/// *any* of them still had a queued suffix, which is the driver's signal to
/// yield a scheduler turn before collecting again.
///
/// Lane order is a contract, not an implementation detail. Child lifecycle
/// events lead so a large externally generated admission prefix cannot strand
/// the exit that completes shutdown. Disposal completions trail, and
/// `arbitrate` sorts stably, so a `ConstructionDisposed` always follows every
/// same-class `Exited` collected in the same wake — even one produced later.
/// A disposal is therefore a batch-tail event. It carries no verdict — the
/// disposed child's exit published at dispatch, and its startup failure was
/// routed there too — so its position only decides when the membership's
/// release edge (SPEC §9) is crossed. Disposal runs on the blocking pool, so
/// its completion never had a fixed position relative to concurrent exits.
///
/// Every lane — disposal included — is capped. An uncapped lane can monopolize
/// a wake before the loop returns to the top and observes a shutdown request
/// (whose timeout does not start until `Draining` is published). The cap adds
/// one ordering surface: a deferred suffix is processed one wake later, so it
/// sorts against that wake's batch rather than this one's. For the disposal
/// lane that widens the same axis once more — the next wake's shutdown/force
/// checks run before its collection, and `ScopeShutdown` sorts ahead of
/// `ChildExit`, so a deferred disposal can observe a drain that a same-wake
/// exit had not yet begun. Same rationale: a blocking-pool completion never
/// had a fixed position relative to a request that can arrive on any wake.
///
/// The disposal cap only bites for a dynamic scope whose initial plan is small
/// relative to its admitted population. At most one construction disposal is in
/// flight per child (the reducer's `Disposing` state admits one), so an ordered
/// scope's disposal lane can never reach the `plan.children.len() * 3` limit.
pub(super) fn collect_event_lanes(
    lanes: EventLanes<'_>,
    limit: usize,
    pending: &mut Vec<(ArbitrationClass, Pending)>,
) -> bool {
    let primary_batch_full = collect_driver_events(lanes.primary, limit, pending);
    let control_batch_full = lanes
        .control
        .is_some_and(|receiver| collect_driver_events(receiver, limit, pending));
    let disposal_batch_full = collect_driver_events(lanes.disposal, limit, pending);
    primary_batch_full || control_batch_full || disposal_batch_full
}

pub(super) fn collect_driver_events(
    receiver: &mut runtime::UnboundedMpscReceiver<DriverEvent>,
    limit: usize,
    pending: &mut Vec<(ArbitrationClass, Pending)>,
) -> bool {
    for _ in 0..limit {
        let Some(event) = receiver.try_recv() else {
            return false;
        };
        pending.push(Pending::from(event).classified());
    }
    // Collecting exactly `limit` events does not by itself show a capped
    // lane. Probe once more so a lane that drained right at the limit skips
    // the full-batch yield; a probed event joins this batch rather than
    // being deferred a wake.
    let Some(event) = receiver.try_recv() else {
        return false;
    };
    pending.push(Pending::from(event).classified());
    true
}

impl ScopeRuntime {
    /// Projects the membership status from the *removal* sources alone:
    /// `Removing` when one has latched for this membership — the dynamic
    /// entry's latched `Removing` control-plane state or a fired
    /// fused-cancel latch on its `Resident` state. Scope-level stop
    /// sources (drain, force, latched shutdown requests, ancestor latches)
    /// are deliberately excluded: each of those has a guaranteed follow-up
    /// event that owns the scope verdict, so exit dispatch must not
    /// reclassify the membership as `Removing` on their behalf.
    pub(super) fn dispatch_membership_status(&mut self, key: ChildKey) -> MembershipStatus {
        if self.removal_latched(key) {
            self.reduce(SupervisorEvent::RemovalSampled { child: key });
        }
        self.supervisor.membership_status(key)
    }

    /// Reports whether any level-triggered stop source forbids constructing
    /// a new incarnation: a removal source for the membership itself, or a
    /// scope-level stop (drain, force, a latched shutdown request, or an
    /// ancestor latch). Every scope-level source has a guaranteed follow-up
    /// event, so this broad consult belongs only at sites that would
    /// otherwise invoke user construction — not at exit dispatch, where it
    /// would misclassify the membership and reroute the scope verdict.
    pub(super) fn construction_is_suppressed(&self, key: ChildKey) -> bool {
        self.supervisor.lifecycle().is_draining()
            || self.supervisor.hard_forced()
            || self.root.has_stop_request(self.epoch)
            || self.role.ancestor().is_some_and(|latches| {
                latches.framework_shutdown.is_fired() || latches.abort.is_fired()
            })
            || self.removal_latched(key)
    }

    pub(super) fn control_event_work(
        &self,
        event: ScopeControlEvent,
    ) -> Option<(ArbitrationClass, Pending)> {
        match event {
            // Resolving a window stop can cancel a pending restart, so it is
            // arbitrated as restart work: a child exit collected in the same
            // wake first gets the chance to trip intensity or fail startup,
            // and the resolver's execution-time checks then observe that.
            ScopeControlEvent::WindowStop { membership, target } => self
                .supervisor
                .key_for(membership)
                .map(|child| Pending::WindowStop { child, target }.classified()),
        }
    }

    /// Whether the child's restart policy would restart an incarnation that
    /// a stop had ended: the clean cooperative outcome, `Completed` with
    /// `Cancellation::Observed` (SPEC §12's nested-shutdown rule).
    pub(super) fn restarts_a_stopped_incarnation(&self, key: ChildKey) -> bool {
        self.children.get(key).is_some_and(|child| {
            dispatch_exit(
                &Exit::completed(Cancellation::Observed),
                child.options.restart,
                false,
                MembershipStatus::Active,
            ) == ExitDispatch::ScheduleRestart
        })
    }

    /// Resolves a scope stop accepted while the membership had no live
    /// incarnation — a restart window or a pre-spawn handle — without
    /// constructing one (SPEC §11).
    ///
    /// Terminality owns the request whenever another stop source already
    /// does: a joined or disposing membership, a sampled removal, or a
    /// scope-level stop whose follow-up drain terminalizes this member. An
    /// active incarnation means construction already began, so the request
    /// addresses that incarnation as a live stop. Otherwise the child's
    /// policy decides: one that would restart the stopped incarnation spends
    /// the request by vacating its target epoch, leaving any pending restart
    /// on its schedule; one that would not cancels the pending restart and
    /// terminalizes the membership with its last exit. A never-spawned
    /// member under the second policy waits for its construction site, which
    /// `spawn_child` owns, so ordered startup keeps its order.
    pub(super) fn resolve_window_stop(&mut self, key: ChildKey, target: Epoch) {
        if self.supervisor.joined(key)
            || self.supervisor.is_disposing(key)
            || self.construction_is_suppressed(key)
        {
            return;
        }
        let Some(child) = self.children.get(key) else {
            return;
        };
        if child.active.is_some() {
            return;
        }
        let Some(scope) = child.slot.scope.as_ref().map(Arc::clone) else {
            return;
        };
        if scope.pending_incarnation_shutdown() != Some(target) {
            return;
        }
        if self.restarts_a_stopped_incarnation(key) {
            let vacated = scope.vacate_pending_shutdown(target);
            // Only this driver begins the member's incarnations, and a
            // concurrent request for the same idle target is absorbed into
            // the one pending, so the target sampled above is still vacatable.
            debug_assert!(vacated, "a sampled pending target is vacatable");
            return;
        }
        if self.supervisor.spawned_once(key) {
            let startup = self.terminal_startup_disposition(key);
            self.terminate_inactive(key, startup);
        }
    }

    pub(super) fn handle_deadline(&mut self, deadline: DeadlineKind) {
        match deadline {
            DeadlineKind::Readiness {
                child: key,
                incarnation,
            } => {
                let Some(child) = self.children.get_mut(key) else {
                    return;
                };
                let Some(active) = child.active.as_mut() else {
                    return;
                };
                if active.incarnation != incarnation {
                    return;
                }
                // The queue already consumed this registration. Feed the
                // retained latch into the engine so signal-at-deadline policy
                // is decided in exactly one place.
                active.readiness_deadline.take();
                let effect = active.readiness.step(ReadinessEvent::Deadline {
                    now: runtime::now(),
                    signal_seen: active.ready_signal.is_fired(),
                });
                if effect
                    .map(|effect| self.apply_readiness_effect(key, incarnation, effect))
                    .unwrap_or(false)
                {
                    self.progress_startup();
                }
            }
            DeadlineKind::Restart { child } => {
                // A removal or scope stop can latch after the exit scheduled
                // this deadline but before the deadline's batch runs. Recheck
                // the level-triggered sources at execution time so a stale
                // backoff edge never invokes user construction.
                if self.construction_is_suppressed(child) {
                    if let Some(child) = self.children.get_mut(child) {
                        child.restart_deadline.take();
                    }
                } else {
                    self.spawn_child(child);
                    // A restart-deadline caller is outside `progress_startup`'s
                    // ordered loop. Revisit the aggregate in case this spawn's
                    // immediate-readiness effect released its last gate.
                    self.progress_startup();
                }
            }
            DeadlineKind::Stop { child, incarnation } => {
                if self
                    .children
                    .get(child)
                    .and_then(|child| child.active.as_ref())
                    .is_some_and(|active| active.incarnation == incarnation)
                {
                    self.advance_ladder(child, runtime::now());
                }
            }
        }
    }
}
