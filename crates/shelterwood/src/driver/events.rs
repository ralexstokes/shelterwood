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
        recorded: Option<RecordedOutcome>,
        join: runtime::JoinOutcome<()>,
        cancellation: Cancellation,
        readiness_signal_seen: bool,
    },
    ConstructionDisposed {
        child: ChildKey,
        panic: Option<runtime::DisposalPanic>,
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
    RestartShutdown { child: ChildKey, target: Epoch },
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
            Self::RestartShutdown { .. } => ArbitrationClass::BackoffDue,
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
/// A disposal is therefore a batch-tail event: the exit it trails may begin a
/// drain first, after which `handle_construction_disposed` sees `is_draining`
/// and routes the disposed child through stop progression instead of
/// `fail_startup`. That is a widening of an order that was already reachable,
/// not a new one: disposal runs on the blocking pool, so its completion never
/// had a fixed position relative to concurrent exits.
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
/// flight per child (`pending_terminal` admits one), so an ordered scope's
/// disposal lane can never reach the `plan.children.len() * 3` limit.
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
        let Some(event) = runtime::unbounded_mpsc_try_recv(receiver) else {
            return false;
        };
        pending.push(Pending::from(event).classified());
    }
    // Collecting exactly `limit` events does not by itself show a capped
    // lane. Probe once more so a lane that drained right at the limit skips
    // the full-batch yield; a probed event joins this batch rather than
    // being deferred a wake.
    let Some(event) = runtime::unbounded_mpsc_try_recv(receiver) else {
        return false;
    };
    pending.push(Pending::from(event).classified());
    true
}

pub(super) fn restart_shutdown_work(child: ChildKey, target: Epoch) -> (ArbitrationClass, Pending) {
    // This starts a pending incarnation, so it is restart work, not a
    // scope-shutdown transition. A child exit collected in the same wake must
    // first get the chance to trip intensity or fail startup; the
    // execution-time suppression check then observes that drain.
    Pending::RestartShutdown { child, target }.classified()
}

impl ScopeRuntime {
    /// Projects the membership status from the *removal* sources alone:
    /// `Removing` when one has latched for this membership — the dynamic
    /// entry's authoritative `Removing` control-plane state or a fired
    /// fused-cancel latch on its `Resident` state. Scope-level stop
    /// sources (drain, force, latched shutdown requests, ancestor latches)
    /// are deliberately excluded: each of those has a guaranteed follow-up
    /// event that owns the scope verdict, so exit dispatch must not
    /// reclassify the membership as `Removing` on their behalf.
    pub(super) fn dispatch_membership_status(&self, key: ChildKey) -> MembershipStatus {
        let Some(child) = self.children.get(key) else {
            return MembershipStatus::Removing;
        };
        let removing = self.dynamic.as_ref().is_some_and(|control| {
            control
                .state
                .lock()
                .expect("dynamic-state mutex poisoned")
                .entry(child.slot.member.id())
                .filter(|entry| entry.slot.member.membership() == child.slot.member.membership())
                .is_some_and(|entry| entry.restart_is_suppressed(key))
        });
        if removing {
            MembershipStatus::Removing
        } else {
            MembershipStatus::Active
        }
    }

    /// Reports whether any level-triggered stop source forbids constructing
    /// a new incarnation: a removal source for the membership itself, or a
    /// scope-level stop (drain, force, a latched shutdown request, or an
    /// ancestor latch). Every scope-level source has a guaranteed follow-up
    /// event, so this broad consult belongs only at sites that would
    /// otherwise invoke user construction — not at exit dispatch, where it
    /// would misclassify the membership and reroute the scope verdict.
    pub(super) fn restart_is_suppressed(&self, key: ChildKey) -> bool {
        self.lifecycle.is_draining()
            || self.hard_forced
            || self.root.has_stop_request(self.epoch)
            || self
                .role
                .ancestor()
                .is_some_and(|latches| latches.shutdown.is_fired() || latches.abort.is_fired())
            || self.dispatch_membership_status(key) == MembershipStatus::Removing
    }

    pub(super) fn control_event_work(
        &self,
        event: ScopeControlEvent,
    ) -> Option<(ArbitrationClass, Pending)> {
        match event {
            ScopeControlEvent::RestartShutdown { membership, target } => self
                .child_keys
                .get(&membership)
                .copied()
                .map(|child| restart_shutdown_work(child, target)),
        }
    }

    pub(super) fn expedite_restart_shutdown(&mut self, key: ChildKey, target: Epoch) {
        // Collection and execution are separated by arbitration. Recheck
        // every level-triggered stop source so teardown/removal latched in the
        // same batch suppresses user construction immediately.
        if self.restart_is_suppressed(key) {
            if let Some(child) = self.children.get_mut(key) {
                child.restart_shutdown_pending = None;
            }
            return;
        }
        let target_is_pending = self
            .children
            .get(key)
            .and_then(|child| child.slot.scope.as_ref())
            .is_some_and(|scope| scope.has_pending_incarnation_shutdown(target));
        let Some(child) = self.children.get_mut(key) else {
            return;
        };
        if !target_is_pending || child.is_terminal() || child.is_disposing() {
            child.restart_shutdown_pending = None;
            return;
        }
        if child.active.is_some() {
            child.restart_shutdown_pending = Some(target);
            return;
        }
        if !child.spawned_once {
            // Only a member in the restart gap may be expedited. The wake-start
            // scan this path replaced required `MemberStage::Restarting`; with
            // no active incarnation and the terminal/disposing cases excluded
            // above, `spawned_once` is that stage bit — false means the member
            // is still `Admitted` and has never run. Expediting it would let a
            // shutdown request against the first (pending) incarnation start an
            // ordered child before its in-order turn. Leave the request latched
            // on the nested cell: the first incarnation claims it when
            // `progress_startup` reaches the child.
            return;
        }
        child.restart_shutdown_pending = None;
        self.spawn_child(key);
        // This path runs outside the ordered-startup loop. Revisit the
        // aggregate in case the spawn became ready synchronously, just like a
        // restart-deadline spawn below.
        self.progress_startup();
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
                if self.restart_is_suppressed(child) {
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
