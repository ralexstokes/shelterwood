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
    RestartShutdown(ChildKey),
    AncestorShutdown,
    AncestorAbort,
    Force,
    Driver(DriverEvent),
    Deadline(DeadlineKind),
}

impl Pending {
    pub(super) fn class(&self) -> ArbitrationClass {
        match self {
            Self::Shutdown | Self::AncestorShutdown | Self::AncestorAbort | Self::Force => {
                ArbitrationClass::ScopeShutdown
            }
            Self::RestartShutdown(_) => ArbitrationClass::BackoffDue,
            Self::Driver(event) => driver_event_class(event),
            Self::Deadline(DeadlineKind::Readiness { .. }) => ArbitrationClass::ReadinessDeadline,
            Self::Deadline(DeadlineKind::Restart { .. }) => ArbitrationClass::BackoffDue,
            Self::Deadline(DeadlineKind::Stop { .. }) => ArbitrationClass::StopDeadline,
        }
    }

    pub(super) fn classified(self) -> (ArbitrationClass, Self) {
        (self.class(), self)
    }
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
        pending.push(Pending::Driver(event).classified());
    }
    // Collecting exactly `limit` events does not by itself show a capped
    // lane. Probe once more so a lane that drained right at the limit skips
    // the full-batch yield; a probed event joins this batch rather than
    // being deferred a wake.
    let Some(event) = runtime::unbounded_mpsc_try_recv(receiver) else {
        return false;
    };
    pending.push(Pending::Driver(event).classified());
    true
}

pub(super) fn restart_shutdown_work(child: ChildKey) -> (ArbitrationClass, Pending) {
    // This starts a pending incarnation, so it is restart work, not a
    // scope-shutdown transition. A child exit collected in the same wake must
    // first get the chance to trip intensity or fail startup; the
    // execution-time suppression check then observes that drain.
    Pending::RestartShutdown(child).classified()
}

pub(super) fn driver_event_class(event: &DriverEvent) -> ArbitrationClass {
    match event {
        DriverEvent::Child(ChildEvent::SelfStop { .. }) => ArbitrationClass::MembershipRemoval,
        DriverEvent::Removal(_) => ArbitrationClass::MembershipRemoval,
        DriverEvent::Child(ChildEvent::Ready { .. }) => ArbitrationClass::ReadinessSignal,
        DriverEvent::Child(ChildEvent::Exited { .. } | ChildEvent::ConstructionDisposed { .. }) => {
            ArbitrationClass::ChildExit
        }
        DriverEvent::Admission(_) => ArbitrationClass::Admission,
    }
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
            ScopeControlEvent::RestartShutdown { membership } => self
                .child_keys
                .get(&membership)
                .copied()
                .map(restart_shutdown_work),
        }
    }

    pub(super) fn expedite_restart_shutdown(&mut self, key: ChildKey) {
        // Collection and execution are separated by arbitration. Recheck
        // every level-triggered stop source so teardown/removal latched in the
        // same batch suppresses user construction immediately.
        if !self.restart_is_suppressed(key) {
            self.spawn_child(key);
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
