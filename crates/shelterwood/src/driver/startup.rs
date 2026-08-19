use super::*;

impl ScopeRuntime {
    pub(super) fn apply_readiness_effect(
        &mut self,
        key: ChildKey,
        incarnation: Incarnation,
        effect: ReadinessEffect,
    ) -> bool {
        match effect {
            ReadinessEffect::ArmDeadline { deadline } => {
                let handle = self.deadlines.push(
                    deadline,
                    DeadlineKind::Readiness {
                        child: key,
                        incarnation,
                    },
                );
                if let Some(active) = self
                    .children
                    .get_mut(key)
                    .and_then(|child| child.active.as_mut())
                    && active.incarnation == incarnation
                {
                    active.readiness_deadline = Some(handle);
                } else {
                    self.deadlines.cancel(handle);
                }
                false
            }
            ReadinessEffect::BecameReady => {
                // Removal outranks readiness structurally, and arbitration
                // alone does not deliver that: `SelfStop` shares removal's
                // class, so a same-batch `Removal` cannot preempt the
                // readiness `handle_self_stop` replays, and an exit's
                // readiness step runs from the same collection. Consult the
                // removal sources at execution time — the discipline exit
                // dispatch already follows — so no readiness edge is
                // published for, or credited to startup by, a membership the
                // owner already observes as `Removing`. The internal latch
                // still records that the edge happened: a removing member's
                // terminal routes through `finalize_removal`, and its exit
                // classification must not mistake a post-ready stop for a
                // pre-ready failure.
                let removal_latched = self.removal_latched(key);
                self.reduce(SupervisorEvent::Ready {
                    child: key,
                    removal_latched,
                });
                let removing = self.supervisor.membership_status(key) == MembershipStatus::Removing;
                let Some(child) = self.children.get_mut(key) else {
                    return false;
                };
                let Some(active) = child.active.as_mut() else {
                    return false;
                };
                if active.incarnation != incarnation {
                    return false;
                }
                if let Some(deadline) = active.readiness_deadline.take() {
                    self.deadlines.cancel(deadline);
                }
                active.ready_signal.fire();
                if removing {
                    return false;
                }
                self.root.transition_child_stage(
                    &child.slot.member,
                    MemberTransition::Running,
                    Some(LifecycleEventKind::Ready {
                        id: child.slot.member.id().clone(),
                        membership: child.slot.member.membership(),
                        incarnation,
                    }),
                );
                true
            }
            ReadinessEffect::TimedOut { deadline } => {
                self.begin_stop_child(key, Some(RecordedOutcome::readiness_timed_out(deadline)));
                false
            }
            ReadinessEffect::Disarmed => false,
        }
    }

    pub(super) fn progress_startup(&mut self) {
        self.settle_supervisor();
    }

    pub(super) fn publish_startup_complete(&mut self, state: ScopeState) {
        self.root.set_state_and_startup(state, Ok(()));
        if let Some(parent_ready) = self.role.parent_ready() {
            parent_ready.fire();
        }
    }

    pub(super) fn handle_ready(&mut self, key: ChildKey, incarnation: Incarnation) {
        let effect = self
            .children
            .get_mut(key)
            .and_then(|child| child.active.as_mut())
            .filter(|active| active.incarnation == incarnation)
            .and_then(|active| active.readiness.step(ReadinessEvent::Signal));
        let became_ready = effect
            .map(|effect| self.apply_readiness_effect(key, incarnation, effect))
            .unwrap_or(false);
        if became_ready {
            self.progress_startup();
        }
        #[cfg(test)]
        self.record_storage();
    }

    pub(super) fn fail_startup(&mut self, key: ChildKey, exit: Exit) {
        // Several initial children can fail in one arbitration batch. The
        // first failure owns the startup verdict and its sole lifecycle edge;
        // later exits are still terminalized, but cannot republish the scope
        // transition or replace the authoritative cause.
        let child = &self.children[key];
        let failure = StartupFailure {
            cause: StartupFailureCause::Child {
                id: child.slot.member.id().clone(),
                membership: child.slot.member.membership(),
                exit,
            },
        };
        let Some(state) = supervisor_fail_startup(&mut self.supervisor) else {
            return;
        };
        if self.root.flavor == ScopeFlavor::Ordered {
            let later_children: Vec<_> = self.supervisor.keys_after(key).collect();
            for later in later_children {
                if !self.supervisor.spawned_once(later)
                    && !self.supervisor.is_disposing(later)
                    && !self.supervisor.joined(later)
                {
                    self.begin_terminal_disposal(
                        later,
                        Exit::never_started(),
                        None,
                        StartupDisposition::NotAborted,
                    );
                }
            }
        }
        if self.role.is_root() {
            self.root
                .set_state_and_startup(state, Err(StartupError::StartupFailed(failure.clone())));
        } else {
            self.begin_drain_with_startup(
                StopReason::StartupFailed(failure.clone()),
                Err(StartupError::StartupFailed(failure)),
            );
        }
    }
}
