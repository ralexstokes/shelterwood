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
                let removing = self.dispatch_membership_status(key) == MembershipStatus::Removing;
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
                if !self.lifecycle.startup_complete() {
                    child.initial_ready = true;
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
        if !self.lifecycle.is_starting() {
            return;
        }
        match self.root.flavor {
            ScopeFlavor::Ordered => {
                while let Some(key) = self.next_ordered_start {
                    // The cursor is held across `spawn_child`, and every
                    // reclaim path (`finalize_removal`, `prune_terminal`) can
                    // vacate a slot, so never index the arena with it: a
                    // reclaimed slot is treated as already gone, the same
                    // discipline `stop_next_ordered` follows for its own
                    // cursor. `keys_after` ranges over the arena's ordered
                    // key domain, so it still advances past a vacated key.
                    if self
                        .children
                        .get(key)
                        .is_some_and(|child| !child.spawned_once)
                    {
                        self.spawn_child(key);
                    }
                    if self
                        .children
                        .get(key)
                        .is_some_and(|child| !child.initial_ready)
                    {
                        return;
                    }
                    self.next_ordered_start = self.children.keys_after(key).next();
                }
                if self
                    .children
                    .values()
                    .filter(|child| child.initial)
                    .all(|child| child.initial_ready)
                {
                    self.complete_startup();
                }
            }
            ScopeFlavor::Dynamic => {
                if self
                    .children
                    .values()
                    .filter(|child| child.initial)
                    .all(|child| child.initial_ready)
                {
                    self.complete_startup();
                }
            }
        }
    }

    pub(super) fn complete_startup(&mut self) {
        let Some(state) = self.lifecycle.complete_startup() else {
            return;
        };
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
        let Some(state) = self.lifecycle.fail_startup() else {
            return;
        };
        if self.root.flavor == ScopeFlavor::Ordered {
            let later_children: Vec<_> = self.children.keys_after(key).collect();
            for later in later_children {
                if !self.children[later].spawned_once
                    && !self.children[later].is_disposing()
                    && !self.children[later].is_terminal()
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
