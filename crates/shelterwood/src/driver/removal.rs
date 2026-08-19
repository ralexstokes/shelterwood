use super::*;

#[derive(Clone, Copy)]
pub(super) struct RemovalRequest {
    pub(super) key: ChildKey,
}

impl ScopeRuntime {
    pub(super) fn removal_latched(&self, key: ChildKey) -> bool {
        if self.supervisor.membership_status(key) == MembershipStatus::Removing {
            return true;
        }
        let Some(child) = self.children.get(key) else {
            return false;
        };
        let Some(control) = &self.dynamic else {
            return false;
        };
        let state = control.state.lock().expect("dynamic-state mutex poisoned");
        state.entry(child.slot.member.id()).is_some_and(|entry| {
            entry.slot.member.membership() == child.slot.member.membership()
                && entry.matches_key(key)
                && entry.removal_latched()
        })
    }

    pub(super) fn handle_removal(&mut self, removal: RemovalRequest) {
        let key = removal.key;
        if !self.removal_latched(key) {
            return;
        }
        self.reduce(SupervisorEvent::RemovalLatched { child: key });
        self.flush_supervisor_effects();
    }

    pub(super) fn finalize_removal(&mut self, key: ChildKey) {
        let Some(control) = &self.dynamic else {
            return;
        };
        let member = Arc::clone(&self.children[key].slot.member);
        let id = member.id().clone();
        let root = Arc::clone(&self.root);
        let entry = root.with_observation_gate(|txn| {
            let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
            let matches = state.entry(&id).is_some_and(|entry| {
                entry.slot.member.membership() == member.membership()
                    && entry.matches_key(key)
                    && entry.is_removing()
            });
            if !matches {
                return None;
            }
            // Residency is withdrawn while the identifier remains claimed.
            // A new reservation can therefore observe only the old resident
            // or the committed Removed edge, never a reused-id overlap.
            root.prune_child_locked(&member, txn);
            state.remove(&id, txn)
        });
        if let Some(entry) = entry {
            self.reclaim_child(key);
            self.release_removed_entry(entry);
        }
    }

    /// Releases a committed removal's entry. The drop completes the in-flight
    /// removal response, so during `Starting` — where the reclaim that
    /// precedes it can shrink the declared initial set — the completion is
    /// retained until the batch epilogue recomputes the aggregate. That
    /// ordering is what makes a returned `RemoveOutcome::Removed` imply
    /// startup already saw the shrunken set (SPEC §6).
    fn release_removed_entry(&mut self, entry: DynamicEntry) {
        if self.supervisor.lifecycle().is_starting() {
            self.pending_startup_removals.push(entry);
        } else {
            drop(entry);
        }
    }

    /// Publishes removal completions whose committed membership shrink had to
    /// be observed by startup settlement first.
    pub(super) fn publish_startup_removals(&mut self) {
        drop(std::mem::take(&mut self.pending_startup_removals));
    }

    pub(super) fn prune_terminal(&mut self, key: ChildKey) {
        let member = Arc::clone(&self.children[key].slot.member);
        let root = Arc::clone(&self.root);
        let removed = root.with_observation_gate(|txn| {
            let mut state = self
                .dynamic
                .as_ref()
                .map(|control| control.state.lock().expect("dynamic-state mutex poisoned"));
            let matches = state.as_ref().is_some_and(|state| {
                state.entry(member.id()).is_some_and(|entry| {
                    entry.slot.member.membership() == member.membership() && entry.matches_key(key)
                })
            });
            root.prune_child_locked(&member, txn);
            if matches {
                state
                    .as_mut()
                    .and_then(|state| state.remove(member.id(), txn))
            } else {
                None
            }
        });
        if self.root.flavor == ScopeFlavor::Dynamic {
            self.reclaim_child(key);
        }
        // The entry's release completes any in-flight removal response; it
        // must follow the Removed edge so a woken remover never sees the
        // child resident. A removal that latches between a terminal route's
        // membership-status check and the gate above lands its response here
        // rather than in `finalize_removal`, so it takes the same
        // starting-phase retention.
        if let Some(entry) = removed {
            self.release_removed_entry(entry);
        }
    }

    pub(super) fn reclaim_child(&mut self, key: ChildKey) {
        let Some(mut child) = self.children.remove(key) else {
            return;
        };
        debug_assert!(
            self.supervisor.joined(key),
            "reclaim runs only after joined terminal completion"
        );
        if let Some(deadline) = child.restart_deadline.take() {
            self.deadlines.cancel(deadline);
        }
        if let Some(mut active) = child.active.take() {
            if let Some(deadline) = active.readiness_deadline.take() {
                self.deadlines.cancel(deadline);
            }
            if let Some(deadline) = active.stop_deadline.take() {
                self.deadlines.cancel(deadline);
            }
        }
        self.reduce(SupervisorEvent::Reclaim { child: key });
        #[cfg(test)]
        self.record_storage();
    }
}
