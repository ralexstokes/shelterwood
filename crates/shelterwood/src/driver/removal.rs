use super::*;

#[derive(Clone, Copy)]
pub(super) struct RemovalRequest {
    pub(super) membership: Membership,
    pub(super) key: ChildKey,
}

impl ScopeRuntime {
    pub(super) fn dynamic_membership_is_removing(&self, key: ChildKey) -> bool {
        let Some(child) = self.children.get(key) else {
            return false;
        };
        self.dynamic.as_ref().is_some_and(|control| {
            control
                .state
                .lock()
                .expect("dynamic-state mutex poisoned")
                .entry(child.slot.member.id())
                .filter(|entry| entry.slot.member.membership() == child.slot.member.membership())
                .is_some_and(|entry| entry.is_removing() && entry.matches_key(key))
        })
    }

    pub(super) fn handle_removal(&mut self, removal: RemovalRequest) {
        let RemovalRequest { membership, key } = removal;
        let Some(member) = self
            .children
            .get(key)
            .map(|child| Arc::clone(&child.slot.member))
            .filter(|member| member.membership() == membership)
        else {
            return;
        };
        let Some(control) = &self.dynamic else {
            return;
        };
        let tracked = control
            .state
            .lock()
            .expect("dynamic-state mutex poisoned")
            .entry(member.id())
            .filter(|entry| entry.slot.member.membership() == membership)
            .is_some_and(|entry| entry.is_removing() && entry.matches_key(key));
        if !tracked {
            return;
        }
        if self.children[key].is_terminal() {
            self.finalize_removal(key);
        } else {
            self.begin_stop_child(key, None);
            if self.children[key].is_terminal() {
                self.finalize_removal(key);
            }
        }
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
            drop(entry);
        }
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
        // The entry's drop completes any in-flight removal response; it must
        // follow the Removed edge so a woken remover never sees the child
        // resident.
        drop(removed);
    }

    pub(super) fn reclaim_child(&mut self, key: ChildKey) {
        let Some(mut child) = self.children.remove(key) else {
            return;
        };
        // Reclaim never adjusts `incomplete_children`: every reclaim path runs
        // after terminal completion, which already decremented the count. An
        // early reclaim would leak the count and stall scope completion, so
        // fail loudly instead.
        debug_assert!(
            !child.is_incomplete(),
            "reclaim runs only after terminal completion"
        );
        let removed = self.child_keys.remove(&child.slot.member.membership());
        debug_assert_eq!(
            removed,
            Some(key),
            "reclaimed memberships leave the key index"
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
        #[cfg(test)]
        self.record_storage();
    }
}
