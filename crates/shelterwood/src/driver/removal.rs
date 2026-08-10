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
                .entries
                .get(child.slot.member.id())
                .filter(|entry| entry.slot.member.membership() == child.slot.member.membership())
                .is_some_and(|entry| entry.is_removing() && entry.matches_key(key))
        })
    }

    pub(super) fn publish_dynamic_removal(&self, key: ChildKey) {
        let Some(member) = self
            .children
            .get(key)
            .map(|child| Arc::clone(&child.slot.member))
        else {
            return;
        };
        // Idempotency hygiene rather than a load-bearing guard: an explicit
        // removal publishes this projection before its request reaches the
        // driver, and duplicate deliveries re-enter here after the first
        // publication. Skipping the transition only avoids re-publishing an
        // identical record under the observation gate; no public observable
        // distinguishes that redundant publication, so tests pin the call
        // sites (the fused-only removal path, where this write is the sole
        // Removing-projection writer) rather than this check.
        if member.record().membership_status != MembershipStatus::Removing {
            self.root.set_child_removing(&member);
        }
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
        let tracked = {
            let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
            state
                .entries
                .get_mut(member.id())
                .filter(|entry| entry.slot.member.membership() == membership)
                .and_then(DynamicEntry::mark_removing)
                .is_some_and(|tracked| tracked == key)
        };
        if !tracked {
            return;
        }
        // A fused drop and an explicit `remove` each queue one
        // `RemovalRequest` for the same membership, and `mark_removing`
        // deliberately re-succeeds on an already-Removing entry, so a second
        // delivery reaches this point. Every step below is idempotent:
        // `publish_dynamic_removal` is guarded by the record's status
        // flag, `begin_stop_child` by its ladder/disposal guards, and
        // `finalize_removal` removes the entry it matched.
        self.publish_dynamic_removal(key);
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
        let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
        if state.entries.get(&id).is_some_and(|entry| {
            entry.slot.member.membership() == member.membership()
                && entry.matches_key(key)
                && entry.is_removing()
        }) {
            let entry = state.entries.remove(&id).expect("entry was just resolved");
            drop(state);
            self.root.prune_child(&member);
            self.reclaim_child(key);
            drop(entry);
        }
    }

    pub(super) fn prune_terminal(&mut self, key: ChildKey) {
        let member = Arc::clone(&self.children[key].slot.member);
        let mut removed = None;
        if let Some(control) = &self.dynamic {
            let id = member.id().clone();
            let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
            if state.entries.get(&id).is_some_and(|entry| {
                entry.slot.member.membership() == member.membership() && entry.matches_key(key)
            }) {
                removed = state.entries.remove(&id);
            }
        }
        self.root.prune_child(&member);
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
