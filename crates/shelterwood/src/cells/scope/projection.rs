use std::sync::{Arc, atomic::Ordering};

use shelterwood_core::{
    Exit, Intensity, Strategy, TotalRestarts, engine::ScopeState, exit::StartupError,
    policy::ScopeFlavor,
};

#[cfg(test)]
use crate::runtime;

use crate::cells::{
    Guarded, MemberStage, ObservationTxn, RetainGuards, Retained,
    observe::{
        ChildSnapshot, ChildState, LifecycleEventKind, LifecycleEvents, LifecycleSeq,
        RetainedLifecycleEvent, ScopeSnapshot, SnapshotReceiver,
    },
};

use super::{ResidentProjection, ScopeCell};

/// Observation-only projection of the authoritative engine lifecycle.
/// Driver decisions never read this record back as liveness policy.
#[derive(Clone, Debug)]
pub(crate) struct ScopeRecord {
    pub state: ScopeState,
    pub startup: Option<Result<(), StartupError>>,
    /// Read only by this crate's snapshot publication; the driver takes its
    /// restart totals from the decision that produced them.
    pub(crate) total_restarts: TotalRestarts,
}

impl RetainGuards for ScopeRecord {
    fn retain_guards(&self, guards: &mut Vec<Retained<Exit>>) {
        self.state.retain_guards(guards);
        self.startup.retain_guards(guards);
    }
}

impl ScopeCell {
    pub(crate) fn record(&self) -> Guarded<ScopeRecord> {
        self.observation.record.read_cloned()
    }

    #[cfg(test)]
    pub(crate) fn record_watcher(&self) -> runtime::WatchReceiver<Guarded<ScopeRecord>> {
        self.observation.record.watcher()
    }

    pub(crate) fn set_intensity(&self, intensity: Intensity) {
        self.with_observation_gate(|wakes| {
            *self
                .observation
                .intensity
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = intensity;
            self.publish_snapshot_chain_locked(wakes);
        });
    }

    #[cfg(test)]
    pub(crate) fn emit(&self, event: LifecycleEventKind) {
        self.with_observation_gate(|wakes| self.emit_locked(wakes, event));
    }

    pub(crate) fn snapshot(&self) -> Arc<ScopeSnapshot> {
        self.with_observation_gate(|txn| self.snapshot_locked().release(txn))
    }

    pub(crate) fn subscribe_snapshots(&self) -> SnapshotReceiver {
        // No gate-identity check here or in `subscribe_lifecycle`: both acquire
        // the gate themselves, and `with_observation_gate` already retries
        // until the guard it hands the closure is this scope's installed one.
        // Only a `*_locked` writer that receives someone else's transaction has
        // an identity left to verify.
        let (receiver, closed_consistent) = self.with_observation_gate(|wakes| {
            let initial = self.snapshot_locked();
            let receiver = self.observation.snapshots.subscribe(initial, wakes);
            let closed_consistent = !self.observation.closed.load(Ordering::Acquire)
                || receiver.borrow_latest_and_closed().1;
            (receiver, closed_consistent)
        });
        assert!(
            closed_consistent,
            "closed snapshot state is installed before later subscriptions"
        );
        receiver
    }

    pub(crate) fn subscribe_lifecycle(&self) -> LifecycleEvents {
        let (events, closed_consistent) = self.with_observation_gate(|txn| {
            let events = self.observation.lifecycle.subscribe(txn);
            let closed_consistent = !self.observation.closed.load(Ordering::Acquire)
                || self.observation.lifecycle.is_closed();
            (events, closed_consistent)
        });
        assert!(
            closed_consistent,
            "closed lifecycle state is installed before later subscriptions"
        );
        events
    }

    fn snapshot_locked(&self) -> Guarded<Arc<ScopeSnapshot>> {
        Guarded::new(self.project_locked())
    }

    /// Builds the raw recursive projection under the observation gate.
    ///
    /// Every exit it clones stays co-owned by a member or scope record, which
    /// only a gated writer can change, so a partial projection unwinding here
    /// is refcount traffic. [`Self::snapshot_locked`] guards the finished cut.
    fn project_locked(&self) -> Arc<ScopeSnapshot> {
        let record = self.record();
        let intensity = *self
            .observation
            .intensity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let children = self.current_children();
        // An admission that unwound past its residency push leaves a resident
        // whose `Added` was never published. SPEC §3.2 keeps such a membership
        // out of `children` / `child(id)` / `descendant(path)` exactly as it
        // keeps it out of the event stream, so the cut skips it. Gate rehoming,
        // terminality lookup and straggler collection still see it: it is
        // owned residency, just not yet public.
        let projected: Vec<_> = children
            .iter()
            .filter(|resident| resident.announced)
            .map(|resident| self.child_snapshot_locked(resident.projection()))
            .collect();
        Arc::new(ScopeSnapshot {
            state: record.state.clone(),
            kind: self.flavor,
            strategy: (self.flavor == ScopeFlavor::Ordered).then_some(Strategy::default()),
            intensity,
            total_restarts: record.total_restarts,
            lifecycle_seq: LifecycleSeq::new(
                self.observation.lifecycle_seq.load(Ordering::Acquire),
            ),
            children: projected.into(),
        })
    }

    fn child_snapshot_locked(&self, child: &ResidentProjection) -> ChildSnapshot {
        let record = child.member.record();
        let options = child.member.options();
        let terminal = matches!(&record.stage, MemberStage::Terminal(_));
        let nested = child.scope.as_ref().and_then(|scope| {
            (record.incarnation.is_some() || terminal).then(|| scope.project_locked())
        });
        let state = match &record.stage {
            MemberStage::Reserved | MemberStage::Admitted => ChildState::Admitted,
            MemberStage::Starting => ChildState::Starting,
            MemberStage::Running => ChildState::Running,
            MemberStage::Restarting => ChildState::Restarting,
            MemberStage::Stopping => ChildState::Stopping,
            MemberStage::Terminal(exit) if record.startup_aborted => {
                ChildState::StartupAborted { exit: exit.clone() }
            }
            MemberStage::Terminal(exit) => ChildState::Stopped { exit: exit.clone() },
        };
        ChildSnapshot {
            id: child.member.id().clone(),
            membership: child.member.membership(),
            incarnation: record.incarnation,
            state,
            last_exit: record.last_exit.clone(),
            membership_status: record.membership_status,
            restart_count: record.restart_count,
            restart_policy: options.restart,
            retention: options.retention,
            restart_at: record.restart_at,
            nested,
            scope_seq: child.scope.as_ref().map(|scope| {
                LifecycleSeq::new(scope.observation.lifecycle_seq.load(Ordering::Acquire))
            }),
        }
    }

    fn ancestors_locked(&self, txn: &mut ObservationTxn<'_>) -> Vec<Arc<ScopeCell>> {
        let mut ancestors = Vec::new();
        let mut current = self.parent();
        while let Some(scope) = current {
            txn.retain_shared(&scope);
            current = scope.parent();
            ancestors.push(scope);
        }
        ancestors
    }

    fn publish_snapshot_chain_through_locked(
        &self,
        wakes: &mut ObservationTxn<'_>,
        ancestors: &[Arc<ScopeCell>],
    ) {
        #[cfg(debug_assertions)]
        wakes.debug_assert_gate(&self.current_observation_gate());
        // Each producer owns the scope it projects: the cut is built at
        // commit, once per hub, after every publication in this transaction
        // has been coalesced onto it.
        let scope = self.owned();
        self.observation
            .snapshots
            .publish(wakes, move || scope.snapshot_locked());
        for ancestor in ancestors {
            #[cfg(debug_assertions)]
            wakes.debug_assert_gate(&ancestor.current_observation_gate());
            let scope = Arc::clone(ancestor);
            ancestor
                .observation
                .snapshots
                .publish(wakes, move || scope.snapshot_locked());
        }
    }

    pub(super) fn publish_snapshot_chain_locked(&self, wakes: &mut ObservationTxn<'_>) {
        let ancestors = self.ancestors_locked(wakes);
        self.publish_snapshot_chain_through_locked(wakes, &ancestors);
    }

    pub(super) fn emit_locked(&self, wakes: &mut ObservationTxn<'_>, kind: LifecycleEventKind) {
        #[cfg(debug_assertions)]
        wakes.debug_assert_gate(&self.current_observation_gate());
        // Guard the kind first, so every path below — including the
        // receiverless early return — retires a *guarded* edge instead of
        // destroying a raw `Exit` under the observation gate.
        let kind = Guarded::new(kind);
        // Parent links cannot change under the resident-tree observation gate.
        // Resolve them once for snapshot and lifecycle propagation so one leaf
        // edge does not repeatedly lock every ancestor's parent mutex.
        let ancestors = self.ancestors_locked(wakes);
        // The resident-tree observation gate serializes every mint; the
        // atomic is the published watermark as well as the counter, avoiding
        // a second, provably uncontended lock on every lifecycle edge. The
        // mint is still a compare-and-swap so an emit that ever escaped the
        // gate could reorder events but never duplicate a sequence value.
        let seq = LifecycleSeq::new(
            self.observation
                .lifecycle_seq
                .mint(Ordering::Release, Ordering::Relaxed),
        );
        self.publish_snapshot_chain_through_locked(wakes, &ancestors);

        // Sequence minting and snapshot watermarks stay unconditional: the
        // catch-up protocol uses them across stretches with no subscribers.
        // When the whole propagation chain is receiverless, retire the
        // guarded kind after unlock and avoid event construction,
        // path extension, cloning, and per-hub publication entirely.
        if !self.observation.lifecycle.has_receivers()
            && ancestors
                .iter()
                .all(|ancestor| !ancestor.observation.lifecycle.has_receivers())
        {
            wakes.defer(move || drop(kind));
            return;
        }

        let scope = self.member.membership();
        let mut event = RetainedLifecycleEvent::new(scope, seq, kind);
        self.observation.lifecycle.publish(wakes, event.clone());
        let mut child_id = self.member.id().clone();
        for ancestor in &ancestors {
            event.prepend_scope(child_id);
            child_id = ancestor.member.id().clone();
            ancestor.observation.lifecycle.publish(wakes, event.clone());
        }
        // The producer's own copy still owns a retained exit. Retiring it here
        // would submit a disposal job — and can start a native thread — with
        // the observation gate held. This caller owns an effects sink, so it
        // takes the preferred path and retires after unlock.
        wakes.defer(move || drop(event));
    }

    pub(super) fn close_observation_locked(&self, wakes: &mut ObservationTxn<'_>) {
        #[cfg(debug_assertions)]
        wakes.debug_assert_gate(&self.current_observation_gate());
        if self.observation.closed.load(Ordering::Acquire) {
            return;
        }
        // Closure follows the final state/snapshot/event publication performed
        // by the caller while this same observation gate remains held.
        let scope = self.owned();
        self.observation
            .snapshots
            .close(wakes, move || scope.snapshot_locked());
        self.observation.lifecycle.close(wakes);
        // Both hub closures are idempotent. Set the aggregate marker last so
        // an unexpected panic leaves the operation retryable.
        self.observation.closed.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use shelterwood_core::{ChildId, ScopeState, identity::ScopeIdentity, policy::ScopeFlavor};

    use super::*;
    use crate::cells::MemberCell;

    #[test]
    fn a_staged_cut_is_installed_before_its_gate_is_released() {
        let id = ChildId::from("root");
        let mut identity = ScopeIdentity::new();
        let member = MemberCell::new(identity.mint_membership(&id));
        let scope = ScopeCell::new(member, ScopeFlavor::Dynamic, ScopeIdentity::new());
        let gate = scope.observation_gate();
        let receiver = scope.subscribe_snapshots();

        // Installation must not slide past the unlock: two transactions on
        // one gate would otherwise be free to interleave as "T1 stages, T1
        // unlocks, T2 stages and installs a newer cut, T1 installs its stale
        // one", leaving every ungated borrow behind the tree until the next
        // publication. The staged producer runs inside the install, so asking
        // it whether the gate is still held asks exactly that.
        let under_gate = Arc::new(AtomicBool::new(false));
        scope.with_observation_gate(|txn| {
            let probe = Arc::clone(&under_gate);
            let gate = gate.clone();
            let cell = Arc::clone(&scope);
            scope.observation.snapshots.publish(txn, move || {
                probe.store(gate.is_held(), Ordering::Relaxed);
                cell.snapshot_locked()
            });
        });

        assert!(
            under_gate.load(Ordering::Relaxed),
            "a staged cut is built and installed while the transaction still holds its gate"
        );
        assert_eq!(receiver.borrow_latest().state, ScopeState::Unstarted);
    }
}
