use std::sync::{Arc, atomic::Ordering};

use shelterwood_core::{
    Intensity, Strategy, TotalRestarts, engine::ScopeState, exit::StartupError, policy::ScopeFlavor,
};

#[cfg(test)]
use crate::cells::ObservationGate;
#[cfg(any(test, feature = "test-util"))]
use shelterwood_runtime as runtime;

use crate::{
    cells::{MemberRecord, MemberStage, ObservationTxn, RetainedExit},
    observe::{
        ChildSnapshot, ChildState, LifecycleEventKind, LifecycleEvents, LifecycleSeq,
        RetainedLifecycleEvent, RetainedScopeSnapshot, ScopeSnapshot, SnapshotReceiver,
    },
};

use super::{ResidentProjection, ScopeCell};

/// Observation-only projection of the authoritative engine lifecycle.
/// Driver decisions never read this record back as liveness policy.
#[derive(Clone, Debug)]
pub struct ScopeRecord {
    pub state: ScopeState,
    pub startup: Option<Result<(), StartupError>>,
    /// Read only by this crate's snapshot publication; the driver takes its
    /// restart totals from the decision that produced them.
    pub(crate) total_restarts: TotalRestarts,
    // Keep this field after every public projection that can contain a child
    // exit. ScopeRecord clones share the guard allocation, so read-only
    // observation does not submit one disposal job per read.
    pub(super) retained_exits: Arc<Vec<RetainedExit>>,
}

impl ScopeRecord {
    pub(super) fn refresh_retained_exits(&mut self) {
        let mut retained = Vec::new();
        RetainedExit::retain_scope_state(&mut retained, &self.state);
        if let Some(startup) = &self.startup {
            RetainedExit::retain_startup_result(&mut retained, startup);
        }
        RetainedExit::install(&mut self.retained_exits, retained);
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ObservationConfig {
    intensity: Intensity,
}

impl ScopeCell {
    pub fn record(&self) -> ScopeRecord {
        self.observation.record.read_cloned()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn record_watcher(&self) -> runtime::WatchReceiver<ScopeRecord> {
        self.observation.record.watcher()
    }

    pub fn set_observation_config(&self, intensity: Intensity) {
        self.with_observation_gate(|wakes| {
            *self
                .observation
                .config
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                ObservationConfig { intensity };
            self.publish_snapshot_chain_locked(wakes);
        });
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn emit(&self, event: LifecycleEventKind) {
        self.with_observation_gate(|wakes| self.emit_locked(wakes, event));
    }

    pub fn snapshot(&self) -> Arc<ScopeSnapshot> {
        self.with_observation_gate(|_| self.snapshot_locked().into_public())
    }

    pub fn subscribe_snapshots(&self) -> SnapshotReceiver {
        self.with_observation_gate(|wakes| {
            let receiver = self
                .observation
                .snapshots
                .subscribe(self.snapshot_locked(), wakes);
            debug_assert!(
                !self.observation.closed.load(Ordering::Acquire)
                    || receiver.borrow_latest_and_closed().1,
                "closed snapshot state is installed before later subscriptions"
            );
            receiver
        })
    }

    pub fn subscribe_lifecycle(&self) -> LifecycleEvents {
        self.with_observation_gate(|_txn| {
            let events = self.observation.lifecycle.subscribe();
            debug_assert!(
                !self.observation.closed.load(Ordering::Acquire)
                    || self.observation.lifecycle.is_closed(),
                "closed lifecycle state is installed before later subscriptions"
            );
            events
        })
    }

    fn snapshot_locked(&self) -> RetainedScopeSnapshot {
        let record = self.record();
        let config = *self
            .observation
            .config
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let children = self.current_children();
        let mut projected = Vec::with_capacity(children.len());
        let mut retained_exits = Vec::new();
        RetainedExit::retain_scope_state(&mut retained_exits, &record.state);
        for resident in children.iter() {
            let (child, exits) = self.child_snapshot_locked(&resident.projection);
            projected.push(child);
            for exit in exits {
                RetainedExit::retain_owned(&mut retained_exits, exit);
            }
        }
        let snapshot = Arc::new(ScopeSnapshot {
            state: record.state,
            kind: self.flavor,
            strategy: (self.flavor == ScopeFlavor::Ordered).then_some(Strategy::default()),
            intensity: config.intensity,
            total_restarts: record.total_restarts,
            lifecycle_seq: LifecycleSeq::new(
                self.observation.lifecycle_seq.load(Ordering::Acquire),
            ),
            children: projected.into(),
        });
        RetainedScopeSnapshot::new(snapshot, retained_exits)
    }

    fn child_snapshot_locked(
        &self,
        child: &ResidentProjection,
    ) -> (ChildSnapshot, Vec<RetainedExit>) {
        let MemberRecord {
            stage,
            incarnation,
            last_incarnation: _,
            last_exit,
            restart_count,
            restart_at,
            membership_status,
            startup_aborted,
            // Held to the end of this projection: the record's own guards keep
            // every raw exit below alive while the snapshot's guard set is
            // built from them.
            retained_exits: record_guards,
        } = child.member.record();
        let options = child.member.options();
        let terminal = matches!(&stage, MemberStage::Terminal(_));
        let mut retained_exits = Vec::new();
        let nested = child
            .scope
            .as_ref()
            .and_then(|scope| (incarnation.is_some() || terminal).then(|| scope.snapshot_locked()))
            .map(|nested| {
                let (snapshot, exits) = nested.into_parts();
                for exit in exits {
                    RetainedExit::retain_owned(&mut retained_exits, exit);
                }
                snapshot
            });
        let state = match stage {
            MemberStage::Reserved | MemberStage::Admitted => ChildState::Admitted,
            MemberStage::Starting => ChildState::Starting,
            MemberStage::Running => ChildState::Running,
            MemberStage::Restarting => ChildState::Restarting,
            MemberStage::Stopping => ChildState::Stopping,
            MemberStage::Terminal(exit) if startup_aborted => {
                RetainedExit::retain_exit(&mut retained_exits, &exit);
                ChildState::StartupAborted { exit }
            }
            MemberStage::Terminal(exit) => {
                RetainedExit::retain_exit(&mut retained_exits, &exit);
                ChildState::Stopped { exit }
            }
        };
        let last_exit = last_exit.inspect(|exit| {
            RetainedExit::retain_exit(&mut retained_exits, exit);
        });
        let snapshot = ChildSnapshot {
            id: child.member.id().clone(),
            membership: child.member.membership(),
            incarnation,
            state,
            last_exit,
            membership_status,
            restart_count,
            restart_policy: options.restart,
            retention: options.retention,
            restart_at,
            nested,
            scope_seq: child.scope.as_ref().map(|scope| {
                LifecycleSeq::new(scope.observation.lifecycle_seq.load(Ordering::Acquire))
            }),
        };
        // The projection above now owns a raw copy of everything these guards
        // cover, so releasing them here is refcount traffic; their last owner
        // is the member record itself.
        drop(record_guards);
        (snapshot, retained_exits)
    }

    fn ancestors_locked(&self) -> Vec<Arc<ScopeCell>> {
        let mut ancestors = Vec::new();
        let mut current = self.parent();
        while let Some(scope) = current {
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
        // Each producer owns the scope it projects: the cut is built at
        // commit, once per hub, after every publication in this transaction
        // has been coalesced onto it.
        let scope = self.owned();
        self.observation
            .snapshots
            .publish(wakes, move || scope.snapshot_locked());
        for ancestor in ancestors {
            let scope = Arc::clone(ancestor);
            ancestor
                .observation
                .snapshots
                .publish(wakes, move || scope.snapshot_locked());
        }
    }

    pub(super) fn publish_snapshot_chain_locked(&self, wakes: &mut ObservationTxn<'_>) {
        let ancestors = self.ancestors_locked();
        self.publish_snapshot_chain_through_locked(wakes, &ancestors);
    }

    pub(super) fn emit_locked(&self, wakes: &mut ObservationTxn<'_>, kind: LifecycleEventKind) {
        // Mint the retention guards before any fallible framework bookkeeping.
        // A sequence-exhaustion path can then defer a *guarded* edge instead
        // of destroying a raw `Exit` under the observation gate.
        let guards = RetainedLifecycleEvent::retain_guards(&kind);
        // Parent links cannot change under the resident-tree observation gate.
        // Resolve them once for snapshot and lifecycle propagation so one leaf
        // edge does not repeatedly lock every ancestor's parent mutex.
        let ancestors = self.ancestors_locked();
        // The resident-tree observation gate serializes every mint; the
        // atomic is the published watermark as well as the counter, avoiding
        // a second, provably uncontended lock on every lifecycle edge. The
        // mint is still a compare-and-swap so an emit that ever escaped the
        // gate could reorder events but never duplicate a sequence value.
        let seq = self
            .observation
            .lifecycle_seq
            .mint(Ordering::Release, Ordering::Relaxed);
        let Some(seq) = seq.map(LifecycleSeq::new) else {
            // Raw projection first, guards last, so the guards are the final
            // owner and destruction is submitted to isolated disposal — the
            // same field-order argument `RetainedLifecycleEvent` makes.
            wakes.defer(move || {
                drop(kind);
                drop(guards);
            });
            self.publish_snapshot_chain_through_locked(wakes, &ancestors);
            self.observation.lifecycle.publish_lagged(wakes, 1);
            for ancestor in &ancestors {
                ancestor.observation.lifecycle.publish_lagged(wakes, 1);
            }
            return;
        };
        self.publish_snapshot_chain_through_locked(wakes, &ancestors);

        let scope = self.member.membership();
        let mut event = RetainedLifecycleEvent::from_parts(scope, seq, kind, guards);
        self.observation.lifecycle.publish(wakes, event.clone());
        let mut child_id = self.member.id().clone();
        for ancestor in ancestors {
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

    #[cfg(any(test, feature = "test-util"))]
    pub fn set_lifecycle_sequence(&self, current: u64) {
        self.observation
            .lifecycle_seq
            .set(current, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fmt,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        time::Duration,
    };

    use shelterwood_core::{
        Cancellation, ChildId, Exit, ExitError, ScopeState, identity::ScopeIdentity,
        policy::ScopeFlavor,
    };

    use super::*;
    use crate::cells::MemberCell;

    struct GateDropError {
        gate: super::ObservationGate,
        dropped: mpsc::SyncSender<bool>,
    }

    impl fmt::Debug for GateDropError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("GateDropError")
        }
    }

    impl fmt::Display for GateDropError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("gate drop probe")
        }
    }

    impl std::error::Error for GateDropError {}

    impl Drop for GateDropError {
        fn drop(&mut self) {
            let _ = self.dropped.send(!self.gate.is_held());
        }
    }

    #[test]
    fn a_staged_cut_is_installed_before_its_gate_is_released() {
        let id = ChildId::from("root");
        let mut identity = ScopeIdentity::new();
        let member = MemberCell::new(
            id.clone(),
            identity
                .mint_membership(&id)
                .expect("root membership is available"),
        );
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

    #[test]
    fn lifecycle_sequence_exhaustion_retires_the_exit_after_unlock() {
        let id = ChildId::from("root");
        let mut identity = ScopeIdentity::new();
        let member = MemberCell::new(
            id.clone(),
            identity
                .mint_membership(&id)
                .expect("root membership is available"),
        );
        let membership = member.membership();
        let mut incarnations = member.take_incarnation_counter();
        let scope = ScopeCell::new(member, ScopeFlavor::Dynamic, ScopeIdentity::new());
        let gate = scope.observation_gate();
        scope.set_lifecycle_sequence(u64::MAX - 2);
        scope.emit(LifecycleEventKind::ScopeState {
            state: ScopeState::Running,
        });
        let (dropped, observed) = mpsc::sync_channel(1);
        let exhausted = LifecycleEventKind::Exited {
            id,
            membership,
            incarnation: incarnations.mint().expect("incarnation available"),
            exit: Exit::failed(
                ExitError::from(GateDropError { gate, dropped }),
                Cancellation::NotObserved,
            ),
        };

        // Retiring a `RetainedExit` never destroys the user error inline: it
        // always submits the value to isolated disposal, so the destructor
        // runs on a worker thread in either arrangement and its own view of
        // the gate is a race. What the deferral changes is *when the
        // submission happens*. Hold the gate open across the emit: nothing
        // can arrive on this channel unless the submission already happened
        // under the gate.
        let retired_under_gate = scope.with_observation_gate(|wakes| {
            scope.emit_locked(wakes, exhausted);
            observed.recv_timeout(Duration::from_millis(500)).is_ok()
        });
        assert!(
            !retired_under_gate,
            "an unsequenced exit must not be submitted for disposal under the observation gate"
        );

        assert!(
            observed
                .recv_timeout(Duration::from_secs(10))
                .expect("retained exit destructor reports"),
            "an unsequenced exit is retired after the observation gate unlocks"
        );
    }
}
