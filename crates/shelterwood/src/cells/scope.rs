use std::sync::{Arc, Mutex, Weak};

#[cfg(test)]
use std::sync::atomic::AtomicUsize;

use crate::runtime;
use shelterwood_core::{
    engine::ScopeState,
    identity::{
        AtomicMonotonicCounter, ChildId, MembershipReconciliation, MintedMembership,
        ProvisionalMembership, ScopeIdentity,
    },
    policy::{Intensity, ScopeFlavor, TotalRestarts},
};

use crate::cells::observe::{LifecycleHub, SnapshotHub};

use super::{Guarded, MemberCell, MemberRecord, ObservationGate, ObservationTxn};

/// Shared scope state follows two distinct synchronization regimes.
///
/// One gate per resident tree serializes compound observation-visible
/// transitions across configuration, records, resident children, and parent
/// links. Configuration, residency, and ancestry are plain synchronized state;
/// their individual locks do not make a multi-field transition atomic, so
/// every recursive observation path continues to hold that one tree gate.
///
/// Control requests, the dynamic route, records, residency, and hubs retain
/// their narrow storage locks, but every mutation is subordinate to the tree
/// gate and takes an [`ObservationTxn`] capability. Identity allocation and
/// lifecycle sequence minting remain independent driver-only counters. The
/// member-record watch is intentionally also the driver's wake bus.
struct ScopeObservation {
    intensity: Mutex<Intensity>,
    record: runtime::WatchSender<Guarded<ScopeRecord>>,
    // Removal paths move residents into transaction effects before emitting
    // their `Removed` edges. A projection can be the last member/mailbox
    // owner, so neither this mutex nor the observation gate may retire one.
    current_children: Mutex<Vec<ResidentChild>>,
    parent: Mutex<Option<Weak<ScopeCell>>>,
    lifecycle_seq: AtomicMonotonicCounter,
    lifecycle: LifecycleHub,
    snapshots: SnapshotHub,
}

pub(crate) struct ScopeCell {
    pub member: Arc<MemberCell>,
    pub flavor: ScopeFlavor,
    /// This cell's own handle, for work it defers past the current borrow.
    ///
    /// Snapshot construction is staged during a transaction and runs at its
    /// commit, by which time the `&self` that staged it has returned, so the
    /// producer must own the cell rather than borrow it.
    me: Weak<ScopeCell>,
    child_identity: Mutex<ScopeIdentity>,
    control: Mutex<ScopeControl>,
    dynamic_route: Mutex<Option<Arc<dyn DynamicRoute>>>,
    observation: ScopeObservation,
    #[cfg(test)]
    ancestor_parent_reads: AtomicUsize,
    #[cfg(test)]
    runtime_storage: Mutex<RuntimeStorage>,
    #[cfg(test)]
    gate_capture_probe: Mutex<Option<std::sync::mpsc::Sender<GateCapture>>>,
}

/// One observation-gate capture reported to a test probe.
///
/// A capture is reported after a thread has cloned the gate it is about to
/// acquire and before it blocks on that acquisition. Unit tests use these
/// reports as explicit barriers in place of scheduler or strong-count
/// polling: receiving a capture proves the reporting thread committed to the
/// gate that was current at that instant.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GateCapture {
    /// [`ScopeCell::with_observation_gate`] captured its current gate.
    Observation,
    /// Gate adoption captured an obsolete gate it must acquire to hand off.
    Adoption,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeStorage {
    pub children: usize,
    pub child_slots: usize,
    pub deadlines: usize,
    pub deadline_slots: usize,
}

mod control;
mod projection;
mod publication;
mod residency;
#[cfg(test)]
mod stress_tests;
#[cfg(test)]
mod tests;

pub(crate) use control::{DynamicRoute, ScopeControlEvent};
pub(crate) use projection::ScopeRecord;
pub(crate) use residency::ResidentProjection;

use control::ScopeControl;
use residency::ResidentChild;

impl ScopeCell {
    pub(crate) fn mint_membership(&self, id: &ChildId) -> MintedMembership {
        self.child_identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .mint_membership(id)
    }

    pub(crate) fn adopt_or_mint_membership(
        &self,
        provisional: ProvisionalMembership,
    ) -> MembershipReconciliation {
        self.child_identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .adopt_or_mint_membership(provisional)
    }

    pub(crate) fn evict_child_identity(&self, member: &MemberCell) {
        self.child_identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .evict(member.id(), member.membership());
    }

    pub(crate) fn new(
        member: Arc<MemberCell>,
        flavor: ScopeFlavor,
        child_identity: ScopeIdentity,
    ) -> Arc<Self> {
        let (record, _) = runtime::watch(Guarded::new(ScopeRecord {
            state: ScopeState::Unstarted,
            startup: None,
            total_restarts: TotalRestarts::ZERO,
        }));
        Arc::new_cyclic(|me| Self {
            member,
            flavor,
            me: me.clone(),
            child_identity: Mutex::new(child_identity),
            control: Mutex::new(ScopeControl::default()),
            dynamic_route: Mutex::new(None),
            observation: ScopeObservation {
                intensity: Mutex::new(Intensity::default()),
                record,
                current_children: Mutex::new(Vec::new()),
                parent: Mutex::new(None),
                lifecycle_seq: AtomicMonotonicCounter::new(),
                lifecycle: LifecycleHub::default(),
                snapshots: SnapshotHub::default(),
            },
            #[cfg(test)]
            ancestor_parent_reads: AtomicUsize::new(0),
            #[cfg(test)]
            runtime_storage: Mutex::new(RuntimeStorage::default()),
            #[cfg(test)]
            gate_capture_probe: Mutex::new(None),
        })
    }

    /// An owning handle to this cell.
    ///
    /// `ScopeCell::new` is the only constructor and it publishes the cell
    /// inside an `Arc`, so the upgrade can only fail from within this cell's
    /// own destructor — which observes nothing.
    fn owned(&self) -> Arc<ScopeCell> {
        self.me
            .upgrade()
            .expect("a live scope cell owns a handle to itself")
    }

    #[cfg(test)]
    pub(crate) fn runtime_storage(&self) -> RuntimeStorage {
        *self
            .runtime_storage
            .lock()
            .expect("runtime-storage mutex poisoned")
    }

    #[cfg(test)]
    pub(crate) fn record_runtime_storage(&self, storage: RuntimeStorage) {
        *self
            .runtime_storage
            .lock()
            .expect("runtime-storage mutex poisoned") = storage;
    }

    #[cfg(test)]
    pub(crate) fn observation_gate(&self) -> ObservationGate {
        self.current_observation_gate()
    }

    /// Installs a probe reporting every gate capture made through this scope.
    #[cfg(test)]
    pub(crate) fn probe_gate_captures(&self) -> std::sync::mpsc::Receiver<GateCapture> {
        let (sender, receiver) = std::sync::mpsc::channel();
        *self
            .gate_capture_probe
            .lock()
            .expect("gate capture probe mutex poisoned") = Some(sender);
        receiver
    }

    #[cfg(test)]
    fn report_gate_capture(&self, capture: GateCapture) {
        if let Some(probe) = &*self
            .gate_capture_probe
            .lock()
            .expect("gate capture probe mutex poisoned")
        {
            // The probe channel is unbounded, so reporting never blocks and
            // cannot reorder the acquisition it announces.
            let _ = probe.send(capture);
        }
    }

    fn current_observation_gate(&self) -> ObservationGate {
        self.member.current_observation_gate()
    }

    /// Runs against the current resident-tree observation gate. Adoption can
    /// race an early pre-start observer, so an obsolete-gate acquisition is
    /// detected and retried before the operation enters its critical section.
    pub(crate) fn with_observation_gate<R>(
        &self,
        operation: impl FnOnce(&mut ObservationTxn<'_>) -> R,
    ) -> R {
        self.member.with_observation_txn_probed(
            || {
                #[cfg(test)]
                self.report_gate_capture(GateCapture::Observation);
            },
            operation,
        )
    }

    #[cfg(test)]
    pub(crate) fn replace_observation_gate(&self, gate: ObservationGate) {
        *self
            .member
            .observation_gate
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = gate;
    }

    pub(crate) fn signal(&self) -> &runtime::WatchSender<Guarded<MemberRecord>> {
        &self.member.record
    }
}
