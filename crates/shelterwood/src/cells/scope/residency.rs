use std::sync::{Arc, MutexGuard, Weak};

#[cfg(test)]
use std::sync::atomic::Ordering;

use crate::runtime;
use shelterwood_core::{
    Membership,
    panic::{catch_panic, discard_panic},
};

use crate::cells::observe::LifecycleEventKind;

use crate::cells::{MemberCell, MemberTransition, ObservationGate, ObservationTxn};

#[cfg(test)]
use super::GateCapture;
use super::{
    ScopeCell,
    control::{ControlPoison, ScopeControlEvent},
};

/// Driver-independent view of one declaration slot while its membership is
/// resident in a running scope.
#[derive(Clone)]
pub(crate) struct ResidentProjection {
    pub member: Arc<MemberCell>,
    pub scope: Option<Arc<ScopeCell>>,
}

impl ResidentProjection {
    pub(crate) fn new(member: Arc<MemberCell>, scope: Option<Arc<ScopeCell>>) -> Self {
        Self { member, scope }
    }
}

/// One slot of a scope's observed child set, with its own unwind boundary.
///
/// A displaced set is disposed as a whole `Vec`, and `Vec`'s slice drop glue
/// keeps destroying the remaining elements after one of them panics. A
/// resident can own the last handle to a mailbox still holding unread user
/// messages, so without a boundary here a second hostile destructor in the
/// same set panics *inside* the first one's unwind, which aborts the process
/// rather than surfacing anywhere. SPEC §5.5 requires this lane to run with
/// per-element panic containment; keeping the boundary on the element rather
/// than on the collection is what makes it hold at every depth of a nested
/// scope's residency, and on `ScopeCell`'s own drop glue, not merely at the
/// displaced root.
///
/// The diagnostic is discarded rather than reported because every venue that
/// destroys a resident already discards it: `dispose_detached` passes an
/// empty completion, and plain drop glue has nobody to report to.
pub(super) struct ResidentChild {
    /// `None` only while [`Drop`] is destroying the projection.
    projection: Option<ResidentProjection>,
    /// Whether this scope published this resident's `Added` edge.
    ///
    /// Admission owns the residency slot across its fallible steps, so an
    /// unwind can leave a resident installed that was never announced. SPEC
    /// §3.2's pairing is exact — both edges or neither — so a withdrawal
    /// mirrors this flag rather than emitting `Removed` unconditionally, and
    /// the snapshot producer leaves an unannounced resident out of the
    /// observed child set for the same reason.
    pub(super) announced: bool,
}

impl ResidentChild {
    pub(super) fn new(projection: ResidentProjection) -> Self {
        Self {
            projection: Some(projection),
            announced: false,
        }
    }

    pub(super) fn projection(&self) -> &ResidentProjection {
        self.projection
            .as_ref()
            .expect("a resident child owns its projection until it is dropped")
    }

    /// Withdraws the upward observation edge for the resident being retired.
    ///
    /// This is deliberately not part of `Drop`: a refused duplicate admission
    /// temporarily owns a `ResidentChild`, but must not sever the subtree from
    /// its original parent. Prune and displacement call this only after they
    /// have selected their own resident for retirement.
    fn sever_parent(&self) {
        if let Some(scope) = &self.projection().scope {
            scope.sever_parent();
        }
    }
}

impl Drop for ResidentChild {
    fn drop(&mut self) {
        let Some(projection) = self.projection.take() else {
            return;
        };
        discard_panic(catch_panic(|| drop(projection)).err());
    }
}

/// The exact residency slot installed by one in-flight admission.
///
/// The observation gate prevents a production admission from interleaving a
/// second residency mutation, but the slot still carries its own index and
/// membership. That makes both of admission's later residency edits —
/// announcement on success, withdrawal on refusal — structurally address the
/// resident this admission installed instead of relying on whichever child
/// happens to be last.
///
/// Dropping the token unconsumed is the containment path, not a leak: an
/// unwind between the push and either edit deliberately leaves the resident
/// installed and unannounced, so scope clear retires its possibly-last mailbox
/// owner through detached disposal instead of unwinding it under the gate.
#[must_use = "an installed resident slot is announced on success and withdrawn on refusal"]
pub(super) struct ResidentSlot<'a> {
    scope: &'a ScopeCell,
    index: usize,
    membership: Membership,
}

impl ResidentSlot<'_> {
    /// Marks this admission's exact resident as announced.
    ///
    /// Returns whether the slot still holds it. The token and everything
    /// inspected under the residency mutex are plain framework-owned data, so
    /// this adds no effect to the doubled observation gate section's lock-rule
    /// accounting.
    pub(super) fn announce(self) -> bool {
        let mut children = self.scope.current_children();
        children
            .get_mut(self.index)
            .filter(|resident| resident.projection().member.membership() == self.membership)
            .is_some_and(|resident| {
                resident.announced = true;
                true
            })
    }

    /// Removes this admission's resident again, publishing neither edge.
    ///
    /// Returns the displaced resident together with whether the slot still
    /// held this admission's own. Popping is the diagnostic: no other
    /// residency mutation may interleave under the observation gate, so the
    /// entry this slot installed must still be the final one. A displaced
    /// resident can own the last handle to a mailbox holding unread user
    /// messages, so it is handed back by value for the caller to retire after
    /// unlock rather than dropped here.
    #[must_use = "a displaced resident retires after unlock, and the verdict is a diagnostic"]
    pub(super) fn withdraw(self) -> (Option<ResidentChild>, bool) {
        let mut children = self.scope.current_children();
        let is_admission = children.len() == self.index + 1
            && children[self.index].projection().member.membership() == self.membership;
        (children.pop(), is_admission)
    }
}

impl ScopeCell {
    pub(crate) fn resident_projections(&self) -> Vec<ResidentProjection> {
        self.with_observation_gate(|_| {
            self.current_children()
                .iter()
                .map(|resident| resident.projection().clone())
                .collect()
        })
    }

    pub(crate) fn has_resident_child(&self, member: &MemberCell) -> bool {
        self.with_observation_gate(|_| {
            self.current_children()
                .iter()
                .any(|resident| resident.projection().member.membership() == member.membership())
        })
    }

    /// Diagnostic for the dynamic admission install exemption.
    ///
    /// A reservation is adopted onto this scope's gate before publication and
    /// the live dynamic route prevents the root from being re-homed. The
    /// driver asserts that invariant when it creates the install ledger, so
    /// the later handoff check cannot acquire another gate under dynamic
    /// state.
    pub(crate) fn shares_observation_gate_with(&self, member: &MemberCell) -> bool {
        self.current_observation_gate()
            .shares_gate(&member.current_observation_gate())
    }

    pub(super) fn current_children(&self) -> MutexGuard<'_, Vec<ResidentChild>> {
        self.observation
            .current_children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn push_unannounced(&self, child: ResidentProjection) -> ResidentSlot<'_> {
        let membership = child.member.membership();
        let mut children = self.current_children();
        let index = children.len();
        children.push(ResidentChild::new(child));
        ResidentSlot {
            scope: self,
            index,
            membership,
        }
    }

    pub(super) fn parent(&self) -> Option<Arc<ScopeCell>> {
        #[cfg(test)]
        self.ancestor_parent_reads.fetch_add(1, Ordering::Relaxed);
        self.observation
            .parent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade)
    }

    fn sever_parent(&self) {
        *self
            .observation
            .parent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    #[cfg(test)]
    pub(crate) fn take_ancestor_parent_reads(&self) -> usize {
        self.ancestor_parent_reads.swap(0, Ordering::Relaxed)
    }

    fn set_parent(&self, parent: &Arc<ScopeCell>, txn: &mut ObservationTxn<'_>) {
        *self
            .observation
            .parent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::downgrade(parent));
        let control = self.control.lock().expect("scope control mutex poisoned");
        let pending_shutdown = control.shutdown.filter(|request| {
            !request.consumed && control.epochs.request_is_pending(request.epoch)
        });
        drop(control);
        if let Some(request) = pending_shutdown {
            parent.publish_control_event_locked(
                ScopeControlEvent::WindowStop {
                    membership: self.member.membership(),
                    target: request.epoch,
                },
                txn,
                ControlPoison::Reject,
            );
        }
    }

    fn adopt_observation_gate(
        &self,
        parent: &ScopeCell,
        gate: &ObservationGate,
        txn: &mut ObservationTxn<'_>,
    ) {
        if std::ptr::eq(self, parent) {
            txn.defer(|| panic!("a scope cannot adopt from itself"));
            return;
        }
        // The caller holds `gate` through `parent.with_observation_gate`.
        // Re-homing the parent would first have to acquire that same gate, so
        // rereading its installed pointer here cannot race a parent handoff.
        if !gate.shares_gate(&parent.current_observation_gate()) {
            txn.defer(|| {
                panic!("observation gates are adopted only in the parent-to-child direction")
            });
            return;
        }
        self.member.adopt_observation_gate_with(
            gate,
            || {
                #[cfg(test)]
                self.report_gate_capture(GateCapture::Adoption);
            },
            |current| {
                if self.dynamic_route_in(txn).is_some() {
                    txn.defer(|| panic!("a scope with a live dynamic route is never re-homed"));
                    return;
                }
                self.member.install_observation_gate_locked(current, gate);
                self.adopt_descendant_observation_gates_locked(current, gate, txn);
            },
        );
    }

    /// Admits this scope's subtree onto `gate`, or refuses without touching it.
    ///
    /// Legality is probed before anything is committed and applied only after
    /// the recursive handoff has succeeded. Both halves run under the child's
    /// own gate, held across the whole attempt, so no writer can change the
    /// stage between them; and an unwind out of the handoff's `assert!` leaves
    /// the record still reading its pre-admission stage.
    fn admit_observation_gate(
        &self,
        parent: &ScopeCell,
        gate: &ObservationGate,
        txn: &mut ObservationTxn<'_>,
    ) -> bool {
        if std::ptr::eq(self, parent) {
            txn.defer(|| panic!("a scope cannot be admitted into itself"));
            return false;
        }
        // The caller holds `gate` through `parent.with_observation_gate`.
        // Re-homing the parent would first have to acquire that same gate, so
        // rereading its installed pointer here cannot race a parent handoff.
        if !gate.shares_gate(&parent.current_observation_gate()) {
            txn.defer(|| {
                panic!("observation gates are admitted only in the parent-to-child direction")
            });
            return false;
        }
        self.member.with_handoff_gate(
            gate,
            || {
                #[cfg(test)]
                self.report_gate_capture(GateCapture::Adoption);
            },
            |current| {
                if !self.member.would_accept(&MemberTransition::Admitted) {
                    return false;
                }
                if !current.shares_gate(gate) {
                    // A live dynamic route needs a started driver, which needs
                    // a stage past `Reserved`; the probe above already refused
                    // every such stage, so re-homing one is unconstructible
                    // rather than merely unreached.
                    self.member.install_observation_gate_locked(current, gate);
                    self.adopt_descendant_observation_gates_locked(current, gate, txn);
                }
                let admitted = self
                    .member
                    .transition_locked(txn, MemberTransition::Admitted);
                if !admitted {
                    txn.defer(|| {
                        panic!("the probed admission cannot be refused under the same held gate")
                    });
                }
                admitted
            },
        )
    }

    pub(crate) fn adopt_child_observation_gate(
        self: &Arc<Self>,
        member: &MemberCell,
        child: Option<&ScopeCell>,
        txn: &mut ObservationTxn<'_>,
    ) {
        let gate = self.current_observation_gate();
        if let Some(child) = child {
            child.adopt_observation_gate(self, &gate, txn);
        } else {
            member.adopt_observation_gate(&gate, txn);
        }
    }

    /// Re-homes a resident subtree while its prior tree gate is held. The
    /// destination gate is also held, so observers cannot enter either tree
    /// while the handoff is installed recursively. Walking residents is
    /// exhaustive here: a reserved dynamic slot requires the live route that
    /// only a started driver installs, while gate adoption happens before
    /// that driver can run, and no running scope is subsequently re-homed.
    fn adopt_descendant_observation_gates_locked(
        &self,
        previous: &ObservationGate,
        gate: &ObservationGate,
        _txn: &mut ObservationTxn<'_>,
    ) {
        let descendants = self
            .current_children()
            .iter()
            .map(|resident| resident.projection().clone())
            .collect::<Vec<_>>();
        for descendant in descendants {
            descendant
                .member
                .install_observation_gate_locked(previous, gate);
            if let Some(scope) = descendant.scope {
                scope.adopt_descendant_observation_gates_locked(previous, gate, _txn);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn prune_child(&self, member: &MemberCell) -> bool {
        self.with_observation_gate(|wakes| self.prune_child_locked(member, wakes))
    }

    pub(crate) fn prune_child_locked(
        &self,
        member: &MemberCell,
        txn: &mut ObservationTxn<'_>,
    ) -> bool {
        let membership = member.membership();
        let resident = {
            let mut children = self.current_children();
            let index = children
                .iter()
                .position(|child| child.projection().member.membership() == membership);
            index.map(|index| children.remove(index))
        };
        let Some(resident) = resident else {
            return false;
        };
        // Residency is the ownership edge for upward observation. Withdraw
        // the nested scope's parent link in the same gate transaction that
        // removes the resident, before publishing `Removed` or retiring its
        // graph. An unannounced resident takes this path too.
        resident.sever_parent();
        // Mirror the announcement: a resident an unwound admission installed
        // never published `Added`, and SPEC §3.2's pairing is both edges or
        // neither. It is still withdrawn and disposed.
        let event = resident.announced.then(|| LifecycleEventKind::Removed {
            id: resident.projection().member.id().clone(),
            membership,
            last_incarnation: resident.projection().member.record().last_incarnation,
        });
        // The projection can carry the last member/mailbox owner. Put it in
        // the transaction before the fallible publication path so unwind also
        // retires it only after the observation gate is released. The detached
        // handoff deliberately makes final member teardown asynchronous.
        txn.defer(move || runtime::dispose_detached(resident));
        if let Some(event) = event {
            self.emit_locked(txn, event);
        }
        true
    }

    /// Replaces this scope's residency, returning whether every child was
    /// admitted. A refused child is left out of the residency entirely.
    #[must_use = "a refused admission leaves the child out of the residency"]
    pub(crate) fn set_admitted_children(
        self: &Arc<Self>,
        children: Vec<ResidentProjection>,
    ) -> bool {
        self.with_observation_gate(|wakes| {
            self.clear_residents_locked(wakes);
            let mut admitted = true;
            for child in children {
                admitted &= self.admit_child_locked(child, wakes);
            }
            admitted
        })
    }

    #[cfg(test)]
    #[must_use = "a refused admission leaves the child out of the residency"]
    pub(crate) fn admit_child(self: &Arc<Self>, child: ResidentProjection) -> bool {
        self.with_observation_gate(|wakes| self.admit_child_locked(child, wakes))
    }

    /// Admits one projection, or refuses it whole.
    ///
    /// Returns whether the member's `Admitted` transition was legal. A refusal
    /// publishes nothing: no gate handoff, no parent wiring, no retained
    /// residency and no `Added` event. The residency diagnostic below returns
    /// `false` after those steps have landed, but it also queues a panic, so no
    /// caller observes that verdict.
    ///
    /// A *panic* between the residency push and the `Added` publication is
    /// different: residency keeps the resident so its possibly-last mailbox
    /// owner retires through detached disposal at scope clear rather than
    /// unwinding under the gate. Such a resident stays `announced == false`,
    /// which keeps it out of the observed child set and out of the `Removed`
    /// edge its withdrawal would otherwise publish — SPEC §3.2's pairing is
    /// both edges or neither.
    #[must_use = "a refused admission leaves the child out of the residency"]
    pub(crate) fn admit_child_locked(
        self: &Arc<Self>,
        child: ResidentProjection,
        txn: &mut ObservationTxn<'_>,
    ) -> bool {
        // Residency takes ownership before every fallible step below. A panic
        // can therefore leave this entry half-wired, but it cannot unwind the
        // last owner of a mailbox-bearing member through the observation gate;
        // scope teardown later routes the resident through detached disposal.
        //
        // Every reader of `current_children` is accounted for during this
        // window. Snapshot construction, descendant handoff, terminal lookup,
        // pruning and clearing already run under this observation gate. The
        // public shutdown projection and driver terminality membership probe
        // (`resident_projections` / `has_resident_child`) take the same gate
        // themselves. The admission-local clone below is consequently the
        // only code that can observe this entry before its transition lands.
        // Publication readers additionally skip an unannounced entry, which is
        // what makes a lingering half-wired resident invisible rather than a
        // membership with only half its edges.
        let projection = child.clone();
        let resident_slot = self.push_unannounced(child);
        let gate = self.current_observation_gate();
        let admitted = if let Some(scope) = &projection.scope {
            let admitted = scope.admit_observation_gate(self, &gate, txn);
            if admitted {
                scope.set_parent(self, txn);
            }
            admitted
        } else {
            // Same probe-handoff-apply order as the nested-scope path above.
            projection.member.with_handoff_gate(
                &gate,
                || {},
                |current| {
                    if !projection.member.would_accept(&MemberTransition::Admitted) {
                        return false;
                    }
                    if !current.shares_gate(&gate) {
                        projection
                            .member
                            .install_observation_gate_locked(current, &gate);
                    }
                    let admitted = projection
                        .member
                        .transition_locked(txn, MemberTransition::Admitted);
                    if !admitted {
                        txn.defer(|| {
                            panic!(
                                "the probed admission cannot be refused under the same held gate"
                            )
                        });
                    }
                    admitted
                },
            )
        };
        if !admitted {
            // Refusal withdraws through the same token the announcement below
            // consumes: it removes the entry pushed above without emitting
            // either half of the Added/Removed pair and retires its
            // potentially last mailbox owner after unlock.
            let (rejected, rejected_is_admission) = resident_slot.withdraw();
            // Move both the resident and this function's lookup clone into
            // the effect before diagnosing the structural pop-last
            // invariant. If that diagnostic ever fires, neither possibly-last
            // mailbox owner may unwind through the observation gate.
            txn.defer(move || runtime::dispose_detached((projection, rejected)));
            if !rejected_is_admission {
                txn.defer(|| panic!("refusal removes the admission-local resident"));
            }
            return false;
        }
        let id = projection.member.id().clone();
        let membership = projection.member.membership();
        // Mark this admission's exact resident before `emit_locked`: the Added
        // publication schedules the commit-time snapshot, and that producer
        // walks residency and skips whatever is still unannounced. Refusing a
        // missing or mismatched index prevents a different resident from being
        // paired with this Added edge and diagnoses the same no-interleaving
        // invariant as the refusal withdrawal above.
        let announced_is_admission = resident_slot.announce();
        if !announced_is_admission {
            // Residency no longer holds this admission, so the lookup clone
            // can be the last member/mailbox owner. Move it into the
            // transaction ahead of the diagnostic, exactly as the refusal pop
            // above does, rather than unwinding it under the observation gate.
            txn.defer(move || runtime::dispose_detached(projection));
            txn.defer(|| panic!("success announces the admission-local resident"));
            return false;
        }
        self.emit_locked(txn, LifecycleEventKind::Added { id, membership });
        true
    }

    pub(crate) fn clear_residents(&self) {
        self.with_observation_gate(|wakes| self.clear_residents_locked(wakes));
    }

    pub(crate) fn clear_residents_locked(&self, wakes: &mut ObservationTxn<'_>) {
        let residents = {
            let mut children = self.current_children();
            std::mem::take(&mut *children)
        };
        // Sever each nested observation edge before any `Removed` publication
        // or detached graph retirement. The parent mutex contains only a
        // framework-owned weak link, so this remains legal under the resident
        // tree's observation gate.
        for resident in &residents {
            resident.sever_parent();
        }
        // An unannounced resident is one an admission installed and then
        // unwound past. It never published `Added`, so SPEC §3.2's exact
        // pairing forbids publishing its `Removed`; it is still displaced and
        // disposed with the rest of the set.
        let removals = residents
            .iter()
            .filter(|resident| resident.announced)
            .map(|resident| LifecycleEventKind::Removed {
                id: resident.projection().member.id().clone(),
                membership: resident.projection().member.membership(),
                last_incarnation: resident.projection().member.record().last_incarnation,
            })
            .collect::<Vec<_>>();
        // Schedule the whole displaced set before emitting any edge. This
        // both preserves last-owner disposal and makes an unwind retire the
        // untouched suffix after unlock. Detached disposal means final member
        // teardown may complete after this transaction returns -- and that a
        // resident's own destructor can never reach an `ObservationTxn`, so
        // SPEC §15.5 requires this removal site to emit the edge explicitly
        // below instead.
        wakes.defer(move || runtime::dispose_detached(residents));
        for removal in removals {
            self.emit_locked(wakes, removal);
        }
    }
}
