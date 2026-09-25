//! The scope control plane: incarnation epochs, shutdown and force requests,
//! control events for the parent driver, and the dynamic-admission route.

use std::{
    any::Any,
    collections::VecDeque,
    sync::{Arc, MutexGuard},
};

use shelterwood_core::{
    engine::{Epoch, RequestTarget, ScopeEpochs, ScopeState},
    exit::{Exit, StopReason},
    identity::Membership,
    policy::TotalRestarts,
};

use crate::cells::observe::LifecycleEventKind;

use crate::cells::{Guarded, MemberStage, ObservationTxn, Retained, StartupDisposition};

use super::ScopeCell;

/// Crate-private close-admission hook retained by a restart-stable scope cell.
///
/// # Implementation boundary
///
/// This is not a user extension point. Its crate visibility makes a foreign
/// implementation unrepresentable. The callback runs while the restart-stable
/// tree owns its observation gate, and requiring the transaction capability in
/// the signature keeps that critical-section boundary explicit.
pub(crate) trait DynamicRoute: Any + Send + Sync {
    fn close_admission(&self, txn: &mut ObservationTxn<'_>);
}

#[derive(Debug, Default)]
pub(super) struct ScopeControl {
    pub(super) epochs: ScopeEpochs,
    pub(super) shutdown: Option<ScopeRequest>,
    pub(super) force: Option<ScopeRequest>,
    pub(super) events: VecDeque<ScopeControlEvent>,
}

/// How a control-mutex acquisition treats poison. Ordinary callers reject
/// it. Destructor paths ignore it, because a panic there aborts a thread that
/// is already unwinding. Control holds plain request and epoch state, so a
/// poisoner's partial update is no worse than any other racing write.
#[derive(Clone, Copy)]
pub(super) enum ControlPoison {
    Reject,
    Ignore,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ScopeRequest {
    pub(super) epoch: Epoch,
    pub(super) consumed: bool,
}

#[derive(Clone, Copy)]
enum ScopeRequestSlot {
    Shutdown,
    Force,
}

impl ScopeRequestSlot {
    fn get(self, control: &ScopeControl) -> Option<&ScopeRequest> {
        match self {
            Self::Shutdown => control.shutdown.as_ref(),
            Self::Force => control.force.as_ref(),
        }
    }

    fn get_mut(self, control: &mut ScopeControl) -> Option<&mut ScopeRequest> {
        match self {
            Self::Shutdown => control.shutdown.as_mut(),
            Self::Force => control.force.as_mut(),
        }
    }
}

/// One fact published by a scope control-plane transaction for its driver.
///
/// The payload is restart-stable identity rather than a mutable driver key.
/// The driver resolves it through its incrementally maintained membership
/// index, so stale events miss instead of addressing a replacement child.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScopeControlEvent {
    /// A shutdown request was accepted while the member scope had no live
    /// incarnation — a restart window or a pre-spawn handle. The parent
    /// resolves it in place, without constructing the targeted incarnation.
    WindowStop {
        membership: Membership,
        target: Epoch,
    },
}

impl ScopeCell {
    #[cfg(test)]
    pub(crate) fn begin_incarnation(&self, state: ScopeState) -> Option<Epoch> {
        let mut epoch = None;
        self.begin_incarnation_into(state, &mut epoch);
        epoch
    }

    /// Installs epoch ownership before publishing startup. The caller keeps
    /// its owning guard outside this call so its destructor runs after the
    /// observation transaction releases the gate, including on a wake panic.
    pub(crate) fn begin_incarnation_into(&self, state: ScopeState, owned: &mut Option<Epoch>) {
        assert!(owned.is_none(), "a scope epoch owner begins empty");
        assert!(
            matches!(state, ScopeState::Starting),
            "a fresh incarnation publishes its lifecycle machine's initial state"
        );
        let projection_idle = self.with_observation_gate(|wakes| {
            let projection_idle = matches!(
                self.record().state,
                ScopeState::Unstarted | ScopeState::Stopped { .. }
            );
            if !projection_idle {
                return false;
            }
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            let Some(epoch) = control.epochs.begin() else {
                return true;
            };
            *owned = Some(epoch);
            // The idle epoch plane pairs only with a settled projection:
            // `Unstarted` before any mint, `Stopped` after every finish. That
            // pairing is what lets `settled` treat terminal membership
            // plus a settled projection as final without stranding a scope
            // that still owns a live incarnation.
            self.observation.record.modify_silently(|record| {
                record.update(wakes, |record| {
                    record.total_restarts = TotalRestarts::ZERO;
                    record.startup = None;
                    record.state = state.clone();
                });
            });
            // Hold epoch ownership through its observation projection, so a
            // stale finish and a newer begin cannot cross these two state
            // planes in opposite orders.
            drop(control);
            wakes.pulse(&self.observation.record);
            wakes.pulse(&self.member.record);
            self.emit_locked(wakes, LifecycleEventKind::ScopeState { state });
            true
        });
        assert!(
            projection_idle,
            "an idle scope projection is Unstarted or Stopped before a fresh incarnation mints"
        );
    }

    pub(super) fn lock_control(&self, poison: ControlPoison) -> MutexGuard<'_, ScopeControl> {
        match poison {
            ControlPoison::Reject => self.control.lock().expect("scope control mutex poisoned"),
            ControlPoison::Ignore => self
                .control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        }
    }

    pub(crate) fn finish_incarnation(&self, epoch: Epoch, reason: StopReason) {
        self.finish_incarnation_with_terminal(
            epoch,
            Guarded::new(reason),
            None,
            ControlPoison::Reject,
        );
    }

    /// [`Self::finish_incarnation`] for destructors: tolerates a poisoned
    /// control mutex, like [`Self::request_shutdown_ignoring_poison`].
    pub(crate) fn finish_incarnation_ignoring_poison(&self, epoch: Epoch, reason: StopReason) {
        self.finish_incarnation_with_terminal(
            epoch,
            Guarded::new(reason),
            None,
            ControlPoison::Ignore,
        );
    }

    /// Takes the terminal exit as a carrier so a root completion never
    /// extracts a raw `Exit` between framework layers. Wrapping before the
    /// stop reason keeps a failed payload guarded across every step of this
    /// call, including the reason's own retention walk.
    #[cfg(test)]
    pub(crate) fn finish_root_incarnation(
        &self,
        epoch: Epoch,
        reason: StopReason,
        exit: impl Into<Retained<Exit>>,
    ) {
        let exit = exit.into();
        self.finish_incarnation_with_terminal(
            epoch,
            Guarded::new(reason),
            Some(exit),
            ControlPoison::Reject,
        );
    }

    fn finish_incarnation_with_terminal(
        &self,
        epoch: Epoch,
        reason: Guarded<StopReason>,
        mut terminal_exit: Option<Retained<Exit>>,
        poison: ControlPoison,
    ) {
        self.with_observation_gate(move |wakes| {
            let mut control = self.lock_control(poison);
            if !control.epochs.finish(epoch) {
                // A stale driver must not overwrite the observation
                // projection of a newer live incarnation. Membership
                // terminality is not part of that projection: whoever owns a
                // terminal exit still publishes it exactly once, so declining
                // the epoch can never strand `wait_terminal`.
                drop(control);
                if let Some(exit) = terminal_exit.take() {
                    self.member.terminalize_locked(
                        exit.get().clone(),
                        StartupDisposition::Unchanged,
                        wakes,
                    );
                    wakes.surrender([exit]);
                    wakes.pulse(&self.member.record);
                    wakes.pulse(&self.observation.record);
                    self.close_observation_locked(wakes);
                }
                // `StopReason::StartupFailed` recursively owns the failed
                // child's user error. A stale verdict is unused, but the
                // framework still owns this copy: retaining it before the
                // deferred drop sends a possibly-blocking or panicking user
                // destructor to `dispose_critical` instead of running it
                // inline on the committing thread once the gate is released.
                wakes.defer(move || drop(reason));
                return;
            }
            if control
                .shutdown
                .is_some_and(|request| request.epoch <= epoch)
            {
                control.shutdown = None;
            }
            if control.force.is_some_and(|request| request.epoch <= epoch) {
                control.force = None;
            }
            let terminal = terminal_exit.is_some();
            let membership_terminal =
                matches!(self.member.record().stage, MemberStage::Terminal(_));
            self.publish_stopped_locked(
                wakes,
                reason.get().clone(),
                terminal_exit.as_ref().map(|exit| exit.get().clone()),
                Some(control),
            );
            if terminal || membership_terminal {
                // A parent-driver fallback may have terminalized this nested
                // membership while its live scope epilogue was still
                // pending. The epilogue owns the final Stopped projection and
                // closes observation only after publishing it.
                self.close_observation_locked(wakes);
            }
            wakes.defer(move || drop(reason));
            if let Some(exit) = terminal_exit.take() {
                wakes.surrender([exit]);
            }
        });
    }

    pub(crate) fn finish_live_root_incarnation(&self, reason: StopReason, exit: Exit) {
        // These wrappers precede the control lookup: a poisoned framework
        // mutex must not retire either user-bearing input on this thread.
        let reason = Guarded::new(reason);
        let exit = Retained::new(exit);
        let epoch = {
            let control = self.control.lock().expect("scope control mutex poisoned");
            control.epochs.live_epoch()
        };
        if let Some(epoch) = epoch {
            self.finish_incarnation_with_terminal(epoch, reason, Some(exit), ControlPoison::Reject);
        } else {
            self.with_observation_gate(move |wakes| {
                self.publish_stopped_locked(
                    wakes,
                    reason.get().clone(),
                    Some(exit.get().clone()),
                    None,
                );
                self.close_observation_locked(wakes);
                wakes.defer(move || drop(reason));
                wakes.surrender([exit]);
            });
        }
    }

    pub(crate) fn request_shutdown(&self) -> Epoch {
        self.with_observation_gate(|txn| {
            let control = self.lock_control(ControlPoison::Reject);
            self.request_shutdown_locked(control, txn, ControlPoison::Reject)
        })
    }

    /// [`Self::request_shutdown`] for destructors: tolerates a poisoned
    /// control mutex so a drop-path request cannot panic — and abort — on a
    /// thread that is already unwinding. The tolerance extends to the
    /// parent's control mutex, which a pending-incarnation request also
    /// writes.
    pub(crate) fn request_shutdown_ignoring_poison(&self) -> Epoch {
        self.with_observation_gate(|txn| {
            let control = self.lock_control(ControlPoison::Ignore);
            self.request_shutdown_locked(control, txn, ControlPoison::Ignore)
        })
    }

    pub(super) fn request_shutdown_locked(
        &self,
        mut control: MutexGuard<'_, ScopeControl>,
        txn: &mut ObservationTxn<'_>,
        poison: ControlPoison,
    ) -> Epoch {
        let RequestTarget {
            epoch: target,
            pending_incarnation,
        } = control.epochs.request_target();
        let published = control
            .shutdown
            .is_none_or(|request| request.epoch < target);
        if published {
            control.shutdown = Some(ScopeRequest {
                epoch: target,
                consumed: false,
            });
        }
        drop(control);
        if published {
            txn.pulse(&self.member.record);
            if pending_incarnation && let Some(parent) = self.parent() {
                txn.retain_shared(&parent);
                parent.publish_control_event_locked(
                    ScopeControlEvent::WindowStop {
                        membership: self.member.membership(),
                        target,
                    },
                    txn,
                    poison,
                );
            }
        }
        target
    }

    pub(super) fn publish_control_event_locked(
        &self,
        event: ScopeControlEvent,
        txn: &mut ObservationTxn<'_>,
        poison: ControlPoison,
    ) {
        self.lock_control(poison).events.push_back(event);
        txn.pulse(&self.member.record);
    }

    /// Poisons the control mutex, as a panic inside a control critical
    /// section would.
    #[cfg(test)]
    pub(crate) fn poison_control(&self) {
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _control = self.control.lock().expect("control starts healthy");
            panic!("inject control poison");
        }));
        assert!(poisoned.is_err() && self.control.is_poisoned());
    }

    pub(crate) fn take_control_events(&self) -> Vec<ScopeControlEvent> {
        if self
            .control
            .lock()
            .expect("scope control mutex poisoned")
            .events
            .is_empty()
        {
            return Vec::new();
        }
        self.with_observation_gate(|_txn| {
            self.control
                .lock()
                .expect("scope control mutex poisoned")
                .events
                .drain(..)
                .collect()
        })
    }

    /// The epoch of a shutdown request accepted with no live incarnation to
    /// consume it, if one is still pending.
    ///
    /// A pending request always targets the epoch the next `begin` would
    /// mint, which is what makes this a level the parent can sample at every
    /// construction site.
    pub(crate) fn pending_incarnation_shutdown(&self) -> Option<Epoch> {
        let control = self.control.lock().expect("scope control mutex poisoned");
        control
            .shutdown
            .filter(|request| !request.consumed && control.epochs.request_is_pending(request.epoch))
            .map(|request| request.epoch)
    }

    /// Spends a pending-incarnation shutdown request without constructing
    /// the incarnation it targets (SPEC §11).
    ///
    /// The target epoch is marked finished without having run, so every
    /// `shutdown_and_wait` on it settles, and the next incarnation mints the
    /// epoch after it with a clear latch. Returns whether `target` was the
    /// pending request and is now vacated.
    pub(crate) fn vacate_pending_shutdown(&self, target: Epoch) -> bool {
        self.with_observation_gate(|txn| {
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            let pending = control
                .shutdown
                .is_some_and(|request| request.epoch == target && !request.consumed);
            let vacated = pending && control.epochs.vacate(target);
            if vacated {
                control.shutdown = None;
            }
            drop(control);
            if vacated {
                txn.pulse(&self.member.record);
            }
            vacated
        })
    }

    pub(crate) fn has_stop_request(&self, epoch: Epoch) -> bool {
        let control = self.control.lock().expect("scope control mutex poisoned");
        control
            .shutdown
            .is_some_and(|request| request.epoch == epoch)
            || control.force.is_some_and(|request| request.epoch == epoch)
    }

    pub(crate) fn take_shutdown_request(&self, epoch: Epoch) -> bool {
        self.take_request(ScopeRequestSlot::Shutdown, epoch)
    }

    pub(crate) fn force_shutdown(&self, epoch: Epoch) {
        self.with_observation_gate(|txn| {
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            if control.epochs.is_current(epoch) {
                control.force = Some(ScopeRequest {
                    epoch,
                    consumed: false,
                });
            }
            drop(control);
            txn.pulse(&self.member.record);
        });
    }

    pub(crate) fn take_force_request(&self, epoch: Epoch) -> bool {
        self.take_request(ScopeRequestSlot::Force, epoch)
    }

    fn take_request(&self, slot: ScopeRequestSlot, epoch: Epoch) -> bool {
        let pending = slot
            .get(&self.control.lock().expect("scope control mutex poisoned"))
            .is_some_and(|request| request.epoch == epoch && !request.consumed);
        if !pending {
            return false;
        }
        self.with_observation_gate(|_txn| {
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            match slot.get_mut(&mut control) {
                Some(request) if request.epoch == epoch && !request.consumed => {
                    request.consumed = true;
                    true
                }
                _ => false,
            }
        })
    }

    fn incarnation_complete(&self, epoch: Epoch) -> bool {
        let control = self.control.lock().expect("scope control mutex poisoned");
        control.epochs.finished(epoch)
    }

    pub(super) fn membership_terminal(&self) -> bool {
        matches!(self.member.record().stage, MemberStage::Terminal(_))
    }

    fn joined(&self) -> bool {
        matches!(
            self.record().state,
            ScopeState::Stopped { .. } | ScopeState::Unstarted
        )
    }

    /// Whether a shutdown wait has crossed the finality fence for its target.
    ///
    /// Membership terminality alone is insufficient: parent-driver
    /// destruction publishes it before the aborted nested driver runs the
    /// scope epilogue that finishes the incarnation and publishes `Stopped`.
    /// `None` is used only by the entry check, before a target epoch exists.
    ///
    /// This predicate is strictly weaker than membership terminality, so
    /// shutdown liveness rests on two structural invariants. First, every
    /// live epoch has exactly one owner — the pre-driver epoch guard before a
    /// scope runtime exists, the scope runtime itself afterwards — and both
    /// finish it from `Drop`, so an unsettled target always has a pending
    /// finisher. Second, an idle epoch plane implies a settled projection:
    /// `begin_incarnation_into` is the only mint and it publishes `Starting`
    /// under the control guard,
    /// while [`Self::finish_incarnation`] always publishes `Stopped` under
    /// that same guard, so `ScopeEpochs::Idle` can only pair with
    /// `Unstarted` (never begun) or `Stopped`. Together they mean the
    /// terminal-membership arm can never be the *only* reachable settlement
    /// for a scope that still owns work.
    pub(crate) fn settled(&self, epoch: Option<Epoch>) -> bool {
        epoch.is_some_and(|epoch| self.incarnation_complete(epoch))
            || (self.membership_terminal() && self.joined())
    }

    pub(crate) fn set_dynamic_route(&self, route: Option<Arc<dyn DynamicRoute>>) {
        self.with_observation_gate(|txn| {
            self.set_dynamic_route_locked(route, txn);
        });
    }

    pub(crate) fn set_dynamic_route_locked(
        &self,
        route: Option<Arc<dyn DynamicRoute>>,
        txn: &mut ObservationTxn<'_>,
    ) {
        let previous = std::mem::replace(
            &mut *self
                .dynamic_route
                .lock()
                .expect("scope dynamic-route mutex poisoned"),
            route,
        );
        txn.defer(move || drop(previous));
        txn.pulse(&self.member.record);
    }

    pub(crate) fn dynamic_route_in(
        &self,
        _txn: &ObservationTxn<'_>,
    ) -> Option<Arc<dyn DynamicRoute>> {
        self.dynamic_route
            .lock()
            .expect("scope dynamic-route mutex poisoned")
            .clone()
    }

    #[cfg(test)]
    pub(crate) fn dynamic_route(&self) -> Option<Arc<dyn DynamicRoute>> {
        self.with_observation_gate(|txn| self.dynamic_route_in(txn))
    }
}
