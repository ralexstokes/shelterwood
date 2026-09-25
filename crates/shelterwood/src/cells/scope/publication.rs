//! Observation-visible state publication: scope state and startup records,
//! child stage transitions, and the monotonic `Stopped` projection.

use std::sync::{Arc, MutexGuard};

use crate::runtime;
use shelterwood_core::{
    Exit, Incarnation, TotalRestarts,
    engine::{MembershipStatus, ScopeState},
    exit::{StartupError, StopReason, stop_reason_precedence},
};

use crate::cells::observe::LifecycleEventKind;

use crate::cells::{
    Guarded, MemberCell, MemberStage, MemberTransition, ObservationTxn, Retained,
    StartupDisposition,
};

use super::{ScopeCell, control::ScopeControl};

impl ScopeCell {
    /// Publishes a fabricated scope state.
    ///
    /// `Stopped` goes through the live terminal publisher rather than the
    /// nonterminal writer, so a fixture that publishes it twice — or over a
    /// stronger recorded reason — is suppressed by SPEC §11 stop precedence
    /// instead of overwriting.
    #[cfg(test)]
    pub(crate) fn set_state(&self, state: ScopeState) {
        self.with_observation_gate(|txn| match state {
            ScopeState::Stopped { reason } => {
                self.publish_stopped_locked(txn, reason, None, None);
            }
            state => self.set_state_locked(state, &[], txn),
        });
    }

    pub(crate) fn set_state_and_startup(
        &self,
        state: ScopeState,
        startup: Result<(), StartupError>,
    ) {
        assert!(
            !matches!(state, ScopeState::Stopped { .. }),
            "terminal scope state is published through publish_stopped_locked"
        );
        let startup = Guarded::new(startup);
        self.with_observation_gate(|txn| {
            self.set_startup_locked(startup, txn);
            self.set_state_locked(state, &[], txn);
        });
    }

    /// Publishes drain entry together with terminal-cleanup intent selected by
    /// the same driver step.
    ///
    /// A zero-budget shutdown waiter wakes from the state publication and
    /// immediately samples descendants. Installing these markers under the
    /// shared observation gate keeps that sample from splitting drain entry
    /// between the scope transition and an inactive child's cleanup.
    pub(crate) fn publish_drain(
        &self,
        state: ScopeState,
        startup: Option<Result<(), StartupError>>,
        terminal_disposals: &[Arc<MemberCell>],
    ) {
        assert!(matches!(state, ScopeState::Draining));
        // Keep the failed startup's user error guarded across the foreign-gate
        // diagnostic below: if it fires, the carrier's drop glue transfers
        // final destruction to critical disposal rather than running it on
        // this driver thread.
        let mut startup = startup.map(Guarded::new);
        let mut state = Some(state);
        let published = self.with_observation_gate(|txn| {
            if !terminal_disposals.iter().all(|member| {
                self.current_observation_gate()
                    .shares_gate(&member.current_observation_gate())
            }) {
                return false;
            }
            if let Some(startup) = startup.take() {
                self.set_startup_locked(startup, txn);
            }
            self.set_state_locked(
                state
                    .take()
                    .expect("validated drain state is published once"),
                terminal_disposals,
                txn,
            );
            true
        });
        assert!(
            published,
            "a drain entry may mark only a resident member on its observation gate"
        );
    }

    fn set_state_locked(
        &self,
        state: ScopeState,
        terminal_disposals: &[Arc<MemberCell>],
        txn: &mut ObservationTxn<'_>,
    ) {
        debug_assert!(
            !matches!(state, ScopeState::Stopped { .. }),
            "terminal scope state is published through publish_stopped_locked"
        );
        // The marker slice is part of the state-writer signature so a caller
        // cannot reorder the markers after the record write. Every marker is
        // stored before the `Draining` record write whose release edge makes
        // it visible to zero-budget shutdown samplers on other workers.
        for member in terminal_disposals {
            member.set_terminal_disposal_pending(true);
        }
        if matches!(state, ScopeState::Draining | ScopeState::StartupFailed)
            && let Some(route) = self.dynamic_route_in(txn)
        {
            route.close_admission(txn);
        }
        self.observation.record.modify_silently(|record| {
            record.update(txn, |record| record.state = state.clone());
        });
        txn.pulse(&self.observation.record);
        txn.pulse(&self.member.record);
        self.emit_locked(txn, LifecycleEventKind::ScopeState { state });
    }

    pub(crate) fn set_child_removing_locked(
        &self,
        member: &MemberCell,
        txn: &mut ObservationTxn<'_>,
    ) {
        if member.record().membership_status == MembershipStatus::Removing {
            return;
        }
        member.update_locked(txn, |record| {
            record.membership_status = MembershipStatus::Removing;
        });
        self.publish_snapshot_chain_locked(txn);
    }

    /// Applies `transition` and publishes `event`, or refuses both.
    ///
    /// Returns whether the reducer accepted the transition. A refusal is a
    /// no-op *for this scope*; the caller is responsible for whatever it
    /// committed beforehand.
    #[must_use = "an illegal member transition is rejected, not applied"]
    pub(crate) fn transition_child_stage(
        &self,
        member: &MemberCell,
        transition: MemberTransition,
        event: Option<LifecycleEventKind>,
    ) -> bool {
        // Routed through `transition_locked` rather than a record-only update
        // so a restart schedule's displaced exit leaves the gate on this path
        // too.
        self.with_observation_gate(|wakes| {
            if !member.transition_locked(wakes, transition) {
                if let Some(event) = event {
                    wakes.defer(move || runtime::dispose_detached(event));
                }
                return false;
            }
            if let Some(event) = event {
                self.emit_locked(wakes, event);
            } else {
                self.publish_snapshot_chain_locked(wakes);
            }
            true
        })
    }

    /// Commits a nested member's construction boundary against shutdown.
    /// Both cells share the resident-tree gate with `request_shutdown`, so a
    /// request either prevents this start (or is vacated by restart policy),
    /// or observes an already-starting member and addresses its live body.
    /// Even vacated-request wakes run only after `Starting` is installed.
    pub(crate) fn start_scope_child(
        &self,
        child: &ScopeCell,
        incarnation: Incarnation,
        restart_stopped: bool,
    ) -> bool {
        let started = self.with_observation_gate(|txn| {
            let mut control = child.control.lock().expect("scope control mutex poisoned");
            if let Some(request) = control.shutdown
                && !request.consumed
                && control.epochs.request_is_pending(request.epoch)
            {
                if !restart_stopped {
                    return None;
                }
                control.epochs.vacate(request.epoch);
                control.shutdown = None;
            }
            drop(control);
            let member = &child.member;
            if !member.transition_locked(txn, MemberTransition::Starting { incarnation }) {
                return Some(false);
            }
            self.emit_locked(
                txn,
                LifecycleEventKind::Started {
                    id: member.id().clone(),
                    membership: member.membership(),
                    incarnation,
                },
            );
            Some(true)
        });
        // As with ordinary child starts, a rejected publication cannot launch
        // an unannounced body. Report it only after the transaction unlocks.
        debug_assert!(
            started != Some(false),
            "a scope spawn starts an admitted or restarting member"
        );
        started == Some(true)
    }

    /// Publishes one restart schedule, or refuses the whole publication.
    ///
    /// Returns whether the reducer accepted the transition. The restart
    /// bookkeeping its caller already charged is not rolled back here.
    #[must_use = "an illegal member transition is rejected, not applied"]
    pub(crate) fn publish_child_restart(
        &self,
        member: &MemberCell,
        total_restarts: TotalRestarts,
        exit_guard: Retained<Exit>,
        transition: MemberTransition,
        exited: LifecycleEventKind,
        scheduled: LifecycleEventKind,
    ) -> bool {
        self.with_observation_gate(|wakes| {
            if !member.transition_locked(wakes, transition) {
                wakes.defer(move || drop(exit_guard));
                wakes.defer(move || runtime::dispose_detached((exited, scheduled)));
                return false;
            }
            // The member record now owns the exit used by both public edges.
            // Surrender while its transaction is still open; the prioritized
            // effect runs before any queued owner can enter disposal.
            wakes.surrender([exit_guard]);
            self.observation.record.modify_silently(|scope| {
                scope.update(wakes, |scope| scope.total_restarts = total_restarts);
            });
            wakes.pulse(&self.observation.record);
            self.emit_locked(wakes, exited);
            self.emit_locked(wakes, scheduled);
            true
        })
    }

    pub(crate) fn terminalize_child(
        &self,
        member: &MemberCell,
        exit: impl Into<Retained<Exit>>,
        exited_incarnation: Option<Incarnation>,
        startup: StartupDisposition,
    ) -> bool {
        // Keep a retained owner across the residency assertion and every
        // fallible cell lookup. If an invariant fails, the raw argument can
        // unwind under the gate only as refcount traffic.
        let exit = exit.into();
        let terminalized = self.with_observation_gate(move |wakes| {
            let record = member.record();
            if matches!(record.stage, MemberStage::Terminal(_)) {
                // The outer guard defines losing supervised terminalization:
                // no record or lifecycle edge is published. Keep the incoming
                // failed exit behind the same isolated-disposal boundary as a
                // losing direct terminalizer, and do not destroy it under the
                // observation gate.
                wakes.defer(move || drop(exit));
                return Some(false);
            }
            // Nested publication and closure are reached only through this
            // residency lookup, so a supervised child must still be resident
            // here when it is terminalized. That holds because residency is
            // installed before the child can be spawned (`set_admitted_children`
            // for a planned incarnation, `admit_child` before dynamic
            // `spawn_child`) and is only withdrawn by pruning, which always
            // follows terminality. A child that has already left residency
            // owns its nested scope through `SlotCell::terminalize_never_started`
            // instead.
            let resident = self
                .current_children()
                .iter()
                .find(|resident| resident.projection().member.membership() == member.membership())
                .map(|resident| resident.projection().clone());
            let Some(resident) = resident else {
                // Retire the incoming failed exit before the post-unlock
                // assertion resumes, preserving the retention-before-verdict
                // ordering in every profile.
                wakes.defer(move || drop(exit));
                return None;
            };
            let nested = resident.scope;
            let terminal_exit = member.terminalize_locked(exit.get().clone(), startup, wakes);
            // The terminal member record now owns the equivalent retained
            // copy, so surrender this transient guard as refcount traffic.
            wakes.surrender([exit]);
            self.evict_child_identity(member);
            if record.last_incarnation.is_none()
                && let Some(scope) = &nested
            {
                scope.publish_stopped_locked(wakes, StopReason::NeverStarted, None, None);
            }
            if let Some(incarnation) = exited_incarnation {
                self.emit_locked(
                    wakes,
                    LifecycleEventKind::Exited {
                        id: member.id().clone(),
                        membership: member.membership(),
                        incarnation,
                        exit: terminal_exit,
                    },
                );
            } else {
                // Terminals without a current incarnation have no Exited
                // event to carry snapshot publication. Publish the final
                // parent projection explicitly before nested observation
                // closes.
                self.publish_snapshot_chain_locked(wakes);
            }
            if let Some(scope) = nested
                && matches!(scope.record().state, ScopeState::Stopped { .. })
            {
                // A parent fallback can terminalize a live nested membership
                // before cancellation drops the nested driver. Keep that
                // scope's stream open until its own epilogue publishes the
                // final Stopped record; otherwise waiters would receive a
                // terminal stream carrying Starting/Running as its payload.
                scope.close_observation_locked(wakes);
            }
            Some(true)
        });
        terminalized.expect("a supervised terminal child must remain in parent residency")
    }

    pub(crate) fn set_startup(&self, startup: Result<(), StartupError>) {
        let startup = Guarded::new(startup);
        self.with_observation_gate(|txn| self.set_startup_locked(startup, txn));
    }

    /// Installs the startup result unless one is already recorded.
    fn set_startup_locked(
        &self,
        startup: Guarded<Result<(), StartupError>>,
        txn: &mut ObservationTxn<'_>,
    ) {
        let mut rejected = None;
        self.observation.record.modify_silently(|record| {
            if record.startup.is_some() {
                rejected = Some(startup);
                return;
            }
            // The record keeps the raw result, a co-owner of every exit the
            // incoming guards cover, so those guards surrender.
            let startup = startup.release(txn);
            record.update(txn, |record| record.startup = Some(startup));
        });
        if let Some(rejected) = rejected {
            txn.defer(move || drop(rejected));
        } else {
            txn.pulse(&self.member.record);
            txn.pulse(&self.observation.record);
        }
    }

    pub(crate) async fn wait_started(&self) -> Result<(), StartupError> {
        let mut watcher = self.observation.record.watcher();
        loop {
            if let Some(result) = watcher.borrow_and_update_cloned().startup.clone() {
                return result;
            }
            watcher.changed().await;
        }
    }

    pub(crate) async fn wait_stopped(&self) -> StopReason {
        self.member.wait_terminal().await;
        // Parent-driver destruction can terminalize a nested membership
        // synchronously before the aborted nested driver runs its own scope
        // epilogue. Membership terminality is therefore the finality fence,
        // not proof that the scope record has already reached `Stopped`.
        let mut watcher = self.observation.record.watcher();
        loop {
            match &watcher.borrow_and_update_cloned().state {
                ScopeState::Stopped { reason } => return reason.clone(),
                ScopeState::Unstarted => return StopReason::NeverStarted,
                ScopeState::Starting
                | ScopeState::Running
                | ScopeState::StartupFailed
                | ScopeState::Draining => watcher.changed().await,
            }
        }
    }

    /// Commits a stopped-scope projection monotonically and applies its
    /// optional member terminal edge under the resident-tree observation gate.
    ///
    /// Several owners can reach a stop verdict for one incarnation — a
    /// driver's drain epilogue, a join monitor's fallback, a never-started
    /// terminalization — so competing reasons resolve through
    /// `StopPrecedence`, never through arrival order. A publication commits
    /// only when it strictly outranks the recorded reason; equal or weaker
    /// verdicts are idempotent repeats that mutate nothing. An upgrade
    /// republishes the record, the snapshot and a corrected `ScopeState` edge,
    /// so the stream never ends on an event that disagrees with the final
    /// record (SPEC B.4's non-final-`Stopped` rule admits exactly this step).
    ///
    /// Member terminalization stores its record, then prepares mailbox
    /// teardown (SPEC §3.2); its deferred discharge is therefore queued
    /// before either member or scope pulses.
    /// `epoch_owner` carries a live incarnation's control ownership through
    /// both retained record mutations and is released before snapshot and
    /// lifecycle publication, preserving the stop transition's ownership
    /// boundary. A suppressed repeat still terminalizes the member and still
    /// releases the epoch: only the scope-record mutation, snapshot pulse and
    /// lifecycle edge are skipped. Because the ancestor snapshot chain is
    /// republished by `emit_locked`, a suppressed repeat publishes no ancestor
    /// snapshot either — a caller whose member terminal edge must reach an
    /// ancestor projection has to publish that itself. Observation closure
    /// remains a caller decision because a nested scope's final event must
    /// precede its parent's terminal event and only then close the nested
    /// streams; a subscriber attaching after the final event and before that
    /// closure therefore resolves by closure alone, as it already does on the
    /// stale-epoch path of `finish_incarnation_with_terminal`.
    ///
    /// Reachability note: in production the upgrade arm's only visitor is
    /// `ShutdownRequested` outranking an already-recorded weaker reason
    /// under SPEC §11 stop precedence. The synthetic lattice tests in
    /// `tests.rs` (and `begin_drain`'s twin in `shelterwood-core`) drive
    /// the full precedence table directly.
    pub(super) fn publish_stopped_locked(
        &self,
        wakes: &mut ObservationTxn<'_>,
        reason: StopReason,
        terminal_exit: Option<Exit>,
        epoch_owner: Option<MutexGuard<'_, ScopeControl>>,
    ) {
        let incoming = stop_reason_precedence(&reason);
        let state = Guarded::new(ScopeState::Stopped { reason });
        let mut published = false;
        self.observation.record.modify_silently(|record| {
            if let ScopeState::Stopped { reason: recorded } = &record.state
                && incoming <= stop_reason_precedence(recorded)
            {
                return;
            }
            record.update(wakes, |record| {
                if record.startup.is_none() {
                    record.startup = Some(Err(StartupError::ShutdownRequested));
                }
                record.state = state.get().clone();
            });
            published = true;
        });
        if let Some(exit) = terminal_exit {
            self.member
                .terminalize_locked(exit, StartupDisposition::Unchanged, wakes);
        }
        drop(epoch_owner);
        wakes.pulse(&self.member.record);
        // `wait_started` must not observe terminal startup until the member
        // and incarnation-control planes are mutually consistent.
        if published {
            // The record and lifecycle event now retain the raw projection,
            // so the transient guards surrender rather than scheduling
            // duplicate disposal jobs.
            let state = state.release(wakes);
            wakes.pulse(&self.observation.record);
            self.emit_locked(wakes, LifecycleEventKind::ScopeState { state });
        } else {
            wakes.defer(move || drop(state));
        }
    }

    pub(crate) fn terminalize_never_started(&self) {
        self.with_observation_gate(|txn| self.terminalize_never_started_locked(txn));
    }

    /// Publishes a nested scope body that was spawned but never began its own
    /// incarnation. The parent driver remains responsible for the shared
    /// membership's terminal exit and, unless it already published that exit,
    /// closes observation after the terminal parent projection.
    ///
    /// `Unstarted` is precisely SPEC B.6's "no scope incarnation ever began"
    /// state, and `Stopped { NeverStarted }` is its terminal
    /// twin; publishing that pair is what makes the record agree with
    /// `wait_stopped`'s own `Unstarted` answer. A body dropped before its
    /// *restart* incarnation began is a different case: that scope already
    /// published a real prior-incarnation reason, which is both its final
    /// verdict and terminal, so the fallback must leave it alone. Publishing
    /// `NeverStarted` over it would falsely claim that no scope incarnation
    /// ever began and — because the parent path can terminalize and close
    /// first — would only land in one of two arrival orders. The fallback
    /// therefore supplies a missing terminal projection and never replaces a
    /// published one; closure is the only effect it owns unconditionally.
    /// `total_restarts` and `startup` need no reset under this gate: an
    /// `Unstarted` scope never charged a restart or recorded a startup
    /// result.
    pub(crate) fn close_never_started_body(&self) {
        self.with_observation_gate(|txn| {
            if self.observation.lifecycle.is_closed() {
                return;
            }
            if matches!(self.record().state, ScopeState::Unstarted) {
                self.publish_stopped_locked(txn, StopReason::NeverStarted, None, None);
            }
            // The same guard `terminalize_child` applies to its trailing
            // close: a stream whose payload is still `Starting`/`Running`
            // must not end on that projection.
            if self.membership_terminal()
                && matches!(self.record().state, ScopeState::Stopped { .. })
            {
                self.close_observation_locked(txn);
            }
        });
    }

    pub(crate) fn terminalize_never_started_locked(&self, txn: &mut ObservationTxn<'_>) {
        if self.observation.lifecycle.is_closed() {
            return;
        }
        self.member
            .terminalize_locked(Exit::never_started(), StartupDisposition::Unchanged, txn);
        self.publish_stopped_locked(txn, StopReason::NeverStarted, None, None);
        self.close_observation_locked(txn);
    }
}
