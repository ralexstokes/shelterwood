//! Pure structural supervision reducer.
//!
//! Runtime adapters sample latches and clocks before constructing an
//! [`Event`]. The reducer owns membership, startup, and ordered-stop state and
//! appends commands to a caller-owned effect vector; it never runs user code,
//! reads a clock, samples randomness, or performs I/O.

use std::{
    collections::{BTreeMap, HashMap},
    ops::Bound,
};

use crate::{
    Membership, MembershipStatus, PoisonedCounter, ScopeFlavor, ScopeState, StopReason,
    engine::{ChildCompletionState, ScopeLifecycle},
};

/// A never-reused child registration.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChildKey(u64);

impl ChildKey {
    #[cfg(any(test, feature = "test-util"))]
    pub const fn fixture(value: u64) -> Self {
        Self(value)
    }
}

/// The incarnation-level portion of one authoritative child state.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum IncarnationState {
    /// No incarnation has been started yet.
    Unstarted,
    /// The current incarnation is executing.
    Active,
    /// Cooperative or forced shutdown is in progress.
    Stopping,
    /// The incarnation has completed and no restart/terminal route has won.
    Complete,
    /// A restart deadline is pending.
    RestartPending,
    /// Terminal definition disposal is pending.
    Disposing,
    /// Membership terminality and joined disposal have both completed.
    Joined,
}

/// Membership state and transition reason in one enum.
///
/// Removal is deliberately not a boolean parallel to incarnation state. The
/// synchronous control-plane latch is sampled into [`Event::RemovalLatched`];
/// after that transition, this enum is the reducer's sole authority.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ChildState {
    /// Ordinary resident membership.
    Resident(IncarnationState),
    /// Planned removal won while the child was in the enclosed state.
    Removing(IncarnationState),
}

impl ChildState {
    fn incarnation(self) -> IncarnationState {
        match self {
            Self::Resident(state) | Self::Removing(state) => state,
        }
    }

    fn with_incarnation(self, state: IncarnationState) -> Self {
        match self {
            Self::Resident(_) => Self::Resident(state),
            Self::Removing(_) => Self::Removing(state),
        }
    }

    fn removing(self) -> Self {
        match self {
            Self::Resident(state) | Self::Removing(state) => Self::Removing(state),
        }
    }

    /// Whether the current incarnation has stopped executing.
    pub fn incarnation_complete(self) -> bool {
        matches!(
            self.incarnation(),
            IncarnationState::Unstarted
                | IncarnationState::Complete
                | IncarnationState::RestartPending
                | IncarnationState::Disposing
                | IncarnationState::Joined
        )
    }

    /// Whether terminal disposal has joined and no child work remains.
    pub fn joined(self) -> bool {
        self.incarnation() == IncarnationState::Joined
    }

    pub fn membership_status(self) -> MembershipStatus {
        match self {
            Self::Resident(_) => MembershipStatus::Active,
            Self::Removing(_) => MembershipStatus::Removing,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartupMembership {
    Initial { ready: bool },
    Runtime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ChildRecord {
    membership: Membership,
    state: ChildState,
    startup: StartupMembership,
    spawned_once: bool,
}

impl ChildRecord {
    /// Whether a [`Effect::StartChild`] can still produce a [`Event::Spawned`]
    /// edge, deliberately spelled as this event's own acceptance set.
    ///
    /// Settlement is level-triggered and its driver re-enters [`Event::Settle`]
    /// until a pass emits nothing, so an effect the shell would decline is not
    /// merely wasted — it is re-derived from unchanged state forever. Every
    /// start emission is therefore gated on this predicate, and the reducer
    /// never asks for construction it could not observe the result of.
    fn startable(self) -> bool {
        matches!(
            self.state,
            ChildState::Resident(IncarnationState::Unstarted | IncarnationState::RestartPending)
        )
    }
}

/// One input to the structural reducer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    Spawned {
        child: ChildKey,
    },
    Ready {
        child: ChildKey,
        /// Sample of the synchronous removal latch at step entry.
        removal_latched: bool,
    },
    IncarnationComplete {
        child: ChildKey,
    },
    RestartPending {
        child: ChildKey,
    },
    StopStarted {
        child: ChildKey,
    },
    DisposalStarted {
        child: ChildKey,
    },
    Terminalized {
        child: ChildKey,
    },
    /// Samples a fired removal latch into authoritative state without
    /// replaying the separately queued removal command.
    RemovalSampled {
        child: ChildKey,
    },
    RemovalLatched {
        child: ChildKey,
    },
    Reclaim {
        child: ChildKey,
    },
    FailStartup,
    BeginDrain {
        reason: StopReason,
    },
    Force,
    /// Level-triggered startup, ordered-stop, and finish recomputation.
    Settle,
}

/// A command for the runtime shell or observation projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Effect {
    StartChild { child: ChildKey },
    StopChild { child: ChildKey },
    ForceChild { child: ChildKey },
    FinalizeRemoval { child: ChildKey },
    StartupCompleted { state: ScopeState },
    Finished { reason: StopReason },
}

/// Authoritative structural state for one scope incarnation.
#[derive(Clone, Debug)]
pub struct SupervisorState {
    flavor: ScopeFlavor,
    lifecycle: ScopeLifecycle,
    children: BTreeMap<ChildKey, ChildRecord>,
    child_keys: HashMap<Membership, ChildKey>,
    keys: PoisonedCounter,
    next_ordered_start: Option<ChildKey>,
    ordered_stop_cursor: Option<ChildKey>,
    ordered_stop_waiting: Option<ChildKey>,
    hard_forced: bool,
    finish_emitted: bool,
    #[cfg(any(test, feature = "test-util"))]
    ordered_stop_inspections: usize,
}

impl SupervisorState {
    pub fn new(flavor: ScopeFlavor, lifecycle: ScopeLifecycle) -> Self {
        Self {
            flavor,
            lifecycle,
            children: BTreeMap::new(),
            child_keys: HashMap::new(),
            keys: PoisonedCounter::new(),
            next_ordered_start: None,
            ordered_stop_cursor: None,
            ordered_stop_waiting: None,
            hard_forced: false,
            finish_emitted: false,
            #[cfg(any(test, feature = "test-util"))]
            ordered_stop_inspections: 0,
        }
    }

    pub fn lifecycle(&self) -> &ScopeLifecycle {
        &self.lifecycle
    }

    pub fn flavor(&self) -> ScopeFlavor {
        self.flavor
    }

    pub fn hard_forced(&self) -> bool {
        self.hard_forced
    }

    pub fn key_for(&self, membership: Membership) -> Option<ChildKey> {
        self.child_keys.get(&membership).copied()
    }

    pub fn contains(&self, child: ChildKey) -> bool {
        self.children.contains_key(&child)
    }

    pub fn membership(&self, child: ChildKey) -> Option<Membership> {
        self.children.get(&child).map(|record| record.membership)
    }

    fn child_state(&self, child: ChildKey) -> Option<ChildState> {
        self.children.get(&child).map(|record| record.state)
    }

    pub fn membership_status(&self, child: ChildKey) -> MembershipStatus {
        self.child_state(child)
            .map_or(MembershipStatus::Removing, ChildState::membership_status)
    }

    pub fn is_initial(&self, child: ChildKey) -> bool {
        self.children
            .get(&child)
            .is_some_and(|record| matches!(record.startup, StartupMembership::Initial { .. }))
    }

    pub fn initial_ready(&self, child: ChildKey) -> bool {
        self.children.get(&child).is_some_and(|record| {
            matches!(record.startup, StartupMembership::Initial { ready: true })
        })
    }

    pub fn spawned_once(&self, child: ChildKey) -> bool {
        self.children
            .get(&child)
            .is_some_and(|record| record.spawned_once)
    }

    pub fn is_disposing(&self, child: ChildKey) -> bool {
        self.child_state(child)
            .is_some_and(|state| state.incarnation() == IncarnationState::Disposing)
    }

    pub fn incarnation_complete(&self, child: ChildKey) -> bool {
        self.child_state(child)
            .is_some_and(ChildState::incarnation_complete)
    }

    pub fn joined(&self, child: ChildKey) -> bool {
        self.child_state(child).is_some_and(ChildState::joined)
    }

    pub fn is_incomplete(&self, child: ChildKey) -> bool {
        self.contains(child) && !self.joined(child)
    }

    /// Derived completion query; no counter can drift from child state.
    pub fn all_children_joined(&self) -> bool {
        self.children.values().all(|record| record.state.joined())
    }

    pub fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    pub fn len(&self) -> usize {
        self.children.len()
    }

    pub fn keys(&self) -> impl DoubleEndedIterator<Item = ChildKey> + '_ {
        self.children.keys().copied()
    }

    pub fn keys_after(&self, child: ChildKey) -> impl DoubleEndedIterator<Item = ChildKey> + '_ {
        self.children
            .range((Bound::Excluded(child), Bound::Unbounded))
            .map(|(key, _)| *key)
    }

    fn previous_key(&self, child: ChildKey) -> Option<ChildKey> {
        self.children
            .range(..child)
            .next_back()
            .map(|(key, _)| *key)
    }

    fn transition_incarnation(
        &mut self,
        child: ChildKey,
        expected: &[IncarnationState],
        next: IncarnationState,
    ) -> bool {
        if let Some(record) = self.children.get_mut(&child) {
            if record.state.joined() || !expected.contains(&record.state.incarnation()) {
                return false;
            }
            record.state = record.state.with_incarnation(next);
            return true;
        }
        false
    }

    fn mark_removing(&mut self, child: ChildKey) -> Option<IncarnationState> {
        let record = self.children.get_mut(&child)?;
        record.state = record.state.removing();
        Some(record.state.incarnation())
    }

    fn admit(&mut self, membership: Membership, initial: bool) -> Option<ChildKey> {
        if self.child_keys.contains_key(&membership) {
            return None;
        }
        let raw = self.keys.mint()?;
        let child = ChildKey(raw);
        let replaced = self.children.insert(
            child,
            ChildRecord {
                membership,
                state: ChildState::Resident(IncarnationState::Unstarted),
                startup: if initial {
                    StartupMembership::Initial { ready: false }
                } else {
                    StartupMembership::Runtime
                },
                spawned_once: false,
            },
        );
        debug_assert!(replaced.is_none(), "monotonic child keys are never reused");
        let replaced = self.child_keys.insert(membership, child);
        assert!(
            replaced.is_none(),
            "one live membership maps to exactly one child key"
        );
        if initial && self.flavor == ScopeFlavor::Ordered && self.next_ordered_start.is_none() {
            self.next_ordered_start = Some(child);
        }
        Some(child)
    }

    fn settle_startup(&mut self, effects: &mut Vec<Effect>) {
        if !self.lifecycle.is_starting() {
            return;
        }
        match self.flavor {
            ScopeFlavor::Ordered => {
                while let Some(child) = self.next_ordered_start {
                    let Some(record) = self.children.get(&child) else {
                        let next = self.keys_after(child).next();
                        self.next_ordered_start = next;
                        continue;
                    };
                    if record.state.membership_status() == MembershipStatus::Removing {
                        // The queued removal transition owns stop and commit.
                        // Until that commit shrinks the initial set, this
                        // membership still gates ordered startup but can no
                        // longer schedule construction.
                        return;
                    }
                    if !record.spawned_once {
                        // A never-spawned membership can still be terminalized
                        // in place — incarnation exhaustion is the reachable
                        // case. It gates ordered startup exactly as an
                        // unstarted one does, but asking for construction that
                        // `Event::Spawned` would reject would spin settlement.
                        if record.startable() {
                            effects.push(Effect::StartChild { child });
                        }
                        return;
                    }
                    if matches!(record.startup, StartupMembership::Initial { ready: false }) {
                        return;
                    }
                    let next = self.keys_after(child).next();
                    self.next_ordered_start = next;
                }
            }
            ScopeFlavor::Dynamic => {
                for (&child, record) in &self.children {
                    // `startable` subsumes the membership check: a `Removing`
                    // record is never `Resident`.
                    if matches!(record.startup, StartupMembership::Initial { .. })
                        && !record.spawned_once
                        && record.startable()
                    {
                        effects.push(Effect::StartChild { child });
                    }
                }
            }
        }
        if self
            .children
            .values()
            .all(|record| !matches!(record.startup, StartupMembership::Initial { ready: false }))
            && let Some(state) = self.lifecycle.complete_startup()
        {
            effects.push(Effect::StartupCompleted { state });
        }
    }

    fn fail_startup(&mut self) -> Option<ScopeState> {
        self.lifecycle.fail_startup()
    }

    fn begin_drain(
        &mut self,
        reason: StopReason,
        effects: &mut Vec<Effect>,
    ) -> Option<(bool, ScopeState)> {
        let effect = self.lifecycle.begin_drain(reason)?;
        match self.flavor {
            ScopeFlavor::Ordered => {
                self.ordered_stop_cursor = self.children.keys().next_back().copied();
            }
            ScopeFlavor::Dynamic => {
                for child in self.children.keys().copied() {
                    if !self.children[&child].state.joined() {
                        effects.push(Effect::StopChild { child });
                    }
                }
            }
        }
        Some(effect)
    }

    fn force(&mut self, effects: &mut Vec<Effect>) -> Option<(bool, ScopeState)> {
        self.hard_forced = true;
        let drain = self.begin_drain(StopReason::ShutdownRequested, effects);
        for child in self.children.keys().copied() {
            if !self.children[&child].state.joined() {
                effects.push(Effect::ForceChild { child });
            }
        }
        drain
    }

    fn settle_ordered_stop(&mut self, effects: &mut Vec<Effect>) {
        if self.flavor != ScopeFlavor::Ordered || !self.lifecycle.is_draining() {
            return;
        }
        if let Some(waiting) = self.ordered_stop_waiting {
            if self.is_incomplete(waiting) {
                return;
            }
            self.ordered_stop_waiting = None;
        }
        while let Some(child) = self.ordered_stop_cursor {
            self.ordered_stop_cursor = self.previous_key(child);
            #[cfg(any(test, feature = "test-util"))]
            {
                self.ordered_stop_inspections += 1;
            }
            if !self.is_incomplete(child) {
                continue;
            }
            self.ordered_stop_waiting = Some(child);
            effects.push(Effect::StopChild { child });
            return;
        }
    }

    fn settle_finish(&mut self, effects: &mut Vec<Effect>) {
        if self.finish_emitted {
            return;
        }
        let reason = self.lifecycle.finish_if_ready(
            self.flavor,
            ChildCompletionState {
                has_children: !self.children.is_empty(),
                all_terminal: self.all_children_joined(),
            },
        );
        if let Some(reason) = reason {
            self.finish_emitted = true;
            effects.push(Effect::Finished { reason });
        }
    }

    fn apply(&mut self, event: Event, effects: &mut Vec<Effect>) {
        match event {
            Event::Spawned { child } => {
                if let Some(record) = self.children.get_mut(&child)
                    && matches!(record.state, ChildState::Resident(_))
                    && matches!(
                        record.state.incarnation(),
                        IncarnationState::Unstarted | IncarnationState::RestartPending
                    )
                {
                    record.spawned_once = true;
                    record.state = record.state.with_incarnation(IncarnationState::Active);
                }
            }
            Event::Ready {
                child,
                removal_latched,
            } => {
                if removal_latched {
                    self.mark_removing(child);
                }
                let Some(record) = self.children.get_mut(&child) else {
                    return;
                };
                if record.state.membership_status() == MembershipStatus::Removing
                    || record.state.joined()
                    || !matches!(
                        record.state.incarnation(),
                        IncarnationState::Active | IncarnationState::Stopping
                    )
                {
                    return;
                }
                if let StartupMembership::Initial { ready } = &mut record.startup {
                    *ready = true;
                }
            }
            Event::IncarnationComplete { child } => {
                self.transition_incarnation(
                    child,
                    &[IncarnationState::Active, IncarnationState::Stopping],
                    IncarnationState::Complete,
                );
            }
            Event::RestartPending { child } => {
                if self.transition_incarnation(
                    child,
                    &[IncarnationState::Complete],
                    IncarnationState::RestartPending,
                ) && let Some(record) = self.children.get_mut(&child)
                    // Deliberately the *consumers'* predicate, not
                    // `is_starting`: a startup-failed or draining scope has
                    // not completed startup either, and every reader of this
                    // flag gates on `startup_complete`. Narrowing it here
                    // would let a pre-ready exit publish `NotAborted` where a
                    // scope that already failed startup published `Aborted`.
                    && !self.lifecycle.startup_complete()
                    && let StartupMembership::Initial { ready } = &mut record.startup
                {
                    *ready = false;
                }
            }
            Event::StopStarted { child } => {
                self.transition_incarnation(
                    child,
                    &[IncarnationState::Active],
                    IncarnationState::Stopping,
                );
            }
            Event::DisposalStarted { child } => {
                self.transition_incarnation(
                    child,
                    &[
                        IncarnationState::Unstarted,
                        IncarnationState::Complete,
                        IncarnationState::RestartPending,
                    ],
                    IncarnationState::Disposing,
                );
            }
            Event::Terminalized { child } => {
                if self.transition_incarnation(
                    child,
                    &[IncarnationState::Disposing],
                    IncarnationState::Joined,
                ) && self.membership_status(child) == MembershipStatus::Removing
                {
                    effects.push(Effect::FinalizeRemoval { child });
                }
            }
            Event::RemovalSampled { child } => {
                self.mark_removing(child);
            }
            Event::RemovalLatched { child } => {
                let Some(state) = self.mark_removing(child) else {
                    return;
                };
                if state == IncarnationState::Joined {
                    effects.push(Effect::FinalizeRemoval { child });
                } else {
                    effects.push(Effect::StopChild { child });
                }
            }
            Event::Reclaim { child } => {
                let Some(record) = self.children.get(&child) else {
                    return;
                };
                if !record.state.joined() {
                    return;
                }
                let membership = record.membership;
                self.children.remove(&child);
                let removed = self.child_keys.remove(&membership);
                debug_assert_eq!(removed, Some(child));
            }
            Event::FailStartup => {
                let _ = self.fail_startup();
            }
            Event::BeginDrain { reason } => {
                let _ = self.begin_drain(reason, effects);
            }
            Event::Force => {
                let _ = self.force(effects);
            }
            Event::Settle => {
                self.settle_startup(effects);
                self.settle_ordered_stop(effects);
                self.settle_finish(effects);
            }
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn ordered_stop_inspections(&self) -> usize {
        self.ordered_stop_inspections
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn next_ordered_start(&self) -> Option<ChildKey> {
        self.next_ordered_start
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn ordered_stop_waiting(&self) -> Option<ChildKey> {
        self.ordered_stop_waiting
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn set_next_ordered_start_for_test(&mut self, next: Option<ChildKey>) {
        self.next_ordered_start = next;
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn set_hard_forced_for_test(&mut self, forced: bool) {
        self.hard_forced = forced;
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn exhaust_child_keys_for_test(&mut self) {
        self.keys = PoisonedCounter::near_exhaustion();
        assert!(self.keys.mint().is_some(), "the final child key is usable");
        assert!(
            self.keys.mint().is_none(),
            "the child-key domain reaches its permanent poison state"
        );
    }

    #[cfg(test)]
    fn check_invariants(&self) {
        assert_eq!(self.children.len(), self.child_keys.len());
        for (&key, child) in &self.children {
            assert_eq!(self.child_keys.get(&child.membership), Some(&key));
            // A start effect is only ever derived from `startable`, so a
            // spawnable record must be one `Event::Spawned` away from
            // executing. This is the emission/acceptance agreement that keeps
            // level-triggered settlement terminating.
            assert!(!child.startable() || !child.state.joined());
        }
        // The ordered cursors are flavor-owned state; a dynamic scope that
        // grew one would silently acquire a second stop sequencer.
        if self.flavor != ScopeFlavor::Ordered {
            assert!(self.next_ordered_start.is_none());
            assert!(self.ordered_stop_cursor.is_none());
            assert!(self.ordered_stop_waiting.is_none());
        }
        if let Some(waiting) = self.ordered_stop_waiting {
            assert!(self.lifecycle.is_draining());
            assert!(self.contains(waiting) || self.ordered_stop_cursor < Some(waiting));
        }
    }
}

/// Applies one total transition to [`SupervisorState`].
pub fn step(state: &mut SupervisorState, event: Event, effects: &mut Vec<Effect>) {
    state.apply(event, effects);
}

/// Admits one membership and returns its never-reused registration key.
///
/// Admission policy lives in the runtime facade. This structural operation
/// rejects only a duplicate live membership or an exhausted key domain.
pub fn admit(
    state: &mut SupervisorState,
    membership: Membership,
    initial: bool,
) -> Option<ChildKey> {
    state.admit(membership, initial)
}

/// Records the first startup failure and returns the state its owner must publish.
pub fn fail_startup(state: &mut SupervisorState) -> Option<ScopeState> {
    state.fail_startup()
}

/// Begins a drain and returns the transition its owner must publish.
///
/// The tuple is `(startup_pending, state)`: `startup_pending` reports whether
/// startup was still in flight when the drain opened, so the owner knows a
/// startup result accompanies this publication. `None` means no transition —
/// a drain was already in progress and only its reason may have been upgraded.
pub fn begin_drain(
    state: &mut SupervisorState,
    reason: StopReason,
    effects: &mut Vec<Effect>,
) -> Option<(bool, ScopeState)> {
    state.begin_drain(reason, effects)
}

/// Hard-forces all incomplete children and returns a newly entered drain.
///
/// The tuple is `(startup_pending, state)`, exactly as `begin_drain` returns
/// it; `None` means the force landed on an already-draining scope and there is
/// no new transition to publish.
pub fn force(state: &mut SupervisorState, effects: &mut Vec<Effect>) -> Option<(bool, ScopeState)> {
    state.force(effects)
}

#[cfg(test)]
mod tests {
    use crate::{ChildId, ScopeIdentity};

    use super::*;

    mod exploration;

    fn memberships(count: usize) -> Vec<Membership> {
        let mut identity = ScopeIdentity::new();
        (0..count)
            .map(|index| {
                identity
                    .mint_membership(&ChildId::from(format!("child-{index}")))
                    .expect("membership mints")
                    .into_pair()
                    .0
            })
            .collect()
    }

    fn admit(state: &mut SupervisorState, membership: Membership, initial: bool) -> ChildKey {
        super::admit(state, membership, initial).expect("fixture admission mints one key")
    }

    #[test]
    fn transition_table_keeps_removal_in_the_authoritative_state() {
        struct Case {
            before: IncarnationState,
            event: EventKind,
            after: ChildState,
            effect: EffectKind,
        }
        #[derive(Clone, Copy)]
        enum EventKind {
            Remove,
            Terminal,
        }
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        enum EffectKind {
            Stop,
            Finalize,
            None,
        }

        let cases = [
            Case {
                before: IncarnationState::Active,
                event: EventKind::Remove,
                after: ChildState::Removing(IncarnationState::Active),
                effect: EffectKind::Stop,
            },
            Case {
                before: IncarnationState::Joined,
                event: EventKind::Remove,
                after: ChildState::Removing(IncarnationState::Joined),
                effect: EffectKind::Finalize,
            },
            Case {
                before: IncarnationState::Disposing,
                event: EventKind::Terminal,
                after: ChildState::Resident(IncarnationState::Joined),
                effect: EffectKind::None,
            },
        ];

        for case in cases {
            let membership = memberships(1)[0];
            let mut state = SupervisorState::new(ScopeFlavor::Dynamic, ScopeLifecycle::starting());
            let child = admit(&mut state, membership, true);
            state.children.get_mut(&child).expect("child").state =
                ChildState::Resident(case.before);
            let mut effects = Vec::new();
            step(
                &mut state,
                match case.event {
                    EventKind::Remove => Event::RemovalLatched { child },
                    EventKind::Terminal => Event::Terminalized { child },
                },
                &mut effects,
            );
            let effect = match effects.as_slice() {
                [Effect::StopChild { .. }] => EffectKind::Stop,
                [Effect::FinalizeRemoval { .. }] => EffectKind::Finalize,
                [] => EffectKind::None,
                other => panic!("unexpected effects: {other:?}"),
            };
            assert_eq!(state.child_state(child), Some(case.after));
            assert_eq!(effect, case.effect);
            state.check_invariants();
        }
    }

    #[test]
    fn derived_completion_property_matches_the_child_states() {
        let members = memberships(3);
        for mask in 0_u8..8 {
            let mut state = SupervisorState::new(ScopeFlavor::Dynamic, ScopeLifecycle::starting());
            let keys: Vec<_> = members
                .iter()
                .map(|membership| admit(&mut state, *membership, true))
                .collect();
            for (index, child) in keys.iter().enumerate() {
                if mask & (1 << index) != 0 {
                    step(
                        &mut state,
                        Event::DisposalStarted { child: *child },
                        &mut Vec::new(),
                    );
                    step(
                        &mut state,
                        Event::Terminalized { child: *child },
                        &mut Vec::new(),
                    );
                }
            }
            assert_eq!(state.all_children_joined(), mask == 0b111);
            assert_eq!(
                state.all_children_joined(),
                keys.iter().all(|child| state.joined(*child))
            );
            state.check_invariants();
        }
    }

    #[test]
    fn sampled_removal_suppresses_start_effects_until_commit() {
        for flavor in [ScopeFlavor::Ordered, ScopeFlavor::Dynamic] {
            let membership = memberships(1)[0];
            let mut state = SupervisorState::new(flavor, ScopeLifecycle::starting());
            let child = admit(&mut state, membership, true);
            step(&mut state, Event::RemovalSampled { child }, &mut Vec::new());
            let mut effects = Vec::new();
            step(&mut state, Event::Settle, &mut effects);
            assert!(
                effects.is_empty(),
                "{flavor:?} startup cannot schedule an already-latched membership"
            );
            assert!(state.lifecycle().is_starting());

            step(&mut state, Event::RemovalLatched { child }, &mut effects);
            assert_eq!(effects, [Effect::StopChild { child }]);
            state.check_invariants();
        }
    }

    /// R4 — ordered startup hands out one start edge at a time *and reaches
    /// every member*. The cursor's advance is a progress rule, so no safety
    /// property over reachable states can see it: a cursor that skipped a
    /// middle member would leave that member unready forever, which is a
    /// startup that never completes rather than a state that is wrong.
    #[test]
    fn ordered_startup_advances_through_every_initial_member_in_order() {
        let members = memberships(3);
        let mut state = SupervisorState::new(ScopeFlavor::Ordered, ScopeLifecycle::starting());
        let keys: Vec<_> = members
            .iter()
            .map(|membership| admit(&mut state, *membership, true))
            .collect();

        let mut effects = Vec::new();
        for &expected in &keys {
            step(&mut state, Event::Settle, &mut effects);
            assert_eq!(
                effects,
                [Effect::StartChild { child: expected }],
                "ordered startup starts the cursor's own member next"
            );
            effects.clear();
            step(&mut state, Event::Spawned { child: expected }, &mut effects);
            step(
                &mut state,
                Event::Ready {
                    child: expected,
                    removal_latched: false,
                },
                &mut effects,
            );
            assert!(effects.is_empty());
        }

        step(&mut state, Event::Settle, &mut effects);
        assert!(matches!(
            effects.as_slice(),
            [Effect::StartupCompleted { .. }]
        ));
        state.check_invariants();
    }

    /// R4 — dynamic startup emits one start per *unspawned* initial member.
    /// A member awaiting a restart deadline has spawned once already, and its
    /// re-construction belongs to the restart path rather than to settlement;
    /// re-attracting it here would ask two owners to construct the same
    /// incarnation. No state-space property can see this — the record is
    /// `startable`, so the start is acknowledgeable, and re-settling reproduces
    /// it, so the fixed point holds — which is why it is pinned directly.
    #[test]
    fn dynamic_startup_leaves_a_restart_pending_member_to_the_restart_path() {
        let membership = memberships(1)[0];
        let mut state = SupervisorState::new(ScopeFlavor::Dynamic, ScopeLifecycle::starting());
        let child = admit(&mut state, membership, true);
        let mut effects = Vec::new();
        step(&mut state, Event::Settle, &mut effects);
        assert_eq!(effects, [Effect::StartChild { child }]);
        effects.clear();

        for event in [
            Event::Spawned { child },
            Event::IncarnationComplete { child },
            Event::RestartPending { child },
        ] {
            step(&mut state, event, &mut effects);
        }
        assert_eq!(
            state.child_state(child),
            Some(ChildState::Resident(IncarnationState::RestartPending))
        );
        assert!(state.spawned_once(child));
        effects.clear();

        step(&mut state, Event::Settle, &mut effects);
        assert!(
            effects.is_empty(),
            "settlement does not re-attract a member the restart path owns, got {effects:?}"
        );
        state.check_invariants();
    }

    /// The ordered half of R6's aggregate rule, which is narrower than the
    /// section states: a member latched for removal parks the ordered cursor
    /// until the removal commits, so ordered startup can have every initial
    /// member ready and still withhold completion. Dynamic startup completes
    /// on the readiness predicate alone.
    #[test]
    fn ordered_startup_waits_for_a_removing_member_to_commit() {
        let members = memberships(2);
        let mut state = SupervisorState::new(ScopeFlavor::Ordered, ScopeLifecycle::starting());
        let keys: Vec<_> = members
            .iter()
            .map(|membership| admit(&mut state, *membership, true))
            .collect();
        let mut effects = Vec::new();
        for &child in &keys {
            step(&mut state, Event::Settle, &mut effects);
            effects.clear();
            step(&mut state, Event::Spawned { child }, &mut effects);
            step(
                &mut state,
                Event::Ready {
                    child,
                    removal_latched: false,
                },
                &mut effects,
            );
        }
        effects.clear();

        let removing = keys[1];
        step(
            &mut state,
            Event::RemovalLatched { child: removing },
            &mut effects,
        );
        assert_eq!(effects, [Effect::StopChild { child: removing }]);
        effects.clear();
        assert!(
            keys.iter().all(|&child| state.initial_ready(child)),
            "every initial member is ready before the cursor is asked to advance"
        );

        step(&mut state, Event::Settle, &mut effects);
        assert!(
            effects.is_empty(),
            "the ordered cursor waits on the removing member, {effects:?}"
        );

        step(
            &mut state,
            Event::IncarnationComplete { child: removing },
            &mut effects,
        );
        step(
            &mut state,
            Event::DisposalStarted { child: removing },
            &mut effects,
        );
        step(
            &mut state,
            Event::Terminalized { child: removing },
            &mut effects,
        );
        assert_eq!(effects, [Effect::FinalizeRemoval { child: removing }]);
        effects.clear();
        step(&mut state, Event::Reclaim { child: removing }, &mut effects);
        step(&mut state, Event::Settle, &mut effects);
        assert!(
            matches!(effects.as_slice(), [Effect::StartupCompleted { .. }]),
            "committing the removal releases the aggregate, {effects:?}"
        );
        state.check_invariants();
    }

    #[test]
    fn ordered_stop_releases_one_child_per_join_in_reverse_order() {
        let members = memberships(3);
        let mut state = SupervisorState::new(ScopeFlavor::Ordered, ScopeLifecycle::running());
        let keys: Vec<_> = members
            .iter()
            .map(|membership| admit(&mut state, *membership, true))
            .collect();
        for &child in &keys {
            step(&mut state, Event::Spawned { child }, &mut Vec::new());
        }

        let mut effects = Vec::new();
        let (startup_pending, drain_state) =
            super::begin_drain(&mut state, StopReason::ShutdownRequested, &mut effects)
                .expect("the first drain returns its publication result");
        assert_eq!(drain_state, ScopeState::Draining);
        assert!(!startup_pending);
        assert!(
            effects.is_empty(),
            "an ordered drain exposes its first stop through settlement"
        );

        for &expected in keys.iter().rev() {
            step(&mut state, Event::Settle, &mut effects);
            assert_eq!(effects, [Effect::StopChild { child: expected }]);
            effects.clear();
            assert_eq!(state.ordered_stop_waiting(), Some(expected));

            step(
                &mut state,
                Event::IncarnationComplete { child: expected },
                &mut effects,
            );
            step(
                &mut state,
                Event::DisposalStarted { child: expected },
                &mut effects,
            );
            step(
                &mut state,
                Event::Terminalized { child: expected },
                &mut effects,
            );
            assert!(effects.is_empty());
        }

        step(&mut state, Event::Settle, &mut effects);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Finished {
                reason: StopReason::ShutdownRequested
            }]
        ));
        assert_eq!(state.ordered_stop_waiting(), None);
        assert!(state.all_children_joined());
        state.check_invariants();
    }

    #[test]
    fn transition_owner_results_are_separate_from_commands() {
        let members = memberships(2);

        let mut failed = SupervisorState::new(ScopeFlavor::Dynamic, ScopeLifecycle::starting());
        assert_eq!(
            super::fail_startup(&mut failed),
            Some(ScopeState::StartupFailed)
        );
        assert_eq!(
            super::fail_startup(&mut failed),
            None,
            "only the winning startup failure publishes the transition"
        );

        let mut draining = SupervisorState::new(ScopeFlavor::Dynamic, ScopeLifecycle::starting());
        let child = admit(&mut draining, members[0], true);
        let mut effects = Vec::new();
        let (startup_pending, drain_state) =
            super::begin_drain(&mut draining, StopReason::ShutdownRequested, &mut effects)
                .expect("the first drain returns its publication result");
        assert_eq!(drain_state, ScopeState::Draining);
        assert!(startup_pending);
        assert_eq!(effects, [Effect::StopChild { child }]);
        effects.clear();
        assert!(
            super::begin_drain(&mut draining, StopReason::ShutdownRequested, &mut effects,)
                .is_none(),
            "a drain upgrade does not republish its entry result"
        );
        assert!(effects.is_empty());

        let mut forced = SupervisorState::new(ScopeFlavor::Dynamic, ScopeLifecycle::running());
        let child = admit(&mut forced, members[1], false);
        let (startup_pending, drain_state) = super::force(&mut forced, &mut effects)
            .expect("forcing a live scope returns its initial drain");
        assert_eq!(drain_state, ScopeState::Draining);
        assert!(!startup_pending);
        assert_eq!(
            effects,
            [Effect::StopChild { child }, Effect::ForceChild { child }]
        );
        effects.clear();
        assert!(super::force(&mut forced, &mut effects).is_none());
        assert_eq!(effects, [Effect::ForceChild { child }]);
    }

    #[test]
    fn registration_keys_are_monotonic_and_exhaustion_is_fail_closed() {
        let members = memberships(4);
        let mut state = SupervisorState::new(ScopeFlavor::Dynamic, ScopeLifecycle::running());
        let first = admit(&mut state, members[0], false);
        step(
            &mut state,
            Event::DisposalStarted { child: first },
            &mut Vec::new(),
        );
        step(
            &mut state,
            Event::Terminalized { child: first },
            &mut Vec::new(),
        );
        step(&mut state, Event::Reclaim { child: first }, &mut Vec::new());
        let successor = admit(&mut state, members[1], false);
        assert!(successor > first, "reclaim never makes a key reusable");

        state.keys = PoisonedCounter::near_exhaustion();
        let last = admit(&mut state, members[2], false);
        assert_eq!(last, ChildKey(u64::MAX - 1));
        assert_eq!(super::admit(&mut state, members[3], false), None);
        assert_eq!(state.key_for(members[3]), None);
        assert!(
            super::admit(&mut state, members[3], false).is_none(),
            "poisoned exhaustion remains fail closed"
        );
        state.check_invariants();
    }

    #[test]
    fn duplicate_admission_and_stale_events_are_total_noops() {
        let membership = memberships(1)[0];
        let mut state = SupervisorState::new(ScopeFlavor::Dynamic, ScopeLifecycle::running());
        let child = admit(&mut state, membership, false);
        let mut effects = Vec::new();
        assert_eq!(super::admit(&mut state, membership, false), None);
        assert!(effects.is_empty());
        assert_eq!(state.len(), 1);

        step(&mut state, Event::DisposalStarted { child }, &mut effects);
        step(&mut state, Event::Terminalized { child }, &mut effects);
        let terminal = state.child_state(child);
        for event in [
            Event::Spawned { child },
            Event::Ready {
                child,
                removal_latched: false,
            },
            Event::IncarnationComplete { child },
            Event::RestartPending { child },
            Event::StopStarted { child },
            Event::DisposalStarted { child },
        ] {
            step(&mut state, event, &mut effects);
            assert_eq!(state.child_state(child), terminal);
        }
        step(
            &mut state,
            Event::Reclaim {
                child: ChildKey::fixture(999),
            },
            &mut effects,
        );
        assert_eq!(state.child_state(child), terminal);
        state.check_invariants();
    }

    #[test]
    fn stale_events_cannot_skip_or_regress_incarnation_phases() {
        let membership = memberships(1)[0];
        let mut state = SupervisorState::new(ScopeFlavor::Dynamic, ScopeLifecycle::running());
        let child = admit(&mut state, membership, false);

        for event in [
            Event::Ready {
                child,
                removal_latched: false,
            },
            Event::IncarnationComplete { child },
            Event::RestartPending { child },
            Event::StopStarted { child },
            Event::Terminalized { child },
        ] {
            step(&mut state, event, &mut Vec::new());
            assert_eq!(
                state.child_state(child),
                Some(ChildState::Resident(IncarnationState::Unstarted)),
                "an event without its predecessor is a total no-op"
            );
        }

        step(&mut state, Event::Spawned { child }, &mut Vec::new());
        assert_eq!(
            state.child_state(child),
            Some(ChildState::Resident(IncarnationState::Active))
        );
        for event in [
            Event::Spawned { child },
            Event::RestartPending { child },
            Event::DisposalStarted { child },
            Event::Terminalized { child },
        ] {
            step(&mut state, event, &mut Vec::new());
            assert_eq!(
                state.child_state(child),
                Some(ChildState::Resident(IncarnationState::Active)),
                "duplicate/future events cannot skip an executing incarnation"
            );
        }

        step(&mut state, Event::StopStarted { child }, &mut Vec::new());
        step(&mut state, Event::Spawned { child }, &mut Vec::new());
        assert_eq!(
            state.child_state(child),
            Some(ChildState::Resident(IncarnationState::Stopping)),
            "a stale spawn cannot rewind a stop"
        );
        step(
            &mut state,
            Event::IncarnationComplete { child },
            &mut Vec::new(),
        );
        step(&mut state, Event::StopStarted { child }, &mut Vec::new());
        assert_eq!(
            state.child_state(child),
            Some(ChildState::Resident(IncarnationState::Complete)),
            "a stale stop cannot rewind completed work"
        );
        step(&mut state, Event::RestartPending { child }, &mut Vec::new());
        step(
            &mut state,
            Event::IncarnationComplete { child },
            &mut Vec::new(),
        );
        assert_eq!(
            state.child_state(child),
            Some(ChildState::Resident(IncarnationState::RestartPending)),
            "a duplicate completion cannot cancel pending restart state"
        );
        state.check_invariants();
    }

    /// Settlement is level-triggered and its driver re-enters [`Event::Settle`]
    /// until a pass emits nothing, so a start effect the shell declines is
    /// re-derived from unchanged state forever rather than merely wasted. Pin
    /// the emission set to [`Event::Spawned`]'s acceptance set across every
    /// child state, in both flavors and both membership statuses.
    #[test]
    fn start_effects_are_confined_to_the_spawn_transition() {
        let phases = [
            IncarnationState::Unstarted,
            IncarnationState::Active,
            IncarnationState::Stopping,
            IncarnationState::Complete,
            IncarnationState::RestartPending,
            IncarnationState::Disposing,
            IncarnationState::Joined,
        ];
        for flavor in [ScopeFlavor::Ordered, ScopeFlavor::Dynamic] {
            for removing in [false, true] {
                for phase in phases {
                    let membership = memberships(1)[0];
                    let mut state = SupervisorState::new(flavor, ScopeLifecycle::starting());
                    let child = admit(&mut state, membership, true);
                    state.children.get_mut(&child).expect("child").state = if removing {
                        ChildState::Removing(phase)
                    } else {
                        ChildState::Resident(phase)
                    };

                    let mut effects = Vec::new();
                    step(&mut state, Event::Settle, &mut effects);
                    let started = effects.contains(&Effect::StartChild { child });

                    // `spawned_once` is still false in every arm, so the flag
                    // reports exactly whether the transition was accepted —
                    // unlike the resulting state, which already reads `Active`
                    // in the arm a stale spawn must not rewind.
                    let mut spawned = state.clone();
                    step(&mut spawned, Event::Spawned { child }, &mut Vec::new());
                    assert_eq!(
                        started,
                        spawned.spawned_once(child),
                        "{flavor:?} removing={removing} {phase:?}: a start effect must be one \
                         accepted `Spawned` away from executing"
                    );

                    if !started {
                        let mut again = Vec::new();
                        step(&mut state, Event::Settle, &mut again);
                        assert!(
                            again.is_empty(),
                            "{flavor:?} removing={removing} {phase:?}: settlement with no start \
                             effect to honour must already be at its fixed point, got {again:?}"
                        );
                    }
                    state.check_invariants();
                }
            }
        }
    }
}
