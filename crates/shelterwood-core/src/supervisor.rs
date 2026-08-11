//! Pure structural supervision reducer.
//!
//! Runtime adapters sample latches and clocks before constructing an
//! [`Event`]. The reducer owns membership, startup, and ordered-stop state and
//! emits commands through an [`EffectSink`]; it never runs user code, reads a
//! clock, samples randomness, or performs I/O.

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
pub enum IncarnationState {
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
pub enum ChildState {
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

    /// Whether the membership-terminal edge has been published.
    pub fn membership_terminal(self) -> bool {
        self.incarnation() == IncarnationState::Joined
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
    Admit {
        membership: Membership,
        initial: bool,
        start_immediately: bool,
    },
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
    Admitted {
        child: ChildKey,
    },
    StartChild {
        child: ChildKey,
    },
    StopChild {
        child: ChildKey,
    },
    ForceChild {
        child: ChildKey,
    },
    FinalizeRemoval {
        child: ChildKey,
    },
    StartupCompleted {
        state: ScopeState,
    },
    StartupFailed {
        state: ScopeState,
    },
    DrainStarted {
        state: ScopeState,
        startup_pending: bool,
    },
    Finished {
        reason: StopReason,
    },
}

/// Destination for effects emitted synchronously by [`step`].
pub trait EffectSink {
    fn push(&mut self, effect: Effect);
}

impl EffectSink for Vec<Effect> {
    fn push(&mut self, effect: Effect) {
        Vec::push(self, effect);
    }
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

    pub fn child_state(&self, child: ChildKey) -> Option<ChildState> {
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

    pub fn membership_terminal(&self, child: ChildKey) -> bool {
        self.child_state(child)
            .is_some_and(ChildState::membership_terminal)
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
            if record.state.membership_terminal() || !expected.contains(&record.state.incarnation())
            {
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

    fn admit(
        &mut self,
        membership: Membership,
        initial: bool,
        start_immediately: bool,
        effects: &mut impl EffectSink,
    ) {
        if self.child_keys.contains_key(&membership) {
            return;
        }
        let Some(raw) = self.keys.mint() else {
            return;
        };
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
        effects.push(Effect::Admitted { child });
        if start_immediately && !self.lifecycle.is_draining() && !self.lifecycle.startup_failed() {
            effects.push(Effect::StartChild { child });
        }
    }

    fn settle_startup(&mut self, effects: &mut impl EffectSink) {
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

    fn begin_drain(&mut self, reason: StopReason, effects: &mut impl EffectSink) {
        let Some(effect) = self.lifecycle.begin_drain(reason) else {
            return;
        };
        effects.push(Effect::DrainStarted {
            state: effect.state(),
            startup_pending: effect.startup_pending(),
        });
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
    }

    fn settle_ordered_stop(&mut self, effects: &mut impl EffectSink) {
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

    fn settle_finish(&mut self, effects: &mut impl EffectSink) {
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

    fn apply(&mut self, event: Event, effects: &mut impl EffectSink) {
        match event {
            Event::Admit {
                membership,
                initial,
                start_immediately,
            } => self.admit(membership, initial, start_immediately, effects),
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
                    || record.state.membership_terminal()
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
                    && self.lifecycle.is_starting()
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
                if let Some(state) = self.lifecycle.fail_startup() {
                    effects.push(Effect::StartupFailed { state });
                }
            }
            Event::BeginDrain { reason } => self.begin_drain(reason, effects),
            Event::Force => {
                self.hard_forced = true;
                self.begin_drain(StopReason::ShutdownRequested, effects);
                for child in self.children.keys().copied() {
                    if !self.children[&child].state.joined() {
                        effects.push(Effect::ForceChild { child });
                    }
                }
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

    #[cfg(test)]
    fn check_invariants(&self) {
        assert_eq!(self.children.len(), self.child_keys.len());
        for (&key, child) in &self.children {
            assert_eq!(self.child_keys.get(&child.membership), Some(&key));
            assert_eq!(child.state.membership_terminal(), child.state.joined());
        }
        if let Some(waiting) = self.ordered_stop_waiting {
            assert!(self.lifecycle.is_draining());
            assert!(self.contains(waiting) || self.ordered_stop_cursor < Some(waiting));
        }
    }
}

/// Applies one total transition to [`SupervisorState`].
pub fn step(state: &mut SupervisorState, event: Event, effects: &mut impl EffectSink) {
    state.apply(event, effects);
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::{ChildId, ScopeIdentity};

    use super::*;

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
        let mut effects = Vec::new();
        step(
            state,
            Event::Admit {
                membership,
                initial,
                start_immediately: false,
            },
            &mut effects,
        );
        let [Effect::Admitted { child }] = effects.as_slice() else {
            panic!("admission emits exactly one key")
        };
        *child
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
        step(
            &mut state,
            Event::BeginDrain {
                reason: StopReason::ShutdownRequested,
            },
            &mut effects,
        );
        assert!(matches!(effects.as_slice(), [Effect::DrainStarted { .. }]));
        effects.clear();

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
        let mut effects = Vec::new();
        step(
            &mut state,
            Event::Admit {
                membership: members[3],
                initial: false,
                start_immediately: false,
            },
            &mut effects,
        );
        assert!(effects.is_empty());
        assert_eq!(state.key_for(members[3]), None);
        step(
            &mut state,
            Event::Admit {
                membership: members[3],
                initial: false,
                start_immediately: false,
            },
            &mut effects,
        );
        assert!(
            effects.is_empty(),
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
        step(
            &mut state,
            Event::Admit {
                membership,
                initial: false,
                start_immediately: true,
            },
            &mut effects,
        );
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

    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    enum SmallEvent {
        Spawn(usize),
        Ready(usize),
        Remove(usize),
        Terminal(usize),
    }

    fn permutations<T: Copy>(values: &mut [T], at: usize, visit: &mut impl FnMut(&[T])) {
        if at == values.len() {
            visit(values);
            return;
        }
        for index in at..values.len() {
            values.swap(at, index);
            permutations(values, at + 1, visit);
            values.swap(at, index);
        }
    }

    /// Exhausts every ordering of spawn/readiness/removal/terminal inputs for
    /// two children (8! schedules), then every three-child ordering of the
    /// adjudication-sensitive readiness/removal/terminal inputs (9!
    /// schedules). Invalid/stale events are intentionally part of the state
    /// space: totality requires them to be harmless.
    #[test]
    fn exhaustive_small_scope_interleavings_preserve_reducer_invariants() {
        for width in [2, 3] {
            let members = memberships(width);
            let mut schedule = Vec::new();
            for index in 0..width {
                if width == 2 {
                    schedule.push(SmallEvent::Spawn(index));
                }
                schedule.extend([
                    SmallEvent::Ready(index),
                    SmallEvent::Remove(index),
                    SmallEvent::Terminal(index),
                ]);
            }
            for flavor in [ScopeFlavor::Ordered, ScopeFlavor::Dynamic] {
                let mut unique_final = HashSet::new();
                permutations(&mut schedule, 0, &mut |ordering| {
                    let mut state = SupervisorState::new(flavor, ScopeLifecycle::starting());
                    let keys: Vec<_> = members
                        .iter()
                        .map(|membership| admit(&mut state, *membership, true))
                        .collect();
                    let mut published_ready = HashSet::new();
                    let mut removal_seen = HashSet::new();
                    let mut joined_seen = HashSet::new();
                    for event in ordering {
                        let mut effects = Vec::new();
                        match *event {
                            SmallEvent::Spawn(index) => step(
                                &mut state,
                                Event::Spawned { child: keys[index] },
                                &mut effects,
                            ),
                            SmallEvent::Ready(index) => {
                                let removing = state.membership_status(keys[index])
                                    == MembershipStatus::Removing;
                                step(
                                    &mut state,
                                    Event::Ready {
                                        child: keys[index],
                                        removal_latched: removing,
                                    },
                                    &mut effects,
                                );
                                if !removing && state.initial_ready(keys[index]) {
                                    published_ready.insert(keys[index]);
                                }
                            }
                            SmallEvent::Remove(index) => step(
                                &mut state,
                                Event::RemovalLatched { child: keys[index] },
                                &mut effects,
                            ),
                            SmallEvent::Terminal(index) => {
                                // Runtime terminal completion joins retained
                                // definition disposal before publishing the
                                // membership edge. Keep that ordered pair atomic
                                // while permuting it against the other children'
                                // inputs.
                                step(
                                    &mut state,
                                    Event::DisposalStarted { child: keys[index] },
                                    &mut effects,
                                );
                                step(
                                    &mut state,
                                    Event::Terminalized { child: keys[index] },
                                    &mut effects,
                                );
                            }
                        }
                        step(&mut state, Event::Settle, &mut effects);
                        state.check_invariants();
                        for child in &keys {
                            if removal_seen.contains(child) {
                                assert_eq!(
                                    state.membership_status(*child),
                                    MembershipStatus::Removing,
                                    "removal is a monotone state transition"
                                );
                            }
                            if joined_seen.contains(child) {
                                assert!(state.joined(*child), "joined state cannot regress");
                            }
                            if state.membership_status(*child) == MembershipStatus::Removing {
                                removal_seen.insert(*child);
                                assert!(
                                    !state.initial_ready(*child) || published_ready.contains(child),
                                    "removal never manufactures a readiness edge"
                                );
                            }
                            if state.joined(*child) {
                                joined_seen.insert(*child);
                            }
                        }
                        assert_eq!(
                            state.all_children_joined(),
                            keys.iter().all(|child| state.joined(*child))
                        );
                        for effect in &effects {
                            match effect {
                                Effect::StartChild { child }
                                | Effect::StopChild { child }
                                | Effect::ForceChild { child }
                                | Effect::FinalizeRemoval { child } => {
                                    assert!(state.contains(*child), "effects name a live key");
                                }
                                _ => {}
                            }
                            if let Effect::StartChild { child } = effect {
                                assert!(
                                    state.children[child].startable(),
                                    "a start effect the shell would decline re-derives from \
                                     unchanged state and never lets settlement terminate"
                                );
                            }
                        }
                    }
                    unique_final.insert(
                        keys.iter()
                            .map(|child| state.child_state(*child).expect("resident"))
                            .collect::<Vec<_>>(),
                    );
                });
                assert!(!unique_final.is_empty());
            }
        }
    }
}
