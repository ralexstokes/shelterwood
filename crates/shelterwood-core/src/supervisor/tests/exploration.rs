//! Exhaustive reachable-state exploration of the supervision reducer.
//!
//! The reducer is a finite transition system once its child roster is fixed,
//! so the whole reachable state space can be walked directly instead of
//! sampled through schedules. Every state is expanded against the *entire*
//! event alphabet — including events it will reject — and visited once, which
//! is what lets the alphabet stay wide enough for the drain and restart rules
//! to be reachable at all.
//!
//! What the walk checks is the reducer-expressible subset of SPEC's invariant
//! list: R1–R6, E4, S3–S5, and T5's removal lifecycle (§15.3). Most are stated
//! per transition; the cardinality and ordering ones — an effect issued at
//! most once along a path, or only after another — are stated over a per-path
//! [`History`] the walk explores alongside the state. The rest of the list —
//! the stop ladder (S1/S2), the sampled latches (S6), driver death (S7), the
//! exit funnel (E1–E3/E5/E6), tree lowering (T1–T4, T6, T7) and T5's handle
//! matching — is not stated over `SupervisorState`, so it is not claimable
//! here and is left to the engine and integration suites that already own it.

use std::collections::{HashSet, VecDeque};

use crate::{
    engine::{MembershipStatus, ScopeLifecycle},
    exit::{StopReason, stop_reason_precedence},
    policy::ScopeFlavor,
    supervisor::{
        ChildKey, ChildRecord, ChildState, Effect, Event, IncarnationState, StartupMembership,
        SupervisorState, begin_drain, fail_startup, force, step,
    },
};

use super::{admit, memberships};

/// The widest roster a fingerprint can encode; see [`fingerprint`].
const MAX_WIDTH: usize = 4;

/// Bits one child slot occupies in a fingerprint.
const SLOT_BITS: usize = 9;

// The scope-level fields packed above the slots end 18 bits past them.
const _: () = assert!(SLOT_BITS * MAX_WIDTH + 18 <= 64);

/// Packs every field of [`SupervisorState`] into one hashable word.
///
/// The projection is what makes the walk terminate: two states with the same
/// fingerprint are treated as the same state, so only the first is expanded.
/// That makes any field dropped from it a silent pruning bug — the successors
/// only the discarded state had are never explored — which is why the
/// destructure below is exhaustive and a new field cannot compile until it is
/// classified. Four fields are deliberately not encoded, each with a reason
/// that holds by construction rather than by inspection:
///
/// - `child_keys` is the exact inverse of `children`, and the walk asserts
///   that agreement through `check_invariants` at every visited state, so it
///   cannot carry a distinction `children` does not already carry.
/// - `keys` only advances on admission, which is not in the alphabet, so it is
///   constant across a walk. [`explore`] asserts that instead of encoding it.
/// - `ordered_stop_inspections` is diagnostic; see the destructure below.
/// - `ChildRecord::membership` is immutable for the life of its key, so the
///   slot index already carries it; see `slot` below.
///
/// A packed word rather than a struct of collections because the walk hashes
/// tens of millions of them: allocation per state dominated everything else
/// when this was a `Vec`-shaped projection.
fn fingerprint(state: &SupervisorState, keys: &[ChildKey]) -> u64 {
    let SupervisorState {
        flavor,
        lifecycle,
        children,
        child_keys: _,
        keys: _,
        next_ordered_start,
        ordered_stop_cursor,
        ordered_stop_waiting,
        hard_forced,
        finish_emitted,
        // Diagnostic-only: `apply` never reads it, so two states differing
        // only here have identical futures. Including it would make every
        // revisit look novel and the walk would never converge.
        ordered_stop_inspections: _,
    } = state;

    // Child slots are positional, and a membership is immutable for the life
    // of its key, so the slot index carries the identity the record would.
    let slot = |key: ChildKey| {
        keys.iter()
            .position(|candidate| *candidate == key)
            .expect("the walk only ever names roster keys") as u64
    };
    let mut word = 0;
    for (&key, record) in children {
        let ChildRecord {
            membership: _,
            state,
            startup,
            spawned_once,
        } = *record;
        let (initial, ready) = match startup {
            StartupMembership::Initial { ready } => (true, ready),
            StartupMembership::Runtime => (false, false),
        };
        let incarnation = match state.incarnation() {
            IncarnationState::Unstarted => 0,
            IncarnationState::Active => 1,
            IncarnationState::Stopping => 2,
            IncarnationState::Complete => 3,
            IncarnationState::RestartPending => 4,
            IncarnationState::Disposing => 5,
            IncarnationState::Joined => 6,
        };
        let removal = match state {
            ChildState::Resident(_) => 0,
            ChildState::RemovalSampled(_) => 1,
            ChildState::Removing(_) => 2,
        };
        let packed = 1
            | incarnation << 1
            | removal << 4
            | u64::from(initial) << 6
            | u64::from(ready) << 7
            | u64::from(spawned_once) << 8;
        word |= packed << (SLOT_BITS * slot(key) as usize);
    }

    let cursor = |key: &Option<ChildKey>| key.map_or(7, slot);
    let (lifecycle_state, lifecycle_reason) = lifecycle.fingerprint();
    // The exhaustive match in `ScopeLifecycle::fingerprint` forces a new
    // variant to choose a projection, but not to choose one that fits: a value
    // of 8 would alias into the flavor bit and prune the space silently.
    assert!(lifecycle_state < 8 && lifecycle_reason < 8, "3-bit fields");
    word |= u64::from(lifecycle_state) << (SLOT_BITS * MAX_WIDTH);
    word |= u64::from(lifecycle_reason) << (SLOT_BITS * MAX_WIDTH + 3);
    word |= u64::from(*flavor == ScopeFlavor::Ordered) << (SLOT_BITS * MAX_WIDTH + 6);
    word |= u64::from(*hard_forced) << (SLOT_BITS * MAX_WIDTH + 7);
    word |= u64::from(*finish_emitted) << (SLOT_BITS * MAX_WIDTH + 8);
    word |= cursor(next_ordered_start) << (SLOT_BITS * MAX_WIDTH + 9);
    word |= cursor(ordered_stop_cursor) << (SLOT_BITS * MAX_WIDTH + 12);
    word |= cursor(ordered_stop_waiting) << (SLOT_BITS * MAX_WIDTH + 15);
    word
}

/// The roster a walk starts from: one entry per child, `true` for an initial
/// membership and `false` for a runtime one.
///
/// Admission is deliberately not in the alphabet. Every admission mints a
/// fresh key, so an explorable `Admit` would make the state space infinite;
/// fixing the roster up front keeps it finite while still covering the
/// initial/runtime distinction R1 is stated over. Consequently this walk is
/// also blind by design to the runtime facade's admit-during-drain rejection;
/// the facade's targeted admission tests are the verification boundary for
/// that policy.
type Roster = &'static [bool];

/// One walk input: a reducer [`Event`], or one of the owner transitions that
/// return a publication and therefore enter through their own functions. The
/// walk discards those publications; what it checks is state and effects.
#[derive(Clone, Debug)]
enum Input {
    Step(Event),
    FailStartup,
    BeginDrain(StopReason),
    Force,
}

impl Input {
    fn apply(&self, state: &mut SupervisorState, effects: &mut Vec<Effect>) {
        match self {
            Self::Step(event) => step(state, event.clone(), effects),
            Self::FailStartup => {
                let _ = fail_startup(state);
            }
            Self::BeginDrain(reason) => {
                let _ = begin_drain(state, reason.clone(), effects);
            }
            Self::Force => {
                let _ = force(state, effects);
            }
        }
    }
}

/// Every input the walk offers in every state, including the ones the state
/// will reject — totality is a property under test, not a precondition.
fn alphabet(keys: &[ChildKey]) -> Vec<Input> {
    let mut events = Vec::new();
    for &child in keys {
        events.extend([
            Event::Spawned { child },
            Event::Ready { child },
            Event::IncarnationComplete { child },
            Event::RestartPending { child },
            Event::StopStarted { child },
            Event::DisposalStarted { child },
            Event::Terminalized { child },
            Event::RemovalSampled { child },
            Event::RemovalLatched { child },
            Event::Reclaim { child },
        ]);
    }
    events.push(Event::Settle);
    let mut inputs: Vec<_> = events.into_iter().map(Input::Step).collect();
    inputs.extend([
        Input::FailStartup,
        // Two drain reasons at opposite ends of the precedence lattice, so
        // both the upgrading and the ignored direction of S4 are reachable.
        Input::BeginDrain(StopReason::Finished),
        Input::BeginDrain(StopReason::ShutdownRequested),
        Input::Force,
    ]);
    for input in &inputs {
        // Exhaustive over `Input` and `Event` without restating their
        // construction: a new variant does not compile until this guard is
        // updated alongside the canonical list above, preventing silent
        // pruning from the walk.
        let Input::Step(event) = input else {
            match input {
                Input::Step(_) | Input::FailStartup | Input::BeginDrain(_) | Input::Force => {}
            }
            continue;
        };
        match event {
            Event::Spawned { .. }
            | Event::Ready { .. }
            | Event::IncarnationComplete { .. }
            | Event::RestartPending { .. }
            | Event::StopStarted { .. }
            | Event::DisposalStarted { .. }
            | Event::Terminalized { .. }
            | Event::RemovalSampled { .. }
            | Event::RemovalLatched { .. }
            | Event::Reclaim { .. }
            | Event::Settle => {}
        }
    }
    inputs
}

struct Transition<'a> {
    before: &'a SupervisorState,
    event: &'a Input,
    after: &'a SupervisorState,
    effects: &'a [Effect],
    keys: &'a [ChildKey],
    /// What the path reaching `before` has already issued.
    history: &'a History,
    /// `history` extended by this transition.
    history_after: &'a History,
}

/// Where a [`Effect::StopChild`] comes from. The reducer has two stop sources
/// with separate cardinality rules: a removal commit stops its child once per
/// key, and a drain stops each child once per scope (S3). Force is a third
/// command, but it speaks [`Effect::ForceChild`], not a stop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StopSource {
    Removal,
    Drain,
}

impl StopSource {
    fn of(event: &Input) -> Self {
        match event {
            Input::Step(Event::RemovalLatched { .. }) => Self::Removal,
            _ => Self::Drain,
        }
    }
}

/// The per-path facts the cardinality and ordering invariants are stated over,
/// as one bit per roster slot (or per scope).
///
/// Cardinality ("at most once along a path") is a property of paths, and the
/// walk visits states. Rather than trust the reducer's own bookkeeping to record
/// that an effect was issued — which is the very thing a duplicate-emission bug
/// gets wrong — the walk runs this monitor beside the reducer and explores the
/// product: [`explore`] keys its visited set on the pair of the state's
/// fingerprint and this history. Every check below is a function of
/// `(before, history, transition)`, and every reachable pair is expanded
/// against the whole alphabet, so checking every product transition is
/// checking every path. The monitor is bits, never counts: a second issue is a
/// failure at the transition that issues it, so no value above one is ever
/// stored and the product stays finite.
///
/// Every field is learned from what the walk observes — effects emitted and
/// state edges taken — never read from a reducer field, so a mutation that
/// forgets its own issue flag cannot also blind the monitor.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
struct History {
    /// Slots whose incarnation has taken an accepted spawn edge.
    spawned: u8,
    /// Slots a drain has issued a stop for.
    drain_stopped: u8,
    /// Slots whose removal command has committed and issued its effect.
    committed: u8,
    /// Slots whose removal has been finalized.
    finalized: u8,
    finished: bool,
}

// One bit per slot in each `u8` field.
const _: () = assert!(MAX_WIDTH <= u8::BITS as usize);

fn bit(keys: &[ChildKey], child: ChildKey) -> u8 {
    1 << keys
        .iter()
        .position(|candidate| *candidate == child)
        .expect("the walk only ever names roster keys")
}

impl History {
    /// Records one emitted effect. Folded in emission order, so a duplicate
    /// within one step is as visible to the checks as one across steps.
    fn issue(&mut self, effect: &Effect, event: &Input, keys: &[ChildKey]) {
        match effect {
            Effect::StopChild { child } => match StopSource::of(event) {
                StopSource::Removal => self.committed |= bit(keys, *child),
                StopSource::Drain => self.drain_stopped |= bit(keys, *child),
            },
            Effect::FinalizeRemoval { child } => {
                // A commit that finds its child already joined finalizes in
                // place: the finalize is the commit's effect.
                if matches!(event, Input::Step(Event::RemovalLatched { .. })) {
                    self.committed |= bit(keys, *child);
                }
                self.finalized |= bit(keys, *child);
            }
            Effect::Finished { .. } => self.finished = true,
            Effect::StartChild { .. }
            | Effect::ForceChild { .. }
            | Effect::StartupCompleted { .. } => {}
        }
    }

    /// The history of the path extended by one transition.
    fn after(
        mut self,
        before: &SupervisorState,
        event: &Input,
        after: &SupervisorState,
        effects: &[Effect],
        keys: &[ChildKey],
    ) -> Self {
        for &child in keys {
            // E4 admits exactly one edge into `Active`: the spawn.
            if incarnation(before, child) != Some(IncarnationState::Active)
                && incarnation(after, child) == Some(IncarnationState::Active)
            {
                self.spawned |= bit(keys, child);
            }
        }
        for effect in effects {
            self.issue(effect, event, keys);
        }
        // A reclaimed key never returns (E4) and no effect can name it (R5),
        // so its history can no longer fail a check. Forgetting it keeps two
        // paths that differ only in a retired key's past from being explored
        // twice.
        for &child in keys {
            if !after.contains(child) {
                self.forget(bit(keys, child));
            }
        }
        self
    }

    fn forget(&mut self, slot: u8) {
        let Self {
            spawned,
            drain_stopped,
            committed,
            finalized,
            finished: _,
        } = self;
        for field in [spawned, drain_stopped, committed, finalized] {
            *field &= !slot;
        }
    }

    fn has(field: u8, keys: &[ChildKey], child: ChildKey) -> bool {
        field & bit(keys, child) != 0
    }
}

struct Exploration {
    states: usize,
    transitions: usize,
}

/// Walks every reachable state of one (flavor, roster) configuration, calling
/// `check` once per transition.
fn explore(
    flavor: ScopeFlavor,
    roster: Roster,
    mut check: impl FnMut(&Transition<'_>),
) -> Exploration {
    assert!(roster.len() <= MAX_WIDTH, "the fingerprint encodes slots");
    let members = memberships(roster.len());
    let mut root = SupervisorState::new(flavor, ScopeLifecycle::starting());
    let keys: Vec<_> = roster
        .iter()
        .zip(&members)
        .map(|(&initial, membership)| admit(&mut root, *membership, initial))
        .collect();
    let alphabet = alphabet(&keys);
    let minted = root.keys.current();

    // The visited set is over the product of reducer state and path history;
    // see [`History`] for why that makes path properties checkable.
    let mut seen = HashSet::new();
    let history = History::default();
    seen.insert((fingerprint(&root, &keys), history));
    let mut frontier = VecDeque::new();
    frontier.push_back((root, history));
    let mut transitions = 0;
    let mut effects = Vec::new();
    while let Some((before, history)) = frontier.pop_front() {
        for event in &alphabet {
            let mut after = before.clone();
            effects.clear();
            event.apply(&mut after, &mut effects);
            transitions += 1;
            after.check_invariants();
            let history_after = history.after(&before, event, &after, &effects, &keys);
            check(&Transition {
                before: &before,
                event,
                after: &after,
                effects: &effects,
                keys: &keys,
                history: &history,
                history_after: &history_after,
            });
            // The key counter is left out of the fingerprint on the grounds
            // that only admission advances it, and admission is not in the
            // alphabet. That is the whole argument, so check it rather than
            // trust it.
            assert_eq!(after.keys.current(), minted, "the walk never admits");
            if seen.insert((fingerprint(&after, &keys), history_after)) {
                frontier.push_back((after, history_after));
            }
        }
    }
    Exploration {
        states: seen.len(),
        transitions,
    }
}

/// The configurations every property is checked against.
///
/// Both flavors, plus a dynamic roster carrying a runtime membership so R1's
/// initial-only aggregate has a counterexample available. There is
/// deliberately no ordered mixed roster: runtime admission is reachable only
/// through the dynamic surface, so an ordered scope with a runtime member is
/// not a state the library can produce, and asserting over it would pin
/// behavior nothing generates.
///
/// **Two children, not three, and that is not a budget compromise.** Every
/// property here is per-child, a fold over children, or cursor-versus-one-
/// child; the reducer has no rule that couples three memberships, so a third
/// adds combinations rather than cases. Measured, a third child cost 40x in
/// states and transitions, and no mutation
/// covering R1–R6, E4 or S3–S5 survives width two but falls to width three.
/// The one genuinely three-body distinction — `keys_after(child).next()`
/// against `.last()`, which needs a middle element to differ at all — is a
/// progress rule that no width detects; `ordered_startup_advances_through_every_initial_member_in_order`
/// and `ordered_stop_releases_one_child_per_join_in_reverse_order` own that
/// class. Widening is a one-line change here if the reducer grows a rule that
/// needs it, and [`MAX_WIDTH`] leaves the fingerprint room for it.
const CONFIGURATIONS: &[(ScopeFlavor, Roster)] = &[
    (ScopeFlavor::Ordered, &[true, true]),
    (ScopeFlavor::Dynamic, &[true, true]),
    (ScopeFlavor::Dynamic, &[true, false]),
];

fn explore_all(
    configurations: &[(ScopeFlavor, Roster)],
    mut check: impl FnMut(&Transition<'_>),
) -> Exploration {
    let mut total = Exploration {
        states: 0,
        transitions: 0,
    };
    for &(flavor, roster) in configurations {
        let run = explore(flavor, roster, &mut check);
        println!(
            "{flavor:?} {roster:?}: {} states, {} transitions",
            run.states, run.transitions
        );
        total.states += run.states;
        total.transitions += run.transitions;
    }
    total
}

fn incarnation(state: &SupervisorState, child: ChildKey) -> Option<IncarnationState> {
    state.child_state(child).map(ChildState::incarnation)
}

/// E4 — one authoritative membership/incarnation state. Phases advance only
/// along the documented edges, removal is monotone into `Removing`, and a key
/// leaves the roster only by reclaiming a joined child.
fn check_e4_authoritative_membership_and_incarnation_state(transition: &Transition<'_>) {
    for &child in transition.keys {
        let before = incarnation(transition.before, child);
        let after = incarnation(transition.after, child);
        let (Some(before_phase), Some(after_phase)) = (before, after) else {
            if before.is_some() && after.is_none() {
                assert_eq!(
                    before,
                    Some(IncarnationState::Joined),
                    "only a joined child can leave the roster"
                );
                assert!(
                    matches!(transition.event, Input::Step(Event::Reclaim { child: key }) if *key == child),
                    "only `Reclaim` removes a key, got {:?}",
                    transition.event
                );
            } else {
                assert_eq!(before, after, "a key cannot reappear");
            }
            continue;
        };
        let allowed: &[IncarnationState] = match before_phase {
            IncarnationState::Unstarted => &[IncarnationState::Active, IncarnationState::Disposing],
            IncarnationState::Active => &[IncarnationState::Stopping, IncarnationState::Complete],
            IncarnationState::Stopping => &[IncarnationState::Complete],
            IncarnationState::Complete => &[
                IncarnationState::RestartPending,
                IncarnationState::Disposing,
            ],
            IncarnationState::RestartPending => {
                &[IncarnationState::Active, IncarnationState::Disposing]
            }
            IncarnationState::Disposing => &[IncarnationState::Joined],
            IncarnationState::Joined => &[],
        };
        assert!(
            after_phase == before_phase || allowed.contains(&after_phase),
            "{:?} moved {before_phase:?} -> {after_phase:?}, which is not an edge of the \
                 incarnation state machine",
            transition.event
        );

        if transition.before.membership_status(child) == MembershipStatus::Removing {
            assert_eq!(
                transition.after.membership_status(child),
                MembershipStatus::Removing,
                "removal is monotone"
            );
        }
        assert!(
            !transition.before.spawned_once(child) || transition.after.spawned_once(child),
            "a spawn fact cannot be unlearned"
        );
    }
}

/// R5 — settlement effects must be acknowledgeable, and the effect stream can
/// only ever name work the shell can act on: a live key, a start the spawn
/// transition would accept, a stop for a child that has not joined, and a
/// removal finalization for a joined `Removing` membership.
fn check_r5_effects_are_acknowledgeable(transition: &Transition<'_>) {
    for effect in transition.effects {
        match effect {
            Effect::StartChild { child } => {
                let record = transition
                    .after
                    .children
                    .get(child)
                    .expect("a start effect names a live key");
                assert!(
                    record.startable(),
                    "a start effect outside `Event::Spawned`'s acceptance set re-derives \
                         from unchanged state and never lets settlement terminate"
                );
            }
            Effect::StopChild { child } | Effect::ForceChild { child } => {
                assert!(
                    transition.after.is_incomplete(*child),
                    "a stop effect names a child with work left to end"
                );
            }
            Effect::FinalizeRemoval { child } => {
                assert_eq!(
                    transition.after.membership_status(*child),
                    MembershipStatus::Removing,
                    "removal is finalized only for a removing membership"
                );
                assert!(
                    transition.after.joined(*child),
                    "removal is finalized only once disposal has joined"
                );
            }
            Effect::StartupCompleted { .. } | Effect::Finished { .. } => {}
        }
    }
}

/// R5 — a settlement pass that emits no acknowledgeable work is already at a
/// fixed point. Start effects are the one class the shell is expected to
/// consume, so re-settling reproduces exactly those and nothing else; any
/// other repeated effect would spin the driver's level-triggered loop.
fn check_r5_settlement_reaches_a_fixed_point(transition: &Transition<'_>) {
    if !matches!(transition.event, Input::Step(Event::Settle)) {
        return;
    }
    let mut again = transition.after.clone();
    let mut repeated = Vec::new();
    step(&mut again, Event::Settle, &mut repeated);
    let starts: Vec<_> = transition
        .effects
        .iter()
        .filter(|effect| matches!(effect, Effect::StartChild { .. }))
        .cloned()
        .collect();
    assert_eq!(
        repeated, starts,
        "re-settling an unchanged state must reproduce its start effects and nothing else"
    );
}

/// R3 — removal is sampled at the publication transition. The shell reduces a
/// fired latch as `RemovalSampled` before `Ready`; the sample marks the
/// membership `Removing`, and readiness is then rejected, so the removal path
/// can never manufacture the readiness edge it raced.
fn check_r3_removal_is_sampled_at_publication(transition: &Transition<'_>) {
    // The rule is stated over the membership, not over the event that latched
    // it: a record that reached `Removing` through any route rejects readiness
    // from then on.
    for &child in transition.keys {
        if transition.before.contains(child)
            && transition.before.membership_status(child) == MembershipStatus::Removing
        {
            assert!(
                !transition.after.initial_ready(child) || transition.before.initial_ready(child),
                "a removing membership never gains readiness, got {:?}",
                transition.event
            );
        }
    }

    let Input::Step(Event::RemovalSampled { child }) = transition.event else {
        return;
    };
    if !transition.after.contains(*child) {
        return;
    }
    assert_eq!(
        transition.after.membership_status(*child),
        MembershipStatus::Removing,
        "a latch sample marks the membership before readiness is considered"
    );
}

/// R1/R2/R6 — the startup aggregate is derived from initial memberships only,
/// readiness is monotone until a restart rearms it while the scope is
/// still `Starting`, and a completed startup never rewinds.
fn check_r1_r2_r6_startup_aggregate(transition: &Transition<'_>) {
    assert!(
        !transition.before.lifecycle().startup_complete()
            || transition.after.lifecycle().startup_complete(),
        "a completed startup never rewinds"
    );

    for &child in transition.keys {
        // Reclaim retires the whole record, which is R3's shrink of the
        // initial set rather than a readiness edge; E4 owns that transition.
        if !transition.after.contains(child) {
            continue;
        }
        let before = transition.before.initial_ready(child);
        let after = transition.after.initial_ready(child);
        if !before && after {
            assert!(
                matches!(transition.event, Input::Step(Event::Ready { child: key }) if *key == child),
                "only a readiness edge sets the aggregate bit, got {:?}",
                transition.event
            );
            // Naming the event is not enough: readiness is incarnation-local,
            // so the edge must also come from an executing incarnation. Without
            // this, a `Ready` accepted in `Complete` or `RestartPending` — R2's
            // own restart direction — is invisible to the walk, because E4
            // permits an unchanged phase and the aggregate bit is not a phase.
            assert!(
                matches!(
                    incarnation(transition.before, child),
                    Some(IncarnationState::Active | IncarnationState::Stopping)
                ),
                "readiness publishes only from an executing incarnation, got {:?}",
                incarnation(transition.before, child)
            );
        }
        if before && !after {
            assert!(
                matches!(
                    transition.event,
                    Input::Step(Event::RestartPending { child: key }) if *key == child
                ),
                "only a restart clears the aggregate bit, got {:?}",
                transition.event
            );
            assert!(
                transition.before.lifecycle().is_starting(),
                "a restart rearms the gate only while startup is in progress"
            );
        }
    }

    let completed = transition
        .effects
        .iter()
        .filter(|effect| matches!(effect, Effect::StartupCompleted { .. }))
        .count();
    assert!(completed <= 1, "the aggregate fires at most once per step");
    let unready = transition
        .before
        .children
        .values()
        .any(|record| matches!(record.startup, StartupMembership::Initial { ready: false }));
    let settling_a_starting_scope = matches!(transition.event, Input::Step(Event::Settle))
        && transition.before.lifecycle().is_starting();

    // The safety half of R6, universally: an unready initial member — and only
    // an initial one, which is R1 — always withholds the aggregate.
    if completed == 1 {
        assert!(
            settling_a_starting_scope && !unready,
            "startup completed with an unready initial member, or outside settlement"
        );
    }

    // The progress half. R6 states it as an "iff", which holds exactly for
    // dynamic scopes: ordered startup additionally waits for its cursor, and a
    // member latched for removal parks that cursor until the removal commits
    // even when every initial member is already ready. That is a deliberate
    // ordered-sequencing rule rather than a defect, but it is not what R6 says
    // — see `ordered_startup_waits_for_a_removing_member_to_commit`, which
    // pins the ordered behavior on its own.
    // Scoped to the cursor rather than to the whole roster: a member latched
    // for removal *behind* the cursor has already been passed and withholds
    // nothing, so exempting it would switch off a live property in states that
    // satisfy it anyway.
    let ordered_cursor_may_wait = transition.before.flavor() == ScopeFlavor::Ordered
        && transition
            .before
            .next_ordered_start()
            .is_some_and(|cursor| {
                transition.before.children.iter().any(|(&key, record)| {
                    key >= cursor && record.state.membership_status() == MembershipStatus::Removing
                })
            });
    if settling_a_starting_scope && !unready && !ordered_cursor_may_wait {
        assert_eq!(
            completed,
            1,
            "a settle that finds no unready initial member completes startup: {:?} {:?}",
            transition.before.flavor(),
            transition.before.children,
        );
    }
}

/// R4 — ordered start is one accepted edge at a time, and no settlement pass
/// ever asks for the same child twice.
fn check_r4_one_accepted_start_edge(transition: &Transition<'_>) {
    let starts: Vec<_> = transition
        .effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::StartChild { child } => Some(*child),
            _ => None,
        })
        .collect();
    let unique: HashSet<_> = starts.iter().copied().collect();
    assert_eq!(
        starts.len(),
        unique.len(),
        "a child is started at most once"
    );
    if transition.before.flavor() == ScopeFlavor::Ordered {
        assert!(
            starts.len() <= 1,
            "ordered startup exposes one start edge at a time, got {starts:?}"
        );
    }
}

/// S3/S4 — flavor owns stop sequencing, and the drain lattice only ever
/// climbs: a dynamic drain stops every incomplete child at once, an ordered
/// drain exposes one, and force sets the hard-force fact for all of them.
fn check_s3_s4_stop_sequencing_and_drain_lattice(transition: &Transition<'_>) {
    assert!(
        !transition.before.lifecycle().is_draining() || transition.after.lifecycle().is_draining(),
        "a drain never rewinds"
    );
    assert!(
        !transition.before.hard_forced() || transition.after.hard_forced(),
        "a hard force never rewinds"
    );
    if let (Some(before), Some(after)) = (
        transition.before.lifecycle().draining_reason(),
        transition.after.lifecycle().draining_reason(),
    ) {
        assert!(
            stop_reason_precedence(after) >= stop_reason_precedence(before),
            "the drain lattice never downgrades"
        );
    }

    let stops: HashSet<_> = transition
        .effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::StopChild { child } => Some(*child),
            _ => None,
        })
        .collect();
    let forces: HashSet<_> = transition
        .effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::ForceChild { child } => Some(*child),
            _ => None,
        })
        .collect();
    let incomplete: HashSet<_> = transition
        .after
        .keys()
        .filter(|&child| transition.after.is_incomplete(child))
        .collect();

    match transition.event {
        Input::Force => {
            assert!(transition.after.hard_forced());
            assert!(transition.after.lifecycle().is_draining());
            assert_eq!(
                forces, incomplete,
                "force reaches exactly the children with work left to end"
            );
        }
        Input::BeginDrain(_) if !transition.before.lifecycle().is_draining() => {
            assert!(transition.after.lifecycle().is_draining());
            if transition.before.flavor() == ScopeFlavor::Dynamic {
                assert_eq!(
                    stops, incomplete,
                    "a dynamic drain stops every incomplete child at once"
                );
            } else {
                assert!(
                    stops.is_empty(),
                    "an ordered drain sequences its stops through settlement"
                );
            }
        }
        Input::Step(Event::Settle) if transition.before.flavor() == ScopeFlavor::Ordered => {
            assert!(
                stops.len() <= 1,
                "ordered settlement exposes one incomplete child at a time, got {stops:?}"
            );
            if let Some(&child) = stops.iter().next() {
                assert_eq!(
                    transition.after.ordered_stop_waiting(),
                    Some(child),
                    "the ordered cursor waits on the child it just stopped"
                );
            }
        }
        _ => {}
    }
}

/// Removal effects are edge-triggered: each key is finalized at most once, and
/// a removal stops it at most once.
///
/// Cardinality is a path property and the walk visits states, not paths, so it
/// is stated over the state that records the issue. Every removal effect
/// leaves the key committed (`Removing`), a committed key never uncommits, and
/// `Removing(Joined)` — the state every finalize leaves behind — is absorbing
/// until `Reclaim` retires a key the walk never reuses. An emission from a
/// state that already records its effect is therefore exactly a second one.
fn check_t9_removal_effects_are_issued_once(transition: &Transition<'_>) {
    for &child in transition.keys {
        let (Some(before), after) = (
            transition.before.child_state(child),
            transition.after.child_state(child),
        ) else {
            continue;
        };
        if let ChildState::Removing(state) = before {
            assert!(
                after.is_none_or(|after| after == ChildState::Removing(after.incarnation())),
                "a committed removal never uncommits, got {after:?}"
            );
            if state == IncarnationState::Joined {
                assert!(
                    after.is_none_or(|after| after == before),
                    "a finalized removal is absorbing until reclaim, got {after:?}"
                );
            }
        }
    }
    for effect in transition.effects {
        match effect {
            Effect::FinalizeRemoval { child } => {
                assert_ne!(
                    transition.before.child_state(*child),
                    Some(ChildState::Removing(IncarnationState::Joined)),
                    "a removal is finalized once per key"
                );
                assert_eq!(
                    transition.after.child_state(*child),
                    Some(ChildState::Removing(IncarnationState::Joined)),
                    "a finalize records itself as a joined, committed removal"
                );
            }
            Effect::StopChild { child }
                if matches!(transition.event, Input::Step(Event::RemovalLatched { .. })) =>
            {
                assert!(
                    !matches!(
                        transition.before.child_state(*child),
                        Some(ChildState::Removing(_))
                    ),
                    "a removal command stops its child once per key"
                );
            }
            _ => {}
        }
    }
}

/// R4 along a path — the reducer asks for one accepted start per unspawned
/// initial member, and ordered startup reaches them in declaration order.
///
/// Start effects are level-triggered, so repeating one before its spawn is
/// expected (R5's fixed point reproduces it); what R4 forbids is a request
/// after the edge it asks for has been taken. Restarts are not the reducer's
/// to request: an incarnation that has spawned once is respawned by the
/// shell's restart path, so a start for it would be a second, unaccepted edge.
///
/// For ordered scopes R4 is stated over the cursor: "the settlement step emits
/// a start effect only for the current initial cursor, and advances the cursor
/// only past a spawned-and-ready member or a reclaimed slot, reaching every
/// initial member in declaration order" (§2: "readiness-gated startup in
/// declaration order"). Declaration order along the path follows from the
/// cursor only ever moving forward, so the ordering is checked per transition
/// and needs no history — which matters, because "was ready when the cursor
/// passed" is not recoverable from a later state once a restart re-arms the
/// bit, and carrying it as history cost the walk half again its states.
fn check_r4_start_effects_along_a_path(transition: &Transition<'_>) {
    let keys = transition.keys;
    if matches!(transition.event, Input::Step(Event::Settle))
        && transition.before.lifecycle().is_starting()
    {
        // Derive the eligible frontier from the input state, not from the
        // emitted effects or the cursor left by settlement. An empty effect
        // list must not make a stalled startup look like a fixed point.
        let ordered_frontier = transition.before.next_ordered_start().and_then(|cursor| {
            keys.iter().copied().find(|&child| {
                child >= cursor
                    && transition.before.contains(child)
                    && (transition.before.membership_status(child) == MembershipStatus::Removing
                        || !History::has(transition.history.spawned, keys, child)
                        || !transition.before.initial_ready(child))
            })
        });
        for &child in keys {
            let eligible = transition.before.is_initial(child)
                && !History::has(transition.history.spawned, keys, child)
                && transition.before.child_state(child)
                    == Some(ChildState::Resident(IncarnationState::Unstarted))
                && (transition.before.flavor() == ScopeFlavor::Dynamic
                    || ordered_frontier == Some(child));
            let starts = transition
                .effects
                .iter()
                .filter(
                    |effect| matches!(effect, Effect::StartChild { child: key } if *key == child),
                )
                .count();
            assert_eq!(
                starts,
                usize::from(eligible),
                "settlement must request exactly one start for each eligible initial member (R4): {child:?}"
            );
        }
    }
    for effect in transition.effects {
        let Effect::StartChild { child } = effect else {
            continue;
        };
        assert!(
            transition.before.is_initial(*child),
            "the reducer starts initial members only; runtime admissions are started by the \
             shell, got a start for {child:?}"
        );
        assert!(
            !History::has(transition.history.spawned, keys, *child),
            "a start was requested for {child:?} after its spawn edge was accepted: one \
             accepted start per member (R4)"
        );
        if transition.before.flavor() == ScopeFlavor::Ordered {
            assert_eq!(
                transition.after.next_ordered_start(),
                Some(*child),
                "ordered startup starts only the member under its cursor (R4)"
            );
        }
    }

    let (from, to) = (
        transition.before.next_ordered_start(),
        transition.after.next_ordered_start(),
    );
    if from == to {
        return;
    }
    let from = from.expect("the ordered start cursor never rewinds from exhaustion (R4)");
    assert!(
        to.is_none_or(|to| to > from),
        "the ordered start cursor moved {from:?} -> {to:?}, which is not forward (R4)"
    );
    for &passed in keys
        .iter()
        .filter(|&&key| key >= from && to.is_none_or(|to| key < to))
    {
        // A reclaimed slot is passed freely; `is_initial` is false for it.
        if !transition.before.is_initial(passed) {
            continue;
        }
        assert!(
            History::has(transition.history.spawned, keys, passed)
                && transition.before.initial_ready(passed),
            "the ordered start cursor passed {passed:?} before it had spawned and published \
             readiness (R4), via {:?}",
            transition.event
        );
    }
}

/// S3 along a path — a drain stops each child once, and an ordered drain
/// stops them in reverse declaration order, one at a time.
///
/// Dynamic drain entry "emits one stop per incomplete child"; ordered
/// settlement "exposes at most one incomplete child and does not advance
/// until it joins" (S3), which §11 states as "reverse declaration order, one
/// at a time … the cursor child is aborted *and joined* … before the ladder
/// advances to the next sibling". The per-transition S3 check sees one pass;
/// this one sees the sequence. Removal stops are a separate source with their
/// own once-per-key rule (see [`check_t5_removal_effects_along_a_path`]).
///
/// A reclaimed key's stop is forgotten with the key (see [`History::after`]).
/// It had joined to be reclaimed, so it has already met the one-at-a-time
/// rule; only the order comparison against it is given up.
fn check_s3_drain_stops_along_a_path(transition: &Transition<'_>) {
    if StopSource::of(transition.event) != StopSource::Drain {
        return;
    }
    let keys = transition.keys;
    let mut history = *transition.history;
    for effect in transition.effects {
        if let Effect::StopChild { child } = effect {
            assert!(
                !History::has(history.drain_stopped, keys, *child),
                "a drain stopped {child:?} twice along one path (S3), via {:?}",
                transition.event
            );
            if transition.before.flavor() == ScopeFlavor::Ordered {
                for &stopped in keys {
                    if !History::has(history.drain_stopped, keys, stopped) {
                        continue;
                    }
                    assert!(
                        stopped > *child,
                        "ordered teardown stopped {child:?} after {stopped:?}, which is not \
                         reverse declaration order (S3, §11)"
                    );
                    assert!(
                        !transition.before.is_incomplete(stopped),
                        "ordered teardown stopped {child:?} before the previously stopped \
                         {stopped:?} had joined (S3, §11)"
                    );
                }
            }
        }
        history.issue(effect, transition.event, keys);
    }
}

/// T5 along a path — each key's removal commits once, is finalized once, is
/// finalized only after it commits, and is finalized as soon as a committed
/// key has joined.
///
/// This restates [`check_t9_removal_effects_are_issued_once`] without its
/// premise: that check derives cardinality from `Removing(Joined)` being
/// absorbing, so it is only as good as the reducer's state encoding. This one
/// counts the effects themselves. The pairing is the contract on
/// [`Effect::FinalizeRemoval`] — "whichever of the commit and the terminal join
/// comes second emits it" — and SPEC T5's "once sampled, the state stays
/// `Removing` through stop, terminalization, finalization, and reclaim"; R3
/// puts reclaim after the `Removed` publication that finalization drives, so a
/// committed key must never join without its finalize.
fn check_t5_removal_effects_along_a_path(transition: &Transition<'_>) {
    let keys = transition.keys;
    // Observe acceptance independently of the effect under test. In
    // particular, committing an already joined child must finalize it even
    // if no earlier stop taught the history that a removal was pending.
    for &child in keys {
        if matches!(
            transition.before.child_state(child),
            Some(ChildState::Resident(_) | ChildState::RemovalSampled(_))
        ) && matches!(
            transition.after.child_state(child),
            Some(ChildState::Removing(_))
        ) {
            let expected = if transition.after.joined(child) {
                Effect::FinalizeRemoval { child }
            } else {
                Effect::StopChild { child }
            };
            assert_eq!(
                transition
                    .effects
                    .iter()
                    .filter(|effect| **effect == expected)
                    .count(),
                1,
                "an accepted removal commit must issue its effect: {expected:?}"
            );
        }
    }
    let committing = matches!(transition.event, Input::Step(Event::RemovalLatched { .. }));
    let mut history = *transition.history;
    for effect in transition.effects {
        // A commit's effect is its stop, or its finalize when the child has
        // already joined; either way it is the key's one commit.
        if committing
            && let Effect::StopChild { child } | Effect::FinalizeRemoval { child } = effect
        {
            assert!(
                !History::has(history.committed, keys, *child),
                "a removal of {child:?} committed twice along one path"
            );
        }
        if let Effect::FinalizeRemoval { child } = effect {
            assert!(
                !History::has(history.finalized, keys, *child),
                "a removal of {child:?} was finalized twice along one path, via {:?}",
                transition.event
            );
            assert!(
                committing || History::has(history.committed, keys, *child),
                "a removal of {child:?} was finalized before its command committed, via {:?}",
                transition.event
            );
        }
        history.issue(effect, transition.event, keys);
    }
    for &child in keys {
        if History::has(transition.history_after.committed, keys, child)
            && transition.after.joined(child)
        {
            assert!(
                History::has(transition.history_after.finalized, keys, child),
                "a committed removal of {child:?} joined without being finalized, via {:?}",
                transition.event
            );
        }
    }
}

/// S5 along a path — the finish effect is issued at most once.
///
/// S5: "the finish effect is emitted once". The per-transition S5 check reads
/// `finish_emitted` to say so, which is only as good as that flag: a
/// transition that cleared it would clear the evidence with it. This bounds the
/// edge per path from the effects themselves.
///
/// R6's "emitted at most once" needs no counterpart here: the per-transition
/// R6 check already confines completion to a settle of a `Starting` scope and
/// forbids a completed startup from rewinding, so a second completion along
/// any path fails one of those first.
fn check_s5_finish_issues_once(transition: &Transition<'_>) {
    let mut history = *transition.history;
    for effect in transition.effects {
        if let Effect::Finished { .. } = effect {
            assert!(
                !history.finished,
                "the scope finished twice along one path (S5), via {:?}",
                transition.event
            );
        }
        history.issue(effect, transition.event, transition.keys);
    }
}

/// S5 — completion is derived and level-triggered: `all_children_joined`
/// agrees with the per-child fold at every reachable state, and `Finished` is
/// emitted once, only against that derived value.
fn check_s5_derived_level_triggered_completion(transition: &Transition<'_>) {
    assert_eq!(
        transition.after.all_children_joined(),
        transition
            .after
            .keys()
            .all(|child| transition.after.joined(child)),
        "the derived completion query cannot drift from child state"
    );
    let finished = transition
        .effects
        .iter()
        .any(|effect| matches!(effect, Effect::Finished { .. }));
    if finished {
        assert!(
            !transition.before.finish_emitted,
            "the finish edge is published once"
        );
        assert!(
            transition.after.all_children_joined(),
            "a scope finishes only once every child has joined"
        );
        // The other direction of S5's "iff". Stated over the lifecycle rather
        // than by calling `finish_if_ready`, which would restate the body of
        // the code under test: a scope that is neither draining nor a non-empty
        // ordered workload has nothing to finish.
        assert!(
            transition.after.lifecycle().is_draining()
                || (transition.after.flavor() == ScopeFlavor::Ordered
                    && transition.after.lifecycle().startup_complete()
                    && !transition.after.is_empty()),
            "only a draining scope, or a running non-empty ordered one, finishes"
        );
    }
    // And its liveness half: settlement is level-triggered, so a drained scope
    // whose children have all joined must publish on the very next settle
    // rather than wait for an edge that is not coming.
    if matches!(transition.event, Input::Step(Event::Settle))
        && !transition.before.finish_emitted
        && transition.before.lifecycle().is_draining()
        && transition.after.all_children_joined()
    {
        assert!(
            finished,
            "a drained scope whose children have all joined finishes"
        );
    }
}

/// Walks every reachable reducer state of every configuration, asserting the
/// reducer-expressible half of SPEC §15.3's invariant list at every transition.
///
/// Exploring states rather than event schedules visits each reachable state
/// once instead of once per schedule that reaches it, which is what makes room
/// for the wider alphabet the drain, force, restart and reclaim rules need to
/// be reachable at all.
#[test]
fn exhaustive_reachable_states_preserve_the_reducer_invariants() {
    let run = explore_all(CONFIGURATIONS, check_every_invariant);
    println!(
        "explored {} states over {} transitions",
        run.states, run.transitions
    );
}

fn check_every_invariant(transition: &Transition<'_>) {
    check_e4_authoritative_membership_and_incarnation_state(transition);
    check_r5_effects_are_acknowledgeable(transition);
    check_r5_settlement_reaches_a_fixed_point(transition);
    check_r3_removal_is_sampled_at_publication(transition);
    check_r1_r2_r6_startup_aggregate(transition);
    check_r4_one_accepted_start_edge(transition);
    check_s3_s4_stop_sequencing_and_drain_lattice(transition);
    check_s5_derived_level_triggered_completion(transition);
    check_t9_removal_effects_are_issued_once(transition);
    check_r4_start_effects_along_a_path(transition);
    check_s3_drain_stops_along_a_path(transition);
    check_t5_removal_effects_along_a_path(transition);
    check_s5_finish_issues_once(transition);
}
