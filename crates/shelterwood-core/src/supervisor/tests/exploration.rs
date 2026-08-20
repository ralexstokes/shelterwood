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
//! list: R1–R6 (§6.1), E4 (§7.1), and S3–S5 (§10.1). The rest of the list —
//! the stop ladder (S1/S2), the sampled latches (S6), driver death (S7), the
//! exit funnel (E1–E3/E5/E6), and tree lowering (T1–T7) — is not stated over
//! `SupervisorState`, so it is not claimable here and is left to the engine
//! and integration suites that already own it.

use std::collections::{HashSet, VecDeque};

use crate::{
    MembershipStatus, ScopeFlavor, StopReason,
    engine::ScopeLifecycle,
    supervisor::{
        ChildKey, ChildRecord, ChildState, Effect, Event, IncarnationState, StartupMembership,
        SupervisorState, step,
    },
};

use super::{admit, memberships};

/// The widest roster a fingerprint can encode; see [`fingerprint`].
const MAX_WIDTH: usize = 4;

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
        let removing = state.membership_status() == MembershipStatus::Removing;
        let packed = 1
            | incarnation << 1
            | u64::from(removing) << 4
            | u64::from(initial) << 5
            | u64::from(ready) << 6
            | u64::from(spawned_once) << 7;
        word |= packed << (8 * slot(key));
    }

    let cursor = |key: &Option<ChildKey>| key.map_or(7, slot);
    let (lifecycle_state, lifecycle_reason) = lifecycle.fingerprint();
    // The exhaustive match in `ScopeLifecycle::fingerprint` forces a new
    // variant to choose a projection, but not to choose one that fits: a value
    // of 8 would alias into the flavor bit and prune the space silently.
    assert!(lifecycle_state < 8 && lifecycle_reason < 8, "3-bit fields");
    word |= u64::from(lifecycle_state) << (8 * MAX_WIDTH);
    word |= u64::from(lifecycle_reason) << (8 * MAX_WIDTH + 3);
    word |= u64::from(*flavor == ScopeFlavor::Ordered) << (8 * MAX_WIDTH + 6);
    word |= u64::from(*hard_forced) << (8 * MAX_WIDTH + 7);
    word |= u64::from(*finish_emitted) << (8 * MAX_WIDTH + 8);
    word |= cursor(next_ordered_start) << (8 * MAX_WIDTH + 9);
    word |= cursor(ordered_stop_cursor) << (8 * MAX_WIDTH + 12);
    word |= cursor(ordered_stop_waiting) << (8 * MAX_WIDTH + 15);
    word
}

/// The roster a walk starts from: one entry per child, `true` for an initial
/// membership and `false` for a runtime one.
///
/// Admission is deliberately not in the alphabet. Every admission mints a
/// fresh key, so an explorable `Admit` would make the state space infinite;
/// fixing the roster up front keeps it finite while still covering the
/// initial/runtime distinction R1 is stated over.
type Roster = &'static [bool];

/// Every event the walk offers in every state, including the ones the state
/// will reject — totality is a property under test, not a precondition.
fn alphabet(keys: &[ChildKey]) -> Vec<Event> {
    let mut events = Vec::new();
    for &child in keys {
        events.extend([
            Event::Spawned { child },
            Event::Ready {
                child,
                removal_latched: false,
            },
            Event::Ready {
                child,
                removal_latched: true,
            },
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
    events.extend([
        Event::FailStartup,
        // Two drain reasons at opposite ends of the precedence lattice, so
        // both the upgrading and the ignored direction of S4 are reachable.
        Event::BeginDrain {
            reason: StopReason::Finished,
        },
        Event::BeginDrain {
            reason: StopReason::ShutdownRequested,
        },
        Event::Force,
        Event::Settle,
    ]);
    events
}

struct Transition<'a> {
    before: &'a SupervisorState,
    event: &'a Event,
    after: &'a SupervisorState,
    effects: &'a [Effect],
    keys: &'a [ChildKey],
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

    let mut seen = HashSet::new();
    seen.insert(fingerprint(&root, &keys));
    let mut frontier = VecDeque::new();
    frontier.push_back(root);
    let mut transitions = 0;
    let mut effects = Vec::new();
    while let Some(before) = frontier.pop_front() {
        for event in &alphabet {
            let mut after = before.clone();
            effects.clear();
            step(&mut after, event.clone(), &mut effects);
            transitions += 1;
            after.check_invariants();
            check(&Transition {
                before: &before,
                event,
                after: &after,
                effects: &effects,
                keys: &keys,
            });
            // The key counter is left out of the fingerprint on the grounds
            // that only admission advances it, and admission is not in the
            // alphabet. That is the whole argument, so check it rather than
            // trust it.
            assert_eq!(after.keys.current(), minted, "the walk never admits");
            if seen.insert(fingerprint(&after, &keys)) {
                frontier.push_back(after);
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
/// adds combinations rather than cases. Measured, a third child costs 40x
/// (1.4M states and 53M transitions against 49k and 1.3M) and no mutation
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
                    matches!(transition.event, Event::Reclaim { child: key } if *key == child),
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
    if !matches!(transition.event, Event::Settle) {
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

/// R3 — removal is sampled at the publication transition. A true sample marks
/// the membership `Removing` first, and readiness is then rejected, so the
/// removal path can never manufacture the readiness edge it raced.
fn check_r3_removal_is_sampled_at_publication(transition: &Transition<'_>) {
    // The rule is stated over the membership, not over the event that latched
    // it: a record that reached `Removing` through any route rejects readiness
    // from then on. Checking only the latched `Ready` variant would leave the
    // `RemovalSampled`-then-`Ready { removal_latched: false }` pair — which the
    // alphabet offers in every such state — asserted about by nothing.
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

    let Event::Ready {
        child,
        removal_latched: true,
    } = transition.event
    else {
        return;
    };
    if !transition.after.contains(*child) {
        return;
    }
    assert_eq!(
        transition.after.membership_status(*child),
        MembershipStatus::Removing,
        "a true latch sample marks the membership before readiness is considered"
    );
    assert!(
        !transition.after.initial_ready(*child) || transition.before.initial_ready(*child),
        "a latched readiness edge cannot join the startup aggregate"
    );
}

/// R1/R2/R6 — the startup aggregate is derived from initial memberships only,
/// readiness is monotone until a restart rearms it while startup is
/// incomplete, and a completed startup never rewinds.
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
                matches!(transition.event, Event::Ready { child: key, .. } if *key == child),
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
                    Event::RestartPending { child: key } if *key == child
                ),
                "only a restart clears the aggregate bit, got {:?}",
                transition.event
            );
            assert!(
                !transition.before.lifecycle().startup_complete(),
                "a restart cannot rearm the gate once startup has completed"
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
    let settling_a_starting_scope =
        matches!(transition.event, Event::Settle) && transition.before.lifecycle().is_starting();

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
            crate::stop_reason_precedence(after) >= crate::stop_reason_precedence(before),
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
        Event::Force => {
            assert!(transition.after.hard_forced());
            assert!(transition.after.lifecycle().is_draining());
            assert_eq!(
                forces, incomplete,
                "force reaches exactly the children with work left to end"
            );
        }
        Event::BeginDrain { .. } if !transition.before.lifecycle().is_draining() => {
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
        Event::Settle if transition.before.flavor() == ScopeFlavor::Ordered => {
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
    if matches!(transition.event, Event::Settle)
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
/// reducer-expressible half of SPEC's invariant list at every transition.
///
/// This replaces an enumeration of event *schedules* (8!/9! permutations over
/// a four-event alphabet). Exploring states rather than orderings visits each
/// reachable state once instead of once per schedule that reaches it, which is
/// what makes room for the wider alphabet the drain, force, restart and
/// reclaim rules need to be reachable at all.
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
}
