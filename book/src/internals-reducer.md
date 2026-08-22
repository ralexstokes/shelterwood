# The decision core and its effects

SPEC §15.3 mandates a pure decision core: `step(state, event) -> effects`,
with no awaits, clock reads, channel operations, or spawns in any decision
module. This chapter maps how that mandate is realized, because the later
flow chapters lean on it constantly — "the reducer emits `StartChild`" is
shorthand for the machinery described here.

## A federation of machines

There is no single monolithic reducer. The decision layer is a family of
small pure machines in `shelterwood-core`, composed by the driver:

- **`SupervisorState`** (`supervisor.rs`) owns *structure*: membership
  records, startup gating, ordered-stop sequencing, and finish
  derivation. Per child it holds only a `ChildState` — `Resident` or
  `Removing`, each wrapping an incarnation state
  (`Unstarted → Active → Stopping → Complete → RestartPending |
  Disposing → Joined`) — plus its startup membership and a spawned-once
  bit. It embeds a **`ScopeLifecycle`** (`engine.rs`) for the scope's own
  Starting → Running → Draining → Stopped arc and the stop-reason
  lattice.
- Its siblings in `engine.rs` are consumed by the driver directly:
  **`StopLadder`** (per-child stop escalation over time),
  **`ReadinessGate`**, **`IntensityState`** with `schedule_restart`,
  **`dispatch_exit`** (restart-vs-terminal), **`arbitrate`** (the
  event-class ordering), and **`DeadlineQueue`**.

The division of labor: the supervisor decides *that* a child starts or
stops and in what order; the ladder decides *how a stop escalates* as
time passes; `dispatch_exit` decides a child's fate; the driver owns
every venue — tasks, mailboxes, deadlines, publication. The ordering
obligations *between* machines (readiness replay before exit handling,
force owning stop arbitration) live in driver code and the arbitration
table; the exhaustive reachable-state exploration suite under
`supervisor.rs` pins safety on the structural machine alone, and targeted
tests carry the cross-machine progress properties.

## The step contract

The entry point is `step(&mut SupervisorState, Event, &mut Vec<Effect>)`.
Three properties of that signature carry most of the design:

**Events are sampled facts, not live handles.** `Event::Ready` carries
the removal latch's value *at step entry* — the reducer never reads the
latch itself. Clocks arrive as pre-computed deadlines, randomness as a
pre-drawn jitter sample, exits as an already-classified verdict. The
`Exit` payload never enters `SupervisorState` at all, which is why the
state is plain `Clone + Debug` data holding no user values, and why every
time-of-check question is confined to the one sampling pass at the top of
the driver loop.

**Effects are six plain-data commands** — `StartChild`, `StopChild`,
`ForceChild`, `FinalizeRemoval`, `StartupCompleted`, `Finished` —
appended to a caller-owned vector. They are `Eq` and `Debug`, so tests
drive events in and assert effect sequences out with no runtime anywhere.

**Every transition is total.** State changes go through an
expected-states check (`transition_incarnation`), and a stale or
out-of-order event is a silent no-op — SPEC §15.3's E4. That is what
makes the driver's event lanes safe to arbitrate and replay without the
reducer trusting their ordering. Missing keys fail *closed*: querying a
reclaimed child answers `Removing`, the status in which nothing
schedules.

## The settle loop

`Event::Settle` is level-triggered: it recomputes startup progress, the
ordered-stop cursor, and the finish predicate from current state, rather
than reacting to any particular edge. The driver runs it to a fixed
point — reduce `Settle`, execute the emitted effects, repeat until a pass
emits nothing (`settle_supervisor` in `driver.rs`). Effect execution is a
trampoline: executing `StartChild` runs `spawn_child`, which feeds
`Event::Spawned` back through the reducer, appending any new effects to
the same vector for the next drain.

Termination rests on a discipline the code names explicitly: **every
emitted effect must be acknowledgeable** — SPEC §15.3's R5. The settle
pass emits `StartChild` only for a child in exactly the states
`Event::Spawned` accepts (`ChildRecord::startable`, documented as "this
event's own acceptance set"). The failure mode this prevents is sharp:
because settlement is level-triggered, an effect the shell would decline
is not merely wasted — it is re-derived from unchanged state on every
pass, forever, and the loop spins. The reducer therefore never asks for
construction it could not observe the result of.

On top of the level-triggered derivation sit edge latches for emissions
that must happen once: completion is *derived* — `all_children_joined` is
a fold over child states, deliberately not a counter that could drift —
but *emitted* exactly once, guarded by `finish_emitted` (SPEC S5).

## Two output channels

Not every decision comes back as an `Effect`. `begin_drain`, `force`, and
`fail_startup` return the state transition directly to the caller —
`(startup_pending, ScopeState)` — instead of queueing it. The convention:
an output that must *publish atomically* with the caller's already-open
observation transaction (the `Draining` edge commits together with its
terminal-disposal intents) rides the return value; a command the shell
executes *afterwards* rides the vector. When adding a transition, decide
which channel it belongs to by that coupling, not by convenience.

## Removal, and purity under the lock rule

Two smaller shapes are worth knowing before editing this code:

- Removal is not a boolean beside the state. `ChildState::Removing`
  wraps the incarnation state, and the only method across the boundary
  goes one way — removal is monotone by construction, with no path back
  to `Resident`. Two events split the sampling protocol:
  `RemovalLatched` records the fact *and* queues the stop command;
  `RemovalSampled` records the fact without replaying the command, for
  the paths where the command was queued separately.
- The purity mandate reaches into error handling. `admit` refuses an
  impossible key-domain collision by returning `None` rather than
  asserting, because admission callers can hold framework locks while
  retaining a user construction — a pure reducer must never diagnose a
  broken invariant by unwinding through them. The lock rule
  ([Locks, effects, and disposal](internals-concurrency.md)) constrains
  even what a pure function may do on input it believes unreachable.
