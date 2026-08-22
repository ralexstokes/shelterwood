# The life of a child

This chapter follows a child from declaration through startup, exit, and
restart — the supervision loop that is the library's reason to exist. The
through-line is SPEC §15.3's pure-decision-core rule: every decision on
this path is made by a synchronous reducer in `shelterwood-core` fed with
sampled events, and the driver shell merely executes the effects the
reducer returns — the contract the
[previous chapter](internals-reducer.md) maps.

## Declaration

`Tree` and `DynamicTree` (`crates/shelterwood/src/tree/builders.rs`) wrap
a shared `BuilderCore` (`plan.rs`). Reserving or adding a child mints its
`Membership` immediately from the parent's scope identity and creates a
`SlotCell` — which is why cyclic wiring works: `ActorRef` handles exist
at declaration time, before anything runs. Attaching an actor definition
is also the moment its `MailboxCell<M>` is created and paired with the
runtime capability object (`tree/slots.rs`).

`Tree::spawn` first checks that a runtime is ambient (else
`BuildError::NoRuntime`), then *lowers* the declaration:
`BuilderCore::lower` resolves every slot's policy against scope defaults,
rejects unfilled reservations, reconciles memberships when a subtree is
adopted, and produces an owned `ScopePlan` of `ChildPlan`s. Both the
builder and the plan carry a terminality obligation, so an abandoned plan
terminalizes every never-started member rather than leaking them.

## Startup

`driver::spawn_system` spawns `run_scope` as the scope's driver task
(plus a root-join watchdog) and hands back `System`. Inside, per child:

1. `ChildRuntime::from_plan` arms a fail-closed `ChildTerminality`
   obligation *before any fallible step*, takes the member's incarnation
   counter, and — for actors — configures the mailbox to obtain the first
   `MailboxBindToken` (a linear, non-clonable permission to bind exactly
   one incarnation).
2. The child is admitted into the core reducer's `SupervisorState`,
   yielding a `ChildKey`, and the resident projection is installed into
   the `ScopeCell`, publishing `Added` lifecycle edges.
3. Settlement runs the reducer, which emits `StartChild` for the first
   ordered child — or for every child of a dynamic scope at once.
4. `spawn_child` (`driver/child.rs`) mints an `Incarnation`, builds the
   body for the child's kind (raw actor, task, or nested scope), binds
   the mailbox — which promotes anything parked during the rebind window
   — publishes `Started`, and spawns up to four tasks: the body (wrapped
   in panic capture, ending by recording its outcome), the exit joiner, a
   readiness watcher, and a local-stop watcher.

Readiness is a core state machine (`ReadinessGate`). A readiness signal
becomes a `Ready` lifecycle edge and, in an ordered scope, advances the
startup cursor so the reducer emits the next `StartChild`; the aggregate
completion becomes the startup result `System::wait_started` observes on
the scope record's watch channel. Readiness deadlines feed the driver's
single deadline queue, and a timeout stops the child with a recorded
readiness-timeout outcome.

## Exit capture

Exits follow SPEC §15.3's E1 rule — record, destroy, join, publish:

- The body task owns a `ReportToken`, an obligation whose `Drop` records
  the fallback outcome, so a killed task still reports. A normal return
  records `Completed` or `Failed`; a panic is captured, the incarnation's
  teardown runs in SPEC §6.5 order (freeze mailbox, freeze resources,
  join offloads, drop context, drop actor state) with cleanup panics
  subordinated to the primary, and the primary is resumed so the runtime
  join observes it.
- Recording wraps the outcome in `RetainedRecordedOutcome`: a `Failed`
  outcome owns a type-erased user error, and retention keeps its eventual
  destruction off framework-critical paths (see
  [Locks, effects, and disposal](internals-concurrency.md)).
- The joiner task awaits the runtime join, claims the report, and sends
  `ChildEvent::Exited` — recorded outcome, join outcome, sampled
  cancellation, readiness evidence — down the scope's primary event lane.

## Classification and arbitration

The driver loop collects its event lanes (child events, dynamic control,
disposal completions) and stably sorts the batch by `ArbitrationClass` —
shutdown outranks removal outranks exits outranks readiness, and so on —
so one wake processes concurrent facts in a deterministic order.

For an exit, `handle_exit` replays readiness ordering, closes the mailbox
(keeping the next bind token for a restart and sending unread messages to
detached disposal), then folds the recorded outcome with the join outcome
through the retaining classifiers. Verdict precedence is total and
table-driven — a panic outranks a readiness timeout outranks a failure
outranks an abort outranks completion — with the one deliberate
asymmetry that a recorded `Failed` survives a later abort while a
recorded `Completed` does not. The result is the `Exit` observers see.

The decision itself is `dispatch_exit` in core: given the exit, the
child's restart policy, the scope mode, and the membership status, it
returns `ScheduleRestart` or `Terminal`. A draining scope or a removing
membership always forces `Terminal` — restart suppression is derived
from state, never from a flag.

## Restart

`schedule_restart` (core) bumps the attempt counters, computes the
backoff delay from validated policy data plus a pre-drawn jitter sample,
charges the scope's intensity window, and returns a `RestartDecision`.
The scope publishes `Exited` and `RestartScheduled` in one observation
transaction and arms a restart deadline; when it fires, `spawn_child`
runs again — a new `Incarnation`, the same `MailboxCell` re-bound with
the recycled token, parked senders promoted. If the decision tripped the
intensity budget instead, the scope enters drain with
`StopReason::IntensityTripped`, which escalates to its parent as an
ordinary child exit.

## Runtime admission

A dynamic scope admits children at runtime through the same machinery,
with a control-plane protocol in front of it
(`crates/shelterwood/src/driver/admission_control.rs`). The public
surface on `DynamicScopeRef` — `add_actor`, `reserve_actor`, and their
kind siblings — splits into a synchronous *reservation* that claims the
child id immediately (so a handle can exist before admission resolves,
and a duplicate id fails fast with a `ReserveError`) and an
`Admission` future that resolves when the driver actually admits.

The driver samples admission requests as its lowest-arbitration-class
pending work. `handle_admission` first re-checks liveness — a draining
scope, a failed startup, or a superseded scope incarnation rejects with
the matching `NotAdmittingCause` — then builds the `ChildRuntime`
*outside* the control-plane lock, because conversion can unwind while
acquiring identity or configuring the mailbox, and driver teardown must
still be able to close reservations. The install itself is one
observation-gate transaction holding the dynamic-state mutex: the
reservation is re-matched, arena insertion and entry promotion happen as
a single transition (so an exact remover sees either the reservation or
a resident, never an unindexed intermediate), and
`admit_child_locked` pushes the incoming projection into residency
*before* any remaining fallible step. If bookkeeping panics after that
push, the graph is owned by residency and retires at scope clear as an
*unannounced* resident — the same rule that keeps snapshots'
`Added`/`Removed` pairing exact
([Observation from the inside](internals-observation.md)). Every
rejection path routes the definition or built child through detached
disposal rather than dropping it in place.

Admitting a *subtree* adds the gate handoff: the adoptee's observation
gate is taken while the parent's is held, the one sanctioned doubled
gate section and the deepest point of that exemption
([Locks, effects, and disposal](internals-concurrency.md)). Removal —
the other half of dynamic membership — is covered with the rest of
teardown in [Shutdown from the inside](internals-shutdown.md).

## Terminal

A terminal exit first retains the exit (`RetainedExit`) and hands the
child's retained construction to the disposal lane; its completion comes
back as a disposal event so destructor panics are folded into the
verdict before terminalization. Terminalizing writes the member's
terminal stage and last exit, and prepares mailbox termination: every
parked sender wakes with `Terminated`, and unread payloads leave for
detached disposal. A pre-ready failure becomes a startup failure — an
ordered scope terminalizes its never-started suffix and a nested scope
begins rollback — and the retention option then decides whether the
terminal membership is pruned immediately or kept resident as a
tombstone.
