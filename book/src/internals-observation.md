# Observation from the inside

The [observation chapter](observation.md) covers the two user-facing
contracts: conflating snapshots and bounded lifecycle streams. Both are
produced by one publication path on the `ScopeCell`, and the machinery
underneath exists to make every published value a *consistent cut* of the
resident tree — SPEC §14's central demand.

## The gate

The observation gate (`crates/shelterwood/src/cells/gate.rs`) is a shared
mutex-of-unit: one critical section for one resident tree's entire
observation projection. Tree membership is defined by gate *identity* —
two cells are in the same tree iff they share the gate `Arc` — which is
what lets a subtree be adopted by re-homing its gate pointer. That
re-homing (`MemberCell::with_handoff_gate`) is the single sanctioned
gate-under-gate acquisition: parent-to-child direction only, re-reading
the installed pointer under the acquired guard so a concurrent handoff
retries instead of deadlocking.

The gate deliberately tolerates poisoning: a panic on an observation path
must not permanently wedge later observation or a handoff.

## The transaction

Nothing mutates observation state with the bare guard. Every writer works
through `ObservationTxn`: the guard plus a deferred-effect list. `defer`
queues arbitrary post-unlock work, `pulse` queues a watch wake (the
runtime invokes registered wakers synchronously, so a pulse under the
gate would run user waker code inside the tree's critical section), and
`stage_snapshot` queues a publication. `commit` installs staged snapshots
*first*, then drops the guard, then runs the effect list under a panic
accumulator — and the transaction's `Drop` runs the same path, so an
unwind cannot strand already-committed wakes. The install-before-unlock
order is load-bearing: released first, two transactions could interleave
and install a stale cut over a newer one.

Because every retained control-plane writer takes `&mut ObservationTxn`,
an out-of-transaction mutation is unavailable by construction — the
token *is* the rule.

## Lifecycle events

Lifecycle streams are per-subscriber bounded rings fed by the scope's
`LifecycleHub`. The event kinds are the edges the model promises —
`Added`, `Started`, `Ready`, `Exited`, `RestartScheduled`, `Removed`,
`ScopeState` — each stamped with the emitting scope's membership and a
monotone, membership-owned sequence number that is continuous across
restarts and starts a fresh domain under a replacement membership.
Overflow drops oldest events behind a *leading* `Lagged` marker, and the
documented resync protocol is subscribe-then-snapshot; the ring capacity
is deliberately not API.

An `Exited` event carries a projection of the child's `Exit`. The
retained wrapper stores the public event before its retention guards, so
ring eviction drops the projection while a `RetainedExit` still protects
the user error inside it — its eventual destruction goes through the
critical disposal lane, never a subscriber's thread.

## Snapshots

A snapshot is computed on demand under the gate — never served from the
last published value — and skips *unannounced* residents: a child pushed
into residency before its first fallible admission step, whose admission
then unwound, must not appear in a cut that never saw its `Added` edge.
This keeps the `Added`/`Removed` pairing exact.

Publication stages a **producer, not a value**. A compound transition
touching many children would otherwise build a full recursive projection
per stage and retain all but the last until commit; coalescing replaces
the staged producer instead, so a superseded cut is never built and the
survivor runs exactly once inside `commit`, with the gate still holding
the tree still. The superseded producer's captured `Arc`s leave with the
effect list, after unlock. Snapshot receivers conflate: a subscriber
always reads the latest committed cut, and every retained value is a
complete transaction.

Publishing a scope's snapshot also publishes every ancestor's, walking
the resident chain under the same transaction, which is what makes a
recursive `System`-level snapshot agree with the scope-level one taken in
the same commit.
