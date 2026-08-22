# Glossary

The vocabulary of the model, alphabetized. The normative definitions live
in the specification (`specs/SPEC.md`, §2 and the sections it points
into); this appendix is the short form.

**Actor** — a mailbox-owning message loop, one of the three child kinds.
Declared either through the handler trait (`Actor`, whose `init` and
`handle` the framework's loop drives) or as a raw actor that owns its own
loop.

**Backoff** — the delay schedule between restarts of one membership:
fixed, or exponential (`base × factor^(n−1)` clamped to `max`), with
optional jitter. All durations are validated non-zero at construction.

**Child** — one supervised member of a scope: an actor, a task, or a
nested scope. Each child has an id — a non-empty string unique among the
resident memberships of its scope — which names a slot for humans and
traversal but is not identity.

**Drain** — the mailbox shutdown mode that delivers the frozen accepted
prefix to the handler before `on_stop` runs. Intake freezes first, so the
drained log is exactly what was accepted before the stop.

**Grace** — a stopping child's cooperative window on the shutdown ladder:
the time between cooperative cancellation and escalation toward abort. It
is a supervisor-side upper bound, set per child by the shutdown policy.

**Incarnation** — one run of a membership. A restart mints a new
incarnation of the same membership; within a membership, incarnations are
ordered by `supersedes`.

**Intensity** — a scope's churn budget: at most `max_restarts` restart
charges within a rolling `within` window. Every scheduled respawn charges
it, and exceeding it is scope-fatal, escalating to the parent.

**Lifecycle event** — one ordered edge in a scope's bounded event stream
(admission, start, exit, restart scheduling, removal, scope state),
carrying the emitting scope's membership and sequence number. Buffers are
per subscriber; overflow drops oldest events behind a leading
`Lagged` marker.

**Mailbox** — the bounded delivery channel a membership owns for its
actor. `queue(capacity)` is a bounded FIFO with real backpressure — a full
queue makes `send` wait and never evicts; `latest()` is a single
conflating slot that accepts by replacement.

**Membership** — a child's slot in a scope across restarts: a process-wide
identity key created by declaration or dynamic insertion and ended by
terminal removal. Handles address memberships by default, so they ride
through restarts.

**Membership terminal** — see *terminalization*.

**Offload** — incarnation-owned async or blocking work started from a
handler, whose completion re-enters the actor loop as a message under one
deadline budget. Cancellation suppresses the continuation, so no offload
extends the incarnation that created it.

**Pruning** — the edge at which a membership stops being a resident of its
scope: the `Removed` event fires, the id is freed, and anything not yet
resolved terminal now does. Distinct from terminalization; retention
chooses the distance between the two.

**Readiness** — declared data (`Immediate`, `AfterInit`, or `Manual`)
saying when a child counts as ready, read before its future is first
polled. In an ordered scope, each child's readiness gates the next child's
start.

**Rebind window** — the interval during a restart when a membership's
mailbox is bound to no incarnation. `try_send` fails fast with
`NotRunning`; plain `send` parks and waits through it.

**Retention** — the per-child option choosing when a terminal membership
is pruned. `Remove` prunes immediately after terminalization; `Retain`
keeps the terminal membership resident as an observable tombstone that
still occupies its id until explicit removal or scope teardown.

**Scope** — one supervisor node, owning an ordered or dynamic set of
children. An *ordered* scope has membership fixed at build time,
sequential readiness-gated startup in declaration order, and reverse-order
teardown; a *dynamic* scope has runtime membership, concurrent start and
stop, and per-child fate-sharing only.

**Snapshot** — an authoritative recursive projection of a scope's current
state, computed on demand from the published view. Snapshot subscriptions
conflate: a watch delivers the latest committed cut, never a partial
transaction.

**Subtree** — a nested scope supervised as one child of its parent: a
whole scope with its own children and policy, whose failure surfaces at
the parent as an ordinary child exit.

**Supervision strategy** — an ordered scope's fate-sharing rule relating
one child's exit to its siblings. Core ships `OneForOne` only — a child's
exit affects that child alone — and dynamic scopes are structurally
`OneForOne`, carrying no strategy at all.

**System** — the running instance spawned from a `Tree` or `DynamicTree`
declaration, as distinct from the declaration itself and from the engine
that runs it. Exactly one owning `System` handle exists per spawned root;
dropping it requests graceful shutdown.

**Task** — an arbitrary supervised future with a `TaskContext`, one of the
three child kinds. Tasks are first-class peers of actors: same startup,
restart, shutdown, and observation treatment, with no mailbox.

**Terminalization** — the edge at which a membership becomes terminal: its
final exit publishes and its state resolves to `Stopped` or
`StartupAborted`. A terminal membership never restarts; whether it is
pruned immediately or retained as a tombstone is the retention option's
choice.

**Tree** — the shape of a running system: scopes owning children, some of
which are scopes themselves. Also the name of the ordered root builder
(`Tree`, beside `DynamicTree`) that a system is spawned from.

**Watermark** — a per-scope lifecycle sequence recorded in a snapshot,
looked up by membership with `watermark()`. During catch-up, an event with
`seq <= watermark` is already reflected by the snapshot and is discarded.
