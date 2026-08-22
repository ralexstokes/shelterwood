# Shutdown from the inside

The [shutdown chapter](shutdown.md) teaches the user-facing contract: one
escalation ladder, reverse order, a budget. This chapter maps how the
implementation delivers it. Two ideas organize everything: shutdown
*requests* are sampled latches, and the shutdown *budget* bounds
cooperative teardown, never the framework's own bookkeeping.

## Requests are latches

Every entry point — `System::shutdown`, `ScopeRef::request_shutdown`, a
child calling `request_scope_shutdown`, or dropping the sole `System`
owner — converges on the same level-triggered, per-incarnation latch on
the `ScopeCell`. Latching stores the request against the scope's current
epoch, pulses the member record so the driver wakes, and returns. The
driver samples the latch at the top of its loop pass, alongside force
requests and ancestor latches, and the sampled facts are arbitrated with
everything else that arrived in the same wake — which is how "force owns
shutdown arbitration" holds: a readiness edge sampled in the same pass
cannot publish `Running` after the stop boundary.

The owner-drop path latches through a poison-tolerant variant, so a drop
on an already-unwinding thread cannot abort the process.

## The waiting side

`shutdown_scope` (`crates/shelterwood/src/driver/shutdown.rs`) is what
`System::shutdown` and `ScopeRef::shutdown_and_wait` await. Its sequence:
return early if the scope is already settled; latch the request; wait for
drain entry; then wait for settlement under the budget. A zero budget
skips the wait but still delivers the cooperative request. When the
budget elapses, it collects *stragglers* — the innermost incomplete
members, by path — forces the scope, and then **keeps waiting to
settlement**: the timeout bounds cooperation, not the call, so the error
a caller gets (`ShutdownTimeout` with straggler paths) describes a tree
that has nonetheless been fully torn down. `System::shutdown` then joins
the driver task itself.

## Drain entry

Inside the driver, a sampled shutdown becomes `begin_drain`. The stop
reason is first retained (it can carry a user error — a startup failure
owns the triggering child's exit), then the core reducer enters drain:

- an **ordered** scope initializes a reverse cursor at its last child and
  stops one child at a time, advancing only past a joined child;
- a **dynamic** scope emits a stop for every non-joined child at once,
  with parallel grace clocks.

One settlement pass runs before publication so the `Draining` edge
carries the first stop's intent, and the state change plus any terminal
disposal intents publish as a single observation commit. A force request
(`force_all`) instead marks the hard-force fact and emits a force per
child directly.

## The drain lattice

A scope can be asked to stop for several reasons before it finishes
stopping — a shutdown request landing during an intensity trip, a startup
failure during a drain. The stop reason is therefore a monotone verdict
lattice, severity-ascending:

`Finished < IntensityTripped < StartupFailed < ShutdownRequested <
NeverStarted`

Both consumers — the in-progress drain in core's `ScopeLifecycle` and the
already-published `Stopped` projection on the `ScopeCell` — join new
verdicts into the current one with strictly-greater-wins, so repeats are
idempotent and a later, more severe fact upgrades the reason without
repeating drain entry.

## The per-child ladder

Each stopping child gets a `StopLadder`
(`crates/shelterwood-core/src/engine.rs`): a pure state machine
`Idle → Cooperative → Escalated → Finished` whose `advance(now)` returns
the next `StopAction` — cancel, escalate, framework-abort (for a nested
scope's driver, with its own acknowledgement), or hard abort. The driver
funnels every stop through `begin_stop_child`, which **freezes the
mailbox unconditionally**, cancels the readiness deadline, installs the
ladder, and pushes its deadlines into the scope's single deadline queue.
Actions map onto latches and abort handles; `force(now)` only ever moves
the current deadline earlier and never skips the tidy beat between
escalation and abort. A grace expiry records `AfterGrace` on the exit; a
declared `Abort` policy records `WithinGrace`.

## Driver death discharges

The driver task itself can be aborted — a parent's hard force, a host
tearing down the runtime. `ScopeRuntime`'s drop epilogue is written for
that case: per child it folds pending disposal, freezes and closes the
mailbox, fires the shutdown and abort edges, aborts the task, and
discharges the child's terminality obligation, publishing the coarse kill
verdict. It then clears residency (publishing every `Removed` edge before
the scope's final event) and unconditionally retires the epoch. Every
step runs under a panic accumulator, so one hostile destructor cannot
stop the discharge of the rest. The rule of thumb the code comments use:
the epilogue *discharges* obligations, it never *absolves* them.

## Removal

Dynamic removal rides the same machinery with one extra care: committing
a removal happens inside a single observation transaction that withdraws
residency while the child's id remains claimed, so a concurrent
reservation can never observe a reused-id overlap. During startup,
releasing the removal response is deferred until startup has seen the
shrunken initial set, which is what makes a returned `Removed` a safe
signal to re-admit under the same id.
