# Locks, effects, and disposal

This is the chapter to read before touching any `.lock()` in the
codebase. The governing invariant — stated normatively in `CLAUDE.md` and
SPEC §15.4, explained here — is the **lock rule**: code holding a
framework mutex may only manipulate plain framework-owned data. Wakes,
drops of user values, formatting, user callbacks, and panic resumption
happen after unlock.

The reason is that the framework runs arbitrary user code at moments it
does not choose: waker vtable functions, destructors of messages and
actor state, `Debug` on user errors, `Hash`/`Eq` on timer keys. All of it
is safe Rust that may panic, block, or re-enter — and under a framework
mutex each of those becomes poisoning, an ABBA deadlock, or a
double-panic abort during an unwind.

## The lock order

The documented order, outermost to innermost:

```text
observation gate  →  MemberCell::mailbox  →  MailboxCell::state
                                          →  SendOperation::state
```

The `WakerProxy` mutex is a **leaf** at the far end: `wake_by_ref`
acquires it from whatever thread drives an external primitive, so no
framework lock may ever be taken under it. The one exception to "gate is
outermost" is the parent-to-child gate handoff described in
[Observation from the inside](internals-observation.md); admission is the
deepest use of that doubled section, and anything new added there needs
the same accounting `CLAUDE.md` records for it. Everything else — scope
control state, dynamic-membership maps, plan definition state — is
framework-owned data taken under the gate or standalone.

## The effects shapes

The rule is implemented, not remembered: the two transaction types make
deferral structural, and a family of single-purpose carriers moves
specific effects past specific unlocks.

- **`ObservationTxn`** (`cells/gate.rs`) — the gate guard plus a deferred
  effect list; commit drops the guard then flushes under a panic
  accumulator, and `Drop` runs the same path during an unwind.
- **`MailboxTxn`** (`mailbox/cell/effects.rs`) — the mailbox state guard beside a
  `MailboxEffects` sink collecting pulses, waker actions, displaced
  payloads, and disposal requests. Its `Drop` empties the guard field
  before Rust drops the sink, so field order alone guarantees the flush
  runs with no mailbox mutex held, unwind included.
- **`WakerSlot` / `WakerAction` / `WakerEffects`** — the waker half made
  structural: slot storage is private to core's waker module, and no
  operation returns or replaces a `Waker` without an effects sink.
- **`Withdrawal`**, **`Termination`**, **`MailboxPayload`** — carriers
  that hand a cancellation outcome, a terminating mailbox's waiters, or
  a closed mailbox's unread contents out of the critical section as one
  owned value, so the destination — not the transition — decides where
  user values die.
- **`PanicAccumulator`** (core, `panic.rs`) — keeps the first panic
  payload while later cleanup steps still run, and resumes the preferred
  panic only once everything has been released. During an existing unwind
  it contains rather than resumes, which is what keeps a hostile
  destructor an ordinary outcome instead of a process abort.

What the rule does *not* forbid is as important as what it does:
framework-owned counters and maps need no ceremony; *moving* a user value
out under a lock is fine (the destination must be outside); and `Arc`
traffic that provably cannot reach zero is refcount work. The full
exemption catalog, with the audit history behind it, lives in
`CLAUDE.md` — treat that as the checklist of record.

## Disposal lanes

A value that may block or panic on destruction is not merely dropped
after unlock — it is handed to a lane in
`crates/shelterwood-runtime/src/disposal.rs`:

- **`dispose_detached`** — for values whose owner is *not* inside a
  critical section but whose destructor may block: unread mailbox
  payloads, construction closures, displaced resident graphs, rejected
  admissions. Falls back from the blocking pool to a shared fallback
  thread to, ultimately, inline destruction on the submitting thread.
- **`dispose_critical`** — for values whose *last owner may be inside a
  framework critical section*: the `Retained<T>` carrier around failed
  exits, recorded outcomes and incarnation results. No path ever
  destroys the value on the submitting thread; under exhausted thread
  creation the accepted fail-safe is a queued job held for the life of
  the process, which is the trade against destroying user state under a
  lock.
- **`dispose_then` / `dispose_all`** — contained destruction with a
  completion, used where a later step must wait for the destruction to
  finish (a child's construction gating its release edge). The
  completion reports only that destruction finished; a destructor panic
  is contained and never reaches a verdict.

The `Retained*` family in `cells/retained.rs` is the bridge: a framework-
retained copy of an `Exit` owns a type-erased user error, and its drop
transfers destruction to the critical lane. Framework code folds exits
through the `*_retaining` classifiers so the losing half of every verdict
fold retires through the lane too; exits handed to *users* keep ordinary
drop timing.

## The waker proxy

External primitives — the timer wheel, senders, any executor — must never
hold a raw caller waker, because its `clone`/`wake`/`drop` run
caller-owned code on the primitive's thread. `WakerProxy`
(`crates/shelterwood-core/src/waker_proxy.rs`, private to that module)
registers a stable
framework-owned waker with the primitive and keeps the caller's real
waker in a slot behind the leaf mutex; every removal queues the resulting
user-code effect for after unlock.

The subtle part is the lost-wake handshake: cloning the caller's waker
happens *between* two critical sections, and a wake landing in that
window would find an empty slot or the previous poll's waker. So the
proxy's `wake_by_ref` bumps a wake count under the leaf mutex, and a
registration snapshots the count before cloning: if it moved by the time
the fresh waker is installed, that waker is taken straight back out and
woken after unlock. A wake that completed before registration began is
not replayed — the caller's poll of its level-readiness target after
registering observes its cause — so an ordinary delivery costs no extra
poll.

`ProxiedPoll` wraps the proxy in a probe/install/re-poll state machine:
first poll with a noop waker (preserving the already-ready fast path with
no allocation), install and register only if pending, and on the ready
edge retire the stored caller waker synchronously with any panic
contained and discarded. It is the proxy's only owner and the only
waker-proxy item core exports; its one `retire` takes the disposition
(inline drop or the adapter's disposal function) and flushes it after
unlock. `ProxiedSleep` applies the same boundary to
runtime timers, retiring slot-first then cancelling the wheel entry, with
the poll path retiring inline and drop glue handing the waker to the
disposal lane.

## Reviewing a new lock

The one-pass review from `CLAUDE.md` applies to every new critical
section: name every value it can destroy, every callback it can invoke,
and every panic it can raise. If any of those is user code, hand it to a
transaction, an effects struct, or the caller. The shapes above exist so
that the easy way to write the code is also the compliant one — reach for
them before inventing a new deferral mechanism.
