# The life of a message

This chapter follows one message from a caller's `send` to the receiving
actor's handler, naming every structure it passes through. The mailbox is
the most concurrency-dense subsystem in the library, and almost every
shape it uses exists to satisfy the lock discipline described in
[Locks, effects, and disposal](internals-concurrency.md): no user code —
wakers, destructors, message drops — ever runs under a mailbox mutex.

## The handle

`ActorRef<M>` (`crates/shelterwood/src/mailbox/futures.rs`) is the only
send surface. It holds exactly two `Arc`s: a `dyn ActorIdentity` (the
restart-stable membership identity, implemented by `MemberCell`) and the
`MailboxCell<M>` itself. Equality and hashing go by membership, which is
why a handle keeps working across restarts: it addresses the slot, not
the incarnation. Handles are minted only through the crate-private
`actor_ref_from_parts` seam — at declaration time in `tree/slots.rs`,
from `RawContext::myself`, and at dynamic admission.

Four send flavors sit on the handle: `send` (waits through backpressure
and rebind windows), `try_send` (fails fast), `send_timeout` (a
deadline around `send`), and `call` (send plus reply under one budget).

## Submission

`SendFuture` is lazy — nothing happens until first poll. That poll calls
`MailboxCell::submit` (`mailbox/cell.rs`), which opens a `MailboxTxn`:
the guard over the cell's state mutex paired with a `MailboxEffects`
sink. Every wake, displaced payload, and disposal request the transition
produces is queued into the sink and flushed only after the guard drops.

`submit` branches on the mailbox's binding status:

- **Bound with capacity** — the message is stamped with an
  `AcceptedSequence` and pushed onto the queue (or, for a `latest()`
  mailbox, swapped into the conflating slot, with the displaced envelope
  routed to disposal through the effects sink). The caller gets the
  accepting `Incarnation` back as evidence.
- **Full, unbound, or frozen** — a `SendOperation` is created and parked
  in the binding's waiter queue. Unbound covers the pre-spawn and restart
  rebind windows; frozen means intake stopped at shutdown entry.
- **Terminal** — the message comes back inside
  `SendError { kind: Terminated, .. }`, never silently dropped.

## Parking and backpressure

A parked `SendFuture` polls its `SendOperation`, whose state holds the
outcome slot and a `WakerSlot` for the caller's waker. Two details are
worth knowing before touching this code:

- Cloning a caller's waker runs caller-owned vtable code, so the
  operation poll returns a `NeedsWakerClone` result rather than cloning
  under its own lock; the clone happens outside, and repeat polls use
  `WakerSlot::will_wake` (a pointer compare) to skip it.
- When capacity frees up, `MailboxCell::receive` promotes waiters FIFO:
  each promoted operation's message moves into the queue and its waker is
  taken as a `Wake` action into the effects sink, to be woken after
  unlock.

Cancelling a parked send (`SendFuture::drop`) produces a `Withdrawal`: a
single value carrying both the outcome and the deferred waker effects.
Since no caller remains to receive them, the recovered message and the
registered waker go to isolated disposal.

## Waking the actor

Every accepting or binding transition queues a pulse. After unlock, the
flush pulses the cell's `MailboxSignal` — the capability object's change
signal, a watch channel in the Tokio adapter. The receiving incarnation
holds a `MailboxReceiver<M>` (the mailbox `Arc`, its incarnation, and a
signal watcher); the pulse is what makes its pending `changed()` future
ready.

## The receive loop

The loop every actor runs — raw or callback — lives in
`RawContext::recv` (`crates/shelterwood/src/raw/context.rs`). Its wait is
a nested two-way select over four sources: the shutdown latch, the
local-stop latch, the mailbox/offload change signals, and the earliest
keyed-timer deadline (kept outside the nested select and applied as a
timeout around it).

When something is ready, `next_ready` arbitrates fairly over one
`ReadyBatch` with a fixed stage priority: a fairness continuation first,
then mailbox messages up to the batch's accepted-sequence cutoff, then
offload completions, then remaining continuations, then due timers. The
cutoff matters: a batch reads only what was accepted when it was formed,
so a fast sender cannot starve timers and offloads.

During shutdown drain the selector is bypassed entirely: `try_recv`
freezes intake and reads the frozen accepted prefix synchronously.

## The handler layer

Callback actors ride on `Handler<A>` (`crates/shelterwood/src/actor.rs`),
which implements `RawActor` — the high-level loop is a client of the same
raw surface a hand-written actor uses. `Handler::run` drives
`A::init`, then loops `recv().await` into `A::handle`, and on loop exit
either drains the frozen prefix through the same `handle` (mailbox policy
`Drain`, with the context marked as delivering the frozen prefix) or
returns immediately (`Discard`, the framework disposing the prefix),
before running `A::on_stop`. A handler error freezes and joins the
incarnation's resources before the exit propagates, so no offload
outlives the incarnation that started it.

## Request and reply

`Reply<T>` wraps a one-shot sender minted from the mailbox's capability
object. Its `send` consumes it; if the receiver is already gone, the
value routes to isolated disposal rather than being dropped on the actor
task. `ActorRef::call` builds the whole conversation under one deadline
budget: constructing the message from the user's closure, waiting for
acceptance, and waiting for the reply. Pre-acceptance timeouts withdraw
the parked send and dispose both the recovered request and the registered
waker; the error catalog (`CallErrorKind`) distinguishes acceptance
timeout, response timeout, terminated membership, and a dropped reply.
