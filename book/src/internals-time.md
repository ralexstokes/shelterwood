# Time, timers, and offloads

Time enters the library in exactly one way, and no decision module ever
reads it. This chapter maps the clock's single entry point, the two timer
subsystems built on it (the driver's deadline queue and the raw actor's
keyed timer store), and offloads — the mechanism that lets an actor start
concurrent work whose completion re-enters its own loop.

## One clock

The capability object installed per mailbox carries the clock: `now` and
`sleep_until` on `MailboxRuntime`, implemented by the Tokio adapter over
`tokio::time`. Because deadline futures and reply channels flow through
the same object as the mailbox that minted them, virtual time in tests
(the adapter's `advance`, under the `test-util` feature) reaches every
timer in a system consistently — there is no second clock to drift.

Decision modules never call it. `StopLadder::advance(now)` and
`schedule_restart(..., now, jitter)` take the instant as an argument, and
the reducer sees deadlines only as "this deadline fired" events. That is
SPEC §15.3's rule, and it is why the stop ladder is a state machine
rather than a sleeping task per child.

## The driver's deadline queue

Each scope driver owns one `DeadlineQueue`, multiplexing every timed
obligation its children have: stop-ladder deadlines, restart backoff
expirations, and readiness deadlines, each tagged with a `DeadlineKind`.
The driver's select sleeps until the earliest entry; a firing becomes a
sampled event, arbitrated alongside everything else that arrived in the
same wake. One consequence worth internalizing: no child ever owns a
timer task, so a scope with hundreds of stopping children still holds
exactly one pending sleep.

## The raw actor's keyed timers

`set_timeout`/`set_interval`/`clear_timer` on the actor contexts are
backed by the keyed timer store in
`crates/shelterwood/src/raw/context/timers.rs`. Its ownership rule is
the interesting part: the store owns every armed timer's *user key and
message*, mints the `ArmingOrder` values that index them (mintable only
by the store), and is the only place either value is destroyed — so both
always retire through the raw incarnation's contained-disposal path
rather than on the actor task, where a hostile `Drop`, `Hash`, or `Eq`
would otherwise run at a moment the framework didn't choose.

A requested delay that overflows the clock becomes a deadline of `None`
— a timer that never fires, mirroring the offload path — never one that
is "due now". The earliest due deadline is applied as a timeout *around*
the receive loop's nested select rather than as an arm inside it, and
due timers are the last stage of the fairness batch's arbitration
([The life of a message](internals-message.md)), so a hot mailbox cannot
indefinitely pre-empt them within a batch but timers also cannot starve
message delivery.

Mailbox-side deadlines — `send_timeout`, `call`, reply waits — use the
`Deadlined` wrapper over `ProxiedSleep`, which is how a runtime timer
wheel is prevented from ever holding a caller's raw waker
([Locks, effects, and disposal](internals-concurrency.md)).

## Offloads

`offload` and `offload_scoped` (`crates/shelterwood/src/raw/offload.rs`
and the context methods) start incarnation-owned async work with a
continuation and a single `DeadlineBudget` covering the whole
conversation. The mechanics:

- The work runs as its own task; its completion is queued into the
  incarnation's event queue and re-enters the actor loop as an ordinary
  message via the continuation — which is why offload results obey the
  same at-most-once, no-cross-incarnation rules as everything else.
  Completion storage is unbounded but bounded in practice by one entry
  per offload the actor itself started, and it drains in bounded
  arbitration turns beside mailbox input.
- `offload_scoped` returns a `Guard`: a cancel-on-drop lease with
  explicit `cancel` and `detach`. Cancellation suppresses the
  continuation, so no offload extends the incarnation that created it;
  `offload` is simply the scoped form with the guard detached.
- A zero budget never polls the work at all and queues the continuation
  with `DeadlineElapsed` — the honest degenerate case rather than a
  race.

`run_blocking` is deliberately different. It has no stopping gate —
teardown code may still need blocking work, so it keeps working from
stop paths — and its cancellation is cooperative: dropping the returned
`Blocking` future or hard-aborting the actor detaches the OS thread,
which can outlive the incarnation. A closure panic resumes at the await
point; an operation cancelled by runtime teardown before it ever ran
panics with a distinct teardown diagnostic when awaited. The
blocking-pool submission path underneath is the same one the disposal
lanes use, and its rejection-ownership subtleties are why the workspace
pins the exact Tokio release.
