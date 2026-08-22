# Supervision policy

Supervision policy in Shelterwood is plain data, validated when it is
constructed. There are no policy traits to implement and no callbacks to
register: a policy value describes what the framework should do, and the
constructors reject invalid configurations before they can exist — a zero
backoff delay, a zero intensity window, or a backoff factor below one
each fail with a `PolicyError` at the call site, never at runtime. If a
policy value exists, it is valid.

## When to restart: `RestartCondition`

`RestartCondition` says which exits schedule a restart: `Always` restarts
after every exit including successful completion, `OnFailure` restarts
only after a failure exit, and `Never` never restarts. A
`RestartCondition` pairs with a `Backoff` in a `RestartPolicy`; the
default policy is `OnFailure` with no delay. One-shot definitions
(`ActorOnceDef`, `TaskOnceDef`, `SubtreeOnceDef`) are structurally
`Never` — they carry no restart setter at all.

## How long to wait: `Backoff`

`Backoff` chooses the delay before each restart attempt: `Immediate`,
`Backoff::fixed` (a constant non-zero delay), or `Backoff::exponential`
(a base delay multiplied by a validated `BackoffFactor` per attempt,
clamped to a maximum). Either shape can add `Jitter::Equal`, which draws
each delay uniformly from the upper half of the derived value so a herd
of failing children does not restart in lockstep; `Jitter::None` uses the
derived delay exactly.

## The scope's budget: `Intensity`

Restarts are also budgeted per scope. `Intensity::new(max_restarts,
within)` allows at most `max_restarts` restart charges inside a rolling
window; every scheduled restart charges the budget, and the charge that
exceeds it *trips* the scope. Tripping is scope-fatal: the scope stops
restarting, tears its children down, and escalates to its parent as a
failure — a scope that cannot keep its children alive within budget is
itself treated as failed. The default budget is five restarts within
thirty seconds.

## Per-child options

Alongside restart policy, each child declaration carries a small set of
options, all plain validated data with the same construction discipline:

- `Shutdown` picks the child's stop behavior: `Shutdown::graceful(grace)`
  requests cooperative shutdown for up to a validated non-zero grace
  (five seconds by default), while `Shutdown::Abort` escalates
  immediately after cancellation.
- `Mailbox` declares an actor's mailbox: `Mailbox::queue(capacity)` for a
  bounded FIFO (64 by default) or `Mailbox::latest()` for a conflating
  latest-value slot. `MailboxShutdown` decides the fate of already
  accepted messages during shutdown: `Drain` delivers them first,
  `Discard` drops them.
- `Retention` decides whether a terminal membership stays visible:
  `Retain` keeps it as an inspectable tombstone until explicitly removed,
  `Remove` prunes it immediately. Restartable children default to
  `Retain`, one-shots to `Remove`.

Options attach through consuming setters on the definition, and
`ScopeDefaults` lets a scope set inherited defaults for all of them.

## A restart, observed

The `supervision_restart` example declares a worker that fails on demand:

```rust
{{#include ../../crates/shelterwood/examples/supervision_restart.rs:actor}}
```

Its declaration attaches an `OnFailure` policy with a short fixed
backoff:

```rust
{{#include ../../crates/shelterwood/examples/supervision_restart.rs:policy}}
```

After sending `Msg::Crash`, the example watches the scope's snapshot
for the restarted incarnation and then calls the *same* handle again:

```rust
{{#include ../../crates/shelterwood/examples/supervision_restart.rs:restart_wait}}
```

Note what the assertion checks: `after.supersedes(before)`, not "the next
incarnation". Snapshot watches conflate — if the child crashed and
restarted more than once between observations, the watcher sees only the
latest state — so the reliable claim is that the new incarnation
supersedes the old one, never that it is exactly one generation later.
The handle survives all of it: an `ActorRef` addresses the membership,
which is stable across restarts, so `worker.call` reaches whichever
incarnation is current. [Identity, incarnations, and the retry
discipline](identity-incarnations-retries.md) develops that model.

Exit classification — which exits count as failures, and the structured
`Exit` type supervisors record — and the full policy vocabulary are
documented in the [`Exit` and policy reference on
docs.rs](https://docs.rs/shelterwood).
