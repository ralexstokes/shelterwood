# Readiness and startup order

Startup order in an ordered tree is only half the contract; the other
half is what "started" means for each child. Shelterwood answers with
*readiness*: declared data attached to each child, never inferred from
when the child's future happens to be polled.

## The three modes

`Readiness` has three variants:

- `Immediate` — the child is ready the moment its incarnation is
  launched, before its future is first polled. This is the default for
  tasks and raw actors: a plain task has no initialization phase the
  framework could observe, so it gates nothing unless it says otherwise.
- `AfterInit` — the child is ready when its `init` returns `Ok`. This is
  the default for handler actors declared with the `Actor` trait, and it
  is why an ordered tree of actors "just works": each actor finishes
  initializing before the next one starts. It is meaningless for kinds
  without an `init` and is rejected eagerly at declaration for them.
- `Manual` — the child is ready only when it explicitly says so, by
  calling `mark_ready` on its context. Use this when readiness means an
  external handshake — a listener bound, a connection established —
  rather than construction finishing. The mark is one-shot per
  incarnation, and a restart re-arms it.

A gated child (`AfterInit` or `Manual`) is bounded by a readiness
deadline. `ReadinessDeadline` resolves declaration → scope default →
library default (thirty seconds); `Bounded` applies an explicit non-zero
bound, `Unbounded` waits forever, and expiry is a startup-failure exit
like any other.

## How an ordered scope uses readiness

An ordered scope starts its children one at a time: it launches a child,
waits for that child's readiness, and only then starts the next declared
child. A gated child that fails before becoming ready is restarted under
its [restart policy](supervision-policy.md) with the sequence still
blocked; startup aborts only when a pre-ready exit is terminal — a
`Never` policy, a one-shot, or an intensity trip. Children declared after
that point never start at all.

`System::wait_started` is the barrier over the whole sequence: it
resolves `Ok` once every declared child is ready, and otherwise reports
the recorded `StartupError` — a terminal child failure during startup, an
intensity trip, or a shutdown requested before startup completed. A
failure reported by `wait_started` deliberately leaves the successfully
started prefix running and supervised, so the caller decides what a
partial start means; `System::start_or_shutdown` is the rollback form
that tears the prefix down and preserves the original cause.

## Watching the order happen

The `ordered_startup` example makes the ordering observable by having
three identical workers append to a shared log:

```rust
{{#include ../../crates/shelterwood/examples/ordered_startup.rs:ordered_startup}}
```

Each worker declares `Readiness::Manual` and calls `mark_ready` on its
context only after logging its start line, so the next declared child
cannot start before the previous one's line is in the log. The first
assertion is therefore exact, not probabilistic: when `wait_started`
resolves, the log reads `start:first`, `start:second`, `start:third` in
declaration order — the readiness gate serialized the starts.

The second assertion shows the reverse side. Shutdown stops one fully
joined child at a time in reverse declaration order, so each worker's
`stop` line lands only after every later-declared sibling has fully
stopped: `stop:third`, `stop:second`, `stop:first`. The workers observe
the request through `context.shutdown_token()`; the grace, escalation,
and drain rules behind that are the subject of [Shutdown](shutdown.md).

Dynamic scopes trade this gating away: their initial members start
concurrently, and `wait_started` aggregates readiness over the initial
set only — see [Dynamic scopes](dynamic-scopes.md). The per-item
contracts for `Readiness`, `ReadinessDeadline`, and `StartupError` live
in the [API reference](https://docs.rs/shelterwood).
