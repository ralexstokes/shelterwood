# Tasks and offloads

Not everything is an actor. A connection acceptor, a metrics flusher, or a
one-time migration has no mailbox and no protocol — it is just a future
that should be supervised like everything else. Shelterwood calls these
**tasks**, and they are peers of actors in the tree: declared with an id,
started in order, restarted under policy, and stopped down the same
escalation ladder as everyone else.

This chapter covers tasks first, then the two ways an *actor* pushes work
out of its handler: offloads and `run_blocking`.

## Task definitions

`TaskDef::new` takes a factory closure from `TaskContext` to a future
returning `ExitResult`. Because it is a factory — callable again — the
task is restartable under supervision, exactly like an actor whose `init`
runs on every start.

`TaskOnceDef::new` instead takes a consuming `FnOnce` body, so it gets
exactly one incarnation: the owned body cannot be invoked again. In
exchange it is typed — the body returns `Result<T, ExitError>`, and
declaring the task yields, beside the ordinary `TaskRef`, a
`OneShotTaskRef<T>`: the sole claim to the typed completion value.
Awaiting `OneShotTaskRef::wait` resolves `Ok(value)` only when the
membership's authoritative terminal verdict is `Completed`; any competing
failure, panic, abort, or readiness timeout wins instead and the claim
resolves to that `Exit`. "One-shot" constrains incarnations, not
iterations — the body may loop for the life of the system and hand back a
final summary.

## The task context

The `TaskContext` handed to the body is the task's whole capability
surface, and it is small:

- `id()` and `incarnation()` name this child and this run of it.
- `shutdown_token()` is the cooperative cancellation signal — the first
  rung of the [shutdown ladder](shutdown.md).
- `abort_token()` fires at escalation, strictly after the shutdown token.
- `mark_ready()` publishes manual readiness. Once the task is already
  stopping, the call is a documented no-op — a stopping incarnation can
  no longer claim to be ready.

The shape that uses all of it appears in
`crates/shelterwood/examples/ordered_startup.rs`:

```rust
{{#include ../../crates/shelterwood/examples/ordered_startup.rs:worker}}
```

This is the canonical long-running task: do the setup, `mark_ready()` so
the next declared sibling may start, then park on the shutdown token until
teardown. The `Readiness::Manual` option is what makes `mark_ready` the
readiness edge rather than the first poll;
[Readiness and startup order](readiness-startup-order.md) covers that
machinery, and [Embedding in a host](embedding.md) uses the same task
shape to gate a host resource.

## Offloads: async work that returns to the handler

An actor handler must not await slow work — the mailbox behind it is
frozen while it runs. `Context::offload` is the escape hatch: it starts
**incarnation-owned** async work whose completion re-enters `handle` as an
ordinary message.

The contract has three load-bearing pieces:

- **One deadline budget.** Every offload takes a single deadline, and the
  continuation is *total*: it receives `Result<T, DeadlineElapsed>` and
  must produce a message either way. The framework's timeout verdict is
  structurally distinct from the operation's own error, so there are no
  hand-ordered inner/outer timeout pairs. A zero budget never polls the
  work at all and delivers `Err(DeadlineElapsed)` straight away.
- **Incarnation ownership.** The work belongs to the incarnation that
  started it. It is cancelled at the stop-time intake freeze and never
  outlives its incarnation — which is why offloads are rejected once the
  actor is stopping, and why `StopContext` has no offload surface at all.
  There is no `Cancelled` continuation arm; cancellation suppresses the
  continuation entirely.
- **Ordinary supervision.** A panic in the offloaded future or the
  continuation resumes on the actor task and is classified like any other
  actor panic.

`Context::offload_scoped` is the same operation returning a `Guard`
whose drop cancels the work; `Guard::detach` releases only that
cancel-on-drop behavior — detached or not, the work stays
incarnation-owned. There is no offload example in the book; the per-item
contracts live on
[`Context::offload`](https://docs.rs/shelterwood/latest/shelterwood/struct.Context.html)
at docs.rs.

## `run_blocking`: threads for blocking work

Offloads are for async work; `run_blocking` moves a *blocking* closure to
a blocking thread. It is deliberately different in both directions:

- It is available from `StopContext` as well as `Context` — teardown code
  may legitimately need a blocking close, so unlike offloads it keeps
  working while the actor is stopping.
- Its cancellation is cooperative only. The closure receives a
  cancellation token tied to actor shutdown and to the returned future's
  drop, but nothing can forcibly stop an OS thread: after a hard abort
  the thread detaches and may keep running, and Shelterwood never joins
  it. A late return value or panic from a detached thread is discarded
  through detached disposal.

That detach caveat is the design constraint to remember: a blocking
operation must be safe to outlive its actor, and must not hold an
exclusive process resource indefinitely. The rustdoc guide
[`guides::shutdown_and_resources`](https://docs.rs/shelterwood/latest/shelterwood/guides/shutdown_and_resources/index.html)
carries the full contract, including the runtime-teardown fallback paths.

The rule of thumb, in one line: async work from a handler goes through
`offload` and comes back as a message; blocking work goes through
`run_blocking`; and anything with its own lifecycle deserves to be a task
of its own.

Next, [Dynamic scopes](dynamic-scopes.md) opens membership up at runtime.
