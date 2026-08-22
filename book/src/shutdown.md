# Shutdown

Shelterwood has exactly one shutdown story, and it is used everywhere: for
`System::shutdown`, for a scope stopping its children, for dynamic removal,
and for restarts that must stop the old incarnation first. Every stopping
child walks the same escalation ladder:

```text
cooperative cancellation -> grace expiry -> tidy-abort beat -> hard abort
```

The rungs, in order:

- **Cooperative cancellation.** The child's shutdown token fires. A
  well-behaved child observes it, finishes what it must, and exits. This
  is the only rung most children ever see.
- **Grace expiry.** Each child has a `Shutdown` policy carrying a grace
  duration. A child still running when its grace expires is escalated.
- **The tidy-abort beat.** The abort token fires — strictly after the
  shutdown token, always — and a short beat gives a cancelled child one
  last chance to yield a final cleanup step.
- **Hard abort.** The child's future is dropped.

`Shutdown::Abort` is not a second mechanism; it is the zero-grace point on
the same ladder. The tokens still fire in order and the beat still runs.

## Grace is an upper bound

Grace is a supervisor-side budget, not a guaranteed amount of CPU time. A
child whose shutdown token fires gets *at most* its grace; scheduler
latency and competing work can reduce what its code actually observes.
Size grace for the work teardown must do, and treat everything past the
cooperative rung as the system defending itself, not as a code path to
rely on.

## The frozen mailbox prefix: drain or discard

When an actor starts stopping, its mailbox intake freezes unconditionally:
new sends are rejected from that point. The only choice — the actor's
`MailboxShutdown` policy — is what happens to messages *already accepted*
before the freeze. `Drain` (the default) delivers that frozen prefix
through `handle` before `on_stop` runs; `Discard` drops the prefix and
proceeds straight to `on_stop`. One grace budget covers both the drain and
`on_stop`, so a resource owner's grace must be sized for drain plus close,
or the policy set to `Discard` when the close matters more than the
backlog.

`on_stop` is best-effort resource return, not a durability boundary. A
handler error, a panic, or a hard abort can truncate the drain or skip
`on_stop` entirely. Anything that must survive a crash has to be durable
already; `on_stop` is for giving connections, files, and registrations
back promptly on the orderly path.

## A bounded shutdown, end to end

The example `crates/shelterwood/examples/graceful_shutdown.rs` puts all of
this in one program. The actor owns a counter standing in for a resource:

```rust
{{#include ../../crates/shelterwood/examples/graceful_shutdown.rs:actor}}
```

`handle` accumulates values; `on_stop` marks the resource closed. Because
the mailbox policy below is `Drain`, `on_stop` runs only after the frozen
prefix has been delivered, all within the same grace.

The tree declares a cooperative task alongside the actor:

```rust
{{#include ../../crates/shelterwood/examples/graceful_shutdown.rs:declare}}
```

The task is the cooperative rung in its purest form: it parks on
`shutdown_token().cancelled()` and returns as soon as the token fires.
The actor opts into `MailboxShutdown::Drain` explicitly, making the
delivery guarantee visible at the declaration site.

Then the shutdown itself:

```rust
{{#include ../../crates/shelterwood/examples/graceful_shutdown.rs:shutdown}}
```

The send is accepted before shutdown begins, so it is part of the frozen
prefix and `Drain` guarantees its delivery ahead of `on_stop` — even if
teardown wins the race to freeze intake. The two assertions after
`shutdown` returns are the whole contract in miniature: the accepted
message was handled, and the resource was returned.

## Ordered scopes stop in reverse; dynamic scopes stop together

An ordered tree stops its children in reverse declaration order, one fully
joined child at a time. Each child gets its full grace, so per-child
graces *sum* in an ordered scope. This is the shutdown half of the
ordering bargain from
[Readiness and startup order](readiness-startup-order.md): declare a slow
resource owner early so its dependents stop first and its close runs
quiescent.

A dynamic scope instead starts every member's stop ladder at once and
drains concurrently — grace clocks run in parallel, not summed.
[Dynamic scopes](dynamic-scopes.md) covers that model.

## `System::shutdown`

`System::shutdown(timeout)` consumes the owner, requests teardown, and
joins the root driver before returning. The timeout bounds the cooperative
phase — after it expires, stragglers are escalated down the ladder rather
than waited on forever, and the call returns a `ShutdownTimeout` error to
report that escalation happened. Dropping a `System` requests graceful
shutdown too, but the explicit call is the form a host should await before
tearing down its runtime.

That is the whole ladder at book altitude. The full contract — what
"straggler" means precisely, how `run_blocking` threads detach past hard
abort, and the owner-and-runtime lifetime obligations for embedding hosts
— lives in the rustdoc guide
[`guides::shutdown_and_resources`](https://docs.rs/shelterwood/latest/shelterwood/guides/shutdown_and_resources/index.html).
Read it before shipping a host that must bound its own exit.

Next, [Tasks and offloads](tasks-and-offloads.md) introduces the
supervised peers of actors — including the cooperative task shape this
chapter's example already used.
