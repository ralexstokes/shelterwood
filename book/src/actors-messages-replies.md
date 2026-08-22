# Actors, messages, and replies

An actor is owned state driven by a message loop the framework runs for
you. The `Actor` trait is the whole contract: `init` builds the state,
`handle` processes one message at a time, and `on_stop` is best-effort
teardown. This chapter uses the request/reply example
(`crates/shelterwood/examples/request_reply.rs`), which CI compiles and
runs.

## The contract

```rust
{{#include ../../crates/shelterwood/examples/request_reply.rs:actor}}
```

`init` runs on every start of the membership — the first start and every
restart — receiving a fresh copy of `Args` and a `Context`. Acquire
per-incarnation resources here; a failed `init` is a classified exit for
the supervisor, like any other failure.

`handle` gets `&mut self`, one message, and the same `Context` (which also
offers `myself()`, timers, continuations, and offloads — see
[Tasks and offloads](tasks-and-offloads.md)). Returning `Ok(())` continues
the loop; an `Err` or a panic ends the incarnation with a classified exit.

`on_stop` (not needed above) runs at orderly stop, after the frozen mailbox
prefix is drained or discarded, inside the child's grace budget. It
receives a narrowed `StopContext` and is best-effort resource return, not a
durability boundary — a panic or hard abort can skip it. The
[shutdown guide](https://docs.rs/shelterwood/latest/shelterwood/guides/shutdown_and_resources/index.html)
covers what it may rely on.

Messages are one enum per actor: the protocol in a single type, matched
exhaustively. A fire-and-forget variant carries only data; a request
variant embeds a `Reply<T>`. `Reply` is a one-shot capability:
`reply.send(value)` consumes it and is infallible (if the caller has given
up, the value is discarded — actor code never branches on caller liveness).
Dropping it unanswered is itself an observable outcome: the caller sees
`ReplyDropped`.

## Split reply: send and response as separate steps

```rust
{{#include ../../crates/shelterwood/examples/request_reply.rs:split_reply}}
```

`reply_channel` mints the pair explicitly: a `Reply` to embed in the
message and a `ReplyReceiver` to await. The send and the response then have
independent outcomes. Here `try_send` is fail-fast — it returns the
accepting incarnation on success, or a `SendError` (`Full`, `NotRunning`,
`Terminated`) without waiting — and `recv` bounds only the response wait.
Acceptance evidence belongs to the send's own result, not the receiver.

Use the split when the two halves genuinely have different homes: sending
from a place that must not block (a teardown path, another actor's
handler), awaiting the response somewhere else, or giving acceptance and
response different deadlines.

## `call`: the packaged form

```rust
{{#include ../../crates/shelterwood/examples/request_reply.rs:call}}
```

`call` bundles the same pieces — construct the message around a fresh
`Reply`, get it accepted, await the response — under **one deadline**. The
budget starts when the call future is first polled, before the message
constructor runs, and covers construction, acceptance, and the reply. There
is no per-phase budget to reason about, and a `CallError` tells you which
side of acceptance the deadline landed on. This is the default shape for
ordinary request/reply; reach for the split only when you need its extra
degrees of freedom.

A successful call resolves to `Replied`, pairing the value with the
incarnation that accepted the request. That token is identity evidence, not
diagnostics: it names exactly which run of the actor answered, and the
example asserts it equals the incarnation `try_send` reported earlier.
`CallError` carries the same evidence on the post-acceptance failures.

## Was the request accepted?

Every messaging error answers that one question. Pre-acceptance failures
(`AcceptanceTimedOut`, `Terminated`, `Full`, `NotRunning`, `TimedOut`) are
guaranteed-never-accepted, so retrying cannot duplicate an effect.
Post-acceptance failures (`ResponseTimedOut`, `ReplyDropped`) have unknown
effects, and only application ground truth licenses a resend. The
[error catalog](https://docs.rs/shelterwood/latest/shelterwood/guides/errors/index.html)
tabulates every kind and its pinned evidence, and the
[retry guide](https://docs.rs/shelterwood/latest/shelterwood/guides/retry_and_ordering/index.html)
turns the evidence into a recipe — including the one hard rule inside a
handler: never `call` and await `myself()`. The
[identity chapter](identity-incarnations-retries.md) walks the discipline
in book form.

## Mailbox kinds, briefly

A mailbox is declared per actor as `Mailbox`: a bounded **queue** (FIFO per
sender task within one incarnation, backpressure when full) or
**latest-value** (`Mailbox::latest()`, which keeps only the newest accepted
message by replacement). Prefer a queue for commands and calls — conflating
a call message drops its embedded `Reply`, so that caller sees
`ReplyDropped` — and reserve `latest()` for state-like updates where only
the newest value matters. Mailbox choice, capacity, and shutdown drain
policy are declared with the child; see
[Supervision policy](supervision-policy.md).
