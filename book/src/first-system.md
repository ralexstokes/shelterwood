# A first system

This chapter walks through the front-page quickstart: a counter actor with
a request/reply protocol, spawned under an ordered tree inside an ambient
Tokio runtime. The complete program lives in the repository as
`crates/shelterwood/examples/quickstart.rs`, which CI compiles and runs.

## The actor

```rust
{{#include ../../crates/shelterwood/examples/quickstart.rs:actor}}
```

`Counter` is plain owned state — no locks, no shared references. The actor
loop delivers one message at a time, so `handle` gets `&mut self` and the
count needs no synchronization.

`Msg` is the actor's protocol, one enum for everything it can receive.
`Add` is fire-and-forget. `Total` is a request: it carries a `Reply<u64>`,
a one-shot capability the handler consumes with `reply.send` to answer
whoever asked.

The `Actor` implementation supplies two of the three callbacks. `init`
builds the actor's state from its `Args` — this runs on every start,
including restarts, so it is where per-incarnation resources are acquired.
`handle` processes one message and returns `ExitResult`: `Ok(())` keeps the
actor running, while an error (or a panic) becomes a classified exit that
the supervising scope acts on. The third callback, `on_stop`, is optional
teardown; the [next chapter](actors-messages-replies.md) covers it.

## Declaring, running, and stopping

```rust
{{#include ../../crates/shelterwood/examples/quickstart.rs:run}}
```

Line by line:

- `Tree::new()` starts an ordered declaration. Nothing runs yet; the tree
  is a plan, and declaration order will be startup order and reverse
  shutdown order.
- `add_actor("counter", ActorDef::<Counter>::cloned(()))` declares one
  child. `cloned(())` supplies `Args` that are cloned for each start,
  which is what makes the actor restartable under supervision. The call
  returns an `ActorRef` immediately — a cheap, cloneable send handle that
  addresses the child's membership and follows it through restarts.
- `tree.spawn()` lowers the declaration and starts the root inside the
  ambient Tokio runtime. It returns a `System`: the sole owning handle,
  not `Clone`. Startup proceeds asynchronously from here.
- `system.wait_started()` resolves `Ok` once every declared child is ready,
  gated one child at a time in declaration order. A startup failure is
  reported here as a `StartupError` and deliberately leaves the
  successfully started prefix running and supervised, so the host chooses
  what happens next; `System::start_or_shutdown` is the variant that rolls
  a failed startup back instead.
- `counter.send(Msg::Add(2))` waits until the bounded mailbox accepts the
  message. Acceptance is the guarantee: the message is queued for this
  membership, with backpressure rather than an unbounded buffer behind it.
- `counter.call(Msg::Total, ...)` is request/reply under one deadline
  covering message construction, mailbox acceptance, and the response. It
  resolves to a `Replied`, pairing the value with the incarnation that
  accepted the request — evidence you will want once restarts enter the
  picture.
- `system.shutdown(Duration::from_secs(5))` consumes the owner, stops
  children in reverse declaration order, and joins the root before
  returning. The budget bounds cooperative teardown; a child that ignores
  its shutdown signal past its grace is escalated rather than waited on
  forever. Dropping a `System` requests graceful shutdown too, but an
  explicit `shutdown` is the form that lets you await and observe it.

## What you have

Thirty-odd lines bought a supervised system: a counter with single-threaded
mutable state and no locks, a bounded mailbox with backpressure, a typed
request/reply protocol with a deadline, startup you can await, and shutdown
that is bounded rather than hopeful. If `handle` returned an error or
panicked, the tree's supervision policy — not the caller — would decide
whether and when a fresh incarnation runs `init` again.

Next, [Actors, messages, and replies](actors-messages-replies.md) fills in
the actor contract and the messaging surface this example only touched:
`on_stop`, the send flavors, split reply channels, and what the identity
evidence in `Replied` is for.
