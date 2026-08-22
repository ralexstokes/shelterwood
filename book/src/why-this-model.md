# Why this model

Asynchronous Rust makes it easy to start work and hard to own it. A bare
`spawn` returns a join handle and nothing else: no startup ordering, no
answer to "is it ready?", no restart when it fails, no bounded way to stop
it, and no way to watch it from outside. Every project reinvents those
pieces — usually as a mesh of channels, `Arc<Mutex<_>>` state, and shutdown
flags that each module must remember to check.

Shelterwood replaces the mesh with a tree. You declare a system as a tree
of supervised children, and the tree — not each caller — owns the whole
lifecycle:

- **Startup order.** In an ordered `Tree`, declaration order is startup
  order, and each child's readiness gates the next. A database pool is
  ready before the server that needs it starts.
- **Readiness.** "Started" and "ready" are distinct, and readiness is a
  first-class signal rather than a sleep in a test.
- **Restart.** Supervision policy is plain, validated data: what counts as
  a failure, how often restart may happen, and with what backoff.
- **Mailboxes.** Actor mailboxes are bounded, declared per child, and
  frozen and drained (or discarded) at shutdown by declared policy.
- **Shutdown.** One escalation ladder — cooperative cancellation, grace
  expiry, then abort — runs children down in reverse declaration order,
  under a budget you choose.
- **Observation.** Recursive snapshots and lifecycle event streams report
  the tree's state from outside, without instrumenting each child.

## Identity instead of a registry

There is no global name registry. A child's identity is structural: a
`Membership` names its slot in a scope, stable across restarts and never
reused by a same-id replacement, and an `Incarnation` names one run of that
membership. Handles like `ActorRef` address memberships, so they follow
restarts automatically and fail only when the membership is terminal.
Messaging results carry the accepting incarnation as evidence, which makes
"did my request land before or after the crash?" a question with a checkable
answer. The [identity chapter](identity-incarnations-retries.md) builds the
retry discipline on top of this.

## Actors and tasks are peers

Supervision does not require a mailbox. A child of a scope is an actor (a
mailbox-owning message loop), a plain supervised task (an arbitrary future
with a `TaskContext`), or a nested scope — and all three get the same
startup, restart, shutdown, and observation treatment. A background sweeper
that never receives a message is declared as a task; nothing forces a
message protocol onto it merely to obtain supervision. Dynamic scopes admit
and remove members at runtime when membership cannot be fixed at build time.

## The correctness posture

The design principles in the specification (SPEC §1) shape the whole
surface. Invalid configuration is unrepresentable: policy values are
validated at construction, and duplicate or empty child ids are rejected at
declaration, not discovered at spawn. Delivery is at-most-once with no
hidden buffering across restarts, so what the mailbox promises is exactly
what it does. Error inventories are exhaustive by design — match without a
wildcard arm and the compiler surfaces new variants as decisions. Panics in
child code are classified as exits and handled by supervision (under
`panic = "unwind"`; an abort ends the process before any supervisor can
see it). And shutdown is bounded: a misbehaving child delays teardown by at
most its grace, then is escalated. The public API is runtime-independent;
the current adapter runs on an ambient Tokio runtime.

## The shape in one page

Here is a complete system: a counter actor with a request/reply protocol,
declared under an ordered tree, spawned, exercised, and shut down.

```rust
{{#include ../../crates/shelterwood/examples/quickstart.rs:quickstart}}
```

Everything the rest of this book covers is visible in miniature here: a
declaration built before anything runs, a `System` that solely owns the
running root, readiness awaited rather than assumed, a deadline on the
call, and a budgeted shutdown. [A first system](first-system.md) walks
through this program line by line.
