# Outlines, direct hosting, and finite lifetimes

M11 adds three small seams over the same declaration, incarnation, mailbox,
and shutdown machinery used by supervised trees. None introduces a registry or
an alternate actor runner.

## Resolved declaration outlines

With the `serde` feature, `Tree::outline()` and `DynamicTree::outline()` borrow
a declaration and return its fully resolved policy/topology projection. The
projection includes inherited scope defaults, concrete mailbox capacities,
every child policy, declaration order, and child kind. It neither lowers nor
spawns the tree.

A concrete one-shot subtree is represented recursively. A restartable subtree
factory is deliberately `Opaque`: outlining never invokes user code, so policy
inside such a factory cannot be fingerprinted. An outline contains no actor
implementation, arguments, closure, or state. It describes a declaration; it
cannot rebuild one and is not a transport or distribution format.

Outlines are suitable for golden tests and startup logging. Their serde schema
is intentionally strict: unknown fields and missing required fields are
errors. A new declaration option therefore has to add its resolved outline
field in the same change.

## One-incarnation hosting

With the `host` feature, `Hosted`, `HostedRaw`, and `HostedTask` expose exactly
one structurally non-restarting incarnation. The returned actor or task ref has
ordinary membership and incarnation tokens. `HostedHandle::wait_ready()`
observes the ordinary readiness gate; `shutdown(grace)` applies the usual
cooperative/escalation ladder and joins; `wait()` joins natural completion.

Hosted code receives the same typed `Exit` as supervised code, including
`Failed`, `ReadinessTimedOut`, `Aborted`, and `Panicked`. Do not add a local
`catch_unwind` around hosted user code. Dropping the non-clone owner starts
shutdown with `HostOptions`' configured grace; explicitly await `shutdown` when
resource teardown must be known complete.

## Cross-actor timers are mailbox traffic

`send_after_to` waits for its delay and then performs a full mailbox `send`.
`interval_to` starts after one period and performs one `try_send` per tick; a
full or temporarily unbound target skips that tick, with no catch-up burst.
Zero interval periods are rejected. A zero one-shot delay is an asynchronous
ordinary turn.

This differs from keyed self-timers: cross-actor deliveries transit the target
mailbox, so its capacity and conflation policy apply and its accepted counters
advance. The returned `Guard` is owned jointly by its lease and the sender
incarnation. Guard drop means cancel; `detach()` removes only lease ownership,
so sender restart or exit still cancels the timer. Once mailbox acceptance
wins, that message is no longer retractable.

## Finite systems and sibling barriers

`System::run_until_all(tasks, grace)` consumes the owner, waits for every
selected `TaskRef` in input order, then shuts the root down. The result always
retains every selected membership and exit, even when shutdown reports forced
stragglers. An empty selection proceeds immediately to shutdown. Any-of remains
an ordinary user-level `select`.

`await_sibling_ready(id, deadline)` replaces an offloaded snapshot wait. It is
scope-relative and returns the ready sibling snapshot, whose membership fences
later same-id reuse. An ordered child may await only an earlier declaration;
self or later waits return `WouldDeadlock`, and undeclared ids return
`UnknownSibling`. Dynamic scopes allow an absent id to remain pending for later
admission. The operation is rejected while an actor is draining and is absent
from `StopContext`.
