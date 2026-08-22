# Observation

Shelterwood ships two restart-stable ways to watch a running system from
outside, without instrumenting any child. `snapshot()` is authoritative
recursive current state, and snapshot subscriptions conflate to the latest
value. Lifecycle subscriptions are bounded, ordered histories of every edge
in the subscribed scope and all of its descendants. Both surfaces are owned
by the scope *membership*, not one incarnation, so they remain valid across
scope restarts.

## Subscribe, then snapshot

Subscriptions begin "now"; they never replay history. To attach an observer
without a gap, use the catch-up protocol — both at initial attachment and
whenever the stream reports `Lagged`:

1. Subscribe to lifecycle events.
2. Read `snapshot()` after subscribing and treat it as ground truth.
3. Discard queued events already reflected by that snapshot.
4. Apply later events as deltas. On `Lagged`, restart from a fresh
   snapshot.

```rust
{{#include ../../crates/shelterwood/examples/observation.rs:subscribe_then_snapshot}}
```

Step 3 is a sequence comparison. Every emitting scope has a membership
token and its own sequence. A recursive `ScopeSnapshot` carries
`lifecycle_seq` for the subscribed scope itself; a scope child additionally
carries `scope_seq`, including during a restart window when its nested
snapshot is temporarily absent. For an event emitted by the subscribed
scope, compare `event.scope` with the handle's `membership()` and use
`snapshot.lifecycle_seq`; for a descendant event,
`snapshot.watermark(event.scope)` finds the matching watermark. An event is
already reflected exactly when `event.seq <= watermark`:

```rust
{{#include ../../crates/shelterwood/examples/observation.rs:watermark}}
```

These values are `LifecycleSeq` tokens. `LifecycleSeq::EXHAUSTED` is the
permanent watermark after the sequence space is exhausted and is never
assigned to an event; `get()` exposes the underlying `u64` when a numeric
integration requires one.

If an event's scope token is absent from the snapshot entirely, use causal
introduction rather than guessing:

- if you already applied a later `Added` whose membership is that scope
  token, the scope is new and has no prior watermark; apply its causally
  following events;
- otherwise it is a stale membership whose removal the snapshot already
  reflects; discard the event.

### Lagged marks an overflow episode

`Lagged { dropped }` is a subscription-level stream item, not a lifecycle
event: it has no scope path or sequence, because the dropped events may
have come from several descendant scopes. The marker *leads* its overflow
episode — it arrives before the retained events, which may be older than
the snapshot you resync against and may themselves still be evicted by
further overflow. Resynchronize with the subscribe-then-snapshot protocol
instead of reasoning about what follows the marker. Buffers are per
subscriber, so a slow observer only ever loses its own view; it cannot
cause another observer to drop events.

### Restart deadlines

A child in `Restarting` normally exposes its exact absolute backoff
deadline through `restart_at`. The runtime waits for very distant points in
bounded internal timer slices, so the timer implementation cannot clamp
them to an earlier tick. If the requested point cannot be represented or
safely scheduled, `restart_at` is `None`: no earlier or alternative public
deadline is substituted, and the child remains in `Restarting` until
removal or shutdown rather than restarting immediately.

## Identity and paths

An event's `scope_path` is a human-readable child-id path relative to the
subscribed scope. Child ids are reusable, so the path is not a fence. To
distinguish a same-id replacement from stale traffic, use the event's
`scope` membership and the membership carried by each `Added`/`Removed`
event.

An `ActorRef` follows all incarnations of one membership but never follows
a same-id replacement membership. Compare incarnations with `supersedes`,
never with an expected generation number: several restarts may occur
between two observations. The
[identity chapter](identity-incarnations-retries.md) builds the retry
discipline on these rules.

## Waits must accept at-or-past state

Each snapshot-watch value is a complete observation transaction: compound
changes such as an initial batch admission appear all at once, and an
ungated `borrow_latest` sees either the preceding committed cut or the
final one. Watches can still conflate intermediate *committed* states, so a
`wait_for_child` predicate must accept every state at or beyond the desired
edge, not only the single transition it hopes to catch. A readiness wait
should accept a running or terminal state according to the application's
protocol, and a restart wait should accept any incarnation that
`supersedes` the saved one. Pin the returned `membership` when a same-id
replacement would not satisfy the logical wait.

```rust
{{#include ../../crates/shelterwood/examples/observation.rs:wait_for_child}}
```

Give operational waits a finite deadline budget. The wait accepts any
`impl Into<DeadlineBudget>`, so a plain `Duration` reads naturally.
`Duration::ZERO` still examines the current snapshot once, so an
already-satisfied predicate succeeds. A budget too large for the platform
clock to represent (including `Duration::MAX`) behaves as an unbounded wait
rather than an immediate timeout. A missing child does not match yet,
because a future `Added` under that label may satisfy the wait; if the
containing scope terminalizes first, `wait_for_child` returns the terminal
scope state instead.

## Terminal state is not pruning

Terminalization records a child's final state and emits its terminal
lifecycle edge. Pruning removes that membership from scope residency. The
retention option chooses only the distance between the two edges. With
`Retention::Retain`, a terminal child remains as an observable tombstone
and keeps making its id a duplicate until explicit removal or scope
teardown; it does not remain live and cannot restart. With
`Retention::Remove`, pruning follows terminalization immediately. Explicit
removal, successful exact-handle retirement, and containing-scope teardown
also prune. The lifecycle `Removed` event describes the pruning edge — do
not infer that a terminal snapshot must immediately disappear.

For a planned replacement, retain the exact handle returned by admission,
remove that membership, await `RemoveOutcome::Removed`, then add the
replacement. An add racing an in-progress same-id removal fails with
`RemovalInProgress`; await the removal and retry. The replacement's
membership is distinct rather than a new incarnation of the old one:
terminalization evicts the old id's lineage, so the replacement and the
removed membership are deliberately incomparable in both directions, just
like different ids and different owning scopes.

## Pre-spawn observation

Scope handles, snapshots, and subscriptions exist before spawn, with an
initial scope state of `Unstarted`. Dropping an unspawned tree terminalizes
its declared members as never started, publishes each final state and
event, and closes the streams — so pre-spawn observation and shutdown waits
resolve structurally instead of hanging.
