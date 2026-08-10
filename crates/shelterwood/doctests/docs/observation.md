# Snapshots and lifecycle events

Shelterwood provides two restart-stable observation surfaces. `snapshot()` is
authoritative recursive current state and snapshot subscriptions conflate to
the latest value. Lifecycle subscriptions are bounded ordered histories for the
subscribed scope and all descendants. Both are owned by the scope membership,
so they remain valid across scope incarnations.

## Subscribe, then snapshot

Subscriptions begin “now”; they do not replay history. Use this catch-up
protocol both at initial attachment and after `LifecycleItem::Lagged`:

1. Subscribe to lifecycle events.
2. Read `snapshot()` after subscribing and treat it as ground truth.
3. Discard queued events already reflected by the snapshot.
4. Apply later events as deltas. On `Lagged`, restart from a fresh snapshot.

Every emitting scope has a membership token and sequence. A recursive
`ScopeSnapshot` carries `lifecycle_seq` for itself; a scope child also carries
`scope_seq`, including during a restart window when its nested snapshot is
temporarily absent. For an event emitted by the subscribed scope itself, compare
`event.scope` with the scope handle's `membership()` and use
`snapshot.lifecycle_seq`. For a descendant event,
`snapshot.watermark(event.scope)` finds the matching watermark. An event is
already reflected when `event.seq <= watermark`.

These values use `LifecycleSeq`. `LifecycleSeq::EXHAUSTED` is the permanent
watermark after the sequence space is exhausted and is never emitted on an
event; `get()` exposes the underlying `u64` when numeric integration requires it.

A child in `Restarting` normally exposes its exact absolute backoff deadline
through `restart_at`. The runtime waits for very distant points in bounded
internal timer slices, so the timer implementation cannot clamp them to an
earlier tick. If the requested point cannot be represented or safely scheduled,
`restart_at` is `None`; no earlier or alternative public deadline is
substituted, and the child remains in `Restarting` until removal or shutdown
rather than restarting immediately.

If the event's scope token is absent from the snapshot, use causal introduction:

- if you applied a later `Added` whose membership is that scope token, the new
  scope has no prior watermark; apply its causally following events;
- otherwise it is a stale membership whose removal the snapshot already
  reflects; discard it.

`Lagged { dropped }` is a subscription item, not a lifecycle event. It has no
scope path or sequence because the dropped events may have come from several
descendant scopes. Each subscriber has its own buffer, so a slow observer
cannot cause another observer to lose events.

## Identity and paths

`scope_path` is a human-readable child-id path relative to the subscribed
scope. Child ids are reusable, so the path is not a fence. Use the event's
`scope` membership and each `Added`/`Removed` membership to distinguish a
replacement from stale traffic.

An `ActorRef` follows all incarnations of one membership but never follows a
same-id replacement membership. Compare incarnations with `supersedes`, not an
expected generation number. Several restarts may occur between observations.

## Waits must accept at-or-past state

Snapshot watches conflate intermediate states. A `wait_for_child` predicate
must accept every state at or beyond the desired edge, not only the single
transition it hopes to catch. For example, a readiness wait should accept a
running or terminal state according to the application's protocol, and a
restart wait should accept any incarnation that `supersedes` the saved one.
Pin the returned `membership` when a same-id replacement would not satisfy the
logical wait.

Use a finite deadline for operational waits. `Duration::ZERO` still examines
the current snapshot once, so an already-satisfied predicate succeeds. A
duration too large for the platform clock to represent (including
`Duration::MAX`) behaves as an unbounded wait rather than an immediate timeout.
A missing child does not match yet because a future `Added` under that label may
satisfy the wait. If the containing scope terminalizes first, `wait_for_child`
returns its terminal scope state.

## Terminal state is not pruning

Terminalization records the final child state and emits its terminal lifecycle
edge. Pruning removes that membership from scope residency. With
`Retention::Retain`, a terminal child remains as an observable tombstone and
continues to make the id a duplicate until explicit removal or scope teardown.
It does not remain live and cannot restart.

With `Retention::Remove`, pruning follows terminalization. Explicit removal,
successful exact-handle retirement, and containing-scope teardown also prune.
The lifecycle `Removed` event describes that pruning edge; do not infer that a
terminal snapshot must immediately disappear.

For planned replacement, retain the admission receipt or exact handle, remove
that membership, await `RemoveOutcome::Removed`, then add the replacement. An
add racing an in-progress same-id removal fails with `RemovalInProgress`; await
the removal and retry. The replacement's membership is distinct rather than a
new incarnation of the old one, and it supersedes the removed same-id
membership. Different ids and different owning scopes remain incomparable.

## Pre-spawn observation

Scope handles, snapshots, and subscriptions exist before spawn. Their initial
scope state is `Unstarted`. Dropping an unspawned tree terminalizes declared
members as never started, publishes the final state/event, and closes streams;
pre-spawn observation and shutdown waits therefore resolve structurally instead
of hanging.
