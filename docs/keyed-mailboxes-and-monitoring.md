# Keyed mailboxes and peer monitoring

Keyed latest-value mailboxes and peer watches are deliberately separate
tools. A keyed mailbox bounds state-like application traffic by key. A watch
delivers membership transitions through a separate actor-loop source, so
those transitions do not consume mailbox capacity or participate in mailbox
conflation.

## Keyed latest-value mailboxes

Use `latest_by_key` when each key represents replaceable state and only its
newest pending value matters:

```rust
let definition = RawDef::factory(make_worker).latest_by_key(
    KeyedCapacity::Explicit(NonZeroUsize::new(256).unwrap()),
    |update: &Update| update.instrument_id,
);
```

The capacity is the maximum number of distinct pending keys. Size it to at
least the expected key cardinality. `KeyedCapacity::Inherit` uses the resolved
scope or library mailbox capacity.

An update for an existing key replaces that key's pending message in place.
Replacement does not refresh its eviction age. An update for a new key when
the mailbox is full evicts the oldest pending key and is accepted immediately;
it never waits for capacity and never returns a full-mailbox error. Both
replacement and eviction can drop an embedded `Reply`, so calls require the
same explicit idempotency and reconciliation discipline as calls through a
single-slot latest-value mailbox.

The key function runs synchronously on the sender's acceptance path and may
run concurrently on several sender threads. Keep it pure, cheap, and
non-blocking. If it panics, the panic stays on the sender stack: the message is
not accepted and the actor does not exit.

A keyed mailbox is not a priority or control lane. A reserved “control” key
can still be evicted by enough distinct data keys, and an older pending key is
not delivered ahead of newer keys by priority. M12's trading-engine acceptance
port showed that a capacity sized to the expected data/control key cardinality
admits an urgent `try_send` under same-key data flood, so the evidence gate
closed **do not build** for a first-class control lane. This does not make a
keyed mailbox safe for irreplaceable barriers: put traffic that must never be
replaced or evicted on a non-conflating protocol path.

## Peer watches

An actor can watch the membership behind an `ActorRef`, `TaskRef`, or
`ScopeRef`:

```rust
context.watch(&peer, Message::PeerEvent)?;
let lease = context.watch_scoped(&task, Message::TaskEvent)?;
let removed = context.unwatch(&peer);
```

Each watch owns an independent bounded queue of 128 raw `MonitorEvent`s.
Events preserve FIFO order within that watch. When the queue overflows, the
oldest edges are dropped and one leading `Lagged { dropped }` marker reports
the exact loss; a terminal `Removed` edge is always retained as the newest
edge. The mapping closure runs only when the watcher actor dequeues an event.
Monitor delivery counts as received work, but never as mailbox acceptance.

Watching a currently running target queues an immediate `Started`. Watching a
membership that has not spawned waits for its first real `Started`; it does
not invent an incarnation. Watching an already-terminal membership queues its
final `Removed`. Actor, task, and scope events all carry the target's stable
membership token, member kind, child id, and incarnation evidence.

A watch belongs to the watcher incarnation and is cancelled when that
incarnation stops. Registering the same target again replaces the queued
events and mapping closure without adding another immediate edge.
`unwatch` and dropping a `watch_scoped` guard synchronously close the watch and
discard queued edges. If cancellation races a dequeue, at most the one event
already owned by the actor loop can still invoke its mapping closure.
