# Actor statistics and loss-recovering reducers

The M10 observation layer packages common projections over Shelterwood's
membership identities, authoritative scope snapshots, and bounded lifecycle
subscriptions. The adapters are runtime-agnostic library types: they are
`Send`, expose `next()` directly, and close only after their scope membership
has published its final state.

## Actor statistics

`ActorRef::stats()` reads one `ActorStatsSnapshot` on demand. Counters saturate
at `u64::MAX` and belong to the membership, so they survive incarnation
restarts. `observed_incarnation` identifies the mailbox binding sampled with
the counters. Attribute work to one incarnation by differencing snapshots only
when their tokens provide the fence the application needs.

`ScopeRef::stats_recursive()` returns actors in recursive snapshot child order.
Each row retains declaration metadata distinguishing callback `Actor` children
from loop-owning `RawActor` children. `path` identifies the containing scope
relative to the queried scope; `id` is the actor's id in that scope. Tasks and
scope rows are not guessed from runtime attachments.

Message byte accounting is opt-in and typed:

```rust
let definition = shelterwood::RawOnceDef::new(actor)
    .message_size(|message: &Request| message.encoded_len());
```

The observer executes on the sender's ingress stack. A panic resumes there
without poisoning the mailbox, as for `latest_by_key` extraction and
`ActorRef::contramap`. Rejected ingress contributes no bytes, and
`message_bytes_accepted` is `None` when no observer is installed.

Counter edges are deliberately narrow:

| Edge | Effect |
|---|---|
| FIFO or non-replacing conflating acceptance | `messages_accepted += 1`; add measured bytes |
| actor loop dequeues a mailbox message | `messages_received += 1` |
| latest or same-key replacement | accepted and bytes as above; `messages_conflated += 1` |
| new-key acceptance evicts another key | accepted and bytes as above; `messages_evicted += 1` |
| offload, timer, or monitor delivery dequeues | `messages_received += 1`, never accepted |
| ingress rejects before acceptance | `sends_rejected += 1`, never bytes |

Actor-local `continue_with` work is intentionally outside received ingress and
external-delivery accounting. `mailbox_depth`, `mailbox_capacity`, and
`outstanding_offloads` are current gauges rather than cumulative counters.

## Child observation

`ScopeRef::observe_children()` performs the subscribe-then-snapshot catch-up
protocol. Its first item is always:

```text
Reset { snapshot, dropped: 0 }
```

Ordinary lifecycle edges become `Changed { snapshot, cause }`, pairing the
cause with consistent-or-newer authoritative state. Subscriber overflow is
never exposed as raw `LifecycleItem::Lagged`: the observer reads a fresh
snapshot and returns only `Reset { snapshot, dropped }`. Replace reducer state
wholesale on every reset.

`ScopeRef::restart_counts()` applies the same recovery protocol to
`ScopeSnapshot.total_restarts`. It suppresses lifecycle edges that do not
change the total. Every result contains the authoritative `total`, a saturating
`delta` from the prior emitted total, and `resynced = true` for the initial
sample or a loss-recovery reset. A lag therefore cannot double-charge a
restart or hide cumulative work.

## Metrics feature

The disabled-by-default `metrics` feature emits the same structured samples
through the `metrics` facade. Every instrument carries `scope.path`,
`actor.id`, and `actor.membership`. The exported names are exactly:

- counters: `shelterwood.actor.messages_accepted`,
  `shelterwood.actor.messages_received`,
  `shelterwood.actor.messages_conflated`,
  `shelterwood.actor.messages_evicted`,
  `shelterwood.actor.message_bytes_accepted`, and
  `shelterwood.actor.sends_rejected`;
- gauges: `shelterwood.actor.outstanding_offloads`,
  `shelterwood.actor.mailbox_depth`, and
  `shelterwood.actor.mailbox_capacity`.

The byte counter is emitted only for actors with a size observer. With no
global recorder installed, the facade keeps the observation path inert.
