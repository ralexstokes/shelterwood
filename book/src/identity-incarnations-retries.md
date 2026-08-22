# Identity, incarnations, and the retry discipline

Shelterwood has no global name registry. Identity is structural, and it
comes in three distinct levels:

- A **child id** is a scope-local name: a non-empty string, unique among
  the resident memberships of its scope. Ids are labels for humans and
  traversal — they can be reused by a later, distinct child, so an id is
  never identity.
- A **`Membership`** is a process-wide key for one child's slot in a scope
  across restarts. It is created by declaration or dynamic insertion and
  ends at terminal removal; it is never reused.
- An **`Incarnation`** is one run of a membership. A restart mints a new
  incarnation of the same membership.

Handles address memberships. An `ActorRef` follows every incarnation of
*its* membership — it rides through restarts and fails only on terminality
— but it never follows a same-id replacement: after remove and re-add
under the same id, the replacement is a fresh membership with its own
handles, and the old handle reports `Terminated` forever. Terminalization
evicts the removed id's lineage, so the replacement and the removed
membership are deliberately incomparable in both directions, exactly like
different ids and memberships owned by different scopes.

## `supersedes` is the only ordering test

Within one membership, `Incarnation::supersedes` is the restart ordering
test. Never compare against an expected generation number: a restart storm
can advance an incarnation by more than one between two observations.
Snapshot watches also conflate intermediate committed states, so a wait
for "the restart" must accept any incarnation at or past the edge:

```rust
{{#include ../../crates/shelterwood/examples/supervision_restart.rs:restart_wait}}
```

The same handle keeps answering across the superseding incarnation — that
is membership addressing doing its job.

## The retry discipline

Every messaging failure answers one question first: **was the request
accepted?** A pre-acceptance failure is guaranteed-never-accepted, so
retrying cannot duplicate an effect. A post-acceptance failure has an
unknown outcome, so only application ground truth can license a resend.

`ActorRef::call` runs message construction, mailbox acceptance, and the
reply under one deadline, and every result carries identity evidence: a
success resolves to `Replied`, pairing the value with the accepting
incarnation, and a `CallError` records the accepting or observed
incarnation when one exists. Treat those tokens as part of the protocol,
not diagnostic metadata.

```rust
{{#include ../../crates/shelterwood/examples/request_reply.rs:call}}
```

The four call outcomes, compactly:

| Outcome | Accepted? | Action |
| --- | --- | --- |
| `AcceptanceTimedOut` | No — withdrawn | Safe to retry within the operation's overall deadline. |
| `Terminated` | No | The handle is permanently dead; obtain the replacement's new handle deliberately. |
| `ResponseTimedOut` | **Yes** | Never resend blindly; reconcile against durable state first. |
| `ReplyDropped` | **Yes** — reply lost | Retry only an idempotent operation, after a superseding incarnation is running. |

The authoritative tables — including per-kind identity evidence and the
`send`/`try_send` errors — live in the rustdoc
[retry and ordering guide][retry-guide] and the [error catalog][errors].

[retry-guide]:
  https://docs.rs/shelterwood/latest/shelterwood/guides/retry_and_ordering/index.html
[errors]:
  https://docs.rs/shelterwood/latest/shelterwood/guides/errors/index.html

## The safe `ReplyDropped` recipe

`ReplyDropped` means the request was accepted but its `Reply` was dropped
unanswered — typically because the accepting incarnation crashed. A safe
retry is a protocol, not a loop:

1. Give the logical operation a durable id and make duplicate processing
   idempotent.
2. Save `CallError::incarnation_observed` — the incarnation that accepted
   and then lost the reply.
3. Watch snapshots or lifecycle events until the live incarnation
   `supersedes` the saved one. Retrying earlier lands the resend in the
   same doomed mailbox or the restart's rebind window.
4. Resend the same operation id using only the time left in one overall
   deadline for the whole logical operation.
5. Bound the retry horizon, and garbage-collect the idempotency ledger
   only once a retry is no longer possible.

`ResponseTimedOut` deliberately has no equivalent recipe. Acceptance
happened, so only the application can distinguish "not applied", "applied
without a reply", and "still running" — reconcile first. And a
`Terminated` handle never becomes retryable: replacement is a deliberate
act with a new handle, as the [observation chapter](observation.md)
describes for planned replacement.
