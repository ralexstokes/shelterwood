# Calls, retries, and message ordering

`ActorRef::call` uses one deadline for both mailbox acceptance and the reply.
Its result always carries identity evidence: `Replied<T>` identifies the
incarnation that accepted a successful request, while `CallError` records the
accepting or observed incarnation when one exists. Treat these tokens as part
of the protocol, not merely diagnostic metadata.

## The retry decision

| Result | What is known | Required action |
| --- | --- | --- |
| `AcceptanceTimedOut` | The request was withdrawn and was not accepted. | Retrying is safe. Keep it within the logical operation's overall deadline. |
| `ResponseTimedOut` | The request was accepted; its effect and reply are unknown. | Do not resend blindly. Reconcile against durable application state. |
| `ReplyDropped` | The request was accepted, but its `Reply` was dropped unanswered. | Retry only an idempotent operation, only under one overall deadline, and only after a newer incarnation is running. |
| `Terminated` | The target membership ended before acceptance. | That handle can never follow a same-id replacement. Obtain the replacement's new handle deliberately. |

For a safe `ReplyDropped` retry in the core API:

1. Give the logical operation a durable id and make duplicate processing
   idempotent.
2. Save `CallError::incarnation_observed`; it identifies the incarnation that
   accepted and then lost the reply.
3. Observe snapshots or lifecycle events until the live incarnation
   `supersedes` the saved one. Do not assume the next generation is exactly
   `old + 1`: several restarts may happen between observations.
4. Resend the same operation id using only the time left in one overall
   deadline.
5. Bound the retry horizon and garbage-collect the application's idempotency
   ledger only after a retry is no longer possible.

`ResponseTimedOut` deliberately has no equivalent retry recipe. Acceptance
happened, so only application ground truth can distinguish “not applied,”
“applied without a reply,” and “still running.” Reconcile first. The shard-store
acceptance test demonstrates both the post-commit reconciliation case and the
idempotent `ReplyDropped` loop.

Membership and incarnation answer different questions. An `ActorRef` follows
restarts of one membership. After remove and re-add under the same child id,
the replacement has a new `Membership`; neither membership supersedes the
other, and an old handle never retargets it. Within one membership,
`Incarnation::supersedes` is the correct restart ordering test.

## Latest-value mailboxes and calls

`Mailbox::latest()` stores only the newest accepted message. Replacing a call
message drops its embedded `Reply`, so that caller receives `ReplyDropped`.
Calls are allowed on a latest-value mailbox, but this behavior makes them a
correctness trap unless conflation is an intentional, idempotent part of the
protocol. Prefer a queue mailbox for commands and calls; reserve `latest()` for
state-like updates where only the newest value matters.

`Reply::send` consumes the capability and is infallible. If the caller has
cancelled or timed out, the value is discarded. Actor code should not branch on
caller liveness.

## Ordering is intentionally narrow

A queue mailbox guarantees FIFO only for messages sent by the same sender task,
accepted by the same actor incarnation. There is no order guarantee across
sender tasks, across incarnations, or between mailbox messages and offload
completions. A latest-value mailbox orders by replacement: only its newest
accepted survivor is delivered.

Actor-local `continue_with` messages run ahead of external input, with a
fairness turn between consecutive continuations. Use this when a handler needs
to split work into protocol steps without awaiting itself.

## Never call and await `myself()` in a handler

Awaiting `context.myself().call(...)` from `handle` deadlocks: the reply can be
produced only by the actor loop that the current handler is blocking. Use one
of these shapes instead:

- `continue_with` for a later step on the same actor;
- call another actor through an incarnation-owned `offload`, returning the
  result as a continuation message; or
- split `Reply::channel()`, send the request without awaiting it in the
  handler, and await the receiver from an offload.

Offloads use one deadline and their completion returns through the actor loop.
Cancellation suppresses the continuation, so no offload extends the actor
incarnation that created it.
