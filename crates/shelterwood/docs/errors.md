# The error catalog

Which error type means what, and what to match on. Messaging errors carry
*identity evidence* — the incarnation observed or accepting — because the
retry discipline consumes it; treat those tokens as part of the protocol,
not diagnostic metadata.

## One rule before the tables

Every kind below answers one question: **was the request accepted?** A
pre-acceptance failure is guaranteed-never-accepted, so retrying cannot
duplicate an effect. A post-acceptance failure has an unknown outcome, so
only application ground truth can license a resend. The
[retry guide](crate::guides::retry_and_ordering) turns this into a recipe.

## `SendError` (`send`, `try_send`, `send_timeout`)

The failed message is recovered in the error's public `message` field, so
a failed send loses nothing.

| `SendErrorKind` | Returned by | Meaning | Accepted? |
| --- | --- | --- | --- |
| `NotRunning` | `try_send` only | Membership currently not accepting: a restart rebind window, or intake frozen at stop. | No — safe to retry or reroute. |
| `Full` | `try_send` only | Queue mailbox at capacity (latest-value mailboxes accept by replacement instead). | No — apply backpressure. |
| `Terminated` | all flavors | Membership is terminal. The only failure plain `send` can return: it waits through rebind windows and backpressure. | No — the handle is permanently dead; a same-id replacement needs a new handle. |
| `TimedOut` | `send_timeout` only | Deadline hit; the message was withdrawn and recovered. | No — guaranteed-never-accepted. |

`incarnation_observed` is pinned per kind: `Full` always names the bound
incarnation; `NotRunning` names the stopping incarnation at an intake
freeze and is `None` in a rebind window or pre-spawn; `Terminated` names
the membership's final incarnation (`None` if none ever ran); `TimedOut`
names the newest incarnation observed bound during the attempt.

## `CallError` (`call`)

One deadline covers message construction, mailbox acceptance, and the
reply.

| `CallErrorKind` | Meaning | Accepted? | Required action |
| --- | --- | --- | --- |
| `Terminated` | Membership terminal before acceptance. | No | Obtain the replacement's new handle deliberately. |
| `AcceptanceTimedOut` | Deadline hit before acceptance; the request was withdrawn. | No | Retrying is safe within the operation's overall deadline. |
| `ResponseTimedOut` | Deadline hit after acceptance; effect and reply unknown. | **Yes** | Do not resend blindly — reconcile against durable state first. |
| `ReplyDropped` | The handler dropped the `Reply` unanswered (also what latest-value conflation of a call looks like). | **Yes** | Retry only an idempotent operation, under one overall deadline, after a superseding incarnation is running. |

The post-acceptance kinds always carry the accepting incarnation in
`incarnation_observed`; the retry-after-newer step of the retry guide is
measured against exactly that token. A successful call resolves to
`Replied`, which pairs the value with the accepting incarnation.

## `ReplyError` (`ReplyReceiver::recv`)

For a reply awaited separately from its send (`reply_channel`):
`Dropped` means the capability was destroyed unanswered; `Timeout` means
the receiver's own deadline elapsed. Acceptance evidence belongs to the
accompanying send's result — the receiver's deadline bounds only the
response wait.

## The rest of the error surface, briefly

- **Declaration and admission** — `ReserveError` (duplicate or in-removal
  id, scope not admitting: `NotAdmittingCause`), `BuildError` (root
  lowering), `PolicyError` (invalid policy data, rejected at
  construction).
- **Startup** — `StartupError` from `wait_started`; `StartOrShutdownError`
  pairs the startup failure with any rollback timeout report;
  `StartupFailure`/`StartupFailureCause` describe a nested scope's failed
  start inside an `ExitError`.
- **Exits** — `Exit` classifies one incarnation's end (`ExitKind`), with
  `ExitError` carrying the application error and `StopReason` the
  supervisor-side terminal reason. `ShutdownTimeout`/`ShutdownStraggler`
  report children that exceeded a shutdown deadline.
- **Observation** — `WaitError` for bounded child waits, `SnapshotClosed`
  and `LifecycleTryRecvError` for closed or empty observation streams.
- **In-actor operations** — `Rejected` when a stopping incarnation refuses
  `continue_with`, timers, or offloads; `DeadlineElapsed` marks an
  offload's expired deadline; `ExitResult` is what child code itself
  returns.

These inventories are exhaustive by design, so new behavior cannot hide
behind wildcard arms: match exhaustively rather than with a catch-all, and
the compiler will surface new variants as decisions rather than surprises.
