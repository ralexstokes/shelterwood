# Embedding in a host

Shelterwood does not need to own `main` or the async runtime. A host can
open process-lifetime resources, build and spawn a tree, await readiness,
operate the service, then consume the `System` with bounded shutdown — and
a fresh tree can repeat that whole cycle in the same process. This chapter
walks the cycle end to end; the example below runs it twice over shared
host-owned state.

Plain `TaskDef` and `TaskOnceDef` children are first-class siblings of
actors and subtrees, so a task-first service can supervise config loading,
telemetry, serving, and cleanup directly, adding an actor subtree only
where a mailbox protocol is useful:

```rust
{{#include ../../crates/shelterwood/examples/embedding.rs:service}}
```

## Startup ownership

An ordered `Tree` starts children in declaration order and gates each
following child on readiness. Manual readiness, as in the service above,
lets a task or actor publish that its external resource is actually usable
rather than merely that its future was spawned.

At the root there is a choice. `wait_started()` reports a startup failure
but deliberately leaves the successfully started prefix supervised, which
permits a host-specific decision about what to do with it. Most embeddings
instead want rollback, and `start_or_shutdown` is the usual choice: on
success it returns the owner; on failure it drives full shutdown of the
started prefix and returns both the original startup error and any
rollback timeout report. Rollback never masks the cause.

## One-shot means one incarnation

The `_once` declaration family consumes owned construction input and can
create exactly one incarnation. It does not mean "one loop iteration": a
cleanly returning restartable child under `RestartCondition::OnFailure`
also runs only once. Reach for `_once` when the arguments or future cannot
be minted again, or when the type should make restart impossible.

Restartable definitions take cloneable arguments or a factory that creates
fresh input for every incarnation. Put reconnectable per-incarnation state
there. Keep process-lifetime durable state in the host, and pass cloneable
handles to it into each fresh tree or actor incarnation — that is what
lets state cross cycles.

## A complete host cycle

1. Open host-owned resources and construct a fresh `Tree` or
   `DynamicTree`.
2. Spawn inside a Tokio runtime with time enabled.
3. Use `start_or_shutdown` to either receive a ready owner or roll back.
4. Keep the `System` owner alive while the service runs. Non-owning scope
   and actor handles may be cloned freely.
5. Call `system.shutdown(timeout).await` before closing host resources or
   destroying the runtime.
6. Inspect the structured timeout report if cooperative teardown exceeded
   its budget. The root driver is joined on return; a nested framework
   driver that hits the hard-abort fallback can leave deeper task
   cancellation finishing asynchronously, as detailed in the shutdown
   guide linked below.
7. Build a new tree for another cycle. Builders and `System` are
   single-use by design; durable host state is what crosses cycles.

The example runs steps 1 through 7 twice, sharing one host-owned counter:

```rust
{{#include ../../crates/shelterwood/examples/embedding.rs:embedding}}
```

## Drop requests shutdown; awaiting it joins

Dropping `System` also requests graceful shutdown, but awaiting explicit
`shutdown` is the only way for the host to join the root driver and
receive straggler evidence. See the
[shutdown and resources guide][shutdown-guide] for the recursive
hard-abort boundary, grace, blocking-work, and teardown-notification
rules.

[shutdown-guide]:
  https://docs.rs/shelterwood/latest/shelterwood/guides/shutdown_and_resources/index.html

## Runtime preconditions

Three preconditions keep an embedding on contract:

- The ambient Tokio runtime must have **time enabled** — backoff, grace,
  and deadlines all depend on it.
- Supervised panic classification requires **`panic = "unwind"`**; with
  `panic = "abort"` a child panic ends the process instead of becoming a
  supervised exit.
- **Resolve shutdown before destroying the runtime**: await
  `system.shutdown(timeout)` (or let the drop-requested shutdown finish)
  before the host tears the runtime down, so supervised teardown is not
  cut off mid-flight.
