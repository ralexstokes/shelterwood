# Embedding Shelterwood in a host process

Shelterwood does not need to own `main` or the Tokio runtime. A host can open
process-lifetime resources, build and spawn a tree, await readiness, operate it,
then consume the `System` with bounded shutdown. A fresh tree can repeat that
cycle in the same process; the sidecar acceptance test runs two complete
embed/start/stop cycles over shared host-owned state.

Plain `TaskDef` and `TaskOnceDef` children are first-class siblings of actors
and subtrees. A task-first service can therefore supervise config loading,
telemetry, serving, and cleanup directly, adding an actor subtree only where a
mailbox protocol is useful.

## Startup ownership

An ordered `Tree` starts children in declaration order and gates each following
child on readiness. Manual readiness lets a task or actor publish that its
external resource is usable rather than merely that its future was spawned.

At the root, `wait_started()` reports a startup failure but deliberately leaves
the successfully started prefix supervised. This permits a host-specific
decision, but most embeddings want rollback:

```rust,ignore
let system = tree.spawn()?;
let system = system
    .start_or_shutdown(Duration::from_secs(5))
    .await?;
```

`start_or_shutdown` returns the owner on success. On failure it drives full
shutdown of the started prefix and returns both the original startup error and
any rollback timeout report; rollback never masks the cause.

## One-shot means one incarnation

The `_once` declaration family consumes owned construction input and can create
exactly one incarnation. It does not mean “one loop iteration.” A cleanly
returning restartable child under `RestartCondition::OnFailure` also runs only
once; use `_once` when the arguments or future cannot be minted again, or when
the type should make restart impossible.

Restartable definitions take cloneable arguments or a factory that creates
fresh input for every incarnation. Put reconnectable incarnation state there.
Keep process-lifetime durable state in the host and pass cloneable handles into
each fresh tree or actor incarnation.

## A complete host cycle

1. Open host-owned resources and construct a fresh `Tree` or `DynamicTree`.
2. Spawn inside a Tokio runtime with time enabled.
3. Use `start_or_shutdown` to either receive a ready owner or roll back.
4. Keep the `System` owner alive while the service runs. Non-owning scope and
   actor handles may be cloned freely.
5. Call `system.shutdown(timeout).await` before closing host resources or
   destroying the runtime.
6. Inspect a structured timeout report if cooperative teardown exceeded its
   budget; all actor futures are nevertheless joined when the call returns.
7. Build a new tree for another cycle. Builders and `System` are single-use by
   design; durable host state is what crosses cycles.

Dropping `System` also requests graceful shutdown, but awaiting explicit
shutdown is the only way for the host to know teardown has joined and to receive
straggler evidence. See [shutdown and resource ownership](shutdown-and-resources.md)
for grace, blocking-work, and teardown-notification rules.
