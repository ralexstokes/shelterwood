# shelterwood

Shelterwood is a structured-supervision and actor runtime for asynchronous
Rust. A system is declared as a tree of actors, plain tasks, and nested scopes;
the tree owns startup order, readiness, restart policy, bounded mailboxes,
shutdown, and observation. Stable membership and incarnation identities make
failure recovery explicit without a global registry.

The current `0.1` surface is the core described by [SPEC.md](SPEC.md): ordered
and dynamic scopes, `OneForOne` supervision, handler and raw actors, supervised
tasks, queue and latest-value mailboxes, lifecycle events, and recursive
snapshots. Part II features in the specification are intentionally not part of
the core API yet.

## Getting started

The repository toolchain comes from Nix. `scripts/dev` enters the dev shell for
the current checkout, including when invoked from a worktree:

```sh
./scripts/dev just test
./scripts/dev just ci
./scripts/dev just ci-nix
```

A task-first application needs no actor merely to obtain supervision:

```rust
use std::time::Duration;

use shelterwood::{ExitError, TaskOnceDef, Tree};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tree = Tree::new();
    let (_worker, _completion) = tree.add_task_once(
        "worker",
        TaskOnceDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok::<_, ExitError>(())
        }),
    )?;

    let system = tree.spawn()?;
    system.wait_started().await?;

    // The owner drives bounded teardown and waits for every child to join.
    system.shutdown(Duration::from_secs(5)).await?;
    Ok(())
}
```

Use `Tree` when declaration order should also be startup order and reverse
shutdown order. Use `DynamicTree` for concurrently started membership that can
be added and removed at runtime. Actors and tasks are peers in either kind of
scope, and subtrees compose both recursively.

## Operational guides

- [Calls, retries, and message ordering](docs/retry-and-ordering.md)
- [Shutdown and resource ownership](docs/shutdown-and-resources.md)
- [Snapshots and lifecycle events](docs/observation.md)
- [Embedding Shelterwood in a host process](docs/embedding.md)

The executable application-scale examples live in the M5 acceptance tests:
[shard store](crates/shelterwood/tests/shard_store.rs),
[sidecar](crates/shelterwood/tests/sidecar.rs), and
[assistant control plane](crates/shelterwood/tests/assistant.rs).

## Operational preconditions

Shelterwood supervision classifies panics only when the process uses
`panic = "unwind"`; `panic = "abort"` ends the process before a supervisor can
observe the panic. Rust's ordinary double-panic rule still applies: if a
destructor panics while the same user future is already unwinding, the process
aborts before supervision can publish an exit. Spawn systems inside a Tokio
runtime with time enabled, and resolve `System::shutdown` (or allow the dropped
owner to finish teardown) before destroying that runtime. Destroying the
runtime around a live system is outside the contract.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you shall be dual licensed as above, without
any additional terms or conditions.
