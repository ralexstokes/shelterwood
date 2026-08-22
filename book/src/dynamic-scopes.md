# Dynamic scopes

An ordered `Tree` fixes its membership at declaration time. A
`DynamicTree` does not: its membership stays open after spawn, so children
can be admitted and removed while the system runs. This is the shape for
per-connection actors, per-job workers — anything whose population is
decided by traffic rather than by the program text.

The trade is ordering. A dynamic scope starts its declared children
concurrently — no child's readiness gates a sibling's start — and shutdown
runs every member's stop ladder together, grace clocks in parallel rather
than summed. If startup order matters between two children, they belong in
an ordered tree, possibly one mounted as a subtree of a dynamic scope.

## The handle

Spawning a `DynamicTree` yields a `System` whose `scope()` returns a
`DynamicScopeRef`: a cheap, cloneable handle carrying the admission and
removal capability. Its `as_scope()` exposes the plain `ScopeRef`
observation surface underneath. The example
`crates/shelterwood/examples/dynamic_scope.rs` runs the whole loop:
spawn, admit, exercise, remove.

## Admission

Admission is asynchronous: `add_actor` (and its task, raw, and subtree
siblings) returns an `Admission` future that resolves once the scope has
admitted the child — with exactly the same kind of handle the ordered
builder would have returned:

```rust
{{#include ../../crates/shelterwood/examples/dynamic_scope.rs:admit}}
```

Admission resolves at *admission*, not at startup: the child's `init` has
not necessarily run when the future completes. The returned `ActorRef` is
usable immediately — sends wait through the startup window just as they
wait through restarts. Note also what `wait_started` on the system means
here: it aggregates readiness over the *initial* declared members only;
children admitted later never join that aggregate.

Two admission failures are worth a sentence each. Admitting an id that a
resident membership already occupies fails with
`ReserveError::DuplicateId` — ids are unique within a scope, and a
retained terminal child still counts. Admitting an id whose incumbent is
currently mid-removal fails with `ReserveError::RemovalInProgress` — await
the removal, then retry.

After admission the child is an ordinary supervised member: the example
sends to it and `call`s it exactly as in
[A first system](first-system.md), and its restart policy applies as
declared.

## Planned removal

Removal is where dynamic scopes demand a discipline, because child ids are
reusable. The safe primitive is **exact-handle removal**: retain the exact
handle admission returned, and remove *that membership*:

```rust
{{#include ../../crates/shelterwood/examples/dynamic_scope.rs:remove}}
```

The protocol for replacing a child, step by step:

1. Keep the handle admission returned.
2. Call `remove_actor` (or `remove_task`, `remove_scope`) with it.
3. Await `RemoveOutcome::Removed`.
4. Admit the replacement.

The point of the exact handle is the race it closes: a stale handle can
never remove a same-id successor, because it names a membership, not an
id. The replacement admitted in step 4 is a *distinct* membership — not a
new incarnation of the old one — so handles, waits, and registry entries
keyed to the removed member never confuse the two.

Removal semantics are deliberately forgiving at the API boundary. The call
latches the removal synchronously and returns an observation future:
dropping that future abandons only the observation, never the removal
itself, and the engine drives a latched removal to completion regardless.
Removing an already-absent member resolves `RemoveOutcome::AlreadyAbsent`
rather than erroring, and concurrent removes of the same membership join
the one removal and see its one outcome. There is also id-based
`remove(id)` for when no handle survives — but reach for the exact-handle
form whenever you hold one.

A removed member walks the same escalation ladder as
[Shutdown](shutdown.md) describes — removal is a forced stop, not a
different mechanism — and when the whole system shuts down, every current
member's ladder starts at once and the scope drains concurrently.

## Watching membership change

A dynamic population usually has an operator: something that needs to see
children come, go, restart, and fail. That is the observation surface —
snapshots, lifecycle events, and bounded waits on the `ScopeRef` under
`as_scope()`. The next chapter, [Observation](observation.md), covers it,
including how membership identity distinguishes a replacement from the
member it replaced.

Reference detail for everything here — reservation (`reserve_actor` and
friends, the split form of admission), subtree admission, and the full
`Admission` and `Removal` contracts — lives at
[docs.rs](https://docs.rs/shelterwood).
