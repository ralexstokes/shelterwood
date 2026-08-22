# Trees, slots, and wiring

A Shelterwood system starts as a declaration. `Tree::new()` creates an
empty ordered tree, the `add_*` methods (`add_actor`, `add_task`,
`add_subtree`, and their `_once` and raw twins) attach children to it, and
`Tree::spawn` lowers the declaration and returns the owning `System`.
Nothing runs until `spawn`; every `add_*` call hands back the child's
membership-addressed handle — an `ActorRef`, `TaskRef`, or scope handle —
before the child exists, which is what makes wiring a matter of plain data
flow rather than startup choreography.

## Declaration order is the contract

In an ordered tree, the order in which you add children is the order in
which they start. Each child's readiness gates the start of the next
declared child, so a child can assume every earlier sibling is already up
when it initializes — [Readiness and startup
order](readiness-startup-order.md) covers exactly what "ready" means for
each child kind. Shutdown runs the same list in reverse, stopping one
fully joined child at a time, so a child can also assume its earlier
siblings outlive it on the way down — see [Shutdown](shutdown.md).
Dependency order therefore falls out of declaration order: declare the
thing depended on first.

Ordered membership is fixed at `spawn`. An ordered scope neither admits
nor removes children at runtime; when membership must change while the
system runs, declare a dynamic scope instead (below).

## Composing subtrees

A tree nests. `SubtreeDef::factory` wraps a closure that builds a whole
`Tree` (or `DynamicTree`), and `add_subtree` mounts it as a single child
of the parent — the subtree starts, reports readiness, restarts, and
stops as one unit in its parent's order. `SubtreeOnceDef::new` is the
one-shot form for a tree you build once and consume. A subtree edge also
chooses, via `DefaultsInheritance`, whether unset `ScopeDefaults` inside
it keep resolving through the parent's defaults or reset to the library
defaults.

## Slots: reserving before defining

Handles come from `add_*` calls, but an `add_*` call needs the finished
definition — and a definition sometimes needs a handle to a sibling that
is not defined yet. Two actors that must hold each other's `ActorRef`
cannot both be added first. The `reserve_*` methods break the cycle by
splitting reservation from definition: `reserve_actor` claims an id and
returns an `ActorSlot`, the slot hands out `ActorRef`s immediately, and
the slot's `define_once` (or `define`) supplies the definition later.

The `cyclic_wiring` example wires a ping-pong pair this way. The two
actors are ordinary — each one just holds the other's handle:

```rust
{{#include ../../crates/shelterwood/examples/cyclic_wiring.rs:actors}}
```

The tree reserves both slots, takes a handle from each, and only then
defines each actor with the other's handle in its arguments:

```rust
{{#include ../../crates/shelterwood/examples/cyclic_wiring.rs:reserve}}
```

Every reservation must be defined before `spawn`; a tree with an
undefined slot fails to spawn with `BuildError::UnfilledReservations`
rather than starting a partial system. Slots exist for tasks and subtrees
too (`reserve_task`, `reserve_subtree`), and the sends through a
cyclically wired pair are ordinary sends — the handles were valid before
either actor started.

## Dynamic trees

`DynamicTree` is the same builder surface with a different scope flavor:
its declared children start concurrently rather than gated one-by-one,
and membership stays open after `spawn` — the scope handle admits new
members and removes existing ones at runtime. `System::wait_started`
aggregates readiness over the *initial* members only; children admitted
later never join that aggregate. Runtime admission, removal, and the
dynamic slot types are the subject of [Dynamic
scopes](dynamic-scopes.md).

The full builder surface — every `add_*`/`reserve_*` variant, scope
`intensity` and `defaults` setters, and the `Subtree` dispatch trait — is
documented on `Tree` and `DynamicTree` in the
[API reference](https://docs.rs/shelterwood).
