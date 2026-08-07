# Group restart strategies

Ordered scopes can select how one restartable child failure affects its
resident siblings:

```rust
tree.strategy(Strategy::OneForAll);
tree.strategy(Strategy::RestForOne);
```

`OneForOne` remains the default. `OneForAll` restarts the trigger and every
other resident restartable membership. `RestForOne` restarts the trigger and
resident restartable memberships declared after it. Dynamic scopes do not
expose a group strategy.

A group cycle begins only when the triggering exit would have restarted under
`OneForOne`. A one-shot child or a child with `RestartCondition::Never` cannot
trigger a cycle and is never pulled into one. Terminal tombstones remain
terminal. Apart from the trigger, an unstarted declaration is not resident and
is not started early merely because a group cycle occurred.

The affected set is frozen once per cycle. Its live members stop in reverse
declaration order through their ordinary shutdown ladders, so configured grace
and abort behavior does not change. The trigger's backoff delays the whole
group. Survivors then respawn in declaration order, and each member must pass
its normal readiness gate before the next is spawned. Every incarnation gets
fresh shutdown and abort tokens; nothing from the previous cycle is reused.

Each planned respawn consumes one scope intensity charge, including forced
sibling respawns. The complete batch is charged before any spawn. If that
batch exceeds the budget, every charge and `RestartScheduled` edge is retained,
the scope fails, and no member partially respawns. The trigger's backoff
attempt advances; a forced sibling's attempt does not, although its cumulative
`restart_count` and the scope's `total_restarts` both advance.

Exits that arrive from outside the frozen set are recorded but wait until the
active cycle finishes. They are then processed as fresh funnel input and may
start a later, independently frozen cycle; they never merge into or widen the
current one. An exit or readiness failure during group respawn follows the
same rule, allowing the current declared-order pass to finish before ordinary
supervision handles that outcome.
