# Part II acceptance evidence

Shelterwood's five application-scale acceptance tests exercise the combined
core and Part II surface. They use bounded observations and deterministic
gates instead of timing sleeps.

## Full-fidelity scenarios

- `shard_store.rs` covers planned topology replacement, durable operation
  identities, incarnation-aware idempotent calls, reply-loss reconciliation,
  and exact membership retirement.
- `sidecar.rs` covers task-first embedding, readiness-ordered startup,
  host-owned rollback, bounded shutdown escalation, and repeated host cycles.
- `assistant.rs` covers nested dynamic scopes, group restart strategies, peer
  watches, journal-boundary redelivery, conflated streaming, idle eviction,
  and remove/re-add races.
- `trading_engine.rs` covers slot-before-define cyclic wiring, a
  restart-budgeted subtree, FIFO and keyed mailboxes, pipelined
  deadline-budgeted calls, peer-watch staleness, restart-count health
  breaking, and metric names and labels.
- `build_farm.rs` covers dynamic consuming one-shot workers, auto-removal,
  readiness retry with backoff, keyed progress, exact wedged-worker
  retirement, completion-driven lifetime, outlines, and warm durable rebuild.

## Final evidence-gate verdicts

The control/priority-lane gate is **do not build**. In the trading port, eight
updates for one parked data key conflate to one pending slot. A control
`try_send` is then admitted as the second key when capacity equals the two
expected key classes. This proves admission under that flood without
promising priority order. Irreplaceable barriers still belong on a separate,
non-conflating protocol path.

The mapped-context `project` gate is **do not build**. The durability-bearing
shard-store, build-farm, and assistant scenarios all preserve the provenance
they need with explicit actor boundaries, same-message `for_actor`, and
`ActorRef::contramap`. None demonstrates the provenance wall that was required
to justify another context type and its boxed mapping layer.
