# Part III — Exclusions

This document is Part III of the library specification; [SPEC.md](SPEC.md)
holds Part I — Core (§1–§16), the normative appendices, and the governing
conventions, and [core-plus.md](core-plus.md) holds Part II (§17–§25).
Section numbering is global across the three documents.

## 26. Out of core, permanently

These compose from the public surface and live in separate crates (or
downstream applications), never in core:

- **Console / dashboard** — a pure consumer of §14/§22: snapshot
  subscription, lifecycle subscription, and recursive actor statistics
  are a sufficient public surface for a full console, which is the
  evidence it needs nothing from inside the library.
- **Registries, routers, name-directories** — userland patterns over
  typed refs; the framework deliberately omits name→ref messaging
  (§12): snapshots carry ids and states, never senders, and handle
  `Eq`/`Hash` by slot identity (B.10) is what makes a userland registry
  ordinary code.
- **`ServiceRef`/route-cell handoff adapter** (§3.4) and the
  **transactional handoff helper** (mount/commit/retire hooks over
  dynamic scopes): the primitives — direct admission handles (§3.2),
  idempotent and exact-handle removal (§12), the sibling barrier
  (§25) — are library surface; the orchestration is a utilities crate,
  and an in-core version would have to weaken exact membership identity
  to seem convenient (§3.4 forbids that).
- **Distribution / remote refs** — a proxy-actor layer over the network,
  not a core capability: it needs code identity, args transport, state
  handoff, and its own wire format and failure model, none of which core
  carries (§23's non-goals state the same boundary for outlines), and
  every core contract is process-local by design — at-most-once delivery
  (§1 principle 6) and process-wide identity tokens (§3) do not extend
  across a network boundary unchanged.
- **Durable at-least-once delivery** — core's delivery model is
  at-most-once with no hidden buffering (§1 principle 6); durability
  changes mailbox semantics, requires storage the library deliberately
  does not prescribe (§3.3's ledger rule), and belongs to the
  application or an adapter above the library.
- **Pluggable scheduler as public API** — scheduling remains an internal
  seam behind the runtime façade (§15.1); a public pluggability surface
  would freeze internal execution contracts as API and is adopted, if
  ever, only behind demonstrated performance need (§15.6's
  measure-first posture).
