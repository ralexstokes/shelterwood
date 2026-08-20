# Part III — Outside the library

This document is Part III of the library specification; [SPEC.md](SPEC.md)
holds Part I — Core (§1–§16), the normative appendices, and the governing
conventions, and [core-plus.md](core-plus.md) holds Part II (§17–§25).
Section numbering is global across the three documents.

Part III has two sections of different kinds. §26 lists capabilities that
are never first-party: they live in downstream applications or separate
projects, or name internal seams that stay unexposed. §27 charters the
**utility tier**: first-party crates *above* the library that compose
strictly from the public surface.

## 26. Out of the library, permanently

These compose from the public surface and live in downstream applications
or separate projects — never in the library, and never in §27's utility
tier:

- **Console / dashboard** — a pure consumer of §14/§22: snapshot
  subscription, lifecycle subscription, and recursive actor statistics
  are a sufficient public surface for a full console, which is the
  evidence it needs nothing from inside the library. That evidentiary
  role is load-bearing: a first-party console would stop proving the
  observation surface sufficient to outsiders.
- **Routers and name-directories** — userland patterns over typed refs;
  the framework deliberately omits name→ref messaging (§12): snapshots
  carry ids and states, never senders, and handle `Eq`/`Hash` by slot
  identity (B.10) is what makes a userland registry ordinary code. The
  one packaged exception is §27's atomic-rebind directory, which
  internalizes the handoff-fencing subtleties without adding name→ref
  messaging to core.
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
  application or an adapter above the library. The delivery-model
  exclusion is permanent; the adapter's seams are §27's durability
  entry.
- **Pluggable scheduler as public API** — scheduling remains an internal
  seam behind the runtime façade (§15.1); a public pluggability surface
  would freeze internal execution contracts as API and is adopted, if
  ever, only behind demonstrated performance need (§15.6's
  measure-first posture).

## 27. The utility tier

First-party crates above the library, composing strictly from the public
surface. A utility crate MUST depend only on the supported `shelterwood`
façade, never on an internal crate: that dependency edge is the
structural proof that everything here remains buildable by any consumer,
and that nothing here can invalidate an internal ruling (the lock rule's
seam exemptions included). The tier exists to internalize subtle
constructions once — safety and ergonomics, not capability — and its
items are eventually re-exported through a prelude. A Part II section
whose placement note reads *public-surface composition* MAY ship here
instead of as a library feature; its contract and conformance clauses
bind unchanged wherever it ships.

- **Transactional handoff helper and `ServiceRef`/route-cell adapter**
  (§3.4) — the primitives (direct admission handles §3.2, idempotent and
  exact-handle removal §12, the sibling barrier §25) are library
  surface; the orchestration — mount → readiness → directory cutover →
  exact-handle retire, with idempotent operation ids, compensating
  cleanup, and the crash-window fence — is the utility crate. An
  in-library version would have to weaken exact membership identity to
  seem convenient (§3.4 forbids that). Acceptance scenario 1 builds this
  construction as test userland; the packaged form is its extraction,
  and scenario 1 doubles as its integration proof.
- **Atomic-rebind directory** — the registry a planned handoff cuts
  over: atomic rebind plus stale-handle fencing via incarnation tokens
  (§3.3). Core's omission of name→ref messaging (§12) stands unchanged;
  the directory is packaged because its fencing is subtle enough that
  every consumer would otherwise re-derive it, and because the handoff
  helper above needs a directory to cut over.
- **Durability adapter** *(design-gated)* — storage-agnostic journal
  seams and the journal/ack redelivery boundary (acceptance scenario 5's
  shape), plus the provenance-explicit durable-actor construction §19
  anticipates: a wrapper that journals decided events or
  provenance-tagged arms, never the raw inbound message stream. It
  changes nothing about core's delivery model (§26) and prescribes no
  storage (§3.3's ledger rule). This entry reserves placement, not
  surface: a design round must first clear §19's provenance wall, and
  that consumer — if its design genuinely needs cross-`Msg` context
  mapping — is what would justify implementing `project`.
