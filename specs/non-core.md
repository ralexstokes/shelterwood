# Part III — Non-core

This document is Part III of the library specification; [SPEC.md](SPEC.md)
holds Part I — Core (§1–§14), the normative appendices, and the governing
conventions, and [core-plus.md](core-plus.md) holds Part II (§15–§23).
Section numbering is global across the three documents.

## 24. Out of core, permanently

These compose from the public surface and live in separate crates (or
downstream applications), never in core:

- **Console / dashboard** — a pure consumer of §12/§20 (the origin console
  needed exactly three public calls: snapshot subscription, lifecycle
  subscription, recursive actor stats). Not part of the v0 workspace.
- **Registries, routers, name-directories** — userland patterns over typed
  refs; the framework deliberately omits name→ref messaging (§11).
- **`ServiceRef`/route-cell handoff adapter** (§3.4) and the
  **transactional handoff helper** (mount/commit/retire hooks over dynamic
  scopes): the primitives — direct admission handles (§3.2), idempotent and
  exact-handle removal (§11), the sibling barrier (§23) — are library surface; the
  orchestration is a utilities crate.
- **Distribution / remote refs** (proxy-actor design, #247).
- **Durable at-least-once delivery** (#244).
- **Pluggable scheduler as public API** (#274 — internal seam only [#359],
  adopted only behind performance gates).
