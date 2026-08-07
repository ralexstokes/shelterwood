# Part II progress

This ledger records the stacked implementation work for issue #9. `SPEC.md`
is authoritative; this file records branch ancestry, evidence-gate outcomes,
validation, and deviations for review.

## Mandatory M6 base

- PR #10 head selected and fetched on 2026-08-07:
  `de34ec4d279dc94207a5a0572ad5c057930f6a15`.
- M7 branch: `agent/issue-9-m7-incarnation-mapping`.
- M7 starts directly from the selected SHA; no M7 implementation is present
  on PR #10.

## Milestones

| Milestone | Status | Evidence gates | Validation | Deviations |
|---|---|---|---|---|
| M7 — incarnation refinements and `contramap` | complete | `call_idempotent`: **build** — repaired shard-store re-port passed; `project`: open | focused M7 and shard-store tests pass; `just ci` passes with 165 tests | none |
| M8 — keyed conflation and peer monitoring | pending | control/priority lane remains open until M12 | pending | none |
| M9 — group strategies | pending | none | pending | none |
| M10 — observation extensions | pending | none | pending | none |
| M11 — outline, hosting, conveniences | pending | none | pending | none |
| M12 — acceptance wave two | pending | control/priority lane and `project` final verdicts due | pending | none |
