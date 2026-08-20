# Part II — Core plus

This document is Part II of the library specification. [SPEC.md](SPEC.md) is
the entry document: it holds Part I — Core (§1–§14), the normative
appendices (which span all parts), and the conventions — normative language,
provenance annotations — that govern this document unchanged.
[non-core.md](non-core.md) holds Part III. Section numbering is global
across the three documents: references below §15 resolve in SPEC.md, and
§24 resolves in non-core.md.

Specified now, built after core ships. Each section is individually
adoptable and names the hook core already carries for it. Order within this
part is a suggested sequence, not a dependency chain, except where stated.

## Table of contents

- [15. Incarnation refinements](#15-incarnation-refinements)
- [16. Keyed conflation: `latest_by_key`](#16-keyed-conflation-latest_by_key)
- [17. Message mapping: `contramap` and `project`](#17-message-mapping-contramap-and-project-351)
- [18. Peer monitoring](#18-peer-monitoring)
- [19. Group strategies: `OneForAll`, `RestForOne`](#19-group-strategies-oneforall-restforone)
- [20. Observation extensions](#20-observation-extensions)
- [21. Outline (`serde` feature)](#21-outline-serde-feature)
- [22. Hosting (`host` feature)](#22-hosting-host-feature)
- [23. Lifetime and timing conveniences](#23-lifetime-and-timing-conveniences)

---

## 15. Incarnation refinements

Core already mints `Incarnation` tokens and carries them in events, errors,
and snapshots (§3.3) — that was the retrofit-hostile half. This adds the
convenience surface:

- `ActorRef::pinned(incarnation) -> PinnedRef` — sends only to that
  incarnation and fails fast once it is superseded or terminal. Membership
  addressing remains the default; pinning is the explicit refinement.
- A public awaitable for "the next incarnation after `inc` is running" —
  the packaged form of §3.3's retry-discipline step 2.
- `call_idempotent(make_msg, deadline)` — the packaged form of §3.3's
  retry-discipline steps 1, 2, and 4. Steps 3 (reconcile) and 5 (ledger
  bounds) are application obligations under any surface and stay outside
  it. Decided shape:
  - The caller supplies a message **constructor** (`Fn(Reply<T>) -> M`),
    not a message: each attempt needs a fresh `Reply`, so supplying the
    re-mint source is §4.2's capability proof replayed for calls — plain
    `call` remains the one-shot primitive. The name still carries the
    caller's assertion: re-mintability proves the message can be rebuilt,
    not that repeating the operation is safe; asserting idempotency is
    what choosing this entry point means.
  - One overall deadline budget covers all attempts — binding waits,
    acceptance, response, and inter-attempt delay included [#352].
    Inter-attempt delay reuses `Backoff` as plain data (§9.2). Each
    attempt's inner `call` runs under a **per-attempt slice** — a
    per-attempt duration carried in the same retry-policy data as the
    `Backoff`, clamped to the remaining overall budget. The slice is
    load-bearing, not tuning: with the whole budget as every attempt's
    deadline, `AcceptanceTimedOut` could only ever coincide with overall
    exhaustion, making the retry arm unreachable.
  - Retry transitions: `AcceptanceTimedOut` retries within budget;
    `ReplyDropped` first awaits the incarnation-after (the awaitable
    above), then retries. `ResponseTimedOut` and terminality return
    immediately: the unsafe retry is unrepresentable, not discouraged —
    the outcome type offers no retry continuation for them.
  - The terminal error carries the attempt history (each attempt's
    observed incarnation and where it ended) as data — input for the
    application's reconciliation and for bounding its idempotency ledger
    (§3.3 steps 3 and 5).
  - Boundary with §24: this is a client-side combinator re-sending under
    an explicit capability proof. Nothing buffers, nothing persists, and
    each individual send remains at-most-once (§1 principle 6); it is not
    durable or at-least-once delivery.
  - Adoption gate, per §16's pattern: the shard-store scenario ports in
    the core wave and hand-rolls this discipline first (Appendix C); the
    helper is built by re-porting that retry loop onto it, and the port
    decides. If the scenario's per-operation ledger bookkeeping cannot
    live with the synchronous constructor closure acceptably, that
    artifact justifies the revision (or nothing) — the helper is not
    built on spec alone.
  - Conformance tests (with this feature): a call failing only with
    `ResponseTimedOut` produces exactly one send attempt — the helper
    never resends after acceptance; and a `ReplyDropped` retry lands only
    on an incarnation that strictly supersedes the one that dropped the
    reply, never the rebind window it exited through.

Activates the pinning clause of invariant §13.2.

## 16. Keyed conflation: `latest_by_key`

`latest_by_key(capacity, key_fn)` — conflation per key with a bounded key
set, joining `queue` and `latest` (§5.1; the non-exhaustive mailbox
constructor is the hook). Semantics: same key replaces in place (counted as
conflated); a new key at capacity **evicts the oldest key's pending
message** and accepts — it never blocks and never errors. This is documented
conflation semantics (newest state wins), and the documentation MUST say so,
because it means a "reserved control key" is *not* a priority lane: enough
distinct data keys can evict it. Evictions MUST be observable: each
cross-key eviction increments a dedicated counter in the actor statistics
(§20), because an evicted key's state is lost until that key's next
update — which may never come. Documentation states the sizing rule:
capacity at least the expected key cardinality. Capacity MAY defer to the
scope default. The §5.1 `call`-on-conflating contract applies. The key
extractor is code, not policy: §1's plain-data rule and §21's outline
cover the mailbox *kind and capacity*; the extractor is excluded, exactly
as actor implementations are (§21 non-goals). The eviction counter's
public home is §20's statistics — adopting §16 before §20 keeps the
counter internal (test-observable) until the stats surface lands, so
neither section depends on the other.

Decided: documentation-guided — no first-class control/priority lane,
gated the same way as `project` (§17). If the trading-engine port's
urgent-control-under-flood pattern cannot be written acceptably with
`try_send` plus adequate key capacity, that artifact justifies a lane;
otherwise it never gets built. Already decided either way: per-view
conflation machinery is rejected (§17), and the state-plane/control-plane
split is real — a barrier cannot safely share a keyed conflating mailbox
with replaceable state.

## 17. Message mapping: `contramap` and `project` [#351]

Two primitives, preserving identity and fencing semantics of the underlying
ref. Their settled design decisions are normative for whoever implements
them:

```rust
impl<M: Send + 'static> ActorRef<M> {
    fn contramap<N: Send + 'static>(&self, wrap: impl Fn(N) -> M + Send + Sync + 'static) -> ActorRef<N>;
}
impl<'a, A: Actor + ?Sized> Context<'a, A> {
    fn project<B: Actor + ?Sized>(&mut self, wrap: impl Fn(B::Msg) -> A::Msg + Send + Sync + 'static) -> Context<'_, B>;
}
```

- The wrap is a **pure, cheap injection executed eagerly at every ingress
  point**; everything stateful, ordered, or observable belongs to the outer
  actor's single set of resources — one timer table, one continuation
  queue, one mailbox, one identity. Never buffer-and-replay: cross-call
  timer state (a `clear_timer` in call N seeing call N−1's arm) only works
  if there is exactly one table, typed to the outer message.
- Closure bound is `Fn + Send + Sync + 'static` — the wrap runs on sender
  threads and at concurrent ingress points; stateful enrichment belongs in
  the wrapper's `handle`, where `&mut self` exists and ordering is the
  mailbox's. Document that the wrap must be cheap and non-blocking.
- A contramapped ref **shares the outer ref's identity, id, and stats
  attribution** — the wrapper and inner actor are one actor (one mailbox,
  one loop, one lifecycle); a fabricated inner id would lie to monitoring.
  Pin this with a test.
- Backpressure, capacity, conflation, and message-size observation are the
  *outer* mailbox's; a conflating outer mailbox conflates wrapped
  self-sends (pin with a test). Do not build per-view conflation machinery.
- `call` needs no special support: the `Reply` rides inside the message.
- Eager conversion has an error-payload cost, decided: a contramapped
  ref's send errors carry no recoverable `N` — the wrap already consumed
  it. Mapped refs surface B.3's boxed projection (id + kind, payload
  dropped), and B.3's payload-recovery clause is scoped to unmapped refs.
  The alternatives (deferred conversion, a mandatory inverse, `N: Clone`
  on every mapped send) each break a settled rule above — one mailbox
  typed to the outer message, cheap pure wraps.
- Unwrapped actors pay one predictable enum branch; boxing exists only on
  the mapped arm; layers compose by nesting, one closure hop per layer. The
  same-`Msg` re-entry (`for_actor`, core) stays as the zero-cost identity
  case.
- Sequence: `contramap` first (independently motivated, testable in
  isolation). **Gate `project` on a concrete consumer** that demonstrates
  the provenance wall: an origin-blind journal (same-`Msg` middleware)
  cannot distinguish externally-sent messages from self-regenerated
  effects, which breaks replay-correct durability (a replayed `Compact`
  both replays and regenerates — the effect runs twice). If a durability
  prototype gets by origin-blind, `project` never gets built.

## 18. Peer monitoring

`ctx.watch(&ref, wrap)` (and a cancel-on-drop `watch_scoped`) — where
`wrap: impl Fn(MonitorEvent) -> A::Msg + Send + Sync + 'static`, the §17
closure discipline, since a `MonitorEvent` cannot enter an arbitrary
`Msg` mailbox unmapped — delivers
`MonitorEvent`s (shape: Appendix B.4) into the watcher's mailbox via a
bounded drop-oldest queue per watch (depth: Appendix A); overflow coalesces
into one `Lagged`; terminal `Removed` is never dropped (it is always
newest); `Started` carries the `Incarnation`; stale-membership routing uses
the §3.1 primitive — that primitive and the single publication path are the
core hooks. Watching an already-running target delivers an immediate
`Started`; re-registering a watch aliases the existing one without a
duplicate immediate `Started`. Because immediate restart can outrun an
external query, transition evidence is available from events without
keeping application-side history. In core, sibling-failure reaction routes
through the supervisor (fate-sharing) or lifecycle-stream subscription;
watch is the in-mailbox refinement.

## 19. Group strategies: `OneForAll`, `RestForOne`

New variants on the non-exhaustive `Strategy` (§9.1). Group restarts drain
the affected set, re-mint the group cancellation context, and respawn in
declared order. **Every sibling respawn charges the scope intensity budget**
(§9.2 — stated in core precisely so this section cannot relitigate it).
Group teardown reuses §10's ladder unchanged; the exit funnel's
mode-dispatch (§10) is the hook — group drain is a `Draining { scope:
subset, reason }` mode, not a second dispatch path. Decided edges: a
group respawn re-runs only restartable members — one-shot members
(structurally `Never`) are terminally removed by the group restart per
their §8 retention and do not block it, and other `Never` members
likewise stay down. The triggering child's own `Backoff` delays the whole
group respawn; siblings' attempt counters do not advance (every respawn
still charges intensity, §9.2). Intensity is charged **atomically**: all
forced respawns of one group restart are charged together, before any
member respawns — if the batch trips the budget, the scope fails without
a partial respawn. Exits arriving while the group drains are recorded by
the funnel but schedule nothing and charge nothing (mode dispatch, §10) —
they are part of the drain. Respawn is declared-order and
readiness-gated, exactly like ordered startup (§6). Activates the group
clause of invariant §13.9.

## 20. Observation extensions

All adapters over core's two streams and identity types:

- **Actor statistics** (field inventory: Appendix B.6): readable per
  membership; per-incarnation attribution distinguishable via §3.3;
  recursive scope-wide stats resolve through typed child metadata, not
  attachment scans [#362]. Brings `message_size` observation with it,
  added as a typed actor-spec extension — the measurer is code, so it
  lives outside §8's plain-data options record, exactly like §16's key
  extractor (§8's extractor boundary).
- **Child observation** — self-recovering reducer projection that resets
  with a full snapshot after lag; consumers never see a raw `Lagged`, they
  see a reset carrying the fresh snapshot and the dropped count.
- **Packaged restart-counter view** — subscription plus cumulative total
  that survives `Lagged`, deduplicated, making breaker patterns turnkey
  without hand-carried totals.
- **`metrics` feature** — optional metric emission from the same single
  choke point as `tracing`; the debugging surface exposes structured
  snapshots rather than name-filtered tuples.

## 21. Outline (`serde` feature)

**Purpose.** The outline is a policy-drift fingerprint: a serializable,
injective projection of a tree's *resolved* declaration, capturing
everything §9.3's inheritance machinery decides silently (scope defaults,
inherit-vs-reset at subtree edges, library fallbacks). Its uses are
golden-outline tests that pin a system's effective supervision policy in
CI without spawning anything, startup logging, and cross-environment
diffing. Ad hoc diagnostic output cannot substitute (no injectivity or
stability contract), nor can snapshots (they need a running system and
carry only part of the policy surface, B.6).

**Non-goals.** The outline carries no actor implementations, closures,
args, or state: it is a description of a declaration, not a constructor
for one. It cannot rebuild a tree, cannot "port a System" to another
machine, and distribution (§24, #247) will not build on it — a
distribution layer needs code identity, args transport, and state
handoff, and will define its own wire format.

**Placement.** If this exists at all it must be a library feature: the
orphan rule bars downstream crates from implementing `Serialize` for
library-owned policy types. Feature-gated `serde` derives cost non-users
nothing; core's only obligation is the "serializable where §21 needs it"
clause on the §8 options record.

**Contract.** Any two trees that differ in the **declaration surface the
outline carries** — the policy and topology fields below — MUST serialize
differently. Injectivity is over that surface, not over code: per the
non-goals above, two trees identical in outline may still differ in spawn
outcome through the implementations, closures, or args they carry.
The outline carries, per scope: kind,
strategy (ordered only), intensity, and every scope default including
mailbox capacity; per child: kind, id, restart condition + backoff,
shutdown policy, readiness mode + deadline, terminal-membership retention,
and (actors) mailbox kind + capacity. Outlines reject unknown fields and
missing required fields on deserialization, so schema drift is loud. Ship
it complete on first release — the unknown-fields discipline makes late
field additions wire breaks for persisted outlines; this completeness
obligation on every future option is the feature's real cost, accepted for
the loud-failure property. Activates invariant §13.11.

## 22. Hosting (`host` feature)

Exposes running incarnations without a supervisor, using the **same**
incarnation runner as supervised execution — that single runner is the core
hook (§7): same exit type (including `Panicked`, so hosted users never
hand-write `catch_unwind`), same readiness handling, same teardown
ordering. It is the seam for embedding and for a future
`!Send`/thread-per-core mode. Activates the hosted-parity clause of
invariant §13.7.

## 23. Lifetime and timing conveniences

- **Cross-actor delayed delivery** (`send_after_to` / `interval_to`): a
  mailbox-semantics facility (capacity and conflation apply), owned by the
  sender's incarnation via a `Guard` (Appendix B.7), and the only
  spawned-task timer path (§5.3).
- **Completion-driven lifetime**: bind the root's lifetime to selected
  child completions ("run until these tasks finish, then shut down") for
  finite/batch applications; composes `OneShotTaskRef` awaitables with
  `shutdown()`.
- **Sibling-readiness barrier**: first-class support for a child awaiting a
  *named sibling's* readiness (a scope-relative readiness barrier),
  replacing offload-the-wait plumbing.

