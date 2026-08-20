# Part II — Extensions

This document is Part II of the library specification. [SPEC.md](SPEC.md)
is the entry document: it holds Part I — Core (§1–§16), the normative
appendices (which span all parts), and the conventions — normative
language among them — that govern this document unchanged.
[non-core.md](non-core.md) holds Part III. Section numbering is global
across the three documents: references below §17 resolve in SPEC.md, and
§26–§27 resolve in non-core.md.

Every section here is an optional extension of core: individually
adoptable, in any order except where a section states a dependency, and
each names the hook Part I already carries for it. A library that ships
none of them is still complete (Part I's conformance statement); a
library that ships one MUST meet that section in full, including the
Part-II-tagged rows of the appendices and the activated clauses of §16.

Each section (or, where it splits, each part) carries a **placement**
note recording why it sits in Part II rather than Part III. *Internal
seam* means the feature reaches machinery no public surface exposes and
can only live in the library. *Public-surface composition* means the
hook is public API: the section is in Part II as a packaging decision,
its packaged form MAY ship in the utility tier (§27) rather than as a
library feature, and its contract and conformance clauses bind unchanged
wherever it ships.

## Table of contents

- [17. Incarnation refinements](#17-incarnation-refinements)
- [18. Keyed conflation: `latest_by_key`](#18-keyed-conflation-latest_by_key)
- [19. Message mapping: `contramap` and `project`](#19-message-mapping-contramap-and-project)
- [20. Peer monitoring](#20-peer-monitoring)
- [21. Group strategies: `OneForAll`, `RestForOne`](#21-group-strategies-oneforall-restforone)
- [22. Observation extensions](#22-observation-extensions)
- [23. Outline (`serde` feature)](#23-outline-serde-feature)
- [24. Hosting (`host` feature)](#24-hosting-host-feature)
- [25. Lifetime and timing conveniences](#25-lifetime-and-timing-conveniences)

---

## 17. Incarnation refinements

Core already mints `Incarnation` tokens and carries them in events,
errors, and snapshots (§3.3) — that was the retrofit-hostile half. This
adds the convenience surface:

- `ActorRef::pinned(incarnation) -> PinnedRef` — sends only to that
  incarnation and fails fast once it is superseded or terminal.
  Membership addressing remains the default; pinning is the explicit
  refinement.
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
    caller's assertion: re-mintability proves the message can be
    rebuilt, not that repeating the operation is safe; asserting
    idempotency is what choosing this entry point means.
  - One overall deadline budget covers all attempts — binding waits,
    acceptance, response, and inter-attempt delay included.
    Inter-attempt delay reuses `Backoff` as plain data (§10.2). Each
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
  - Boundary with §26: this is a client-side combinator re-sending under
    an explicit capability proof. Nothing buffers, nothing persists, and
    each individual send remains at-most-once (§1 principle 6); it is
    not durable or at-least-once delivery.
  - Conformance tests (with this feature): a call failing only with
    `ResponseTimedOut` produces exactly one send attempt — the helper
    never resends after acceptance; and a `ReplyDropped` retry lands
    only on an incarnation that strictly supersedes the one that dropped
    the reply, never the rebind window it exited through.

Activates the pinning clause of obligation §16.2.

**Placement.** Split. `pinned` is an internal seam: fencing a send to
one incarnation must be checked on the mailbox admission path — a
check-then-send outside it races with rebind and delivers to the
successor. The incarnation-after awaitable and `call_idempotent` are
public-surface compositions over §3.3's tokens, B.3's errors, and the
lifecycle stream (§27).

## 18. Keyed conflation: `latest_by_key`

`latest_by_key(capacity, key_fn)` — conflation per key with a bounded key
set, joining `queue` and `latest` (§5.1; the non-exhaustive mailbox
constructor is the hook). Semantics: same key replaces in place (counted
as conflated); a new key at capacity **evicts the oldest key's pending
message** and accepts — it never blocks and never errors. This is
documented conflation semantics (newest state wins), and the
documentation MUST say so, because it means a "reserved control key" is
*not* a priority lane: enough distinct data keys can evict it. Evictions
MUST be observable: each cross-key eviction increments a dedicated
counter in the actor statistics (§22), because an evicted key's state is
lost until that key's next update — which may never come. Documentation
states the sizing rule: capacity at least the expected key cardinality.
Capacity MAY defer to the scope default. The §5.3 `call`-on-conflating
contract applies. The key extractor is code, not policy: §15.2's
plain-data rule and §23's outline cover the mailbox *kind and capacity*;
the extractor is excluded, exactly as actor implementations are (§23
non-goals). The eviction counter's public home is §22's statistics —
adopting §18 before §22 keeps the counter internal (test-observable)
until the stats surface lands, so neither section depends on the other.

Decided, either way: there is no first-class control/priority lane —
urgent-control-under-flood composes from `try_send` plus adequate key
capacity; per-view conflation machinery is rejected (§19); and the
state-plane/control-plane split is real — a barrier cannot safely share
a keyed conflating mailbox with replaceable state.

**Placement.** Internal seam — conflation, eviction, capacity behavior,
the §5.3 `call` contract, and the counters all live on the mailbox
admission path; a front-actor emulation adds a hop and changes
backpressure semantics.

## 19. Message mapping: `contramap` and `project`

Two primitives, preserving identity and fencing semantics of the
underlying ref. Their settled design decisions are normative for whoever
implements them:

```rust
impl<M: Send + 'static> ActorRef<M> {
    fn contramap<N: Send + 'static>(&self, wrap: impl Fn(N) -> M + Send + Sync + 'static) -> ActorRef<N>;
}
impl<'a, A: Actor + ?Sized> Context<'a, A> {
    fn project<B: Actor + ?Sized>(&mut self, wrap: impl Fn(B::Msg) -> A::Msg + Send + Sync + 'static) -> Context<'_, B>;
}
```

- The wrap is a **pure, cheap injection executed eagerly at every
  ingress point**; everything stateful, ordered, or observable belongs
  to the outer actor's single set of resources — one timer table, one
  continuation queue, one mailbox, one identity. Never buffer-and-replay:
  cross-call timer state (a `clear_timer` in call N seeing call N−1's
  arm) only works if there is exactly one table, typed to the outer
  message.
- Closure bound is `Fn + Send + Sync + 'static` — the wrap runs on
  sender threads and at concurrent ingress points; stateful enrichment
  belongs in the wrapper's `handle`, where `&mut self` exists and
  ordering is the mailbox's. Document that the wrap must be cheap and
  non-blocking.
- A contramapped ref **shares the outer ref's identity, id, and stats
  attribution** — the wrapper and inner actor are one actor (one
  mailbox, one loop, one lifecycle); a fabricated inner id would lie to
  monitoring. Pin this with a test.
- Backpressure, capacity, conflation, and message-size observation are
  the *outer* mailbox's; a conflating outer mailbox conflates wrapped
  self-sends (pin with a test). Do not build per-view conflation
  machinery.
- `call` needs no special support: the `Reply` rides inside the message.
- Eager conversion has an error-payload cost, decided: a contramapped
  ref's send errors carry no recoverable `N` — the wrap already consumed
  it. Mapped refs surface B.3's boxed projection (id + kind, payload
  dropped), and B.3's payload-recovery clause is scoped to unmapped
  refs. The alternatives (deferred conversion, a mandatory inverse,
  `N: Clone` on every mapped send) each break a settled rule above — one
  mailbox typed to the outer message, cheap pure wraps.
- Unwrapped actors pay one predictable enum branch; boxing exists only
  on the mapped arm; layers compose by nesting, one closure hop per
  layer. The same-`Msg` re-entry (`for_actor`, core) stays as the
  zero-cost identity case.
- `contramap` stands alone and is independently motivated. `project`
  exists only for a consumer that genuinely needs cross-`Msg` context
  mapping, and any such consumer must first clear the **provenance
  wall**: an origin-blind same-`Msg` middleware (e.g. a journal) cannot
  distinguish externally-sent messages from self-regenerated effects,
  which breaks replay-correct durability — a replayed effect message
  both replays and regenerates, running the effect twice. A design that
  works origin-blind has no need of `project`; implementing `project`
  without such a consumer is unjustified surface. Durable actors are
  the anticipated consumer (§27's durability adapter): a
  provenance-explicit wrapper — one that journals decided events or
  provenance-tagged arms, never the raw inbound stream — is not
  origin-blind, and is the shape of design that could clear the wall.

**Placement.** Internal seam — a mapped ref shares the outer ref's
identity, mailbox, and stats attribution, which no wrapper actor can
provide (the forwarding-actor emulation is exactly what this section
rejects), and `project` operates on `Context` by definition.

## 20. Peer monitoring

`ctx.watch(&ref, wrap)` (and a cancel-on-drop `watch_scoped`) — where
`wrap: impl Fn(MonitorEvent) -> A::Msg + Send + Sync + 'static`, the §19
closure discipline, since a `MonitorEvent` cannot enter an arbitrary
`Msg` mailbox unmapped — delivers `MonitorEvent`s (shape: Appendix B.4)
into the watcher's mailbox via a bounded drop-oldest queue per watch
(depth: Appendix A); overflow coalesces into one `Lagged`; terminal
`Removed` is never dropped (it is always newest); `Started` carries the
`Incarnation`; stale-membership routing uses the §3.1 primitive — that
primitive and the single publication path are the core hooks. Watching
an already-running target delivers an immediate `Started`;
re-registering a watch aliases the existing one without a duplicate
immediate `Started`. Because immediate restart can outrun an external
query, transition evidence is available from events without keeping
application-side history. In core, sibling-failure reaction routes
through the supervisor (fate-sharing) or lifecycle-stream subscription;
watch is the in-mailbox refinement.

**Placement.** Public-surface composition — the lifecycle stream, the
§3.1 primitive, and ordinary sends can implement this contract (the
immediate-`Started` dedup rides on incarnation tokens), at the cost of a
per-watch or multiplexed forwarding task; library placement removes the
hop, no seam requires it. MAY ship in the utility tier (§27).

## 21. Group strategies: `OneForAll`, `RestForOne`

New variants on the non-exhaustive `Strategy` (§10.1). Group restarts
drain the affected set, re-mint the group cancellation context, and
respawn in declared order. **Every sibling respawn charges the scope
intensity budget** (§10.2 — stated in core precisely so this section
cannot relitigate it). Group teardown reuses §11's ladder unchanged; the
exit funnel's mode-dispatch (§11) is the hook — group drain is a
`Draining { scope: subset, reason }` mode, not a second dispatch path.
Decided edges: a group respawn re-runs only restartable members —
one-shot members (structurally `Never`) are terminally removed by the
group restart per their §9 retention and do not block it, and other
`Never` members likewise stay down. The triggering child's own `Backoff`
delays the whole group respawn; siblings' attempt counters do not
advance (every respawn still charges intensity, §10.2). Intensity is
charged **atomically**: all forced respawns of one group restart are
charged together, before any member respawns — if the batch trips the
budget, the scope fails without a partial respawn. Exits arriving while
the group drains are recorded by the funnel but schedule nothing and
charge nothing (mode dispatch, §11) — they are part of the drain.
Respawn is declared-order and readiness-gated, exactly like ordered
startup (§7). Activates the group clause of obligation §16.9.

**Placement.** Internal seam — atomic intensity charging and drain-mode
dispatch live in §11's exit funnel; a lifecycle-driven emulation is racy
and cannot charge the budget atomically.

## 22. Observation extensions

All adapters over core's two streams and identity types:

- **Actor statistics** (field inventory: Appendix B.6): readable per
  membership; per-incarnation attribution distinguishable via §3.3;
  recursive scope-wide stats resolve through typed child metadata, not
  attachment scans. Brings `message_size` observation with it, added as
  a typed actor-spec extension — the measurer is code, so it lives
  outside §9's plain-data options record, exactly like §18's key
  extractor (§9's extractor boundary).
- **Child observation** — self-recovering reducer projection that resets
  with a full snapshot after lag; consumers never see a raw `Lagged`,
  they see a reset carrying the fresh snapshot and the dropped count.
- **Packaged restart-counter view** — subscription plus cumulative total
  that survives `Lagged`, deduplicated, making breaker patterns turnkey
  without hand-carried totals.
- **`metrics` feature** — optional metric emission from the same single
  choke point as `tracing`; the debugging surface exposes structured
  snapshots rather than name-filtered tuples.

**Placement.** Split. The statistics fields, counters, and message-size
measurement are an internal seam (counted inside the mailbox and loop,
B.6), and the `metrics` feature shares `tracing`'s internal choke point;
every packaged view above the two streams is a public-surface adapter
and MAY ship in the utility tier (§27).

## 23. Outline (`serde` feature)

**Purpose.** The outline is a policy-drift fingerprint: a serializable,
injective projection of a tree's *resolved* declaration, capturing
everything §10.3's inheritance machinery decides silently (scope
defaults, inherit-vs-reset at subtree edges, library fallbacks). Its
uses are golden-outline tests that pin a system's effective supervision
policy in CI without spawning anything, startup logging, and
cross-environment diffing. Ad hoc diagnostic output cannot substitute
(no injectivity or stability contract), nor can snapshots (they need a
running system and carry only part of the policy surface, B.6).

**Non-goals.** The outline carries no actor implementations, closures,
args, or state: it is a description of a declaration, not a constructor
for one. It cannot rebuild a tree, cannot "port a System" to another
machine, and distribution (§26) will not build on it — a distribution
layer needs code identity, args transport, and state handoff, and will
define its own wire format.

**Placement.** Internal seam — if this exists at all it must be a
library feature, and the binding reason is access, not the orphan rule:
the outline captures the *resolved* declaration, everything §10.3's
inheritance machinery decides silently, and no public surface exposes
that resolution for traversal. (The orphan rule alone would not decide
it — a downstream crate could mirror the policy types rather than
implement `Serialize` on library-owned ones, but it would have nothing
to fill the mirrors with.) Feature-gated `serde` derives cost non-users
nothing; core's only obligation is the "serializable where §23 needs it"
clause on the §9 options record (§15.2).

**Contract.** Any two trees that differ in the **declaration surface the
outline carries** — the policy and topology fields below — MUST
serialize differently. Injectivity is over that surface, not over code:
per the non-goals above, two trees identical in outline may still differ
in spawn outcome through the implementations, closures, or args they
carry. The outline carries, per scope: kind, strategy (ordered only),
intensity, and every scope default including mailbox capacity; per
child: kind, id, restart condition + backoff, shutdown policy, readiness
mode + deadline, terminal-membership retention, and (actors) mailbox
kind + capacity. Outlines reject unknown fields and missing required
fields on deserialization, so schema drift is loud. Ship it complete on
first release — the unknown-fields discipline makes late field additions
wire breaks for persisted outlines; this completeness obligation on
every future option is the feature's real cost, accepted for the
loud-failure property. Activates obligation §16.11.

## 24. Hosting (`host` feature)

Exposes running incarnations without a supervisor, using the **same**
incarnation runner as supervised execution — that single runner is the
core hook (§8): same exit type (including `Panicked`, so hosted users
never hand-write `catch_unwind`), same readiness handling, same teardown
ordering. It is the seam for embedding and for a future
`!Send`/thread-per-core mode. Activates the hosted-parity clause of
obligation §16.7.

**Placement.** Internal seam — the feature *is* the incarnation runner,
exposed; the downstream substitute is hand-written `catch_unwind` and
teardown, which is exactly what it exists to eliminate.

## 25. Lifetime and timing conveniences

- **Cross-actor delayed delivery** (`send_after_to` / `interval_to`): a
  mailbox-semantics facility (capacity and conflation apply), owned by
  the sender's incarnation via a `Guard` (Appendix B.7), and the only
  spawned-task timer path (§6.3).
- **Completion-driven lifetime**: bind the root's lifetime to selected
  child completions ("run until these tasks finish, then shut down") for
  finite/batch applications; composes `OneShotTaskRef` awaitables with
  `shutdown()`.
- **Sibling-readiness barrier**: first-class support for a child
  awaiting a *named sibling's* readiness (a scope-relative readiness
  barrier), replacing offload-the-wait plumbing.

**Placement.** Public-surface composition — respectively: an
incarnation-owned task that sleeps and sends (subject to the target's
mailbox semantics by construction), `OneShotTaskRef` awaitables composed
with `shutdown()`, and packaged offload-the-wait. MAY ship in the
utility tier (§27).
