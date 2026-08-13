# Library specification

This document is the definitive behavioral reference for the supervision/actor
library (the project name is out of scope here; "the library" throughout). It
is written to be sufficient for a from-scratch implementation: someone holding
only this file should be able to build a conforming library without consulting
the current codebase, its issue tracker, or its documentation.

It consolidates the current architecture's proven semantics with a series of
foundational redesigns and an API-friction inventory drawn from five
application-scale examples. Bracketed issue references (`[#361]`) are
provenance annotations pointing at the origin repository's tracker; they carry
**no required content** — every decision they informed is stated in full here.
Where this spec contradicts the origin implementation, the spec wins — that is
its purpose.

**Structure.** The spec is staged in three parts, in build order:

- **Part I — Core** (§1–§14): the v0 conformance target. Complete on its own:
  a library implementing only Part I is useful and correct.
- **Part II — Core plus** (§15–§23): features specified now, built later.
  Each is individually adoptable; each section states the hook Part I must
  already carry so the feature lands without redesign.
- **Part III — Non-core** (§24): never in the core crate.

**Normative language:** MUST / MUST NOT are conformance requirements. SHOULD
is a strong default that needs a recorded reason to break. MAY marks optional
surface. *Open* marks decisions this spec deliberately leaves to the
implementation spike (§14 lists the core ones, with the constraints any
resolution must satisfy).

Appendices are normative unless marked otherwise and span all parts, with
Part II surfaces tagged: **A** — default values and bounds; **B** — context,
handle, error, and event surfaces; **C** — acceptance scenarios and preserved
patterns.

## Table of contents

- [Part I — Core](#part-i--core)
  - [1. Design principles](#1-design-principles)
  - [2. Core model and vocabulary](#2-core-model-and-vocabulary)
  - [3. Identity](#3-identity)
  - [4. Construction and restart](#4-construction-and-restart-360)
  - [5. Mailboxes, delivery, and the event loop](#5-mailboxes-delivery-and-the-event-loop)
  - [6. Readiness](#6-readiness-363)
  - [7. Exits](#7-exits-364)
  - [8. Child specification and options](#8-child-specification-and-options-368)
  - [9. Scope policy: strategy, intensity, defaults](#9-scope-policy-strategy-intensity-defaults)
  - [10. Shutdown](#10-shutdown-370)
  - [11. Trees, spawning, and lifetime](#11-trees-spawning-and-lifetime)
  - [12. Observation](#12-observation)
  - [13. Invariant checklist](#13-invariant-checklist)
  - [14. Open questions](#14-open-questions-resolve-during-the-core-spike)
- [Part II — Core plus](#part-ii--core-plus)
  - [15. Incarnation refinements](#15-incarnation-refinements)
  - [16. Keyed conflation: `latest_by_key`](#16-keyed-conflation-latest_by_key)
  - [17. Message mapping: `contramap` and `project`](#17-message-mapping-contramap-and-project-351)
  - [18. Peer monitoring](#18-peer-monitoring)
  - [19. Group strategies: `OneForAll`, `RestForOne`](#19-group-strategies-oneforall-restforone)
  - [20. Observation extensions](#20-observation-extensions)
  - [21. Outline (`serde` feature)](#21-outline-serde-feature)
  - [22. Hosting (`host` feature)](#22-hosting-host-feature)
  - [23. Lifetime and timing conveniences](#23-lifetime-and-timing-conveniences)
- [Part III — Non-core](#part-iii--non-core)
  - [24. Out of core, permanently](#24-out-of-core-permanently)
- [Appendix A. Normative defaults and bounds](#appendix-a-normative-defaults-and-bounds)
- [Appendix B. Surface reference](#appendix-b-surface-reference)
- [Appendix C. Acceptance scenarios](#appendix-c-acceptance-scenarios-informative-in-prose-normative-in-obligation)

---

# Part I — Core

## 1. Design principles

These generated every specific rule below; when the spec is silent, decide by
these, in order.

1. **The honest case is the primitive.** One-shot, consuming, `FnOnce`-shaped
   construction is the base case; repeatability is an *added capability proven
   in the types before erasure*, never a runtime field asserted after the
   bound it should relax has been erased. [#360]
2. **One mechanism per concept.** Staleness fencing, readiness, stop
   escalation, exit classification, child identity: each exists exactly once,
   as a named primitive, and every subsystem consumes that primitive.
   [#361, #363, #364, #370]
3. **Invariants live in ownership and types, not comments.** A rule that two
   modules must both remember is a design defect. Exactly-once construction
   effects are expressed by consuming owned values (drop = the fallback
   effect), not by `Mutex<Option<_>>` take-once flags. Internal
   synchronization may still use an optional payload as a non-panicking claim
   protocol when it does not re-assert a construction capability or conflict
   with a more specific ownership requirement elsewhere in this spec. [#360,
   #363]
4. **Capabilities are never erased and re-asserted.** If the caller proved a
   capability statically (a dynamic scope, a restartable actor), every value
   derived from it carries that proof in its type. Runtime downgrades
   (`.dynamic().expect(...)`) exist only for genuinely dynamic queries such as
   name-based traversal. [#360, #365]
5. **Everything the framework's own high-level layer uses is public.** The
   blanket handler loop MUST NOT rely on any capability a hand-written raw
   actor cannot reach. [#363]
6. **At-most-once delivery, no hidden buffering.** Restart and shutdown
   windows may drop messages; nothing buffers across an incarnation boundary.
   Durable or at-least-once delivery is out of core (§24).
7. **The public API is runtime-independent.** No tokio (or other runtime)
   types are reachable from any public item; boundary types are library-owned.
   Internally, all runtime touchpoints route through the private façade over
   `shelterwood-runtime`. [#359]

**Layering.** The implementation is structured as four layers, each complete
before the next begins, so simple things are easy at the top and every layer
below is a reachable escape hatch:

- **L1 — engine**: scopes, membership cells, incarnations, spawn/lowering,
  exit classification, the shutdown ladder, intensity, readiness gating.
  The boundary is execution, not vocabulary: L1 models all three child
  kinds as typed variants (§2) and carries their declared policy —
  mailbox settings included — as plain data (§8); what it contains none
  of is actor *execution or mailbox mechanics* — no message delivery,
  no mailbox operations, no actor callbacks (§11's task-first
  embedding — supervision with zero actors — is this layer plus the
  tree façade).
- **L2 — raw actor + mailbox**: `RawActor`/`RawContext`, the mailbox kinds,
  the send flavors, `call`. The escape hatch — per principle 5 it ships with
  core, not after it.
- **L3 — handler actor**: `Actor`/`init`/`handle`, keyed timers,
  continuations, offloads. The "simple things easy" layer. Its public
  `Handler<A>` wrapper is the composition point for raw decorators and
  encapsulates the callback loop's error-path freeze-and-join discipline;
  decorators need no framework-internal teardown surface.
- **L4 — observation**: snapshots and lifecycle events over L1's single
  publication path.

**Implementation shape.** Three cross-cutting constraints on *how* the
layers are built — they determine testability and reviewability more than
any single design rule:

- **Policies are plain data.** Every policy and configuration surface —
  `RestartPolicy`, `Backoff`, `Shutdown`, `Readiness` and its deadline,
  `Strategy`, `Intensity`, mailbox settings, the shared options record
  (§8) — is a plain enum or struct: `Clone` (`Copy` where cheap), `Eq`
  (universal — a float-valued field stores a validated newtype whose
  invariant makes bit-equality correct: §9.2's backoff factor),
  serializable where Part II §21 needs it, and carrying **no
  behavior** beyond small pure derivation functions of the
  `should_restart(exit)` / `next_delay(restart_attempt, JitterSample)`
  shape. Runtime behavior is *derived from* the data by local
  functions; it is never encoded as trait objects, callbacks, or builder
  side effects. Plain data is not *open* data: every payload carrying an
  invariant (§9.3's eager validation — non-zero durations, the backoff
  factor, the intensity window, mailbox capacity) is a **sealed** struct or
  newtype whose only mint is its validating constructor, with read access
  through accessors. Making the invalid unrepresentable is what retires
  validation as a re-runnable step: there is no second boundary that could
  re-reject a value, so no `validate()` survives on the public surface and
  no error variant exists downstream to report one (B.8). Partiality that is
  legitimate *declared* state — an unset scope default, a deferred mailbox
  capacity, an inherited deadline — stays openly representable; only the
  values inside it are sealed.
- **Pure core, mutable shell.** The engine's decision layer is a
  synchronous state machine: `step(state, event) -> effects`, where
  `event` carries everything external as data (a child exit, a command, a
  deadline having been reached, `now` as an argument, a pre-drawn jitter
  sample) and `effects` is data describing what the shell should do
  (spawn this, abort that, arm a deadline, publish a snapshot). Decision
  modules contain no awaits, no clock reads, no channel operations, no
  spawns, and depend on neither Tokio nor `shelterwood-runtime` — only the
  thin driver shell does, feeding events in and executing effects out. This
  is what makes policy behavior unit-testable with no runtime at all (§13),
  and it is why the ladder (§10) is `advance(now) -> Option<action>`
  rather than a sleeping task per child. The same shape applies
  opportunistically to the actor loop's selection policy (§5.2 fairness
  and timer retraction): decide the next action as a function of observed
  loop state, then await it. User code — actor bodies — is of course
  effectful; the constraint binds the engine.
- **Locks hold framework data only.** A critical section over a framework
  mutex manipulates plain framework-owned state and nothing else: wakes,
  destruction of user-owned values, formatting of user types, user
  callbacks, and panic resumption all happen after the guard is released.
  The framework runs user code it did not schedule — waker vtable
  functions, destructors of messages, actor state and the type-erased
  error inside an `Exit`, `Hash` on a timer key — and every one of those
  may panic, block, or re-enter, which under a lock is a poisoned mutex, a
  deadlock, or an abort during unwind. The mechanism is an effects value
  that accumulates the user-visible work inside the critical section and
  discharges it, panic-contained, from `Drop` — the observation
  transaction and the mailbox promotion are the two reference
  implementations, and moving a value out (rather than dropping it in
  place) is the degenerate case. What the rule buys is that a hostile
  waker or destructor is an ordinary, testable outcome instead of a
  liveness failure (§13.18).
- **Promises are owned completions.** Every cross-task promise — an
  admission or removal awaiting resolution, an exit report awaiting
  publication, a resident child awaiting its `Removed` edge, a member
  cell awaiting terminality, a waiter awaiting a wake — is held as an
  owned value whose destructor discharges it, fail-closed, with a
  **synchronous** fallback: complete with the terminal rejection,
  publish the coarse exit, emit the edge, pulse the signal — never an
  await, never a join. The event loop that services the orderly path is
  an optimization of these values' consumption, never the sole
  guarantor: when a driver future is destroyed at any await point —
  hard-abort cascade, panic, natural return with events still queued —
  unwinding alone MUST discharge every outstanding promise (§10's
  driver-death rule, §13.17's test anchor). Two corollaries are
  normative. Residency in a scope's observed child set is itself such a
  value — its drop emits `Removed`, making §3.2's exact pairing
  structural rather than remembered. And each cell has exactly **one**
  change signal from which every compound wait derives — a second wake
  path is a lost wakeup waiting to be written. (Structural adoption is
  tracked as #17; the discharge behavior is required now.)

**Performance posture.** The decision layer is a control plane — its
events are exits, restarts, commands, and deadlines, rare even in a
restart storm — so its purity is not a performance concern and is never
relaxed. The hot path (`send → mailbox → recv → handle`) lives in the
shell, which the shape above never constrained. Three permissions are
normative so that "pure" is not misread into pessimization:

- `step` MAY take `&mut State` — pure means no I/O, clocks, randomness, or
  awaits; it does not mean persistent data structures or defensive clones.
- Effects MAY flow through a sink (`step(&mut state, event, &mut impl
  EffectSink)`) rather than a returned `Vec` — tests pass a `Vec`,
  production passes an inline executor; hot steps allocate nothing.
- The actor loop's next-action decision is a small `Copy` enum, not a
  boxed plan.

Where measured cost actually concentrates, the mitigations are (SHOULD):
snapshots published as `Arc`-shared immutable values (clone = refcount),
publication skipped when no subscriber exists — a subscriber-channel
optimization only: pull-side `snapshot()` is an on-demand projection
and can never go stale from the skip, and a fresh subscription's
initial value is computed at subscribe time (§12) — (the conflating
watch already makes cost O(observations), not O(events)); per-message
`tracing` built lazily and near-zero with no active subscriber. What stays
unsanctioned even under measured pressure: clock reads or randomness
inside decision modules (pass `now` and samples in — it costs nothing),
dispatch paths that bypass the exit funnel, and per-call-site staleness
shortcuts — the origin's exact failure modes in a performance costume.
Measure before relaxing anything else; the origin's impure 33k-line
implementation never once found the supervision engine hot.

## 2. Core model and vocabulary

A running **system** is a **tree** of **scopes** — "system" is the vocabulary
for the running instance throughout, as distinct from the `Tree` /
`DynamicTree` declaration it is spawned from (§11) and from the **engine**,
the L1 machinery that runs it (§1). A scope is one supervisor node.
Each scope owns an ordered or unordered set of **children**. A child is one of
three **kinds**, and the engine models all three as first-class typed
variants — there is no erased side channel through which a child's kind or
metadata is smuggled and recovered by downcast [#362]:

- **Actor** — a mailbox-owning message loop (§5).
- **Task** — an arbitrary supervised future with a `TaskContext`.
- **Scope** — a nested supervisor (subtree).

Scope flavors:

- **Ordered** — declared membership fixed at build time; sequential,
  readiness-gated startup in declaration order; reverse-order teardown.
- **Dynamic** — runtime membership; concurrent start/stop; per-child
  fate-sharing only (strategy is structurally `OneForOne`; the strategy knob
  does not exist on dynamic scopes [#371]).

**Child ids.** Every child has an id: a non-empty UTF-8 string, unique among
the **resident** memberships of its containing scope — live *or*
retained-terminal: a tombstone kept for observability (§8's retention
semantics) still occupies its id until pruned. Sibling scopes may reuse an id;
an id is meaningful only relative to its scope. Duplicate or empty ids are
rejected **at the point of declaration or insertion** (§9.3 eager
validation), not at spawn. Ids identify *slots for humans and traversal*;
they are not identity — identity is the membership/incarnation machinery of
§3, and an id can be reused by a later, distinct membership (§3.4).

Identity vocabulary (all three levels are distinct and all three are values,
§3):

- **Membership** — a child's slot in a scope across restarts. Created by
  declaration or dynamic insertion; ends at terminal removal.
- **Incarnation** — one run of a membership. A restart mints a new
  incarnation of the same membership.
- **Generation / lineage** — the internal coordinates of incarnation and
  membership respectively; exposed only through the identity types, never as
  bare integers [#361].

Ownership: exactly one owning handle (`System`) exists per spawned root;
dropping it requests graceful shutdown (`System` is not `Clone` and is
`#[must_use]`). All other handles (`ScopeRef`, `ActorRef`, `TaskRef`, …) are
non-owning, cheap, `Clone`, and address **memberships** by default — they ride
through restarts and fail only on terminality. Incarnation-pinned addressing
is an explicit refinement (Part II §15). One deliberate exception:
`OneShotTaskRef<T>` is an owned, non-`Clone` completion claim (§4.2) — the
cheap `Clone` `TaskRef` for the same child exists alongside it.

## 3. Identity

### 3.1 One fencing primitive [#361]

The implementation MUST have exactly one staleness-fencing primitive, used
everywhere the question "is this event from the current incarnation /
membership?" arises: bindings, monitors, stable channels, snapshot
publication, child-event handling, attachment/metadata publication.

- An `Epoch`-like ordered token with a `supersedes` relation and a single,
  centrally-decided overflow policy. Bare integers MUST NOT be threaded
  positionally; `(lineage, generation)` MUST be one value, not two adjacent
  `u64`s.
- **The overflow policy is: fail closed.** Counters are `u64` and advance by
  saturation; a saturated fence rejects all subsequent comparisons rather
  than wrapping. Rationale: the two candidate policies have *opposite*
  failure modes at the limit — wrapping makes a fence **accept a stale
  value**, saturating makes it **reject everything forever** — and for a
  staleness fence, rejecting is the safe failure. The limit is unreachable
  at `u64` scale in practice; the point of deciding it here is that exactly
  one shared primitive decides it once, instead of each counter choosing
  independently. (The origin implementation had nine counters in five
  representations with three different overflow policies, two of them
  wrapping.)
- Saturation alone would still break equality at the limit: repeated
  minting from a saturated counter would issue the same value for
  distinct identities, and token equality promises exact identity
  (§3.2). So the primitive separates *advancing* from *minting*, and
  **the saturated value is a poisoned terminal, never a minted token**:
  minting requires a successful advance, and once the last usable value
  has been spent the counter mints no successor. What that failure means
  is decided per counter role, both structural and fail-closed: an
  unmintable **incarnation** (a restart reaching exhaustion) is simply
  not scheduled — the membership terminalizes exactly as under `Never`,
  its last published exit standing as the terminal state; an unmintable
  **membership** fails the reservation or admission with a distinct,
  enumerated exhaustion rejection (the exhaustive reserve/admission errors
  carry it, B.8). Unreachable at `u64` scale either way — the
  rule exists so identity stays exact even in the theory, and so
  §13.4's fail-closed property has no duplicate-token counterexample.
- Inside a scope's runtime, the child address is a versioned handle whose
  **resolution is the staleness check**: resolving a stale address yields
  `None`. Per-call-site ad-hoc comparison of identity-field subsets MUST NOT
  exist; an unchecked panicking index into child state MUST NOT be reachable
  outside the resolution module.

### 3.2 Membership identity is minted before placement, for every kind [#366]

Every child — actor, task, subtree — gets a **membership cell** reserved at
declaration (or at the start of a dynamic insertion) and stamped atomically at
insertion. All handles resolve through their cell.

`Membership` is a public opaque token — the membership-level twin of
§3.3's `Incarnation` (trait matrix: B.10 — `Copy`-cheap, `Eq + Hash`,
`Send + Sync`). It appears wherever a membership is identified: events
(B.4), returned admission handles (B.8), snapshots (B.6), and §7's
structured error payloads. Both tokens are views of §3.1's one fencing primitive;
`Incarnation::membership()` projects an incarnation's owning membership,
and equality between an incarnation's projection and a held membership
token is the "same slot?" question answered exactly. `supersedes` on
`Membership` orders tokens only while one stable owning scope retains the
same child-id lineage, such as declaration reconciliation before the prior
membership terminalizes. Terminalization evicts that lineage (§3.4), so a
later remove-and-re-add is deliberately incomparable in both directions.
Different child ids and different owning scopes are likewise incomparable and
return `false` in both directions (fail closed, §3.1's rule; never a panic).
Equality is exact identity; there is deliberately no total `Ord` — comparison
outside one retained `(scope, child-id, lineage)` domain has no meaning.

Consequences (normative):

- Handles exist **before spawn** for all kinds. Cross-wiring (actor A needs
  task B's ref; two actors reference each other) is done by declaring cells
  first — the slot-before-define pattern is uniform, not actor-only. The
  concrete public surface is §8's slot API (`reserve_*` / `define`).
- A cell can carry the child's configuration; there is no ordering rule of
  the form "apply options to the returned spec, not the slot".
- `remove` and lookup by handle are cell reads, not scans. There is exactly
  one "child not found" outcome, not one per handle flavor.
- Dynamic mutations resolve **at admission**: the `add_*` future's value is
  the stamped cell's exact per-kind handle set, and startup is never part of
  the call — observe startup separately through those handles (B.8). A caller
  that abandons or times out its own startup wait therefore already holds
  the exact identity needed to reconcile or remove — closing the
  unknown-outcome window without application-level epoch bookkeeping.
- **Minting is identity; admission is membership.** Between `reserve_*`
  and admission a dynamic cell is *identity without residency*: the id
  is claimed (`DuplicateId` against every other caller, §8), the slot's
  handles resolve through the cell, and subscriptions through them are
  live — but the cell is not yet a member of its scope. It appears in
  no snapshot (`children`, `child(id)`, `descendant(path)` do not know
  it), emits no `Added`, holds no admission-order position (B.6), and
  feeds no counters. Admission is where public membership begins: the
  `Added` event fires and the child enters `children` at its admission
  position, state `Admitted` (B.4, B.6). Consequently a cell
  terminalized before admission — dropped slot, withdrawn fused call,
  `NotAdmitting` rejection, removal by id (§8) — emits no `Removed`
  either (B.4's `Added → … → Removed` pairing is exact: both edges or
  neither), leaves no tombstone under any retention setting (§8's
  retention governs admitted members), and frees its id at
  terminalization; the terminalization bullet below still holds in
  full — the *cell's own* observers get the structured closure, the
  parent's event stream just never mentions it. (The builder flavor
  differs only in where the edge sits: a declared cell's admission is
  lowering at `spawn()`, so its `Added` fires there, while §3.2's
  pre-spawn declaration projection may already show the row — under
  the same membership token, minted at declaration, so a catch-up
  reader applying that `Added` to the already-projected row performs
  the identity.)
- A reserved cell whose tree is never spawned, whose spawn fails, or whose
  insertion is rejected is **terminalized**, never leaked: its handles
  resolve terminal, pre-spawn sends fail `Terminated`, and subscriptions
  close — nothing parked against it hangs. A membership that terminalizes
  with no incarnation ever spawned publishes the membership-level
  `NeverStarted` exit (§7), so exit-awaiting surfaces (`TaskRef::wait()`,
  `OneShotTaskRef<T>`) resolve with a structured outcome rather than
  hang. A *scope* membership additionally publishes its terminal scope
  state — a final `Stopped { reason: NeverStarted }` snapshot and
  `ScopeState` event (B.6, B.4) — before its streams close, so
  `wait_stopped()` (B.9) and snapshot/lifecycle subscribers resolve
  structurally, never merely by stream closure.
- When a mailbox is attached at terminal publication, publication has one
  precise internal order: store the terminal cell record, synchronously
  discharge parked mailbox operations, then pulse the cell's single change
  signal. A direct or reentrant borrow MAY therefore observe the terminal
  record while mailbox discharge is still in progress; the guarantee is
  **discharge-before-pulse**, not discharge-before-store. If terminality wins
  before attachment, it stores and pulses first; later attachment immediately
  closes and discharges the mailbox without a second terminal pulse. A panic
  collected while waking mailbox operations MUST be resumed only after the
  complete parent snapshot/lifecycle publication and nested observation-close
  transaction, so a hostile mailbox waker cannot strand membership waiters or
  skip the matching terminal observation edges.
- Declaration is O(n): no re-projection of the full child list on every
  builder mutation; no shadow runtime object maintained during declaration;
  no global counters joining side tables [#367]. Pre-spawn snapshots, if
  offered, are computed on demand from the declaration.

### 3.3 Incarnation identity is addressable [#361]

`Incarnation` is a public opaque token: `Copy`-cheap, ordered within its
membership (`a.supersedes(b)` — across memberships it is `false` both
ways, like §3.2's membership rule), comparable for equality across the
API, and projecting its owning membership (`membership()`, §3.2).

- `Context::incarnation()` returns the current incarnation. (The task-side
  `TaskContext` exposes the same.)
- Lifecycle events and snapshots expose the token wherever the origin
  exposed bare generation numbers. (Part II's monitor events carry it too,
  §18.)
- `ActorRef::send`/`call` results and errors expose the incarnation that
  accepted (or was observed at failure). **This lands in core** even though
  the pinning refinements are Part II: retrofitting a token into every
  result/error type later is exactly the origin's mistake with bare integers,
  repeated.
- Membership addressing remains the default everywhere. Snapshot generation
  comparisons use `supersedes`/ordering, and documentation MUST teach
  ordering, not equality-with-increment (a restart storm can advance an
  incarnation by more than one between two observations).
- Incarnation-pinned sends and the await-next-incarnation helper are
  Part II (§15).

**The retry discipline these tokens exist to support** (this MUST be taught
in the request/reply documentation; it was hand-built by the shard-store
acceptance scenario in the origin repo and is the semantic contract for
`CallError`, Appendix B.3):

1. Retry after `ReplyDropped` **only if** the operation is idempotent, and
   only under one overall deadline for the whole logical operation.
2. Before retrying, await the incarnation-after: retry only once a *newer*
   incarnation than the one that dropped the reply is running (otherwise the
   retry lands in the same doomed mailbox or the rebind window). In core,
   observe this via lifecycle events or snapshots; §15's awaitable is the
   packaged form.
3. **Never blindly retry `ResponseTimedOut`** — acceptance happened, the
   outcome is unknown; reconcile against durable evidence (e.g. the prepared
   image) instead of resending.
4. `AcceptanceTimedOut` is guaranteed-not-accepted and always safe to retry.
5. Retry horizons and effect-ledger retention are application-side bounds by
   design: the library does not prescribe durability, and applications MUST
   bound or garbage-collect idempotency ledgers once retries become
   impossible.

Part II packages the mechanical steps — §15's `call_idempotent` encodes 1,
2, and 4; steps 3 and 5 remain application obligations under any surface.
This discipline stays specified here regardless: it is the semantic
contract behind `CallError` that core's documentation MUST teach, and core
ships before §15 exists.

### 3.4 Replacement memberships

An `ActorRef` follows incarnations of **its** membership, never a same-id
replacement membership. This boundary is kept (it is what makes identity
exact), but it MUST be discoverable: removal-then-re-add under the same id
yields a fresh membership whose handles come from the new insertion, and the
old handles report terminal. Terminalization evicts the retained child-id
lineage: the replacement and removed membership are deliberately incomparable
in both directions. The same fail-closed rule applies to an initially declared
child and its later runtime replacement, and to corresponding descendants
rebuilt across incarnations of one nested scope membership. A stable scope may
order a provisional declaration only while it still retains the same live
lineage; a temporary builder never defines the ordering domain. Different ids
and different owning scope memberships remain incomparable. A small
routing/registry adapter for planned handoff (a `ServiceRef`/route-cell that the
application repoints at cutover) is non-core (§24) — it must not weaken exact
membership identity.

## 4. Construction and restart [#360]

### 4.1 The actor contract

```rust
trait Actor: Sized + Send + 'static {
    type Msg: Send + 'static;
    type Args: Send + 'static;              // per-incarnation input, consumed

    // Declared in the desugared RPITIT form with `+ Send`; implementors
    // write plain `async fn` (see the Send-bound rule below).
    fn init(args: Self::Args, ctx: &mut Context<'_, Self>)
        -> impl Future<Output = Result<Self, ExitError>> + Send;
    fn handle(&mut self, msg: Self::Msg, ctx: &mut Context<'_, Self>)
        -> impl Future<Output = ExitResult> + Send;
    fn on_stop(&mut self, ctx: &mut StopContext<'_, Self>)
        -> impl Future<Output = ()> + Send { async {} }   // optional
}
```

- **Callback futures are `Send` by declaration.** The trait methods use the
  desugared `-> impl Future + Send` form (likewise `RawActor::run`, §4.3)
  because the single incarnation runner (§7) is generic over the actor type
  and hands the incarnation future to the runtime's multithreaded spawn
  through the private façade over `shelterwood-runtime` — under bare `async fn`
  sugar a generic `A`'s
  callback futures carry no `Send` bound and no such runner compiles.
  Implementors still write plain `async fn`: an ordinary implementation's
  future is auto-`Send` and satisfies the bound; one that holds a `!Send`
  value across an await fails at its own impl site with a targeted error,
  not at a distant spawn. Runner-side `Send` where-clauses (return-type
  notation) are nightly-only, which D.2's stable pin rules out — the same
  reasoning as `associated_type_defaults` below. The asymmetry decides the
  default: relaxing `+ Send` later is non-breaking for implementors, adding
  it later breaks them. `!Send`/thread-per-core execution is Part II §22's
  seam, not a v0 concern.
- Construction and startup are one thing: `init` consumes fresh `Args` and
  produces the actor. There is no separate factory `build` / `on_start`
  split, and no factory derive [#360]. (Consequence for packaging: no derive
  proc-macro crate exists at all — see Appendix D.)
- Durable-vs-incarnation-local state is expressed by what `Args` carries
  (e.g. an `Arc<AtomicU64>` in `Args` survives restarts because cloning args
  clones the handle) versus what `init` constructs.
- **`type Args = ();` ceremony for trivial actors is accepted as-is** — one
  line per trivial actor, no convenience subtrait. Blanket-impl cleverness to
  remove it creates coherence (E0119) risk out of proportion to one line of
  boilerplate; a true trait-side default (`type Args = ();`) is nightly-only
  (`associated_type_defaults`), which D.2's stable pin rules out. Revisit
  only after a full migration if it demonstrably grates — and note that if
  the feature stabilizes, adding the default later is non-breaking (existing
  impls naming `Args` stay valid), so deferring costs nothing.
- `ExitResult` is plain `Result<(), ExitError>` — the exact contract of the
  handler, raw, and task layers (`Ok` classifies `Completed`, `Err` classifies
  `Failed`; §7). Infallible handlers write `Ok(())`; there is no
  `IntoExitResult` conversion trait. There is deliberately no stop-outcome
  return type: clean self-stop is `ctx.stop()` alone (B.1 — effective after
  the current callback, `Err` outcome wins, idempotent), so the return channel
  carries errors only and stop has one mechanism (§1 principle 2). Constraints
  that MUST hold:
  - The blanket loop applies its own `?` only after awaiting the callback's
    exact `ExitResult`.
  - `RawActor::run` (§4.3) stays on the exact, explicit exit contract — the
    raw layer is where explicit runtime mechanics belong.
  - Supervised **task closures keep an exact-type bound**, and the two task
    modes have two signatures: restartable tasks are bound `Future<Output =
    ExitResult>` exactly; one-shot tasks are bound `Future<Output =
    Result<T, ExitError>>` exactly, `T` being the typed completion value
    (§4.2) inferred from the closure's `Ok` arm (`T = ()` for a bare
    `async { Ok(()) }`). In both, the equality bound is what supplies the
    contextual error type that lets such closures compile; relaxing either
    to a conversion-trait output makes the closure ambiguous (E0282). The
    signatures are deliberately distinct — one signature cannot both return
    exactly `ExitResult` and carry a typed completion. Unit-returning task
    closures, if wanted, get a deliberately named separate entry point.
    The concrete closure shapes — `Fn`/`FnOnce`, the `TaskContext`
    parameter, and the exact future bounds — are pinned once, in §8's
    slot surface.
  - **Trait-spelling finding (Rust 1.97.1):** a downstream,
    warnings-denied plain `async fn handle(...)` implementation against the
    nested opaque `-> impl Future<Output = impl IntoExitResult> + Send`
    declaration trips `refining_impl_trait_internal`. Narrowing `handle` to
    the exact `-> impl Future<Output = ExitResult> + Send` declaration shown
    above is warning-free for ordinary plain-`async fn` implementations of
    both `Actor` and `RawActor`; explicit desugaring is not required. The
    finding came from a bootstrap compiler experiment; no standalone probe
    suite is retained. The real trait and its implementations are the compiler
    regression as they land, and a future toolchain change that revisits this
    contract is deliberate.
    An associated output type remains rejected: without nightly-only
    associated-type defaults (the `Args` reasoning above), it adds a per-impl
    ceremony line to every actor to save one, a net loss.
- `init` runs inside the supervised future: an init panic or error follows
  the normal supervision path, classified as a startup failure.
- `on_stop` is best-effort teardown; it runs under the child's shutdown
  grace and its context is the narrowed `StopContext` (§5.4, Appendix B.1).
  A panic in `on_stop` is classified `Panicked` by the fallback report
  token, superseding the run's outcome (§7).

### 4.2 One-shot is primitive; restartable is proven

Restart means: re-run `init` with freshly minted `Args`. Therefore restart
capability *is* the caller's proof that args can be re-minted, and it is
established in the construction-path types, before erasure:

- **One-shot** (the `add_*_once` family, any child kind): args are owned and
  consumed. No restart-configuration methods exist on these spec forms;
  never-restart is structure, not configuration. Terminal membership removal
  defaults on, overridable for observability. `_once` means one
  *incarnation* — args consumed at construction — not one *iteration*:
  run-once behavior needs no variant at all, since a clean completion under
  `OnFailure` is not a failure and never restarts (§7). The twins differ in
  what they can accept (owned, non-re-mintable args) and return
  (`OneShotTaskRef<T>`), not in how long the child runs.
- **Restartable** (`add_*`-family): the caller supplies an args *source* —
  `Args: Clone + Sync`, or `Fn() -> Args + Send + Sync + 'static` re-minted at
  restart time (so a restart can observe the current world: re-resolve an
  address, fresh timestamp).
  This is OTP's `{M, F, A}` translated into ownership: the args value is the
  per-spawn argument, and whether you can clone or re-mint it *is* your
  restart capability. For subtree children the args source *is* the
  declaration source: restartable `add_subtree` takes
  `impl Fn() -> T + Send + Sync + 'static` (`T: Subtree`, §11), re-invoked at
  restart so each incarnation lowers a fresh single-use tree; the `_once` twin
  consumes a tree value outright.
- The erased internal representation of one-shot construction is
  `FnOnce`-shaped all the way down: the closure owns the resource, the
  runner owns the closure, and init-panic / startup-failure /
  shutdown-before-start all reduce to dropping the owner. Within construction
  payloads, `Mutex<Option<_>>` take-once tricks MUST NOT appear, publicly or
  privately — and the same owned-token shape MUST be reused for construction
  claims throughout the lowering and incarnation path. This prohibition does
  not cover internal, non-panicking synchronization claims such as disposal
  completion, where losing the claim race is an ordinary no-op rather than a
  re-asserted construction capability. The independent owned-token and
  consuming rules for readiness (§6), exit reports (§7), guards (B.7), and
  public exactly-once operations (B.10) remain mandatory (§1 principle 3).
- **Every user-supplied construction source executes inside the single
  incarnation runner** (§7): the restartable forms' shared
  `Fn() -> Args + Send + Sync + 'static` factory and `Args::clone`, task body
  factories, raw-actor factories, and subtree factories with the lowering
  they trigger (§11) all run within the incarnation future they are
  constructing. A panic in any of them is that
  incarnation's own exit, classified `Panicked` by the ordinary path —
  never an engine crash, never a per-source failure model — and §13.5's
  drop guarantees hold unchanged, because the source is owned by the same
  future whose destruction is the fallback effect. Source invocation has
  no error channel of its own: `init`'s `Err` is the one structured
  startup failure; factories and `Clone` signal only by panicking.
- The start path SHOULD take ownership of the boxed `FnOnce` when scheduling
  the only incarnation, making a second invocation *unrepresentable*. Where
  the type system cannot prove it, a second invocation of a one-shot
  construction is a framework bug and **panics with a clear message**
  (caught by supervision) — a deliberate, tested decision. It MUST NOT
  degrade to a synthesized error future that masks the framework bug.
- All three kinds are symmetric: each has both modes, built once at
  the child-spec layer (§8 names the entry points: six kind forms plus
  the raw-actor pair), inherited by any
  future kind. One-shot tasks additionally expose a **typed completion
  value**: their closure is bound `Future<Output = Result<T, ExitError>>`
  exactly (§4.1), and the add yields — alongside the ordinary cheap
  `Clone` `TaskRef` — a single **owned, non-`Clone`** `OneShotTaskRef<T>`.
  Awaiting it consumes it and yields `Result<T, _>` with the §7 exit type
  as the error: `T` exists only for `Completed`; panic, abort, and
  readiness-timeout arrive as the same structured exit every other
  consumer sees, and a task that never completes resolves the claim with
  its terminal exit — including the membership-level `NeverStarted` exit
  when the child is terminalized before any incarnation runs (§7). Ownership is the multi-waiter answer (§1 principle
  3): exactly one claimant per completion value, decided in the types —
  fan-out is the application's move after the claim (`T: Clone`,
  `Arc<T>`); dropping the unawaited handle discards the value without
  affecting the task.
- **Shutdown racing an in-flight `init` — decided semantics:** `init` is
  *not* cancelled. It runs to completion under the child's cooperative
  shutdown grace; grace expiry hard-aborts by dropping the whole incarnation
  future, which drops `init`'s owned `Args`. Shutdown-before-start drops the
  unscheduled owner. All four paths — init panic, startup failure,
  shutdown-before-start, normal exit — MUST drop one-shot resources exactly
  once, with a dedicated test for each (§13.5).

### 4.3 Raw actors

`RawActor` remains the minimal loop-owning contract beneath `Actor`:

```rust
trait RawActor: Send + 'static {
    type Msg: Send + 'static;
    // Type-level definition metadata, read before incarnation construction.
    fn readiness() -> Readiness { Readiness::Immediate }        // §6
    // Desugared per §4.1's Send-bound rule; implementors write `async fn`.
    fn run(&mut self, ctx: &mut RawContext<Self::Msg>)
        -> impl Future<Output = ExitResult> + Send;
}
```

- The high-level `Actor` is a blanket `RawActor` implementation — one
  generated receive loop, not a separate execution path.
- `run` borrows the incarnation-owned `RawContext`; one raw context is
  coextensive with one incarnation. Decorators may re-enter an inner actor
  on the same context and share its readiness, stop state, timers, offloads,
  watches, and identity; the context cannot escape into work that outlives
  the run.
- The framework invokes `run` at most once on an incarnation's root raw-actor
  value and never re-enters `run` on that value. Shutdown may destroy a root
  value before its run begins; a restart that reaches construction obtains a
  fresh root value from the definition's source.
- `Handler<A>` is the public composition point that encapsulates the generated
  callback loop, including its error-path freeze-and-join discipline.
  Decorators wrap `Handler<A>` through the public raw-actor surface; they do
  not perform that discipline themselves and need no access to the
  framework-internal resource operations that implement it.
- Raw actors have their own construction path — §8's `define_raw` /
  `define_once_raw` on `ActorSlot`, with fused `add_raw` / `add_raw_once`
  entry points on both scope flavors. There is no `init`/`Args` phase at
  this layer: the actor value itself is the per-incarnation input, so
  §4.2's args-source rule applies to it directly — the restartable form
  takes `impl Fn() -> R + Send + Sync + 'static`, re-invoked at each restart;
  the one-shot form consumes an owned `R`. The value therefore exists
  before `run` is called. The shared options record applies unchanged
  (mailbox settings included — honoring `mailbox_shutdown` is the raw
  loop's own obligation, §10/B.1); readiness defaults `Immediate` per §6.
- **Resolved by the M3 §14.1 spike:** `init` and `Args` remain entirely an
  `Actor`-layer concern. `RawActor::run(&mut self, ctx)` is unchanged and
  construction-agnostic. The handler loop lives in the public `Handler<A>`
  raw-actor wrapper, which owns the `Uninit(Args) → Running(A)` transition;
  `ActorDef` and `ActorOnceDef` construct that wrapper rather than relying on
  a blanket `impl<A: Actor> RawActor for A` (which cannot exist before
  `init` produces `A`). The wrapper supplies `AfterInit` as its type-level
  readiness default; the engine resolves that default with any child-definition
  override before constructing an incarnation, and only an effective
  `AfterInit` mode performs the automatic post-init `mark_ready`. `Immediate`
  and `Manual` retain their declared meanings. Raw decorators can wrap
  `Handler<A>` directly and may
  await before delegation without changing readiness. Handler decorators use
  the zero-cost same-message `Context::for_actor` / `StopContext::for_actor`
  reborrow, sharing identity and incarnation-owned resources. Executable
  shard-store and nested assistant-control-plane spike ports validate both
  composition paths, readiness gating, exact-handle replacement, nested
  dynamic teardown, and stage preservation. **Verdict: accept the provisional
  wrapper design; no decorator-ergonomics blocker remains.**

## 5. Mailboxes, delivery, and the event loop

### 5.1 Mailboxes

Two kinds in core, no unbounded option (a third, keyed conflation, is
Part II §16 — the `Mailbox` constructor surface is non-exhaustive so adding
it is not a break):

- `queue(capacity)` — bounded FIFO with real backpressure: a full queue makes
  `send` wait, it never evicts.
- `latest()` — single conflating slot (capacity is structurally 1).

Capacity parameters MAY be omitted to defer to the scope default (§9.3's
kind-matched resolution); the library default capacity is given in
Appendix A. Zero capacity is rejected
**at construction** (a non-zero type or immediate error), not at spawn
[#369].

Delivery is at-most-once (§1 principle 6). Send flavors on `ActorRef`
(error taxonomy in Appendix B.3):

- `send` — waits; restart-transparent: parks while the membership is unbound
  (a restart window) and through FIFO backpressure; fails only on
  terminality. Cancellation is **linearized at acceptance**, exactly as
  for `call`: dropping the send future *before* acceptance withdraws
  the message (Appendix B's structural withdrawal — it provably never
  was and never will be accepted); dropping it *after* acceptance
  abandons only the wait — the message is already queued and is
  delivered normally, and at-most-once is untouched because the
  message was accepted exactly once. There is no state in which a
  cancelled send leaves the mailbox uncertain: acceptance and
  withdrawal race into the mailbox and exactly one wins. Success
  resolves to the accepting `Incarnation` (§3.3), not `()` — `try_send`
  and `send_timeout` likewise.
- `try_send` — fail-fast: distinct outcomes for unbound-right-now (rebind
  window), full, and terminal. The documented choice for teardown-window
  notifications (§10).
- `send_timeout` — `send` with a `DeadlineBudget`; a zero budget fails
  immediately. `TimedOut` is reported only once withdrawal has
  succeeded, so it always means guaranteed-not-accepted — the
  recovered message (B.3) is safe to re-send. The deadline tie has one
  explicit rule, Appendix B's expiry boundary: an acceptance that wins
  the race at the deadline instant resolves the send successfully;
  the tie is decided by the withdrawal race, never by clock
  comparison.
- `call` — request/reply via a `Reply<T>` capability embedded in the
  message. The defining shape is pinned: the caller supplies a message
  **constructor**, not a message —

  ```rust
  fn call<T: Send + 'static>(
      &self,
      make_msg: impl FnOnce(Reply<T>) -> M,
      deadline: impl Into<DeadlineBudget>,   // trailing, per Appendix B
  ) -> impl Future<Output = Result<Replied<T>, CallError>>;
  // Replied<T>: the reply value plus the accepting Incarnation (§3.3)
  ```

  — so any user message type can carry the `Reply` wherever it chooses,
  and the framework never needs to see inside `M`. `FnOnce` is deliberate:
  one call mints one reply capability (§15's `call_idempotent` is where
  the re-mintable `Fn(Reply<T>) -> M` form lives, per §4.2's capability
  rule).
  **One deadline** covers binding wait, mailbox acceptance, and response
  (one budget, not two hand-ordered constants [#352]); the error
  distinguishes *where* it expired: acceptance timeout (guaranteed not
  accepted, safe to retry) vs response timeout (accepted, unknown outcome —
  reconcile, don't retry; §3.3). Cancellation mirrors expiry: dropping the
  `call` future before acceptance withdraws the message (Appendix B's
  withdrawal rule — it provably never was and never will be accepted);
  dropping it after acceptance abandons only the reply — the message
  stays accepted and is processed normally. `Reply<T>` is `Send + 'static`
  and consumed by `send(T)`, which is infallible: if the caller is gone
  (cancelled, timed out, dropped), the value is discarded — handlers
  never branch on caller liveness. Holding a `Reply` without responding
  models
  a pending acknowledgement; dropping it is observable to the caller as
  `ReplyDropped`. A successful `call` exposes the accepting incarnation
  alongside the reply value — §3.3's retry discipline needs it on success
  as much as on failure. `Reply::channel()` exists as the split escape
  hatch when the reply must be awaited elsewhere: it yields the `Reply`
  plus a `ReplyReceiver<T>`, whose `recv(deadline)` (trailing deadline
  covering only the response wait — acceptance evidence is the
  accompanying send's result) resolves per B.3. Awaiting `call` on
  `myself()` inside `handle` is a guaranteed deadlock — the reply needs
  the very handler being blocked; use `continue_with`, or
  `Reply::channel()` with an offload (documented hazard).

**Binding.** The mailbox binding is membership-owned: created at insertion,
outliving incarnations and actor destruction (§5.5). *Bound* — accepting
sends — is an incarnation property: acceptance opens when an incarnation is
spawned (so messages are accepted while `init` runs and delivered once the
loop starts) and closes at the intake freeze when the incarnation begins
stopping (§5.2 — the freeze precedes drain and `on_stop`, which is what
makes the drained log exactly the accepted prefix; under `latest()`, its
surviving slot — §5.2's freeze rule) or, for an incarnation
that ends without a stop phase (panic, hard abort, plain return), at its
exit publication (§7); outside that window sends park (`send`) or fail fast
(`try_send`, `NotRunning`). Readiness (§6) never
gates acceptance — a gated child's mailbox accepts during its handshake,
which is what lets cross-wired siblings send to a not-yet-ready peer. (An
accepted-during-`init` message dropped by a startup failure is exactly
§1 principle 6's at-most-once window, invariant §13.3.)

**Ordering.** Within one incarnation, a queue mailbox preserves per-sender
FIFO: two sends from the same task, both accepted, are delivered in send
order. There is no ordering guarantee across senders (acceptance order is
the only order), none across incarnations (at-most-once already forbids
it), and none between mailbox messages and timer or offload deliveries
beyond §5.2's loop priority. Conflating mailboxes order by replacement:
the survivor is the newest accepted value.

**Destruction venue.** Live `latest()` displacement drops the displaced
payload inline on the displacing task, after acceptance of its replacement
is visible. This is the deliberate hot-path exception: a panicking foreign
payload destructor surfaces on that task even though the replacement remains
accepted. Framework-initiated disposal of externally submitted mailbox or
reply-bearing payloads — including mailbox teardown, timeout/withdrawal
cleanup, and accepted-prefix batch disposal — runs detached from the
initiating task with per-element panic containment. Incarnation-owned
continuations, timer messages, and offload state instead follow §5.5 and §7's
incarnation teardown and verdict rules. No single disposal-thread identity is
promised.

Request/reply on a conflating mailbox is a correctness trap (a barrier can
be conflated away). A static fence is not possible — mailbox kind is
per-declaration configuration, invisible in `ActorRef<M>`'s type — so the
decided semantics are: `call` is allowed, conflation-away surfaces as
`ReplyDropped`, and the documentation teaches this next to the §3.3 retry
discipline.

### 5.2 The handler event loop

The generated loop services, in this documented priority with explicit
fairness:

1. shutdown request (checked first, biased — a stop request wins over any
   pending delivery);
2. actor-local continuations (`ctx.continue_with` runs as the *next*
   message), with one fairness exception: immediately after a continuation
   runs, one ready mailbox/offload delivery gets a turn before the next
   continuation, so a continuation chain cannot starve external input;
   dropped-continuation reporting on exit is preserved;
3. mailbox and offload deliveries;
4. keyed timers.

Tie order within a class is pinned, never select-arm luck (§14.2's
principle applied to the loop). Continuations form a FIFO queue:
multiple `continue_with` calls from one callback are all retained, in
call order — no last-wins replacement and no single-pending cap (the
queue is unbounded and consumes no mailbox capacity, B.1; discard
happens only at the stop freeze, reported on exit) — each running as a
"next message" ahead of queued mail, with the fairness interleave above
applying between successive continuations. Timers whose deadlines fire
at the same instant deliver in **arming order** — the order their
*current* armings were established; re-arming a key (§5.3's
replacement) takes the new position — and the bounded retraction turn
below runs once for the whole simultaneous batch: work captured in the
batch's bounded source prefixes delivers first, then the still-armed members
of the batch in arming order. Within class 3, ordering between mailbox and
offload deliveries stays deliberately unspecified (§5.1's ordering
contract promises per-sender FIFO and nothing more).

For this ordering rule, a timer **fires** when the event loop observes due
armings and begins taking their timer batch, not merely when the wall clock
passes the timer's deadline. Batch formation records a bounded prefix of each
input source — continuations, mailbox acceptances, and offload completions —
at that source's own cutoff. The sources do not share a global linearization
point: work arriving concurrently with batch formation may land on either side
of its source cutoff, and class 3 promises no ordering across sources.

Already-queued messages get one bounded turn to retract an elapsed timer:
when a timer fires, messages captured by the batch's mailbox prefix are
delivered first (they may `clear_timer` the fired key), then the timer message
goes through if still armed.

Timers rank last deliberately; the asymmetry is the rationale. A timer's
lateness under this order is bounded: once a timer fires, only each source's
captured prefix — queued messages (capped by mailbox capacity), offload
completions through the recorded offload watermark, and continuations through
the recorded queue length — runs before it. An offload still in flight holds
nothing back; work arriving after its source cutoff, continuations included,
does not preempt the fired timer. The continuation
clause is load-bearing — continuations are self-replenishing, so without it
a continuation chain could starve a fired timer forever despite the
per-continuation fairness turn above. The reverse order is unbounded: timers are
self-armed and recurring, so ranking them above externally-bounded input
would let a short interval on a slow handler starve the mailbox
indefinitely (the same self-generated-work hazard the continuation
carve-out above guards against; the loop's principle is externally-bounded
sources above self-generated recurring ones, with bounded-turn exceptions
protecting each side). Deliveries-first also resolves the timeout/response
race benignly — the accepted cancelling message retracts the timer rather
than a spurious timeout firing past it, which is what makes §5.3's
retract-until-delivery guarantee worth having — and `set_interval`'s
skip-missed-ticks posture already accepts bounded lateness as the timer
contract.

On stop (supervisor shutdown, removal, readiness-timeout teardown, or
local `ctx.stop()`), close external intake to freeze the accepted prefix; then
follow the mailbox shutdown policy (§10; handlers observe draining state).
For `Drain`, the actor loop delivers the frozen prefix before `on_stop`. For
`Discard`, freezing makes the prefix permanently undeliverable and the actor
loop returns without draining, then runs `on_stop`; the framework extracts and
schedules the prefix for §5.1's detached, per-element disposal after the actor
run returns. Physical destruction is not ordered before `on_stop` or exit
publication. A payload-destructor panic there is a disposal fault: it MUST NOT
reclassify the actor's exit or skip `on_stop`.
The stop boundary is exact about what drains: **only the frozen accepted
mailbox prefix** — under `queue`, every accepted-but-undelivered message
at the freeze, in acceptance order; under `latest()`, the surviving slot
(at most one message: a message conflated away before the freeze was
*replaced* per §5.1's replacement ordering, and is never handled). At the
freeze, outstanding offloads are cancelled
(incarnation-owned work of a stopping incarnation — continuations
suppressed per §5.5), queued continuations are discarded and reported on
exit (priority item 2 above), and armed timers are dropped (§5.3) — so
for a cooperative `Drain` that runs to completion, "the handled log
equals the accepted prefix" (§13.15) has no asterisks beyond the one
conflation already states: under `queue` the two are identical; under
`latest()` the handled log is the accepted sequence with each conflated
message replaced by its successor. The qualifier is load-bearing: a
handler that fails mid-drain (`Err` or panic — the incarnation is
failed, drain stops there) or a grace-expiry hard abort (§10) truncates
the drain, leaving the handled log a proper prefix of the accepted one.
`on_stop` runs exactly when the stop phase is reached with a live,
non-failed actor: on every cooperative stop path above, whether or not a
drain preceded it. It does not run when `init` failed (no actor value
exists), when a handler — live or draining — returned `Err` or panicked
(the incarnation is failed; cleanup is the crash-only path, `Drop`), or
on hard abort (the future is destroyed). Grace bounds drain plus
`on_stop` together (§10) — including after a local `ctx.stop()`, which
arms the child's own configured ladder (§10).

### 5.3 One timer facility

There is exactly one self-timer mechanism, an incarnation-owned keyed timer
table on `RawContext`, available to raw and handler actors alike, merged into
the single event source with the priority policy above.

- **Keys are values** (owned, `Hash + Eq` — not `&'static str`): dynamic
  keys are first-class, so per-entity deadlines need no application-side
  nearest-deadline sweep.
- **The key domain is public contract, pinned here** (not implementation
  latitude): the timer operations are generic over the key —
  `set_timeout<K>(key: K, msg, after: Duration)`,
  `set_interval<K>(key: K, msg, period: Duration)`,
  `clear_timer<K>(key: &K) -> bool` (returning whether an armed entry
  was retracted) — with `K: Hash + Eq + Send + 'static`, and one
  incarnation's table is **heterogeneous**: slot identity is (key type,
  key value), so keys of distinct types never collide or replace one
  another. Heterogeneity is load-bearing, not convenience — decorators
  share the inner actor's table (§4.3), and per-layer key types are what
  keep one layer's entries out of another's reach by construction.
- Setting a key replaces exactly; clearing retracts exactly (up until
  delivery). Timer deliveries never transit the mailbox: no capacity, no
  conflation; they count as received-but-not-accepted in stats.
- `set_timeout` with a zero duration arms an already-elapsed timer — not
  an error, never a synchronous delivery: it goes through the ordinary
  timer path, §5.2's priority and bounded retraction turn unchanged, so
  "deliver this as a timer, now" is expressible while `continue_with`
  remains the run-next facility. (Appendix B's zero-budget rule governs
  failure deadlines; a timer is scheduled work, not a failure deadline.
  Contrast `set_interval` below, where a zero *period* clears — a zero
  interval is not a degenerate deadline but an infinite immediate loop.)
- The table is incarnation-owned: restart or stop drops every entry, and an
  elapsed timer is not delivered once stopping begins.
- `set_interval` first fires one full period after arming, requires the
  message type `Clone`, skips missed ticks (no burst catch-up), and a zero
  period clears the key instead of arming.

Cross-actor delayed delivery (`send_after_to` / `interval_to`) is Part II
(§23); it is a separate, mailbox-semantics facility and will be the only
spawned-task timer path.

### 5.4 Stage-typed contexts

`Context` (live), draining, and `StopContext` form a narrowing series in
which **unavailable operations are unrepresentable, not silent no-ops**:
during drain, work-deferring operations (`continue_with`, self-timers,
offloads) are either absent from the stage's type or return a `Rejected`
result — they MUST NOT silently succeed-and-drop. Value-level `Rejected` is
the decided semantics, and the dividing rule is: **context types track
callbacks, not stages**. `StopContext` is a distinct type because `on_stop`
is a distinct callback — narrowing its parameter type taxes nobody — while
drain delivers to the *same* `handle` as live processing (§5.2), so a typed
drain stage would force `handle` to become stage-generic or split into a
second method, taxing every actor implementation to type-check a rare
stage. Within one callback, stage narrowing is value-level; a stage earns a
context type only by arriving with its own callback. Revisit only if
value-level checking demonstrably causes a shipped bug. `Rejected`
outcomes are value-carrying: the rejection returns the owned payload (the
continuation or timer message, the not-yet-started offload work) to the
caller rather than dropping it — recovery mirrors B.3's send errors; exact
types are per-operation, the payload-return property is normative. The
full per-stage capability matrix is Appendix B.1.

**`for_actor` — same-`Msg` re-entry — is core, and its contract lives
here.** Signature shape:
`Context<'_, A>::for_actor<B: Actor<Msg = A::Msg>>(&mut self) ->
Context<'_, B>` — the same operation on the drain series (yielding the
drain-stage context), and on `StopContext` yielding `StopContext<'_, B>`,
the only re-entry `on_stop` has (B.1); absent from `RawContext`. It is a
zero-cost reborrow of the same underlying context — no boxing, no
mapping layer (Part II §17's `project` is the paying, boxed cousin; the
`Msg`-equality bound is what makes the free identity possible). The
returned context therefore *is* the outer actor's: same incarnation and
identity (`id()`, `incarnation()`, `myself()`), same mailbox and shared
resources, same incarnation-owned timer table (§5.3's heterogeneous keys
keep an inner actor's keys collision-free by type), and every operation
through it — sends, timers, offloads, `stop()` — attributes to the one
outer actor (one mailbox, one identity, one lifecycle: §17's attribution
rule, already in force for the identity case). Stage narrowing carries
through unchanged: a drain-stage caller yields a drain-stage inner
context with the same value-level `Rejected` semantics, and a
`StopContext` caller yields the same withheld surface — re-entry never
widens a stage. The `&mut` reborrow with the `'_` lifetime makes nesting
safe and escape unrepresentable: the inner context cannot outlive the
callback or be smuggled out of it, and decorators compose by stacking
`for_actor` calls at zero cost — the mechanism §4.3's wrapper decorators
are built on and M3's spike exercises.

### 5.5 Offloads and blocking work

`offload(future, continuation, deadline)` runs incarnation-owned async work
whose completion re-enters the actor loop; `run_blocking` likewise for
blocking work. These are core — they are the escape hatch that keeps
handlers non-blocking. Contracts:

- Offloads take **one deadline budget**. The continuation is *total*: it
  receives `Result<T, DeadlineElapsed>` and must produce a message either
  way, so the framework's timeout verdict is structurally distinct from the
  inner operation's error (`T` may itself be a `Result`) — no hand-ordered
  inner/outer deadline pairs [#352]. There is no `Cancelled` arm:
  cancellation (guard drop, incarnation end) suppresses the continuation
  entirely. The budget's clock starts when the actor loop registers the
  offload — Appendix B's first-poll rule cannot apply, since there is no
  returned future to poll — and a zero budget short-circuits: the work
  future is never polled, and the continuation receives
  `Err(DeadlineElapsed)` through the ordinary loop delivery path.
- `offload` and `offload_scoped` share these semantics and differ only in
  what the caller holds: `offload` returns no handle — the work is
  anchored by the incarnation alone; `offload_scoped` additionally
  returns B.7's owned `Guard` (drop = cancel), scoping the work to a
  caller-held value as well. `detach()` releases only the guard's
  cancel-on-drop; detached or not, the work stays incarnation-owned and
  is cancelled at the intake freeze or incarnation end like any other
  offload — a guard never extends work past the incarnation. Bounds,
  stated once: the offloaded future is `Future<Output = T> + Send +
  'static` with `T: Send + 'static`; the continuation is
  `FnOnce(Result<T, DeadlineElapsed>) -> Msg + Send + 'static` and runs
  on the actor task.
  `Guard::is_finished` / `finished()` report either ordinary work
  completion or an incarnation-teardown cancellation request. They are not
  a join guarantee: a hard-aborted task can still be unwinding after the
  notification.
- Any higher-level helper that composes `call` inside `offload` MUST
  preserve: incarnation ownership; completion through the actor loop; a
  total timeout continuation; and no await inside the handler.
- Offload completions do not consume mailbox capacity and do not participate
  in conflation; their ordering relative to external messages is
  unspecified. Panics in the offloaded future or the continuation resume on
  the actor task, so supervision classifies them as an ordinary actor panic.
- `run_blocking(f)` hands the closure a cancellation token that is a child
  of the actor's shutdown token and is also cancelled if the returned future
  is dropped. Cancellation is cooperative only; after hard abort the
  blocking thread runs detached (documented contract). The returned future
  is `Send + 'static` so it can itself be offloaded. It resolves to the
  closure's return value; a panic in the closure is captured and resumes
  where the future is awaited — on the actor task, the ordinary
  actor-panic path (§7) — while a future dropped before completion
  discards a later panic along with the detached thread (documented).
- On orderly return, error, or caught-panic teardown, offloads, lifetime
  tasks, and monitor leases are frozen, cancelled, and joined before actor
  state is dropped. Hard abort necessarily drops the handler future (and
  therefore handler-owned actor state) before `RawResources::drop` can
  synchronously cancel the remaining async resources; it cannot join from
  `Drop`. `run_blocking` is outside the resource ledger by design and its
  thread detaches after a cancellation request; `StopContext::run_blocking`
  may therefore start work after the async-resource freeze. The mailbox
  binding outlives actor destruction on every path.

## 6. Readiness [#363]

Readiness is **declared data, read before the child's future is first
polled** — never inferred from poll order.

### 6.1 Reducer invariants and transitions

These rules are normative over `SupervisorState`, `Event`, and `Effect`.
Bracketed names are the checked evidence; `integration::…` names the public
integration suite and `supervisor::…` names the pure reducer suite.

1. **R1 — initial-only aggregate.** Only an `Initial { ready }` membership
   gates scope startup. A runtime admission never enters the aggregate.
   (`integration::runtime_dynamic_additions_never_join_aggregate_readiness`.)
2. **R2 — readiness is incarnation-local and monotone until restart.** An
   accepted `Ready` transition changes an initial member's bit `false → true`;
   duplicate readiness is a no-op. An accepted `RestartPending` transition
   resets that bit only while startup is incomplete. Once the aggregate has
   fired it never rewinds. (`integration::earliest_mark_ready_wins_and_later_readiness_edges_are_no_ops`,
   `integration::restart_before_aggregate_readiness_rearms_the_gate`,
   `integration::aggregate_readiness_stays_monotonic_after_a_ready_child_restarts`.)
3. **R3 — removal is sampled at the publication transition.** `Event::Ready`
   carries the synchronous removal-latch sample. A true sample first marks
   the membership `Removing`; readiness is then rejected. A committed removal
   shrinks the initial set only at `Reclaim`, after `Removed` publication and
   before the removal response resolves. (`supervisor::sampled_removal_suppresses_start_effects_until_commit`,
   `integration::queued_removal_suppresses_replayed_self_stop_readiness`,
   `integration::startup_removal_response_follows_aggregate_recomputation`.)
4. **R4 — ordered start is one accepted edge at a time.** `Settle` emits
   `StartChild` only for the current initial cursor, and advances the cursor
   only past a spawned-and-ready member or a reclaimed key. Dynamic startup
   may emit one accepted start per unspawned initial member. (`supervisor::start_effects_are_confined_to_the_spawn_transition`,
   `integration::ordered_startup_advances_past_a_reclaimed_cursor`.)
5. **R5 — settlement effects must be acknowledgeable.** Every
   `StartChild { child }` names a resident `Unstarted | RestartPending` child,
   exactly the acceptance set of `Event::Spawned`; a settlement pass that
   emits no acknowledgeable work is already at a fixed point. This prevents
   the driver’s level-triggered settle loop from spinning. (`supervisor::start_effects_are_confined_to_the_spawn_transition`,
   `supervisor::exhaustive_small_scope_interleavings_preserve_reducer_invariants`.)
6. **R6 — aggregate completion is derived.** `Settle` emits
   `StartupCompleted` at most once, iff no resident initial record remains
   unready. The empty initial set satisfies the predicate. (`integration::dynamic_startup_completes_after_removing_sole_unready_initial_member`,
   `integration::dynamic_startup_completes_after_removing_every_initial_member`.)
7. **R7 — terminal pre-ready failure is flavor-independent, rollback is
   position-dependent.** The exit funnel applies restart policy first. A
   terminal pre-ready failure emits `FailStartup`; an ordered suffix becomes
   `NeverStarted`, while a dynamic scope has no unstarted suffix. Root failure
   parks in `StartupFailed`; nested failure begins ordinary rollback.
   (`integration::ordered_terminal_pre_ready_exit_parks_the_root_and_marks_suffix_never_started`,
   `integration::dynamic_startup_failure_keeps_other_initial_members_supervised`,
   `integration::nested_dynamic_startup_failure_rolls_back_and_preserves_inner_cause`.)

No rule above is flagged unverified: each has direct checked evidence. The
remaining prose in this section specifies declaration defaults, user-facing
classification, and observation shape derived from these transitions.

**Normative prose coverage.** Declaration/mode resolution and eager invalid
mode rejection are checked by `integration::task_families_reject_after_init_readiness_eagerly`,
`integration::raw_readiness_override_rejects_after_init_eagerly`, and
`integration::subtree_readiness_deadline_defaults_inherit_or_reset_end_to_end`.
Immediate-before-first-poll and ready-at-deadline behavior are checked by
`integration::immediate_raw_construction_panic_classifies_post_ready` and
`integration::ready_at_deadline_wins_and_shutdown_disarms_the_gate`.
Ordered/dynamic failure and aggregate prose maps to R1–R7. Public `mark_ready`
reachability and decorator propagation are compile/runtime conformance in
`api_trait_conformance` and `integration::raw_manual_readiness_gates_ordered_startup_but_not_mailbox_acceptance`.
No normative readiness paragraph remains unmapped.

```rust
enum Readiness { Immediate, AfterInit, Manual }   // mode only — the deadline is a child option (§8)
```

- `Actor` (blanket) children default to `AfterInit`: ready when `init`
  returns `Ok`.
- Raw actor types declare via `RawActor::readiness()`; decorators propagate
  with a visible `R::readiness()`. The trait default is `Immediate`, so
  a decorator that omits propagation reports immediate readiness — an
  ordinary, testable bug (the sibling starts unblocked and ordered-startup
  tests see it), not the origin's silent mid-`init` gate release; §13.6's
  visibility clause is scoped accordingly to the decorator that *does*
  propagate but awaits before delegating. There is no first-poll race, no
  "must be the first operation" cross-module ordering, and no mode that only
  the framework's own loop can reach. (Origin defect being prevented: the
  three-valued readiness domain was flattened to `Option<Duration>` and the
  third state reconstructed by racing the first poll — the blanket impl had
  to call a crate-private `defer_automatic_readiness()` synchronously before
  its first await, enforced only by comments on both sides of a module
  boundary. A decorator that awaited anything before delegating silently
  released the ordered-startup gate before the inner actor's init ran.)
- Per-declaration override lives on the child options (§8) uniformly for
  actor and task children. A raw actor type supplies the fallback mode, but
  each child definition resolves its own effective mode [#368] (e.g. "not
  ready until an external handshake completes" must be declarable for one
  ordinary handler child without changing the handler type). Subtree children are the stated
  exception: their readiness is structural (below), so the subtree veneer
  carries no mode override — only the deadline.
- The deadline is not part of the mode: it lives on the shared options
  record (§8) as `ReadinessDeadline: Inherit | Bounded(Duration) |
  Unbounded`, defaulting to `Inherit` — resolution is declaration → scope
  default → library default (Appendix A). Unbounded gating exists only via
  the explicit `Unbounded` value; no `None` does double duty as both
  "inherit" and "unbounded".
- The engine enforces gating and deadline in exactly one place, and sees
  exactly **two** states: immediate, or gated with a resolved deadline.
  `AfterInit` is declaration-level sugar — the blanket loop reports
  readiness through the same public `mark_ready` mechanism after `init`
  returns `Ok`; there is no third engine state and no framework-private
  readiness path (§1 principles 2 and 5). Deadline expiry is a distinct
  startup-failure exit cause (§7) — one type, for tasks and actors alike,
  produced by one engine-side timeout (not re-implemented per child kind
  and reunited by downcast).
- An effective `Immediate` mode publishes readiness **at spawn**: the
  engine marks the child ready when it launches the incarnation, before
  the child's future is first polled — for a raw child, before its
  factory has constructed the actor. Uniformly for tasks and raw actors,
  a failure inside an effectively-immediate child — including a raw
  construction panic — is therefore a **post-ready** exit for startup
  classification (§7): an ordered sequence has already advanced past the
  child, later siblings still start, and `wait_started()` does not
  report a startup abort for it. A definition that wants construction
  observed pre-ready declares a gated mode instead.
- Ordered scopes start children sequentially; a gated child blocks the
  sequence until ready. A pre-ready exit follows the child's restart policy
  like any other exit (§7): while restarts remain eligible the sequence
  stays blocked, and each new incarnation re-arms the gate and its deadline
  (the readiness deadline is per-incarnation — invariant §13.6's "restart
  re-runs the gate"; its clock starts at the incarnation's start instant,
  §9.2's shared stamp). Startup aborts only on a **terminal** pre-ready
  exit
  (`Never`/one-shot, or an intensity trip). Later ordered siblings then
  never start, and their memberships terminalize with the `NeverStarted`
  exit (§7) — the aborted sequence has no resume, and terminal handles
  beat sends parked forever. What happens to the started prefix splits by
  position (§11): at the **root**, it stays running — rollback is the
  owner's decision (`start_or_shutdown()` packages the safe default); in
  a **nested** scope, the scope rolls itself back and exits as an
  ordinary child failure carrying the structured startup-failure payload
  (§11's nested rule). Readiness reported exactly at the deadline wins
  over the timeout. A readiness signal fired before an exit or clean
  self-stop is observed MUST count for startup accounting even when event
  arbitration processes the terminal edge first. In particular,
  ready-then-failed is a post-ready failure: restart policy applies and
  startup advances exactly as it would if the readiness event had been
  delivered first. Nested scopes report ready recursively once their initial
  children are up. `spawn()` stays synchronous; `wait_started()` is the
  readiness barrier.
- **Dynamic scopes start their initial members concurrently**, and their
  pre-ready failure rules are the concurrent restatement of the ordered
  ones, not a separate regime. A non-terminal pre-ready exit restarts
  per policy and holds the aggregate open (the latch rule below),
  exactly as an ordered gate does. A **terminal** pre-ready exit of an
  initial member before the aggregate fires is the scope's terminal
  startup failure — *unless* that exit is the commit of an
  owner-initiated removal, in which case the member simply leaves
  the declared set under the aggregate rule below and startup
  continues — and because every initial member was spawned at
  lowering, there is no not-yet-started suffix: **no sibling
  terminalizes `NeverStarted`**. The transition is pinned: the scope
  leaves `Starting` the moment the exit funnel dispatches that terminal
  pre-ready exit (when one wake makes several eligible, §14.2's
  arbitration order picks; the startup-failure payload names exactly
  the exit that triggered the transition). By position, mirroring the
  ordered split: at the **root**, the scope parks in `StartupFailed`
  (§11) with *every other member* — running or still starting —
  continuing under supervision; rollback is the owner's decision
  (`start_or_shutdown()` packages it). In a **nested** scope, rollback
  is automatic and *concurrent* — the group is cancelled at once and
  drains in parallel (§10's dynamic teardown), runtime-added members
  included — after which the scope publishes
  `Stopped { reason: StartupFailed }` and exits at its parent as the
  ordinary structured child failure (§11's nested rule, unchanged).
  Failures landing during that rollback are recorded under `Draining`
  mode and schedule nothing (§10); they never change the reason
  payload.
- **Aggregate readiness is initial-members-only and monotonic.** A
  scope's structural readiness aggregates exactly the members it was
  lowered with — its declared initial set; runtime additions to a
  dynamic scope never join the aggregate, whether they are admitted
  before or after it fires (their per-child `Ready` events, B.4, are
  the observation surface). Removing an initial member while the scope
  is `Starting` shrinks that declared set, and the aggregate may
  complete when the remaining initial members are ready — including
  the empty case, where removing every initial member completes
  startup. The shrink is **commit-time, not request-time**: the member
  leaves the declared set when its removal commits (residency
  withdrawn, `Removed` published), so a slow-stopping member holds the
  aggregate open for its whole stop ladder, and the recomputation is
  ordered ahead of the removal response — a returned `Removed` implies
  the aggregate has already seen the shrunken set. That commit point
  pins the race against the member's own pre-ready failure: a terminal
  pre-ready exit or readiness timeout whose terminal routing observes
  the membership *not yet* marked removing fails startup under the
  rule above, while once the mark is in place removal outranks it and
  the aggregate simply shrinks. Both orders are legal; which one a
  given run takes is not specified. The mark suppresses the member's
  own **readiness edge** on the same terms: a membership observed
  removing where a readiness edge would be published emits no `Ready`
  (B.4) and is credited to no aggregate, even when its readiness latch
  fired before the mark — and this binds every membership, not only
  initial ones, so a runtime-added member being removed publishes no
  per-child `Ready` either. Which side of *that* race a run takes is
  likewise unspecified; what is pinned is that the mark is consulted
  where the edge would be published, never by arbitration position
  (§14.2). The aggregate fires at most once
  per scope incarnation and is latched: an already-ready child that
  fails and restarts afterwards does not rewind it — readiness is a
  startup-phase edge, not a liveness signal (snapshots carry liveness,
  B.6). The same latch decides the pre-fire race: a gated child that
  restarts *before* the aggregate has fired holds it open until the
  fresh incarnation's re-armed gate releases (the re-arm rule above);
  once fired, later churn is invisible to it. Per-child readiness is
  likewise once per incarnation — the `mark_ready` token — re-armed by
  each restart.
- Kind defaults and valid modes: blanket `Actor` children default
  `AfterInit` with all three modes valid; raw actors and tasks default
  `Immediate` and may declare `Manual` (`AfterInit` is meaningless without
  an `init` and is rejected eagerly, §9.3); subtree children have no mode
  knob — their readiness is structural (the recursive rule above), bounded
  by the resolved readiness deadline.
- Manual readiness is reported through a public context operation
  (`mark_ready`), one-shot per incarnation **by construction** (an owned
  token or equivalent — not a runtime take-once flag; §1 principle 3). It
  is reachable from the raw context, the task context (B.2), and the live
  handler `Context` (B.1) — a `Manual` handler actor completes its
  handshake in `init` or `handle` and marks ready right there [#368].
  Where the declared mode already decided readiness, the call is a
  documented no-op (B.2's rule, uniform across kinds); the readiness
  effect fires at most once, so an explicit early mark under `AfterInit`
  releases the gate and the blanket's own post-`init` mark becomes the
  no-op — earliest mark wins.

**Regression anchor (MUST have a test, §13.6):** a raw decorator that awaits
anything before delegating to an inner `AfterInit` actor still gates ordered
startup — the sibling MUST NOT start until the inner init returns.

## 7. Exits [#364]

One classification, produced at one point, used by every consumer.

### 7.1 Exit invariants and transition rules

1. **E1 — record, destroy, join, publish.** One owned report token records the
   run outcome; dropping it records the fallback. No consumer sees an exit
   until the incarnation is destroyed and joined, and exactly one report is
   then published. (`integration::raw_run_panic_with_panicking_destructor_publishes_one_report`,
   `integration::terminal_stop_paths_share_one_complete_observation_transition`.)
2. **E2 — total verdict precedence.** Classification orders `Panicked >
   ReadinessTimedOut > Failed > Aborted > Completed`; `NeverStarted` is a
   membership fact outside incarnation arbitration. A recorded `Failed`
   survives later abort, while a recorded `Completed` does not.
   (`exit::classification_precedence_is_table_driven`,
   `exit::forced_outcomes_do_not_erase_stronger_recorded_evidence`,
   `integration::one_shot_value_cannot_override_readiness_timeout_verdict`.)
3. **E3 — cancellation is orthogonal sampled state.** The exit records
   `Observed` iff the incarnation cancellation latch had fired when the
   outcome was recorded. Restart eligibility never inspects this field.
   (`engine::funnel_dispatch_depends_on_mode_and_membership_state`,
   `integration::locally_requested_subtree_shutdown_reads_cancelled`.)
4. **E4 — one authoritative membership/incarnation state.** Incarnation
   phases advance only through `Unstarted → Active → Stopping? → Complete →
   RestartPending | Disposing → Joined`; stale or out-of-order events are
   total no-ops. Removal changes the enclosing membership state monotonically
   to `Removing`, never a parallel flag. (`supervisor::transition_table_keeps_removal_in_the_authoritative_state`,
   `supervisor::stale_events_cannot_skip_or_regress_incarnation_phases`,
   `supervisor::exhaustive_small_scope_interleavings_preserve_reducer_invariants`.)
5. **E5 — restart suppression is state-derived.** Exits schedule restart only
   while the scope is running and the membership is resident. Draining or
   `Removing` records schedule nothing and charge no intensity.
   (`engine::funnel_dispatch_depends_on_mode_and_membership_state`,
   `integration::same_batch_removal_suppresses_pending_restart_shutdown`.)
6. **E6 — publication fences replacement.** A replacement spawn follows the
   predecessor’s terminal publication; stale incarnation evidence cannot
   affect the replacement. (`integration::restartable_raw_factory_runs_inside_the_incarnation_task`,
   §13.7’s stale-event race probes.)

No rule above is flagged unverified. User destructor double-panic behavior is
a documented Rust process precondition rather than a state transition and is
therefore not claimed as an in-process checked property.

**Normative prose coverage.** Typed framework/user channels and structured
provenance are checked by `exit::application_errors_cannot_forge_structured_provenance`
and `exit::framework_errors_expose_structured_provenance_and_erased_views`.
The complete variant/display/source inventory is checked by
`exit::child_startup_failure_display_summarizes_every_exit_kind`,
`exit::recorded_outcomes_are_canonical_exit_kinds`, and
`exit::startup_errors_chain_their_structured_detail`. Precedence and
cancellation map to E1–E5. Single-runner and publish-after-join prose maps to
E1/E6 and the raw/task integration tests named there. Structured shutdown
straggler paths are checked by
`integration::zero_timeout_reports_recursive_straggler_paths_and_joins_them`.
The in-process half of double-panic containment is checked by
`integration::raw_run_panic_with_panicking_destructor_publishes_one_report`;
the process-aborting half is explicitly **UNVERIFIED in process** because Rust
terminates the process by contract.

- The actor/task's own failure type and the framework's verdict are
  **separate channels**: user code returns its error; the runner constructs
  the exit. Classification is a pure function of the observed outcome
  (return value, join result, verdict, cancellation flag) — table-testable
  without a runtime (§1 implementation shape). Framework verdicts
  (readiness timeout, cancellation, abort) MUST NOT be boxed into the
  user-error channel and downcast back. That prohibition is scoped to
  verdicts about one incarnation's own termination — anything the
  classifier can state as a typed variant. Two scope-granularity outcomes
  deliberately enter the parent's view as `Failed` — a child scope's
  intensity trip (§9.2) and a nested scope's startup failure (§11) —
  because to the parent each *is* an ordinary child failure. They ride
  the user-error channel without weakening it, by **provenance**:
  `ExitError` internally distinguishes erased application errors (the
  only publicly constructible form — the `From`/string constructors
  below) from these library-constructed structured payloads. B.5's named
  accessors (`intensity_trip()`, `startup_failure()`) are matches on that
  private structure, not downcasts — no downcast exists anywhere on the
  exit path, so §13.8's lint carries zero exceptions — and the payloads
  are non-forgeable: an application error that imitates a trip is still
  an erased user error, for which the accessors return `None`. The
  framework's own classification consults only `is_failure()` either way.
- `ExitError` — the user-error channel's type — is a library-owned,
  type-erased error: constructed via `From` from any `E: Error + Send +
  Sync + 'static` (plus a string-message constructor), erased once into
  `Arc`-shared storage — exits ride `Clone` snapshots and events, so the
  payload must be cheaply cloneable and cross-thread; `Sync` is what makes
  the shared reference sound. `ExitError` itself does NOT implement
  `std::error::Error` (the `anyhow` precedent): implementing it would make
  the blanket `From<E>` overlap `core`'s reflexive `From<T> for T`. It
  carries `Display`, source-chain access, and a by-reference
  `&(dyn Error)` view instead. Applications
  MAY downcast it (e.g. routing on a domain error surfaced in a `Failed`
  exit); the framework NEVER does — framework verdicts have their own
  variants, and §13.8's lint enforces the asymmetry structurally.
- One public exit type covers: `Completed`, `Failed(error)`, `Panicked`
  (carrying the panic message when the payload downcasts to a string — the
  payload itself is never retained, since exits ride `Clone` snapshots and
  events), `ReadinessTimedOut { deadline }`, `Aborted { phase:
  GracePhase }`, and the membership-level `NeverStarted`, with
  cancellation ("supervisor asked it to stop" vs "finished on its own") as
  an orthogonal, explicit `Cancellation::{Observed, NotObserved}` value on
  every exit. `GracePhase::{WithinGrace, AfterGrace}` distinguishes whether
  cooperative grace expired before an abort; scope-level shutdown-timeout
  remains a separate verdict. `NeverStarted` is the terminal outcome of a
  membership that ends with no incarnation ever spawned — a declaring
  tree dropped unspawned, a rejected or withdrawn insertion (§3.2, §8),
  removal before first spawn, or a startup abort terminalizing
  never-started siblings (§6). It is a membership fact, not an
  incarnation verdict: it sits outside the precedence rule below, is
  never input to restart or intensity accounting (both consume
  incarnation exits), and counts as a failure for `is_failure()` —
  awaiting a child that never ran is not success.
- **Cancellation observation is one state-machine fact**, not a narrative
  judgment: the value is `Cancellation::Observed` iff the incarnation's own
  shutdown token (B.1/B.2) had fired before its outcome was recorded (the
  record phase below), and `Cancellation::NotObserved` otherwise.
  The token fires for every engine-initiated stop — scope teardown,
  dynamic removal, readiness-timeout teardown, `shutdown(0)`'s immediate
  escalation — and for local `ctx.stop()`, which arms the same ladder
  (§10), so a self-stopped actor's exit reads `Cancellation::Observed`. A stop
  racing natural completion needs no third rule: whichever of token-fire
  and outcome-record happened first decides the value. `NeverStarted`
  sits outside the rule — no incarnation, no token — and carries
  `Cancellation::NotObserved` uniformly, whether the membership ended by
  tree-drop, withdrawal, rejection, or startup abort: the variant itself
  already says nothing ran.
- **Verdict precedence.** When one incarnation's end admits several
  readings, classification picks the highest of: `Panicked` >
  `ReadinessTimedOut` > `Failed` > `Aborted` > `Completed`; cancellation
  observation is orthogonal and never competes. Concretely: a panic is
  never masked,
  wherever it lands (`run`, `on_stop` — superseding the run's outcome,
  §4.1 — or an incarnation-owned destructor, via the fallback report token;
  §5.1's detached message-disposal faults are outside the incarnation
  verdict); and a
  readiness-deadline expiry names the *cause* even when the teardown it
  triggers ends in a grace-expiry abort (the mechanism).
- **`Aborted` genuinely competes with a recorded outcome, and the rule is
  asymmetric.** `Aborted` describes a future destroyed before yielding an
  outcome, so it reads as if it could never conflict with
  `Failed`/`Completed`, which require one. It conflicts anyway, by race:
  the body can record its result and the destruction still land before
  the join retires, and §10's ladder can arm a supervisor-forced abort
  verdict against a membership that has already recorded one. Precedence
  resolves that race in one direction only, and the asymmetry is
  deliberate rather than an artifact of the ordering:
  - A recorded `Failed(error)` **survives** a later abort. Destruction
    proves only that teardown ended the task; it must not erase the
    structured application error the task already produced, which is the
    only evidence naming *why* the child was failing.
  - A recorded `Completed` does **not** survive: cancellation overrides
    it and the exit reads `Aborted { phase }` with
    `Cancellation::Observed`. A supervisor that destroyed a child before its
    success was observable did not get a successful child. Concretely, a
    one-shot task whose body returned `Ok(value)` inside that window
    exits `Aborted` and `OneShotTaskRef::wait` yields `Err(exit)` — the
    typed value is dropped rather than released past a stop the
    supervisor had already committed to.

  Only the recorded-vs-abort pair is asymmetric this way; the ordering
  above is otherwise a total precedence over the whole variant set.
- **Failure classification (feeds §9.2):** an exit is a *failure* iff it is
  not `Completed`. So `Failed`, `Panicked`, `ReadinessTimedOut`, and
  `Aborted` all restart under `OnFailure`; `Always` restarts even clean
  completions. The cancellation observation is **never** consulted by restart
  classification — restart suppression comes solely from teardown
  *state*, never from the exit's shape: a draining scope schedules no
  restarts (§10), and a membership whose removal is in progress
  (`membership_status: Removing`, B.6) has its restarts suppressed even
  while its containing scope keeps running — dynamic removal of one
  child must not depend on the whole scope draining (§11). The same
  mark suppresses that membership's readiness publication (§6): one
  level-triggered removal source, consulted at execution time by every
  site that would otherwise publish or schedule on the membership's
  behalf.
- There is exactly **one incarnation runner**, and it yields this one exit
  type — including `Panicked` — regardless of how it is hosted. The
  supervisor is a policy loop over the runner's output. (The public
  supervisor-free hosting surface is Part II §22; the single-runner
  property itself is core and internally tested, because it cannot be
  retrofitted: the origin ended with two runners with two failure models,
  one of which let panics unwind to the user.)
- Structured surfaces carry structured payloads: shutdown-timeout errors
  list the affected children as data — child-id *paths* from the scope
  whose timeout expired, each with its membership token (§3.2), since
  bare ids are ambiguous across sibling scopes that reuse them (§2) —
  not a formatted string; `Failed` carries
  the error value, not its `Display` projection; error string projections
  may exist only in display paths.
- Exactly one externally visible exit report per incarnation, produced in
  **two phases — record, then publish**. The run path *records* its
  outcome through an owned report-token (consumed to record; drop records
  the fallback — never a claimed boolean); the report is *published*
  exactly once, only after the incarnation future is destroyed and
  joined, by folding the recorded outcome with the join's own verdict (a
  destructor panic surfaces in the join result) under the precedence rule
  above. This is what lets a destructor panic supersede an outcome the
  run path already recorded while keeping exactly one visible report:
  recording is internal and provisional; nothing downstream — restart
  scheduling, events, snapshots, waiters — observes anything before the
  single post-join publication. The binding's post-exit disposition
  (rebindable vs terminal) is decided once at publication and carried as
  state, not re-derived by parallel drop guards. Sequencing rule: a
  replacement incarnation is spawned — and the mailbox rebound — only
  after the predecessor's publication, so no exit report can race its
  replacement. §13.7's race provocations remain necessary because
  *detached* work (an aborted
  `run_blocking` thread, in-flight observations) can still surface stale
  events after that join; those must fence to the old incarnation.
- **The double-panic containment boundary.** Post-join publication can
  fold a destructor panic into the report only if the process survives
  to join. A callback panic that unwinds *through* the actor value
  would run the actor's `Drop` during the unwind; if that `Drop` also
  panics, the process aborts before anything publishes. The runner
  therefore MUST catch a panic from user callback or construction-source
  execution (`init`, `handle`, `on_stop`, `run`, task bodies, factories)
  at the execution boundary, **before the actor value and its
  incarnation-owned state are destroyed** — the actor's destructor never
  runs inside a user callback's unwind. A callback panic followed by a
  destructor panic is then two separately caught panics folded under the
  precedence rule into the single published report; on that
  equal-precedence collision the recorded outcome wins — the report is
  `Panicked` carrying the *callback's* payload, the destructor's being
  observable only in the process staying alive (§13.7's provocation).
  Explicitly outside the boundary: a destructor panic occurring *inside
  the same unwind* — a local within the user's own future poisoning its
  `Drop` — is a genuine double panic and aborts the process; that is
  Rust's contract, documented alongside §11's `panic = "unwind"`
  precondition, not something the runner can contain.

## 8. Child specification and options [#368]

All child kinds share one uniform options record; per-kind spec types are
thin typed veneers over it, not hand-copied projections. The record ships
whole in core even where an individual option is Part II — the shared record
is the anti-drift structure, and extending it is additive. Its boundary is
§1's plain-data rule: every option is plain `Eq` data, so **code-bearing
extractors are structurally outside the record** — §16's key extractor and
§20's `message_size` measurer are typed spec extensions arriving with their
features, exactly as actor implementations are outside §21's outline, never
options.

Options: `restart` (absent by construction on one-shot forms), `shutdown`,
`readiness` mode override (absent by construction on subtree veneers —
subtree readiness is structural, §6) plus `readiness_deadline`
(`Inherit`/`Bounded`/`Unbounded`, §6, all kinds), terminal-membership retention (one name, one polarity,
one kind-independent default — *retain* on restartable children, *remove* on
one-shot children, stated once), and actor-only mailbox settings
(`mailbox`, `mailbox_shutdown`) present only on actor specs.
(`message_size` observation, Part II §20, is not an option: its measurer
is code, per the extractor boundary above.)

**Retention semantics (tombstones).** Terminality and pruning are two
distinct membership edges, and the retention option chooses only their
distance. A membership becomes *terminal* when its final exit publishes
(§7) — snapshot state `Stopped { exit }` / `StartupAborted { exit }`
(B.6). It is *pruned* when it stops being a resident of its scope: the
`Removed` event fires (B.4), the id is freed, and anything not yet
resolved terminal now does. Under *remove*-on-terminal the edges
coincide: pruning follows terminalization immediately (§11's finishing
test runs strictly between them). Under *retain*, the terminal
membership stays resident as a **tombstone**, with exactly these
properties:

- It still occupies its id: a same-id `reserve_*`/`add_*` fails
  `DuplicateId` until the tombstone is pruned — replacement under a
  reused id is thereby always an explicit remove-then-add, never a
  silent slide past a dead predecessor (§3.4's boundary depends on
  this).
- `child(id)`, `descendant(path)`, and snapshots return its terminal
  `ChildSnapshot` (`incarnation: None`); `Removed` has **not** yet
  fired.
- `remove` — by id or exact handle — prunes it: resolves `Removed`
  (B.8), fires the `Removed` event, frees the id. Scope teardown and
  scope terminalization prune all tombstones as part of ending the
  scope.
- It never blocks §11's natural completion (a retained terminal
  membership counts as terminal there) and never participates in
  restart or intensity accounting — retention is observability, not
  liveness.

API-shape rules:

- One add-method family, names pinned: `add_actor` / `add_task` /
  `add_subtree`, with one-shot twins `add_actor_once` / `add_task_once` /
  `add_subtree_once`
  (the suffix keeps the family grouped in docs and autocomplete), plus
  the raw-actor pair `add_raw` / `add_raw_once` (§4.3) — **eight entry
  points** — each
  taking `(id, definition)`: the id as `impl Into<ChildId>` (`ChildId`
  is a concrete library-owned type, so the string conversions are
  ordinary, coherent `From` impls) and the kind's definition value;
  there are no parallel `*_spec` twins. A spec is that (id, definition)
  pair, where the definition is the construction source (§4.2's bounds,
  pinned in the slot surface below) plus options: `reserve_*` consumes
  the id, a slot's define consumes the id-less definition, and the
  fused `add_*` takes both and splits them — no surface carries the id
  twice.
- **Definitions are built by nominal constructors, never accepted
  through blanket conversions.** The obvious convenience — `define`
  taking `impl Into<ActorDef<A>>` with conversions from a bare
  `A::Args` and from a `Fn() -> A::Args` closure — is not implementable
  in coherent Rust: a conversion from an associated-type projection and
  a conversion from a closure bound are blanket impls that collide with
  `core`'s reflexive `From<T> for T` and can overlap each other (an
  args type can itself be a nullary closure). A Rust 1.97.1 bootstrap
  compiler experiment confirmed that the complete nominal-constructor family
  compiles and infers at representative call sites while the rejected blanket
  surface fails with E0119. No standalone probe is retained; the real
  constructors and their downstream uses are compiler-checked as they land.
  The constructors are the public surface, one per §4.2 mode (signatures
  pinned in the slot block below): `ActorDef::cloned(args)` /
  `ActorDef::factory(f)`,
  `ActorOnceDef::new(args)`, `RawDef::factory(f)` /
  `RawOnceDef::new(actor)`, `TaskDef::new(factory)` /
  `TaskOnceDef::new(body)`, `SubtreeDef::factory(f)` /
  `SubtreeOnceDef::new(tree)`. Each yields the definition carrying
  default options; options attach through consuming setters on the
  definition — one setter per field of §8's record (`.restart(..)`,
  `.shutdown(..)`, `.readiness(..)`, `.readiness_deadline(..)`,
  `.retention(..)`; actor defs additionally `.mailbox(..)` /
  `.mailbox_shutdown(..)`), **minus the setters the record marks absent
  by construction** — no `.restart(..)` on `_once` defs, no
  `.readiness(..)` on subtree defs — and subtree defs additionally
  carry `.defaults(Inherit | Reset)`, the §9.3 edge knob, which is a
  property of the subtree edge rather than a field of the shared
  record (the subtree def *is* §8's "subtree veneer") — so a
  definition with options is still one
  expression at the call site, no build/extract/add dance (cells §3.2
  make this natural).
- The spec surface for one kind names operations identically across kinds —
  the one-shot operation is the `_once` twin, never a differently-named
  `spawn_once`.
- Adding a new child kind or mode extends the shared record, not a
  hand-maintained matrix.

**Slots — the reserve-before-define surface.** §3.2's cell machinery has
one public face, uniform across the three kinds and both scope flavors.
The shape is content-normative (this appendix-style latitude on exact
names applies, per Appendix B's preamble):

```rust
// On ordered tree builders and on DynamicScopeRef alike — reservation is
// synchronous on both flavors, and is where the id errors reject (rules
// below). Receivers differ by flavor: `&mut self` on builders (plain
// owned values, §11 — exclusive access is free), `&self` on
// `DynamicScopeRef` (a cheap-`Clone` shared handle, B.10 — `&mut`
// would be bypassable by cloning and is not pretended); `add_*`
// follows the same split:
fn reserve_actor<M: Send + 'static>(&mut self, id: …) -> Result<ActorSlot<M>, ReserveError>;
fn reserve_task(&mut self, id: …) -> Result<TaskSlot, ReserveError>;
fn reserve_subtree<T: Subtree>(&mut self, id: …) -> Result<SubtreeSlot<T>, ReserveError>;

// Slots are owned and non-`Clone`. Handles are available immediately —
// before define, before spawn (§3.2):
impl<M> ActorSlot<M>            { fn actor_ref(&self) -> ActorRef<M>; }
impl TaskSlot                   { fn task_ref(&self)  -> TaskRef; }
impl<T: Subtree> SubtreeSlot<T> { fn scope_ref(&self) -> T::Ref; }

// Definitions (`*Def`) — a spec minus its id — are built by nominal
// constructors (the §8 constructor rule: blanket `Into` conversions from
// bare sources are rejected for coherence). Every construction-source
// bound is pinned here, once. Constructors yield default options;
// options attach via the consuming setters listed in the §8 rule:
impl<A: Actor> ActorDef<A> {
    fn cloned(args: A::Args) -> Self where A::Args: Clone + Sync;
    fn factory(f: impl Fn() -> A::Args + Send + Sync + 'static) -> Self;
}
impl<A: Actor> ActorOnceDef<A> {
    fn new(args: A::Args) -> Self;               // owned, consumed
}
impl<R: RawActor> RawDef<R> {
    fn factory(f: impl Fn() -> R + Send + Sync + 'static) -> Self;
        // the actor value is the per-incarnation input (§4.3)
}
impl<R: RawActor> RawOnceDef<R> {
    fn new(actor: R) -> Self;                    // owned `R`, consumed
}
impl TaskDef {
    fn new<F>(factory: impl Fn(TaskContext) -> F + Send + Sync + 'static) -> Self
        where F: Future<Output = ExitResult> + Send + 'static;
        // §4.1's exact bound — re-invoked with a fresh `TaskContext`
        // (by value, B.2) for each incarnation
}
impl<T: Send + 'static> TaskOnceDef<T> {
    fn new<F>(body: impl FnOnce(TaskContext) -> F + Send + 'static) -> Self
        where F: Future<Output = Result<T, ExitError>> + Send + 'static;
}
impl<T: Subtree> SubtreeDef<T> {
    fn factory(f: impl Fn() -> T + Send + Sync + 'static) -> Self;
        // re-lowered per incarnation; a failed lowering is the
        // incarnation's startup failure (§11's lowering rule)
}
impl<T: Subtree> SubtreeOnceDef<T> {
    fn new(tree: T) -> Self;                     // owned tree, consumed
}

// Definition consumes the slot; the restartable/one-shot split is §4.2's.
// (Builder flavor shown: defines complete synchronously. On a dynamic
// scope the same operations return the admission future instead — rules
// below.)
impl<M> ActorSlot<M> {
    fn define<A>(self, def: ActorDef<A>) -> ActorRef<M>
        where A: Actor<Msg = M>;
    fn define_once<A>(self, def: ActorOnceDef<A>) -> ActorRef<M>
        where A: Actor<Msg = M>;
    fn define_raw<R>(self, def: RawDef<R>) -> ActorRef<M>
        where R: RawActor<Msg = M>;
    fn define_once_raw<R>(self, def: RawOnceDef<R>) -> ActorRef<M>
        where R: RawActor<Msg = M>;
}
impl TaskSlot {
    fn define(self, def: TaskDef) -> TaskRef;
    fn define_once<T: Send + 'static>(self, def: TaskOnceDef<T>)
        -> (TaskRef, OneShotTaskRef<T>);
}
impl<T: Subtree> SubtreeSlot<T> {
    fn define(self, def: SubtreeDef<T>) -> T::Ref;
    fn define_once(self, def: SubtreeOnceDef<T>) -> T::Ref;
}
```

Rules (normative):

- **Reservation claims the id and mints the cell.** `reserve_*` is
  synchronous on both flavors and returns `Result`: it is §9.3's eager
  point for the id errors — `EmptyId` and `DuplicateId` on either flavor
  (a retained tombstone counts as a duplicate — retention semantics
  above); on dynamic scopes additionally `RemovalInProgress` (same id
  mid-removal, §11), `NoRuntime` (runtime boundary below), and
  `NotAdmitting` (stage rule below). From success the slot's handles
  resolve through the cell like any other handle; the slot is
  parameterized by exactly what a handle needs (`M` for the wire type,
  `T` for the subtree's ref dispatch) so refs exist before any actor type
  or factory is named.
- **`add_*` is sugar**: each of the eight add entry points is exactly
  `reserve_*` followed by the matching define and returns what the pair
  returns. One declaration path, not two — cyclic wiring uses the
  split form, everything else the fused one. Fused forms surface the
  union of the two steps' errors (B.8).
- **Return values are pinned per kind.** On a declaration builder, define
  (and therefore `add_*`) yields the child's handles synchronously:
  `ActorRef<M>` for the four actor forms; `TaskRef` for tasks, joined by
  the owned `OneShotTaskRef<T>` on the one-shot form; `T::Ref` for
  subtrees. On a dynamic scope the admission future returns that same
  per-kind handle set directly (B.8). Every returned set contains a
  membership-addressed component: `ActorRef`, `TaskRef`, or `T::Ref` exposes
  the membership token through `membership()` (§3.2). The slot's pre-admission
  handles and the returned handles resolve through the same cell — one identity,
  no reconciling.
- **Definition is consuming**, so double-definition is unrepresentable
  (§4.2's owned-`FnOnce` shape at the declaration layer). On a
  declaration builder, `define` completes the declaration synchronously
  and cannot fail — spec-level validation already happened eagerly at
  spec construction (§9.3). On a dynamic scope, `define` *is* the
  admission call: a future resolving at admission to the per-kind handles
  (B.8).
  Its operation errors are `NotAdmitting` and the first-poll
  `NoRuntime` rejection below: the id errors were already spent at
  reserve, and definition validation was spent at spec construction
  (§9.3's eager rule), exactly as on the builder flavor — admission
  validates no definition data. Whether the builder and dynamic flavors
  are one generic slot type or two parallel families is implementation
  latitude; the operation inventory and this error split are not.
- **Fused admission futures abort on drop; split ones detach.** Dropping
  an in-flight dynamic `add_*` future withdraws the insertion: if
  admission has not yet happened, the reservation is released and the
  cell terminalized (§3.2); if admission won the race, the child is
  removed exactly as by exact-handle `remove` (§11). Either way the
  scope never retains a child whose identity the caller failed to
  receive — the unknown-outcome window that §3.2 closes for completed
  calls, cancellation-as-abort closes for abandoned ones. That rationale
  is exactly why the split form behaves differently: the slot's handles,
  taken before `define`, already *are* the caller's identity, so a
  dropped dynamic `define` future **detaches** — the insertion proceeds
  to admission (or to its `NotAdmitting` rejection,
  observable as cell terminalization, §3.2), and an admitted child is
  retained, observable and removable through the slot's handles.
  Cancelling admission in the split form is therefore explicit: `remove`
  by the handle already held. Both forms at both pre- and post-admission
  points are invariant §13.12's provocations.
- **Cancellation is linearized by the cell, not the command channel.**
  Dynamic membership operations are *eager at reservation, awaited at
  admission*: the call claims the id and mints the cell synchronously
  before returning — for the fused form too, whose future exists only
  to carry the two steps' outcome — while the admission command rides
  the unified unbounded event lane (§10), driven by the caller's polling.
  The small request record is batched at the consumer; the reservation and
  user payload remain owned by the producer, so a channel capacity would
  bound ingest rate rather than memory. Drop cannot await that lane, so the
  drop rules above are enacted through a
  **level-triggered cancellation latch** owned by the fused future and
  registered on the cell: dropping the future flips the latch
  synchronously (a token edge that always succeeds, like §10's shutdown
  latch). Both exit-time restart scheduling and execution of an already-due
  restart deadline MUST consult that level-triggered source directly before
  charging or running a restart; the public `Removing` projection follows the
  forwarded removal event and is not the synchronization primitive. Queue
  saturation therefore cannot charge restart intensity or run user
  construction for a cancelled membership while its removal event waits for
  forwarding. The driver converts the edge into an engine event that
  resolves the race by stage — a not-yet-admitted operation, whether
  its command is unsent, still queued, or dequeued-but-unprocessed, is
  annulled at the admission check (reservation released, cell
  terminalized, §3.2); an admitted child is removed exactly as by
  exact-handle `remove` (§11). Before-first-poll behavior follows: the
  reservation already exists, the admission command was never sent, and
  dropping the never-polled fused future terminalizes the cell through
  the same latch. The split `define` future carries no latch — that is
  the detach rule above — and its detach guarantee ("admission
  proceeds") holds from first poll, once its command is in flight; one
  dropped before ever being polled has submitted nothing, and the cell
  terminalizes exactly as if the slot had been dropped undefined
  (§3.2).
- **Runtime availability is checked before dynamic mutation.** A dynamic
  `reserve_*` validates the id syntax first, then requires an ambient
  runtime, and only then reads the scope's admitting state or resident-id
  table. The error precedence is therefore `EmptyId` before `NoRuntime`,
  and `NoRuntime` before `NotAdmitting`, `RemovalInProgress`, or
  `DuplicateId` (and before identity minting can yield
  `IdentityExhausted`); a no-runtime rejection mints no cell and claims
  no id.
  The admission future rechecks runtime availability at its first poll,
  before spawning or submitting the admission command. If a slot was
  reserved inside a runtime but its `define` future is first-polled
  outside one, both fused and split forms return `NoRuntime`, release the
  reservation, and publish the cell as terminal `NeverStarted`; the id is
  reusable before that poll returns `Ready`. This failed poll never
  crosses the split form's in-flight/detach edge. A completed admission
  future is fuse-like: any later poll remains pending rather than
  repeating the outcome or panicking.
- **Stage semantics: dynamic operations are admission-stage-exact.** A
  dynamic scope is *admitting* exactly while its membership is
  non-terminal and it has a live incarnation in `Starting` or `Running`
  (B.6) — the scope's own startup counts; drain does not, and neither
  does a dynamic root parked in `StartupFailed` (§11): the park is the
  state the owner must resolve, and admitting new members into a
  half-started root would muddy exactly that decision. Outside the
  admitting window — membership terminal, incarnation `Draining`, root
  parked in `StartupFailed`, or no
  incarnation live (a pre-spawn handle, or the window while an
  ancestor restart re-lowers the subtree) — `reserve_*`, dynamic
  `define`, and the fused `add_*` fail with the single `NotAdmitting`
  outcome (B.8, which enumerates the causes). Nothing queues to await
  a future incarnation: that matches §11's rule that runtime
  membership never survives an ancestor restart — re-adding on restart
  is application logic, and the application holds the lifecycle events
  to drive it. `remove` is stage-exact too: a reserved-but-undefined
  cell is already an identity and is removable by id — removal
  terminalizes it (`NeverStarted`, §7) and frees the id, with no
  `Removed` event and no tombstone (pre-admission cells are not yet
  members — §3.2's minting/admission split), and a later
  `define` on the orphaned slot resolves `NotAdmitting` with the
  reservation-ended cause (B.8 — the one cause that fires while the
  scope itself keeps admitting); a child still
  in startup is removed like any running child (§13.12); on a scope
  that is draining or stopped, `remove` resolves `AlreadyAbsent` — the
  teardown owns every stop, and §11's idempotency makes that
  indistinguishable from having removed the child yourself.
- **Construction-source bounds are pinned in the def constructors
  above** — one
  place, verbatim, per §4.2's capability rule: restartable forms carry a
  re-mintable shared source (`Args: Clone + Sync` or a
  `Fn() -> Args + Send + Sync + 'static` factory; task bodies a
  `Fn(TaskContext) -> F + Send + Sync + 'static` factory; raw actors
  `Fn() -> R + Send + Sync + 'static`; subtrees
  `Fn() -> T + Send + Sync + 'static`), `_once` forms consume owned values.
  Cyclic wiring
  thereby reduces to ordering: every ref a
  factory needs is minted from a sibling slot before any factory is
  written, so factories capture real `ActorRef`s — no `Option<ActorRef>`,
  no registry (C.3 is the acceptance scenario).
- **Failure ownership.** An undefined slot is a broken promise owned by
  whoever holds it: spawning a tree with unfilled reservations fails
  eagerly at `spawn()` with a structured `BuildError` naming every
  unfilled id — dropping a builder slot does not silently un-declare it.
  On a dynamic scope, dropping an undefined slot releases the id and
  terminalizes the cell per §3.2: handles resolve terminal, parked sends
  fail `Terminated`, subscriptions close. After a successful define the
  slot no longer exists, and every later failure — spawn, startup, exit —
  is the child's ordinary supervision story, never surfaced through
  slot-specific machinery.

## 9. Scope policy: strategy, intensity, defaults

### 9.1 Fate-sharing strategy

Core ships `OneForOne` only: a child's exit affects that child alone. The
group strategies (`OneForAll`, `RestForOne`) are Part II §19 — they are the
single largest block of deferred engine complexity, and §10's mode-based
exit funnel is designed so they land without restructuring. The `Strategy`
type is non-exhaustive from day one, is a property of **ordered** scopes
only (§2), and does not exist on dynamic scope builders, configs, or
snapshots.

### 9.2 Restart policy and intensity [#371]

Two separated concerns:

- **Per child** — `RestartPolicy`: the condition (`Always` / `OnFailure` /
  `Never` — with `Never` structural on one-shot forms) and `Backoff` (fixed,
  or exponential: `base × factor^(n−1)` clamped to `max`, with optional
  equal-jitter drawing uniformly from `[d/2, d]`; all durations validated
  non-zero at construction). Delay computation is a pure function of
  (attempt, policy, `JitterSample`) — the sample is an input, not drawn
  inside (§1 implementation shape; `shelterwood-runtime` owns the source).
  `JitterSample`
  owns the `[0, 1)` invariant: its constructor clamps finite inputs and maps
  non-finite inputs to zero, while `from_u64_ratio` owns the driver's integer
  random-source normalization. Pinned
  arithmetic: `factor` is a validated newtype over `f64` — finite and
  `≥ 1.0`, checked at construction — implementing `Eq`/`Hash` as
  bit-equality of the underlying bits, sound because the invariant
  excludes NaN and required because `Backoff` is §1 plain data carrying
  the universal `Eq` bound; delay computation is pinned to nanosecond
  precision: when the effective multiplier is exactly one — the first
  attempt, a factor of `1.0`, or a fixed delay — the whole-nanosecond
  count is used exactly, with no float round-trip; otherwise the base
  delay's whole-nanosecond count is multiplied as `f64`, rounded to the
  nearest nanosecond, and a product at or above `max` saturates to the
  exact configured `max` (never an overflow panic); jitter maps the
  pre-drawn `JitterSample` as `delay = d/2 + sample × d/2`,
  rounded the same way — a zero sample yields the exact half,
  half-nanosecond remainders rounding up with no float round-trip. This exact
  exponentiation contract covers attempts operationally reachable by a
  running membership; the opaque counter's full `u64` domain exists for
  totality, not as a promise that synthetic multi-billion-attempt inputs use
  an unbounded exponent representation. Beyond the implementation's supported
  exponent domain, `next_delay` remains total, nondecreasing for a fixed
  sample, and bounded by `max`; it may saturate the exponent and plateau rather
  than evaluate `factor^(n-1)` exactly. The attempt counter is per
  membership: `n = 1` on the first scheduled restart, incremented per
  scheduled restart — a restart scheduled and then cancelled by teardown
  still advanced it, mirroring the intensity charge below — and reset by
  an incarnation that exits after running at least the scope's intensity
  window `within` (one clock answers "has it settled"); the snapshot's
  `restart_count` (B.6) is its non-resetting cumulative twin. These three
  reset-distinct domains are public opaque values: `RestartAttempt` for the
  resettable backoff position, `RestartCount` for one membership's cumulative
  charges, and `TotalRestarts` for one scope incarnation's cumulative charges.
  Each exposes only `ZERO`, a saturating `bump()`, and `get()`; none implements
  arithmetic traits, so the domains cannot be added, substituted, or compared
  across one another accidentally. Running time is
  passed to the settling decision as one named `IncarnationRun {
  started_at, stopped_at }`, so its endpoints cannot be transposed, and is
  measured between two engine-stamped instants: the incarnation's **start
  instant**, stamped once when the engine schedules the spawn (the
  `Started` event's instant — the same stamp anchors §6's readiness
  deadline), and its exit publication (§7). Failure
  classification is §7's. Downstream code can inspect the policy's
  condition (`is_never`-class queries are public).
- **Per scope** — `Intensity { max_restarts, within }`: the churn budget
  (default: Appendix A), tripping on the restart that *exceeds* the budget.
  **Every** respawn charges it — in core that is every own-child respawn;
  when Part II §19 lands, sibling respawns forced by group strategies charge
  the same budget (that rule is stated here so §19 cannot relitigate it).
  Exceeding it is scope-fatal and escalates to the parent. A per-child cap
  MAY exist as a refinement; it cannot substitute for the scope budget.
  Edges, decided: the budget is charged when a restart is *scheduled*,
  before any backoff delay elapses; the rolling window is strict — a
  charge at time `t` ages out once `now − t > within`, and the trip fires
  on the charge making the in-window count exceed `max_restarts`. The
  over-budget edge itself is exact: the tripping charge is a real
  scheduling charge — the membership's attempt counter, its cumulative
  `restart_count`, and the scope's
  `total_restarts` (B.6) all include it, and
  `RestartScheduled { attempt, delay }` **is** emitted — after which the
  trip fails the scope before the delay elapses, so the scheduled
  restart is cancelled by the ensuing teardown and never spawns (the
  scheduled-then-cancelled-still-advanced rule above, applied to the
  trip itself). The emitting scope's event order is pinned: the child's
  `Exited` → its `RestartScheduled` → the scope's own failure
  (`ScopeState: Draining`, then the nested terminal state or the §11
  root outcome), and the trip payload's in-window count includes the
  tripping charge. The
  budget exists on both scope flavors
  (dynamic scopes restart their own children too); a tripped scope surfaces
  at its parent as an ordinary `Failed` child exit whose error value is a
  structured, library-owned, publicly nameable intensity-trip type —
  carried as library-constructed `ExitError` provenance (§7: reached
  through B.5's named accessor as one compile-checked call, no downcast
  anywhere, non-forgeable), while the framework's own classification
  consults only `is_failure()` — subject to the parent's
  restart policy for that scope child; and at the root, tripping terminates
  the tree — the owner observes it through `wait_started()` during
  startup, `wait()` after it (§11), and the terminal reason
  (`Stopped { reason: IntensityTripped }`) carried by the root's final
  snapshot and `ScopeState` event (B.6, B.4).

### 9.3 Defaults [#369]

**The scope-defaults record is enumerated, not elided.** `ScopeDefaults`
is one plain-data record (§1) with exactly these fields. The first four are
optional (`None` = resolve outward); readiness uses its own explicit
`ReadinessDeadline::Inherit` unset state, so an `Option` cannot represent the
same meaning twice:

- `child_restart: RestartPolicy` — condition + backoff;
- `child_shutdown: Shutdown`;
- `mailbox: Mailbox` — kind **and** capacity travel together (the
  library default is `queue` at Appendix A's capacity; a scope may
  default its actors to `latest()`);
- `mailbox_shutdown: MailboxShutdown`;
- `readiness_deadline: ReadinessDeadline` (§6), default `Inherit`.

Deliberately *not* in the record: readiness mode (per instance, §6),
terminal-membership retention (decided by the child's §4.2 mode with a
per-child override, §8), and strategy/intensity (properties of the
scope itself, not defaults for its children — §9.1, §9.2).

Resolution and mechanics:

- Per-child resolution is declaration → nearest enclosing scope with
  that field set → library default (Appendix A). Exactly one stored
  copy at declaration; children resolve at insertion.
- Subtree edges make an **explicit inherit-or-reset decision**: one
  whole-record knob on the subtree veneer, `defaults: Inherit | Reset`,
  defaulting to `Inherit`. `Inherit` composes — the child scope's unset
  fields keep resolving outward through the parent's resolved record.
  `Reset` severs — the child scope's unset fields resolve straight to
  library defaults. Either way, fields the child scope sets explicitly
  win: `Reset` resets what the scope *didn't* say, never what it did.
  Per-field inherit/reset is rejected as drift surface; the knob is
  whole-record.
- Mailbox constructors can defer capacity to the scope default (§5.1) —
  choosing a mailbox *kind* must not silently discard the capacity
  default. Deferral resolves **by kind**: a declaration that names a
  kind but defers capacity resolves that capacity through enclosing
  defaults *of the same kind* — a scope default of a different kind has
  no capacity to contribute and is passed over, resolution continuing
  outward to the library default for the declared kind (Appendix A).
  The two rules compose without conflict: "kind and capacity travel
  together" governs a *default record's* contribution (a default never
  donates its capacity to a different kind, and applies whole where the
  declaration named no kind); kind-matched deferral governs a
  *declaration's* unfinished capacity. The exact policy case, decided:
  a child declaring `queue` with deferred capacity under a scope
  default of `latest()`, with no enclosing `queue` default beyond it,
  gets `queue` at the library default capacity — the scope default
  neither converts the declared kind nor supplies a capacity across
  kinds, and with no outer `queue` default the outward walk ends at
  the library default. For the full outward walk, suppose the root
  defaults to `queue(10)`, an inheriting child scope defaults to
  `latest()`, and an inheriting grandchild scope contains an actor declared
  with `queue_inherit()`: that actor resolves to `queue(10)`, because the
  intervening `latest()` is passed over when resolving queue capacity. If
  the grandchild edge is `Reset` instead, the same declaration resolves to
  the library `queue(64)`; the reset severs the root's contribution.
- Validation is eager: any configuration that would fail spawn fails at the
  point of declaration where it is decidable — duplicate/empty ids at add
  time, zero capacities at construction, zero backoff durations at
  construction.
- Library-level fallback values are Appendix A; they apply only where
  neither the declaration nor any enclosing scope decided.

(The serializable outline — the declaration companion that must serialize
distinctly for any two trees differing in the surface it carries — is Part II
§21, together with the `serde` feature.)

## 10. Shutdown [#370]

One escalation ladder, one state machine, everywhere:

```
cooperative cancel → grace expiry → tidy-abort beat → hard abort
```

### 10.1 Shutdown invariants and transitions

1. **S1 — one ladder.** Every stop owns exactly one `StopLadder`. Its accepted
   transitions are `Idle --advance→ Cooperative/Cancel`, due cooperative
   deadline `→ Escalated/Escalate`, due tidy deadline `→ Finished/HardAbort`.
   `force(now)` only moves the current deadline earlier and never skips the
   tidy beat. (`engine::ladder_uses_cancel_escalate_and_hard_abort_for_every_policy`,
   `engine::repeated_force_expedites_without_rewinding_the_ladder`,
   `engine::force_preserves_an_already_due_deadline`.)
2. **S2 — stop policy has no zero-grace branch.** `Shutdown::Graceful`
   contains `NonZeroDuration`; `Shutdown::graceful(Duration::ZERO)` is a
   construction error. `Abort` is the sole immediate-escalation policy and
   records `WithinGrace`; expiry of a graceful policy records `AfterGrace`.
   (`policy::graceful_shutdown_rejects_the_zero_duration_branch`,
   `engine::ladder_uses_cancel_escalate_and_hard_abort_for_every_policy`.)
3. **S3 — flavor owns sequencing, not mechanism.** `BeginDrain` initializes a
   reverse cursor for ordered scopes and emits `StopChild` for every incomplete
   child of a dynamic scope. Ordered `Settle` exposes at most one incomplete
   child and does not advance until it joins. (`supervisor::ordered_stop_releases_one_child_per_join_in_reverse_order`,
   `integration::ordered_graces_sum_while_dynamic_graces_overlap`.)
4. **S4 — drain reason is a monotone lattice.** A later transition may upgrade
   `Finished < IntensityTripped < StartupFailed < ShutdownRequested`, never
   downgrade it. Forced shutdown also sets the hard-force fact and emits one
   `ForceChild` per incomplete child. (`engine::scope_lifecycle_upgrades_drain_reasons_monotonically`,
   `integration::latched_shutdown_upgrades_an_intensity_drain`,
   `integration::force_uses_the_stop_funnel_for_every_ordered_child`.)
5. **S5 — completion is derived and level-triggered.** `all_children_joined`
   is derived from child states. `Settle` emits `Finished` once iff the
   lifecycle’s flavor-specific finish predicate accepts that derived value.
   (`supervisor::derived_completion_property_matches_the_child_states`,
   `supervisor::exhaustive_small_scope_interleavings_preserve_reducer_invariants`.)
6. **S6 — shutdown requests are sampled latches.** Scope shutdown and removal
   are synchronous, idempotent latches sampled into reducer events at step
   entry. A request consumed by incarnation N does not reach N+1; a request
   accepted with no live incarnation is owned by the membership until the next
   incarnation begins. (`integration::pre_spawn_shutdown_waits_for_teardown_to_exist`,
   `integration::pending_restart_shutdown_expedites_finite_and_unrepresentable_backoff`.)
7. **S7 — driver death discharges owned obligations.** Destroying a driver
   terminalizes active memberships, resolves admission/removal/shutdown
   completions, and closes observation only after terminal publication. The
   synchronous fallback may sacrifice post-join precision but may not leave a
   promise parked. (§13.17’s fault provocations,
   `integration::ancestor_hard_abort_disposes_a_queued_admission_and_midflight_removal`,
   `integration::hard_aborted_subtree_descendants_still_publish_exits`.)

No transition-system rule above is flagged unverified. The documented
recursive-join exception after an ancestor destroys a driver is intentionally
an ownership boundary, not a stronger join claim.

**Normative prose coverage.** Mailbox freeze/drain policy is checked by
`integration::queue_drain_is_the_exact_frozen_accepted_prefix_and_rejects_deferred_work`
and its `latest` twin. Local-stop reuse of the ladder is checked by
`integration::handler_drain_and_on_stop_share_one_grace_budget`. The latch,
pending-incarnation, ordering, reason, and driver-death paragraphs map to
S1–S7. Resource-teardown guidance is operational advice; the checkable
blocking detach behavior is covered by offload/drain integration tests, while
application resource integrity is explicitly **UNVERIFIED and outside the
framework contract**. No other normative shutdown paragraph is unmapped.

- Per-child stop state is a single owned ladder value (policy, phase,
  deadline) advanced by the engine — conceptually
  `StopLadder { policy, phase, deadline }` with
  `advance(now) -> Option<Cancel | Escalate | HardAbort>` — with all
  pending deadlines in one priority queue, not rescanned per wake. Ordered
  teardown, concurrent drain, dynamic removal, and nested-scope teardown
  differ only in *when ladders start*, never in how a ladder runs.
- The tidy-abort beat (the pause between escalation and hard abort, letting
  a cancelled child finish a final cleanup step) is defined in Appendix A.
- `Shutdown` policy per child: `Shutdown::graceful(nonzero_grace)` (stored as
  `Graceful { grace: NonZeroDuration }`; default: Appendix A) or `Abort`.
  `Abort` is the immediate-escalation point on the same ladder, not a second
  mechanism: the shutdown token fires and the ladder escalates
  immediately — no grace wait — so the `abort_token` fires in the same
  engine step but strictly *after* the shutdown token (the B.2 ordering
  contract is unconditional; the tokens never fire "together"), the
  tidy-abort beat still runs, then hard abort — and the child is joined
  before teardown advances, exactly as under grace expiry. The policy does
  not pre-decide the classification: §7's classifier still reads what
  actually happened, so a child that yields an outcome during the beat
  exits `Completed`/`Failed` (with `Cancellation::Observed`), and
  `Aborted { phase: GracePhase::WithinGrace }` records only a hard abort actually
  reached — the future destroyed before yielding — distinguishable from
  grace-expiry abort (`phase: GracePhase::AfterGrace`). The one boundary case, where
  the beat expires and the hard abort lands *after* the body recorded its
  outcome but before the join retires, is settled by §7's precedence and
  not here: a recorded `Failed` survives that abort, a recorded
  `Completed` does not. Grace is a supervisor-side
  upper bound; child-local time after
  cancellation wakeup is scheduler-dependent — this is documented
  contract. Cancellation-before-escalation ordering is observable to the
  child itself — its `shutdown_token` fires strictly before its
  `abort_token` (B.2), which is what C.2's sidecar reads from the child's
  own journal — and in the exit's `cancellation`/`phase` fields;
  lifecycle events carry **no ladder-transition events** (B.4's inventory
  is deliberately exit-only here), so §13.10's ordering assertions are
  built from child-side observations, not the event stream.
- Ordered scopes tear down in reverse declaration order, one at a time, full
  grace each; the cursor child is aborted *and joined* before the ladder
  advances to the next sibling. Dynamic scopes cancel the group at once and
  drain concurrently (grace clocks run in parallel, not summed). Aborting an
  ancestor arms a recursive hard-abort cascade.
- **Driver death discharges; it never absolves.** A scope driver
  destroyed with obligations outstanding resolves all of them on the way
  down: still-active descendants publish the coarse kill verdict —
  `Aborted { phase: GracePhase::WithinGrace }` with `Cancellation::Observed` — memberships
  terminalize (sends fail `Terminated`, exit-awaiting surfaces resolve),
  in-flight admissions and removals resolve their enumerated rejections,
  and every `Added` is paired with its `Removed` before the scope's own
  final event. First publication wins: an orderly post-join report that
  already landed is never overwritten. §7's post-join precision is an
  orderly-path property, deliberately traded for promptness on the kill
  path — the future was destroyed, so "what would it have reported"
  is unknowable in bounded time; this is the same trade `brutal_kill` →
  `killed` makes, decided here once rather than per call site.
- "Drained" has exactly one definition, derived from child state (no
  hand-maintained live counter).
- Child exits are consumed through one funnel regardless of which await
  point dequeued them — one `ingest(exit) -> Classified` that always
  records, and one `dispatch` whose behavior is a function of the scope's
  current mode (`Running` vs `Draining { scope, reason }`) held on the
  runtime — plus the exiting child's own membership state (`Removing`
  suppresses that child's restart, §7) — not of the call site. The root's
  `StartupFailed` park (§11) is not a third mode: a parked root keeps
  dispatching its started prefix under `Running` — restarts schedule and
  charge intensity as usual, and an intensity trip terminates the tree
  from the park exactly as from `Running` (§9.2). This mode-based design is what makes
  Part II §19's group restarts a bounded change. The ladder and the funnel
  together are the decision layer of §1's implementation shape: they only
  compute; the driver shell awaits.
- The drain reason is a verdict lattice, not a first-writer-wins latch:
  reasons carry the total precedence order `ShutdownRequested` >
  `StartupFailed` > `IntensityTripped` > `Finished`, and a drain already
  in progress upgrades its reason monotonically when a higher-precedence
  cause arrives; nothing downgrades. An explicit shutdown request
  therefore cannot lose its verdict to a lower-precedence cause that
  latched one wake earlier: a restartable exit that trips intensity in
  the same batch as a shutdown request still yields `ShutdownRequested`
  as the scope's terminal reason. A nested lowering failure before the
  driver loop follows the same rule: a stop request already latched for
  its epoch, or a fired ancestor-shutdown latch, upgrades the terminal
  reason and pending startup result to `ShutdownRequested` — and, like the
  loop path, records the stop as observed, so the scope still exits at its
  parent as a cancelled `Completed` (§11). Ladder deadlines
  share the posture: a forced escalation only ever moves a deadline
  *earlier* (`min`), never later. (`NeverStarted` sits outside the order — it
  is the terminal reason of a membership that never had an incarnation to
  drain.)
- Mailbox shutdown policy — `Drain` (the default) or `Discard` — is part
  of the actor options. It is a two-variant choice about exactly one
  thing: the fate of the frozen accepted prefix (§5.2). The intake
  freeze itself is unconditional and engine-enforced — new sends are
  rejected under either policy — so there is no separate "reject-new"
  variant. `Drain` delivers the frozen prefix before `on_stop`;
  `Discard` drops it — where and on what task per §5.1's destruction-venue
  clause, with disposal faults outside the exit verdict (§7). The blanket
  handler loop honors the policy itself;
  for a raw actor the framework enforces only the freeze — the loop owns
  delivery, so honoring the policy is the raw loop's documented
  obligation, using `RawContext`'s resolved-policy accessor and the
  `try_recv` drain primitive (B.1). Grace bounds the drain either way
  (§5.2).
- A local `ctx.stop()` runs the same ladder: the engine observes the
  self-stop and arms the child's configured `Shutdown` policy as the
  bound on its drain-plus-`on_stop` window (§5.2), so a wedged self-stop
  escalates — grace expiry, tidy beat, hard abort — exactly like a
  supervisor-initiated one. Self-stop changes who started the clock,
  never which ladder runs.
- Shutdown requests are **level-triggered, not queued**: owner drop,
  `request_shutdown()` / `request_scope_shutdown()`, and parent
  escalation each set an idempotent per-scope latch (a token edge, like
  the cancellation tokens themselves), which always succeeds
  synchronously. The unified event lane (Appendix A) is unbounded and
  processed in capped batches; awaited *insertion* operations
  (`add_*`/`define`) retain their payloads in producer-owned reservations.
  Fire-and-forget shutdown is lossless because it does not ride that lane.
  `remove` rides no channel either: removal is a forced stop, and it
  latches like one (§11's remove rule). The
  latch is **per-incarnation** state: it stops the scope incarnation it
  was set on and does not outlive it — a restarted scope incarnation
  starts with a clear latch (§11's nested-shutdown rule), so an
  `Always`-restarted scope cannot enter a stop/restart storm. One
  deliberate, named companion: the **pending-incarnation stop latch**.
  A stop request accepted while the membership has *no live
  incarnation* (a restart window — B.9's `shutdown_and_wait` landing
  between incarnations) is held on the membership and armed onto the
  next incarnation at its start, which then starts and immediately
  begins teardown. The two rules partition by target and never
  conflict: fresh-restart-starts-clear says a latch *consumed by*
  incarnation N never carries to N+1; the pending latch holds a
  request that arrived with no incarnation to consume it — it was
  never any incarnation's spent latch, and it waits for its first.
  What cannot exist is a stop request silently dropped in the window.
  Cancelling an awaited membership operation never rides the channel
  either: it is a per-operation level latch on the operation's cell
  (§8's linearization rule).
- Rebinding-transparent `send` during teardown can park against an unbound
  sibling; teardown-window notifications use `try_send`. This tradeoff is
  prominent shutdown guidance, alongside the grace-is-an-upper-bound
  contract above.
- **Resource-teardown discipline** — equally prominent guidance; the
  mechanisms live in §4.1, §5.2, §5.5, §8, and §11, the discipline is
  stated once, here:
  - `on_stop` is best-effort (§4.1): hard abort, process kill, and a
    panicking peer can all skip or truncate it. It exists to return
    resources promptly; correctness MUST NOT depend on it completing —
    durable integrity is the application's job (crash-only posture).
  - Grace is one budget shared by mailbox drain and `on_stop` (§5.2). A
    child owning a slow-closing resource sizes grace for drain *plus*
    close — or opts that mailbox out of draining (`mailbox_shutdown`).
  - Slow-closing resource owners take a long per-child grace and an early
    slot in an ordered scope: reverse teardown stops their dependents
    first, so the close runs quiescent with its full grace. Ordered
    graces sum — that is the deliberate cost, bounded by the owner's
    `shutdown()` timeout.
  - A *blocking* close can survive hard abort: `run_blocking` is available
    in `StopContext` (B.1) and its thread detaches past abort (§5.5).
    Async cleanup cannot — hard abort drops the future, and no async work
    outlives the incarnation by design (offloads are incarnation-owned
    and absent from `StopContext`).
  - A resource that outlives the incarnation does not belong to it: carry
    restart-surviving handles in `Args` (§4.1). The simplest shape needs
    no library feature at all: the host opens the resource before
    `spawn()`, hands clone-able handles in through `Args`, and closes it
    after `shutdown()` resolves — teardown order falls out of host code,
    and the close runs outside any grace budget. The dividing line is
    what restart should heal: a resource owned by `init` is reconnected
    by restart; a host-owned resource is outside supervision, and its
    failures are the host's (or the handle's own reconnect logic's) to
    handle. Host-own process-lifetime, self-healing resources (pools);
    incarnation-own connections whose failure the restart policy should
    repair.

## 11. Trees, spawning, and lifetime

### 11.1 Tree and lifetime transition rules

1. **T1 — declaration consumes into one typed root.** `Tree` lowers to
   `System<ScopeRef>` and `DynamicTree` to `System<DynamicScopeRef>`; subtree
   flavor likewise determines the returned handle. `System` is the sole
   non-cloneable owner and its drop latches shutdown. (`api_trait_conformance`,
   `system::subtree_conversion_moves_without_minting_a_phantom_scope`.)
2. **T2 — root and nested startup failure diverge only after the same reducer
   transition.** Root failure parks `StartupFailed` and keeps the started
   prefix supervised; nested failure drains through S1–S5 and reports a
   structured failed child exit. (`integration::dynamic_startup_failure_keeps_other_initial_members_supervised`,
   `integration::nested_ordered_startup_failure_rolls_back_only_the_started_prefix`.)
3. **T3 — natural completion is flavor-policy derived.** Ordered scopes may
   finish after every membership joins; dynamic scopes and ordered scopes with
   a perpetually restartable member do not finish merely because their current
   resident set is empty/terminal. (`integration::dynamic_and_always_members_do_not_finish_naturally`,
   §13.16.)
4. **T4 — dynamic admission has one linearization path.** Reservation owns id
   uniqueness and payload; an admission event inserts one authoritative child
   record and resolves independently of child readiness. Cancellation chooses
   withdrawal or removal by whether admission won. (`integration::dynamic_actor_add_resolves_at_admission_without_awaiting_init`,
   `integration::fused_drop_withdraws_or_removes_while_split_drop_detaches`.)
5. **T5 — removal is exact, idempotent, and monotone.** Exact-handle removal
   compares membership; id-only removal names the current resident. Once
   sampled, the reducer state stays `Removing` through stop, terminalization,
   finalization, and reclaim. (`supervisor::transition_table_keeps_removal_in_the_authoritative_state`,
   `integration::exact_handles_reject_cross_scope_and_same_id_successors`.)
6. **T6 — non-owning scope shutdown targets an incarnation.** A live request
   resolves after that incarnation’s scope epilogue; a restart-window request
   arms the next incarnation; a membership that never spawns resolves at
   terminality. (`integration::pre_spawn_shutdown_waits_for_teardown_to_exist`,
   `integration::pre_spawn_shutdown_resolves_if_the_tree_is_dropped_unspawned`,
   `integration::shutdown_and_wait_wakes_when_a_parent_drain_terminalizes_a_restarting_subtree`.)
7. **T7 — dynamic capability is explicit.** `DynamicScopeRef` exposes only
   dynamic operations inherently. Shared observation/control is reached via
   `as_scope() -> &ScopeRef`; there is no mirrored forwarding block and no
   `Deref` conversion. (`api_trait_conformance`.)

All rules above have checked evidence. Process runtime and panic-mode
preconditions remain documented host obligations and are not marked verified
by the reducer suite.

**Normative prose coverage.** Builder inventory, typed subtree dispatch, and
trait/resource bounds are compile-checked in `api_trait_conformance` and tree
unit tests. Startup parks/rollback, natural completion, nested shutdown,
admission/removal, pending-incarnation shutdown, and terminal pruning map to
T1–T7 plus the named integration tests. `shutdown`/`start_or_shutdown`
straggler shapes are checked by
`integration::start_or_shutdown_rollback_timeout_preserves_the_startup_cause_and_stragglers`
and `integration::zero_timeout_reports_recursive_straggler_paths_and_joins_them`.
The Tokio/time-enabled and `panic = "unwind"` requirements are explicitly
**UNVERIFIED host preconditions**; the no-runtime error path is checked by
`integration::spawn_without_runtime_is_a_build_error`. No other normative tree
or lifetime paragraph is unmapped.

- `Tree` / `DynamicTree` are the declaration layer; `spawn()` lowers into
  the engine and returns `System` (sole owner; drop = graceful
  shutdown; explicit `shutdown()` with timeout available). `spawn()` is
  synchronous and requires an ambient async runtime; with none present it
  returns an error (`BuildError::NoRuntime` — the type is enumerated in
  B.8) — it never panics.
- **Builder operation inventory** (content-normative; Appendix B's
  naming latitude applies). One constructor per flavor — `Tree` for an
  ordered root, `DynamicTree` for a dynamic one. Per-scope
  configuration setters: `strategy` (ordered only, §9.1), `intensity`
  (§9.2), `defaults(ScopeDefaults)` (§9.3). Membership declaration:
  the eight `add_*` entry points and the `reserve_*` slot family (§8) —
  on an ordered builder, declaration order is start order (§2); nested
  scopes declare through `add_subtree` / `add_subtree_once`, whose
  subtree veneer carries the `Inherit | Reset` defaults knob (§9.3)
  and the readiness deadline (§6, no mode knob). Builders are plain
  owned values: no interior mutability, no registration side effects —
  dropping an unspawned builder terminalizes its cells (§3.2) — and
  `spawn()` consumes the root (typed per the dispatch rule below).
  That inventory is exhaustive: a builder operation outside it is
  spec-extension, not implementation latitude.
- **Operational preconditions** (documented contract, checked where
  cheap): the host process runs `panic = "unwind"` — §7's `Panicked`
  classification is unwind-based, and under `panic = "abort"` a panic
  kills the process before supervision can observe anything (the
  documentation states this; there is nothing to detect). The ambient
  runtime is a Tokio runtime with time enabled, reached only through the
  private façade over `shelterwood-runtime`. The owner resolves `shutdown()`
  (or drops `System` and lets teardown finish) before tearing the runtime
  itself down — destroying the runtime around a live system is outside the
  contract.
- `wait_started()` resolves when the whole declared tree is up, or reports
  terminal startup failure. **At the root, startup failure does not
  auto-roll-back the live started prefix** — that is the owner's
  decision — so a
  `start_or_shutdown()`-shaped composition is provided making the safe
  default (roll back the started prefix on startup failure) one call, not a
  pattern each host must remember. Its contract is pinned: it consumes
  the `System` and takes the rollback timeout as its trailing deadline
  (Appendix B's shutdown exemption applies — an escalation bound, not a
  failure deadline). On successful startup it returns the `System`; on
  terminal startup failure it drives the full `shutdown(timeout)` path
  over the started prefix and returns an error carrying the original
  structured startup failure (the same payload `wait_started()` reports)
  together with the rollback outcome, including any shutdown-timeout
  straggler report — rollback never masks the startup error that
  triggered it. The root parks in the `StartupFailed`
  scope state (B.6): the started prefix stays supervised, the
  never-started suffix is terminal (`NeverStarted`, §6/§7; a dynamic
  root has no such suffix — its initial members start concurrently, §6
  — so the park holds every member, all still supervised).
  `StartupFailed` is a park of the scope *state*, not a dispatch mode:
  the exit funnel keeps dispatching the started prefix as `Running`
  (§10), so restarts schedule and charge intensity as usual, and a root
  intensity trip terminates the tree from the park exactly as from
  `Running` (§9.2 — observed through `wait()`, since `wait_started()`
  already resolved with the startup failure). Natural completion is the
  one dispatch outcome the park withholds: a parked root whose every
  membership terminalizes does not publish `Finished` — it stays in
  `StartupFailed`, the state the owner must act on (the finishing rule
  below is scoped accordingly). Beyond the intensity trip, the exits
  from the park are the owner's — `shutdown()` or drop. (`wait()` is a
  consuming await, B.10 — an owner watching the park for a trip has
  surrendered `shutdown(timeout)` and its straggler report; hold the
  `System` and observe through snapshots or lifecycle events instead
  when the structured rollback matters.)
- A **nested** scope has no external owner to hand that decision to: its
  owner is a parent supervisor whose whole vocabulary is child exits and
  restart policy, and a half-started subtree is not a state it can hold.
  Nested terminal startup failure is therefore scope-fatal with automatic
  rollback — the scope terminalizes its never-started members
  (`NeverStarted` — an ordered scope's unstarted suffix; a dynamic
  scope has none, §6), tears down its started members through the
  ordinary ladders (reverse order when ordered; cancelled at once and
  drained concurrently when dynamic — §6, §10), publishes
  `Stopped { reason: StartupFailed }` (B.6), and exits at its parent as
  an ordinary child `Failed` whose error is the structured,
  library-constructed startup-failure payload (§7 provenance; accessor
  B.5) naming the failing child's id and exit. The parent's restart
  policy for the scope child then applies as usual — a restart re-lowers
  and re-runs the whole subtree startup. (Here and in the shutdown case
  below, the published `Stopped` is **per-incarnation**: the scope's
  membership survives the restart, its event stream stays open across
  it, and only membership terminality closes the stream — B.4's
  restart-continuity rule.)
- A nested scope stopped by an **explicit shutdown request** —
  `request_scope_shutdown()` from inside it, or `request_shutdown()` /
  `shutdown_and_wait()` on its handle (incarnation-targeted, B.9) —
  tears down through the ordinary
  §10 ladders, publishes `Stopped { reason: ShutdownRequested }` (B.6),
  and exits at its parent as `Completed` with `Cancellation::Observed` (§7): a
  requested stop is a cooperative completion, not a failure. The
  parent's restart policy then applies as usual — `OnFailure` leaves the
  scope down; `Always` restarts it, and because the shutdown latch is
  per-incarnation (§10), the fresh incarnation starts with a clear latch
  rather than immediately re-stopping. (A scope torn down by its
  *ancestor's* own shutdown is not this case: the exit is recorded by
  the funnel, but the draining parent schedules nothing — §10's mode
  dispatch.)
- The owner has two further consuming awaits. `shutdown(timeout)` requests
  the §10 ladder and waits for the root driver's terminal epilogue. While a
  framework driver remains scheduled, it joins each child before completing.
  The timeout bounds the **cooperative** phase, not the return: at expiry
  the stragglers owned by scheduled drivers are hard-aborted and joined, and
  the call returns the structured shutdown-timeout error (§7) naming them.
  There is one recursive-join exception: if a framework driver misses its
  abort acknowledgement and its ancestor hard-aborts it at the tidy-beat
  backstop, the driver's synchronous `Drop` epilogue requests abort for its
  active children but cannot await their join handles. Those deeper tasks
  may finish cancellation and destroy their user futures after the owner's
  call returns. The return still joins the root driver and completes the
  target scope epilogue; that epilogue has requested stop or abort for each
  directly owned child. It does not claim either a recursive join or completed
  abort propagation through deeper fallback boundaries.
  `run_blocking` threads are a separate, unconditional detach-past-abort
  exception (§5.5) and are never joined.
  Expiry does not bypass the single ladder: the stragglers are driven
  through its abort tail — abort token, one tidy-abort beat, hard abort
  (§10) — concurrently, then joined while their scope driver remains
  scheduled. A
  zero timeout skips only the cooperative *wait*: every descendant is
  escalated immediately through that same abort tail (tokens still fire
  in order, the tidy beat still runs — `shutdown(0)` means "every child
  under `Abort` policy", §10, not a second mechanism), then the same
  driver-owned joins. The zero form is exempt from Appendix B's
  zero-budget-fails-immediately rule (stated there): this timeout is an
  escalation bound, not a failure deadline. Except for the documented
  hard-abort fallback and `run_blocking` detach boundaries, no teardown
  remains after return; consuming the owner makes that wait explicit.
  `wait()` awaits natural termination without requesting
  shutdown, and resolves with the root's terminal reason (B.6:
  `Finished`, `IntensityTripped`, or `ShutdownRequested` when teardown
  was requested concurrently elsewhere);
  `wait_started()` resolves once at startup and cannot observe a
  later trip, so `wait()` is the post-startup observation point. Natural
  completion is pinned exactly: an **ordered** scope *finishes* when it
  has at least one membership and every membership is terminal (a
  retained terminal child counts — §8's retention is observability, not
  liveness); a root parked in `StartupFailed` is exempt — it never
  finishes (the park rule above). The finishing test runs **at each membership's
  terminalization, strictly before retention-based pruning** removes it
  (§8's remove-on-terminal default), and its result is latched — so
  pruning the final one-shot membership can never turn a finished
  workload into an idling empty scope, and pruning order is otherwise
  unobservable. The scope then publishes `Stopped { reason: Finished }` and, when
  nested, exits at its parent as `Completed` — completion cascades upward
  structurally, to the root and `wait()`. A **dynamic** scope never
  finishes on its own: open membership is its point, and "currently
  empty" is indistinguishable from "between members" — it ends only by
  removal, shutdown, or escalation (completion-driven lifetime is
  composed explicitly from `OneShotTaskRef` awaits plus `shutdown()`;
  §23 packages it). An **empty ordered** scope likewise idles
  indefinitely: completion requires a finished workload, not the absence
  of one — which is what keeps §13.1's zero-children root alive until
  its owner acts.
- `add_subtree` returns the handle type matching its input: mounting a
  `DynamicTree` yields a `DynamicScopeRef`; mounting a `Tree` yields a
  `ScopeRef`. Sealed-trait dispatch (`trait Subtree { type Ref; }`); no
  capability downgrade — and `spawn()` uses the same dispatch at the root:
  spawning a `DynamicTree` yields an owner whose scope handle is a
  `DynamicScopeRef`. The §4.2 mode split applies: `add_subtree_once`
  consumes a single-use tree value (`Never` structural); restartable
  `add_subtree` takes the declaration *source*,
  `impl Fn() -> T + Send + Sync + 'static` (`T: Subtree`), re-invoked at each
  restart to lower a fresh tree.
  `spawn()` consumes a tree value directly — the root has no supervisor
  and no restart, so no source is needed. `dynamic()` survives only as
  the runtime query for name-based traversal [#365].
- Restart rebuilds only declarations: a restarted subtree is re-lowered
  from its source, so runtime-added children of any dynamic scope inside
  it are **not** re-created — their memberships end terminally and their
  handles report terminal. Dynamic membership that must survive an
  ancestor restart is application state (own the roster; re-add on
  restart), not framework state.
- **The lowering rule.** Lowering is where tree validation happens, on
  every flavor; `spawn()` is simply the root's lowering and the only
  lowering with a builder caller to hand `BuildError` to. Every other
  lowering — a restartable subtree's factory-produced tree at each
  (re)start, a once-tree mounted at runtime — validates identically,
  and a failure (a tree containing unfilled reservations, §8) is the
  **scope incarnation's terminal startup failure**: the incarnation
  starts (`Started` fires — the spawn was scheduled, so §9.2's attempt
  and intensity accounting see an ordinary incarnation), validation
  fails before any child membership is created, the rejected tree's
  cells terminalize exactly as a dropped unspawned tree's do (§3.2),
  and the incarnation exits `Failed` carrying the structured
  startup-failure payload with a **lowering cause** naming the
  undefined slots' child-id paths — the same data
  `BuildError::UnfilledReservations` carries, reached through B.5's
  `startup_failure()` accessor (§7 provenance, non-forgeable). The
  scope publishes `Stopped { reason: StartupFailed }` (B.6) and exits
  at its parent as an ordinary child failure; the parent's restart
  policy applies as usual — each retry re-invokes the factory, so a
  stateful source can heal, while a deterministic factory bug churns
  to the parent's intensity trip (§9.2), the designed containment.
  Each such attempt occupies a scope incarnation claimed *before* its
  factory runs: a factory invocation that panics or is torn down
  mid-invocation still spent an ordinary incarnation — observable on
  the scope's stream as that incarnation's own `ScopeState: Starting` →
  `Stopped` pair (§13's restart-continuity rule) and advancing
  incarnation identity (§3.1) exactly like an attempt whose factory
  returned. In
  an ordered parent this is a pre-ready exit and §6's sequence rules
  apply unchanged.
- Dynamic membership operations: `add_*` and their `_once` twins resolve
  at admission with their exact per-kind handles (§3.2) — startup is never
  part of the call
  (B.8); `remove` is **idempotent at the API
  boundary** — removing an already-absent child is one unified
  already-absent outcome (success-shaped or a single variant; not distinct
  errors per handle flavor). Exact-handle removal (remove only the
  membership I hold) is supported and is the safe primitive for planned
  replacement: a stale handle never removes a same-id successor. Inserting
  a duplicate id while the incumbent is mid-removal is a distinct, documented
  rejection (the caller can await removal and retry).
- **The remove rule: level-triggered at the call, detached from its
  future.** `remove` is a forced stop, and it latches like §10's
  shutdown requests rather than riding the command channel: the call
  synchronously resolves its target and, on a resident match, flips the
  cell's removal latch — `membership_status` becomes `Removing` and
  that membership's restarts are suppressed from that instant (§7) —
  then returns a pure *observation* future resolving to
  `Removed | AlreadyAbsent` (B.8). The engine drives a latched removal
  to completion regardless of that future: dropping it — before first
  poll included — **detaches**, abandoning observation only, never the
  removal. There is no unsent, queued, or dequeued-but-unprocessed
  command state to race, because there is no command; backpressure
  concerns do not arise (latches are per-cell state, bounded by
  membership count). Concurrent removes — and a remove landing on an
  already-`Removing` membership — join the one removal and resolve
  with its one outcome; a target that resolves to nothing latches
  nothing and the future resolves `AlreadyAbsent` immediately. The
  asymmetry with §8's fused-add abort-on-drop is deliberate: aborting
  an abandoned add closes an unknown-outcome window (identity never
  delivered), while a removal's outcome is decided at the latch edge —
  abort-on-drop here would reopen exactly the uncertainty the latch
  closes, making drop timing decide whether a child lives.
- Single-use `Tree` values (moved on spawn/mount) are retained; rebuilding a
  tree from retained host state is the documented re-embedding pattern
  (validated by two full embed/run/stop cycles in one process, Appendix C).
- Task-first embedding (supervision with zero actors) is a supported,
  documented mode of the same façade — stated explicitly in the guide, since
  the actor-oriented naming otherwise hides it.
- There is deliberately **no** per-child kill/restart/pause control surface:
  restart is policy-driven; the only forced stops are scope shutdown and
  dynamic removal. Snapshots carry ids and states, not senders — messaging
  an arbitrary actor requires holding its typed `ActorRef` (wired at build
  time or via a userland registry). Anything holding a `ScopeRef` has that
  scope's full observation/control power; "operator" is a role, not a
  framework-enforced privilege level.
- Completion-driven lifetime and the scope-relative sibling-readiness
  barrier are Part II (§23); in core, compose them from `OneShotTaskRef`
  awaitables / `wait_started()` plus an explicit `shutdown()`.

## 12. Observation

Core ships two independent, restart-stable contracts, both rooted in the
engine's single publication path (child metadata rides the same path with
the same fencing — there is no second, separately-fenced view [#362]):

1. **Snapshots** — conflating watch of recursive current state, plus
   the `wait_for_child` helper (contract pinned in B.9). Snapshots
   expose membership and
   incarnation identity as the §3 types; a snapshot is a pure projection
   of decision-layer state, published by the shell (§1 implementation
   shape). Field inventory: Appendix B.6. Alignment with the
   no-subscriber publication skip (§1): `snapshot()` is computed on
   demand from current decision-layer state — never served from the
   last value pushed into the watch — and a new subscription's initial
   value is computed at subscribe time, so the skip is invisible to
   every observer: a lifecycle subscriber that reads `snapshot()` sees
   §13.14's consistent-or-newer guarantee whether or not any snapshot
   subscriber ever existed.
2. **Lifecycle events** — ordered, bounded stream (event inventory,
   ordering, closure contract, and the membership-owned sequencing that
   makes both contracts stable across subtree restart: Appendix B.4;
   buffer size: Appendix A;
   the buffer is per subscriber, so a lagging reader drops only its own
   view); overflow drops oldest and coalesces into one leading
   `Lagged { dropped }` marker (a subscription-level stream item, not an
   event — B.4); event staging aligns with snapshot publication so an event-woken
   reader always sees a consistent-or-newer snapshot (the conformance test
   reads the snapshot *synchronously inside the event arm*, §13.14).
   Cumulative counters distinguish crash restarts from planned remove/add.

**The consistent-cut guarantee.** The single publication path is
transactional: every control-plane transition — scope state changes
(`Draining`, `StartupFailed`, `Stopped`), member terminalization, dynamic
entry release, residency withdrawal and `Removed` publication — commits
atomically with respect to observation. No observer can see a cut
mid-transition: not a terminal membership whose enclosing scope record is
still live, not a released dynamic id whose member is still
snapshot-resident, not a reservation accepted after `Draining` was
published. The wait/observe helpers (`wait_for_child`, exit-awaiting
surfaces — B.9) are observers in this sense: they read the published
view, never the engine's internals, so their outcomes agree with what
snapshots and lifecycle events show at the same cut. Concretely for
dynamic removal: the removed id becomes reusable (and a repeated `remove`
reports `AlreadyAbsent`) only at the commit that withdraws the member
from residency and publishes its `Removed` edge — §2's
resident-membership uniqueness holds at every observable cut.

`tracing` spans emit from one choke point (the optional `metrics` surface
is Part II §20). Everything else observational — peer monitoring, actor
statistics, the self-recovering child-observation reducer, the packaged
restart-counter view — is Part II (§18, §20): all are adapters over these
two streams and the §3 identity types, which is what makes them safely
deferred.

## 13. Invariant checklist

A conforming **core** implementation satisfies all of the following
(test-anchor list; each MUST have direct test coverage). Clauses that
activate with a Part II feature are marked. Each entry includes the
provocation technique proven effective in the origin suite; the shared test
toolkit these recipes assume is: **drop-flag guards** (a `LiveFlag`/
`LiveGuard` pair proving a future was destroyed), **consume-once witnesses**
(oneshot senders / drop counters asserted `== 1`), **per-child `Notify`
release gates** (to sequence which child does what, when), **park-before-
receive actors** (accept into the mailbox without reading), **destructor-
blocking and destructor-panicking actors** (a `Condvar` block or `panic!` in
`Drop` to freeze/poison exact windows), **virtual time** (paused-clock tests
plus explicit `advance`, for backoff and deadline windows), **stats as
acceptance oracle** (poll accepted-counts to know acceptance happened
without racing), and **quiet windows** (bounded negative assertions that an
event does *not* arrive). Where §1's implementation shape is followed,
prefer a lower gear first: drive the decision state machine directly —
events in, effects out, `now` and jitter as data — and reserve the
integration toolkit for the driver shell and the end-to-end invariants.

1. **Exactly one owner per running system; drop = graceful shutdown.**
   Provoke both directions: drop every non-owning handle and assert a quiet
   window (nothing stops), then drop the owner alone and observe
   cancellation; cover a zero-children root and a fire-and-forget
   `let _ = tree.spawn()`. Ownership itself is type-enforced (non-`Clone`,
   `#[must_use]`).
2. **Refs address memberships; sends ride restart windows; terminality is
   the only hard send failure.** Park a send on a full capacity-1 mailbox
   of a never-receiving actor, fail the incarnation, and run the same
   fixture under three restart policies to get all three outcomes
   (ride-through / terminated / timed out). Freeze the rebind window itself
   with a destructor-blocking actor and hand-poll a boxed send future to
   prove `Pending` inside the window, then release and assert delivery to
   the next incarnation. Cancellation and expiry are linearized at
   acceptance (§5.1, Appendix B): dropping a parked send before
   acceptance withdraws it — a quiet window proves it is never
   delivered; dropping after acceptance still delivers; `send_timeout`
   and `call` report their not-accepted outcome only after withdrawal
   succeeds (message recovered and safely re-sendable), and an
   acceptance winning the race at the deadline instant resolves the
   operation — provoke the boundary with a park-before-receive actor
   released exactly at the deadline under virtual time. *(Pinning
   clause activates with Part II §15: a
   pinned ref fails fast across restart while the membership ref rides
   through.)*
3. **At-most-once delivery; nothing buffers across incarnations.** A
   park-before-receive actor accepts several messages, the first delivered
   one poisons the incarnation; assert the queued remainder is never seen
   by the next incarnation while a freshly-sent message is. Repeat the
   shape for `call` (accepted-then-killed ⇒ `ReplyDropped`), offload
   completions, and timers.
4. **One staleness primitive; no bare fencing integers; stale resolution
   fails closed.** Manufacture an identity collision: two scopes each add a
   child with the same id (and, by construction, colliding internal
   coordinates), then present scope A's handle to scope B and require
   rejection. Replay remove→re-add under one id and assert the stale handle
   fails while the replacement is untouched and incomparable with it; assert
   that different ids and different owning scopes are also incomparable.
   Repeat the fail-closed comparison check for a declared child replaced at
   runtime and for a corresponding descendant rebuilt after a nested-scope
   restart. Exhaustion
   mints nothing (§3.1): drive a counter to saturation and assert no duplicate
   token is ever issued — an unmintable incarnation terminalizes the
   membership as under `Never`; an unmintable membership is the enumerated
   reservation rejection (B.8), or structured nested-startup provenance when
   a stable scope cannot rebase a produced declaration. The structural half
   is enforced by API shape (no public bare integers) and review, not tests.
5. **One-shot construction drops its resources exactly once across: init
   panic, startup failure, shutdown-before-start, normal exit.** One
   drop-counting guard type owned by the args, four tests asserting exactly
   one drop each; the shutdown-before-start case cancels the in-flight
   add/start operation mid-flight (select against it) and asserts the
   fallback effect fired. A oneshot sender consumed inside `init` doubles
   as the consume-exactly-once witness on the happy path. The four paths
   run for **every one-shot kind form** — task, actor, raw actor
   (owned `R` as the resource), and subtree (the owned tree value as
   the resource) — not just the kind that first made the fixture easy.
6. **Readiness is declared, engine-enforced, deadline-bounded unless
   explicitly unbounded; a propagating decorator that awaits before
   delegating cannot release the gate early** (the visibility clause — a
   decorator that omits propagation reports the default `Immediate`, an
   ordinary testable bug, §6). Gate with an `init` parked on a `Notify`
   and a shared order log; assert the later sibling never appears until
   release. Deadline expiry under virtual time yields the *typed* readiness
   verdict carrying the deadline. Edge tests: ready-at-deadline beats
   timeout; readiness fired before an immediate clean exit, failure, or
   self-stop counts before that terminal edge; shutdown disarms a pending
   deadline; a *terminal* pre-ready exit aborts startup while an eligible
   restart re-runs the gate with a fresh per-incarnation deadline (§6) — on
   abort the never-started
   siblings terminalize `NeverStarted`; at the root the started prefix
   stays running, while in a nested scope assert the automatic rollback
   and the structured startup-failure exit at the parent (§11).
   The dynamic half (§6's concurrent-start rule): initial members start
   concurrently; a terminal pre-ready exit fails startup with **no**
   sibling terminalized `NeverStarted` — at the root, every other
   member (running or still starting) stays supervised through the
   park; a nested dynamic scope rolls back concurrently
   (runtime-added members included) and exits with the payload naming
   exactly the triggering child's id and exit.
   **Regression:** an inner `AfterInit` actor behind a decorator that
   awaits before delegating still gates ordered startup (§6). Aggregate
   readiness is initial-members-only and monotonic (§6): a runtime
   addition never joins the aggregate (quiet window on scope readiness
   while a gated runtime member sits unready); an already-ready child
   restarting before the aggregate fires holds it open, and one
   restarting after it fires does not rewind it.
   Removal shrinks the declared set (§6), pinned by public-API repros
   that hang the unfixed driver for a full virtual timeout: removing
   the sole unready initial member completes startup; so does removing
   the last unready one beside a ready sibling, removing every initial
   member concurrently (the empty declared set), and removing the sole
   unready member of a *nested* dynamic scope — which must publish the
   aggregate up to an ordered parent and release its gated sibling.
   The negative pins the guard: removing an *already-ready* initial
   member while an unready one remains leaves the scope quietly
   `Starting`.
7. **Exactly one published exit report per incarnation, on every path
   including panic and abort; publication is post-join (§7's two-phase
   rule); one runner, one exit type.** The two hard provocations: an
   actor that panics in its `Drop` *after* the run path recorded
   `Completed` must publish exactly one report, and it reads `Panicked` —
   the destructor verdict supersedes the recorded outcome; and a dropped
   in-flight incarnation followed by its replacement must publish the old
   exit, bound to the old incarnation token, strictly before the
   replacement spawns (§7's sequencing rule), with detached stragglers
   (an aborted `run_blocking` thread's observations) fenced to the old
   incarnation. The containment boundary (§7): a `handle` panic
   unwinding toward an actor whose `Drop` also panics is caught at the
   callback boundary before actor destruction — the process survives
   and exactly one report publishes, `Panicked` with the callback's
   payload. The single-runner property is
   tested internally in core; *(the public hosted-parity clause activates
   with Part II §22: host the same actor without a supervisor and assert
   the identical exit value, including `Panicked` — no user
   `catch_unwind`)*.
8. **Framework verdicts never travel through the user-error channel.**
   Provoke each verdict (readiness expiry, grace-expiry abort, cancelled
   completion) and match the typed variant — never a stringly `Failed`.
   Enforce the mechanism structurally: core classification consumes typed
   `RecordedOutcome` and `JoinVerdict` values and contains no `Any` or
   downcast path; the runtime adapter converts its join result before core
   sees it, while user error erasure stays at the façade boundary. The former
   grep-level exit-path check is retired with the crate split because the
   dependency boundary now makes that direction explicit. Add the forgery
   probe: an application error type
   imitating the intensity-trip or startup-failure payload arrives as an
   erased user error — `intensity_trip()` / `startup_failure()` return
   `None` for it. Note what the split changed here: minting an authenticated
   payload was a crate-privacy rule and is now a convention, because the
   functions that wrap `ExitError`'s private inner variants must be `pub` for
   the façade to reach them across the crate boundary. They are
   `#[doc(hidden)]` implementation seams. The property the probe checks is
   unaffected — the blanket user conversion still cannot produce an
   authenticated payload — but a user who depends on `shelterwood-core`
   directly is outside the boundary rather than defeated by it.
9. **Every respawn charges the scope intensity budget.** Terminal removal
   cannot mask exhaustion; the window ages out under virtual time; backoff
   progression is unit-tested as pure math. The over-budget edge is
   exact (§9.2): the tripping charge advances the attempt counter and
   `total_restarts`, its `RestartScheduled` precedes the scope's own
   failure in the emitting scope's event order, and the scheduled
   restart never spawns. *(Group clause activates with
   Part II §19: under `OneForAll` with budget N, sibling respawns forced by
   one child's failures consume the same budget — engineer failures so the
   group trips the scope budget even though no single child exceeds a
   per-child count. The origin suite pinned the opposite behavior; that
   test inverts.)*
10. **Ordered teardown is reverse-declaration-order with full per-child
    grace; escalation follows the single ladder; cancellation-before-
    escalation ordering is observable.** Three children park on their
    cancellation, report, then park on per-child release gates; interleave
    positive assertions ("third cancelled") with quiet windows ("second not
    yet"), releasing one at a time; finally assert the exited order is
    exactly reversed. Grace expiry on a stubborn cursor child: a drop-flag
    guard proves the future was aborted *and joined* before the ladder
    advanced; a wrapper that overruns the tidy beat is hard-aborted; an
    aborted ancestor cascades recursively; ordered graces sum while dynamic
    graces run concurrently.
11. **Outlines are injective over trees that differ in the declaration
    surface the outline carries (§21).** *(Activates with Part II §21.)* Build tree pairs differing in
    exactly one dimension — a default, a mailbox kind, a capacity, a
    readiness deadline, a policy — and assert the outlines differ; serde
    round-trip equality; mutated-JSON tests prove unknown fields are
    rejected and required keys missed loudly.
12. **Dynamic mutations resolve at admission with usable identity — they
    never await startup; removal is idempotent.** Await an `add_*` against
    a child whose startup is gated shut (a parked `init`, an unreleased
    `Manual` gate) and require it to resolve anyway; use the returned handle
    (snapshot subscription, removal) while the child has still never
    started; subscribe from a pre-spawn handle (initial value: the
    `Unstarted` scope snapshot / `Admitted` child state — B.4, B.6) and
    follow the same
    identity through startup. Run remove→re-add→remove-with-stale-handle for all
    three kinds; double-remove yields the single already-absent outcome.
    Drop an in-flight `add_*` future at provoked pre- and post-admission
    points (§8's fused abort-on-drop rule): afterwards either the id is
    free and the cell terminal, or the child has been removed — a
    quiet-window scan proves no identity-less child survives, and a
    subsequent same-id add succeeds. Drop an in-flight split `define`
    future at the same two points (§8's detach rule): admission
    proceeds, and the child survives, observable and removable through
    the slot's pre-taken handles — or, on a rejected define, the cell
    terminalizes. Stage-exactness (§8's stage rule): `reserve_*`/`add_*`
    against a pre-spawn handle, a draining scope, and a restart window
    each fail `NotAdmitting`; `remove` during drain resolves
    `AlreadyAbsent`; a same-id add while the incumbent is mid-removal
    is the distinct `RemovalInProgress` rejection and succeeds after
    awaiting the removal (§11, B.8). Race a queued split `define`
    against removal of its own reservation: the remove-by-id lands
    first, the define resolves `NotAdmitting` with the
    reservation-ended cause (B.8) while the scope keeps admitting
    other children, and a subsequent same-id reserve succeeds.
    Tombstone occupancy (§8's
    retention semantics): a
    retained terminal child blocks a same-id add with `DuplicateId`
    until removed; `remove` on the tombstone resolves `Removed`, fires
    the `Removed` event, and frees the id for a successful re-add;
    `child(id)` on the tombstone returns the terminal snapshot. A scope
    membership terminalized with no incarnation ever spawned publishes
    the final `Stopped { reason: NeverStarted }` snapshot and terminal
    event, and `wait_stopped()` resolves with it (§3.2, B.6). `remove`
    detaches from its future (§11's remove rule): drop an in-flight
    remove future — never-polled included — and assert the removal
    still completes, `membership_status: Removing` having flipped
    synchronously at the call; concurrent removes resolve one shared
    outcome. A pre-spawn `shutdown_and_wait` arms the pending stop
    latch, its timeout arming only at teardown start (B.9). §10's
    pending-incarnation stop latch: a stop request landing in a
    restart window stops the next incarnation, while a latch consumed
    by a previous incarnation never carries forward — no stop/restart
    storm under `Always`.
13. **No Tokio or runtime-adapter types are reachable from public façade
    items.** Tokio and `fastrand` integration is confined to
    `shelterwood-runtime`; `shelterwood-core` has neither dependency, so no
    core item can name one. The mailbox state machines still reach the adapter
    for the clock, sleeps, and detached disposal, so their contribution to the
    public surface is checked rather than implied. The state and projection
    layer lives in `shelterwood-cells`, structurally below the mutable driver;
    its supported observation and cancellation types are re-exported by the
    façade. Cross-crate implementation seams are necessarily `pub` in that
    implementation crate (and hidden from its generated API), while helpers
    that do not cross the boundary retain narrower visibility. CI's
    rustdoc-JSON walk rejects public reachability of `shelterwood_runtime`,
    `tokio`, `tokio_util`, or `fastrand`, and runs once per crate that
    contributes public façade items — cross-crate re-exports are absent from
    the façade's own document, so a single walk over it would not see them.
    The restart-stable cell seam is `#[doc(hidden)]` and therefore outside
    that walk, which sees only documented items; its signatures name runtime
    watch channels and latches by construction. What holds it out of the
    public surface is the façade's `pub(crate)` shim over it, under which a
    public re-export is a compile error rather than a check failure. The
    former regex/awk source-path checks and their fixtures are retired by the
    crate split; removing them does not relax either architectural
    requirement.
    The boundary now compiler-enforces the state/projection direction:
    `shelterwood-cells` depends only on core, mailbox, and runtime substrate,
    while the façade's `driver`, `tree`, and definition machinery depend on
    it. The definition layer deliberately remains convention-only: `plan`
    stays in the façade because it consumes `definition`, `raw`, and `task`;
    extracting it would require a separate `shelterwood-plan` boundary and is
    not justified by the narrower residue. The former driver-path no-downcast
    source rule is retired separately. Exit classification now lives in core
    behind typed verdicts, and the property that matters at the remaining
    façade boundary — blanket user error conversion cannot mint an
    authenticated structured payload — is pinned by its conformance probe.
14. **Event-woken observers see consistent-or-newer snapshots.** Subscribe
   to lifecycle events; *synchronously inside the event arm*, read the
   snapshot and assert it already reflects the event — at both ends of the
   lifecycle (first start, final stop). Any staging where events lead
   snapshots fails immediately. Run the fixture with **zero snapshot
   subscribers** too: the no-subscriber publication skip (§1) must be
   invisible to the pull path — `snapshot()` read after a lifecycle
   event reflects it (§12's on-demand rule).
15. **Draining-stage contexts cannot silently drop deferred work.** An
    actor stops itself, asserts it observes the draining stage on the next
    delivery, and attempts `continue_with` / a self-timer / an offload
    there: each is either unrepresentable (type-level) or returns
    `Rejected` — assert the result, and assert the work provably did not
    run. External intake freezes at stop (§5.1's close point): after the
    freeze `try_send` fails fast (`NotRunning`) while an ordinary `send`
    parks per its restart-transparent contract — it can deliver only to a
    later incarnation or fail `Terminated` at terminality, never to this
    one — and, the drain completing cooperatively (no handler failure or
    abort — §5.2's truncation qualifier), the handled log equals exactly
    the accepted prefix (§5.2's freeze rule: under `latest()`, the
    post-conflation surviving sequence — run the fixture under both
    mailbox kinds).
16. **Natural completion follows §11's rules exactly.** An ordered scope
    of one-shot tasks finishes when the last membership terminalizes:
    assert the cascade — child `Completed` exits, `Stopped { reason:
    Finished }` at each level, root `wait()` resolving `Finished` — with
    no shutdown ever requested. Negative halves under quiet windows: a
    zero-children root, a dynamic scope whose every member has
    terminalized, and an ordered scope holding one `Always`-policy child
    all stay alive until the owner acts; a retained terminal sibling
    does not block completion.
17. **Every pending completion resolves under driver death.** The
    provocation is fault-shaped: provoke a hard abort of an ancestor
    (grace-expiry escalation, `Shutdown::Abort`, forced shutdown) with
    each obligation class provably outstanding — a stubborn descendant
    holding exit-waiters and a parked send, an admission first-polled
    but not yet dequeued, a removal latched mid-flight, a lifecycle
    subscriber holding the stream, a `shutdown_and_wait` parked on a
    restart-window membership — and assert, bounded, that every one
    resolves: exit-awaiting surfaces yield `Aborted`, `try_send` fails
    `Terminated` (never a permanent `NotRunning`), parked sends resolve
    `Terminated`, add/remove futures yield their enumerated rejections,
    the stream pairs every `Added` with `Exited`/`Removed` before the
    final scope event, and no snapshot of a stopped scope carries a live
    incarnation (§1's owned-completion constraint, §10's driver-death
    rule). Where a fault-injection harness can destroy the driver at
    arbitrary await points, run the same assertions there; the fixed
    provocations above are the floor, not the ceiling.
18. **User code never runs inside a framework critical section (§1's lock
    rule).** The provocation is a hostile implementation of each seam the
    framework invokes without scheduling it, driven through the path that
    would run it under a lock. A waker whose `wake`/`clone`/`drop` panics
    or re-enters the framework: park a send and complete it, cancel a
    parked send during an unwind, and wake a snapshot or lifecycle
    subscription from a publication — each must observe a released lock
    (re-entering `snapshot()` from the waker succeeds; the mutex is
    unpoisoned afterwards). A message, actor-state, or exit payload whose
    destructor blocks or panics: displace it by conflation, recover it by
    withdrawal, retire it by terminalization, and supersede it by
    publication — every one must be destroyed after the guard, and the
    blocking kind must reach isolated disposal rather than any framework
    thread. The gate-held probe is the direct oracle: a payload
    destructor that asks whether the observation gate is held must answer
    no, on every path that retires one.

## 14. Core-spike decisions

Both questions are resolved.

### 14.1 `init`/`Args` threading through the raw-actor layer

Resolved in M3.

   The public `Handler<A>` wrapper and same-message context re-entry design
   described in §4.3 passed the shard-store and assistant-control-plane
   executable spikes, including a raw decorator that awaits before delegation.

### 14.2 Engine event arbitration

Resolved in M1. The transition rule is a stable total order. Every pending
item derives its class from its variant; callers cannot provide a conflicting
class. Within one wake, the driver first collects all currently eligible
inputs, stable-sorts them by this table, then reduces them one by one before
flushing effects:

| rank | class | transition obligation | checked evidence |
|---:|---|---|---|
| 1 | scope shutdown | enter/upgrade drain before any child policy decision | `engine::arbitration_order_is_explicit_and_stable`, `driver::events::blocking_primary_wake_recollects_control_removal_before_arbitration` |
| 2 | membership removal | mark the child `Removing` before exit/readiness dispatch | same arbitration test; `supervisor::sampled_removal_suppresses_start_effects_until_commit` |
| 3 | child exit | record and route terminal outcome before readiness/deadline artifacts | arbitration test; E1–E5 |
| 4 | readiness signal | accept an already-fired signal at the exact deadline | `engine::readiness_configuration_and_signal_deadline_race_are_engine_owned` |
| 5 | readiness deadline | terminalize only if readiness did not win | same readiness test |
| 6 | backoff due | spawn only after newly observed terminal facts | arbitration test; R5 |
| 7 | stop deadline | advance only ladders not completed/disarmed above | arbitration test; S1 |
| 8 | queued admission | reject after every same-wake terminal fact is applied | arbitration test; `driver::events::queued_admissions_yield_to_shutdown_without_forwarder_tasks` |

No arbitration row is flagged unverified. When one driver wake makes
   several events eligible, the engine processes them in this order:
   scope shutdown, membership removal, child exit, readiness signal,
   readiness deadline, backoff-due restart, stop-ladder deadline, queued
   admission. Items in one class retain their stable source order. Scope
   shutdown precedes
   removal because teardown owns all stops once it begins; both precede
   child exits, so an already-observed stop suppresses restart scheduling
   and its intensity charge. A readiness signal precedes its deadline, so
   ready-at-deadline wins. Child exits precede both, making an incarnation
   that has already ended in the same wake an exit rather than a spurious
   readiness edge. Exit handling nevertheless consults the incarnation's
   retained readiness latch: a signal causally fired before that exit is
   accounted before the exit is classified, without reordering the event
   classes themselves. Backoff work follows all newly observed terminal facts,
   and ladder deadlines follow because the earlier facts can complete or
   disarm them. Queued admissions run after all already-observed terminal and
   temporal facts, so they cannot enter a scope that the same wake has made
   non-admitting. The decision layer represents this as an ordered class and
   pins the complete table without runtime selection; each pending item
   derives its class through `Pending::class()` rather than accepting a class
   beside it at collection sites, and the driver drains all currently eligible
   inputs into that table before applying effects. Class position is not,
   however, what protects a leaving membership from a spurious readiness edge:
   a child's self-stop shares membership removal's class, so a queued removal
   cannot be ordered ahead of the readiness that self-stop dispatch replays.
   That rule is structural instead — the readiness publication site consults
   the membership's removal mark at execution time (§6, §7), the same
   discipline exit dispatch follows for restart suppression.

(Resolved elsewhere: stage-generic `handle` is rejected — drain-stage
rejection stays value-level, §5.4; the conflating-mailbox control lane is
documentation-guided and evidence-gated, Part II §16; the transactional
handoff helper is non-core, §24; the origin's `type Args = ()` ceremony
question is resolved — accept the boilerplate, §4.1.)

---

# Part II — Core plus

Specified now, built after core ships. Each section is individually
adoptable and names the hook core already carries for it. Order within this
part is a suggested sequence, not a dependency chain, except where stated.

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

---

# Part III — Non-core

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

---

## Appendix A. Normative defaults and bounds

Library-level fallbacks, applying only where neither the declaration nor an
enclosing scope decided (§9.3). "Default" means a conforming implementation
ships these values; each is overridable at the documented level. Rows
marked *(II)* ship with the named Part II feature.

| Concern | Default | Notes |
|---|---|---|
| Actor mailbox kind | **`queue`** | The default when neither the declaration nor a scope default names a kind (§9.3); kind and capacity travel together |
| Bounded mailbox capacity | **64** messages | Scope-overridable; per-child overridable; zero rejected at construction |
| `latest()` slot | **1** | Structural, not configurable |
| `latest_by_key` capacity *(II §16)* | defers to scope/library mailbox default | Full key set evicts oldest key |
| Mailbox shutdown policy | **Drain** | Two variants: `Drain` delivers the frozen prefix, `Discard` drops it (destruction venue per §5.1; disposal faults per §7); the intake freeze is unconditional either way (§5.2, §10) |
| Child shutdown policy | **`Graceful { grace: NonZeroDuration(5 s) }`** | construct with `Shutdown::graceful`; zero is rejected; `Abort` is the sole immediate-escalation policy |
| Tidy-abort beat | **`grace / 10`, clamped to [1 ms, 10 ms]** | §10 |
| Restart condition | **`OnFailure`** | Failure = any non-`Completed` exit (§7) |
| Backoff | **none** (immediate restart) | Exponential: `base × factor^(n−1)` clamped to `max`; `factor` a validated-finite newtype `≥ 1.0` with bit-`Eq` (§9.2); nanosecond rounding per §9.2; equal jitter uniform in `[d/2, d]`; all durations non-zero, validated at construction, with the fixed and exponential payloads sealed behind their constructors (§1); attempt origin/reset per §9.2 |
| Scope intensity | **5 restarts within 30 s** | Trips on the restart *exceeding* the budget; every respawn charges it (§9.2) |
| Readiness (blanket `Actor`) | **`AfterInit`** | Raw actors and tasks default `Immediate`; subtree readiness is structural (§6) |
| Readiness deadline (gated modes) | **30 s** | Resolution: declaration → scope default → this; unbounded only via explicit opt-in (§6) |
| Terminal-membership retention | **retain** (restartable) / **remove** (one-shot) | One name, one polarity, stated once (§8) |
| Monitor per-watch queue *(II §18)* | **128** events, minimum 2 | Drop-oldest; coalesced leading `Lagged`; terminal `Removed` never dropped |
| Lifecycle event buffer | **128** events, minimum 2 | Same overflow shape; per subscriber |
| Unified event lane | **unbounded; capped per-wake drain** | Requests are small; insertion payloads remain in producer-owned reservations. The batch cap bounds driver monopolization, not channel memory. Shutdown and `remove` ride level latches (§10, §11) |
| Snapshot channel | conflating watch, capacity 1 | Structural |
| `call` / `send_timeout` deadline | **none — always explicit** | One `DeadlineBudget` per call (§5.1); zero selects the no-attempt behavior |
| Identity counters | `u64`, saturating | Fail-closed overflow, decided once in the fencing primitive (§3.1); lifecycle `seq`/`lifecycle_seq` mint through the same primitive (B.4's exhaustion rule) |
| Unrepresentable deadline | **never arrives** | `Instant + Duration` overflow or an exact point the runtime cannot arm produces no deadline; it MUST NOT substitute the budget's start or any other instant |

---

## Appendix B. Surface reference

Shapes here are normative in *content* (which operations/fields/variants
exist, with which semantics); exact names may vary if the documentation maps
them clearly.

**Exhaustiveness decision.** Pre-release, public state, outcome, error, and
reason inventories are exhaustive so adding semantics breaks every match at
compile time. `#[non_exhaustive]` remains only where the specification already
names a concrete additive axis: `Mailbox` (§16), `Strategy` (§19),
`LifecycleEventKind` (§20), and `PolicyError` (future sealed policy payloads).
This decision covers every public type; release tagging may deliberately
revisit it, never inherit it accidentally.

One cross-cutting shape rule: where a surface takes a deadline budget over
other arguments, the deadline is the trailing parameter. Every such parameter
accepts `impl Into<DeadlineBudget>`, so a plain `Duration` reads naturally at
the call site while the semantics below have exactly one name; its clock origin
follows the operation family rather than the representation. Mailbox futures and `wait_for_child` capture it on
first poll. `offload`/`offload_scoped` return no future, so they start it when
the actor loop registers the offload at the call (§5.5). The shutdown family
uses the value as an escalation budget: `System::shutdown` and
`ScopeRef::shutdown_and_wait` arm it only when the targeted incarnation enters
drain, while `start_or_shutdown` does not spend it during startup and arms it
only if rollback reaches that same drain edge (B.9;
`integration::pre_spawn_shutdown_waits_for_teardown_to_exist`,
`driver::observation::hard_aborted_incarnation_fences_shutdown_and_wait_without_arming_its_budget`,
`integration::start_or_shutdown_rollback_timeout_preserves_the_startup_cause_and_stragglers`).
`DeadlineBudget` permits zero and is the single home for the following
exhaustive zero-width semantics; each API selects one behavior in its
implementation rather than interpreting a bare duration locally:

| behavior | APIs | zero-width transition | checked evidence |
|---|---|---|---|
| no attempt | `send_timeout`, `call`, `ReplyReceiver::recv`, `offload`, `offload_scoped` | do not submit/poll work or observe completion; return/deliver the timeout result | `mailbox::deadline::a_zero_budget_short_circuits_without_polling_the_operation`, `integration::zero_deadlines_short_circuit_without_acceptance_or_message_construction`, `integration::reply_receiver_reports_drop_and_is_safe_to_abandon`, `integration::zero_budget_offload_never_polls_work_and_times_out_on_actor_task` |
| poll once | `wait_for_child` | evaluate the current snapshot once with precedence match → terminal scope → timeout; never await | `integration::zero_duration_wait_observes_an_already_satisfied_child`, `integration::wait_for_child_handles_later_ids_terminal_children_timeouts_and_scope_termination` |
| immediate escalation | `System::shutdown`, `ScopeRef::shutdown_and_wait`, `start_or_shutdown` rollback | request cooperative cancellation, skip only the cooperative wait, then run the ordinary abort tail | `integration::zero_shutdown_reports_the_live_child_but_detaches_blocking_factory_disposal` (the discriminating pin: a child that *could* settle on the skipped poll is still reported as a straggler), `integration::zero_timeout_reports_recursive_straggler_paths_and_joins_them`, `integration::start_or_shutdown_rollback_timeout_preserves_the_startup_cause_and_stragglers` |

For a no-attempt offload, whose timeout outcome is a delivery rather than a
call failure, the work future is never polled and the total continuation
receives `DeadlineElapsed` through the normal completion path (§5.5).

Expiry boundary, uniform: completion observed at exactly the deadline
instant counts as within budget (§6's ready-at-deadline rule is this rule
applied to readiness). For the accepting flavors — `send_timeout` and
`call`, and equally for cancellation by dropping their futures (§5.1:
expiry and cancellation share one withdrawal mechanism) — the acceptance
side of the boundary is decided structurally rather than by clock
comparison: at expiry the caller *withdraws* the in-flight message, and
the not-accepted outcome (`TimedOut` for `send_timeout`,
`AcceptanceTimedOut` for `call`) is reported only once withdrawal has
succeeded — the message provably never was and never will be accepted —
while a message that won the race into the mailbox is accepted even when
acceptance and expiry were simultaneous: the send resolves with the
accepting incarnation; the call proceeds to its response wait and, if
the budget is spent, reports `ResponseTimedOut`. That is what makes
guaranteed-not-accepted (§3.3 step 4) exact rather than probabilistic.

Public time representation, pinned once: absolute points on the public
surface — B.6's `restart_at`, B.5's `ReadinessTimedOut { deadline }` —
are `std::time::Instant`; spans and budgets are `std::time::Duration`.
No runtime time type is public (the retained rustdoc reachability gate checks
this): `shelterwood-runtime` converts at the private façade boundary, and
under virtual time its clock mints the instants — still `std` values,
mutually coherent, which is all any contract here compares.

Rows marked *(II)* ship with the named Part II feature.

### B.1 Capability matrix — actor-side contexts

Stages: **Raw** = `RawContext<M>` (raw loop, coextensive with one
incarnation); **Live** = `Context<'_, A>` in ordinary `handle`; **Drain** =
the same series during shutdown drain; **Stop** = `StopContext<'_, A>` in
`on_stop`. ✓ = available; **R** = MUST be `Rejected`-or-absent (§5.4);
— = absent from the stage's type.

| Operation | Raw | Live | Drain | Stop |
|---|---|---|---|---|
| `id()`, `incarnation()`, `myself()`, `scope()`, `shutdown_token()` | ✓ | ✓ | ✓ | ✓ |
| `request_scope_shutdown()` (fire-and-forget; awaiting your own scope's shutdown deadlocks — documented) | ✓ | ✓ | ✓ | ✓ |
| `run_blocking(f)` | ✓ | ✓ | ✓ | ✓ |
| `recv()` / `try_recv()` (the merged event source: mailbox, offload completions, fired timers, queued continuations, §5.2 priority; `recv` yields `None` on stop request, biased; `try_recv` ignores the stop token — the drain primitive for raw loops: under `Drain`, exhaust the frozen prefix via `try_recv` after `recv` yields `None`, §10's raw-loop obligation) | ✓ | — | — | — |
| `mailbox_shutdown()` (the resolved §10 policy for this actor's mailbox — what a raw loop consults to honor `Drain` vs `Discard`) | ✓ | — | — | — |
| `mark_ready()` (one-shot effect by construction; meaningful only under gated readiness, else a documented no-op — B.2's rule, uniformly; during drain always the no-op) | ✓ | ✓ | ✓ | — |
| `stop()` (clean self-stop; `Err` outcome wins; idempotent — during drain the already-stopping no-op; arms the child's configured §10 ladder as the stop bound. Live/Drain: effective after the current callback. Raw: freezes intake at the call — drain the frozen prefix via `try_recv` after `recv` yields `None`; §1 principle 5's public primitive for the blanket loop's `stop()`) | ✓ | ✓ | ✓ | — |
| `is_draining()` | — | ✓ | ✓ | — |
| `continue_with(msg)` (next-message continuation; no mailbox capacity; anti-starvation per §5.2) | ✓ | ✓ | **R** | — |
| Keyed timers: `set_timeout` / `set_interval` / `clear_timer` (§5.3) | ✓ | ✓ | **R** | — |
| `send_after_to` / `interval_to` *(II §23)* | ✓ | ✓ | **R** | — |
| `watch` / `watch_scoped` *(II §18)* | ✓ | ✓ | **R** | — |
| `offload` / `offload_scoped` (§5.5) | ✓ | ✓ | **R** | — |
| Re-entry/mapping: `for_actor` (same-`Msg`, core); `project` *(II §17)* | — | ✓ | ✓ | `for_actor` only |

`StopContext` withholds everything that queues future work for this
incarnation — there is no one left to deliver to; `myself()` is present but
documented "do not post work to yourself."

### B.2 `TaskContext`

Passed by value into each incarnation of a supervised task (§8's factory
signatures):
`id()`, `incarnation()` (the §3.3 token), `shutdown_token()` (cooperative
stop), `abort_token()` (fires at escalation — grace expiry, or
immediately under `Abort` policy; the tidy-abort beat runs after it
fires, and §10's classification rule applies: a task that yields an
outcome during the beat classifies by that outcome, while a future
destroyed by the ensuing hard abort records `Aborted { phase }`),
`mark_ready()` (one-shot by construction; no-op only where declared
readiness makes it meaningless, and that is a documented no-op, not a silent
state change — the same rule covers a stopping incarnation: once either
cooperative shutdown or escalation has begun, readiness can no longer be
published and the call is likewise a documented no-op, matching B.1's
during-drain rule for the actor contexts).

### B.3 Send/call errors

```text
SendError<M> { actor_id, incarnation_observed: Option<Incarnation>, message: M, kind }
  kind: NotRunning   — membership currently not accepting (rebind window,
                       or intake frozen at stop — §5.1); try_send only
        Full         — FIFO at capacity; try_send only (conflating mailboxes accept instead)
        Terminated   — membership terminal; the only failure `send` can return
        TimedOut     — send_timeout only; reported post-withdrawal:
                       guaranteed-not-accepted, message recovered (§5.1)
  (message recoverable; a boxed projection drops the payload but keeps id + kind)

CallError { actor_id, incarnation_observed: Option<Incarnation>, kind }
  kind: Terminated          — terminal before acceptance
        AcceptanceTimedOut  — deadline hit before acceptance: guaranteed-not-accepted, safe retry
        ResponseTimedOut    — deadline hit after acceptance: unknown outcome — reconcile (§3.3)
        ReplyDropped        — handler dropped the Reply unanswered (what conflation-away looks like)

ReplyReceiver::recv(self, deadline) → Result<T, ReplyError { Dropped | Timeout }>
  (trailing deadline covers the response wait only — acceptance evidence
   is the accompanying send's result; §5.1)
```

Success values carry identity too: `send` / `try_send` / `send_timeout`
resolve to the accepting `Incarnation`; `call` exposes the accepting
incarnation alongside `T` (§3.3).

`incarnation_observed` is pinned per kind, not best-effort — the §3.3
retry discipline consumes it:

- **Post-acceptance kinds always carry the accepting incarnation**:
  `ResponseTimedOut` and `ReplyDropped` are `Some(accepting)`,
  unconditionally — acceptance happened, and retry-after-newer (§3.3
  step 2) is measured against exactly this token.
- `Terminated` (send and call) carries the membership's **final**
  incarnation: `Some` iff any incarnation ever ran, `None` on a
  `NeverStarted` terminal.
- `try_send`'s fail-fast kinds report the instantaneous observation:
  `Full` is always `Some` (a full mailbox is a bound incarnation's);
  `NotRunning` is `Some` of the stopping incarnation at an intake
  freeze and `None` in a rebind window or pre-spawn.
- Pre-acceptance expiries (`TimedOut`, `AcceptanceTimedOut`) carry the
  **newest incarnation observed bound during the attempt**, `None` if
  none ever was — never an "accepting" incarnation, since successful
  withdrawal proved there is none.

`ReplyReceiver<T>` is an owned at-most-once value (B.10): `Send`, not
`Clone`, and `recv` is **consuming** — one receiver, one wait, per
B.10's consuming rule. Its deadline is one budget, like `call`'s:
expiry consumes the receiver and a reply arriving later is discarded,
exactly as a timed-out `call` abandons its reply (§5.1) — a longer wait
is composed by choosing a longer deadline, not by a second `recv`.
Dropping the receiver unawaited discards the value only; `Reply::send`
stays infallible either way (§5.1).

These exact error inventories are exhaustive pre-release so new behavior
cannot hide behind wildcard arms. `send` ↔ flavor mapping is normative:
`send` fails only `Terminated`; `try_send` never `TimedOut`. The
payload-recovery clause is scoped to unmapped refs: a contramapped ref
*(II §17)* always surfaces the boxed projection — the wrap consumed the
caller's payload at ingress.

### B.4 Events

**Core lifecycle events** (§12) — one stream contract:

```text
Subscription item: Event(LifecycleEvent) | Lagged { dropped }

LifecycleEvent { scope_path, scope, seq, kind }
  scope_path: child-id path from the subscribed scope to the emitting
              scope (empty = the subscribed scope itself) — subscription-
              relative, extended as events forward upward; ids are
              reusable (§3.4), so the path is a label, not a fence
  scope:      the emitting scope's own membership token (§3.2) — the
              fence scope_path cannot be: a replacement scope under a
              reused id is a different membership. The root scope is no
              scope's child, so `spawn()` mints it a root membership
              cell through the same §3.2 machinery; its token and
              sequence behave exactly like a nested scope's — one
              uniform identity for every emitting scope, root included
  seq:        LifecycleSeq from the emitting scope's single monotone sequence,
              owned by the scope's membership cell — continuous across
              subtree restart (a rebuilt incarnation continues it; only
              a replacement membership starts fresh, and `scope`
              distinguishes that). Same space as
              ScopeSnapshot.lifecycle_seq (B.6), which
              is how invariant §13.14 aligns events with snapshots
  kind: Added            { id, membership }               // membership begins: lowered or admitted —
                                                          //   never at reservation (§3.2)
        Started          { id, membership, incarnation }  // incarnation spawned
        Ready            { id, membership, incarnation }  // readiness gate released (§6)
        Exited           { id, membership, incarnation, exit }   // the §7 exit type
        RestartScheduled { id, membership,
                           attempt: RestartAttempt, delay }       // charged per §9.2
        Removed          { id, membership,
                           last_incarnation: Option<Incarnation> }  // terminal; None = never started
        ScopeState       { state }                        // the emitting scope's own B.6 state transitions
```

`Lagged` is a **subscription-level stream item, not an event**: the
events it replaces may have come from many scopes, so it can truthfully
carry no `scope_path` and no `seq` — only the per-subscriber dropped
count. One leading, coalesced marker per overflow episode (§12); the
aligned snapshot is the resync.
The retained events delivered after that leading marker may be older than
the resync snapshot. This is intentional: the post-`Lagged` watermark
protocol below discards those already-reflected events and applies only the
newer suffix.

Ordering and delivery contract:

- Per emitting scope, events are totally ordered by `seq` and gap-free per
  subscriber except across a `Lagged` marker.
- Subscribing to a scope yields its whole subtree: descendant scopes'
  events forward upward unchanged except for `scope_path` extension.
  Forwarding preserves each origin scope's order and the causal edges — a
  child's `Added → Started/Ready/… → Removed` chain is never reordered,
  and a subtree child's own `Added` precedes any event from inside it.
- Subscriptions and the snapshot watch are membership-owned, like the
  mailbox binding (§5.1): they ride the scope's own restarts, and they
  exist from cell creation (§3.2) — a subscription through a pre-spawn
  handle is well-defined, and the snapshot watch's initial value is the
  scope's `Unstarted` snapshot (B.6), so observation begins before
  admission or spawn (§13.12's pre-spawn clause). Across a
  subtree restart the stream stays open and the sequence continuous —
  the outgoing incarnation's teardown is ordinary events (descendant
  `Exited`s, runtime-added members' terminal `Removed`s, `ScopeState:
  Draining`), closed by that incarnation's own `ScopeState: Stopped
  { reason }` (§11 publishes one per incarnation — `StartupFailed`,
  `ShutdownRequested`, `IntensityTripped`, or `Finished`: a naturally
  completed ordered subtree exits `Completed` at its parent, and an
  `Always` policy restarts it), and the rebuild is ordinary
  events (`ScopeState: Starting`, fresh `Added`/`Started` under new
  descendant memberships). `Stopped` is an incarnation edge, not a
  closure signal: only membership terminality ends the stream (closure
  rule below), so every non-final `Stopped` is followed on the same
  sequence either by the next incarnation's `Starting` or by a
  strictly-higher-precedence `Stopped` for the *same* incarnation
  (B.6's stop-reason lattice — a bounded, monotone correction, never a
  repeat of an equal verdict), and the final `Stopped` is the one
  followed by neither. The positive
  terminality signals are the membership edges — the parent's `Removed`
  for this scope child, or this stream's own closure, always preceded
  by the final event. Snapshot receivers hold the last published snapshot through the
  gap; a `ChildSnapshot`'s `incarnation`/`nested` are `None` while no
  incarnation is live (B.6).
- Subscription starts at now: no history replay. Catch-up is a
  prescribed two-step protocol, not an atomic acquisition operation:
  **subscribe first, then read `snapshot()`**. §12's on-demand rule
  makes that snapshot consistent-or-newer than every event already
  delivered to the new subscription (§13.14), so the reader takes the
  snapshot as ground truth and the stream as deltas, discarding any
  event the snapshot already reflects — decidable exactly, because
  the snapshot carries a watermark for every scope it spans: each
  `ScopeSnapshot` its own `lifecycle_seq` (B.6, recursively), and
  each scope *child* additionally `scope_seq` on the containing
  `ChildSnapshot` — present through the restart window while `nested`
  is `None`, so the watermark never vanishes with the recursive
  snapshot (B.6). An event is already-reflected iff its `seq` is ≤
  the watermark of the snapshot's scope matching the event's `scope`
  token. A `scope` token absent from the snapshot splits by **causal
  introduction**: a scope the reader has since seen born — an applied
  post-watermark `Added` whose `membership` is that token (for a
  scope child, the `Added`'s membership *is* the token its events
  will carry, and the causal-order rule above guarantees the `Added`
  precedes any event from inside) — has no watermark and needs none:
  apply its events. A token neither in the snapshot nor introduced by
  an applied `Added` is stale — a membership whose teardown the
  snapshot already reflects (§13.14's consistent-or-newer guarantee
  covers every event delivered before the snapshot read) — discard
  it. The same
  protocol is the post-`Lagged` resync. The documentation MUST teach
  it in this form.
- `LifecycleSeq` exposes `get()` plus the documented `EXHAUSTED` sentinel;
  `seq`/`lifecycle_seq` mint through §3.1's one primitive: `u64`,
  saturating advance, the saturated value poisoned and never minted.
  Exhaustion — unreachable at `u64` scale, pinned per §3.1's
  decide-once rule — fails closed for observation: the scope mints no
  further events, and each subscriber accounts the unmintable
  remainder as ordinary `Lagged` drops (the marker carries no `seq`
  and needs none). `snapshot()` stays authoritative, and the
  saturated `lifecycle_seq` watermark truthfully reads "every minted
  event is reflected", so the catch-up protocol degenerates to
  snapshot-as-ground-truth exactly; closure at terminality then
  follows a final `Lagged` in place of a mintable terminal event.
- The stream ends at membership terminality, after the subscribed
  scope's final event — closure is always preceded by one (under
  sequence exhaustion, by the final `Lagged`), and per the restart
  rule above a `Stopped` alone is not closure: the final `Stopped` is
  the one no restart and no precedence upgrade follows. For a scope
  membership that never spawns
  (a declaring tree dropped unspawned, a withdrawn or rejected
  insertion, §3.2), that terminal event is
  `ScopeState { Stopped { reason: NeverStarted } }` (B.6), published at
  terminalization, then the stream closes. Per-subscriber buffering,
  overflow, and `Lagged` coalescing are §12 / Appendix A.
- Membership edges (`Added`/`Removed`) versus incarnation edges
  (`Started`/`Exited`) are what distinguish planned remove/add from crash
  restart without application-side history; cumulative counters ride
  snapshots (B.6).

**Monitor events** *(II §18)*:

```text
MonitorEvent { member_id, kind }
  kind: Started { incarnation }
        Exited  { incarnation, exit }        // the §7 exit type
        Lagged  { dropped }                  // resync point, not an edge
        Removed { last_incarnation: Option<Incarnation> }   // terminal; None = never started
```

Delivery semantics: §18 (bounded drop-oldest per watch; coalesced `Lagged`
kept at the front; `Removed` never dropped; immediate `Started` on watching
a running target; re-registration aliases).

### B.5 The exit type

One public type (§7): variants `Completed`, `Failed(error)` (the error
value, not a string), `Panicked { message: Option<String> }` (the panic
message when the payload downcasts to a string; the payload is never
retained), `ReadinessTimedOut { deadline }`,
`Aborted { phase: GracePhase }`, `NeverStarted` (membership terminal with no
incarnation ever spawned, §7); orthogonal
`Cancellation::{Observed, NotObserved}` on every exit.
`ReadinessTimedOut.deadline` is the absolute expiry instant —
`std::time::Instant` per this appendix's time rule, B.6's `restart_at`
convention — so a retained exit stays interpretable; the configured
*span* is the child's resolved `readiness_deadline` option (§8), not
this field.
Helpers:
`is_failure()` (= not `Completed`), `cancellation()`, accessors per variant,
and two named cross-variant accessors:
`intensity_trip() -> Option<&IntensityTrip>` (§9.2's structured trip
data) and `startup_failure() -> Option<&StartupFailure>` (§11's
startup-failure data, cause-bearing: a *child* cause naming the failing
child's id and exit, or a *lowering* cause naming the undefined
reserved slots' child-id paths — §11's lowering rule). They exist so
routing on "this subtree churned out / never came up" (a breaker, an
operator surface) is one compile-checked call. Both are matches on
`ExitError`'s private provenance structure (§7) — no downcast, and
non-forgeable: only the library can *authenticate* a payload into an
`ExitError`, so an imitating application error routed through the blanket
conversion yields `None`. Both payload types are public and exhaustive
pre-release, which does make the payload values themselves constructible by
an application; that is deliberate and costs nothing, because authentication
lives in the provenance structure rather than in the payload's privacy.
Adding a cause or field must update every façade match.
Scope-level shutdown-timeout errors carry the affected children as
structured data: child-id paths plus membership tokens (§7) — never
bare ids, which sibling scopes may reuse (§2).

### B.6 Snapshots and statistics

```text
ChildSnapshot   { id, membership,                       // §3 identity types
                  incarnation: Option<Incarnation>,     // the live incarnation; None when none is
                                                        //   live (Admitted, Restarting, terminal)
                  state: Admitted                       // membership created; first spawn not yet begun
                         | Starting | Running | Stopping
                         | Restarting                   // between incarnations: restart scheduled,
                                                        //   waiting out backoff (§9.2)
                         | Stopped { exit }             // terminal; the §7 exit type —
                                                        //   exit NeverStarted is the never-ran
                                                        //   terminal (§7)
                         | StartupAborted { exit },     // terminal pre-ready failure (§6)
                  last_exit: Option<Exit>,              // newest prior exit, if any incarnation has exited
                  membership_status: Active | Removing,
                  restart_count: RestartCount,          // cumulative scheduled-restart charges for
                                                        //   this membership (§9.2): incremented at
                                                        //   scheduling, never reset, the over-budget
                                                        //   scheduled-but-never-spawned charge
                                                        //   included — the counter behind B.4's
                                                        //   planned-vs-crash distinction; the
                                                        //   *resettable* backoff attempt is a
                                                        //   different number, carried by
                                                        //   RestartScheduled { attempt } events and
                                                        //   deliberately not duplicated here (the
                                                        //   conflating watch may skip states —
                                                        //   events are the history surface)
                  restart_policy, retention,
                  restart_at: Option<Instant>,          // a representable, safely schedulable backoff
                                                        //   deadline while Restarting, as an absolute
                                                        //   runtime-adapter clock instant; None outside
                                                        //   Restarting and also for an unrepresentable
                                                        //   or unschedulable requested point. The runtime
                                                        //   may wait in bounded internal timer slices,
                                                        //   but no earlier or alternative public deadline
                                                        //   is substituted: that
                                                        //   restart remains pending until removal or
                                                        //   shutdown. Render a present value relative
                                                        //   by subtracting now
                  nested: Option<ScopeSnapshot>,         // recursive for scope children; None while
                                                         //   no incarnation is live and the membership
                                                         //   is non-terminal (restart window); a
                                                         //   terminal scope child carries its final
                                                         //   ScopeSnapshot — Stopped { NeverStarted }
                                                         //   included (§3.2) — so traversal reaches
                                                         //   the terminal scope state
                  scope_seq: Option<LifecycleSeq> }      // scope children only (None otherwise): the
                                                         //   nested scope's lifecycle_seq watermark,
                                                         //   sampled at the same publication point —
                                                         //   equal to nested.lifecycle_seq whenever
                                                         //   nested is Some, and still present through
                                                         //   the restart window while nested is None,
                                                         //   so B.4's catch-up dedupe never loses the
                                                         //   emitting scope's watermark
ScopeSnapshot   { state: Unstarted                              // membership exists, no incarnation has
                                                                //   ever spawned (reserved/admitted); the
                                                                //   initial value of a pre-spawn
                                                                //   subscription (B.4, §13.12)
                         | Starting | Running                   // the B.4 ScopeState space
                         | StartupFailed                        // root only: terminal startup failure;
                                                                //   started prefix still supervised (§11)
                         | Draining
                         | Stopped { reason: Finished           // natural termination (§11 wait())
                                           | ShutdownRequested  // owner/ancestor-requested teardown
                                           | IntensityTripped   // carries §9.2's structured trip data
                                           | StartupFailed      // nested rollback complete (§11);
                                                                //   carries the startup-failure data
                                           | NeverStarted },    // membership terminal, no incarnation
                                                                //   ever spawned (§3.2): dropped-unspawned
                                                                //   tree, rejected/withdrawn insertion,
                                                                //   removal before first spawn, startup-
                                                                //   aborted ordered sibling (§6) — the
                                                                //   scope-state twin of §7's exit, an
                                                                //   invariant the stop-reason lattice
                                                                //   below preserves in either order
                  kind: Ordered | Dynamic, strategy (ordered only), intensity,
                  total_restarts: TotalRestarts,         // charges per §9.2 — group respawns count
                  lifecycle_seq: LifecycleSeq,           // aligns events with snapshots (§12)
                  children: Vec<ChildSnapshot> }         // declaration order (ordered scopes);
                                                         //   admission order (dynamic scopes) —
                                                         //   pre-admission reserved cells are
                                                         //   absent (§3.2)
                + child(id), descendant(path) traversal helpers

**Stop-reason lattice.** Several owners can independently reach a stop verdict
for one incarnation — a driver's drain epilogue, a join monitor's fallback
after that driver panicked or was cancelled, a never-started terminalization —
so a scope's published `Stopped { reason }` resolves competing verdicts by
**precedence, never by arrival order**. The total order is
`Finished < IntensityTripped < StartupFailed < ShutdownRequested <
NeverStarted`. A later verdict replaces the published reason — and emits a
corrected `ScopeState` edge, per B.4's non-final-`Stopped` rule — iff it
strictly outranks the recorded one; equal or weaker verdicts are idempotent
repeats that publish nothing. The order is severity-ascending: `Finished` is
the weakest claim, since a drain that began on natural completion says nothing
about how the teardown itself ended; `ShutdownRequested` supersedes the
structured failures, matching §11's drain-upgrade rule, which joins through
this same lattice; and `NeverStarted` is the top element because it is not a
live incarnation's verdict but the membership-terminal twin of §7's
`NeverStarted` exit, so the scope-state projection and the membership exit
agree whichever publication lands first. The consequences are that
`wait_stopped()`, the final snapshot, and the stream's last `ScopeState` event
always report the same, highest-precedence verdict, and that a root driver
that dies mid-drain reports the join monitor's `ShutdownRequested` rather than
the abandoned drain's `Finished`.

ActorStats (II §20)
                { messages_received, messages_accepted, messages_conflated,
                  messages_evicted,                      // cross-key evictions (§16)
                  message_bytes_accepted: Option<u64>, sends_rejected,
                  outstanding_offloads, mailbox_depth, mailbox_capacity }
                — per membership; per-incarnation attribution distinguishable via §3.3;
                recursive scope-wide stats resolve through typed child metadata (§20)
```

### B.7 Guards

Scheduled/owned work (scoped offloads; with Part II: cross-actor timers,
scoped watches) returns a `Guard`: consuming `cancel(self)` and
`detach(self)`; by-reference probes `is_cancelled()`, `is_finished()`,
and the awaitable `finished()`; **drop = cancel**.
For core offloads, "finished" is a completion-or-cancellation notification,
not a hard-abort join: incarnation teardown may fire it when cancellation is
requested while the task is still unwinding (§5.5).
`detach(self)` releases only the guard's cancel-on-drop; the underlying
facility's ownership rule is unchanged (offloads stay incarnation-owned,
§5.5; cross-actor timers stay sender-incarnation-owned, §23).
Exactly-once cancel is by owned construction, not an atomic claim flag:
`cancel` and `detach` consume the guard, so cancel-after-detach and
double-cancel are unrepresentable (§1 principle 3, B.10's consuming
rule) — there is no runtime "already cancelled" error arm to specify.

### B.8 Control-operation outcomes

`remove` never errors: it resolves to `Removed | AlreadyAbsent`. A
draining, stopped, or terminated scope counts as `AlreadyAbsent` (§8's
stage rule — the teardown owns every stop), a retained tombstone counts
as `Removed` (§8's retention semantics — the prune is real work: the
`Removed` event fires and the id is freed), and a
reserved-but-undefined cell counts as `Removed` too (terminalized
`NeverStarted`, §8 — the *outcome* only: never having been admitted,
it fires no `Removed` event and leaves no tombstone, §3.2's
minting/admission split) — §11's idempotency, made concrete. `remove`
futures are observation only: removal latches synchronously at the
call (§11's remove rule), so dropping the future — polled or not —
detaches, and a latched removal still completes. The owned-completion
invariant makes an internal response loss unreachable on a conforming
path. If that invariant regresses, release builds nevertheless fail
closed: admission observes `NotAdmitting(Terminal)` and a latched removal
observes `Removed`, since its route becoming terminal satisfies the
removal goal. Debug builds MAY instead assert and panic at that boundary
to expose the internal regression instead of returning an outcome. The public
`ReserveError` is exhaustive and includes `NoRuntime`: it names
the absent ambient runtime at dynamic reservation or first poll, with
the cleanup and precedence pinned in §8. Dynamic `add_*`
fails with exactly the union of its two halves (§8): `EmptyId`,
`NoRuntime`, `DuplicateId` (tombstones included), and
`RemovalInProgress` (same id mid-removal — await removal and retry) from
reserve, with `NoRuntime` also possible at first poll — plus §3.1's
enumerated identity-exhaustion rejection, unreachable in practice but
named so fail-closed has a shape; `NotAdmitting`
from either half. `NotAdmitting` is one outcome with an enumerated,
data-carried cause: the scope membership is terminal, its live
incarnation is draining, the dynamic root is parked in `StartupFailed`
(§8's stage rule — the park is the owner's decision point, not an
admission window), no incarnation is live — a pre-spawn
handle, or an ancestor restart's re-lowering window (§8's stage rule) —
or, with the scope itself still admitting, the operation's **own
reservation has ended**: the cell was terminalized before the define
reached admission (removed by id, §8's orphaned-slot rule; or annulled
by the fused drop latch). That last cause is cell-level, enumerated
distinctly so a caller can tell "the scope closed" from "my
reservation is gone".
Defines add no definition-validation errors: validation is spent eagerly
at spec construction (§9.3), on both flavors. A dynamic define still
crosses §8's admission boundary, so it can return `NoRuntime` at first
poll or `NotAdmitting`; declaration builders share the reserve id errors
(`EmptyId`, `DuplicateId`), require no runtime for reservation or define,
and their defines cannot fail (§8).

`BuildError` (spawn-time, §11) is enumerated and exhaustive pre-release:
`NoRuntime` (no ambient async runtime reachable through the private façade
over `shelterwood-runtime`) and `UnfilledReservations` (the child-id paths of
every undefined reserved slot, §8). Nothing else lives there by design:
everything decidable earlier fails at declaration (§9.3's eager validation),
and everything later is the child's ordinary supervision story — spawn is not
a third validation point. `BuildError` is spawn-only because spawn
is the only lowering with a builder caller: a lowering elsewhere that
finds unfilled reservations is the scope incarnation's startup failure
instead, carried as the startup-failure payload's lowering cause
(§11's lowering rule, B.5). The `add_*`
future resolves **at admission** and returns, per kind, the same handles the
builder forms return (§3.2): `ActorRef<M>`; `TaskRef`, plus
`OneShotTaskRef<T>` on one-shot task forms; or the subtree's `T::Ref`. Every
set contains a membership-addressed component: `ActorRef`, `TaskRef`, or
`T::Ref` exposes the membership token through `membership()`. Startup is never
awaited by the call; observe it through the returned handles (the
`wait_for_child` helper — B.9, snapshots, events). A caller
that abandons its own startup wait therefore still holds identity, and
one that *cancels the call itself* is covered by §8's drop rules — a
dropped fused `add_*` future withdraws or removes, never orphans; a
dropped split `define` future detaches, the slot's handles remaining
the caller's identity. Startup failure after admission is reported
through the child's exit, not through the add call.

### B.9 Handle surfaces

Content-normative operation inventories for the remaining public handles
(identity accessors — id, membership, incarnation where applicable — are
implied on all; error/outcome types are B.3 and B.8):

- **`System`** (owner): `scope()` (the root scope handle, typed per §11's
  dispatch), `wait_started()`, `start_or_shutdown()`, `wait()` (resolves
  with the root's terminal reason, §11),
  `shutdown(timeout)` (§11); not `Clone`, `#[must_use]`, drop = request
  graceful shutdown.
- **`ActorRef<M>`**: cheap `Clone`, membership-addressed (§2); `send` /
  `try_send` / `send_timeout` (each resolving to the accepting
  `Incarnation`) and `call` per §5.1, error and success shapes per B.3;
  `contramap` *(II §17)*, `pinned` *(II §15)*.
- **`ScopeRef`**: `snapshot()`, `subscribe_snapshots()` (conflating
  watch), `subscribe_lifecycle()` (B.4), the `wait_for_child` helper
  (contract below), `child(id)` / `descendant(path)` traversal (B.6),
  `request_shutdown()` (fire-and-forget), `shutdown_and_wait(timeout)` —
  the owner's `shutdown(timeout)` contract (§11) on a non-owning handle:
  same trailing escalation-bound timeout (Appendix B's exemption), same
  structured straggler report. Because the handle is
  membership-addressed (§2) and the §10 latch is per-incarnation, the
  call is **incarnation-targeted by construction**: the request rides
  the latch of the scope incarnation live at acceptance (a request
  landing in a restart window is held by §10's pending-incarnation
  stop latch and armed onto the next incarnation, which
  starts and immediately begins teardown), and the call resolves once
  *that incarnation* has finished its scope epilogue. On the ordinary
  teardown path that includes joining its children; §11 defines the
  recursive-join exception when an ancestor hard-aborts a framework driver.
  Under a parent `Always`
  policy (§11's nested-shutdown rule) a fresh incarnation may already
  be running at resolution — the contract is about the incarnation the
  latch stopped, deliberately not about the membership. A **pre-spawn**
  handle is the same window at the membership's start of life: no
  incarnation has ever existed, so the request arms §10's pending
  latch and waits for the first incarnation, which starts and
  immediately begins teardown. The timeout is an escalation bound on a
  live teardown and **arms only when the latch begins acting** — at that
  incarnation's *drain entry*, never at the call: pre-spawn there is
  nothing to escalate, and the call waits exactly as a parked send
  does, bounded by §3.2's no-hang rule — a membership terminalized
  with no incarnation ever spawned (tree dropped unspawned, rejected
  or withdrawn insertion) resolves the call immediately as
  already-stopped. Drain entry is the precise arming edge because the
  budget bounds the **cooperative** phase: the incarnation must first get
  the wake in which it consumes the latch, enters `Draining`, and starts
  each child's stop ladder, or a zero budget would report every child that
  cooperates on that wake — and every child sitting in a restart backoff
  window — as a straggler, which §7's report explicitly is not for. One
  consequence is normative: when an ancestor hard-aborts the incarnation
  **before** it reaches drain entry, the latch never acts and the budget
  never arms, so there is no cooperative phase to bound and no straggler
  report to make. The call then waits on that incarnation's drop epilogue
  — synchronous, awaiting nothing (§11's fallback boundary) — and resolves
  `Ok`. A caller therefore cannot use this timeout to bound its own
  return; the return is bounded by the epilogue, as it is on the ordinary
  path once the budget expires (§11's unbounded join remainder).
  Concurrent
  callers ride one latch and observe one teardown. A scope whose
  membership is already terminal resolves immediately only when its scope
  projection is `Unstarted` or `Stopped`; if parent teardown published
  terminal membership before a live incarnation's epilogue, the call still
  waits for that incarnation to finish (`Ok` — the terminal state is
  `wait_stopped()`'s and the snapshot's to report, not this call's).
  That settlement test reads membership and scope projection as two
  planes, not one atomic fact: a nested driver already inside its first
  poll when its ancestor publishes terminal membership still reaches
  `begin_incarnation`, so a wait can settle a hair before that epoch
  becomes visible. The window is sanctioned rather than closed — the
  incarnation publishes `Starting` from `begin_incarnation`, superseding
  the stale `Unstarted`/`Stopped` projection under the same observation
  gate *before* any of that incarnation's user code runs, and its epoch
  owner still publishes the final `Stopped` projection — so the settled
  call reports the state that held at its own resolution and the later
  incarnation remains `wait_stopped()`'s and the snapshot's to report.
  `wait_stopped()` is the membership-level await — the scope
  analogue of `TaskRef::wait()`: it rides restarts and resolves at
  membership terminality with the scope's terminal state
  (`Stopped { reason: NeverStarted }` for a scope membership that
  never spawned — §3.2, B.6); observing one
  incarnation's transient stop is the event stream's job (B.4
  `ScopeState`), not this helper's. `dynamic()` as the runtime
  downgrade query (§11).
- **`DynamicScopeRef`**: `as_scope() -> &ScopeRef` is the single explicit
  access path to the shared observation/control surface; there are no mirrored
  forwards and no `Deref`. Its inherent dynamic-only surface is the eight add
  entry points (§8, the raw pair included; resolving at admission, B.8),
  the `reserve_*`
  slot family (§8 — `add_*` is reserve-plus-define sugar), and `remove` — by
  exact handle (the safe primitive for planned replacement) or by id,
  both with the single idempotent outcome (B.8).
- **`TaskRef`**: cheap `Clone`, membership-addressed; a terminal-exit
  awaitable (`wait()` — rides restarts, resolves at terminality with the
  §7 exit, `NeverStarted` included).
- **`OneShotTaskRef<T>`**: owned, non-`Clone`; consuming await yielding
  `Result<T, Exit>` (§4.2); drop discards the completion value only.
- **`Reply<T>`**: consuming, infallible `send(T)` (caller gone = value
  discarded, §5.1); `channel()` split (§5.1); drop
  observed by the caller as `ReplyDropped`. **`ReplyReceiver<T>`**:
  owned, non-`Clone`, consuming `recv(deadline)` — contract in B.3.
- **Cancellation tokens** (`shutdown_token()`, `abort_token()`,
  `run_blocking`'s child token): library-owned; `is_cancelled()`,
  awaitable `cancelled()`; derivation and detach-past-abort per §5.5.
- **Snapshot receiver**: conflating watch — borrow-latest and
  changed-await operations; closes at terminality, including a declaring
  tree dropped unspawned (§3.2 — the terminal
  `Stopped { reason: NeverStarted }` snapshot is published first).

**Pinned result shapes for the wait/stop surface.** Names carry
Appendix B's latitude; the shapes and payloads are content-normative,
enumerated here exactly as `BuildError` is in B.8. Error and reason enums are
exhaustive pre-release so every semantic addition forces downstream matches to
be reconsidered.

- `wait_started(&self) -> Result<(), StartupError>` — `StartupError`
  carries the structured cause of terminal startup failure:
  `StartupFailed(StartupFailure)` (B.5 — child or lowering cause),
  `IntensityTripped(IntensityTrip)` (a trip during startup, §9.2), or
  `ShutdownRequested` (teardown requested concurrently before the tree
  came up).
- `start_or_shutdown(self, timeout) -> Result<System, StartOrShutdownError>`
  — the error pairs the original `StartupError` with the rollback
  outcome: an `Option<ShutdownTimeout>` straggler report, `None` when
  rollback completed within its timeout (§11: rollback never masks the
  startup error).
- `shutdown(self, timeout) -> Result<(), ShutdownTimeout>` — `Ok` iff
  every descendant stopped within the cooperative phase;
  `ShutdownTimeout` is §7's structured straggler report (child-id
  paths with membership tokens). The root driver is joined on return either
  way; recursive joining is subject to §11's hard-abort fallback boundary.
- `wait(self) -> StopReason` — infallible; `StopReason` is B.6's
  `Stopped { reason }` payload (`IntensityTripped` and `StartupFailed`
  carrying their structured data).
- `shutdown_and_wait(&self, timeout) -> Result<(), ShutdownTimeout>` —
  the owner's `shutdown` shapes on the non-owning handle (semantics
  above); an already-terminal scope resolves `Ok` immediately only after any
  live incarnation has finished its scope epilogue. Descendant joining is
  subject to §11's hard-abort fallback boundary.
- `wait_stopped(&self) -> StopReason` — the membership's terminal
  reason, `NeverStarted` included (§3.2).

**The `wait_for_child` contract.** One helper; "helper-class" means an
implementation MAY layer convenience wrappers over it (e.g. a
running-state or ready-state shorthand) that add no semantics of their
own:

```rust
fn wait_for_child(
    &self,
    id: impl Into<ChildId>,
    pred: impl FnMut(&ChildSnapshot) -> bool + Send,
    timeout: impl Into<DeadlineBudget>,       // trailing, per Appendix B
) -> impl Future<Output = Result<ChildSnapshot, WaitError>> + Send;
// WaitError { TimedOut, ScopeTerminated { state } } — exhaustive pre-release;
// ScopeTerminated carries the scope's terminal B.6 state
```

Semantics: the predicate is evaluated against the named child's
snapshot within the scope's current snapshot, then against each
subsequently published one; the future resolves with the first
**matching** `ChildSnapshot`. The watch conflates, so intermediate
states can be skipped — the predicate MUST be written to accept any
state at-or-past the awaited edge (§3.3's ordering discipline: state
predicates and `supersedes`, never equality with an expected next
state), and the documentation teaches this. An id with no resident
membership simply does not match yet; a later `Added` under that id can
satisfy the wait — ids are labels (§2), so callers needing exactness
pin the membership token from the returned snapshot. A child snapshot
in a terminal state is not an error: the predicate sees it and decides
(retained tombstones included, §8). The predicate runs on the
observation path: it MUST be cheap and non-blocking, and it is a plain
`FnMut` — no `Sync` needed, it is not shared. Errors: `TimedOut` per
Appendix B's deadline rules (zero evaluates the current snapshot exactly once
with match → terminal scope → timeout precedence; a match observed exactly at
the deadline wins); `ScopeTerminated` when the subscribed
scope's membership terminalizes before a match, carrying its terminal
state.

### B.10 Trait and concurrency matrix

Uniform bounds, stated once; a conforming implementation provides at
least these. Policy/config data additionally follows §1's plain-data rule
(`Clone`, `Eq`, `Copy` where cheap).

- **Identity tokens** — `Membership`, `Incarnation`: `Copy`, `Eq`,
  `Hash`, `Send`, `Sync`. Ordering is `supersedes` (§3.1–§3.3),
  deliberately not `Ord`: membership comparison across owning scopes or
  child ids, and incarnation comparison across memberships, has no meaning
  and fails closed.
- **Non-owning handles** — `ActorRef<M>`, `TaskRef`, `ScopeRef`,
  `DynamicScopeRef`, snapshot receivers, lifecycle subscriptions:
  `Send + Sync`; the refs additionally cheap `Clone` with `Eq` + `Hash`
  **by slot identity** — two handles to one slot (equivalently, at any
  instant, to one membership) compare equal and collide as map keys,
  which is what makes userland registries and routing tables ordinary
  code. Slot identity is what stays fixed when lowering rebases a
  rebuilt declaration's membership (§3.4): the token read through the
  handle refreshes; the handle's map identity MUST NOT change.
- **Cancellation tokens** (B.9 — `shutdown_token()`, `abort_token()`,
  `run_blocking`'s child token): `Clone + Send + Sync` — they are held
  across awaits inside `Send`-declared callback futures (§4.1) and
  handed into blocking closures (§5.5).
- **Owned, at-most-once values** — `System`, slots, `OneShotTaskRef<T>`,
  `Reply<T>`, `ReplyReceiver<T>`, `Guard`: `Send`, not `Clone`; `Sync`
  is not promised (they are moved, not shared). `System` is
  `#[must_use]`. (`mark_ready()` is deliberately not here: its
  at-most-once is the *effect*, owned internally — the public call is
  repeatable with the documented no-op, B.1/B.2.)
- **Returned futures**: every future returned by a public operation is
  `Send`; none is required to be `Sync`. Whether a send/call future
  borrows its handle or owns a clone is implementation latitude; the
  `Send` bound is not.
- **Closures accepted by the API**: stated at each site (§5.5, §8, §17,
  B.9) — ingress-path closures that run at concurrent call sites and shared
  restartable construction sources carry `Fn + Send + Sync`. Their invocation
  still occurs inside one incarnation (§4.2); `Sync` is required because the
  retained source itself is shared. Equivalently, `ActorDef::cloned` requires
  `Args: Clone + Sync`. Observation predicates that run from one place at a
  time carry `FnMut + Send` without `Sync`.
- **Exactly-once operations are consuming.** Where ownership enforces
  at-most-once (§1 principle 3), the method takes `self`:
  `Guard::cancel`/`detach` (B.7), `Reply::send`, `ReplyReceiver::recv`
  (B.3), the `OneShotTaskRef`
  await, slot `define`, `System::shutdown` / `start_or_shutdown` /
  `wait`. Probes and observers take `&self`. A conforming surface MUST
  NOT re-shape a consuming operation as `&self` plus a runtime
  already-used error.

---

## Appendix C. Acceptance scenarios *(informative in prose, normative in obligation)*

A conforming implementation MUST be validated against application-scale
scenarios equivalent to the following five, which are the source of most of
this spec's API-shape requirements. Port them early; they are executable
acceptance tests, not demos — every wait in them is a bounded event,
lifecycle, snapshot, or state poll, never a sleep-and-hope.

**Wave mapping:** scenarios 1, 2, and 5 validate core (with their Part II
touches — peer watches, keyed conflation, metrics — stubbed or simplified
until those land); scenarios 3 and 4 exercise Part II surface (watch, keyed
conflation, outlines, metrics) and port in full alongside it.

1. **Shard store — deliberate topology change over durable state.** An
   ordered root of a directory actor (atomic rebind registry), a dynamic
   scope of per-key-range ordered subtrees, and a single topology-writer
   router. Planned replacement runs as a userland transaction:
   mount → readiness → directory cutover → exact-handle retire, with
   idempotent operation ids, compensating cleanup, a durable abort path,
   and post-commit reconciliation. Fault injection covers pre-commit crash
   and post-commit reply loss; the script proves accepted-request
   quiescence, the crash-window fence, and reconcile-or-rollback for each
   outcome. This scenario is the reason direct admission handles (§3.2),
   incarnation tokens and the retry discipline (§3.3), idempotent/exact-handle removal
   (§11), and the replacement-membership boundary (§3.4) exist.
2. **Sidecar — task-first embedding in a host-owned process.** Four plain
   supervised tasks plus one small actor subtree as a sibling, in a process
   that owns `main`, init, and teardown. Proves ordered readiness-gated
   startup, per-child `Abort` vs `Graceful`, startup-failure reporting with
   the started prefix left running (then host-driven rollback — the
   motivation for `start_or_shutdown()`), grace-bound enforcement with
   `phase: GracePhase::AfterGrace` and cancellation-before-escalation ordering read
   from the child's own journal, and two full embed/run/stop cycles in one
   process.
3. **Trading engine — cyclic wiring, pipelining, and a restart breaker.**
   Slot-before-define declaration throughout (every ref minted from a cell
   before any factory exists — no registry, no `Option<ActorRef>`), a
   restart-budgeted venue subtree, bounded FIFO + keyed-conflation
   mailboxes side by side, pipelined `call`s, `try_send` on the urgent
   control lane under mailbox flood, peer watches for feed staleness,
   deadline-budgeted offloads around calls (the §5.5 one-budget rule), and
   a health breaker driven off the cumulative restart stream (the packaged
   restart-counter view of §20).
4. **Build farm — a finite batch application.** A dynamic scope of
   consuming one-shot workers (`FnOnce` payloads, auto-removed on terminal
   exit), a readiness-gated lease task restarted with backoff, keyed
   latest-wins progress conflation, exact-handle retirement of a wedged
   worker, completion-driven lifetime ("run until the scheduler finishes,
   then shut down", §23), outline verification, and a warm re-run over a
   fresh tree sharing durable state — proving the durable-vs-incarnation
   `Args` split (§4.1) end to end.
5. **Assistant control plane — nested dynamic scopes and staged
   shutdown.** The stress composite: ordered root over a dynamic session
   scope whose members are themselves subtrees each owning a further
   dynamic scope, plus a gateway chain ending in a readiness-gated bridge.
   Two levels of panic isolation, transport redelivery at a journal/ack
   boundary, cancellable streaming over `latest()` conflation, idle
   eviction, and a racing remount — the remove/re-add race that
   §3.4 + §11's idempotent removal must make safe. (Its `OneForAll`/
   `RestForOne` scope flavors and peer watches join with Part II §18/§19;
   the core port uses `OneForOne` scopes and lifecycle subscriptions.)

**Patterns that must remain expressible** (they composed well in the origin
and later designs must not regress them): slot-before-define wiring for
reference cycles; incarnation-owned offloads completing through the actor
loop; lifecycle forwarding, monitor events, and snapshot identity working
together; lineage/incarnation/restart counters cleanly distinguishing crash
recovery from planned remove/add; per-instance restart policy on temporary
children plus exact-handle removal; `continue_with` rehydration;
holding a `Reply` to model a pending acknowledgement; received/conflated
statistics; rebuilding a single-use `Tree` from retained host state for
re-embedding.
