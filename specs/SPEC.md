# Library specification

This document is the definitive behavioral reference for the supervision/actor
library (the project name is out of scope here; "the library" throughout). It
is written to be sufficient for a from-scratch implementation: someone holding
only the documents in this folder should be able to build a conforming library
without consulting any existing codebase, tracker, or documentation. Where
this specification and an implementation disagree, the specification wins
until it is deliberately amended.

**Structure.** The specification has three parts, one document per part:

- **Part I — Core** (§1–§16, this document): the conformance target. A
  library implementing only Part I is complete, useful, and correct on its
  own.
- **Part II — Extensions** (§17–§25, [core-plus.md](core-plus.md)):
  optional features. Each is individually adoptable, and each names the
  hook Part I already carries so that adopting it later requires no
  redesign of core.
- **Part III — Outside the library** (§26–§27,
  [non-core.md](non-core.md)): capabilities that are deliberately never
  part of the library, and the utility tier — first-party crates above
  the library that compose strictly from the public surface.

Section numbering is global across the three documents: a `§N` reference
resolves to whichever document carries that section, and the conventions
below govern all three.

**Normative language.** MUST / MUST NOT are conformance requirements. SHOULD
is a strong default that needs a recorded reason to break. MAY marks optional
surface.

**Toolchain and packaging posture.** The library builds on stable Rust; no
nightly-only feature may be load-bearing anywhere in the public contract
(several individual rules below cite this pin as their reason). The library
ships no procedural-macro crate: every contract here is expressible with
ordinary traits, types, and functions, and §4's construction design is what
removes the need for a derive.

**Appendices** are normative unless marked otherwise and span all parts,
with Part II surfaces tagged *(II)*: **A** — default values and bounds;
**B** — context, handle, error, and event surfaces; **C** — acceptance
scenarios.

## Table of contents

- [Part I — Core](#part-i--core)
  - [1. Design principles](#1-design-principles)
  - [2. Core model and vocabulary](#2-core-model-and-vocabulary)
  - [3. Identity](#3-identity)
  - [4. Construction and restart](#4-construction-and-restart)
  - [5. Mailboxes and delivery](#5-mailboxes-and-delivery)
  - [6. The actor event loop](#6-the-actor-event-loop)
  - [7. Readiness](#7-readiness)
  - [8. Exits](#8-exits)
  - [9. Child specification and options](#9-child-specification-and-options)
  - [10. Scope policy: strategy, intensity, defaults](#10-scope-policy-strategy-intensity-defaults)
  - [11. Shutdown](#11-shutdown)
  - [12. Trees, spawning, and lifetime](#12-trees-spawning-and-lifetime)
  - [13. Engine event ordering](#13-engine-event-ordering)
  - [14. Observation](#14-observation)
  - [15. Construction requirements](#15-construction-requirements)
  - [16. Conformance obligations](#16-conformance-obligations)
- [Part II — Extensions](core-plus.md) (§17–§25, own document)
- [Part III — Outside the library](non-core.md) (§26–§27, own document)
- [Appendix A. Normative defaults and bounds](#appendix-a-normative-defaults-and-bounds)
- [Appendix B. Surface reference](#appendix-b-surface-reference)
- [Appendix C. Acceptance scenarios](#appendix-c-acceptance-scenarios-informative-in-prose-normative-in-obligation)

---

# Part I — Core

## 1. Design principles

These principles generated every specific rule below. When the specification
is silent, decide by them, in order. (§15 states their companion: the
required implementation shape — layering, plain-data policies, the pure
decision core, the lock discipline, and owned completions — which constrains
*how* a conforming library is built rather than what it observably does.)

1. **The honest case is the primitive.** One-shot, consuming,
   `FnOnce`-shaped construction is the base case. Repeatability is an
   *added capability proven in the types before erasure* — never a runtime
   field asserted after the bound it should relax has been erased.
2. **One mechanism per concept.** Staleness fencing, readiness, stop
   escalation, exit classification, child identity: each exists exactly
   once, as a named primitive, and every subsystem consumes that primitive.
3. **Invariants live in ownership and types, not comments.** A rule that
   two modules must both remember is a design defect. Exactly-once effects
   are expressed by consuming owned values (drop = the fallback effect),
   not by `Mutex<Option<_>>` take-once flags. Internal synchronization may
   still use an optional payload as a non-panicking claim protocol when it
   does not re-assert a construction capability or conflict with a more
   specific ownership requirement elsewhere in this specification.
4. **Capabilities are never erased and re-asserted.** If the caller proved
   a capability statically (a dynamic scope, a restartable actor), every
   value derived from it carries that proof in its type. Runtime downgrades
   (`.dynamic().expect(...)`) exist only for genuinely dynamic queries such
   as name-based traversal.
5. **Everything the framework's own high-level layer uses is public.** The
   blanket handler loop MUST NOT rely on any capability a hand-written raw
   actor cannot reach.
6. **At-most-once delivery, no hidden buffering.** Restart and shutdown
   windows may drop messages; nothing buffers across an incarnation
   boundary. Durable or at-least-once delivery is out of core (§26).
7. **The public API is runtime-independent.** No async-runtime types are
   reachable from any public item; boundary types are library-owned.
   Internally, every runtime touchpoint routes through one private runtime
   façade (§15.1).

## 2. Core model and vocabulary

A running **system** is a **tree** of **scopes** — "system" is the vocabulary
for the running instance throughout, as distinct from the `Tree` /
`DynamicTree` declaration it is spawned from (§12) and from the **engine**,
the machinery that runs it (§15.1). A scope is one supervisor node. Each
scope owns an ordered or unordered set of **children**. A child is one of
three **kinds**, and the engine models all three as first-class typed
variants — there is no erased side channel through which a child's kind or
metadata is smuggled and recovered by downcast:

- **Actor** — a mailbox-owning message loop (§5, §6).
- **Task** — an arbitrary supervised future with a `TaskContext`.
- **Scope** — a nested supervisor (subtree).

Scope flavors:

- **Ordered** — declared membership fixed at build time; sequential,
  readiness-gated startup in declaration order; reverse-order teardown.
- **Dynamic** — runtime membership; concurrent start and stop; per-child
  fate-sharing only (strategy is structurally `OneForOne`, and a dynamic
  scope carries no strategy at all — not in its builder, its config, or
  its snapshot; §10.1).

**Child ids.** Every child has an id: a non-empty UTF-8 string, unique among
the **resident** memberships of its containing scope — live *or*
retained-terminal: a tombstone kept for observability (§9's retention
semantics) still occupies its id until pruned. Sibling scopes may reuse an
id; an id is meaningful only relative to its scope. Duplicate or empty ids
are rejected **at the point of declaration or insertion** (§10.3's eager
validation), not at spawn. Ids identify *slots for humans and traversal*;
they are not identity — identity is the membership/incarnation machinery of
§3, and an id can be reused by a later, distinct membership (§3.4).

Identity vocabulary (all three levels are distinct and all three are
values, §3):

- **Membership** — a child's slot in a scope across restarts. Created by
  declaration or dynamic insertion; ends at terminal removal.
- **Incarnation** — one run of a membership. A restart mints a new
  incarnation of the same membership.
- **Generation / lineage** — the internal coordinates of incarnation and
  membership respectively; exposed only through the identity types, never
  as bare integers (§3.1).

Ownership: exactly one owning handle (`System`) exists per spawned root;
dropping it requests graceful shutdown (`System` is not `Clone` and is
`#[must_use]`). All other handles (`ScopeRef`, `ActorRef`, `TaskRef`, …) are
non-owning, cheap, `Clone`, and address **memberships** by default — they
ride through restarts and fail only on terminality. Incarnation-pinned
addressing is an explicit refinement (Part II §17). One deliberate
exception: `OneShotTaskRef<T>` is an owned, non-`Clone` completion claim
(§4.2) — the cheap `Clone` `TaskRef` for the same child exists alongside it.

## 3. Identity

### 3.1 One fencing primitive

The implementation MUST have exactly one staleness-fencing primitive, used
everywhere the question "is this event from the current incarnation /
membership?" arises: bindings, monitors, stable channels, snapshot
publication, child-event handling, attachment and metadata publication.

- An ordered token with a `supersedes` relation and a single,
  centrally-decided overflow policy. Bare integers MUST NOT be threaded
  positionally; `(lineage, generation)` is one value, never two adjacent
  `u64`s.
- **The overflow policy is: fail closed.** Counters are `u64` and advance
  by saturation; a saturated fence rejects all subsequent comparisons
  rather than wrapping. Rationale: the two candidate policies have
  *opposite* failure modes at the limit — wrapping makes a fence **accept
  a stale value**, saturating makes it **reject everything forever** — and
  for a staleness fence, rejecting is the safe failure. The limit is
  unreachable at `u64` scale in practice; the point of deciding it here is
  that exactly one shared primitive decides it once, instead of each
  counter choosing independently.
- Saturation alone would still break equality at the limit: repeated
  minting from a saturated counter would issue the same value for distinct
  identities, and token equality promises exact identity (§3.2). So the
  primitive separates *advancing* from *minting*, and **the saturated
  value is a poisoned terminal, never a minted token**: minting requires a
  successful advance, and once the last usable value has been spent the
  counter mints no successor. What that failure means is decided per
  counter role, both structural and fail-closed: an unmintable
  **incarnation** (a restart reaching exhaustion) is simply not
  scheduled — the membership terminalizes exactly as under `Never`, its
  last published exit standing as the terminal state; an unmintable
  **membership** fails the reservation or admission with a distinct,
  enumerated exhaustion rejection (the exhaustive reserve/admission errors
  carry it, B.8). Unreachable at `u64` scale either way — the rule exists
  so identity stays exact even in the theory, and so §16.4's fail-closed
  property has no duplicate-token counterexample.
- Inside a scope's runtime, the child address is a versioned handle whose
  **resolution is the staleness check**: resolving a stale address yields
  `None`. Per-call-site ad-hoc comparison of identity-field subsets MUST
  NOT exist, and an unchecked panicking index into child state MUST NOT be
  reachable outside the one place that implements resolution.

### 3.2 Membership identity is minted before placement, for every kind

Every child — actor, task, subtree — gets a **membership cell** reserved at
declaration (or at the start of a dynamic insertion) and stamped atomically
at insertion. All handles resolve through their cell.

`Membership` is a public opaque token — the membership-level twin of §3.3's
`Incarnation` (trait matrix: B.10 — `Copy`-cheap, `Eq + Hash`,
`Send + Sync`). It appears wherever a membership is identified: events
(B.4), returned admission handles (B.8), snapshots (B.6), and §8's
structured error payloads. Both tokens are views of §3.1's one fencing
primitive; `Incarnation::membership()` projects an incarnation's owning
membership, and equality between an incarnation's projection and a held
membership token is the "same slot?" question answered exactly.
`supersedes` on `Membership` orders tokens only while one stable owning
scope retains the same child-id lineage, such as declaration reconciliation
before the prior membership terminalizes. Terminalization evicts that
lineage (§3.4), so a later remove-and-re-add is deliberately incomparable
in both directions. Different child ids and different owning scopes are
likewise incomparable and return `false` in both directions (fail closed,
§3.1's rule; never a panic). Equality is exact identity; there is
deliberately no total `Ord` — comparison outside one retained
`(scope, child-id, lineage)` domain has no meaning.

Consequences (normative):

- Handles exist **before spawn** for all kinds. Cross-wiring (actor A
  needs task B's ref; two actors reference each other) is done by
  declaring cells first — the slot-before-define pattern is uniform, not
  actor-only. The concrete public surface is §9's slot API
  (`reserve_*` / `define`).
- A cell can carry the child's configuration; there is no ordering rule of
  the form "apply options to the returned spec, not the slot".
- `remove` and lookup by handle are cell reads, not scans. There is
  exactly one "child not found" outcome, not one per handle flavor.
- Dynamic mutations resolve **at admission**: the `add_*` future's value
  is the stamped cell's exact per-kind handle set, and startup is never
  part of the call — observe startup separately through those handles
  (B.8). A caller that abandons or times out its own startup wait
  therefore already holds the exact identity needed to reconcile or
  remove — closing the unknown-outcome window without application-level
  epoch bookkeeping.
- **Minting is identity; admission is membership.** Between `reserve_*`
  and admission a dynamic cell is *identity without residency*: the id is
  claimed (`DuplicateId` against every other caller, §9), the slot's
  handles resolve through the cell, and subscriptions through them are
  live — but the cell is not yet a member of its scope. It appears in no
  snapshot (`children`, `child(id)`, `descendant(path)` do not know it),
  emits no `Added`, holds no admission-order position (B.6), and feeds no
  counters. Admission is where public membership begins: the `Added` event
  fires and the child enters `children` at its admission position, state
  `Admitted` (B.4, B.6). Consequently a cell terminalized before
  admission — dropped slot, withdrawn fused call, `NotAdmitting`
  rejection, removal by id (§9) — emits no `Removed` either (B.4's
  `Added → … → Removed` pairing is exact: both edges or neither), leaves
  no tombstone under any retention setting (§9's retention governs
  admitted members), and frees its id at terminalization; the
  terminalization bullet below still holds in full — the *cell's own*
  observers get the structured closure, the parent's event stream just
  never mentions it. (The builder flavor differs only in where the edge
  sits: a declared cell's admission is lowering at `spawn()`, so its
  `Added` fires there, while a pre-spawn declaration projection may
  already show the row — under the same membership token, minted at
  declaration, so a catch-up reader applying that `Added` to the
  already-projected row performs the identity.)
- A reserved cell whose tree is never spawned, whose spawn fails, or whose
  insertion is rejected is **terminalized**, never leaked: its handles
  resolve terminal, pre-spawn sends fail `Terminated`, and subscriptions
  close — nothing parked against it hangs. A membership that terminalizes
  with no incarnation ever spawned publishes the membership-level
  `NeverStarted` exit (§8), so exit-awaiting surfaces (`TaskRef::wait()`,
  `OneShotTaskRef<T>`) resolve with a structured outcome rather than hang.
  A *scope* membership additionally publishes its terminal scope state — a
  final `Stopped { reason: NeverStarted }` snapshot and `ScopeState` event
  (B.6, B.4) — before its streams close, so `wait_stopped()` (B.9) and
  snapshot/lifecycle subscribers resolve structurally, never merely by
  stream closure.
- When a mailbox is attached at terminal publication, publication has one
  precise internal order: store the terminal cell record, synchronously
  discharge parked mailbox operations, then pulse the cell's single change
  signal. A direct or reentrant borrow MAY therefore observe the terminal
  record while mailbox discharge is still in progress; the guarantee is
  **discharge-before-pulse**, not discharge-before-store. If terminality
  wins before attachment, it stores and pulses first; later attachment
  immediately closes and discharges the mailbox without a second terminal
  pulse. A panic collected while waking mailbox operations MUST be resumed
  only after the complete parent snapshot/lifecycle publication and nested
  observation-close transaction, so a hostile mailbox waker cannot strand
  membership waiters or skip the matching terminal observation edges.
- Declaration is O(n): no re-projection of the full child list on every
  builder mutation; no shadow runtime object maintained during
  declaration; no global counters joining side tables. Pre-spawn
  snapshots, if offered, are computed on demand from the declaration.

### 3.3 Incarnation identity is addressable

`Incarnation` is a public opaque token: `Copy`-cheap, ordered within its
membership (`a.supersedes(b)` — across memberships it is `false` both ways,
like §3.2's membership rule), comparable for equality across the API, and
projecting its owning membership (`membership()`, §3.2).

- `Context::incarnation()` returns the current incarnation. (The task-side
  `TaskContext` exposes the same.)
- Lifecycle events and snapshots identify incarnations by this token
  everywhere; no surface exposes a bare generation number.
- `ActorRef::send`/`call` results and errors expose the incarnation that
  accepted (or was observed at failure). **This lands in core** even
  though the pinning refinements are Part II: a token that is not carried
  by every result and error type from the start cannot be retrofitted
  later without breaking each of those types — the retrofit-hostile half
  of the design is exactly the half that must ship first.
- Membership addressing remains the default everywhere. Snapshot
  generation comparisons use `supersedes`/ordering, and documentation MUST
  teach ordering, not equality-with-increment (a restart storm can advance
  an incarnation by more than one between two observations).
- Incarnation-pinned sends and the await-next-incarnation helper are
  Part II (§17).

**The retry discipline these tokens exist to support** (this MUST be taught
in the request/reply documentation; it is the semantic contract for
`CallError`, Appendix B.3):

1. Retry after `ReplyDropped` **only if** the operation is idempotent, and
   only under one overall deadline for the whole logical operation.
2. Before retrying, await the incarnation-after: retry only once a *newer*
   incarnation than the one that dropped the reply is running (otherwise
   the retry lands in the same doomed mailbox or the rebind window). In
   core, observe this via lifecycle events or snapshots; §17's awaitable
   is the packaged form.
3. **Never blindly retry `ResponseTimedOut`** — acceptance happened, the
   outcome is unknown; reconcile against durable evidence (e.g. the
   prepared image) instead of resending.
4. `AcceptanceTimedOut` is guaranteed-not-accepted and always safe to
   retry.
5. Retry horizons and effect-ledger retention are application-side bounds
   by design: the library does not prescribe durability, and applications
   MUST bound or garbage-collect idempotency ledgers once retries become
   impossible.

Part II packages the mechanical steps — §17's `call_idempotent` encodes 1,
2, and 4; steps 3 and 5 remain application obligations under any surface.
This discipline stays specified here regardless: it is the semantic
contract behind `CallError` that core's documentation MUST teach, and core
is complete without §17.

### 3.4 Replacement memberships

An `ActorRef` follows incarnations of **its** membership, never a same-id
replacement membership. This boundary is kept (it is what makes identity
exact), but it MUST be discoverable: removal-then-re-add under the same id
yields a fresh membership whose handles come from the new insertion, and
the old handles report terminal. Terminalization evicts the retained
child-id lineage: the replacement and removed membership are deliberately
incomparable in both directions. The same fail-closed rule applies to an
initially declared child and its later runtime replacement, and to
corresponding descendants rebuilt across incarnations of one nested scope
membership. A stable scope may order a provisional declaration only while
it still retains the same live lineage; a temporary builder never defines
the ordering domain. Different ids and different owning scope memberships
remain incomparable. A small routing/registry adapter for planned handoff
(a `ServiceRef`/route-cell that the application repoints at cutover) is out
of core, packaged in the utility tier (§27) — it must not weaken exact
membership identity.

## 4. Construction and restart

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

- **Callback futures are `Send` by declaration.** The trait methods use
  the desugared `-> impl Future + Send` form (likewise `RawActor::run`,
  §4.3) because the single incarnation runner (§8) is generic over the
  actor type and hands the incarnation future to the runtime's
  multithreaded spawn through the private runtime façade — under bare
  `async fn` sugar a generic `A`'s callback futures carry no `Send` bound
  and no such runner compiles. Implementors still write plain `async fn`:
  an ordinary implementation's future is auto-`Send` and satisfies the
  bound; one that holds a `!Send` value across an await fails at its own
  impl site with a targeted error, not at a distant spawn. Runner-side
  `Send` where-clauses (return-type notation) are nightly-only, which the
  stable-toolchain pin rules out — the same reasoning as
  `associated_type_defaults` below. The asymmetry decides the default:
  relaxing `+ Send` later is non-breaking for implementors, adding it
  later breaks them. `!Send`/thread-per-core execution is Part II §24's
  seam, not a core concern.
- Construction and startup are one thing: `init` consumes fresh `Args` and
  produces the actor. There is no separate factory `build` / `on_start`
  split, and no factory derive. (Consequence for packaging: no derive
  proc-macro crate exists at all — the front matter's packaging posture.)
- Durable-vs-incarnation-local state is expressed by what `Args` carries
  (e.g. an `Arc<AtomicU64>` in `Args` survives restarts because cloning
  args clones the handle) versus what `init` constructs.
- **`type Args = ();` ceremony for trivial actors is accepted as-is** —
  one line per trivial actor, no convenience subtrait. Blanket-impl
  cleverness to remove it creates coherence (E0119) risk out of proportion
  to one line of boilerplate; a true trait-side default
  (`type Args = ();`) is nightly-only (`associated_type_defaults`), which
  the stable-toolchain pin rules out. Revisit only if it demonstrably
  grates — and note that if the feature stabilizes, adding the default
  later is non-breaking (existing impls naming `Args` stay valid), so
  deferring costs nothing.
- `ExitResult` is plain `Result<(), ExitError>` — the exact contract of
  the handler, raw, and task layers (`Ok` classifies `Completed`, `Err`
  classifies `Failed`; §8). Infallible handlers write `Ok(())`; there is
  no `IntoExitResult` conversion trait. There is deliberately no
  stop-outcome return type: clean self-stop is `ctx.stop()` alone (B.1 —
  effective after the current callback, `Err` outcome wins, idempotent),
  so the return channel carries errors only and stop has one mechanism
  (§1 principle 2). Constraints that MUST hold:
  - The blanket loop applies its own `?` only after awaiting the
    callback's exact `ExitResult`.
  - `RawActor::run` (§4.3) stays on the exact, explicit exit contract —
    the raw layer is where explicit runtime mechanics belong.
  - Supervised **task closures keep an exact-type bound**, and the two
    task modes have two signatures: restartable tasks are bound
    `Future<Output = ExitResult>` exactly; one-shot tasks are bound
    `Future<Output = Result<T, ExitError>>` exactly, `T` being the typed
    completion value (§4.2) inferred from the closure's `Ok` arm
    (`T = ()` for a bare `async { Ok(()) }`). In both, the equality bound
    is what supplies the contextual error type that lets such closures
    compile; relaxing either to a conversion-trait output makes the
    closure ambiguous (E0282). The signatures are deliberately distinct —
    one signature cannot both return exactly `ExitResult` and carry a
    typed completion. Unit-returning task closures, if wanted, get a
    deliberately named separate entry point. The concrete closure
    shapes — `Fn`/`FnOnce`, the `TaskContext` parameter, and the exact
    future bounds — are pinned once, in §9's slot surface.
  - **Trait spelling.** The exact
    `-> impl Future<Output = ExitResult> + Send` declaration shown above
    is warning-free for ordinary plain-`async fn` implementations of both
    `Actor` and `RawActor`; a nested opaque declaration
    (`-> impl Future<Output = impl IntoExitResult> + Send`) trips
    `refining_impl_trait_internal` in warnings-denied downstream impls
    and is rejected. Explicit desugaring is never required of
    implementors. An associated output type is likewise rejected: without
    nightly-only associated-type defaults (the `Args` reasoning above) it
    adds a per-impl ceremony line to every actor to save one — a net
    loss. The real trait and its implementations are the compiler check
    for this contract; a toolchain change that revisits it is a
    deliberate specification decision.
- `init` runs inside the supervised future: an init panic or error follows
  the normal supervision path, classified as a startup failure.
- `on_stop` is best-effort teardown; it runs under the child's shutdown
  grace and its context is the narrowed `StopContext` (§6.4,
  Appendix B.1). `StopContext` exposes no `myself()`: an `ActorRef` is a
  send handle, and posting from `on_stop` is futile. Teardown that needs
  the handle itself — a map keyed by `ActorRef`, or a send — MUST capture
  it from `Context::myself()` during the live phase and carry it in actor
  state. Teardown that needs only identity captures nothing: the stop
  context still exposes `incarnation()`, and `Incarnation::membership()`
  is a process-wide unique key (§3.2, Appendix B.1). A panic in `on_stop`
  is classified `Panicked` by the fallback report token, superseding the
  run's outcome (§8).

### 4.2 One-shot is primitive; restartable is proven

Restart means: re-run `init` with freshly minted `Args`. Therefore restart
capability *is* the caller's proof that args can be re-minted, and it is
established in the construction-path types, before erasure:

- **One-shot** (the `add_*_once` family, any child kind): args are owned
  and consumed. No restart-configuration methods exist on these spec
  forms; never-restart is structure, not configuration. Terminal
  membership removal defaults on, overridable for observability. `_once`
  means one *incarnation* — args consumed at construction — not one
  *iteration*: run-once behavior needs no variant at all, since a clean
  completion under `OnFailure` is not a failure and never restarts (§8).
  The twins differ in what they can accept (owned, non-re-mintable args)
  and return (`OneShotTaskRef<T>`), not in how long the child runs.
- **Restartable** (`add_*`-family): the caller supplies an args *source* —
  `Args: Clone + Sync`, or `Fn() -> Args + Send + Sync + 'static`
  re-minted at restart time (so a restart can observe the current world:
  re-resolve an address, fresh timestamp). This is OTP's `{M, F, A}`
  translated into ownership: the args value is the per-spawn argument, and
  whether you can clone or re-mint it *is* your restart capability. For
  subtree children the args source *is* the declaration source:
  restartable `add_subtree` takes
  `impl Fn() -> T + Send + Sync + 'static` (`T: Subtree`, §12), re-invoked
  at restart so each incarnation lowers a fresh single-use tree; the
  `_once` twin consumes a tree value outright.
- The erased internal representation of one-shot construction is
  `FnOnce`-shaped all the way down: the closure owns the resource, the
  runner owns the closure, and init-panic / startup-failure /
  shutdown-before-start all reduce to dropping the owner. Within
  construction payloads, `Mutex<Option<_>>` take-once tricks MUST NOT
  appear, publicly or privately — and the same owned-token shape MUST be
  reused for construction claims throughout the lowering and incarnation
  path. This prohibition does not cover internal, non-panicking
  synchronization claims such as disposal completion, where losing the
  claim race is an ordinary no-op rather than a re-asserted construction
  capability. The independent owned-token and consuming rules for
  readiness (§7), exit reports (§8), guards (B.7), and public
  exactly-once operations (B.10) remain mandatory (§1 principle 3).
- **Every user-supplied construction source executes inside the single
  incarnation runner** (§8): the restartable forms' shared
  `Fn() -> Args + Send + Sync + 'static` factory and `Args::clone`, task
  body factories, raw-actor factories, and subtree factories with the
  lowering they trigger (§12) all run within the incarnation future they
  are constructing. A panic in any of them is that incarnation's own
  exit, classified `Panicked` by the ordinary path — never an engine
  crash, never a per-source failure model — and §16.5's drop guarantees
  hold unchanged, because the source is owned by the same future whose
  destruction is the fallback effect. Source invocation has no error
  channel of its own: `init`'s `Err` is the one structured startup
  failure; factories and `Clone` signal only by panicking.
- The start path SHOULD take ownership of the boxed `FnOnce` when
  scheduling the only incarnation, making a second invocation
  *unrepresentable*. Where the type system cannot prove it, a second
  invocation of a one-shot construction is a framework bug and **panics
  with a clear message** (caught by supervision) — a deliberate decision
  that MUST be pinned by a test. It MUST NOT degrade to a synthesized
  error future that masks the framework bug.
- All three kinds are symmetric: each has both modes, built once at the
  child-spec layer (§9 names the entry points: six kind forms plus the
  raw-actor pair), inherited by any future kind. One-shot tasks
  additionally expose a **typed completion value**: their closure is bound
  `Future<Output = Result<T, ExitError>>` exactly (§4.1), and the add
  yields — alongside the ordinary cheap `Clone` `TaskRef` — a single
  **owned, non-`Clone`** `OneShotTaskRef<T>`. Awaiting it consumes it and
  yields `Result<T, _>` with the §8 exit type as the error: `T` exists
  only for `Completed`; panic, abort, and readiness-timeout arrive as the
  same structured exit every other consumer sees, and a task that never
  completes resolves the claim with its terminal exit — including the
  membership-level `NeverStarted` exit when the child is terminalized
  before any incarnation runs (§8). Ownership is the multi-waiter answer
  (§1 principle 3): exactly one claimant per completion value, decided in
  the types — fan-out is the application's move after the claim
  (`T: Clone`, `Arc<T>`); dropping the unawaited handle discards the
  value without affecting the task.
- **Shutdown racing an in-flight `init` — decided semantics:** `init` is
  *not* cancelled. It runs to completion under the child's cooperative
  shutdown grace; grace expiry hard-aborts by dropping the whole
  incarnation future, which drops `init`'s owned `Args`.
  Shutdown-before-start drops the unscheduled owner. All four paths —
  init panic, startup failure, shutdown-before-start, normal exit — MUST
  drop one-shot resources exactly once, with a dedicated test for each
  (§16.5).

### 4.3 Raw actors

`RawActor` remains the minimal loop-owning contract beneath `Actor`:

```rust
trait RawActor: Send + 'static {
    type Msg: Send + 'static;
    // Type-level definition metadata, read before incarnation construction.
    fn readiness() -> Readiness { Readiness::Immediate }        // §7
    // Desugared per §4.1's Send-bound rule; implementors write `async fn`.
    fn run(&mut self, ctx: &mut RawContext<Self::Msg>)
        -> impl Future<Output = ExitResult> + Send;
}
```

- The high-level `Actor` runs as a raw actor through one generated receive
  loop, not a separate execution path. A literal
  `impl<A: Actor> RawActor for A` cannot exist — there is no `A` value
  before `init` produces one — so the loop lives in the public
  `Handler<A>` raw-actor wrapper, which owns the
  `Uninit(Args) → Running(A)` transition. `ActorDef` and `ActorOnceDef`
  construct that wrapper. The wrapper supplies `AfterInit` as its
  type-level readiness default; the engine resolves that default with any
  child-definition override before constructing an incarnation, and only
  an effective `AfterInit` mode performs the automatic post-init
  `mark_ready`. `Immediate` and `Manual` retain their declared meanings.
- `run` borrows the incarnation-owned `RawContext`; one raw context is
  coextensive with one incarnation. Decorators may re-enter an inner actor
  on the same context and share its readiness, stop state, timers,
  offloads, watches, and identity; the context cannot escape into work
  that outlives the run.
- The framework invokes `run` at most once on an incarnation's root
  raw-actor value and never re-enters `run` on that value. Shutdown may
  destroy a root value before its run begins; a restart that reaches
  construction obtains a fresh root value from the definition's source.
- `Handler<A>` is the public composition point that encapsulates the
  generated callback loop, including its error-path freeze-and-join
  discipline. Decorators wrap `Handler<A>` through the public raw-actor
  surface; they do not perform that discipline themselves and need no
  access to framework-internal resource operations.
- Raw actors have their own construction path — §9's `define_raw` /
  `define_once_raw` on `ActorSlot`, with fused `add_raw` / `add_raw_once`
  entry points on both scope flavors. There is no `init`/`Args` phase at
  this layer: the actor value itself is the per-incarnation input, so
  §4.2's args-source rule applies to it directly — the restartable form
  takes `impl Fn() -> R + Send + Sync + 'static`, re-invoked at each
  restart; the one-shot form consumes an owned `R`. The value therefore
  exists before `run` is called. The shared options record applies
  unchanged (mailbox settings included — honoring `mailbox_shutdown` is
  the raw loop's own obligation, §11/B.1); readiness defaults `Immediate`
  per §7.
- Raw decorators can wrap `Handler<A>` directly and may await before
  delegation without changing readiness (§7's decorator rule). Handler
  decorators use the zero-cost same-message `Context::for_actor` /
  `StopContext::for_actor` reborrow, sharing identity and
  incarnation-owned resources (§6.4).

## 5. Mailboxes and delivery

### 5.1 Mailbox kinds and capacity

Two kinds in core, no unbounded option (a third, keyed conflation, is
Part II §18 — the `Mailbox` constructor surface is non-exhaustive so adding
it is not a break):

- `queue(capacity)` — bounded FIFO with real backpressure: a full queue
  makes `send` wait, it never evicts.
- `latest()` — single conflating slot (capacity is structurally 1).

Capacity parameters MAY be omitted to defer to the scope default (§10.3's
kind-matched resolution); the library default capacity is given in
Appendix A. Zero capacity is rejected **at construction** (a non-zero type
or immediate error), not at spawn.

### 5.2 Send flavors

Delivery is at-most-once (§1 principle 6). Send flavors on `ActorRef`
(error taxonomy in Appendix B.3):

- `send` — waits; restart-transparent: parks while the membership is
  unbound (a restart window) and through FIFO backpressure; fails only on
  terminality. Cancellation is **linearized at acceptance**, exactly as
  for `call`: dropping the send future *before* acceptance withdraws the
  message (Appendix B's structural withdrawal — it provably never was and
  never will be accepted); dropping it *after* acceptance abandons only
  the wait — the message is already queued and is delivered normally, and
  at-most-once is untouched because the message was accepted exactly once.
  There is no state in which a cancelled send leaves the mailbox
  uncertain: acceptance and withdrawal race into the mailbox and exactly
  one wins. Success resolves to the accepting `Incarnation` (§3.3), not
  `()` — `try_send` and `send_timeout` likewise.
- `try_send` — fail-fast: distinct outcomes for unbound-right-now (rebind
  window), full, and terminal. The documented choice for teardown-window
  notifications (§11).
- `send_timeout` — `send` with a `DeadlineBudget`; a zero budget fails
  immediately. `TimedOut` is reported only once withdrawal has succeeded,
  so it always means guaranteed-not-accepted — the recovered message
  (B.3) is safe to re-send. The deadline tie has one explicit rule,
  Appendix B's expiry boundary: an acceptance that wins the race at the
  deadline instant resolves the send successfully; the tie is decided by
  the withdrawal race, never by clock comparison.

### 5.3 Request/reply: `call`

`call` is request/reply via a `Reply<T>` capability embedded in the
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

— so any user message type can carry the `Reply` wherever it chooses, and
the framework never needs to see inside `M`. `FnOnce` is deliberate: one
call mints one reply capability (§17's `call_idempotent` is where the
re-mintable `Fn(Reply<T>) -> M` form lives, per §4.2's capability rule).

- **One deadline** covers binding wait, mailbox acceptance, and response
  (one budget, not two hand-ordered constants); the error distinguishes
  *where* it expired: acceptance timeout (guaranteed not accepted, safe to
  retry) vs response timeout (accepted, unknown outcome — reconcile,
  don't retry; §3.3). Cancellation mirrors expiry: dropping the `call`
  future before acceptance withdraws the message (Appendix B's withdrawal
  rule); dropping it after acceptance abandons only the reply — the
  message stays accepted and is processed normally.
- `Reply<T>` is `Send + 'static` and consumed by `send(T)`, which is
  infallible: if the caller is gone (cancelled, timed out, dropped), the
  value is discarded — handlers never branch on caller liveness. Holding
  a `Reply` without responding models a pending acknowledgement; dropping
  it is observable to the caller as `ReplyDropped`.
- A successful `call` exposes the accepting incarnation alongside the
  reply value — §3.3's retry discipline needs it on success as much as on
  failure.
- `ActorRef::reply_channel()` exists as the split escape hatch when the
  reply must be awaited elsewhere: it yields the `Reply` plus a
  `ReplyReceiver<T>`, whose `recv(deadline)` (trailing deadline covering
  only the response wait — acceptance evidence is the accompanying send's
  result) resolves per B.3.
- Awaiting `call` on `myself()` inside `handle` is a guaranteed
  deadlock — the reply needs the very handler being blocked; use
  `continue_with`, or `ActorRef::reply_channel()` with an offload
  (documented hazard).
- Request/reply on a conflating mailbox is a correctness trap (a barrier
  can be conflated away). A static fence is not possible — mailbox kind is
  per-declaration configuration, invisible in `ActorRef<M>`'s type — so
  the decided semantics are: `call` is allowed, conflation-away surfaces
  as `ReplyDropped`, and the documentation teaches this next to the §3.3
  retry discipline.

### 5.4 Binding and acceptance windows

The mailbox binding is membership-owned: created at insertion, outliving
incarnations and actor destruction (§6.5). *Bound* — accepting sends — is
an incarnation property: acceptance opens when an incarnation is spawned
(so messages are accepted while `init` runs and delivered once the loop
starts) and closes at the intake freeze when the incarnation begins
stopping (§6.2 — the freeze precedes drain and `on_stop`, which is what
makes the drained log exactly the accepted prefix; under `latest()`, its
surviving slot) or, for an incarnation that ends without a stop phase
(panic, hard abort, plain return), at its exit publication (§8); outside
that window sends park (`send`) or fail fast (`try_send`, `NotRunning`).

Readiness (§7) never gates acceptance — a gated child's mailbox accepts
during its handshake, which is what lets cross-wired siblings send to a
not-yet-ready peer. (An accepted-during-`init` message dropped by a
startup failure is exactly §1 principle 6's at-most-once window, invariant
§16.3.)

**Ordering.** Within one incarnation, a queue mailbox preserves per-sender
FIFO: two sends from the same task, both accepted, are delivered in send
order. There is no ordering guarantee across senders (acceptance order is
the only order), none across incarnations (at-most-once already forbids
it), and none between mailbox messages and timer or offload deliveries
beyond §6.1's loop priority. Conflating mailboxes order by replacement:
the survivor is the newest accepted value.

### 5.5 Destruction venue

Live `latest()` displacement drops the displaced payload inline on the
displacing task, after acceptance of its replacement is visible. This is
the deliberate hot-path exception: a panicking foreign payload destructor
surfaces on that task even though the replacement remains accepted.
Framework-initiated disposal of externally submitted mailbox or
reply-bearing payloads — including mailbox teardown, timeout/withdrawal
cleanup, and accepted-prefix batch disposal — runs detached from the
initiating task with per-element panic containment. After extracting any
string diagnostic, the framework likewise destroys an opaque user panic
payload on the detached disposal lane rather than on the executor publishing
the exit. Incarnation-owned continuations, timer messages, and offload state
instead follow §6.5 and §8's incarnation teardown and verdict rules. No single
disposal-thread identity is promised.

(The synchronization discipline behind every mailbox transition — the
effects sink paired with the state guard, and the structural waker slot —
is a construction requirement, §15.4.)

## 6. The actor event loop

### 6.1 Loop priority and fairness

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

Tie order within a class is pinned, never select-arm luck (§13's principle
applied to the loop). Continuations form a FIFO queue: multiple
`continue_with` calls from one callback are all retained, in call order —
no last-wins replacement and no single-pending cap (the queue is unbounded
and consumes no mailbox capacity, B.1; discard happens only at the stop
freeze, reported on exit) — each running as a "next message" ahead of
queued mail, with the fairness interleave above applying between
successive continuations. Timers whose deadlines fire at the same instant
deliver in **arming order** — the order their *current* armings were
established; re-arming a key (§6.3's replacement) takes the new position —
and the bounded retraction turn below runs once for the whole simultaneous
batch: work captured in the batch's bounded source prefixes delivers
first, then the still-armed members of the batch in arming order. Within
class 3, ordering between mailbox and offload deliveries stays
deliberately unspecified (§5.4's ordering contract promises per-sender
FIFO and nothing more).

For this ordering rule, a timer **fires** when the event loop observes due
armings and begins taking their timer batch, not merely when the wall
clock passes the timer's deadline. Batch formation records a bounded
prefix of each input source — continuations, mailbox acceptances, and
offload completions — at that source's own cutoff. The sources do not
share a global linearization point: work arriving concurrently with batch
formation may land on either side of its source cutoff, and class 3
promises no ordering across sources.

Already-queued messages get one bounded turn to retract an elapsed timer:
when a timer fires, messages captured by the batch's mailbox prefix are
delivered first (they may `clear_timer` the fired key), then the timer
message goes through if still armed.

Timers rank last deliberately; the asymmetry is the rationale. A timer's
lateness under this order is bounded: once a timer fires, only each
source's captured prefix — queued messages (capped by mailbox capacity),
offload completions through the recorded offload watermark, and
continuations through the recorded queue length — runs before it. An
offload still in flight holds nothing back; work arriving after its
source cutoff, continuations included, does not preempt the fired timer.
The continuation clause is load-bearing — continuations are
self-replenishing, so without it a continuation chain could starve a
fired timer forever despite the per-continuation fairness turn above. The
reverse order is unbounded: timers are self-armed and recurring, so
ranking them above externally-bounded input would let a short interval on
a slow handler starve the mailbox indefinitely (the same
self-generated-work hazard the continuation carve-out above guards
against; the loop's principle is externally-bounded sources above
self-generated recurring ones, with bounded-turn exceptions protecting
each side). Deliveries-first also resolves the timeout/response race
benignly — the accepted cancelling message retracts the timer rather than
a spurious timeout firing past it, which is what makes §6.3's
retract-until-delivery guarantee worth having — and `set_interval`'s
skip-missed-ticks posture already accepts bounded lateness as the timer
contract.

### 6.2 Stop, drain, and `on_stop`

On stop (supervisor shutdown, removal, readiness-timeout teardown, or
local `ctx.stop()`), close external intake to freeze the accepted prefix;
then follow the mailbox shutdown policy (§11; handlers observe draining
state). For `Drain`, the actor loop delivers the frozen prefix before
`on_stop`. For `Discard`, freezing makes the prefix permanently
undeliverable and the actor loop returns without draining, then runs
`on_stop`; the framework extracts and schedules the prefix for §5.5's
detached, per-element disposal after the actor run returns. Physical
destruction is not ordered before `on_stop` or exit publication. A
payload-destructor panic there is a disposal fault: it MUST NOT
reclassify the actor's exit or skip `on_stop`.

The stop boundary is exact about what drains: **only the frozen accepted
mailbox prefix** — under `queue`, every accepted-but-undelivered message
at the freeze, in acceptance order; under `latest()`, the surviving slot
(at most one message: a message conflated away before the freeze was
*replaced* per §5.4's replacement ordering, and is never handled). At the
freeze, outstanding offloads are cancelled (incarnation-owned work of a
stopping incarnation — continuations suppressed per §6.5), queued
continuations are discarded and reported on exit (priority item 2 above),
and armed timers are dropped (§6.3) — so for a cooperative `Drain` that
runs to completion, "the handled log equals the accepted prefix" (§16.15)
has no asterisks beyond the one conflation already states: under `queue`
the two are identical; under `latest()` the handled log is the accepted
sequence with each conflated message replaced by its successor. The
qualifier is load-bearing: a handler that fails mid-drain (`Err` or
panic — the incarnation is failed, drain stops there) or a grace-expiry
hard abort (§11) truncates the drain, leaving the handled log a proper
prefix of the accepted one.

`on_stop` runs exactly when the stop phase is reached with a live,
non-failed actor: on every cooperative stop path above, whether or not a
drain preceded it. It does not run when `init` failed (no actor value
exists), when a handler — live or draining — returned `Err` or panicked
(the incarnation is failed; cleanup is the crash-only path, `Drop`), when
any incarnation-owned disposal panic is observed at a receive boundary
(including one entering stop or drain), or on hard abort (the future is
destroyed). Incarnation-owned disposal is §6.5's resource funnel — the
offloaded future and its continuation closure, plus the queued
continuations, armed timers and queued offload completions released at
the intake freeze; the frozen mailbox prefix's own detached disposal
above is not part of it and stays a disposal fault. A disposal panic
**already retained when a receive boundary is reached** therefore fails
the incarnation before any further delivery or `on_stop`. As with
`Guard::finished()` (§6.5) this is not a join: a panic that lands after
the last receive boundary is still the incarnation's exit, but cannot
suppress `on_stop`. Grace bounds drain plus `on_stop` together (§11) —
including after a local `ctx.stop()`, which arms the child's own
configured ladder (§11).

### 6.3 One timer facility

There is exactly one self-timer mechanism, an incarnation-owned keyed
timer table on `RawContext`, available to raw and handler actors alike,
merged into the single event source with the priority policy above.

- **Keys are values** (owned, `Hash + Eq` — not `&'static str`): dynamic
  keys are first-class, so per-entity deadlines need no application-side
  nearest-deadline sweep.
- **The key domain is public contract, pinned here** (not implementation
  latitude): the timer operations are generic over the key —
  `set_timeout<K>(key: K, msg, after: Duration)`,
  `set_interval<K>(key: K, msg, period: Duration)`,
  `clear_timer<K>(key: &K) -> bool` (returning whether an armed entry was
  retracted) — with `K: Hash + Eq + Send + 'static`, and one incarnation's
  table is **heterogeneous**: slot identity is (key type, key value), so
  keys of distinct types never collide or replace one another.
  Heterogeneity is load-bearing, not convenience — decorators share the
  inner actor's table (§4.3), and per-layer key types are what keep one
  layer's entries out of another's reach by construction.
- Setting a key replaces exactly; clearing retracts exactly (up until
  delivery). Timer deliveries never transit the mailbox: no capacity, no
  conflation; they count as received-but-not-accepted in stats.
- `set_timeout` with a zero duration arms an already-elapsed timer — not
  an error, never a synchronous delivery: it goes through the ordinary
  timer path, §6.1's priority and bounded retraction turn unchanged, so
  "deliver this as a timer, now" is expressible while `continue_with`
  remains the run-next facility. (Appendix B's zero-budget rule governs
  failure deadlines; a timer is scheduled work, not a failure deadline.
  Contrast `set_interval` below, where a zero *period* clears — a zero
  interval is not a degenerate deadline but an infinite immediate loop.)
- The table is incarnation-owned: restart or stop drops every entry, and
  an elapsed timer is not delivered once stopping begins.
- `set_interval` first fires one full period after arming, requires the
  message type `Clone`, skips missed ticks (no burst catch-up), and a zero
  period clears the key instead of arming.

Cross-actor delayed delivery (`send_after_to` / `interval_to`) is Part II
(§25); it is a separate, mailbox-semantics facility and will be the only
spawned-task timer path.

### 6.4 Stage-typed contexts

`Context` (live), draining, and `StopContext` form a narrowing series in
which **unavailable operations are unrepresentable, not silent no-ops**:
during drain, work-deferring operations (`continue_with`, self-timers,
offloads) are either absent from the stage's type or return a `Rejected`
result — they MUST NOT silently succeed-and-drop. Value-level `Rejected`
is the decided semantics, and the dividing rule is: **context types track
callbacks, not stages**. `StopContext` is a distinct type because
`on_stop` is a distinct callback — narrowing its parameter type taxes
nobody — while drain delivers to the *same* `handle` as live processing
(§6.2), so a typed drain stage would force `handle` to become
stage-generic or split into a second method, taxing every actor
implementation to type-check a rare stage. Within one callback, stage
narrowing is value-level; a stage earns a context type only by arriving
with its own callback. Revisit only if value-level checking demonstrably
causes a shipped bug. `Rejected` outcomes are value-carrying: the
rejection returns the owned payload (the continuation or timer message,
the not-yet-started offload work) to the caller rather than dropping it —
recovery mirrors B.3's send errors; exact types are per-operation, the
payload-return property is normative. The full per-stage capability
matrix is Appendix B.1.

**`for_actor` — same-`Msg` re-entry — is core, and its contract lives
here.** Signature shape:
`Context<'_, A>::for_actor<B: Actor<Msg = A::Msg>>(&mut self) ->
Context<'_, B>` — the same operation on the drain series (yielding the
drain-stage context), and on `StopContext` yielding `StopContext<'_, B>`,
the only re-entry `on_stop` has (B.1); absent from `RawContext`. It is a
zero-cost reborrow of the same underlying context — no boxing, no mapping
layer (Part II §19's `project` is the paying, boxed cousin; the
`Msg`-equality bound is what makes the free identity possible). The
returned context therefore *is* the outer actor's: same incarnation and
identity (`id()`, `incarnation()`), same mailbox and shared resources,
same incarnation-owned timer table (§6.3's heterogeneous keys keep an
inner actor's keys collision-free by type), and every operation through
it — sends, timers, offloads, `stop()` — attributes to the one outer
actor (one mailbox, one identity, one lifecycle: §19's attribution rule,
already in force for the identity case). Stage narrowing carries through
unchanged: a drain-stage caller yields a drain-stage inner context with
the same value-level `Rejected` semantics, and a `StopContext` caller
yields the same withheld surface — re-entry never widens a stage. The
`&mut` reborrow with the `'_` lifetime makes nesting safe and escape
unrepresentable: the inner context cannot outlive the callback or be
smuggled out of it, and decorators compose by stacking `for_actor` calls
at zero cost — the mechanism §4.3's wrapper decorators are built on.

### 6.5 Offloads and blocking work

`offload(future, continuation, deadline)` runs incarnation-owned async
work whose completion re-enters the actor loop; `run_blocking` likewise
for blocking work. These are core — they are the escape hatch that keeps
handlers non-blocking. Contracts:

- Offloads take **one deadline budget**. The continuation is *total*: it
  receives `Result<T, DeadlineElapsed>` and must produce a message either
  way, so the framework's timeout verdict is structurally distinct from
  the inner operation's error (`T` may itself be a `Result`) — no
  hand-ordered inner/outer deadline pairs. There is no `Cancelled` arm:
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
  stated once: the offloaded future is
  `Future<Output = T> + Send + 'static` with `T: Send + 'static`; the
  continuation is `FnOnce(Result<T, DeadlineElapsed>) -> Msg + Send +
  'static` and runs on the actor task.
- `Guard::is_finished` / `finished()` report either ordinary work
  completion or an incarnation-teardown cancellation request. They are
  not a join guarantee: a hard-aborted task can still be unwinding after
  the notification. Ordinary completion does retain a panicking future's
  payload *before* the notification fires, so a `finished()` await that
  observed completion guarantees the panic is retained for §6.2's next
  receive boundary; a teardown cancellation fires the same notification
  without that ordering, which is why §6.2 conditions its guarantee on
  retention rather than on the panic having happened.
- Any higher-level helper that composes `call` inside `offload` MUST
  preserve: incarnation ownership; completion through the actor loop; a
  total timeout continuation; and no await inside the handler.
- Offload completions do not consume mailbox capacity and do not
  participate in conflation; their ordering relative to external messages
  is unspecified. Panics in the offloaded future or the continuation
  resume on the actor task, so supervision classifies them as an ordinary
  actor panic.
- `run_blocking(f)` hands the closure a cancellation token that is a
  child of the actor's shutdown token and is also cancelled if the
  returned future is dropped. Cancellation is cooperative only; after
  hard abort the blocking thread runs detached (documented contract). The
  returned future is `Send + 'static` so it can itself be offloaded. It
  resolves to the closure's return value; a panic in the closure is
  captured and resumes where the future is awaited — on the actor task,
  the ordinary actor-panic path (§8) — while a future dropped before
  completion discards a later panic along with the detached thread
  (documented); a return value or panic payload that already arrived is
  discarded through detached disposal rather than in the awaiting task's
  drop glue. A submission synchronously rejected by the runtime during
  blocking-pool shutdown moves to a detached library-owned thread; the
  operation and destruction of its captured state stay off the submitting
  runtime thread while an isolation worker can be created. An operation
  that never runs at all — the runtime discarded an already-accepted
  submission during its own teardown, or the dedicated fallback thread
  could not start — transfers its captured state to detached disposal,
  and awaiting the future panics with a runtime-teardown cancellation
  diagnostic rather than an internal task-invariant claim. If the system
  cannot create the disposal worker either, disposal's final no-loss
  fallback destroys the state synchronously; thread exhaustion is the
  sole exception to the isolation guarantee.
- On orderly return, error, or caught-panic teardown, offloads, lifetime
  tasks, and monitor leases are frozen, cancelled, and joined before
  actor state is dropped. Hard abort necessarily drops the handler future
  (and therefore handler-owned actor state) before the incarnation's
  synchronous resource teardown can cancel the remaining async resources;
  it cannot join from `Drop`. `run_blocking` is outside the resource
  ledger by design and its thread detaches after a cancellation request;
  `StopContext::run_blocking` may therefore start work after the
  async-resource freeze. The mailbox binding outlives actor destruction
  on every path.

## 7. Readiness

Readiness is **declared data, read before the child's future is first
polled** — never inferred from poll order.

```rust
enum Readiness { Immediate, AfterInit, Manual }   // mode only — the deadline is a child option (§9)
```

- `Actor` (blanket) children default to `AfterInit`: ready when `init`
  returns `Ok`. If that successful initializer requested
  `Context::stop()`, intake freezes at the request but the blanket loop
  publishes the automatic readiness edge before publishing the self-stop.
  This fixed latch order preserves `AfterInit` under arbitrary scheduling
  of the readiness and self-stop watcher tasks; it does not change an
  effective `Manual` initializer's pre-ready stop.
- Raw actor types declare via `RawActor::readiness()`; decorators
  propagate with a visible `R::readiness()`. The trait default is
  `Immediate`, so a decorator that omits propagation reports immediate
  readiness — an ordinary, testable bug (the sibling starts unblocked and
  ordered-startup tests see it), never a silent mid-`init` gate release;
  §16.6's visibility clause is scoped accordingly to the decorator that
  *does* propagate but awaits before delegating. Three shapes MUST NOT
  exist, because each turns readiness from declared data back into a
  scheduling accident: a readiness domain flattened into an optional
  deadline with the third state reconstructed by racing the first poll; a
  crate-private "defer readiness" hook the blanket loop must call
  synchronously before its first await, enforced only by convention
  across a module boundary; and any mode reachable only by the
  framework's own loop (§1 principles 2 and 5). There is no first-poll
  race and no "must be the first operation" cross-module ordering.
- Per-declaration override lives on the child options (§9) uniformly for
  actor and task children. A raw actor type supplies the fallback mode,
  but each child definition resolves its own effective mode (e.g. "not
  ready until an external handshake completes" must be declarable for one
  ordinary handler child without changing the handler type). Subtree
  children are the stated exception: their readiness is structural
  (below), so the subtree veneer carries no mode override — only the
  deadline.
- The deadline is not part of the mode: it lives on the shared options
  record (§9) as `ReadinessDeadline: Inherit | Bounded(NonZeroDuration) |
  Unbounded`, defaulting to `Inherit` — resolution is declaration → scope
  default → library default (Appendix A). Unbounded gating exists only
  via the explicit `Unbounded` value; no `None` does double duty as both
  "inherit" and "unbounded".
- The engine enforces gating and deadline in exactly one place, and sees
  exactly **two** states: immediate, or gated with a resolved deadline.
  `AfterInit` is declaration-level sugar — the blanket loop reports
  readiness through the same public `mark_ready` mechanism after `init`
  returns `Ok`, then publishes any self-stop deferred from that
  initializer; there is no third engine state and no framework-private
  readiness path (§1 principles 2 and 5). Deadline expiry is a distinct
  startup-failure exit cause (§8) — one type, for tasks and actors alike,
  produced by one engine-side timeout (not re-implemented per child kind
  and reunited by downcast).
- An effective `Immediate` mode publishes readiness **at spawn**: the
  engine marks the child ready when it launches the incarnation, before
  the child's future is first polled — for a raw child, before its
  factory has constructed the actor. Uniformly for tasks and raw actors,
  a failure inside an effectively-immediate child — including a raw
  construction panic — is therefore a **post-ready** exit for startup
  classification (§8): an ordered sequence has already advanced past the
  child, later siblings still start, and `wait_started()` does not report
  a startup abort for it. A definition that wants construction observed
  pre-ready declares a gated mode instead.
- Ordered scopes start children sequentially; a gated child blocks the
  sequence until ready. A pre-ready exit follows the child's restart
  policy like any other exit (§8): while restarts remain eligible the
  sequence stays blocked, and each new incarnation re-arms the gate and
  its deadline (the readiness deadline is per-incarnation — invariant
  §16.6's "restart re-runs the gate"; its clock starts at the
  incarnation's start instant, §10.2's shared stamp). Startup aborts only
  on a **terminal** pre-ready exit (`Never`/one-shot, or an intensity
  trip). Later ordered siblings then never start, and their memberships
  terminalize with the `NeverStarted` exit (§8) — the aborted sequence
  has no resume, and terminal handles beat sends parked forever. What
  happens to the started prefix splits by position (§12): at the
  **root**, it stays running — rollback is the owner's decision
  (`start_or_shutdown()` packages the safe default); in a **nested**
  scope, the scope rolls itself back and exits as an ordinary child
  failure carrying the structured startup-failure payload (§12's nested
  rule). Readiness reported exactly at the deadline wins over the
  timeout. A readiness signal fired before an exit or clean self-stop is
  observed MUST count for startup accounting even when event arbitration
  processes the terminal edge first (§13). In particular,
  ready-then-failed is a post-ready failure: restart policy applies and
  startup advances exactly as it would if the readiness event had been
  delivered first. Nested scopes report ready recursively once their
  initial children are up. `spawn()` stays synchronous; `wait_started()`
  is the readiness barrier.
- **Dynamic scopes start their initial members concurrently**, and their
  pre-ready failure rules are the concurrent restatement of the ordered
  ones, not a separate regime. A non-terminal pre-ready exit restarts per
  policy and holds the aggregate open (the latch rule below), exactly as
  an ordered gate does. A **terminal** pre-ready exit of an initial
  member before the aggregate fires is the scope's terminal startup
  failure — *unless* that exit is the commit of an owner-initiated
  removal, in which case the member simply leaves the declared set under
  the aggregate rule below and startup continues — and because every
  initial member was spawned at lowering, there is no not-yet-started
  suffix: **no sibling terminalizes `NeverStarted`**. The transition is
  pinned: the scope leaves `Starting` the moment the exit funnel
  dispatches that terminal pre-ready exit (when one wake makes several
  eligible, §13's arbitration order picks; the startup-failure payload
  names exactly the exit that triggered the transition). By position,
  mirroring the ordered split: at the **root**, the scope parks in
  `StartupFailed` (§12) with *every other member* — running or still
  starting — continuing under supervision; rollback is the owner's
  decision (`start_or_shutdown()` packages it). In a **nested** scope,
  rollback is automatic and *concurrent* — the group is cancelled at once
  and drains in parallel (§11's dynamic teardown), runtime-added members
  included — after which the scope publishes
  `Stopped { reason: StartupFailed }` and exits at its parent as the
  ordinary structured child failure (§12's nested rule, unchanged).
  Failures landing during that rollback are recorded under `Draining`
  mode and schedule nothing (§11); they never change the reason payload.
- **Aggregate readiness is initial-members-only and monotonic.** A
  scope's structural readiness aggregates exactly the members it was
  lowered with — its declared initial set; runtime additions to a dynamic
  scope never join the aggregate, whether they are admitted before or
  after it fires (their per-child `Ready` events, B.4, are the
  observation surface). Removing an initial member while the scope is
  `Starting` shrinks that declared set, and the aggregate may complete
  when the remaining initial members are ready — including the empty
  case, where removing every initial member completes startup. The shrink
  is **commit-time, not request-time**: the member leaves the declared
  set when its removal commits (residency withdrawn, `Removed`
  published), so a slow-stopping member holds the aggregate open for its
  whole stop ladder, and the recomputation is ordered ahead of the
  removal response — a returned `Removed` implies the aggregate has
  already seen the shrunken set. That commit point pins the race against
  the member's own pre-ready failure: a terminal pre-ready exit or
  readiness timeout whose terminal routing observes the membership *not
  yet* marked removing fails startup under the rule above, while once the
  mark is in place removal outranks it and the aggregate simply shrinks.
  Both orders are legal; which one a given run takes is not specified.
  The mark suppresses the member's own **readiness edge** on the same
  terms: a membership observed removing where a readiness edge would be
  published emits no `Ready` (B.4) and is credited to no aggregate, even
  when its readiness latch fired before the mark — and this binds every
  membership, not only initial ones, so a runtime-added member being
  removed publishes no per-child `Ready` either. Which side of *that*
  race a run takes is likewise unspecified; what is pinned is that the
  mark is consulted where the edge would be published, never by
  arbitration position (§13). The aggregate fires at most once per scope
  incarnation and is latched: an already-ready child that fails and
  restarts afterwards does not rewind it — readiness is a startup-phase
  edge, not a liveness signal (snapshots carry liveness, B.6). The same
  latch decides the pre-fire race: a gated child that restarts *before*
  the aggregate has fired holds it open until the fresh incarnation's
  re-armed gate releases (the re-arm rule above); once fired, later churn
  is invisible to it. Per-child readiness is likewise once per
  incarnation — the `mark_ready` token — re-armed by each restart.
- Kind defaults and valid modes: blanket `Actor` children default
  `AfterInit` with all three modes valid; raw actors and tasks default
  `Immediate` and may declare `Manual` (`AfterInit` is meaningless
  without an `init` and is rejected eagerly, §10.3); subtree children
  have no mode knob — their readiness is structural (the recursive rule
  above), bounded by the resolved readiness deadline.
- Manual readiness is reported through a public context operation
  (`mark_ready`), one-shot per incarnation **by construction** (an owned
  token or equivalent — not a runtime take-once flag; §1 principle 3). It
  is reachable from the raw context, the task context (B.2), and the live
  handler `Context` (B.1) — a `Manual` handler actor completes its
  handshake in `init` or `handle` and marks ready right there. Where the
  declared mode already decided readiness, the call is a documented no-op
  (B.2's rule, uniform across kinds); the readiness effect fires at most
  once, so an explicit early mark under `AfterInit` releases the gate and
  the blanket's own post-`init` mark becomes the no-op — earliest mark
  wins.

**Regression anchor (§16.6):** a raw decorator that awaits anything before
delegating to an inner `AfterInit` actor still gates ordered startup — the
sibling MUST NOT start until the inner init returns.

(The decision-layer invariants behind this section — the readiness
aggregate as reducer state — are stated with the rest of the engine's
transition invariants in §15.3.)

## 8. Exits

One classification, produced at one point, used by every consumer.

- The actor/task's own failure type and the framework's verdict are
  **separate channels**: user code returns its error; the runner
  constructs the exit. Classification is a pure function of the observed
  outcome (return value, join result, verdict, cancellation flag) —
  table-testable without a runtime (§15.3). Framework verdicts (readiness
  timeout, cancellation, abort) MUST NOT be boxed into the user-error
  channel and downcast back. That prohibition is scoped to verdicts about
  one incarnation's own termination — anything the classifier can state
  as a typed variant. Two scope-granularity outcomes deliberately enter
  the parent's view as `Failed` — a child scope's intensity trip (§10.2)
  and a nested scope's startup failure (§12) — because to the parent each
  *is* an ordinary child failure. They ride the user-error channel
  without weakening it, by **provenance**: `ExitError` internally
  distinguishes erased application errors (the only publicly
  constructible form — the `From`/string constructors below) from these
  library-constructed structured payloads. B.5's named accessors
  (`intensity_trip()`, `startup_failure()`) are matches on that private
  structure, not downcasts — no downcast exists anywhere on the exit
  path, so §16.8's rule carries zero exceptions — and the payloads are
  non-forgeable: an application error that imitates a trip is still an
  erased user error, for which the accessors return `None`. The
  framework's own classification consults only `is_failure()` either way.
- `ExitError` — the user-error channel's type — is a library-owned,
  type-erased error: constructed via `From` from any `E: Error + Send +
  Sync + 'static` (plus a string-message constructor), erased once into
  `Arc`-shared storage — exits ride `Clone` snapshots and events, so the
  payload must be cheaply cloneable and cross-thread; `Sync` is what
  makes the shared reference sound. `ExitError` itself does NOT implement
  `std::error::Error` (the `anyhow` precedent): implementing it would
  make the blanket `From<E>` overlap `core`'s reflexive `From<T> for T`.
  It carries `Display`, source-chain access, and a by-reference
  `&(dyn Error)` view instead. Applications MAY downcast it (e.g. routing
  on a domain error surfaced in a `Failed` exit); the framework NEVER
  does — framework verdicts have their own variants, and §16.8 enforces
  the asymmetry structurally.
- One public exit type covers: `Completed`, `Failed(error)`, `Panicked`
  (carrying the panic message when the payload downcasts to a string —
  the payload itself is never retained, since exits ride `Clone`
  snapshots and events), `ReadinessTimedOut { deadline }`,
  `Aborted { phase: GracePhase }`, and the membership-level
  `NeverStarted`, with cancellation ("supervisor asked it to stop" vs
  "finished on its own") as an orthogonal, explicit
  `Cancellation::{Observed, NotObserved}` value on every exit.
  `GracePhase::{WithinGrace, AfterGrace}` distinguishes whether
  cooperative grace expired before an abort; scope-level shutdown-timeout
  remains a separate verdict. `NeverStarted` is the terminal outcome of a
  membership that ends with no incarnation ever spawned — a declaring
  tree dropped unspawned, a rejected or withdrawn insertion (§3.2, §9),
  removal before first spawn, or a startup abort terminalizing
  never-started siblings (§7). It is a membership fact, not an
  incarnation verdict: it sits outside the precedence rule below, is
  never input to restart or intensity accounting (both consume
  incarnation exits), and counts as a failure for `is_failure()` —
  awaiting a child that never ran is not success.
- **Cancellation observation is one state-machine fact**, not a narrative
  judgment: the value is `Cancellation::Observed` iff the incarnation's
  own shutdown token (B.1/B.2) had fired before its outcome was recorded
  (the record phase below), and `Cancellation::NotObserved` otherwise.
  The token fires for every engine-initiated stop — scope teardown,
  dynamic removal, readiness-timeout teardown, `shutdown(0)`'s immediate
  escalation — and for local `ctx.stop()`, which arms the same ladder
  (§11), so a self-stopped actor's exit reads `Cancellation::Observed`. A
  stop racing natural completion needs no third rule: whichever of
  token-fire and outcome-record happened first decides the value.
  `NeverStarted` sits outside the rule — no incarnation, no token — and
  carries `Cancellation::NotObserved` uniformly, whether the membership
  ended by tree-drop, withdrawal, rejection, or startup abort: the
  variant itself already says nothing ran.
- **Verdict precedence.** When one incarnation's end admits several
  readings, classification picks the highest of: `Panicked` >
  `ReadinessTimedOut` > `Failed` > `Aborted` > `Completed`; cancellation
  observation is orthogonal and never competes. Concretely: a panic is
  never masked, wherever it lands (`run`, `on_stop` — superseding the
  run's outcome, §4.1 — or an incarnation-owned destructor, via the
  fallback report token; §5.5's detached message-disposal faults are
  outside the incarnation verdict); and a readiness-deadline expiry names
  the *cause* even when the teardown it triggers ends in a grace-expiry
  abort (the mechanism).
- **`Aborted` genuinely competes with a recorded outcome, and the rule is
  asymmetric.** `Aborted` describes a future destroyed before yielding an
  outcome, so it reads as if it could never conflict with
  `Failed`/`Completed`, which require one. It conflicts anyway, by race:
  the body can record its result and the destruction still land before
  the join retires, and §11's ladder can arm a supervisor-forced abort
  verdict against a membership that has already recorded one. Precedence
  resolves that race in one direction only, and the asymmetry is
  deliberate rather than an artifact of the ordering:
  - A recorded `Failed(error)` **survives** a later abort. Destruction
    proves only that teardown ended the task; it must not erase the
    structured application error the task already produced, which is the
    only evidence naming *why* the child was failing.
  - A recorded `Completed` does **not** survive: cancellation overrides
    it and the exit reads `Aborted { phase }` with
    `Cancellation::Observed`. A supervisor that destroyed a child before
    its success was observable did not get a successful child.
    Concretely, a one-shot task whose body returned `Ok(value)` inside
    that window exits `Aborted` and `OneShotTaskRef::wait` yields
    `Err(exit)` — the typed value is dropped rather than released past a
    stop the supervisor had already committed to.

  Only the recorded-vs-abort pair is asymmetric this way; the ordering
  above is otherwise a total precedence over the whole variant set.
- **Failure classification (feeds §10.2):** an exit is a *failure* iff it
  is not `Completed`. So `Failed`, `Panicked`, `ReadinessTimedOut`, and
  `Aborted` all restart under `OnFailure`; `Always` restarts even clean
  completions. The cancellation observation is **never** consulted by
  restart classification — restart suppression comes solely from teardown
  *state*, never from the exit's shape: a draining scope schedules no
  restarts (§11), and a membership whose removal is in progress
  (`membership_status: Removing`, B.6) has its restarts suppressed even
  while its containing scope keeps running — dynamic removal of one child
  must not depend on the whole scope draining (§12). The same mark
  suppresses that membership's readiness publication (§7): one
  level-triggered removal source, consulted at execution time by every
  site that would otherwise publish or schedule on the membership's
  behalf.
- There is exactly **one incarnation runner**, and it yields this one
  exit type — including `Panicked` — regardless of how it is hosted. The
  supervisor is a policy loop over the runner's output. (The public
  supervisor-free hosting surface is Part II §24; the single-runner
  property itself is core and internally tested, because it cannot be
  retrofitted: two runners inevitably grow two failure models, and one of
  them ends up letting panics unwind to the user.)
- Structured surfaces carry structured payloads: shutdown-timeout errors
  list the affected children as data — child-id *paths* from the scope
  whose timeout expired, each with its membership token (§3.2), since
  bare ids are ambiguous across sibling scopes that reuse them (§2) — not
  a formatted string; `Failed` carries the error value, not its `Display`
  projection; error string projections may exist only in display paths.
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
  replacement. §16.7's race provocations remain necessary because
  *detached* work (an aborted `run_blocking` thread, in-flight
  observations) can still surface stale events after that join; those
  must fence to the old incarnation.
- **The double-panic containment boundary.** Post-join publication can
  fold a destructor panic into the report only if the process survives to
  join. A callback panic that unwinds *through* the actor value would run
  the actor's `Drop` during the unwind; if that `Drop` also panics, the
  process aborts before anything publishes. The runner therefore MUST
  catch a panic from user callback or construction-source execution
  (`init`, `handle`, `on_stop`, `run`, task bodies, factories) at the
  execution boundary, **before the actor value and its incarnation-owned
  state are destroyed** — the actor's destructor never runs inside a user
  callback's unwind. A callback panic followed by a destructor panic is
  then two separately caught panics folded under the precedence rule into
  the single published report; on that equal-precedence collision the
  recorded outcome wins — the report is `Panicked` carrying the
  *callback's* payload, the destructor's being observable only in the
  process staying alive (§16.7's provocation). Explicitly outside the
  boundary: a destructor panic occurring *inside the same unwind* — a
  local within the user's own future poisoning its `Drop` — is a genuine
  double panic and aborts the process; that is Rust's contract,
  documented alongside §12's `panic = "unwind"` precondition, not
  something the runner can contain.
- **Declaration-hook isolation.** The static and dynamic `add_*` entry
  points isolate the supplied definition before invoking the caller's
  `Into<ChildId>` conversion, and raw-definition erasure keeps the
  definition isolated while invoking `RawActor::readiness`. A panic from
  either eager hook therefore cannot unwind through and destroy that
  definition on the caller's thread. This is a narrow ownership
  guarantee, not an extension of the runner boundary: other locals in the
  user's synchronous declaration call remain subject to Rust's ordinary
  unwinding and double-panic contract, as do user values that the
  framework never accepted or wrapped.

User destructor double-panic behavior is a documented Rust process
precondition rather than a state transition, and no in-process test can
claim it. (The decision-layer invariants behind this section — the
report/join fold and the authoritative membership state machine — are
stated in §15.3.)

## 9. Child specification and options

All child kinds share one uniform options record; per-kind spec types are
thin typed veneers over it, not hand-copied projections. The record ships
whole in core even where an individual option is Part II — the shared
record is the anti-drift structure, and extending it is additive. Its
boundary is §15.2's plain-data rule: every option is plain `Eq` data, so
**code-bearing extractors are structurally outside the record** — §18's
key extractor and §22's `message_size` measurer are typed spec extensions
arriving with their features, exactly as actor implementations are outside
§23's outline, never options.

Options: `restart` (absent by construction on one-shot forms), `shutdown`,
`readiness` mode override (absent by construction on subtree veneers —
subtree readiness is structural, §7) plus `readiness_deadline`
(`Inherit`/`Bounded`/`Unbounded`, §7, all kinds), terminal-membership
retention (one name, one polarity, one kind-independent default — *retain*
on restartable children, *remove* on one-shot children, stated once), and
actor-only mailbox settings (`mailbox`, `mailbox_shutdown`).
(`message_size` observation, Part II §22, is not an option: its measurer
is code, per the extractor boundary above.)

**Retention semantics (tombstones).** Terminality and pruning are two
distinct membership edges, and the retention option chooses only their
distance. A membership becomes *terminal* when its final exit publishes
(§8) — snapshot state `Stopped { exit }` / `StartupAborted { exit }`
(B.6). It is *pruned* when it stops being a resident of its scope: the
`Removed` event fires (B.4), the id is freed, and anything not yet
resolved terminal now does. Under *remove*-on-terminal the edges coincide:
pruning follows terminalization immediately (§12's finishing test runs
strictly between them). Under *retain*, the terminal membership stays
resident as a **tombstone**, with exactly these properties:

- It still occupies its id: a same-id `reserve_*`/`add_*` fails
  `DuplicateId` until the tombstone is pruned — replacement under a
  reused id is thereby always an explicit remove-then-add, never a silent
  slide past a dead predecessor (§3.4's boundary depends on this).
- `child(id)`, `descendant(path)`, and snapshots return its terminal
  `ChildSnapshot` (`incarnation: None`); `Removed` has **not** yet fired.
- `remove` — by id or exact handle — prunes it: resolves `Removed` (B.8),
  fires the `Removed` event, frees the id. Scope teardown and scope
  terminalization prune all tombstones as part of ending the scope.
- It never blocks §12's natural completion (a retained terminal
  membership counts as terminal there) and never participates in restart
  or intensity accounting — retention is observability, not liveness.

API-shape rules:

- One add-method family, names pinned: `add_actor` / `add_task` /
  `add_subtree`, with one-shot twins `add_actor_once` / `add_task_once` /
  `add_subtree_once` (the suffix keeps the family grouped in docs and
  autocomplete), plus the raw-actor pair `add_raw` / `add_raw_once`
  (§4.3) — **eight entry points** — each taking `(id, definition)`: the
  id as `impl Into<ChildId>` (`ChildId` is a concrete library-owned type,
  so the string conversions are ordinary, coherent `From` impls) and the
  kind's definition value; there are no parallel `*_spec` twins. A spec
  is that (id, definition) pair, where the definition is the construction
  source (§4.2's bounds, pinned in the slot surface below) plus options:
  `reserve_*` consumes the id, a slot's define consumes the id-less
  definition, and the fused `add_*` takes both and splits them — no
  surface carries the id twice.
- **Definitions are built by nominal constructors, never accepted through
  blanket conversions.** The obvious convenience — `define` taking
  `impl Into<ActorDef<A>>` with conversions from a bare `A::Args` and
  from a `Fn() -> A::Args` closure — is not implementable in coherent
  Rust: a conversion from an associated-type projection and a conversion
  from a closure bound are blanket impls that collide with `core`'s
  reflexive `From<T> for T` and can overlap each other (an args type can
  itself be a nullary closure; the rejected blanket surface fails E0119).
  The constructors are the public surface, one per §4.2 mode (signatures
  pinned in the slot block below): `ActorDef::cloned(args)` /
  `ActorDef::factory(f)`, `ActorOnceDef::new(args)`,
  `RawDef::factory(f)` / `RawOnceDef::new(actor)`,
  `TaskDef::new(factory)` / `TaskOnceDef::new(body)`,
  `SubtreeDef::factory(f)` / `SubtreeOnceDef::new(tree)`. Each yields the
  definition carrying default options; options attach through consuming
  setters on the definition — one setter per field of §9's record
  (`.restart(..)`, `.shutdown(..)`, `.readiness(..)`,
  `.readiness_deadline(..)`, `.retention(..)`; actor defs additionally
  `.mailbox(..)` / `.mailbox_shutdown(..)`), **minus the setters the
  record marks absent by construction** — no `.restart(..)` on `_once`
  defs, no `.readiness(..)` on subtree defs — and subtree defs
  additionally carry `.defaults(Inherit | Reset)`, the §10.3 edge knob,
  which is a property of the subtree edge rather than a field of the
  shared record (the subtree def *is* §9's "subtree veneer") — so a
  definition with options is still one expression at the call site, no
  build/extract/add dance (cells §3.2 make this natural).
- The spec surface for one kind names operations identically across
  kinds — the one-shot operation is the `_once` twin, never a
  differently-named `spawn_once`.
- Adding a new child kind or mode extends the shared record and the
  private declaration dispatch, not duplicated reserve/add/define
  choreography. The dispatch is deliberately sealed inside the façade:
  its associated handle set and slot kind let the implementation share
  that choreography, but the eight public add entry points and the
  nominal slot methods above it remain concrete. Making the dispatch
  public would admit an oversized extension surface and turn per-kind
  parameter errors into generic trait-bound errors; collapsing the
  reserve methods would also be false because actor mailbox type and
  subtree flavor are fixed before a definition exists.

**Slots — the reserve-before-define surface.** §3.2's cell machinery has
one public face, uniform across the three kinds and both scope flavors.
The shape is content-normative (Appendix B's latitude on exact names
applies):

```rust
// On ordered tree builders and on DynamicScopeRef alike — reservation is
// synchronous on both flavors, and is where the id errors reject (rules
// below). Receivers differ by flavor: `&mut self` on builders (plain
// owned values, §12 — exclusive access is free), `&self` on
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
// constructors (the §9 constructor rule: blanket `Into` conversions from
// bare sources are rejected for coherence). Every construction-source
// bound is pinned here, once. Constructors yield default options;
// options attach via the consuming setters listed in the §9 rule:
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
        // incarnation's startup failure (§12's lowering rule)
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
  synchronous on both flavors and returns `Result`: it is §10.3's eager
  point for the id errors — `EmptyId` and `DuplicateId` on either flavor
  (a retained tombstone counts as a duplicate — retention semantics
  above); on dynamic scopes additionally `RemovalInProgress` (same id
  mid-removal, §12), `NoRuntime` (runtime boundary below), and
  `NotAdmitting` (stage rule below). From success the slot's handles
  resolve through the cell like any other handle; the slot is
  parameterized by exactly what a handle needs (`M` for the wire type,
  `T` for the subtree's ref dispatch) so refs exist before any actor type
  or factory is named.
- **`add_*` is sugar**: each of the eight add entry points is exactly
  `reserve_*` followed by the matching define and returns what the pair
  returns. One declaration path, not two — cyclic wiring uses the split
  form, everything else the fused one. Fused forms surface the union of
  the two steps' errors (B.8).
- **Return values are pinned per kind.** On a declaration builder, define
  (and therefore `add_*`) yields the child's handles synchronously:
  `ActorRef<M>` for the four actor forms; `TaskRef` for tasks, joined by
  the owned `OneShotTaskRef<T>` on the one-shot form; `T::Ref` for
  subtrees. On a dynamic scope the admission future returns that same
  per-kind handle set directly (B.8). Every returned set contains a
  membership-addressed component: `ActorRef`, `TaskRef`, or `T::Ref`
  exposes the membership token through `membership()` (§3.2). The slot's
  pre-admission handles and the returned handles resolve through the same
  cell — one identity, no reconciling.
- **Definition is consuming**, so double-definition is unrepresentable
  (§4.2's owned-`FnOnce` shape at the declaration layer). On a
  declaration builder, `define` completes the declaration synchronously
  and cannot fail — spec-level validation already happened eagerly at
  spec construction (§10.3). On a dynamic scope, `define` *is* the
  admission call: a future resolving at admission to the per-kind handles
  (B.8). Its operation errors are `NotAdmitting` and the first-poll
  `NoRuntime` rejection below: the id errors were already spent at
  reserve, and definition validation was spent at spec construction
  (§10.3's eager rule), exactly as on the builder flavor — admission
  validates no definition data. Whether the builder and dynamic flavors
  are one generic slot type or two parallel families is implementation
  latitude; the operation inventory and this error split are not.
- **Fused admission futures abort on drop; split ones detach.** Dropping
  an in-flight dynamic `add_*` future withdraws the insertion: if
  admission has not yet happened, the reservation is released and the
  cell terminalized (§3.2); if admission won the race, the child is
  removed exactly as by exact-handle `remove` (§12). Either way the scope
  never retains a child whose identity the caller failed to receive — the
  unknown-outcome window that §3.2 closes for completed calls,
  cancellation-as-abort closes for abandoned ones. That rationale is
  exactly why the split form behaves differently: the slot's handles,
  taken before `define`, already *are* the caller's identity, so a
  dropped dynamic `define` future **detaches** — the insertion proceeds
  to admission (or to its `NotAdmitting` rejection, observable as cell
  terminalization, §3.2), and an admitted child is retained, observable
  and removable through the slot's handles. Cancelling admission in the
  split form is therefore explicit: `remove` by the handle already held.
  Both forms at both pre- and post-admission points are invariant
  §16.12's provocations.
- **Cancellation is linearized by the cell, not the command channel.**
  Dynamic membership operations are *eager at reservation, awaited at
  admission*: the call claims the id and mints the cell synchronously
  before returning — for the fused form too, whose future exists only to
  carry the two steps' outcome — while the admission command rides the
  unified unbounded event lane (§11), driven by the caller's polling. The
  small request record is batched at the consumer; the reservation and
  user payload remain owned by the producer, so a channel capacity would
  bound ingest rate rather than memory. Drop cannot await that lane, so
  the drop rules above are enacted through a **level-triggered
  cancellation latch** owned by the fused future and registered on the
  cell: dropping the future flips the latch synchronously (a token edge
  that always succeeds, like §11's shutdown latch). Both exit-time
  restart scheduling and execution of an already-due restart deadline
  MUST consult that level-triggered source directly before charging or
  running a restart; the public `Removing` projection follows the
  forwarded removal event and is not the synchronization primitive. Queue
  saturation therefore cannot charge restart intensity or run user
  construction for a cancelled membership while its removal event waits
  for forwarding. The driver converts the edge into an engine event that
  resolves the race by stage — a not-yet-admitted operation, whether its
  command is unsent, still queued, or dequeued-but-unprocessed, is
  annulled at the admission check (reservation released, cell
  terminalized, §3.2); an admitted child is removed exactly as by
  exact-handle `remove` (§12). Before-first-poll behavior follows: the
  reservation already exists, the admission command was never sent, and
  dropping the never-polled fused future terminalizes the cell through
  the same latch. The split `define` future carries no latch — that is
  the detach rule above — and its detach guarantee ("admission proceeds")
  holds from first poll, once its command is in flight; one dropped
  before ever being polled has submitted nothing, and the cell
  terminalizes exactly as if the slot had been dropped undefined (§3.2).
- **Runtime availability is checked before dynamic mutation.** A dynamic
  `reserve_*` validates the id syntax first, then requires an ambient
  runtime, and only then reads the scope's admitting state or resident-id
  table. The error precedence is therefore `EmptyId` before `NoRuntime`,
  and `NoRuntime` before `NotAdmitting`, `RemovalInProgress`, or
  `DuplicateId` (and before identity minting can yield
  `IdentityExhausted`); a no-runtime rejection mints no cell and claims
  no id. The admission future rechecks runtime availability at its first
  poll, before spawning or submitting the admission command. If a slot
  was reserved inside a runtime but its `define` future is first-polled
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
  does a dynamic root parked in `StartupFailed` (§12): the park is the
  state the owner must resolve, and admitting new members into a
  half-started root would muddy exactly that decision. Outside the
  admitting window — membership terminal, incarnation `Draining`, root
  parked in `StartupFailed`, or no incarnation live (a pre-spawn handle,
  or the window while an ancestor restart re-lowers the subtree) —
  `reserve_*`, dynamic `define`, and the fused `add_*` fail with the
  single `NotAdmitting` outcome (B.8, which enumerates the causes).
  Nothing queues to await a future incarnation: that matches §12's rule
  that runtime membership never survives an ancestor restart — re-adding
  on restart is application logic, and the application holds the
  lifecycle events to drive it. `remove` is stage-exact too: a
  reserved-but-undefined cell is already an identity and is removable by
  id — removal terminalizes it (`NeverStarted`, §8) and frees the id,
  with no `Removed` event and no tombstone (pre-admission cells are not
  yet members — §3.2's minting/admission split), and a later `define` on
  the orphaned slot resolves `NotAdmitting` with the reservation-ended
  cause (B.8 — the one cause that fires while the scope itself keeps
  admitting); a child still in startup is removed like any running child
  (§16.12); on a scope that is draining or stopped, `remove` resolves
  `AlreadyAbsent` — the teardown owns every stop, and §12's idempotency
  makes that indistinguishable from having removed the child yourself.
- **Construction-source bounds are pinned in the def constructors
  above** — one place, verbatim, per §4.2's capability rule: restartable
  forms carry a re-mintable shared source (`Args: Clone + Sync` or a
  `Fn() -> Args + Send + Sync + 'static` factory; task bodies a
  `Fn(TaskContext) -> F + Send + Sync + 'static` factory; raw actors
  `Fn() -> R + Send + Sync + 'static`; subtrees
  `Fn() -> T + Send + Sync + 'static`), `_once` forms consume owned
  values. Cyclic wiring thereby reduces to ordering: every ref a factory
  needs is minted from a sibling slot before any factory is written, so
  factories capture real `ActorRef`s — no `Option<ActorRef>`, no
  registry (C.3 is the acceptance scenario).
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

## 10. Scope policy: strategy, intensity, defaults

### 10.1 Fate-sharing strategy

Core ships `OneForOne` only: a child's exit affects that child alone. The
group strategies (`OneForAll`, `RestForOne`) are Part II §21 — they are
the single largest block of deferred engine complexity, and §11's
mode-based exit funnel is designed so they land without restructuring.
The `Strategy` type is non-exhaustive from day one, is a property of
**ordered** scopes only (§2), and does not exist on dynamic scope
builders, configs, or snapshots. While `OneForOne` is the sole variant,
ordered builders carry no strategy setter either: a knob whose only value
is its default selects nothing, and offering it would pin a call shape no
caller can vary. The setter is deferred until a second `Strategy` variant
lands with Part II §21; until then the type is reachable only where it is
*read* — `ScopeSnapshot` reports `Some(OneForOne)` for an ordered scope
and `None` for a dynamic one (B.6). Adding the setter back is an additive
change, and the type staying non-exhaustive is what keeps it one.

### 10.2 Restart policy and intensity

Two separated concerns:

- **Per child** — `RestartPolicy`: the condition (`Always` / `OnFailure` /
  `Never` — with `Never` structural on one-shot forms) and `Backoff`
  (fixed, or exponential: `base × factor^(n−1)` clamped to `max`, with
  optional equal-jitter drawing uniformly from `[d/2, d]`; all durations
  validated non-zero at construction). Delay computation is a pure
  function of (attempt, policy, `JitterSample`) — the sample is an input,
  not drawn inside (§15.3; the runtime façade owns the source).
  `JitterSample` owns the `[0, 1)` invariant: its constructor clamps
  finite inputs and maps non-finite inputs to zero, while
  `from_u64_ratio` owns the driver's integer random-source normalization.
  Pinned arithmetic: `factor` is a validated newtype over `f64` — finite
  and `≥ 1.0`, checked at construction — implementing `Eq`/`Hash` as
  bit-equality of the underlying bits, sound because the invariant
  excludes NaN and required because `Backoff` is §15.2 plain data
  carrying the universal `Eq` bound; delay computation is pinned to
  nanosecond precision: when the effective multiplier is exactly one —
  the first attempt, a factor of `1.0`, or a fixed delay — the
  whole-nanosecond count is used exactly, with no float round-trip;
  otherwise the base delay's whole-nanosecond count is multiplied as
  `f64`, rounded to the nearest nanosecond, and a product at or above
  `max` saturates to the exact configured `max` (never an overflow
  panic); jitter maps the pre-drawn `JitterSample` as
  `delay = d/2 + sample × d/2`, rounded the same way — a zero sample
  yields the exact half, half-nanosecond remainders rounding up with no
  float round-trip. This exact exponentiation contract covers attempts
  operationally reachable by a running membership; the opaque counter's
  full `u64` domain exists for totality, not as a promise that synthetic
  multi-billion-attempt inputs use an unbounded exponent representation.
  Beyond the implementation's supported exponent domain, `next_delay`
  remains total, nondecreasing for a fixed sample, and bounded by `max`;
  it may saturate the exponent and plateau rather than evaluate
  `factor^(n-1)` exactly. The attempt counter is per membership: `n = 1`
  on the first scheduled restart, incremented per scheduled restart — a
  restart scheduled and then cancelled by teardown still advanced it,
  mirroring the intensity charge below — and reset by an incarnation that
  exits after running at least the scope's intensity window `within` (one
  clock answers "has it settled"); the snapshot's `restart_count` (B.6)
  is its non-resetting cumulative twin. These three reset-distinct
  domains are public opaque values: `RestartAttempt` for the resettable
  backoff position, `RestartCount` for one membership's cumulative
  charges, and `TotalRestarts` for one scope incarnation's cumulative
  charges. Each exposes only `ZERO`, a saturating `bump()`, and `get()`;
  none implements arithmetic traits, so the domains cannot be added,
  substituted, or compared across one another accidentally. Running time
  is passed to the settling decision as one named
  `IncarnationRun { started_at, stopped_at }`, so its endpoints cannot be
  transposed, and is measured between two engine-stamped instants: the
  incarnation's **start instant**, stamped once when the engine schedules
  the spawn (the `Started` event's instant — the same stamp anchors §7's
  readiness deadline), and its exit publication (§8). Failure
  classification is §8's. Downstream code can inspect the policy's
  condition (`is_never`-class queries are public).
- **Per scope** — `Intensity { max_restarts, within }`: the churn budget
  (default: Appendix A), tripping on the restart that *exceeds* the
  budget. **Every** respawn charges it — in core that is every own-child
  respawn; when Part II §21 lands, sibling respawns forced by group
  strategies charge the same budget (that rule is stated here so §21
  cannot relitigate it). Exceeding it is scope-fatal and escalates to the
  parent. A per-child cap MAY exist as a refinement; it cannot substitute
  for the scope budget. Edges, decided: the budget is charged when a
  restart is *scheduled*, before any backoff delay elapses; the rolling
  window is strict — a charge at time `t` ages out once
  `now − t > within`, and the trip fires on the charge making the
  in-window count exceed `max_restarts`. The over-budget edge itself is
  exact: the tripping charge is a real scheduling charge — the
  membership's attempt counter, its cumulative `restart_count`, and the
  scope's `total_restarts` (B.6) all include it, and
  `RestartScheduled { attempt, delay }` **is** emitted — after which the
  trip fails the scope before the delay elapses, so the scheduled restart
  is cancelled by the ensuing teardown and never spawns (the
  scheduled-then-cancelled-still-advanced rule above, applied to the trip
  itself). The emitting scope's event order is pinned: the child's
  `Exited` → its `RestartScheduled` → the scope's own failure
  (`ScopeState: Draining`, then the nested terminal state or the §12 root
  outcome), and the trip payload's in-window count includes the tripping
  charge. The budget exists on both scope flavors (dynamic scopes restart
  their own children too); a tripped scope surfaces at its parent as an
  ordinary `Failed` child exit whose error value is a structured,
  library-owned, publicly nameable intensity-trip type — carried as
  library-constructed `ExitError` provenance (§8: reached through B.5's
  named accessor as one compile-checked call, no downcast anywhere,
  non-forgeable), while the framework's own classification consults only
  `is_failure()` — subject to the parent's restart policy for that scope
  child; and at the root, tripping terminates the tree — the owner
  observes it through `wait_started()` during startup, `wait()` after it
  (§12), and the terminal reason (`Stopped { reason: IntensityTripped }`)
  carried by the root's final snapshot and `ScopeState` event (B.6, B.4).

### 10.3 Defaults

**The scope-defaults record is enumerated, not elided.** `ScopeDefaults`
is one plain-data record (§15.2) with exactly these fields. The first
four are optional (`None` = resolve outward); readiness uses its own
explicit `ReadinessDeadline::Inherit` unset state, so an `Option` cannot
represent the same meaning twice:

- `child_restart: RestartPolicy` — condition + backoff;
- `child_shutdown: Shutdown`;
- `mailbox: Mailbox` — kind **and** capacity travel together (the library
  default is `queue` at Appendix A's capacity; a scope may default its
  actors to `latest()`);
- `mailbox_shutdown: MailboxShutdown`;
- `readiness_deadline: ReadinessDeadline` (§7), default `Inherit`.

Deliberately *not* in the record: readiness mode (per instance, §7),
terminal-membership retention (decided by the child's §4.2 mode with a
per-child override, §9), and strategy/intensity (properties of the scope
itself, not defaults for its children — §10.1, §10.2).

Resolution and mechanics:

- Per-child resolution is declaration → nearest enclosing scope with that
  field set → library default (Appendix A). Exactly one stored copy at
  declaration; children resolve at insertion.
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
  default. Deferral resolves **by kind**: a declaration that names a kind
  but defers capacity resolves that capacity through enclosing defaults
  *of the same kind* — a scope default of a different kind has no
  capacity to contribute and is passed over, resolution continuing
  outward to the library default for the declared kind (Appendix A). The
  two rules compose without conflict: "kind and capacity travel together"
  governs a *default record's* contribution (a default never donates its
  capacity to a different kind, and applies whole where the declaration
  named no kind); kind-matched deferral governs a *declaration's*
  unfinished capacity. The exact policy case, decided: a child declaring
  `queue` with deferred capacity under a scope default of `latest()`,
  with no enclosing `queue` default beyond it, gets `queue` at the
  library default capacity — the scope default neither converts the
  declared kind nor supplies a capacity across kinds, and with no outer
  `queue` default the outward walk ends at the library default. For the
  full outward walk, suppose the root defaults to `queue(10)`, an
  inheriting child scope defaults to `latest()`, and an inheriting
  grandchild scope contains an actor declared with `queue_inherit()`:
  that actor resolves to `queue(10)`, because the intervening `latest()`
  is passed over when resolving queue capacity. If the grandchild edge is
  `Reset` instead, the same declaration resolves to the library
  `queue(64)`; the reset severs the root's contribution.
- Validation is eager: any configuration that would fail spawn fails at
  the point of declaration where it is decidable — duplicate/empty ids at
  add time, zero capacities at construction, zero backoff durations at
  construction.
- Library-level fallback values are Appendix A; they apply only where
  neither the declaration nor any enclosing scope decided.

(The serializable outline — the declaration companion that must serialize
distinctly for any two trees differing in the surface it carries — is
Part II §23, together with the `serde` feature.)

## 11. Shutdown

One escalation ladder, one state machine, everywhere:

```
cooperative cancel → grace expiry → tidy-abort beat → hard abort
```

- Per-child stop state is a single owned ladder value (policy, phase,
  deadline) advanced by the engine — conceptually
  `StopLadder { policy, phase, deadline }` with
  `advance(now) -> Option<Cancel | Escalate | HardAbort>` — with all
  pending deadlines in one priority queue, not rescanned per wake.
  Ordered teardown, concurrent drain, dynamic removal, and nested-scope
  teardown differ only in *when ladders start*, never in how a ladder
  runs. A forced escalation only ever moves a ladder deadline *earlier*
  (`min`), never later, and never skips the tidy-abort beat.
- The tidy-abort beat (the pause between escalation and hard abort,
  letting a cancelled child finish a final cleanup step) is defined in
  Appendix A.
- `Shutdown` policy per child: `Shutdown::graceful(nonzero_grace)`
  (stored as `Graceful { grace: NonZeroDuration }`; default: Appendix A)
  or `Abort`. `Shutdown::graceful(Duration::ZERO)` is a construction
  error — the policy has no zero-grace branch, and `Abort` is the sole
  immediate-escalation policy. `Abort` is the immediate-escalation point
  on the same ladder, not a second mechanism: the shutdown token fires
  and the ladder escalates immediately — no grace wait — so the
  `abort_token` fires in the same engine step but strictly *after* the
  shutdown token (the B.2 ordering contract is unconditional; the tokens
  never fire "together"), the tidy-abort beat still runs, then hard
  abort — and the child is joined before teardown advances, exactly as
  under grace expiry. The policy does not pre-decide the classification:
  §8's classifier still reads what actually happened, so a child that
  yields an outcome during the beat exits `Completed`/`Failed` (with
  `Cancellation::Observed`), and `Aborted { phase: WithinGrace }` records
  only a hard abort actually reached — the future destroyed before
  yielding — distinguishable from grace-expiry abort
  (`phase: AfterGrace`). The one boundary case, where the beat expires
  and the hard abort lands *after* the body recorded its outcome but
  before the join retires, is settled by §8's precedence and not here: a
  recorded `Failed` survives that abort, a recorded `Completed` does not.
  Grace is a supervisor-side upper bound; child-local time after
  cancellation wakeup is scheduler-dependent — this is documented
  contract. Cancellation-before-escalation ordering is observable to the
  child itself — its `shutdown_token` fires strictly before its
  `abort_token` (B.2), which is what C.2's sidecar reads from the child's
  own journal — and in the exit's `cancellation`/`phase` fields;
  lifecycle events carry **no ladder-transition events** (B.4's inventory
  is deliberately exit-only here), so §16.10's ordering assertions are
  built from child-side observations, not the event stream.
- Ordered scopes tear down in reverse declaration order, one at a time,
  full grace each; the cursor child is aborted *and joined* before the
  ladder advances to the next sibling. Dynamic scopes cancel the group at
  once and drain concurrently (grace clocks run in parallel, not summed).
  Aborting an ancestor arms a recursive hard-abort cascade.
- **Driver death discharges; it never absolves.** A scope driver
  destroyed with obligations outstanding resolves all of them on the way
  down: still-active descendants publish the coarse kill verdict —
  `Aborted { phase: WithinGrace }` with `Cancellation::Observed` —
  memberships terminalize (sends fail `Terminated`, exit-awaiting
  surfaces resolve), in-flight admissions and removals resolve their
  enumerated rejections, and every `Added` is paired with its `Removed`
  before the scope's own final event. An inactive child in the
  classified-but-unpublished terminal-disposal state is not coarsened:
  teardown publishes its stored exit before discharging terminality,
  without waiting for the retained user-state disposal, so that
  classified verdict wins. A retained-construction destructor panic that
  has already been reported is folded into that exit before publication,
  just as on orderly dispatch — whether it is still queued on the
  disposal lane or was already collected into the driver's current event
  batch, which a teardown transition outranks. Teardown folds only what
  has been reported and never waits: a disposal still in flight remains
  detached, and its unknowable result cannot delay the kill path. First
  publication wins, so a later completion cannot overwrite the bounded
  fallback. This is one precision boundary — post-join precision may be
  sacrificed, a parked promise may not survive — decided here once
  rather than per call site.
- "Drained" has exactly one definition, derived from child state (no
  hand-maintained live counter).
- Child exits are consumed through one funnel regardless of which await
  point dequeued them — one `ingest(exit)` that always records, and one
  dispatch whose behavior is a function of the scope's current mode
  (`Running` vs `Draining { scope, reason }`) held on the runtime — plus
  the exiting child's own membership state (`Removing` suppresses that
  child's restart, §8) — not of the call site. The root's
  `StartupFailed` park (§12) is not a third mode: a parked root keeps
  dispatching its started prefix under `Running` — restarts schedule and
  charge intensity as usual, and an intensity trip terminates the tree
  from the park exactly as from `Running` (§10.2). This mode-based
  design is what makes Part II §21's group restarts a bounded change.
  The ladder and the funnel together are the decision layer of §15.3:
  they only compute; the driver shell awaits.
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
  reason and pending startup result to `ShutdownRequested` — and, like
  the loop path, records the stop as observed, so the scope still exits
  at its parent as a cancelled `Completed` (§12). (`NeverStarted` sits
  outside the order — it is the terminal reason of a membership that
  never had an incarnation to drain.)
- Mailbox shutdown policy — `Drain` (the default) or `Discard` — is part
  of the actor options. It is a two-variant choice about exactly one
  thing: the fate of the frozen accepted prefix (§6.2). The intake
  freeze itself is unconditional and engine-enforced — new sends are
  rejected under either policy — so there is no separate "reject-new"
  variant. `Drain` delivers the frozen prefix before `on_stop`;
  `Discard` drops it — where and on what task per §5.5's
  destruction-venue clause, with disposal faults outside the exit
  verdict (§8). The blanket handler loop honors the policy itself; for a
  raw actor the framework enforces only the freeze — the loop owns
  delivery, so honoring the policy is the raw loop's documented
  obligation, using `RawContext`'s resolved-policy accessor and the
  `try_recv` drain primitive (B.1). Grace bounds the drain either way
  (§6.2).
- A local `ctx.stop()` runs the same ladder: the engine observes the
  self-stop and arms the child's configured `Shutdown` policy as the
  bound on its drain-plus-`on_stop` window (§6.2), so a wedged self-stop
  escalates — grace expiry, tidy beat, hard abort — exactly like a
  supervisor-initiated one. Self-stop changes who started the clock,
  never which ladder runs. One case defers the clock rather than
  changing it: an effective `AfterInit` initializer publishes its
  `stop()` only when it returns (§7), so an initializer that requests a
  stop and then never returns has not yet armed the ladder. Intake is
  already frozen and the readiness deadline still bounds it, but a
  `ReadinessDeadline::Unbounded` declaration leaves that wedge bounded
  only by parent or scope shutdown. Declaring an unbounded readiness
  deadline is what accepts that.
- Shutdown requests are **level-triggered, not queued**: owner drop,
  `request_shutdown()` / `request_scope_shutdown()`, and parent
  escalation each set an idempotent per-scope latch (a token edge, like
  the cancellation tokens themselves), which always succeeds
  synchronously. The unified event lane (Appendix A) is unbounded and
  processed in capped batches; awaited *insertion* operations
  (`add_*`/`define`) retain their payloads in producer-owned
  reservations. Fire-and-forget shutdown is lossless because it does not
  ride that lane. `remove` rides no channel either: removal is a forced
  stop, and it latches like one (§12's remove rule). The latch is
  **per-incarnation** state: it stops the scope incarnation it was set
  on and does not outlive it — a restarted scope incarnation starts with
  a clear latch (§12's nested-shutdown rule), so an `Always`-restarted
  scope cannot enter a stop/restart storm. One deliberate, named
  companion: the **pending-incarnation stop latch**. A stop request
  accepted while the membership has *no live incarnation* (a restart
  window — B.9's `shutdown_and_wait` landing between incarnations) is
  held on the membership and armed onto the next incarnation at its
  start, which then starts and immediately begins teardown. The two
  rules partition by target and never conflict: fresh-restart-starts-
  clear says a latch *consumed by* incarnation N never carries to N+1;
  the pending latch holds a request that arrived with no incarnation to
  consume it — it was never any incarnation's spent latch, and it waits
  for its first. What cannot exist is a stop request silently dropped in
  the window. Cancelling an awaited membership operation never rides the
  channel either: it is a per-operation level latch on the operation's
  cell (§9's linearization rule).
- Rebinding-transparent `send` during teardown can park against an
  unbound sibling; teardown-window notifications use `try_send`. This
  tradeoff is prominent shutdown guidance, alongside the
  grace-is-an-upper-bound contract above.
- **Resource-teardown discipline** — equally prominent guidance; the
  mechanisms live in §4.1, §6.2, §6.5, §9, and §12, the discipline is
  stated once, here:
  - `on_stop` is best-effort (§4.1): hard abort, process kill, and a
    panicking peer can all skip or truncate it. It exists to return
    resources promptly; correctness MUST NOT depend on it completing —
    durable integrity is the application's job (crash-only posture).
  - Grace is one budget shared by mailbox drain and `on_stop` (§6.2). A
    child owning a slow-closing resource sizes grace for drain *plus*
    close — or opts that mailbox out of draining (`mailbox_shutdown`).
  - Slow-closing resource owners take a long per-child grace and an
    early slot in an ordered scope: reverse teardown stops their
    dependents first, so the close runs quiescent with its full grace.
    Ordered graces sum — that is the deliberate cost, bounded by the
    owner's `shutdown()` timeout.
  - A *blocking* close can survive hard abort: `run_blocking` is
    available in `StopContext` (B.1) and its thread detaches past abort
    (§6.5). Async cleanup cannot — hard abort drops the future, and no
    async work outlives the incarnation by design (offloads are
    incarnation-owned and absent from `StopContext`).
  - A resource that outlives the incarnation does not belong to it:
    carry restart-surviving handles in `Args` (§4.1). The simplest shape
    needs no library feature at all: the host opens the resource before
    `spawn()`, hands clone-able handles in through `Args`, and closes it
    after `shutdown()` resolves — teardown order falls out of host code,
    and the close runs outside any grace budget. The dividing line is
    what restart should heal: a resource owned by `init` is reconnected
    by restart; a host-owned resource is outside supervision, and its
    failures are the host's (or the handle's own reconnect logic's) to
    handle. Host-own process-lifetime, self-healing resources (pools);
    incarnation-own connections whose failure the restart policy should
    repair.

Application resource integrity under teardown is explicitly outside the
framework contract — the discipline above is guidance for meeting it in
application code. (The ladder's transition invariants — one ladder per
stop, sampled request latches, driver-death discharge — are stated with
the engine's other decision-layer invariants in §15.3.)

## 12. Trees, spawning, and lifetime

- `Tree` / `DynamicTree` are the declaration layer; `spawn()` lowers into
  the engine and returns `System` (sole owner; drop = graceful shutdown;
  explicit `shutdown()` with timeout available). `Tree` lowers to
  `System<ScopeRef>` and `DynamicTree` to `System<DynamicScopeRef>`;
  subtree flavor likewise determines the returned handle (the dispatch
  rule below). `spawn()` is synchronous and requires an ambient async
  runtime; with none present it returns an error
  (`BuildError::NoRuntime` — the type is enumerated in B.8) — it never
  panics.
- **Builder operation inventory** (content-normative; Appendix B's
  naming latitude applies). One constructor per flavor — `Tree` for an
  ordered root, `DynamicTree` for a dynamic one. Per-scope configuration
  setters: `intensity` (§10.2), `defaults(ScopeDefaults)` (§10.3) —
  there is no strategy setter on either flavor, per §10.1. Membership
  declaration: the eight `add_*` entry points and the `reserve_*` slot
  family (§9) — on an ordered builder, declaration order is start order
  (§2); nested scopes declare through `add_subtree` /
  `add_subtree_once`, whose subtree veneer carries the
  `Inherit | Reset` defaults knob (§10.3) and the readiness deadline
  (§7, no mode knob). Builders are plain owned values: no interior
  mutability, no registration side effects — dropping an unspawned
  builder terminalizes its cells (§3.2) — and `spawn()` consumes the
  root (typed per the dispatch rule below). That inventory is
  exhaustive: a builder operation outside it is spec-extension, not
  implementation latitude.
- **Operational preconditions** (documented contract, checked where
  cheap): the host process runs `panic = "unwind"` — §8's `Panicked`
  classification is unwind-based, and under `panic = "abort"` a panic
  kills the process before supervision can observe anything (the
  documentation states this; there is nothing to detect). The ambient
  runtime is an async runtime with time enabled, reached only through
  the private runtime façade (§15.1). The owner resolves `shutdown()`
  (or drops `System` and lets teardown finish) before tearing the
  runtime itself down — destroying the runtime around a live system is
  outside the contract.
- `wait_started()` resolves when the whole declared tree is up, or
  reports terminal startup failure. **At the root, startup failure does
  not auto-roll-back the live started prefix** — that is the owner's
  decision — so a `start_or_shutdown()`-shaped composition is provided
  making the safe default (roll back the started prefix on startup
  failure) one call, not a pattern each host must remember. Its contract
  is pinned: it consumes the `System` and takes the rollback timeout as
  its trailing deadline (Appendix B's shutdown exemption applies — an
  escalation bound, not a failure deadline). On successful startup it
  returns the `System`; on terminal startup failure it drives the full
  `shutdown(timeout)` path over the started prefix and returns an error
  carrying the original structured startup failure (the same payload
  `wait_started()` reports) together with the rollback outcome,
  including any shutdown-timeout straggler report — rollback never masks
  the startup error that triggered it. The root parks in the
  `StartupFailed` scope state (B.6): the started prefix stays
  supervised, the never-started suffix is terminal (`NeverStarted`,
  §7/§8; a dynamic root has no such suffix — its initial members start
  concurrently, §7 — so the park holds every member, all still
  supervised). `StartupFailed` is a park of the scope *state*, not a
  dispatch mode: the exit funnel keeps dispatching the started prefix as
  `Running` (§11), so restarts schedule and charge intensity as usual,
  and a root intensity trip terminates the tree from the park exactly as
  from `Running` (§10.2 — observed through `wait()`, since
  `wait_started()` already resolved with the startup failure). Natural
  completion is the one dispatch outcome the park withholds: a parked
  root whose every membership terminalizes does not publish `Finished` —
  it stays in `StartupFailed`, the state the owner must act on (the
  finishing rule below is scoped accordingly). Beyond the intensity
  trip, the exits from the park are the owner's — `shutdown()` or drop.
  (`wait()` is a consuming await, B.10 — an owner watching the park for
  a trip has surrendered `shutdown(timeout)` and its straggler report;
  hold the `System` and observe through snapshots or lifecycle events
  instead when the structured rollback matters.)
- A **nested** scope has no external owner to hand that decision to: its
  owner is a parent supervisor whose whole vocabulary is child exits and
  restart policy, and a half-started subtree is not a state it can hold.
  Nested terminal startup failure is therefore scope-fatal with
  automatic rollback — the scope terminalizes its never-started members
  (`NeverStarted` — an ordered scope's unstarted suffix; a dynamic scope
  has none, §7), tears down its started members through the ordinary
  ladders (reverse order when ordered; cancelled at once and drained
  concurrently when dynamic — §7, §11), publishes
  `Stopped { reason: StartupFailed }` (B.6), and exits at its parent as
  an ordinary child `Failed` whose error is the structured,
  library-constructed startup-failure payload (§8 provenance; accessor
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
  tears down through the ordinary §11 ladders, publishes
  `Stopped { reason: ShutdownRequested }` (B.6), and exits at its parent
  as `Completed` with `Cancellation::Observed` (§8): a requested stop is
  a cooperative completion, not a failure. The parent's restart policy
  then applies as usual — `OnFailure` leaves the scope down; `Always`
  restarts it, and because the shutdown latch is per-incarnation (§11),
  the fresh incarnation starts with a clear latch rather than
  immediately re-stopping. (A scope torn down by its *ancestor's* own
  shutdown is not this case: the exit is recorded by the funnel, but the
  draining parent schedules nothing — §11's mode dispatch.)
- The owner has two further consuming awaits. `shutdown(timeout)`
  requests the §11 ladder and waits for the root driver's terminal
  epilogue. While a framework driver remains scheduled, it joins each
  child before completing. The timeout bounds the **cooperative** phase,
  not the return: at expiry the stragglers owned by scheduled drivers
  are hard-aborted and joined, and the call returns the structured
  shutdown-timeout error (§8) naming them. There is one recursive-join
  exception: if a framework driver misses its abort acknowledgement and
  its ancestor hard-aborts it at the tidy-beat backstop, the driver's
  synchronous `Drop` epilogue requests abort for its active children but
  cannot await their join handles. Those deeper tasks may finish
  cancellation and destroy their user futures after the owner's call
  returns. The return still joins the root driver and completes the
  target scope epilogue; that epilogue has requested stop or abort for
  each directly owned child. It does not claim either a recursive join
  or completed abort propagation through deeper fallback boundaries.
  `run_blocking` threads are a separate, unconditional
  detach-past-abort exception (§6.5) and are never joined. Expiry does
  not bypass the single ladder: the stragglers are driven through its
  abort tail — abort token, one tidy-abort beat, hard abort (§11) —
  concurrently, then joined while their scope driver remains scheduled.
  A zero timeout skips only the cooperative *wait*: every descendant is
  escalated immediately through that same abort tail (tokens still fire
  in order, the tidy beat still runs — `shutdown(0)` means "every child
  under `Abort` policy", §11, not a second mechanism), then the same
  driver-owned joins. The zero form is exempt from Appendix B's
  zero-budget-fails-immediately rule (stated there): this timeout is an
  escalation bound, not a failure deadline. Except for the documented
  hard-abort fallback and `run_blocking` detach boundaries, no teardown
  remains after return; consuming the owner makes that wait explicit.
  `wait()` awaits natural termination without requesting shutdown, and
  resolves with the root's terminal reason (B.6: `Finished`,
  `IntensityTripped`, or `ShutdownRequested` when teardown was requested
  concurrently elsewhere); `wait_started()` resolves once at startup and
  cannot observe a later trip, so `wait()` is the post-startup
  observation point.
- Natural completion is pinned exactly: once aggregate startup has
  completed, an **ordered** scope *finishes* when it has at least one
  membership and every membership is terminal (a retained terminal child
  counts — §9's retention is observability, not liveness); a root parked
  in `StartupFailed` is exempt — it never finishes (the park rule
  above). The finishing test runs **at each membership's
  terminalization, strictly before retention-based pruning** removes it
  (§9's remove-on-terminal default), and its result is latched — so
  pruning the final one-shot membership can never turn a finished
  workload into an idling empty scope, and pruning order is otherwise
  unobservable. The scope then publishes `Stopped { reason: Finished }`
  and, when nested, exits at its parent as `Completed` — completion
  cascades upward structurally, to the root and `wait()`. A **dynamic**
  scope never finishes on its own: open membership is its point, and
  "currently empty" is indistinguishable from "between members" — it
  ends only by removal, shutdown, or escalation (completion-driven
  lifetime is composed explicitly from `OneShotTaskRef` awaits plus
  `shutdown()`; §25 packages it). An **empty ordered** scope likewise
  idles indefinitely: completion requires a finished workload, not the
  absence of one — which is what keeps §16.1's zero-children root alive
  until its owner acts.
- `add_subtree` returns the handle type matching its input: mounting a
  `DynamicTree` yields a `DynamicScopeRef`; mounting a `Tree` yields a
  `ScopeRef`. Sealed-trait dispatch (`trait Subtree { type Ref; }`); no
  capability downgrade — and `spawn()` uses the same dispatch at the
  root: spawning a `DynamicTree` yields an owner whose scope handle is a
  `DynamicScopeRef`. The §4.2 mode split applies: `add_subtree_once`
  consumes a single-use tree value (`Never` structural); restartable
  `add_subtree` takes the declaration *source*,
  `impl Fn() -> T + Send + Sync + 'static` (`T: Subtree`), re-invoked at
  each restart to lower a fresh tree. `spawn()` consumes a tree value
  directly — the root has no supervisor and no restart, so no source is
  needed. `dynamic()` survives only as the runtime query for name-based
  traversal (§1 principle 4).
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
  (re)start, a once-tree mounted at runtime — validates identically, and
  a failure (a tree containing unfilled reservations, §9) is the **scope
  incarnation's terminal startup failure**: the incarnation starts
  (`Started` fires — the spawn was scheduled, so §10.2's attempt and
  intensity accounting see an ordinary incarnation), validation fails
  before any child membership is created, the rejected tree's cells
  terminalize exactly as a dropped unspawned tree's do (§3.2), and the
  incarnation exits `Failed` carrying the structured startup-failure
  payload with a **lowering cause** naming the undefined slots' child
  ids relative to that subtree root — the same data
  `BuildError::UnfilledReservations` carries, reached through B.5's
  `startup_failure()` accessor (§8 provenance, non-forgeable). The scope
  publishes `Stopped { reason: StartupFailed }` (B.6) and exits at its
  parent as an ordinary child failure; the parent's restart policy
  applies as usual — each retry re-invokes the factory, so a stateful
  source can heal, while a deterministic factory bug churns to the
  parent's intensity trip (§10.2), the designed containment. Each such
  attempt occupies a scope incarnation claimed *before* its factory
  runs: a factory invocation that panics or is torn down mid-invocation
  still spent an ordinary incarnation — observable on the scope's stream
  as that incarnation's own `ScopeState: Starting` → `Stopped` pair
  (B.4's restart-continuity rule) and advancing incarnation identity
  (§3.1) exactly like an attempt whose factory returned. In an ordered
  parent this is a pre-ready exit and §7's sequence rules apply
  unchanged.
- Dynamic membership operations: `add_*` and their `_once` twins resolve
  at admission with their exact per-kind handles (§3.2) — startup is
  never part of the call (B.8); `remove` is **idempotent at the API
  boundary** — removing an already-absent child is one unified
  already-absent outcome (success-shaped or a single variant; not
  distinct errors per handle flavor). Exact-handle removal (remove only
  the membership I hold) is supported and is the safe primitive for
  planned replacement: a stale handle never removes a same-id successor.
  Inserting a duplicate id while the incumbent is mid-removal is a
  distinct, documented rejection (the caller can await removal and
  retry).
- **The remove rule: level-triggered at the call, detached from its
  future.** `remove` is a forced stop, and it latches like §11's
  shutdown requests rather than riding the command channel: the call
  synchronously resolves its target and, on a resident match, flips the
  cell's removal latch — `membership_status` becomes `Removing` and that
  membership's restarts are suppressed from that instant (§8) — then
  returns a pure *observation* future resolving to
  `Removed | AlreadyAbsent` (B.8). The engine drives a latched removal
  to completion regardless of that future: dropping it — before first
  poll included — **detaches**, abandoning observation only, never the
  removal. There is no unsent, queued, or dequeued-but-unprocessed
  command state to race, because there is no command; backpressure
  concerns do not arise (latches are per-cell state, bounded by
  membership count). Concurrent removes — and a remove landing on an
  already-`Removing` membership — join the one removal and resolve with
  its one outcome; a target that resolves to nothing latches nothing and
  the future resolves `AlreadyAbsent` immediately. The asymmetry with
  §9's fused-add abort-on-drop is deliberate: aborting an abandoned add
  closes an unknown-outcome window (identity never delivered), while a
  removal's outcome is decided at the latch edge — abort-on-drop here
  would reopen exactly the uncertainty the latch closes, making drop
  timing decide whether a child lives.
- Single-use `Tree` values (moved on spawn/mount) are retained;
  rebuilding a tree from retained host state is the documented
  re-embedding pattern (validated by two full embed/run/stop cycles in
  one process, Appendix C).
- Task-first embedding (supervision with zero actors) is a supported,
  documented mode of the same façade — stated explicitly in the guide,
  since the actor-oriented naming otherwise hides it.
- There is deliberately **no** per-child kill/restart/pause control
  surface: restart is policy-driven; the only forced stops are scope
  shutdown and dynamic removal. Snapshots carry ids and states, not
  senders — messaging an arbitrary actor requires holding its typed
  `ActorRef` (wired at build time or via a userland registry). Anything
  holding a `ScopeRef` has that scope's full observation/control power;
  "operator" is a role, not a framework-enforced privilege level.
- Completion-driven lifetime and the scope-relative sibling-readiness
  barrier are Part II (§25); in core, compose them from `OneShotTaskRef`
  awaitables / `wait_started()` plus an explicit `shutdown()`.

## 13. Engine event ordering

When one driver wake makes several engine events eligible, their
processing order is a stable total order over event classes — pinned here,
never left to select-arm luck or runtime configuration. The driver first
collects all currently eligible inputs, stable-sorts them by class, then
reduces them one by one before flushing effects. Items within one class
retain their stable source order, and each pending item derives its class
from its own variant — a caller cannot supply a conflicting class beside
an item.

| rank | class | transition obligation |
|---:|---|---|
| 1 | scope shutdown | enter or upgrade drain before any child policy decision |
| 2 | membership removal | mark the child `Removing` before exit/readiness dispatch |
| 3 | child exit | record and route the terminal outcome before readiness/deadline artifacts |
| 4 | readiness signal | accept an already-fired signal at the exact deadline |
| 5 | readiness deadline | terminalize only if readiness did not win |
| 6 | backoff due | spawn only after newly observed terminal facts |
| 7 | stop deadline | advance only ladders not completed or disarmed above |
| 8 | queued admission | reject after every same-wake terminal fact is applied |

The rationale is part of the contract:

- Scope shutdown precedes removal because teardown owns all stops once it
  begins; both precede child exits, so an already-observed stop
  suppresses restart scheduling and its intensity charge.
- A readiness signal precedes its deadline, so ready-at-deadline wins
  (§7).
- Child exits precede both readiness classes, making an incarnation that
  has already ended in the same wake an exit rather than a spurious
  readiness edge. Exit handling nevertheless consults the incarnation's
  retained readiness latch: a signal causally fired before that exit is
  accounted before the exit is classified (§7's startup accounting),
  without reordering the event classes themselves.
- Backoff work follows all newly observed terminal facts, and ladder
  deadlines follow because the earlier facts can complete or disarm
  them.
- Queued admissions run after all already-observed terminal and temporal
  facts, so they cannot enter a scope that the same wake has made
  non-admitting.

Class position is not, however, what protects a leaving membership from a
spurious readiness edge: a child's self-stop shares membership removal's
class, so a queued removal cannot be ordered ahead of the readiness that
self-stop dispatch replays. That rule is structural instead — the
readiness publication site consults the membership's removal mark at
execution time (§7, §8), the same discipline exit dispatch follows for
restart suppression.

## 14. Observation

Core ships two independent, restart-stable contracts, both rooted in the
engine's single publication path (child metadata rides the same path with
the same fencing — there is no second, separately-fenced view):

1. **Snapshots** — conflating watch of recursive current state, plus the
   `wait_for_child` helper (contract pinned in B.9). Snapshots expose
   membership and incarnation identity as the §3 types; a snapshot is a
   pure projection of decision-layer state, published by the shell
   (§15.3). Field inventory: Appendix B.6. Alignment with the
   no-subscriber publication skip (§15.6): `snapshot()` is computed on
   demand from current decision-layer state — never served from the last
   value pushed into the watch — and a new subscription's initial value
   is computed at subscribe time, so the skip is invisible to every
   observer: a lifecycle subscriber that reads `snapshot()` sees §16.14's
   consistent-or-newer guarantee whether or not any snapshot subscriber
   ever existed.
2. **Lifecycle events** — ordered, bounded stream (event inventory,
   ordering, closure contract, and the membership-owned sequencing that
   makes both contracts stable across subtree restart: Appendix B.4;
   buffer size: Appendix A; the buffer is per subscriber, so a lagging
   reader drops only its own view); overflow drops oldest and coalesces
   into one leading `Lagged { dropped }` marker (a subscription-level
   stream item, not an event — B.4); event staging aligns with snapshot
   publication so an event-woken reader always sees a
   consistent-or-newer snapshot (the conformance test reads the snapshot
   *synchronously inside the event arm*, §16.14). Cumulative counters
   distinguish crash restarts from planned remove/add.

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
dynamic removal: the removed id becomes reusable (and a repeated
`remove` reports `AlreadyAbsent`) only at the commit that withdraws the
member from residency and publishes its `Removed` edge — §2's
resident-membership uniqueness holds at every observable cut.

Drain entry follows the same rule: publishing `Draining` includes the
terminal-disposal intent for every inactive child selected to stop in
that same driver step. A zero-budget shutdown's straggler sample cannot
split that entry step, so restart-window cleanup already committed by it
is not a straggler; an active child, or an ordered sibling whose stop has
not yet been selected, remains reportable. The guarantee covers that
entry step only: an ordered scope stops its children one step at a time,
and every step after entry races the sample by design, so whether a later
sibling has already terminalized when the sample runs is
schedule-dependent and both outcomes conform.

A transaction may contain several of those transitions — initial batch
admission is the canonical example. Snapshot-watch publication is
coalesced per scope hub inside the transaction and the single final
projection is installed at commit, before the observation gate is
released; the only install that may precede commit is a new subscription
seeding its own starting value, which no receiver is yet attached to
observe. Consequently the ungated `borrow_latest` surface and
`wait_for_child` see either the prior committed cut or the transaction's
final cut, never a partial batch. Watch conflation remains permitted only
*between committed transaction cuts*; it is not permission to expose the
transaction's internal publications.

`tracing` spans emit from one choke point (the optional `metrics` surface
is Part II §22). Everything else observational — peer monitoring, actor
statistics, the self-recovering child-observation reducer, the packaged
restart-counter view — is Part II (§20, §22): all are adapters over these
two streams and the §3 identity types, which is what makes them safely
deferred.

## 15. Construction requirements

Everything in §§1–14 is observable contract. This section is different in
kind: it constrains *how* a conforming library is built. The rules here
are normative anyway, because each is the only known way to make the
observable contract hold under hostile user code, concurrent teardown, and
review at scale — a library that meets today's test suite without them has
merely hidden the failure modes these rules make structural. Internal
names used below (`step`, `ingest`, ladder, cell) are descriptive
vocabulary, not required identifiers.

### 15.1 Layering

The implementation is structured as four layers, each complete before the
next begins, so simple things are easy at the top and every layer below is
a reachable escape hatch:

- **L1 — engine**: scopes, membership cells, incarnations,
  spawn/lowering, exit classification, the shutdown ladder, intensity,
  readiness gating. The boundary is execution, not vocabulary: L1 models
  all three child kinds as typed variants (§2) and carries their declared
  policy — mailbox settings included — as plain data (§9); what it
  contains none of is actor *execution or mailbox mechanics* — no message
  delivery, no mailbox operations, no actor callbacks (§12's task-first
  embedding — supervision with zero actors — is this layer plus the tree
  façade).
- **L2 — raw actor + mailbox**: `RawActor`/`RawContext`, the mailbox
  kinds, the send flavors, `call`. The escape hatch — per §1 principle 5
  it ships with core, not after it.
- **L3 — handler actor**: `Actor`/`init`/`handle`, keyed timers,
  continuations, offloads. The "simple things easy" layer. Its public
  `Handler<A>` wrapper is the composition point for raw decorators and
  encapsulates the callback loop's error-path freeze-and-join discipline;
  decorators need no framework-internal teardown surface.
- **L4 — observation**: snapshots and lifecycle events over L1's single
  publication path.

**The runtime boundary.** All integration with the ambient async runtime
(and with any randomness source) is confined to one private runtime
façade; nothing above it names a runtime type, and no runtime or adapter
type is reachable from any public item (§1 principle 7, checked per
§16.13). Each mailbox receives one type-erased runtime capability object
through that façade — one-shot delivery, change signals, isolated
disposal, and the clock/timer pair — and the same object flows through
reply channels and deadline futures, so virtual-time and disposal
semantics cannot silently switch adapters. Runtime choice is deliberately
absent from `ActorRef<M>`'s type parameters, and the mailbox layer is
runtime-neutral in its production dependency graph.

**The supported boundary.** Cross-crate implementation seams that must be
`pub` for the implementation's own crates to reach each other are not
user surface: the supported façade exports neither those traits nor their
installation paths, and implementing or installing them through a direct
dependency on an implementation crate is unsupported — it voids the lock
discipline (§15.4) and identity-pairing contracts, which assume
framework-only implementations. How the boundary is enforced (visibility,
sealing, documentation-reachability checks) is implementation-defined;
that the supported surface excludes the seams is not.

### 15.2 Policies are plain data

Every policy and configuration surface — `RestartPolicy`, `Backoff`,
`Shutdown`, `Readiness` and its deadline, `Strategy`, `Intensity`,
mailbox settings, the shared options record (§9) — is a plain enum or
struct: `Clone` (`Copy` where cheap), `Eq` (universal — a float-valued
field stores a validated newtype whose invariant makes bit-equality
correct: §10.2's backoff factor), serializable where Part II §23 needs
it, and carrying **no behavior** beyond small pure derivation functions
of the `should_restart(exit)` / `next_delay(restart_attempt,
JitterSample)` shape. Runtime behavior is *derived from* the data by
local functions; it is never encoded as trait objects, callbacks, or
builder side effects.

Plain data is not *open* data: every payload carrying an invariant
(§10.3's eager validation — non-zero durations, the backoff factor, the
intensity window, mailbox capacity) is a **sealed** struct or newtype
whose only mint is its validating constructor, with read access through
accessors. Making the invalid unrepresentable is what retires validation
as a re-runnable step: there is no second boundary that could re-reject a
value, so no `validate()` survives on the public surface and no error
variant exists downstream to report one (B.8). Partiality that is
legitimate *declared* state — an unset scope default, a deferred mailbox
capacity, an inherited deadline — stays openly representable; only the
values inside it are sealed.

### 15.3 Pure decision core, mutable shell

The engine's decision layer is a synchronous state machine:
`step(state, event) -> effects`, where `event` carries everything
external as data (a child exit, a command, a deadline having been
reached, `now` as an argument, a pre-drawn jitter sample) and `effects`
is data describing what the shell should do (spawn this, abort that, arm
a deadline, publish a snapshot). Decision modules contain no awaits, no
clock reads, no channel operations, no spawns, and no dependency on any
async runtime — only the thin driver shell does, feeding events in and
executing effects out. This is what makes policy behavior unit-testable
with no runtime at all (§16), and it is why the stop ladder (§11) is
`advance(now) -> Option<action>` rather than a sleeping task per child.
The same shape applies opportunistically to the actor loop's selection
policy (§6.1's fairness and timer retraction): decide the next action as
a function of observed loop state, then await it. User code — actor
bodies — is of course effectful; the constraint binds the engine.

**Decision-layer invariants.** However the implementation names its
states and events, its transition system MUST satisfy the following;
each is checkable by driving the decision layer directly — events in,
effects out — with no runtime. They are the engine-side halves of the
observable rules in §7, §8, §11, and §12.

*Readiness and startup:*

- **R1.** Only initial memberships gate scope startup; a runtime admission never
  enters the readiness aggregate.
- **R2.** Readiness is incarnation-local and monotone until restart: an accepted
  readiness transition flips an initial member's bit `false → true`;
  duplicate readiness is a no-op; a restart-pending transition resets the
  bit only while startup is incomplete; once the aggregate has fired it
  never rewinds.
- **R3.** Removal is sampled at the publication transition: the readiness event
  carries the synchronous removal-latch sample; a true sample first marks
  the membership `Removing` and the readiness edge is then rejected. A
  committed removal shrinks the initial set only at the reclaim step that
  follows `Removed` publication and precedes the removal response's
  resolution.
- **R4.** Ordered start is one accepted edge at a time: the settlement step emits
  a start effect only for the current initial cursor, and advances the
  cursor only past a spawned-and-ready member or a reclaimed slot,
  reaching every initial member in declaration order. Dynamic startup may
  emit one accepted start per unspawned initial member.
- **R5.** Settlement effects are acknowledgeable: every emitted start effect
  names a resident child in an unstarted or restart-pending state —
  exactly the set the spawn acknowledgment accepts — and a settlement
  pass that emits no acknowledgeable work is already at a fixed point:
  re-entering settlement on an unchanged state reproduces its start
  effects and nothing else, so a level-triggered settle loop cannot spin.
- **R6.** Aggregate completion is derived, emitted at most once, and never while
  any initial record remains unready — including one already latched for
  removal, which leaves the initial set only at reclaim. The empty
  initial set satisfies the predicate. For a dynamic scope that predicate
  is also sufficient; ordered startup adds its cursor, so a member
  latched for removal at or ahead of the cursor withholds completion
  until the removal commits even when every initial member is ready.
- **R7.** Terminal pre-ready failure is flavor-independent, rollback
  position-dependent: the exit funnel applies restart policy first; a
  terminal pre-ready failure emits the startup-failure transition; an
  ordered suffix becomes `NeverStarted`, while a dynamic scope has no
  unstarted suffix; root failure parks, nested failure begins ordinary
  rollback (§7, §12).

*Exits:*

- **E1.** Record, destroy, join, publish: one owned report token records the run
  outcome; dropping it records the fallback; no consumer sees an exit
  until the incarnation is destroyed and joined, and exactly one report
  is then published (§8's two-phase rule).
- **E2.** Verdict precedence is total and table-driven (§8), with the one
  deliberate asymmetry: a recorded `Failed` survives a later abort; a
  recorded `Completed` does not.
- **E3.** Cancellation is orthogonal sampled state: the exit records `Observed`
  iff the incarnation cancellation latch had fired when the outcome was
  recorded; restart eligibility never inspects this field.
- **E4.** One authoritative membership/incarnation state: incarnation phases
  advance only through
  `Unstarted → Active → Stopping? → Complete → RestartPending | Disposing
  → Joined`; stale or out-of-order events are total no-ops. Removal
  changes the enclosing membership state monotonically to `Removing`,
  never a parallel flag.
- **E5.** Restart suppression is state-derived: exits schedule restart only while
  the scope is running and the membership is resident; draining or
  `Removing` records schedule nothing and charge no intensity.
- **E6.** Publication fences replacement: a replacement spawn follows the
  predecessor's terminal publication; stale incarnation evidence cannot
  affect the replacement.

*Shutdown:*

- **S1.** One ladder per stop, with the accepted transitions of §11 and
  `force(now)` only ever moving the current deadline earlier, never
  skipping the tidy beat.
- **S2.** The stop policy has no zero-grace branch (§11); `Abort` records
  `WithinGrace`, graceful expiry records `AfterGrace`.
- **S3.** Flavor owns sequencing, not mechanism: drain entry initializes a
  reverse cursor for ordered scopes and emits one stop per incomplete
  child of a dynamic scope; the ordered settlement exposes at most one
  incomplete child and does not advance until it joins.
- **S4.** The drain reason is the monotone lattice of §11; forced shutdown also
  sets the hard-force fact and emits one force effect per incomplete
  child.
- **S5.** Completion is derived and level-triggered: "all children joined" is
  derived from child states, and the finish effect is emitted once iff
  the flavor-specific finish predicate accepts that derived value.
- **S6.** Shutdown requests are sampled latches (§11): synchronous, idempotent,
  sampled into decision events at step entry; a request consumed by
  incarnation N does not reach N+1; a request accepted with no live
  incarnation is owned by the membership until the next incarnation
  begins.
- **S7.** Driver death discharges owned obligations: destroying a driver
  terminalizes active memberships, resolves admission/removal/shutdown
  completions, and closes observation only after terminal publication;
  the synchronous fallback may sacrifice post-join precision but may not
  leave a promise parked (§15.5, §16.17).

*Trees and lifetime:*

- **T1.** Declaration consumes into one typed root (§12's dispatch rule);
  `System` is the sole non-cloneable owner and its drop latches shutdown.
- **T2.** Root and nested startup failure diverge only after the same decision
  transition (§7, §12).
- **T3.** Natural completion is flavor-policy derived (§12): ordered scopes may
  finish after every membership joins; dynamic scopes, and ordered scopes
  holding a perpetually restartable member, do not finish merely because
  their current resident set is empty or terminal.
- **T4.** Dynamic admission has one linearization path (§9): reservation owns id
  uniqueness and payload; an admission event inserts one authoritative
  child record and resolves independently of child readiness;
  cancellation chooses withdrawal or removal by whether admission won.
- **T5.** Removal is exact, idempotent, and monotone (§12): exact-handle removal
  compares membership; id-only removal names the current resident; once
  sampled, the state stays `Removing` through stop, terminalization,
  finalization, and reclaim.
- **T6.** Non-owning scope shutdown targets an incarnation (B.9): a live request
  resolves after that incarnation's scope epilogue; a restart-window
  request arms the next incarnation; a membership that never spawns
  resolves at terminality.
- **T7.** Dynamic capability is explicit: `DynamicScopeRef` exposes only dynamic
  operations inherently; shared observation/control is reached via
  `as_scope() -> &ScopeRef`; there is no mirrored forwarding block and no
  `Deref` conversion.

### 15.4 Lock discipline

A critical section over a framework mutex manipulates plain
framework-owned state and nothing else: wakes, destruction of user-owned
values, formatting of user types, user callbacks, and panic resumption
all happen after the guard is released. The framework runs user code it
did not schedule — waker vtable functions, destructors of messages, actor
state and the type-erased error inside an `Exit`, `Hash` on a timer key —
and every one of those may panic, block, or re-enter, which under a lock
is a poisoned mutex, a deadlock, or an abort during unwind. The mechanism
is an effects value that accumulates the user-visible work inside the
critical section and discharges it, panic-contained, from `Drop` — the
observation transaction and the mailbox transition are the two reference
shapes, and moving a value out (rather than dropping it in place) is the
degenerate case. What the rule buys is that a hostile waker or destructor
is an ordinary, testable outcome instead of a liveness failure (§16.18).

Applied to the mailbox layer (§5): mailbox and send-operation mutexes
protect only synchronous state transitions. A transition records signal
pulses, waker wake/drop actions, displaced payloads, and
isolated-disposal requests in an effects sink paired with its guard; the
guard is released before that sink can flush. On every path a transition
can complete — acceptance, rejection, withdrawal, terminal teardown, and
an unwind out of any of them — no waker vtable, message destructor,
signal callback, or runtime-disposal capability runs under either mutex.
The one carve-out is a framework invariant break (identity-space
exhaustion or an unreachable binding state): those unwind with the
payload still under the guard, and they poison the mutex regardless, so
the transition is abandoned rather than completed. The registered-waker
slot exposes no operation that returns or replaces a waker without an
effects sink, making an under-lock caller-code drop structurally
unrepresentable rather than a call-site convention; cancellation returns
one withdrawal outcome plus its post-unlock effects object, never a
tuple carrying a raw waker across the boundary.

Two conventions travel with the rule. Panicking while holding a mutex the
implementation expects unpoisoned poisons it for every later caller —
compute the verdict, release, *then* panic; where releasing is
impossible, debug-assert instead. And a value that may block on
destruction goes to detached disposal (§5.5's venue), not merely past the
unlock.

### 15.5 Promises are owned completions

Every cross-task promise — an admission or removal awaiting resolution,
an exit report awaiting publication, a resident child awaiting its
`Removed` edge, a member cell awaiting terminality, a waiter awaiting a
wake — is held as an owned value whose destructor discharges it,
fail-closed, with a **synchronous** fallback: complete with the terminal
rejection, publish the coarse exit, emit the edge, pulse the signal —
never an await, never a join. The event loop that services the orderly
path is an optimization of these values' consumption, never the sole
guarantor: when a driver future is destroyed at any await point —
hard-abort cascade, panic, natural return with events still queued —
unwinding alone MUST discharge every outstanding promise (§11's
driver-death rule, §16.17's test anchor). Two corollaries are normative.
Residency in a scope's observed child set is itself such a value — its
drop emits `Removed`, making §3.2's exact pairing structural rather than
remembered. And each cell has exactly **one** change signal from which
every compound wait derives — a second wake path is a lost wakeup
waiting to be written.

### 15.6 Performance posture

The decision layer is a control plane — its events are exits, restarts,
commands, and deadlines, rare even in a restart storm — so its purity is
not a performance concern and is never relaxed. The hot path
(`send → mailbox → recv → handle`) lives in the shell, which the shape
above never constrained. Three permissions are normative so that "pure"
is not misread into pessimization:

- `step` MAY take `&mut State` — pure means no I/O, clocks, randomness,
  or awaits; it does not mean persistent data structures or defensive
  clones.
- Effects MAY flow through a caller-owned buffer
  (`step(&mut state, event, &mut Vec<Effect>)`) rather than a returned
  `Vec`, which a caller MAY reuse across steps. Reuse is a permission,
  not a guarantee: a caller that drains the buffer by moving out of it
  gives the allocation back on every flush.
- The actor loop's next-action decision is a small `Copy` enum, not a
  boxed plan.

Where measured cost actually concentrates, the mitigations are (SHOULD):
snapshots published as `Arc`-shared immutable values (clone = refcount);
publication skipped when no subscriber exists — a subscriber-channel
optimization only: pull-side `snapshot()` is an on-demand projection and
can never go stale from the skip, and a fresh subscription's initial
value is computed at subscribe time (§14) — (the conflating watch already
makes cost O(observations), not O(events)); per-message `tracing` built
lazily and near-zero with no active subscriber. What stays unsanctioned
even under measured pressure: clock reads or randomness inside decision
modules (pass `now` and samples in — it costs nothing), dispatch paths
that bypass the exit funnel, and per-call-site staleness shortcuts —
each of these is a known failure mode of impure supervision engines in a
performance costume, and no supervision engine has been observed hot
enough to justify one. Measure before relaxing anything else.

## 16. Conformance obligations

A conforming **core** implementation satisfies all of the following, and
each MUST have direct test coverage. Clauses that activate with a Part II
feature are marked. Each entry names the adversarial situation the
obligation is about and, where the effective provocation is non-obvious,
how to construct it — these provocations are a floor, not a ceiling.
Several presuppose a controllable clock (virtual time with explicit
advance), per-child sequencing gates, drop-detection guards, and bounded
negative assertions ("this event does *not* arrive within the window");
where §15.3 is followed, prefer a lower gear first — drive the decision
state machine directly, events in, effects out — and reserve integration
fixtures for the driver shell and end-to-end invariants.

1. **Exactly one owner per running system; drop = graceful shutdown.**
   Provoke both directions: drop every non-owning handle and assert a
   quiet window (nothing stops), then drop the owner alone and observe
   cancellation; cover a zero-children root and a fire-and-forget
   `let _ = tree.spawn()`. Ownership itself is type-enforced
   (non-`Clone`, `#[must_use]`).
2. **Refs address memberships; sends ride restart windows; terminality is
   the only hard send failure.** Park a send on a full capacity-1 mailbox
   of a never-receiving actor, fail the incarnation, and run the same
   fixture under three restart policies to get all three outcomes
   (ride-through / terminated / timed out). Freeze the rebind window
   itself with a destructor-blocking actor and hand-poll a boxed send
   future to prove `Pending` inside the window, then release and assert
   delivery to the next incarnation. Cancellation and expiry are
   linearized at acceptance (§5.2, Appendix B): dropping a parked send
   before acceptance withdraws it — a quiet window proves it is never
   delivered; dropping after acceptance still delivers; `send_timeout`
   and `call` report their not-accepted outcome only after withdrawal
   succeeds (message recovered and safely re-sendable), and an
   acceptance winning the race at the deadline instant resolves the
   operation — provoke the boundary with an actor that accepts without
   reading, released exactly at the deadline under virtual time.
   *(Pinning clause activates with Part II §17: a pinned ref fails fast
   across restart while the membership ref rides through.)*
3. **At-most-once delivery; nothing buffers across incarnations.** An
   actor that accepts without reading takes several messages, the first
   delivered one poisons the incarnation; assert the queued remainder is
   never seen by the next incarnation while a freshly-sent message is.
   Repeat the shape for `call` (accepted-then-killed ⇒ `ReplyDropped`),
   offload completions, and timers.
4. **One staleness primitive; no bare fencing integers; stale resolution
   fails closed.** Manufacture an identity collision: two scopes each add
   a child with the same id (and, by construction, colliding internal
   coordinates), then present scope A's handle to scope B and require
   rejection. Replay remove→re-add under one id and assert the stale
   handle fails while the replacement is untouched and incomparable with
   it; assert that different ids and different owning scopes are also
   incomparable. Repeat the fail-closed comparison check for a declared
   child replaced at runtime and for a corresponding descendant rebuilt
   after a nested-scope restart. Exhaustion mints nothing (§3.1): drive a
   counter to saturation and assert no duplicate token is ever issued —
   an unmintable incarnation terminalizes the membership as under
   `Never`; an unmintable membership is the enumerated reservation
   rejection (B.8), or structured nested-startup provenance when a stable
   scope cannot rebase a produced declaration. The structural half is
   enforced by API shape (no public bare integers) and review, not tests.
5. **One-shot construction drops its resources exactly once across: init
   panic, startup failure, shutdown-before-start, normal exit.** One
   drop-counting guard type owned by the args, four tests asserting
   exactly one drop each; the shutdown-before-start case cancels the
   in-flight add/start operation mid-flight (select against it) and
   asserts the fallback effect fired. A oneshot sender consumed inside
   `init` doubles as the consume-exactly-once witness on the happy path.
   The four paths run for **every one-shot kind form** — task, actor, raw
   actor (owned `R` as the resource), and subtree (the owned tree value
   as the resource) — not just the kind that first made the fixture easy.
6. **Readiness is declared, engine-enforced, deadline-bounded unless
   explicitly unbounded; a propagating decorator that awaits before
   delegating cannot release the gate early** (the visibility clause — a
   decorator that omits propagation reports the default `Immediate`, an
   ordinary testable bug, §7). Gate with an `init` parked on a release
   gate and a shared order log; assert the later sibling never appears
   until release. Deadline expiry under virtual time yields the *typed*
   readiness verdict carrying the deadline. Edge tests: ready-at-deadline
   beats timeout; readiness fired before an immediate clean exit,
   failure, or self-stop counts before that terminal edge; shutdown
   disarms a pending deadline; a *terminal* pre-ready exit aborts startup
   while an eligible restart re-runs the gate with a fresh
   per-incarnation deadline (§7) — on abort the never-started siblings
   terminalize `NeverStarted`; at the root the started prefix stays
   running, while in a nested scope assert the automatic rollback and the
   structured startup-failure exit at the parent (§12). The dynamic half
   (§7's concurrent-start rule): initial members start concurrently; a
   terminal pre-ready exit fails startup with **no** sibling terminalized
   `NeverStarted` — at the root, every other member (running or still
   starting) stays supervised through the park; a nested dynamic scope
   rolls back concurrently (runtime-added members included) and exits
   with the payload naming exactly the triggering child's id and exit.
   **Regression:** an inner `AfterInit` actor behind a decorator that
   awaits before delegating still gates ordered startup (§7). Aggregate
   readiness is initial-members-only and monotonic (§7): a runtime
   addition never joins the aggregate (quiet window on scope readiness
   while a gated runtime member sits unready); an already-ready child
   restarting before the aggregate fires holds it open, and one
   restarting after it fires does not rewind it. Removal shrinks the
   declared set (§7), pinned by public-API repros that hang an unfixed
   driver for a full virtual timeout: removing the sole unready initial
   member completes startup; so does removing the last unready one beside
   a ready sibling, removing every initial member concurrently (the empty
   declared set), and removing the sole unready member of a *nested*
   dynamic scope — which must publish the aggregate up to an ordered
   parent and release its gated sibling. The negative pins the guard:
   removing an *already-ready* initial member while an unready one
   remains leaves the scope quietly `Starting`.
7. **Exactly one published exit report per incarnation, on every path
   including panic and abort; publication is post-join (§8's two-phase
   rule); one runner, one exit type.** The two hard provocations: an
   actor that panics in its `Drop` *after* the run path recorded
   `Completed` must publish exactly one report, and it reads `Panicked` —
   the destructor verdict supersedes the recorded outcome; and a dropped
   in-flight incarnation followed by its replacement must publish the old
   exit, bound to the old incarnation token, strictly before the
   replacement spawns (§8's sequencing rule), with detached stragglers
   (an aborted `run_blocking` thread's observations) fenced to the old
   incarnation. The containment boundary (§8): a `handle` panic unwinding
   toward an actor whose `Drop` also panics is caught at the callback
   boundary before actor destruction — the process survives and exactly
   one report publishes, `Panicked` with the callback's payload. The
   single-runner property is tested internally in core; *(the public
   hosted-parity clause activates with Part II §24: host the same actor
   without a supervisor and assert the identical exit value, including
   `Panicked` — no user `catch_unwind`)*.
8. **Framework verdicts never travel through the user-error channel.**
   Provoke each verdict (readiness expiry, grace-expiry abort, cancelled
   completion) and match the typed variant — never a stringly `Failed`.
   Enforce the mechanism structurally: core classification consumes typed
   recorded-outcome and join-outcome values and contains no `Any` or
   downcast path; the runtime adapter converts its join result before the
   classifier sees it, and user error erasure stays at the public
   boundary. Add the forgery probe: an application error type imitating
   the intensity-trip or startup-failure payload arrives as an erased
   user error — `intensity_trip()` / `startup_failure()` return `None`
   for it. Where the implementation spans crates, the functions that mint
   authenticated provenance may need to be technically public for its own
   crates to reach; they are then implementation seams outside the
   supported boundary (§15.1), and the property still holds for every
   consumer of that boundary: the blanket user conversion cannot produce
   an authenticated payload.
9. **Every respawn charges the scope intensity budget.** Terminal removal
   cannot mask exhaustion; the window ages out under virtual time;
   backoff progression is unit-tested as pure math. The over-budget edge
   is exact (§10.2): the tripping charge advances the attempt counter and
   `total_restarts`, its `RestartScheduled` precedes the scope's own
   failure in the emitting scope's event order, and the scheduled restart
   never spawns. *(Group clause activates with Part II §21: under
   `OneForAll` with budget N, sibling respawns forced by one child's
   failures consume the same budget — engineer failures so the group
   trips the scope budget even though no single child exceeds a per-child
   count.)*
10. **Ordered teardown is reverse-declaration-order with full per-child
    grace; escalation follows the single ladder;
    cancellation-before-escalation ordering is observable.** Three
    children park on their cancellation, report, then park on per-child
    release gates; interleave positive assertions ("third cancelled")
    with quiet windows ("second not yet"), releasing one at a time;
    finally assert the exited order is exactly reversed. Grace expiry on
    a stubborn cursor child: a drop-flag guard proves the future was
    aborted *and joined* before the ladder advanced; a wrapper that
    overruns the tidy beat is hard-aborted; an aborted ancestor cascades
    recursively; ordered graces sum while dynamic graces run
    concurrently.
11. **Outlines are injective over trees that differ in the declaration
    surface the outline carries (§23).** *(Activates with Part II §23.)*
    Build tree pairs differing in exactly one dimension — a default, a
    mailbox kind, a capacity, a readiness deadline, a policy — and assert
    the outlines differ; serde round-trip equality; mutated-JSON tests
    prove unknown fields are rejected and required keys missed loudly.
12. **Dynamic mutations resolve at admission with usable identity — they
    never await startup; removal is idempotent.** Await an `add_*`
    against a child whose startup is gated shut (a parked `init`, an
    unreleased `Manual` gate) and require it to resolve anyway; use the
    returned handle (snapshot subscription, removal) while the child has
    still never started; subscribe from a pre-spawn handle (initial
    value: the `Unstarted` scope snapshot / `Admitted` child state — B.4,
    B.6) and follow the same identity through startup. Run
    remove→re-add→remove-with-stale-handle for all three kinds;
    double-remove yields the single already-absent outcome. Drop an
    in-flight `add_*` future at provoked pre- and post-admission points
    (§9's fused abort-on-drop rule): afterwards either the id is free and
    the cell terminal, or the child has been removed — a quiet-window
    scan proves no identity-less child survives, and a subsequent same-id
    add succeeds. Drop an in-flight split `define` future at the same two
    points (§9's detach rule): admission proceeds, and the child
    survives, observable and removable through the slot's pre-taken
    handles — or, on a rejected define, the cell terminalizes.
    Stage-exactness (§9's stage rule): `reserve_*`/`add_*` against a
    pre-spawn handle, a draining scope, and a restart window each fail
    `NotAdmitting`; `remove` during drain resolves `AlreadyAbsent`; a
    same-id add while the incumbent is mid-removal is the distinct
    `RemovalInProgress` rejection and succeeds after awaiting the removal
    (§12, B.8). Race a queued split `define` against removal of its own
    reservation: the remove-by-id lands first, the define resolves
    `NotAdmitting` with the reservation-ended cause (B.8) while the scope
    keeps admitting other children, and a subsequent same-id reserve
    succeeds. Tombstone occupancy (§9's retention semantics): a retained
    terminal child blocks a same-id add with `DuplicateId` until removed;
    `remove` on the tombstone resolves `Removed`, fires the `Removed`
    event, and frees the id for a successful re-add; `child(id)` on the
    tombstone returns the terminal snapshot. A scope membership
    terminalized with no incarnation ever spawned publishes the final
    `Stopped { reason: NeverStarted }` snapshot and terminal event, and
    `wait_stopped()` resolves with it (§3.2, B.6). `remove` detaches from
    its future (§12's remove rule): drop an in-flight remove future —
    never-polled included — and assert the removal still completes,
    `membership_status: Removing` having flipped synchronously at the
    call; concurrent removes resolve one shared outcome. A pre-spawn
    `shutdown_and_wait` arms the pending stop latch, its timeout arming
    only at teardown start (B.9). §11's pending-incarnation stop latch: a
    stop request landing in a restart window stops the next incarnation,
    while a latch consumed by a previous incarnation never carries
    forward — no stop/restart storm under `Always`.
13. **No runtime or runtime-adapter types are reachable from public
    façade items.** Runtime and randomness integration is confined to the
    private runtime façade (§15.1); the layers above it carry no such
    dependency, so no public item can name one. The check is real, not
    implied: an automated walk over the public API (e.g. rustdoc-JSON
    reachability) MUST reject public reachability of runtime and adapter
    types, run over every crate that contributes public façade items —
    cross-crate re-exports are invisible to a single-document walk.
    Cross-crate implementation seams that are technically public, and
    hidden bridge items a façade needs from a lower crate, are outside
    the supported boundary per §15.1; what holds them out of the public
    surface (visibility shims, hidden-item conventions, an
    external-consumer probe) is implementation-defined, but a public
    re-export of a runtime-typed seam MUST be a hard failure —
    compile-time where achievable — not a documentation nicety. Public
    adapter types on the event lane are opaque wrappers, never aliases of
    runtime types, and internal buffer capacities are not façade API.
14. **Event-woken observers see consistent-or-newer snapshots.**
    Subscribe to lifecycle events; *synchronously inside the event arm*,
    read the snapshot and assert it already reflects the event — at both
    ends of the lifecycle (first start, final stop). Any staging where
    events lead snapshots fails immediately. Run the fixture with **zero
    snapshot subscribers** too: the no-subscriber publication skip
    (§15.6) must be invisible to the pull path — `snapshot()` read after
    a lifecycle event reflects it (§14's on-demand rule).
15. **Draining-stage contexts cannot silently drop deferred work.** An
    actor stops itself, asserts it observes the draining stage on the
    next delivery, and attempts `continue_with` / a self-timer / an
    offload there: each is either unrepresentable (type-level) or returns
    `Rejected` — assert the result, and assert the work provably did not
    run. External intake freezes at stop (§5.4's close point): after the
    freeze `try_send` fails fast (`NotRunning`) while an ordinary `send`
    parks per its restart-transparent contract — it can deliver only to a
    later incarnation or fail `Terminated` at terminality, never to this
    one — and, the drain completing cooperatively (no handler failure or
    abort — §6.2's truncation qualifier), the handled log equals exactly
    the accepted prefix (§6.2's freeze rule: under `latest()`, the
    post-conflation surviving sequence — run the fixture under both
    mailbox kinds).
16. **Natural completion follows §12's rules exactly.** An ordered scope
    of one-shot tasks finishes when the last membership terminalizes:
    assert the cascade — child `Completed` exits,
    `Stopped { reason: Finished }` at each level, root `wait()` resolving
    `Finished` — with no shutdown ever requested. Negative halves under
    quiet windows: a zero-children root, a dynamic scope whose every
    member has terminalized, and an ordered scope holding one
    `Always`-policy child all stay alive until the owner acts; a retained
    terminal sibling does not block completion.
17. **Every pending completion resolves under driver death.** The
    provocation is fault-shaped: provoke a hard abort of an ancestor
    (grace-expiry escalation, `Shutdown::Abort`, forced shutdown) with
    each obligation class provably outstanding — a stubborn descendant
    holding exit-waiters and a parked send, an admission first-polled but
    not yet dequeued, a removal latched mid-flight, a lifecycle
    subscriber holding the stream, a `shutdown_and_wait` parked on a
    restart-window membership — and assert, bounded, that every one
    resolves: exit-awaiting surfaces yield `Aborted`, `try_send` fails
    `Terminated` (never a permanent `NotRunning`), parked sends resolve
    `Terminated`, add/remove futures yield their enumerated rejections,
    the stream pairs every `Added` with `Exited`/`Removed` before the
    final scope event, and no snapshot of a stopped scope carries a live
    incarnation (§15.5's owned-completion constraint, §11's driver-death
    rule). Where a fault-injection harness can destroy the driver at
    arbitrary await points, run the same assertions there; the fixed
    provocations above are the floor, not the ceiling.
18. **User code never runs inside a framework critical section (§15.4).**
    The provocation is a hostile implementation of each seam the
    framework invokes without scheduling it, driven through the path that
    would run it under a lock. A waker whose `wake`/`clone`/`drop` panics
    or re-enters the framework: park a send and complete it, cancel a
    parked send during an unwind, and wake a snapshot or lifecycle
    subscription from a publication — each must observe a released lock
    (re-entering `snapshot()` from the waker succeeds; the mutex is
    unpoisoned afterwards). A message, actor-state, or exit payload whose
    destructor blocks or panics: displace it by conflation, recover it by
    withdrawal, retire it by terminalization, supersede it by a restart
    schedule, and retire the projection that carried it — every one must
    be destroyed after the guard. The lock-held probe is the direct
    oracle: a payload destructor that asks whether the framework lock is
    held must answer no, on every path that retires one. What this item
    does *not* assert is where the destructor then runs: a mailbox
    payload reaches isolated disposal, an `Exit`'s application error is
    destroyed on the framework thread that released the guard. Moving the
    second to isolated disposal is a separate rule about blocking
    destructors on framework threads, outside this item, because the same
    thread already destroys exits on paths that hold no lock at all.

---

## Appendix A. Normative defaults and bounds

Library-level fallbacks, applying only where neither the declaration nor
an enclosing scope decided (§10.3). "Default" means a conforming
implementation ships these values; each is overridable at the documented
level. Rows marked *(II)* ship with the named [Part II](core-plus.md)
feature.

| Concern | Default | Notes |
|---|---|---|
| Actor mailbox kind | **`queue`** | The default when neither the declaration nor a scope default names a kind (§10.3); kind and capacity travel together |
| Bounded mailbox capacity | **64** messages | Scope-overridable; per-child overridable; zero rejected at construction |
| `latest()` slot | **1** | Structural, not configurable |
| `latest_by_key` capacity *(II §18)* | defers to scope/library mailbox default | Full key set evicts oldest key |
| Mailbox shutdown policy | **Drain** | Two variants: `Drain` delivers the frozen prefix, `Discard` drops it (destruction venue per §5.5; disposal faults per §8); the intake freeze is unconditional either way (§6.2, §11) |
| Child shutdown policy | **`Graceful { grace: NonZeroDuration(5 s) }`** | construct with `Shutdown::graceful`; zero is rejected; `Abort` is the sole immediate-escalation policy |
| Tidy-abort beat | **`grace / 10`, clamped to [1 ms, 10 ms]** | §11 |
| Restart condition | **`OnFailure`** | Failure = any non-`Completed` exit (§8) |
| Backoff | **none** (immediate restart) | Exponential: `base × factor^(n−1)` clamped to `max`; `factor` a validated-finite newtype `≥ 1.0` with bit-`Eq` (§10.2); nanosecond rounding per §10.2; equal jitter uniform in `[d/2, d]`; all durations non-zero, validated at construction, with the fixed and exponential payloads sealed behind their constructors (§15.2); attempt origin/reset per §10.2 |
| Scope intensity | **5 restarts within 30 s** | Trips on the restart *exceeding* the budget; every respawn charges it (§10.2) |
| Readiness (blanket `Actor`) | **`AfterInit`** | Raw actors and tasks default `Immediate`; subtree readiness is structural (§7) |
| Readiness deadline (gated modes) | **30 s** | Resolution: declaration → scope default → this; unbounded only via explicit opt-in (§7) |
| Terminal-membership retention | **retain** (restartable) / **remove** (one-shot) | One name, one polarity, stated once (§9) |
| Monitor per-watch queue *(II §20)* | **128** events, minimum 2 | Drop-oldest; coalesced leading `Lagged`; terminal `Removed` never dropped |
| Lifecycle event buffer | **128** events, minimum 2 | Same overflow shape; per subscriber |
| Unified event lane | **unbounded; capped per-wake drain** | Requests are small; insertion payloads remain in producer-owned reservations. The batch cap bounds driver monopolization, not channel memory. Shutdown and `remove` ride level latches (§11, §12) |
| Snapshot channel | conflating watch, capacity 1 | Structural |
| `call` / `send_timeout` deadline | **none — always explicit** | One `DeadlineBudget` per call (§5.2); zero selects the no-attempt behavior |
| Identity counters | `u64`, saturating | Fail-closed overflow, decided once in the fencing primitive (§3.1); lifecycle `seq`/`lifecycle_seq` mint through the same primitive (B.4's exhaustion rule) |
| Unrepresentable deadline | **never arrives** | `Instant + Duration` overflow or an exact point the runtime cannot arm produces no deadline; it MUST NOT substitute the budget's start or any other instant |

---

## Appendix B. Surface reference

Shapes here are normative in *content* (which operations/fields/variants
exist, with which semantics); exact names may vary if the documentation
maps them clearly.

**Exhaustiveness decision.** Pre-release, public state, outcome, error,
and reason inventories are exhaustive so adding semantics breaks every
match at compile time. `#[non_exhaustive]` remains only where the
specification already names a concrete additive axis: `Mailbox` (§18),
`Strategy` (§21), `LifecycleEventKind` (§22), and `PolicyError` (future
sealed policy payloads). This decision covers every public type; release
tagging may deliberately revisit it, never inherit it accidentally.

One cross-cutting shape rule: where a surface takes a deadline budget over
other arguments, the deadline is the trailing parameter. Every such
parameter accepts `impl Into<DeadlineBudget>`, so a plain `Duration` reads
naturally at the call site while the semantics below have exactly one
name; its clock origin follows the operation family rather than the
representation. Mailbox futures and `wait_for_child` capture it on first
poll. `offload`/`offload_scoped` return no future, so they start it when
the actor loop registers the offload at the call (§6.5). The shutdown
family uses the value as an escalation budget: `System::shutdown` and
`ScopeRef::shutdown_and_wait` arm it only when the targeted incarnation
enters drain, while `start_or_shutdown` does not spend it during startup
and arms it only if rollback reaches that same drain edge (B.9).
`DeadlineBudget` permits zero and is the single home for the following
exhaustive zero-width semantics; each API selects one behavior in its
implementation rather than interpreting a bare duration locally:

| behavior | APIs | zero-width transition |
|---|---|---|
| no attempt | `send_timeout`, `call`, `ReplyReceiver::recv`, `offload`, `offload_scoped` | do not submit/poll work or observe completion; return/deliver the timeout result |
| poll once | `wait_for_child` | evaluate the current snapshot once with precedence match → terminal scope → timeout; never await |
| immediate escalation | `System::shutdown`, `ScopeRef::shutdown_and_wait`, `start_or_shutdown` rollback | request cooperative cancellation, skip only the cooperative wait, then run the ordinary abort tail — a child that *could* settle on the skipped poll is still reported as a straggler |

For a no-attempt offload, whose timeout outcome is a delivery rather than
a call failure, the work future is never polled and the total
continuation receives `DeadlineElapsed` through the normal completion
path (§6.5).

Expiry boundary, uniform: completion observed at exactly the deadline
instant counts as within budget (§7's ready-at-deadline rule is this rule
applied to readiness). For the accepting flavors — `send_timeout` and
`call`, and equally for cancellation by dropping their futures (§5.2:
expiry and cancellation share one withdrawal mechanism) — the acceptance
side of the boundary is decided structurally rather than by clock
comparison: at expiry the caller *withdraws* the in-flight message, and
the not-accepted outcome (`TimedOut` for `send_timeout`,
`AcceptanceTimedOut` for `call`) is reported only once withdrawal has
succeeded — the message provably never was and never will be accepted —
while a message that won the race into the mailbox is accepted even when
acceptance and expiry were simultaneous: the send resolves with the
accepting incarnation; the call proceeds to its response wait and, if the
budget is spent, reports `ResponseTimedOut`. That is what makes
guaranteed-not-accepted (§3.3 step 4) exact rather than probabilistic.

Public time representation, pinned once: absolute points on the public
surface — B.6's `restart_at`, B.5's `ReadinessTimedOut { deadline }` —
are `std::time::Instant`; spans and budgets are `std::time::Duration`. No
runtime time type is public (§16.13's reachability gate checks this): the
runtime façade converts at the private boundary, and under virtual time
its clock mints the instants — still `std` values, mutually coherent,
which is all any contract here compares.

Rows marked *(II)* ship with the named [Part II](core-plus.md) feature.

### B.1 Capability matrix — actor-side contexts

Stages: **Raw** = `RawContext<M>` (raw loop, coextensive with one
incarnation); **Live** = `Context<'_, A>` in ordinary `handle`; **Drain**
= the same series during shutdown drain; **Stop** = `StopContext<'_, A>`
in `on_stop`. ✓ = available; **R** = MUST be `Rejected`-or-absent (§6.4);
— = absent from the stage's type.

| Operation | Raw | Live | Drain | Stop |
|---|---|---|---|---|
| `id()`, `incarnation()`, `scope()`, `shutdown_token()` | ✓ | ✓ | ✓ | ✓ |
| `myself()` | ✓ | ✓ | ✓ | — |
| `request_scope_shutdown()` (fire-and-forget; awaiting your own scope's shutdown deadlocks — documented) | ✓ | ✓ | ✓ | ✓ |
| `run_blocking(f)` | ✓ | ✓ | ✓ | ✓ |
| `recv()` / `try_recv()` (the merged event source: mailbox, offload completions, fired timers, queued continuations, §6.1 priority; `recv` yields `None` on stop request, biased; `try_recv` ignores the stop token — the drain primitive for raw loops: under `Drain`, exhaust the frozen prefix via `try_recv` after `recv` yields `None`, §11's raw-loop obligation) | ✓ | — | — | — |
| `mailbox_shutdown()` (the resolved §11 policy for this actor's mailbox — what a raw loop consults to honor `Drain` vs `Discard`) | ✓ | — | — | — |
| `mark_ready()` (one-shot effect by construction; meaningful only under gated readiness, else a documented no-op — B.2's rule, uniformly; during drain always the no-op) | ✓ | ✓ | ✓ | — |
| `stop()` (clean self-stop; `Err` outcome wins; idempotent — during drain the already-stopping no-op; arms the child's configured §11 ladder as the stop bound. Live/Drain: effective after the current callback; in a successful effective-`AfterInit` initializer, after its automatic readiness edge. Raw: freezes intake at the call — drain the frozen prefix via `try_recv` after `recv` yields `None`; §1 principle 5's public primitive for the blanket loop's `stop()`) | ✓ | ✓ | ✓ | — |
| `is_draining()` | — | ✓ | ✓ | — |
| `continue_with(msg)` (next-message continuation; no mailbox capacity; anti-starvation per §6.1) | ✓ | ✓ | **R** | — |
| Keyed timers: `set_timeout` / `set_interval` / `clear_timer` (§6.3) | ✓ | ✓ | **R** | — |
| `send_after_to` / `interval_to` *(II §25)* | ✓ | ✓ | **R** | — |
| `watch` / `watch_scoped` *(II §20)* | ✓ | ✓ | **R** | — |
| `offload` / `offload_scoped` (§6.5) | ✓ | ✓ | **R** | — |
| Re-entry/mapping: `for_actor` (same-`Msg`, core); `project` *(II §19)* | — | ✓ | ✓ | `for_actor` only |

`StopContext` withholds everything that queues future work for this
incarnation. `myself()` is in that set: `ActorRef<M>` is the send surface
(`send` / `call`, and `Eq`/`Hash` by slot identity), intake is already
frozen, and no callback remains to receive posted work. Absence is the
contract, not a documented-don't-post on a still-present accessor.
Identity is not withheld with it. `incarnation()` remains, and
`Incarnation::membership()` yields a process-wide unique `Copy` key —
unlike scope-local `id()` — stable across restart and reborn on
remove-and-re-add (§3.2, §3.4), so a `Membership`-keyed registry
deregisters from `on_stop` with no capture at all; such a registry evicts
by key equality, since the rebirth leaves `supersedes` incomparable
across a re-add. Only an `ActorRef`-keyed map (B.8) or a teardown that
must send needs `Context::myself()` captured while live and carried in
actor state.

### B.2 `TaskContext`

Passed by value into each incarnation of a supervised task (§9's factory
signatures): `id()`, `incarnation()` (the §3.3 token), `shutdown_token()`
(cooperative stop), `abort_token()` (fires at escalation — grace expiry,
or immediately under `Abort` policy; the tidy-abort beat runs after it
fires, and §11's classification rule applies: a task that yields an
outcome during the beat classifies by that outcome, while a future
destroyed by the ensuing hard abort records `Aborted { phase }`),
`mark_ready()` (one-shot by construction; no-op only where declared
readiness makes it meaningless, and that is a documented no-op, not a
silent state change — the same rule covers a stopping incarnation: once
either cooperative shutdown or escalation has begun, readiness can no
longer be published and the call is likewise a documented no-op, matching
B.1's during-drain rule for the actor contexts).

### B.3 Send/call errors

```text
SendError<M> { actor_id, incarnation_observed: Option<Incarnation>, message: M, kind }
  kind: NotRunning   — membership currently not accepting (rebind window,
                       or intake frozen at stop — §5.4); try_send only
        Full         — FIFO at capacity; try_send only (conflating mailboxes accept instead)
        Terminated   — membership terminal; the only failure `send` can return
        TimedOut     — send_timeout only; reported post-withdrawal:
                       guaranteed-not-accepted, message recovered (§5.2)
  (message recoverable; a boxed projection drops the payload but keeps id + kind)

CallError { actor_id, incarnation_observed: Option<Incarnation>, kind }
  kind: Terminated          — terminal before acceptance
        AcceptanceTimedOut  — deadline hit before acceptance: guaranteed-not-accepted, safe retry
        ResponseTimedOut    — deadline hit after acceptance: unknown outcome — reconcile (§3.3)
        ReplyDropped        — handler dropped the Reply unanswered (what conflation-away looks like)

ReplyReceiver::recv(self, deadline) → Result<T, ReplyError { Dropped | Timeout }>
  (trailing deadline covers the response wait only — acceptance evidence
   is the accompanying send's result; §5.3)
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
  `NotRunning` is `Some` of the stopping incarnation at an intake freeze
  and `None` in a rebind window or pre-spawn.
- Pre-acceptance expiries (`TimedOut`, `AcceptanceTimedOut`) carry the
  **newest incarnation observed bound during the attempt**, `None` if
  none ever was — never an "accepting" incarnation, since successful
  withdrawal proved there is none.

`ReplyReceiver<T>` is an owned at-most-once value (B.10): `Send`, not
`Clone`, and `recv` is **consuming** — one receiver, one wait, per B.10's
consuming rule. Its deadline is one budget, like `call`'s: expiry
consumes the receiver and a reply arriving later is discarded, exactly as
a timed-out `call` abandons its reply (§5.3) — a longer wait is composed
by choosing a longer deadline, not by a second `recv`. Dropping the
receiver unawaited discards the value only; `Reply::send` stays
infallible either way (§5.3).

These exact error inventories are exhaustive pre-release so new behavior
cannot hide behind wildcard arms. `send` ↔ flavor mapping is normative:
`send` fails only `Terminated`; `try_send` never `TimedOut`. The
payload-recovery clause is scoped to unmapped refs: a contramapped ref
*(II §19)* always surfaces the boxed projection — the wrap consumed the
caller's payload at ingress.

### B.4 Events

**Core lifecycle events** (§14) — one stream contract:

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
              is how obligation §16.14 aligns events with snapshots
  kind: Added            { id, membership }               // membership begins: lowered or admitted —
                                                          //   never at reservation (§3.2)
        Started          { id, membership, incarnation }  // incarnation spawned
        Ready            { id, membership, incarnation }  // readiness gate released (§7)
        Exited           { id, membership, incarnation, exit }   // the §8 exit type
        RestartScheduled { id, membership,
                           attempt: RestartAttempt, delay }       // charged per §10.2
        Removed          { id, membership,
                           last_incarnation: Option<Incarnation> }  // terminal; None = never started
        ScopeState       { state }                        // the emitting scope's own B.6 state transitions
```

`Lagged` is a **subscription-level stream item, not an event**: the
events it replaces may have come from many scopes, so it can truthfully
carry no `scope_path` and no `seq` — only the per-subscriber dropped
count. One leading, coalesced marker per overflow episode (§14); the
aligned snapshot is the resync. The retained events delivered after that
leading marker may be older than the resync snapshot. This is
intentional: the post-`Lagged` watermark protocol below discards those
already-reflected events and applies only the newer suffix.

Ordering and delivery contract:

- Per emitting scope, events are totally ordered by `seq` and gap-free
  per subscriber except across a `Lagged` marker.
- Subscribing to a scope yields its whole subtree: descendant scopes'
  events forward upward unchanged except for `scope_path` extension.
  Forwarding preserves each origin scope's order and the causal edges — a
  child's `Added → Started/Ready/… → Removed` chain is never reordered,
  and a subtree child's own `Added` precedes any event from inside it.
- Subscriptions and the snapshot watch are membership-owned, like the
  mailbox binding (§5.4): they ride the scope's own restarts, and they
  exist from cell creation (§3.2) — a subscription through a pre-spawn
  handle is well-defined, and the snapshot watch's initial value is the
  scope's `Unstarted` snapshot (B.6), so observation begins before
  admission or spawn (§16.12's pre-spawn clause). Across a subtree
  restart the stream stays open and the sequence continuous — the
  outgoing incarnation's teardown is ordinary events (descendant
  `Exited`s, runtime-added members' terminal `Removed`s,
  `ScopeState: Draining`), closed by that incarnation's own
  `ScopeState: Stopped { reason }` (§12 publishes one per incarnation —
  `StartupFailed`, `ShutdownRequested`, `IntensityTripped`, or
  `Finished`: a naturally completed ordered subtree exits `Completed` at
  its parent, and an `Always` policy restarts it), and the rebuild is
  ordinary events (`ScopeState: Starting`, fresh `Added`/`Started` under
  new descendant memberships). `Stopped` is an incarnation edge, not a
  closure signal: only membership terminality ends the stream (closure
  rule below), so every non-final `Stopped` is followed on the same
  sequence either by the next incarnation's `Starting` or by a
  strictly-higher-precedence `Stopped` for the *same* incarnation (B.6's
  stop-reason lattice — a bounded, monotone correction, never a repeat
  of an equal verdict), and the final `Stopped` is the one followed by
  neither. The positive terminality signals are the membership edges —
  the parent's `Removed` for this scope child, or this stream's own
  closure, always preceded by the final event. Snapshot receivers hold
  the last published snapshot through the gap; a `ChildSnapshot`'s
  `incarnation`/`nested` are `None` while no incarnation is live (B.6).
- Subscription starts at now: no history replay. Catch-up is a
  prescribed two-step protocol, not an atomic acquisition operation:
  **subscribe first, then read `snapshot()`**. §14's on-demand rule
  makes that snapshot consistent-or-newer than every event already
  delivered to the new subscription (§16.14), so the reader takes the
  snapshot as ground truth and the stream as deltas, discarding any
  event the snapshot already reflects — decidable exactly, because the
  snapshot carries a watermark for every scope it spans: each
  `ScopeSnapshot` its own `lifecycle_seq` (B.6, recursively), and each
  scope *child* additionally `scope_seq` on the containing
  `ChildSnapshot` — present through the restart window while `nested` is
  `None`, so the watermark never vanishes with the recursive snapshot
  (B.6). An event is already-reflected iff its `seq` is ≤ the watermark
  of the snapshot's scope matching the event's `scope` token. A `scope`
  token absent from the snapshot splits by **causal introduction**: a
  scope the reader has since seen born — an applied post-watermark
  `Added` whose `membership` is that token (for a scope child, the
  `Added`'s membership *is* the token its events will carry, and the
  causal-order rule above guarantees the `Added` precedes any event from
  inside) — has no watermark and needs none: apply its events. A token
  neither in the snapshot nor introduced by an applied `Added` is
  stale — a membership whose teardown the snapshot already reflects
  (§16.14's consistent-or-newer guarantee covers every event delivered
  before the snapshot read) — discard it. The same protocol is the
  post-`Lagged` resync. The documentation MUST teach it in this form.
- `LifecycleSeq` exposes `get()` plus the documented `EXHAUSTED`
  sentinel; `seq`/`lifecycle_seq` mint through §3.1's one primitive:
  `u64`, saturating advance, the saturated value poisoned and never
  minted. Exhaustion — unreachable at `u64` scale, pinned per §3.1's
  decide-once rule — fails closed for observation: the scope mints no
  further events, and each subscriber accounts the unmintable remainder
  as ordinary `Lagged` drops (the marker carries no `seq` and needs
  none). `snapshot()` stays authoritative, and the saturated
  `lifecycle_seq` watermark truthfully reads "every minted event is
  reflected", so the catch-up protocol degenerates to
  snapshot-as-ground-truth exactly; closure at terminality then follows
  a final `Lagged` in place of a mintable terminal event.
- The stream ends at membership terminality, after the subscribed
  scope's final event — closure is always preceded by one (under
  sequence exhaustion, by the final `Lagged`), and per the restart rule
  above a `Stopped` alone is not closure: the final `Stopped` is the one
  no restart and no precedence upgrade follows. For a scope membership
  that never spawns (a declaring tree dropped unspawned, a withdrawn or
  rejected insertion, §3.2), that terminal event is
  `ScopeState { Stopped { reason: NeverStarted } }` (B.6), published at
  terminalization, then the stream closes. Per-subscriber buffering,
  overflow, and `Lagged` coalescing are §14 / Appendix A.
- Membership edges (`Added`/`Removed`) versus incarnation edges
  (`Started`/`Exited`) are what distinguish planned remove/add from
  crash restart without application-side history; cumulative counters
  ride snapshots (B.6).

**Monitor events** *(II §20)*:

```text
MonitorEvent { member_id, kind }
  kind: Started { incarnation }
        Exited  { incarnation, exit }        // the §8 exit type
        Lagged  { dropped }                  // resync point, not an edge
        Removed { last_incarnation: Option<Incarnation> }   // terminal; None = never started
```

Delivery semantics: §20 (bounded drop-oldest per watch; coalesced
`Lagged` kept at the front; `Removed` never dropped; immediate `Started`
on watching a running target; re-registration aliases).

### B.5 The exit type

One public type (§8): variants `Completed`, `Failed(error)` (the error
value, not a string), `Panicked { message: Option<String> }` (the panic
message when the payload downcasts to a string; the payload itself is
never retained, since exits ride `Clone` snapshots and events),
`ReadinessTimedOut { deadline }`, `Aborted { phase: GracePhase }`,
`NeverStarted` (membership terminal with no incarnation ever spawned,
§8); orthogonal `Cancellation::{Observed, NotObserved}` on every exit.
`ReadinessTimedOut.deadline` is the absolute expiry instant —
`std::time::Instant` per this appendix's time rule, B.6's `restart_at`
convention — so a retained exit stays interpretable; the configured
*span* is the child's resolved `readiness_deadline` option (§9), not
this field. Helpers: `is_failure()` (= not `Completed`),
`cancellation()`, accessors per variant, and two named cross-variant
accessors: `intensity_trip() -> Option<&IntensityTrip>` (§10.2's
structured trip data) and
`startup_failure() -> Option<&StartupFailure>` (§12's startup-failure
data, cause-bearing: a *child* cause naming the failing child's id and
exit, or a *lowering* cause naming the undefined reserved slots'
child-id paths — §12's lowering rule). They exist so routing on "this
subtree churned out / never came up" (a breaker, an operator surface) is
one compile-checked call. Both are matches on `ExitError`'s private
provenance structure (§8) — no downcast, and non-forgeable: only the
library can *authenticate* a payload into an `ExitError`, so an
imitating application error routed through the blanket conversion yields
`None`. Both payload types are public and exhaustive pre-release, which
does make the payload values themselves constructible by an application;
that is deliberate and costs nothing, because authentication lives in
the provenance structure rather than in the payload's privacy. Adding a
cause or field must update every façade match.

Construction is one named constructor per kind: `completed`, `failed`,
`panicked`, `readiness_timed_out`, and `aborted` take the kind payload
plus the orthogonal cancellation observation; `never_started` fixes
cancellation to `NotObserved`. There is no public constructor that
accepts an arbitrary exit kind, so applications cannot construct the
semantically impossible `NeverStarted`/`Observed` pair; that absence is
part of the public surface and MUST be checked from outside the library.

Scope-level shutdown-timeout errors carry the affected children as
structured data: child-id paths plus membership tokens (§8) — never bare
ids, which sibling scopes may reuse (§2).

### B.6 Snapshots and statistics

```text
ChildSnapshot   { id, membership,                       // §3 identity types
                  incarnation: Option<Incarnation>,     // the live incarnation; None when none is
                                                        //   live (Admitted, Restarting, terminal)
                  state: Admitted                       // membership created; first spawn not yet begun
                         | Starting | Running | Stopping
                         | Restarting                   // between incarnations: restart scheduled,
                                                        //   waiting out backoff (§10.2)
                         | Stopped { exit }             // terminal; the §8 exit type —
                                                        //   exit NeverStarted is the never-ran
                                                        //   terminal (§8)
                         | StartupAborted { exit },     // terminal pre-ready failure (§7)
                  last_exit: Option<Exit>,              // newest prior exit, if any incarnation has exited
                  membership_status: Active | Removing,
                  restart_count: RestartCount,          // cumulative scheduled-restart charges for
                                                        //   this membership (§10.2): incremented at
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
                                                        //   runtime-clock instant; None outside
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
                                                                //   subscription (B.4, §16.12)
                         | Starting | Running                   // the B.4 ScopeState space
                         | StartupFailed                        // root only: terminal startup failure;
                                                                //   started prefix still supervised (§12)
                         | Draining
                         | Stopped { reason: Finished           // natural termination (§12 wait())
                                           | ShutdownRequested  // owner/ancestor-requested teardown
                                           | IntensityTripped   // carries §10.2's structured trip data
                                           | StartupFailed      // nested rollback complete (§12);
                                                                //   carries the startup-failure data
                                           | NeverStarted },    // membership terminal, no incarnation
                                                                //   ever spawned (§3.2): dropped-unspawned
                                                                //   tree, rejected/withdrawn insertion,
                                                                //   removal before first spawn, startup-
                                                                //   aborted ordered sibling (§7) — the
                                                                //   scope-state twin of §8's exit, an
                                                                //   invariant the stop-reason lattice
                                                                //   below preserves in either order
                  kind: Ordered | Dynamic, strategy (ordered only), intensity,
                  total_restarts: TotalRestarts,         // charges per §10.2 — group respawns count
                  lifecycle_seq: LifecycleSeq,           // aligns events with snapshots (§14)
                  children: Vec<ChildSnapshot> }         // declaration order (ordered scopes);
                                                         //   admission order (dynamic scopes) —
                                                         //   pre-admission reserved cells are
                                                         //   absent (§3.2)
                + child(id), descendant(path) traversal helpers
```

**Stop-reason lattice.** Several owners can independently reach a stop
verdict for one incarnation — a driver's drain epilogue, a join monitor's
fallback after that driver panicked or was cancelled, a never-started
terminalization — so a scope's published `Stopped { reason }` resolves
competing verdicts by **precedence, never by arrival order**. The total
order is `Finished < IntensityTripped < StartupFailed <
ShutdownRequested < NeverStarted`. A later verdict replaces the published
reason — and emits a corrected `ScopeState` edge, per B.4's
non-final-`Stopped` rule — iff it strictly outranks the recorded one;
equal or weaker verdicts are idempotent repeats that publish nothing. The
order is severity-ascending: `Finished` is the weakest claim, since a
drain that began on natural completion says nothing about how the
teardown itself ended; `ShutdownRequested` supersedes the structured
failures, matching §11's drain-upgrade rule, which joins through this
same lattice; and `NeverStarted` is the top element because it is not a
live incarnation's verdict but the membership-terminal twin of §8's
`NeverStarted` exit, so the scope-state projection and the membership
exit agree whichever publication lands first. The consequences are that
`wait_stopped()`, the final snapshot, and the stream's last `ScopeState`
event always report the same, highest-precedence verdict, and that a
root driver that dies mid-drain reports the join monitor's
`ShutdownRequested` rather than the abandoned drain's `Finished`.

```text
ActorStats (II §22)
                { messages_received, messages_accepted, messages_conflated,
                  messages_evicted,                      // cross-key evictions (§18)
                  message_bytes_accepted: Option<u64>, sends_rejected,
                  outstanding_offloads, mailbox_depth, mailbox_capacity }
                — per membership; per-incarnation attribution distinguishable via §3.3;
                recursive scope-wide stats resolve through typed child metadata (§22)
```

### B.7 Guards

Scheduled/owned work (scoped offloads; with Part II: cross-actor timers,
scoped watches) returns a `Guard`: consuming `cancel(self)` and
`detach(self)`; by-reference probes `is_cancelled()`, `is_finished()`,
and the awaitable `finished()`; **drop = cancel**. For core offloads,
"finished" is a completion-or-cancellation notification, not a
hard-abort join: incarnation teardown may fire it when cancellation is
requested while the task is still unwinding (§6.5). `detach(self)`
releases only the guard's cancel-on-drop; the underlying facility's
ownership rule is unchanged (offloads stay incarnation-owned, §6.5;
cross-actor timers stay sender-incarnation-owned, §25). Exactly-once
cancel is by owned construction, not an atomic claim flag: `cancel` and
`detach` consume the guard, so cancel-after-detach and double-cancel are
unrepresentable (§1 principle 3, B.10's consuming rule) — there is no
runtime "already cancelled" error arm to specify.

### B.8 Control-operation outcomes

`remove` never errors: it resolves to `Removed | AlreadyAbsent`. A
draining, stopped, or terminated scope counts as `AlreadyAbsent` (§9's
stage rule — the teardown owns every stop), a retained tombstone counts
as `Removed` (§9's retention semantics — the prune is real work: the
`Removed` event fires and the id is freed), and a reserved-but-undefined
cell counts as `Removed` too (terminalized `NeverStarted`, §9 — the
*outcome* only: never having been admitted, it fires no `Removed` event
and leaves no tombstone, §3.2's minting/admission split) — §12's
idempotency, made concrete. `remove` futures are observation only:
removal latches synchronously at the call (§12's remove rule), so
dropping the future — polled or not — detaches, and a latched removal
still completes. The owned-completion requirement (§15.5) makes an
internal response loss unreachable on a conforming path. If that
requirement regresses, release builds nevertheless fail closed: admission
observes `NotAdmitting(Terminal)` and a latched removal observes
`Removed`, since its route becoming terminal satisfies the removal goal.
Debug builds MAY instead assert and panic at that boundary to expose the
internal regression instead of returning an outcome.

The public `ReserveError` is exhaustive and includes `NoRuntime`: it
names the absent ambient runtime at dynamic reservation or first poll,
with the cleanup and precedence pinned in §9. Dynamic `add_*` fails with
exactly the union of its two halves (§9): `EmptyId`, `NoRuntime`,
`DuplicateId` (tombstones included), and `RemovalInProgress` (same id
mid-removal — await removal and retry) from reserve, with `NoRuntime`
also possible at first poll — plus §3.1's enumerated identity-exhaustion
rejection, unreachable in practice but named so fail-closed has a
shape; `NotAdmitting` from either half. `NotAdmitting` is one outcome
with an enumerated, data-carried cause: the scope membership is
terminal, its live incarnation is draining, the dynamic root is parked
in `StartupFailed` (§9's stage rule — the park is the owner's decision
point, not an admission window), no incarnation is live — a pre-spawn
handle, or an ancestor restart's re-lowering window (§9's stage rule) —
or, with the scope itself still admitting, the operation's **own
reservation has ended**: the cell was terminalized before the define
reached admission (removed by id, §9's orphaned-slot rule; or annulled
by the fused drop latch). That last cause is cell-level, enumerated
distinctly so a caller can tell "the scope closed" from "my reservation
is gone".

Defines add no definition-validation errors: validation is spent eagerly
at spec construction (§10.3), on both flavors. A dynamic define still
crosses §9's admission boundary, so it can return `NoRuntime` at first
poll or `NotAdmitting`; declaration builders share the reserve id errors
(`EmptyId`, `DuplicateId`), require no runtime for reservation or
define, and their defines cannot fail (§9).

`BuildError` (spawn-time, §12) is enumerated and exhaustive pre-release:
`NoRuntime` (no ambient async runtime reachable through the private
runtime façade) and `UnfilledReservations` (the child ids of every
undefined reserved slot at that root, §9). Nothing else lives there by
design: everything decidable earlier fails at declaration (§10.3's eager
validation), and everything later is the child's ordinary supervision
story — spawn is not a third validation point. `BuildError` is
spawn-only because spawn is the only lowering with a builder caller: a
lowering elsewhere that finds unfilled reservations is the scope
incarnation's startup failure instead, carried as the startup-failure
payload's lowering cause (§12's lowering rule, B.5).

The `add_*` future resolves **at admission** and returns, per kind, the
same handles the builder forms return (§3.2): `ActorRef<M>`; `TaskRef`,
plus `OneShotTaskRef<T>` on one-shot task forms; or the subtree's
`T::Ref`. Every set contains a membership-addressed component:
`ActorRef`, `TaskRef`, or `T::Ref` exposes the membership token through
`membership()`. Startup is never awaited by the call; observe it through
the returned handles (the `wait_for_child` helper — B.9, snapshots,
events). A caller that abandons its own startup wait therefore still
holds identity, and one that *cancels the call itself* is covered by
§9's drop rules — a dropped fused `add_*` future withdraws or removes,
never orphans; a dropped split `define` future detaches, the slot's
handles remaining the caller's identity. Startup failure after admission
is reported through the child's exit, not through the add call.

### B.9 Handle surfaces

Content-normative operation inventories for the remaining public handles
(identity accessors — id, membership, incarnation where applicable — are
implied on all; error/outcome types are B.3 and B.8):

- **`System`** (owner): `scope()` (the root scope handle, typed per
  §12's dispatch), `wait_started()`, `start_or_shutdown()`, `wait()`
  (resolves with the root's terminal reason, §12), `shutdown(timeout)`
  (§12); not `Clone`, `#[must_use]`, drop = request graceful shutdown.
- **`ActorRef<M>`**: cheap `Clone`, membership-addressed (§2); `send` /
  `try_send` / `send_timeout` (each resolving to the accepting
  `Incarnation`), `call`, and `reply_channel` per §5.3, error and
  success shapes per B.3; `contramap` *(II §19)*, `pinned` *(II §17)*.
- **`ScopeRef`**: `snapshot()`, `subscribe_snapshots()` (conflating
  watch), `subscribe_lifecycle()` (B.4), the `wait_for_child` helper
  (contract below), `child(id)` / `descendant(path)` traversal (B.6),
  `request_shutdown()` (fire-and-forget), `shutdown_and_wait(timeout)` —
  the owner's `shutdown(timeout)` contract (§12) on a non-owning handle:
  same trailing escalation-bound timeout (Appendix B's exemption), same
  structured straggler report. Because the handle is
  membership-addressed (§2) and the §11 latch is per-incarnation, the
  call is **incarnation-targeted by construction**: the request rides
  the latch of the scope incarnation live at acceptance (a request
  landing in a restart window is held by §11's pending-incarnation stop
  latch and armed onto the next incarnation, which starts and
  immediately begins teardown), and the call resolves once *that
  incarnation* has finished its scope epilogue. On the ordinary teardown
  path that includes joining its children; §12 defines the
  recursive-join exception when an ancestor hard-aborts a framework
  driver. Under a parent `Always` policy (§12's nested-shutdown rule) a
  fresh incarnation may already be running at resolution — the contract
  is about the incarnation the latch stopped, deliberately not about the
  membership. A **pre-spawn** handle is the same window at the
  membership's start of life: no incarnation has ever existed, so the
  request arms §11's pending latch and waits for the first incarnation,
  which starts and immediately begins teardown. The timeout is an
  escalation bound on a live teardown and **arms only when the latch
  begins acting** — at that incarnation's *drain entry*, never at the
  call: pre-spawn there is nothing to escalate, and the call waits
  exactly as a parked send does, bounded by §3.2's no-hang rule — a
  membership terminalized with no incarnation ever spawned (tree dropped
  unspawned, rejected or withdrawn insertion) resolves the call
  immediately as already-stopped. Drain entry is the precise arming edge
  because the budget bounds the **cooperative** phase: the incarnation
  must first get the wake in which it consumes the latch, enters
  `Draining`, and starts each child's stop ladder, or a zero budget
  would report every child that cooperates on that wake — and every
  child sitting in a restart backoff window — as a straggler, which §8's
  report explicitly is not for. One consequence is normative: when an
  ancestor hard-aborts the incarnation **before** it reaches drain
  entry, the latch never acts and the budget never arms, so there is no
  cooperative phase to bound and no straggler report to make. The call
  then waits on that incarnation's drop epilogue — synchronous, awaiting
  nothing (§12's fallback boundary) — and resolves `Ok`. A caller
  therefore cannot use this timeout to bound its own return; the return
  is bounded by the epilogue, as it is on the ordinary path once the
  budget expires (§12's unbounded join remainder). Concurrent callers
  ride one latch and observe one teardown. A scope whose membership is
  already terminal resolves immediately only when its scope projection
  is `Unstarted` or `Stopped`; if parent teardown published terminal
  membership before a live incarnation's epilogue, the call still waits
  for that incarnation to finish (`Ok` — the terminal state is
  `wait_stopped()`'s and the snapshot's to report, not this call's).
  That settlement test reads membership and scope projection as two
  planes, not one atomic fact: a nested driver already inside its first
  poll when its ancestor publishes terminal membership still reaches its
  incarnation-begin transition, so a wait can settle a hair before that
  epoch becomes visible. The window is sanctioned rather than closed —
  the incarnation publishes `Starting` from that transition, superseding
  the stale `Unstarted`/`Stopped` projection under the same observation
  gate *before* any of that incarnation's user code runs, and its epoch
  owner still publishes the final `Stopped` projection — so the settled
  call reports the state that held at its own resolution and the later
  incarnation remains `wait_stopped()`'s and the snapshot's to report.
  `wait_stopped()` is the membership-level await — the scope analogue of
  `TaskRef::wait()`: it rides restarts and resolves at membership
  terminality with the scope's terminal state
  (`Stopped { reason: NeverStarted }` for a scope membership that never
  spawned — §3.2, B.6); observing one incarnation's transient stop is
  the event stream's job (B.4 `ScopeState`), not this helper's.
  `dynamic()` as the runtime downgrade query (§12).
- **`DynamicScopeRef`**: `as_scope() -> &ScopeRef` is the single
  explicit access path to the shared observation/control surface; there
  are no mirrored forwards and no `Deref`. Its inherent dynamic-only
  surface is the eight add entry points (§9, the raw pair included;
  resolving at admission, B.8), the `reserve_*` slot family (§9 —
  `add_*` is reserve-plus-define sugar), and `remove` — by exact handle
  (the safe primitive for planned replacement) or by id, both with the
  single idempotent outcome (B.8).
- **`TaskRef`**: cheap `Clone`, membership-addressed; a terminal-exit
  awaitable (`wait()` — rides restarts, resolves at terminality with the
  §8 exit, `NeverStarted` included).
- **`OneShotTaskRef<T>`**: owned, non-`Clone`; consuming await yielding
  `Result<T, Exit>` (§4.2); drop discards the completion value only.
- **`Reply<T>`**: consuming, infallible `send(T)` (caller gone = value
  discarded, §5.3); `channel()` split (§5.3); drop observed by the
  caller as `ReplyDropped`. **`ReplyReceiver<T>`**: owned, non-`Clone`,
  consuming `recv(deadline)` — contract in B.3.
- **Cancellation tokens** (`shutdown_token()`, `abort_token()`,
  `run_blocking`'s child token): library-owned; `is_cancelled()`,
  awaitable `cancelled()`; derivation and detach-past-abort per §6.5.
- **Snapshot receiver**: conflating watch — borrow-latest and
  changed-await operations. Every retained value is a complete §14
  observation-transaction cut: publications within one transaction are
  coalesced and installed once at commit, so an ungated borrow sees the
  prior or final cut, never an intermediate one. The receiver closes at
  terminality, including a declaring tree dropped unspawned (§3.2 — the
  terminal `Stopped { reason: NeverStarted }` snapshot is published
  first).

**Pinned result shapes for the wait/stop surface.** Names carry
Appendix B's latitude; the shapes and payloads are content-normative,
enumerated here exactly as `BuildError` is in B.8. Error and reason enums
are exhaustive pre-release so every semantic addition forces downstream
matches to be reconsidered.

- `wait_started(&self) -> Result<(), StartupError>` — `StartupError`
  carries the structured cause of terminal startup failure:
  `StartupFailed(StartupFailure)` (B.5 — child or lowering cause),
  `IntensityTripped(IntensityTrip)` (a trip during startup, §10.2), or
  `ShutdownRequested` (teardown requested concurrently before the tree
  came up).
- `start_or_shutdown(self, timeout) -> Result<System, StartOrShutdownError>`
  — the error pairs the original `StartupError` with the rollback
  outcome: an `Option<ShutdownTimeout>` straggler report, `None` when
  rollback completed within its timeout (§12: rollback never masks the
  startup error).
- `shutdown(self, timeout) -> Result<(), ShutdownTimeout>` — `Ok` iff
  every descendant stopped within the cooperative phase;
  `ShutdownTimeout` is §8's structured straggler report (child-id paths
  with membership tokens). The root driver is joined on return either
  way; recursive joining is subject to §12's hard-abort fallback
  boundary.
- `wait(self) -> StopReason` — infallible; `StopReason` is B.6's
  `Stopped { reason }` payload (`IntensityTripped` and `StartupFailed`
  carrying their structured data).
- `shutdown_and_wait(&self, timeout) -> Result<(), ShutdownTimeout>` —
  the owner's `shutdown` shapes on the non-owning handle (semantics
  above); an already-terminal scope resolves `Ok` immediately only after
  any live incarnation has finished its scope epilogue. Descendant
  joining is subject to §12's hard-abort fallback boundary.
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

Semantics: the predicate is evaluated against the named child's snapshot
within the scope's current snapshot, then against each subsequently
published one; the future resolves with the first **matching**
`ChildSnapshot`. Each value is a complete §14 transaction cut, including
compound batch admission. The watch can conflate cuts, so intermediate
committed states can be skipped — the predicate MUST be written to
accept any state at-or-past the awaited edge (§3.3's ordering
discipline: state predicates and `supersedes`, never equality with an
expected next state), and the documentation teaches this. An id with no
resident membership simply does not match yet; a later `Added` under
that id can satisfy the wait — ids are labels (§2), so callers needing
exactness pin the membership token from the returned snapshot. A child
snapshot in a terminal state is not an error: the predicate sees it and
decides (retained tombstones included, §9). The predicate runs on the
observation path: it MUST be cheap and non-blocking, and it is a plain
`FnMut` — no `Sync` needed, it is not shared. Errors: `TimedOut` per
Appendix B's deadline rules (zero evaluates the current snapshot exactly
once with match → terminal scope → timeout precedence; a match observed
exactly at the deadline wins); `ScopeTerminated` when the subscribed
scope's membership terminalizes before a match, carrying its terminal
state.

### B.10 Trait and concurrency matrix

Uniform bounds, stated once; a conforming implementation provides at
least these. Policy/config data additionally follows §15.2's plain-data
rule (`Clone`, `Eq`, `Copy` where cheap).

- **Identity tokens** — `Membership`, `Incarnation`: `Copy`, `Eq`,
  `Hash`, `Send`, `Sync`. Ordering is `supersedes` (§3.1–§3.3),
  deliberately not `Ord`: membership comparison across owning scopes or
  child ids, and incarnation comparison across memberships, has no
  meaning and fails closed.
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
  handed into blocking closures (§6.5).
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
- **Closures accepted by the API**: stated at each site (§6.5, §9, §19,
  B.9) — ingress-path closures that run at concurrent call sites and
  shared restartable construction sources carry `Fn + Send + Sync`.
  Their invocation still occurs inside one incarnation (§4.2); `Sync` is
  required because the retained source itself is shared. Equivalently,
  `ActorDef::cloned` requires `Args: Clone + Sync`. Observation
  predicates that run from one place at a time carry `FnMut + Send`
  without `Sync`.
- **Exactly-once operations are consuming.** Where ownership enforces
  at-most-once (§1 principle 3), the method takes `self`:
  `Guard::cancel`/`detach` (B.7), `Reply::send`, `ReplyReceiver::recv`
  (B.3), the `OneShotTaskRef` await, slot `define`,
  `System::shutdown` / `start_or_shutdown` / `wait`. Probes and
  observers take `&self`. A conforming surface MUST NOT re-shape a
  consuming operation as `&self` plus a runtime already-used error.

---

## Appendix C. Acceptance scenarios *(informative in prose, normative in obligation)*

A conforming implementation MUST be validated against application-scale
scenarios equivalent to the following five. They are executable
acceptance tests, not demos — every wait in them is a bounded event,
lifecycle, snapshot, or state poll, never a sleep-and-hope — and most of
this specification's API-shape requirements exist because one of them
needed the shape. Scenarios that use Part II surface (peer watches,
keyed conflation, outlines, metrics) run against core with those touches
stubbed or simplified until the features exist.

1. **Shard store — deliberate topology change over durable state.** An
   ordered root of a directory actor (atomic rebind registry), a dynamic
   scope of per-key-range ordered subtrees, and a single topology-writer
   router. Planned replacement runs as a userland transaction:
   mount → readiness → directory cutover → exact-handle retire, with
   idempotent operation ids, compensating cleanup, a durable abort path,
   and post-commit reconciliation. Fault injection covers pre-commit
   crash and post-commit reply loss; the script proves accepted-request
   quiescence, the crash-window fence, and reconcile-or-rollback for
   each outcome. This scenario is the reason direct admission handles
   (§3.2), incarnation tokens and the retry discipline (§3.3),
   idempotent/exact-handle removal (§12), and the replacement-membership
   boundary (§3.4) exist.
2. **Sidecar — task-first embedding in a host-owned process.** Four
   plain supervised tasks plus one small actor subtree as a sibling, in
   a process that owns `main`, init, and teardown. Proves ordered
   readiness-gated startup, per-child `Abort` vs `Graceful`,
   startup-failure reporting with the started prefix left running (then
   host-driven rollback — the motivation for `start_or_shutdown()`),
   grace-bound enforcement with `phase: AfterGrace` and
   cancellation-before-escalation ordering read from the child's own
   journal, and two full embed/run/stop cycles in one process.
3. **Trading engine — cyclic wiring, pipelining, and a restart
   breaker.** Slot-before-define declaration throughout (every ref
   minted from a cell before any factory exists — no registry, no
   `Option<ActorRef>`), a restart-budgeted venue subtree, bounded FIFO +
   keyed-conflation mailboxes side by side, pipelined `call`s,
   `try_send` on the urgent control lane under mailbox flood, peer
   watches for feed staleness, deadline-budgeted offloads around calls
   (the §6.5 one-budget rule), and a health breaker driven off the
   cumulative restart stream (the packaged restart-counter view of
   §22).
4. **Build farm — a finite batch application.** A dynamic scope of
   consuming one-shot workers (`FnOnce` payloads, auto-removed on
   terminal exit), a readiness-gated lease task restarted with backoff,
   keyed latest-wins progress conflation, exact-handle retirement of a
   wedged worker, completion-driven lifetime ("run until the scheduler
   finishes, then shut down", §25), outline verification, and a warm
   re-run over a fresh tree sharing durable state — proving the
   durable-vs-incarnation `Args` split (§4.1) end to end.
5. **Assistant control plane — nested dynamic scopes and staged
   shutdown.** The stress composite: ordered root over a dynamic session
   scope whose members are themselves subtrees each owning a further
   dynamic scope, plus a gateway chain ending in a readiness-gated
   bridge. Two levels of panic isolation, transport redelivery at a
   journal/ack boundary, cancellable streaming over `latest()`
   conflation, idle eviction, and a racing remount — the remove/re-add
   race that §3.4 + §12's idempotent removal must make safe. (Its
   `OneForAll`/`RestForOne` scope flavors and peer watches join with
   Part II §20/§21; the core port uses `OneForOne` scopes and lifecycle
   subscriptions.)

**Patterns that must remain expressible.** These compositions are load
tests of the API's joints; a design change that regresses any of them
has removed capability, not complexity: slot-before-define wiring for
reference cycles; incarnation-owned offloads completing through the
actor loop; lifecycle forwarding, monitor events, and snapshot identity
working together; lineage/incarnation/restart counters cleanly
distinguishing crash recovery from planned remove/add; per-instance
restart policy on temporary children plus exact-handle removal;
`continue_with` rehydration; holding a `Reply` to model a pending
acknowledgement; received/conflated statistics; rebuilding a single-use
`Tree` from retained host state for re-embedding.
