# The shape of the implementation

Everything before this point teaches the model to someone building *on*
Shelterwood. This part is for someone changing Shelterwood itself: it maps
how the library is constructed, where each responsibility lives, and how
data flows through a running system. It is a map, not a contract — the
normative statement of what the implementation must do is `specs/SPEC.md`
(§15 in particular, which mandates the layering and lock discipline this
part describes), and the invariants contributors must uphold in a diff live
in `CLAUDE.md` beside the code. When an internal name in this part goes
stale, the change that renamed it updates the map in the same PR.

Unlike the rest of the book, these chapters quote no code. They name real
types, modules, and files so you can jump straight to the source; depth
lives in the source and its tests, not here.

## Three crates and a tool

The workspace has three implementation crates with a strict dependency
direction, plus one checking tool:

- **`shelterwood-core`** — runtime-independent supervision types,
  capabilities, and state machines. Its only dependency is `thiserror`: no
  async runtime, no adapter, nothing that can run user code behind its
  back. This is where the pure decision machinery lives (`engine.rs`,
  `supervisor.rs`, `exit.rs`, `policy.rs`), along with the identity types,
  panic-capture facilities, and the doc-hidden seams the other crates
  consume (`capability.rs`, `waker.rs`, `waker_proxy.rs`,
  `proxied_sleep.rs`).
- **`shelterwood-runtime`** — the Tokio adapter. It is the only crate in
  the workspace that names Tokio as a normal dependency, and it pins the
  exact release, because blocking-pool rejection ownership is verified
  against that release. It provides spawning and joining (`spawn.rs`),
  synchronization primitives (`sync.rs`), timers (`timer.rs`), and the
  isolated-disposal lanes (`disposal.rs`).
- **`shelterwood`** — the public façade, and the only crate with a public
  API. All sixteen of its modules are private; the API is a flat set of
  `pub use` lists on the crate root. Tokio appears only in its
  dev-dependencies, for tests.

Core is the leaf; the runtime adapter depends only on core; the façade
depends on both. No edge points back toward the façade, and core never
names the adapter. That graph — not convention — is what makes SPEC §1's
"the public API is runtime-independent" principle structural.

The fourth workspace member, `tools/api-reachability`, is part of the
enforcement story described at the end of this chapter.

## The capability seam

The façade's mailboxes need runtime services — one-shot channels, change
signals, timers, isolated disposal — without naming a runtime. The seam is
`MailboxRuntime` in `crates/shelterwood-core/src/capability.rs`: a
doc-hidden, type-erased trait whose five methods mint the sub-capabilities
(`ErasedOneShotSender`/`ErasedOneShotReceiver`, `MailboxSignal` and its
watcher, `dispose`, `now`/`sleep_until`).

The adapter implements it once (`TokioMailboxRuntime` in
`crates/shelterwood-runtime/src/mailbox.rs`) and exposes a single
process-wide instance through `mailbox_runtime()`. The façade installs
that object per mailbox at slot-attach time
(`crates/shelterwood/src/tree/slots.rs`), and the same object then flows
through reply channels and deadline futures, so virtual-time and disposal
semantics cannot silently switch adapters mid-conversation. Type erasure
is also why `ActorRef<M>` carries no runtime type parameter.

Two one-line modules make the boundary auditable: the façade's
`runtime.rs` (`pub(crate) use shelterwood_runtime::*` — "the only boundary
between the library and its async runtime") and `engine.rs`
(`pub use shelterwood_core::engine::*`). Every runtime touchpoint in the
façade imports through the former, which makes "where do we touch Tokio?"
a one-file question.

## Inside the façade

SPEC §15.1 mandates four layers; the façade's module tree realizes them,
with one extra structural stratum (the cells) underneath the mutable
shell:

- **`tree/`** — declaration and ownership (the tree façade over L1).
  `Tree` and `DynamicTree` wrap a shared `BuilderCore`; `tree/slots.rs`
  holds the reserve-before-define slot machinery for cyclic wiring (and is
  where mailboxes are minted); `tree/system.rs` holds `System`, the sole
  owning handle. `plan.rs` lowers a declaration into an owned
  `ScopePlan`/`ChildPlan` construction plan.
- **`driver/`** — the mutable runtime shell (L1's executor). There is
  deliberately no type named `Driver`: the shell is `run_scope` and its
  helpers around `ScopeRuntime`, feeding sampled events into core's pure
  reducers and executing the effects they return. Submodules split the
  shell by concern: `child.rs` (one incarnation's spawn/join/disposal),
  `events.rs` (event lanes and arbitration), `startup.rs`, `shutdown.rs`,
  `removal.rs`, `admission_control.rs` (dynamic membership), and
  `storage.rs` (fail-closed completion obligations).
- **`cells/`** — restart-stable state, structurally *below* the driver.
  `MemberCell` is the per-membership cell that survives restarts;
  `ScopeCell` is the supervising node's stable state and the publication
  point for observation; `cells/observe.rs` holds snapshots and lifecycle
  streams (L4); `cells/gate.rs` holds the observation gate and its
  transaction; `cells/retained.rs` holds the `RetainedExit` family that
  keeps user errors off framework-critical destruction paths.
- **`mailbox/`** — the mailbox kinds, the send flavors, and request/reply
  (L2's delivery half). `mailbox/cell.rs` is the restart-stable
  `MailboxCell<M>`; `mailbox/futures.rs` is the public send surface
  (`ActorRef<M>` and its futures); the module root declares the
  crate-private control traits a `MemberCell` uses to drive a mailbox it
  cannot name generically (`MailboxControl`, `MailboxTermination`,
  `ActorIdentity`, and friends).
- **`raw/`** — loop-owning raw actors (L2's execution half): the
  `RawActor` trait, the per-incarnation `RawContext<M>` with its keyed
  timers, offloads, and panic containment.
- **`actor.rs`** — the callback layer (L3): the `Actor` trait and the
  `Handler<A>` wrapper, which is itself just a `RawActor` implementation —
  the proof of SPEC §1 principle 5 that the high-level loop uses nothing a
  hand-written raw actor cannot reach.

The remaining top-level modules (`deadline.rs`, `exit.rs`, `identity.rs`,
`policy.rs`, `scope.rs`, `task.rs`, `definition.rs`) are thin façade
wrappers or handle types over the corresponding core machinery.

## The boundary is machine-checked

Three checks in the CI lane turn the crate boundaries from intent into
enforcement:

- **`tools/api-reachability`** walks the façade's rustdoc JSON: every
  public item's signature graph is searched for references into
  `tokio`, `tokio_util`, `fastrand`, or `shelterwood_runtime`. Its known
  blind spot — rustdoc JSON cannot see through cross-crate re-exports
  from core — is covered by the next check.
- **`tools/check-core-manifest.sh`** pins `shelterwood-core`'s direct
  normal dependencies to a literal allowlist (today: `thiserror`). A
  doc-hidden core signature cannot name an adapter type its crate cannot
  reach, which is what makes the reachability walk's blind spot safe.
  Growing the list is a deliberate boundary decision, never a convenience.
- **`tools/check-external-consumer.sh`** compiles a real external crate
  against the façade, then runs negative compile probes that must fail
  with specific diagnostics — including one proving that every private
  installation seam (`MailboxRuntime`, `MailboxControl`, `WakerProxy`,
  `MemberCell`, and the rest of the family) is unimportable from outside.

Together they state one argument in three parts: no public item reachably
names a runtime type; the one un-walkable residue is structurally safe
because core has no runtime dependency; and an actual external consumer
cannot import the seams anyway.

The next chapters follow the two central data flows through this
structure: [a message](internals-message.md) from `send` to handler, and
[a child](internals-child.md) from declaration to restart.
