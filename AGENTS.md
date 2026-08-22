# Project instructions

## The lock rule

**Code holding a framework mutex may only manipulate plain framework-owned
data.** Wakes, drops of user values, formatting, user callbacks, and panic
resumption happen after unlock.

The framework runs arbitrary user code at moments it does not choose: waker
`wake`/`clone`/`drop` vtable functions, destructors of user messages, actor
state, construction closures and the type-erased error inside an `Exit`,
`Debug`/`Display` on user errors, `Hash`/`Eq` on user timer keys. All of it is
safe Rust that may panic, block, or re-enter. Under a framework mutex each of
those becomes poisoning, an ABBA deadlock, or — during an unwind — a double
panic and a process abort.

Two types implement the rule and are the shapes to reach for:

- **`ObservationTxn`** (`crates/shelterwood/src/cells/gate.rs`) holds the
  observation-gate guard plus a deferred-effect list. `defer`/`pulse` queue
  work; `commit` drops the guard *then* runs the queue through a
  `PanicAccumulator`. Its `Drop` runs the same path during an unwind, so a
  poisoned transaction cannot strand already-committed wakes. Every retained
  control-plane writer takes the token, which makes an out-of-transaction
  mutation unavailable by construction.
- **`MailboxTxn`** (`crates/shelterwood/src/mailbox/cell.rs`) is the same idea for
  every mutable mailbox transition. It owns the state guard beside a
  `MailboxEffects` sink that collects signal pulses, waker wake/drop actions,
  displaced payloads and isolated-disposal requests; its `Drop` empties the
  guard field before Rust drops the sink, so the flush necessarily runs with no
  mailbox mutex held, unwind included. `WakerSlot` makes the waker half
  structural rather than conventional: its storage is private to
  `shelterwood-core`'s waker module, and no operation returns or replaces a
  `Waker` without an effects sink. `Withdrawal`, `Termination` and `MailboxPayload` are its
  single-purpose siblings.

What the rule does *not* forbid — the exemptions every remaining lock site
rests on:

- **Framework-owned data.** Counters, epochs, arena keys, binding state,
  registration ids, request flags, identity maps. Locks held only over these
  need no ceremony.
- **Moving a user value out.** `take`/`mem::replace`/returning by value is not
  a drop. What must be outside the critical section is the *destination*:
  `MailboxTxn::finish`/`finish_returned` handing the value back only after
  releasing the guard, `withdraw`'s `Withdrawal` carrying its outcome and
  post-unlock effects together, and `MailboxTxn::take_payload`'s `#[must_use]`
  carrier all encode that.
- **`Arc` traffic that cannot reach zero.** Cloning an `Arc` under a lock is
  refcount work; dropping one is only refcount work while another owner is
  provable. Prefer restructuring over the proof — every violation the #235
  audit confirmed was a drop that *looked* like refcount traffic. Resident
  member records, scope records, lifecycle events, snapshot projections and
  driver completions protect failed exits through `RetainedExit`; its drop
  transfers destruction to `runtime::dispose_critical`, whose every path —
  exhausted thread creation, and a runtime torn down under an accepted
  submission — keeps the job rather than destroying it inline. Records that
  hand out clones (`MemberRecord`, `ScopeRecord`) keep their guards behind one
  shared `Arc` placed after every raw projection, so a read is refcount
  traffic and only the last clone submits disposal. Scope state and startup
  results need that protection too: a structured startup failure recursively
  owns the triggering child's `Exit`. `RetainedRecordedOutcome` is the same
  carrier one step earlier: a *provisional* `RecordedOutcome` owns the same
  type-erased application error before any verdict is selected, so the child
  task's report cell and the driver's exit event carry it retained and the
  losing half of every fold retires through critical disposal. Framework code
  reaches those folds through `classify_exit_retaining`,
  `reconcile_recorded_outcomes_retaining` and
  `classify_disposal_panic_retaining`; the raw `shelterwood-core` folds hand
  both halves back and are for core's own tests.
  `ScopeCell::clear_residents_locked`, `prune_child_locked` and
  `admit_child_locked` route `Arc<MemberCell>`s through
  `runtime::dispose_detached` after unlock: the last member owner can also be
  the last owner of a mailbox containing unread user messages, which
  `RetainedExit` does not cover. The first two displace an existing resident;
  the third pushes the *incoming* projection into residency before its first
  fallible step, so a bookkeeping panic leaves the graph owned by residency
  and retires it the same way at scope clear. Such a resident stays
  unannounced, which keeps SPEC §3.2's `Added`/`Removed` pairing exact and
  keeps it out of the observed child set.
  Containment lives on `ResidentChild::drop` rather than on the displaced
  `Vec`, because `Vec`'s slice drop glue keeps going after an element panics
  and a second hostile mailbox destructor would then panic inside the first
  one's unwind. Per-element containment therefore holds at every depth of a
  nested residency and on `ScopeCell`'s own drop glue, which is what SPEC §5.5
  asks of this lane.
- **Framework `dyn` seams.** `MailboxControl`, `MailboxTermination`,
  `MailboxEffectSink`, `ActorIdentity` and `DynamicRoute` are `pub(crate)`
  implementation seams inside the façade, not user traits. Their only
  implementations are framework-owned, and a foreign implementation is
  unrepresentable. `MailboxRuntime` and the sub-capabilities it mints —
  `MailboxSignal`, `MailboxSignalWatcher` and the `ErasedOneShot*` family —
  remain public, unsealed, doc-hidden traits in `shelterwood-core` because the
  separate `shelterwood-runtime` adapter must implement them. They ride under
  the same framework-only ruling, but every path that installs one in a
  mailbox is private to the façade.
  `WakerProxy` and its `retire_with` seam ride with them: the proxy is a
  public doc-hidden cross-crate type whose `retire_with` takes a
  caller-supplied `fn(Waker)`, but the effect is queued under the proxy's
  leaf mutex and invoked only after unlock, so no foreign code runs under
  the lock. `ProxiedPoll`, the probe/register/re-poll state machine that
  wraps it, rides under the same ruling: its `poll` takes caller-supplied
  closures, but they are invoked only with no proxy mutex held, and its
  ready-edge retirement flushes the stored caller waker through the same
  post-unlock effects path. The supported façade re-exports neither type
  nor anything that could install one.
  `MailboxEffectSink` was the sharpest open case before the fold: the
  framework calls `defer_mailbox_effect` while holding both the resident-tree
  observation gate and `MemberCell::mailbox`. It and `MailboxEffectQueue` are
  now crate-private, so an unsupported direct dependent can neither construct
  a sink nor supply its own. The same construction-held boundary covers
  `MailboxControl`, `MailboxTermination`, `ActorIdentity`, and `DynamicRoute`;
  what remains conventional is the core-to-runtime capability family plus the
  waker machinery that moved to core beside the proxy — `WakerSlot`,
  `WakerAction`, and `WakerEffects` are public doc-hidden core items a direct
  core dependent could construct. They ride under the same framework-only
  ruling; the supported façade re-exports none of them, and the
  external-consumer probe rejects façade reachability for the whole family.
  `MailboxRuntime`
  is nonetheless kept off every locked path: its disposal capability hands
  work to a blocking worker, so it belongs to the effects flush like the user
  code it carries. That is a preference, not a prohibition, and
  `RetainedExit::drop` is where the difference shows: a submission runs no user
  code, so it is legal under a lock, but it can cost a native thread start, so
  a caller that already owns an effects sink should still flush it.
  Retained exits retire from drop glue, which has no sink to reach, so they
  submit in place.
- **Nested framework locks in one direction.** The resident-tree observation
  gate is outermost: everything else is taken under it or standalone, never
  around it. Inside, the documented orders are cells' member mailbox →
  `MailboxCell::state`, and `MailboxCell::state` → `SendOperation::state`.
  Gate-to-gate is the one exception to "outermost":
  `MemberCell::with_handoff_gate` takes the adoptee's *current* gate while the
  adopting parent's is held, in the parent-to-child direction only, and
  re-reads the installed pointer under the acquired guard so a concurrent
  handoff retries rather than deadlocks. Both of its users — plain adoption
  (`ScopeCell::adopt_observation_gate`) and admission
  (`ScopeCell::admit_observation_gate`, `ScopeCell::admit_child_locked`) —
  hold the exemption. Admission widens what runs inside the doubled section:
  the member record's watch-value mutex is taken there, and admission's
  `MemberTransition` reaches `RetainedExit` clone/drop under both gates. That
  stays inside the rule — the record is framework-owned data and the retained
  clone is provably non-last — but it is the deepest the exemption goes, so a
  new operation added to the doubled section needs the same accounting.
  Dynamic admission has two further, narrowly accounted shapes around this
  order. `DynamicControl::reserve` holds its state mutex while
  `mint_reserved_slot` briefly takes the already-published parent scope's
  child-identity mutex, then adoption acquires the new child's gate. The
  identity API releases its guard before returning and cannot call back into
  dynamic control, so no reverse edge can be held; the child gate belongs to a
  member minted inside the state critical section and remains unpublished,
  hence uncontended. Admission install holds the same state mutex inside the
  root gate while calling `admit_child_locked`, but the reserved member was
  already adopted onto that root gate and the root cannot be re-homed while
  its dynamic route is live, so the handoff check short-circuits without a
  second gate acquisition. Changing either publication or re-homing invariant
  requires re-deriving this exception.
  `WakerProxy`'s mutex (`crates/shelterwood-core/src/waker_proxy.rs`) sits at the
  other end of that order: it is a **leaf**. `wake_by_ref` acquires it from
  whatever thread drives the external primitive — the timer driver, a sender,
  any executor — so no framework lock may be taken under it, and a wider lock
  may sit above it only through the effects-sink shapes, which move every
  caller-waker clone, wake and drop past the unlock.

Two conventions that are not the rule itself but travel with it: panicking
while holding a mutex the codebase `.expect()`s poisons it for every later
caller, so compute the verdict, release, *then* panic (`MailboxCell::bind` is
the pattern; where releasing is impossible, `debug_assert!` instead, as
`MailboxState::take_waiters` does). And a value that may block on destruction
goes to `runtime::dispose_detached`, not merely past the unlock. That second
convention is currently met for mailbox payloads, construction closures,
blocking-offload captured state, displaced resident graphs, and rejected
lifecycle events and admission projections. Raw incarnation-owned values —
async offload futures, continuations, timers and completions — instead
follow SPEC §6.5's on-task contained funnel, because a destructor panic
there is cleanup evidence that exit classification must fold in.
Framework-retained `Exit` copies meet it through `RetainedExit`,
including driver completions and pending terminal disposal; exits handed to
users keep ordinary drop timing. Its fail-safe under exhausted thread
creation is an unreclaimed queued job — memory held for the life of the
process, along with whatever the user error owns — which is the accepted
trade against destroying it in a critical section.

Reviewing a new `.lock()` is one pass: name every value the critical section
can destroy, every callback it can invoke, and every panic it can raise. If
any of those is user code, hand it to a transaction (`txn.defer`), an effects
struct, or the caller.

## Runtime naming

Non-test façade implementation imports and invokes only the runtime capability
layer, never a concrete Tokio API. After the crate fold, façade tests, doctests
and examples may use the pinned Tokio dev-dependency for executor control,
test-only synchronization or runnable example syntax; internal unit tests
still prefer `crate::runtime` when it exposes the behavior under test. This
dev-only allowance does not widen the public API or the core crate's dependency
boundary.

## Running anything

All tooling (`cargo`, `just`, `nextest`, `nixfmt`) comes from the Nix
devshell. It is **not** on the base PATH. Prefix commands with `./tools/dev`:

```sh
./tools/dev just ci          # the full local CI mirror
./tools/dev just test
./tools/dev cargo nextest run --workspace
```

`./tools/dev` exec's straight through when the devshell for *this* checkout is already
active, so it is free in an interactive shell and correct everywhere else. Use
it rather than assuming direnv has loaded — see below for why.

`just ci` is the local mirror of CI; `just ci-nix` runs the authoritative clean
Nix lane. CI is defined by the `rust-env` flake input (`mkRustProject` from
rust-nix-template) plus any overrides in `flake.nix`, and the `justfile`
recipes mirror those checks — keep the two in sync when changing either.

## Worktrees

Create them under `.worktrees`.

`./tools/dev` gives any worktree the correct toolchain no matter how it was created:

- direnv never loads in non-interactive (agent) shells, and `direnv allow` is
  keyed on the absolute `.envrc` path, so a fresh worktree has no toolchain on
  the base PATH at all.
- A shell spawned from another checkout inherits *that* checkout's devshell, so
  a worktree can appear to work while silently using the wrong toolchain.
  `./tools/dev` detects this via `REPO_DEVSHELL` and re-enters the right one.
