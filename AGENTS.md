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

- **`ObservationTxn`** (`shelterwood-cells/src/cells.rs`) holds the
  observation-gate guard plus a deferred-effect list. `defer`/`pulse` queue
  work; `commit` drops the guard *then* runs the queue through a
  `PanicAccumulator`. Its `Drop` runs the same path during an unwind, so a
  poisoned transaction cannot strand already-committed wakes. Every retained
  control-plane writer takes the token, which makes an out-of-transaction
  mutation unavailable by construction.
- **`MailboxTxn`** (`shelterwood-mailbox/src/cell.rs`) is the same idea for
  every mutable mailbox transition. It owns the state guard beside a
  `MailboxEffects` sink that collects signal pulses, waker wake/drop actions,
  displaced payloads and isolated-disposal requests; its `Drop` empties the
  guard field before Rust drops the sink, so the flush necessarily runs with no
  mailbox mutex held, unwind included. `WakerSlot` makes the waker half
  structural rather than conventional: its storage is private even from the
  parent module, and no operation returns or replaces a `Waker` without an
  effects sink. `Withdrawal`, `Termination` and `MailboxPayload` are its
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
  post-unlock effects together, and `SnapshotHub`'s deferred retirement all
  encode that.
- **`Arc` traffic that cannot reach zero.** Cloning an `Arc` under a lock is
  refcount work; dropping one is only refcount work while another owner is
  provable. Prefer restructuring over the proof — every violation the #235
  audit confirmed was a drop that *looked* like refcount traffic. One
  exemption still rests on such a proof: `ScopeCell::clear_residents_locked`
  and `prune_child_locked` drop `Arc<MemberCell>`s under the gate, and a
  member record owns an `Exit`. They are safe only because the driver's own
  child map outlives residency (`ScopeRuntime`'s `Drop` clears residents
  before `self.children`), which is an ordering nothing enforces.
- **Framework `dyn` seams.** `MailboxControl`, `MailboxTermination`,
  `MailboxRuntime` and `DynamicRoute` are cross-crate implementation seams with
  framework-only impls, not user traits; calling them under a lock runs no
  caller code. `MailboxRuntime` is nonetheless kept off every locked path: its
  disposal capability hands work to a blocking worker, so it belongs to the
  effects flush like the user code it carries.
- **Nested framework locks in one direction.** The resident-tree observation
  gate is outermost: everything else is taken under it or standalone, never
  around it. Inside, the documented orders are cells' member mailbox →
  `MailboxCell::state`, and `MailboxCell::state` → `SendOperation::state`.
  Gate-to-gate is the one exception to "outermost": `adopt_observation_gate`
  takes the adoptee's *current* gate while the adopting parent's is held, in
  the parent-to-child direction only, and re-reads the installed pointer
  under the acquired guard so a concurrent handoff retries rather than
  deadlocks.

Two conventions that are not the rule itself but travel with it: panicking
while holding a mutex the codebase `.expect()`s poisons it for every later
caller, so compute the verdict, release, *then* panic (`MailboxCell::bind` is
the pattern; where releasing is impossible, `debug_assert!` instead, as
`MailboxState::take_waiters` does). And a value that may block on destruction
goes to `runtime::dispose_detached`, not merely past the unlock. That second
convention is currently met for mailbox payloads, construction closures and
offloads, and *not* met for an `Exit`'s application error: no site in the
workspace routes an `Exit` through isolated disposal, while the driver
destroys exits inline on its own thread on several paths that hold no lock at
all. Treat that as a known gap to close as a class, not by detaching whichever
site a review happens to land on — a half-detached class reads as if the
whole class were handled.

Reviewing a new `.lock()` is one pass: name every value the critical section
can destroy, every callback it can invoke, and every panic it can raise. If
any of those is user code, hand it to a transaction (`txn.defer`), an effects
struct, or the caller.

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
