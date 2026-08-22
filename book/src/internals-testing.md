# How it stays correct

The final question a maintainer needs answered is not how the machinery
works but how a change to it is checked. The verification architecture
follows the construction architecture: because the decision layer is
pure, most properties are tested with no runtime at all, and each higher
gear exists only for what the gear below cannot state.

## The gear ladder

SPEC §16 sets the posture: every conformance obligation must have direct
test coverage, and where §15.3 is followed, prefer the lower gear —
drive the decision state machines directly, events in, effects out — and
reserve integration fixtures for the driver shell and end-to-end
invariants. In practice the suite has three gears:

1. **Pure-machine tests** in `shelterwood-core`, next to each machine:
   effect-sequence assertions against `SupervisorState`, the ladder, the
   readiness gate, the lattice.
2. **The exploration walk** (below) for the properties that are about
   the *whole state space* rather than a chosen schedule.
3. **Integration suites** in `crates/shelterwood/tests/`, organized by
   subsystem (`delivery.rs`, `rebind.rs`, `drain.rs`, `readiness.rs`,
   `offloads.rs`, the waker-proxy files, …) over shared fixtures in
   `tests/common/` — release gates, event recorders, timing helpers,
   hostile wakers.

## The exploration walk

`crates/shelterwood-core/src/supervisor/tests/exploration.rs` walks the
structural reducer exhaustively: once the child roster is fixed, the
reducer is a finite transition system, so the entire reachable state
space is expanded — every state against the *entire* event alphabet,
including events it will reject — and visited once.

Two design points make it trustworthy rather than merely impressive:

- The visited-set fingerprint destructures `SupervisorState`
  exhaustively, so **a new field cannot compile until it is classified**
  into the fingerprint or given a by-construction reason to be excluded.
  A field silently dropped from the fingerprint would prune states — the
  successors only the discarded state had are never explored — and the
  destructure turns that from a review obligation into a compile error.
- Its claims are scoped honestly. The walk checks the
  reducer-expressible subset of §15.3's invariants (R1–R6, E4, S3–S5).
  The stop ladder, the sampled latches, driver death, the exit funnel,
  and tree lowering are not stated over `SupervisorState`, so the walk
  does not claim them — the engine and integration suites own those.

The corollary the project learned early: an exhaustive walk sees
*safety* (nothing bad is reachable); *progress* properties — something
good eventually happens — still need targeted tests, and cross-machine
composition lives in the integration gear.

## Obligations, provocations, and acceptance scenarios

SPEC §16's entries each name the adversarial situation they are about
and, where non-obvious, how to construct it — parked sends on capacity-1
mailboxes of never-receiving actors, drop-counting guards owned by
construction args, hand-polled boxed futures proving `Pending` inside a
rebind window, bounded negative assertions ("this event does *not*
arrive within the window"). Those provocations are a floor, not a
ceiling. Appendix C's acceptance scenarios get their own end-to-end
files (`tests/acceptance_*.rs`).

The hostile inputs are first-class fixtures: test-only `MailboxRuntime`
implementations that panic in their pulse path, wakers that panic or
re-enter, destructor-blocking actors. They exist because the lock rule's
whole premise is that user code misbehaves at framework-chosen moments —
so the suite supplies user code that does.

## Time in tests

Timing properties run under virtual time: the adapter's `test-util`
feature exposes `advance`, and paused-clock tests step it explicitly.
Where a real-clock bound survives, the suite's convention is that it is
*diagnostic only* — a single generous poll timeout covering steps an
idle machine reaches immediately, so the bound exists to fail with a
message instead of hanging. The comment in the acceptance suite states
the reasoning: a machine loaded enough to stretch one bound stretches
them all, so a tighter bound anywhere only relocates a flake rather than
removing it. A timing bound that *is* the property under test belongs
under virtual time, never the wall clock.

## Structural conventions

A few conventions keep the suite honest as it grows:

- **White-box pins live beside the type they pin** and move with it. A
  test asserting an internal invariant (drop-field order, a lock-order
  detail) sits in that module's tests, not in a far-away integration
  file that a refactor would silently orphan.
- **Process isolation is the harness's job.** nextest runs each test in
  its own process, which is what lets abort-class tests — double-panic
  and containment checks — be ordinary `#[test]`s instead of a bespoke
  subprocess harness.
- **Examples are tests.** Every example ends in assertions and runs in
  CI (`just examples`), which is what lets the book quote them without
  a test lane of its own.
- **The seam is provably substitutable.** The capability interface has
  working test implementations inside the façade's own tests; a change
  that breaks substitutability breaks them first.

The full check list is the `ci` recipe in the `justfile` — format, both
clippy passes, nextest plus doctests, examples, rustdoc with denied
warnings, the book build, the three boundary checks from
[The shape of the implementation](internals-shape.md), and Nix
formatting — mirroring the authoritative flake checks that
`just ci-nix` runs clean.
