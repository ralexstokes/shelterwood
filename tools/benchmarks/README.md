# Shelterwood benchmarks

This standalone package contains performance experiments rather than
correctness tests. Its own workspace and lockfile keep benchmark-only
dependencies out of ordinary Shelterwood workspace builds.

Run the whole suite from the repository root:

```sh
./tools/dev just bench
```

Compile and lint the harness without collecting measurements:

```sh
./tools/dev just bench-check
```

Criterion accepts a name filter after `--`. For example:

```sh
./tools/dev cargo bench --locked \
  --manifest-path tools/benchmarks/Cargo.toml -- core/deadline
```

The suites separate deterministic structural costs from executor-backed public
operations:

- `core` covers the supervision reducer and deadline queue.
- `mailbox` covers batched send, fail-fast send, request/reply, and concurrent
  producer tasks over both current-thread and four-worker Tokio runtimes.

`concurrent_send_tasks` deliberately includes task spawn and join overhead. It
represents the common application shape of short-lived producer tasks rather
than claiming to isolate mailbox mutex contention alone.

The core supervisor `startup` and `drain` cases apply one child's transition
sequence and then settle before advancing to the next child. They deliberately
stress incremental, level-triggered settlement; they do not model the driver's
event-batch distribution. End-to-end lifecycle benchmarks build on this
harness in the stacked series.

That schedule is **quadratic** in the child count: it settles once per child,
and every settle rescans the child table — `settle_finish` evaluates
`all_children_joined`, and `settle_startup` scans for unready initial
memberships. Both cases therefore live in their own
`core/supervisor/<flavor>/settle` group with no `Throughput` declared, because
a per-element rate would read as a collapse across the size sweep when nothing
but the schedule changed. Compare a size against itself over time, never
against its neighbours. The linear cases — `admit` and `force`, which touch
each child a bounded number of times — stay in `core/supervisor/<flavor>` and
do report an element rate.

The deadline queue cases are split the same way, by where the fixture boundary
falls. `core/deadline/push_pop` arms *and* drains the queue inside the timed
region; `core/deadline/cancel` arms it in `iter_batched` setup and times only
the cancel-and-drain half. Their element rates are not comparable with each
other. Within the `cancel` group, `baseline_no_cancel` cancels nothing and is
the drain-only baseline the other shares are read against, and the shares
bracket `compact_if_sparse`, which retains the heap only once
`entries > 2 * registrations` — 50% lands just under that threshold and 75%
just over it.

## Measurement contract

- Each benchmark name identifies the exact work inside the timed region.
- Fixture construction stays outside that region unless the benchmark is
  explicitly measuring construction or a cold lifecycle. Two cases that place
  the boundary differently belong in different groups, so one throughput
  column never mixes them.
- `Throughput::Elements` is declared per benchmark, never per group, and only
  where the work really is linear in the element count. A schedule that is
  quadratic in its parameter reports wall time only.
- Scratch buffers a benchmark reuses across steps are allocated in
  `iter_batched` setup and cleared, not reallocated, inside the timed region.
- Invariant checks inside a benchmark use `assert!`, not `debug_assert!`:
  `cargo bench` builds the `bench` profile, which inherits `release`, so a
  debug assertion here never runs. A reducer that silently ignores an
  unaccepted event would otherwise turn a broken sequence into a spurious
  speedup.
- This package's `Cargo.lock` must agree with the workspace root's on every
  shared dependency, so the harness measures `shelterwood-core` against the
  dependency set production builds with. Nothing checks this; after
  regenerating either lockfile, re-pin the other with
  `cargo update --manifest-path tools/benchmarks/Cargo.toml -p <crate> --precise <version>`.
- Async hot paths process a batch per timed iteration and report element
  throughput, amortizing benchmark-executor polling overhead.
- Every spawned system is shut down and joined before its fixture is dropped;
  background work from one sample must not leak into another.
- Runtime configurations are separate benchmark groups. Results from a
  current-thread runtime and a multi-thread runtime are not compared as if
  they were the same environment.
- Real sleeps and deadline expiry do not belong in microbenchmarks. Pure state
  machines use fixed instants; end-to-end timeout behavior remains a
  correctness-test concern unless it receives a dedicated load benchmark.
- Benchmark instrumentation never runs while a framework mutex is held. Use
  the public operation boundary or an external profiler instead.

Store the machine, CPU governor, toolchain revision, and Git commit alongside
any published baseline. Criterion's historical comparison is useful only when
those conditions are comparable.
