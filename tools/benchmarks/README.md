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
  Four details govern how to read it:
  - The timed region of every batched send case ends at the last accepted
    send. `Mailbox::Queue` is FIFO, so a reply barrier inside that region
    would have to wait for the handler to run the whole batch, reporting
    `enqueue + handler + wakeups + a round trip` under the name of a send and
    burying the difference between two send APIs in the shared drain. The
    barrier still runs once per iteration, between measurements, so every
    iteration starts from an empty queue.
  - Capacity selects which operation runs rather than tuning one operation,
    so the arms are named for their regime. `send_buffered` holds the whole
    batch and parks no sender; `send_backpressured` parks every send past its
    capacity, so its timed region legitimately includes consumer scheduling.
    The two are not comparable, and the backpressured arms are the noisy ones.
  - `call` is measured whole. Boxing the message constructor, creating the
    reply channel, and arming and retiring a real deadline wheel entry are
    inside the timed region because the operation cannot be performed without
    them; no deadline ever expires there.
  - The group runs at a reduced sampling budget (20 samples, 0.5 s warm-up,
    2 s measurement) with a 10% noise floor and a 1% significance level. That
    keeps the 48-case suite near three minutes inside `just bench` and keeps
    routine spread on the backpressured arms out of the change reports.
- `lifecycle` covers cold wide and deep trees, immediate restart cycles, and
  dynamic admission/removal with no observer, a snapshot subscriber, a
  lifecycle subscriber, or both.

`concurrent_send_tasks` deliberately includes task spawn and join overhead. It
represents the common application shape of short-lived producer tasks rather
than claiming to isolate mailbox mutex contention alone. Under the
`current_thread` group there is no real contention to isolate: the producers
interleave cooperatively on a single thread, and only the `multi_thread_4`
group exercises parallel senders. Its mailbox capacity sits below the batch
size in both groups, so it is a backpressured arm as well.

The core supervisor `startup` and `drain` cases apply one child's transition
sequence and then settle before advancing to the next child. They deliberately
stress incremental, level-triggered settlement; they do not model the driver's
event-batch distribution. Use the lifecycle suite for end-to-end startup and
shutdown costs.

Cold lifecycle fixtures construct the declaration outside the timed region;
`spawn`, recursive lowering, startup, shutdown, and joining are timed. Dynamic
churn starts its root outside the timed region so it isolates live control-plane
admission/removal and observation publication.

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
