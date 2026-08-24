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
  dynamic admission/removal against a drained snapshot subscriber, a drained
  lifecycle subscriber, both, or no subscriber at all.

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

Cold lifecycle fixtures build their top-level declaration outside the timed
region; `spawn`, lowering, startup, shutdown, and joining are timed. The two
sweeps differ in how much declaration survives into that region. `cold_width`
builds all N `TaskDef`s in setup, so only lowering is timed. `cold_depth`
builds only the outermost level eagerly: `SubtreeDef::factory` stores a
closure that each nested incarnation invokes at construction, so a depth-32
sample constructs 31 further trees and their definition `Arc`s inside the
timed region. That is deliberate — a cold lifecycle is what the sweep measures
— but the depth numbers include recursive declaration construction and the
width numbers do not. `cold_depth/*/1` and `cold_width/*/1` build the
identical one-task fixture on purpose: it is the shared intercept of both
sweeps and a same-run check on host noise.

`immediate_restarts` reports no element rate. Its timed region is a whole
system lifecycle — `spawn`, `wait_started`, the restart cascade, `shutdown`,
and the root driver join — so dividing by the restart count would attribute
that fixed lifecycle cost to every restart. The sweep instead carries a
zero-restart point; the marginal cost of one restart is the difference from
that baseline divided by the restart count. The declared intensity budget
carries head room over the exact number of charges the fixture provokes, so an
unexpected charge cannot trip the scope and turn a slow sample into a panic.

`dynamic_churn` starts its root outside the timed region, so it isolates live
control-plane admission/removal and observation publication. Each cycle admits
a task, waits for that incarnation's first poll, then removes it. The
readiness rendezvous is inside the timed region and is there for determinism:
removal latches immediately, so without it the removal races the driver's
start transaction, the `Removing` mark suppresses the `Ready` edge, and the
edge count per cycle varies between four and five — measured at 96 to 120
edges per 24-cycle batch on a four-worker runtime, a 25% swing in the payload
the arms are meant to hold constant. With the rendezvous every cycle emits
exactly five edges (`Added`, `Started`, `Ready`, `Exited`, `Removed`) under
both runtime configurations.

The four `dynamic_churn` arms differ only in who is subscribed. Each
subscribing arm parks a dedicated consumer task on its stream for the whole
arm rather than draining inside the timed future, so publication has a real
waiter to wake instead of an empty waiter list, and the lifecycle broadcast
ring does not saturate: the arms measure the keeping-up-consumer regime, not a
permanently lagged one. On the current-thread group that consumer is driven by
the same `block_on` Criterion times, so its cost is attributed; on the
multi-thread group it runs on another worker and only its wake and contention
costs are. `none` is not an observer-free baseline for lifecycle work:
snapshot publication skips a scope with no receivers, but lifecycle emission
has no such gate — every edge mints retention guards, resolves ancestors,
mints the sequence, builds the event, clones it per ancestor, takes the hub's
signal mutex and the broadcast tail lock, and pulses the watch, whether or not
anyone is subscribed. Only the ring write itself is skipped, and that is the
channel's own zero-receiver check rather than a Shelterwood gate. The
`lifecycle` arm's delta over `none` is therefore bounded below by that
unconditional work and measures only the incremental cost of a subscribed,
draining consumer — which on a keeping-up consumer is close to zero.

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
