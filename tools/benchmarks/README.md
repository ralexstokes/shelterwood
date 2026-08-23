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

## Measurement contract

- Each benchmark name identifies the exact work inside the timed region.
- Fixture construction stays outside that region unless the benchmark is
  explicitly measuring construction or a cold lifecycle.
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
