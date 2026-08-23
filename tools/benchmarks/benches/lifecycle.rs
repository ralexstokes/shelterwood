use std::{
    hint::black_box,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use shelterwood::{
    Backoff, DynamicScopeRef, DynamicTree, Intensity, LifecycleEvents, LifecycleItem,
    RemoveOutcome, RestartCondition, RestartPolicy, ScopeRef, SnapshotReceiver, SubtreeDef, System,
    TaskDef, Tree,
};
use tokio::{runtime::Runtime, sync::Notify, task::JoinHandle};

const DEADLINE: Duration = Duration::from_secs(30);
const WIDTHS: [usize; 3] = [1, 16, 256];
const DEPTHS: [usize; 3] = [1, 8, 32];
/// Restart counts, including a zero-restart baseline.
///
/// The timed region is a whole system lifecycle, so the marginal cost of one
/// restart is only derivable by differencing against that baseline. See
/// `bench_restarts` for why this group reports no element rate.
const RESTARTS: [usize; 4] = [0, 1, 8, 32];
/// Spare restart charges beyond the exact number the fixture provokes.
///
/// A budget of exactly `restarts` is correct at the boundary but has no
/// tolerance: one extra charge from any source would trip the scope and turn
/// a slow sample into a panic instead of a measurement.
const RESTART_BUDGET_HEADROOM: u64 = 8;
const CHURN_CYCLES: usize = 32;

fn cooperative_task() -> TaskDef {
    TaskDef::new(|context| async move {
        context.shutdown_token().cancelled().await;
        Ok(())
    })
}

/// A cooperative task that announces its first poll.
///
/// The dynamic churn loop uses the announcement to remove a child only after
/// its incarnation has started, which pins the emitted lifecycle edge count.
fn announcing_task(started: Arc<Notify>) -> TaskDef {
    TaskDef::new(move |context| {
        let started = Arc::clone(&started);
        async move {
            started.notify_one();
            context.shutdown_token().cancelled().await;
            Ok(())
        }
    })
}

fn wide_ordered(width: usize) -> Tree {
    let mut tree = Tree::new();
    for index in 0..width {
        tree.add_task(format!("task-{index}"), cooperative_task())
            .expect("benchmark child ids are unique");
    }
    tree
}

fn wide_dynamic(width: usize) -> DynamicTree {
    let mut tree = DynamicTree::new();
    for index in 0..width {
        tree.add_task(format!("task-{index}"), cooperative_task())
            .expect("benchmark child ids are unique");
    }
    tree
}

// Only the outermost level is built here. `SubtreeDef::factory` stores the
// recursion, so every nested level is constructed lazily at its incarnation's
// construction — inside the timed region. The README records that asymmetry
// against the width sweep.
fn deep_ordered(depth: usize) -> Tree {
    let mut tree = Tree::new();
    if depth == 1 {
        tree.add_task("leaf", cooperative_task())
            .expect("the leaf declaration is valid");
    } else {
        tree.add_subtree(
            "scope",
            SubtreeDef::factory(move || deep_ordered(depth - 1)),
        )
        .expect("the nested declaration is valid");
    }
    tree
}

fn deep_dynamic(depth: usize) -> DynamicTree {
    let mut tree = DynamicTree::new();
    if depth == 1 {
        tree.add_task("leaf", cooperative_task())
            .expect("the leaf declaration is valid");
    } else {
        tree.add_subtree(
            "scope",
            SubtreeDef::factory(move || deep_dynamic(depth - 1)),
        )
        .expect("the nested declaration is valid");
    }
    tree
}

async fn ordered_lifecycle(tree: Tree) {
    let system = tree.spawn().expect("the benchmark runtime is active");
    system
        .wait_started()
        .await
        .expect("the benchmark tree starts");
    system
        .shutdown(DEADLINE)
        .await
        .expect("the benchmark tree shuts down within its generous bound");
}

async fn dynamic_lifecycle(tree: DynamicTree) {
    let system = tree.spawn().expect("the benchmark runtime is active");
    system
        .wait_started()
        .await
        .expect("the benchmark tree starts");
    system
        .shutdown(DEADLINE)
        .await
        .expect("the benchmark tree shuts down within its generous bound");
}

fn bench_cold_lifecycle(c: &mut Criterion, runtime_name: &str, runtime: &Runtime) {
    let mut width_group = c.benchmark_group(format!("lifecycle/{runtime_name}/cold_width"));
    for width in WIDTHS {
        width_group.throughput(Throughput::Elements(width as u64));
        width_group.bench_with_input(BenchmarkId::new("ordered", width), &width, |b, &width| {
            b.to_async(runtime).iter_batched(
                || wide_ordered(width),
                ordered_lifecycle,
                BatchSize::PerIteration,
            );
        });
        width_group.bench_with_input(BenchmarkId::new("dynamic", width), &width, |b, &width| {
            b.to_async(runtime).iter_batched(
                || wide_dynamic(width),
                dynamic_lifecycle,
                BatchSize::PerIteration,
            );
        });
    }
    width_group.finish();

    let mut depth_group = c.benchmark_group(format!("lifecycle/{runtime_name}/cold_depth"));
    for depth in DEPTHS {
        depth_group.throughput(Throughput::Elements(depth as u64));
        depth_group.bench_with_input(BenchmarkId::new("ordered", depth), &depth, |b, &depth| {
            b.to_async(runtime).iter_batched(
                || deep_ordered(depth),
                ordered_lifecycle,
                BatchSize::PerIteration,
            );
        });
        depth_group.bench_with_input(BenchmarkId::new("dynamic", depth), &depth, |b, &depth| {
            b.to_async(runtime).iter_batched(
                || deep_dynamic(depth),
                dynamic_lifecycle,
                BatchSize::PerIteration,
            );
        });
    }
    depth_group.finish();
}

fn restarting_tree(restarts: usize) -> (Tree, Arc<Notify>) {
    let starts = Arc::new(AtomicUsize::new(0));
    let stable = Arc::new(Notify::new());
    let mut tree = Tree::new();
    tree.intensity(
        Intensity::new(
            restarts as u64 + RESTART_BUDGET_HEADROOM,
            Duration::from_secs(30),
        )
        .expect("the benchmark intensity window is non-zero"),
    );
    tree.add_task(
        "worker",
        TaskDef::new({
            let starts = Arc::clone(&starts);
            let stable = Arc::clone(&stable);
            move |context| {
                let generation = starts.fetch_add(1, Ordering::Relaxed) + 1;
                let stable = Arc::clone(&stable);
                async move {
                    if generation <= restarts {
                        Ok(())
                    } else {
                        stable.notify_one();
                        context.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            }
        })
        .restart(RestartPolicy::new(
            RestartCondition::Always,
            Backoff::Immediate,
        )),
    )
    .expect("the restart benchmark declaration is valid");
    (tree, stable)
}

fn bench_restarts(c: &mut Criterion, runtime_name: &str, runtime: &Runtime) {
    let mut group = c.benchmark_group(format!("lifecycle/{runtime_name}/immediate_restarts"));
    // Deliberately no `Throughput::Elements`. The timed region is a whole
    // system lifecycle — spawn, startup, the restart cascade, shutdown, and
    // the root driver join — so a per-restart rate would charge every restart
    // with the fixed lifecycle cost. The `0` point makes the marginal restart
    // cost derivable by differencing instead.
    for restarts in RESTARTS {
        group.bench_with_input(
            BenchmarkId::from_parameter(restarts),
            &restarts,
            |b, &restarts| {
                b.to_async(runtime).iter_batched(
                    || restarting_tree(restarts),
                    |(tree, stable)| async move {
                        let system = tree.spawn().expect("the benchmark runtime is active");
                        system
                            .wait_started()
                            .await
                            .expect("the first incarnation becomes ready");
                        stable.notified().await;
                        system
                            .shutdown(DEADLINE)
                            .await
                            .expect("the stable incarnation shuts down");
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

/// What one draining consumer task observed over an arm.
#[derive(Default)]
struct DrainCounts {
    items: AtomicUsize,
    lagged: AtomicUsize,
}

/// The consumers an observation arm parks on its streams.
///
/// The arms exist to price observation, so a subscriber that never consumes
/// prices the wrong thing: `WatchSender::pulse` walks an empty waiter list and
/// the lifecycle ring saturates into a permanently lagged regime. A consumer
/// task parked on the stream for the whole arm, rather than a drain inside the
/// timed future, is what keeps a waiter present at the instant of publication.
struct Observers {
    handles: Vec<JoinHandle<()>>,
    snapshots: Option<Arc<DrainCounts>>,
    lifecycle: Option<Arc<DrainCounts>>,
}

impl Observers {
    fn spawn(mode: &str, runtime: &Runtime, scope: &ScopeRef) -> Self {
        let mut observers = Self {
            handles: Vec::new(),
            snapshots: None,
            lifecycle: None,
        };
        match mode {
            "none" => {}
            "snapshots" => observers.drain_snapshots(runtime, scope.subscribe_snapshots()),
            "lifecycle" => observers.drain_lifecycle(runtime, scope.subscribe_lifecycle()),
            "both" => {
                observers.drain_snapshots(runtime, scope.subscribe_snapshots());
                observers.drain_lifecycle(runtime, scope.subscribe_lifecycle());
            }
            _ => unreachable!("the benchmark supplies a known observation mode"),
        }
        observers
    }

    fn drain_snapshots(&mut self, runtime: &Runtime, mut receiver: SnapshotReceiver) {
        let counts = Arc::new(DrainCounts::default());
        self.snapshots = Some(Arc::clone(&counts));
        self.handles.push(runtime.spawn(async move {
            while let Ok(snapshot) = receiver.changed().await {
                black_box(&snapshot);
                counts.items.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    fn drain_lifecycle(&mut self, runtime: &Runtime, mut events: LifecycleEvents) {
        let counts = Arc::new(DrainCounts::default());
        self.lifecycle = Some(Arc::clone(&counts));
        self.handles.push(runtime.spawn(async move {
            while let Some(item) = events.recv().await {
                match item {
                    LifecycleItem::Event(event) => {
                        black_box(&event);
                        counts.items.fetch_add(1, Ordering::Relaxed);
                    }
                    LifecycleItem::Lagged { dropped } => {
                        counts.lagged.fetch_add(dropped as usize, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    /// Checks the consumers were load-bearing, then retires them.
    fn finish(self, runtime: &Runtime, mode: &str) {
        for (stream, counts) in [
            ("snapshots", &self.snapshots),
            ("lifecycle", &self.lifecycle),
        ] {
            let Some(counts) = counts else { continue };
            assert!(
                counts.items.load(Ordering::Relaxed) > 0,
                "the {mode} arm's {stream} consumer observed nothing, so the arm did not \
                 measure a woken subscriber",
            );
            // Lag is a regime change, not an error: record it so a run that
            // silently fell into the lagged regime is still visible.
            black_box(counts.lagged.load(Ordering::Relaxed));
        }
        runtime.block_on(async move {
            for handle in self.handles {
                handle.abort();
                let _ = handle.await;
            }
        });
    }
}

fn start_dynamic(runtime: &Runtime) -> (System<DynamicScopeRef>, DynamicScopeRef) {
    runtime.block_on(async {
        let system = DynamicTree::new()
            .spawn()
            .expect("the benchmark runtime is active");
        system
            .wait_started()
            .await
            .expect("the empty dynamic root starts");
        let scope = system.scope();
        (system, scope)
    })
}

fn stop_dynamic(runtime: &Runtime, system: System<DynamicScopeRef>) {
    runtime.block_on(async {
        system
            .shutdown(DEADLINE)
            .await
            .expect("the dynamic benchmark root shuts down");
    });
}

fn bench_dynamic_churn(c: &mut Criterion, runtime_name: &str, runtime: &Runtime) {
    let mut group = c.benchmark_group(format!("lifecycle/{runtime_name}/dynamic_churn"));
    group.throughput(Throughput::Elements(CHURN_CYCLES as u64));

    for mode in ["none", "snapshots", "lifecycle", "both"] {
        let (system, scope) = start_dynamic(runtime);
        let observers = Observers::spawn(mode, runtime, scope.as_scope());
        group.bench_function(mode, |b| {
            b.to_async(runtime).iter(|| async {
                for _ in 0..CHURN_CYCLES {
                    let started = Arc::new(Notify::new());
                    let task = scope
                        .add_task("worker", announcing_task(Arc::clone(&started)))
                        .await
                        .expect("the dynamic benchmark admits its task");
                    // Removal latches immediately, so without this rendezvous
                    // it races the driver's start transaction and the
                    // `Removing` mark suppresses the `Ready` edge. Waiting
                    // pins every cycle at five edges under both runtime
                    // configurations; see the README.
                    started.notified().await;
                    let outcome = scope.remove_task(&task).await;
                    // Promoted from `debug_assert_eq!`: `cargo bench` builds
                    // the release-derived `bench` profile, where a debug
                    // assertion is compiled out and this loop would have no
                    // correctness guard at all.
                    assert_eq!(
                        outcome,
                        RemoveOutcome::Removed,
                        "each churn cycle must remove the membership it admitted",
                    );
                }
            });
        });
        observers.finish(runtime, mode);
        stop_dynamic(runtime, system);
    }
    group.finish();
}

fn run_runtime(c: &mut Criterion, name: &str, runtime: Runtime) {
    bench_cold_lifecycle(c, name, &runtime);
    bench_restarts(c, name, &runtime);
    bench_dynamic_churn(c, name, &runtime);
}

fn bench_lifecycle(c: &mut Criterion) {
    let current_thread = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the current-thread benchmark runtime builds");
    run_runtime(c, "current_thread", current_thread);

    let multi_thread = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("the multi-thread benchmark runtime builds");
    run_runtime(c, "multi_thread_4", multi_thread);
}

criterion_group!(benches, bench_lifecycle);
criterion_main!(benches);
