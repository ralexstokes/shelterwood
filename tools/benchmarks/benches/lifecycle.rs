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
    Backoff, DynamicScopeRef, DynamicTree, Intensity, LifecycleEvents, RemoveOutcome,
    RestartCondition, RestartPolicy, ScopeRef, SnapshotReceiver, SubtreeDef, System, TaskDef, Tree,
};
use tokio::{runtime::Runtime, sync::Notify};

const DEADLINE: Duration = Duration::from_secs(30);
const WIDTHS: [usize; 3] = [1, 16, 256];
const DEPTHS: [usize; 3] = [1, 8, 32];
const RESTARTS: [usize; 3] = [1, 8, 32];
const CHURN_CYCLES: usize = 32;

fn cooperative_task() -> TaskDef {
    TaskDef::new(|context| async move {
        context.shutdown_token().cancelled().await;
        Ok(())
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
        Intensity::new(restarts as u64, Duration::from_secs(30))
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
    for restarts in RESTARTS {
        group.throughput(Throughput::Elements(restarts as u64));
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

enum Observation {
    None,
    Snapshots(SnapshotReceiver),
    Lifecycle(LifecycleEvents),
    Both(SnapshotReceiver, LifecycleEvents),
}

impl Observation {
    fn subscribe(mode: &str, scope: &ScopeRef) -> Self {
        match mode {
            "none" => Self::None,
            "snapshots" => Self::Snapshots(scope.subscribe_snapshots()),
            "lifecycle" => Self::Lifecycle(scope.subscribe_lifecycle()),
            "both" => Self::Both(scope.subscribe_snapshots(), scope.subscribe_lifecycle()),
            _ => unreachable!("the benchmark supplies a known observation mode"),
        }
    }

    fn retain(&self) {
        match self {
            Self::None => {}
            Self::Snapshots(receiver) => {
                black_box(receiver);
            }
            Self::Lifecycle(events) => {
                black_box(events);
            }
            Self::Both(receiver, events) => {
                black_box((receiver, events));
            }
        }
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
        let observation = Observation::subscribe(mode, scope.as_scope());
        group.bench_function(mode, |b| {
            b.to_async(runtime).iter(|| async {
                for _ in 0..CHURN_CYCLES {
                    let task = scope
                        .add_task("worker", cooperative_task())
                        .await
                        .expect("the dynamic benchmark admits its task");
                    let outcome = scope.remove_task(&task).await;
                    black_box(outcome);
                    debug_assert_eq!(outcome, RemoveOutcome::Removed);
                }
            });
        });
        observation.retain();
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
