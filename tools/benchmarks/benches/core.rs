use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use shelterwood_core::{
    ChildId, Membership, ScopeIdentity, ScopeState, StopReason,
    engine::{DeadlineHandle, DeadlineQueue, ScopeLifecycle},
    policy::ScopeFlavor,
    supervisor::{self, ChildKey, Effect, Event, SupervisorState},
};

const SCOPE_SIZES: [usize; 4] = [1, 16, 256, 4_096];
const DEADLINE_SIZES: [usize; 3] = [16, 256, 4_096];
/// `compact_if_sparse` retains the heap only once `entries > 2 * registrations`,
/// so a cancellation share must exceed half the queue to reach it. 50 lands
/// just under the threshold and 75 just over it; both sides are measured.
const CANCELLED_PERCENTS: [usize; 4] = [0, 50, 75, 90];

fn memberships(count: usize) -> Vec<Membership> {
    let mut identity = ScopeIdentity::new();
    (0..count)
        .map(|index| {
            identity
                .mint_membership(&ChildId::from(format!("child-{index}")))
                .expect("the benchmark membership domain remains available")
                .into_pair()
                .0
        })
        .collect()
}

fn flavor_name(flavor: ScopeFlavor) -> &'static str {
    match flavor {
        ScopeFlavor::Ordered => "ordered",
        ScopeFlavor::Dynamic => "dynamic",
    }
}

fn populated_state(
    flavor: ScopeFlavor,
    memberships: &[Membership],
    lifecycle: ScopeLifecycle,
) -> (SupervisorState, Vec<ChildKey>) {
    let mut state = SupervisorState::new(flavor, lifecycle);
    let children = memberships
        .iter()
        .copied()
        .map(|membership| {
            supervisor::admit(&mut state, membership, true)
                .expect("each benchmark membership is unique")
        })
        .collect();
    (state, children)
}

fn settle(state: &mut SupervisorState, effects: &mut Vec<Effect>) {
    effects.clear();
    supervisor::step(state, Event::Settle, effects);
    black_box(&*effects);
}

/// Drives every child to readiness, one child at a time.
///
/// The trailing check is a plain `assert!`: `cargo bench` builds the `bench`
/// profile, which inherits `release`, so a `debug_assert!` here would never
/// run. That matters more than usual because the reducer silently ignores an
/// event outside an acceptance set — every arm of `SupervisorState::apply`
/// returns without signalling. Were an acceptance set to change, this loop
/// would quietly become a sequence of no-op steps and the benchmark would
/// report a large speedup instead of a failure. The check costs one scan per
/// timed iteration against a sequence that is already quadratic in the child
/// count.
fn start_all(state: &mut SupervisorState, children: &[ChildKey], effects: &mut Vec<Effect>) {
    settle(state, effects);
    for &child in children {
        effects.clear();
        supervisor::step(state, Event::Spawned { child }, effects);
        supervisor::step(
            state,
            Event::Ready {
                child,
                removal_latched: false,
            },
            effects,
        );
        settle(state, effects);
    }
    assert_eq!(
        state.lifecycle().state(),
        ScopeState::Running,
        "the startup sequence must actually start the scope"
    );
}

/// Tears every child down, one child at a time.
///
/// Carries the same always-on completion check as [`start_all`], for the same
/// reason.
fn terminate_all(
    state: &mut SupervisorState,
    children: impl Iterator<Item = ChildKey>,
    effects: &mut Vec<Effect>,
) {
    for child in children {
        effects.clear();
        supervisor::step(state, Event::StopStarted { child }, effects);
        supervisor::step(state, Event::IncarnationComplete { child }, effects);
        supervisor::step(state, Event::DisposalStarted { child }, effects);
        supervisor::step(state, Event::Terminalized { child }, effects);
        settle(state, effects);
    }
    assert!(
        state.all_children_joined(),
        "the teardown sequence must actually join every child"
    );
}

/// The supervisor cases whose work is linear in the child count.
///
/// `admit` and `force` each touch every child a bounded number of times, so an
/// element rate is a meaningful normalization for them. The settle-driven
/// cases are quadratic and live in their own group; see
/// [`bench_supervisor_settle`].
fn bench_supervisor_linear(c: &mut Criterion, flavor: ScopeFlavor) {
    let mut group = c.benchmark_group(format!("core/supervisor/{}", flavor_name(flavor)));

    for size in SCOPE_SIZES {
        let members = memberships(size);
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("admit", size), &size, |b, _| {
            b.iter_batched(
                || SupervisorState::new(flavor, ScopeLifecycle::starting()),
                |mut state| {
                    for membership in members.iter().copied() {
                        black_box(supervisor::admit(&mut state, membership, true));
                    }
                    black_box(state)
                },
                BatchSize::SmallInput,
            );
        });

        // `force` pushes one `ForceChild` per incomplete child. Its
        // `begin_drain` adds one `StopChild` per child for `Dynamic`, which
        // stops the whole set at once; `Ordered` only seeds a cursor there.
        let force_effects = match flavor {
            ScopeFlavor::Ordered => size,
            ScopeFlavor::Dynamic => size.saturating_mul(2),
        };
        group.bench_with_input(BenchmarkId::new("force", size), &size, |b, _| {
            b.iter_batched(
                || {
                    let (mut state, children) =
                        populated_state(flavor, &members, ScopeLifecycle::starting());
                    start_all(&mut state, &children, &mut Vec::new());
                    (state, Vec::with_capacity(force_effects))
                },
                |(mut state, mut effects)| {
                    black_box(supervisor::force(&mut state, &mut effects));
                    black_box((state, effects))
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

/// The supervisor cases driven by a per-child settle schedule.
///
/// These deliberately settle after every child, and every settle rescans the
/// child table — `settle_finish` alone evaluates `all_children_joined`, and
/// `settle_startup` scans for unready initial memberships. The schedule is
/// therefore quadratic in the child count, so the group declares no
/// `Throughput`: a per-element rate would read as a collapse across the sweep
/// when nothing but the schedule changed. Compare a size against itself over
/// time, not against its neighbours.
fn bench_supervisor_settle(c: &mut Criterion, flavor: ScopeFlavor) {
    let mut group = c.benchmark_group(format!("core/supervisor/{}/settle", flavor_name(flavor)));

    for size in SCOPE_SIZES {
        let members = memberships(size);
        // The effects buffer is allocated in setup, not in the timed region:
        // at the small end of the sweep the allocation was a measurable share
        // of the sample. `settle` and the per-child loops clear it, so each
        // step still measures the reducer rather than a reallocation.
        let effects_capacity = size.saturating_add(1);

        group.bench_with_input(BenchmarkId::new("startup", size), &size, |b, _| {
            b.iter_batched(
                || {
                    let (state, children) =
                        populated_state(flavor, &members, ScopeLifecycle::starting());
                    (state, children, Vec::with_capacity(effects_capacity))
                },
                |(mut state, children, mut effects)| {
                    start_all(&mut state, &children, &mut effects);
                    black_box((state, children, effects))
                },
                BatchSize::LargeInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("drain", size), &size, |b, _| {
            b.iter_batched(
                || {
                    let (mut state, children) =
                        populated_state(flavor, &members, ScopeLifecycle::starting());
                    start_all(&mut state, &children, &mut Vec::new());
                    (state, children, Vec::with_capacity(effects_capacity))
                },
                |(mut state, children, mut effects)| {
                    supervisor::step(
                        &mut state,
                        Event::BeginDrain {
                            reason: StopReason::ShutdownRequested,
                        },
                        &mut effects,
                    );
                    match flavor {
                        ScopeFlavor::Ordered => {
                            terminate_all(&mut state, children.iter().rev().copied(), &mut effects)
                        }
                        ScopeFlavor::Dynamic => {
                            terminate_all(&mut state, children.iter().copied(), &mut effects)
                        }
                    }
                    black_box((state, children, effects))
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

fn bench_supervisor(c: &mut Criterion) {
    for flavor in [ScopeFlavor::Ordered, ScopeFlavor::Dynamic] {
        bench_supervisor_linear(c, flavor);
        bench_supervisor_settle(c, flavor);
    }
}

fn deadline_at(now: Instant, index: usize) -> Instant {
    now + Duration::from_nanos((index % 31) as u64)
}

/// Arms `size` deadlines without retaining their handles.
///
/// A `DeadlineHandle` is a `Copy` newtype over a never-reused registration id
/// and has no destructor, so dropping one leaves its entry armed and
/// registered. Collecting the handles is therefore pure benchmark
/// scaffolding, and the push/pop case omits it so the timed region holds only
/// queue work.
fn fill_deadlines(queue: &mut DeadlineQueue<usize>, now: Instant, size: usize) {
    for index in 0..size {
        black_box(queue.push(deadline_at(now, index), index));
    }
}

fn deadline_fixture(size: usize) -> (DeadlineQueue<usize>, Vec<DeadlineHandle>, Instant) {
    let now = Instant::now();
    let mut queue = DeadlineQueue::default();
    let handles = (0..size)
        .map(|index| queue.push(deadline_at(now, index), index))
        .collect();
    (queue, handles, now)
}

/// Arming and draining a queue, both inside the timed region.
///
/// This is the one deadline case that measures `push`. Its element rate is not
/// comparable with the `core/deadline/cancel` group, which arms its queue in
/// `iter_batched` setup and times only the cancel-and-drain half.
fn bench_deadline_push_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("core/deadline/push_pop");

    for size in DEADLINE_SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                Instant::now,
                |now| {
                    let mut queue = DeadlineQueue::default();
                    fill_deadlines(&mut queue, now, size);
                    let due = now + Duration::from_secs(1);
                    for _ in 0..size {
                        black_box(queue.pop_due(due).expect("every deadline is due"));
                    }
                    black_box(queue)
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Cancelling a share of an armed queue, then draining the remainder.
///
/// The queue is armed in setup, so the timed region covers cancellation, the
/// heap compaction it may trigger, and the drain. The `0` case cancels nothing
/// and exists as the drain-only baseline the other shares are read against.
fn bench_deadline_cancel(c: &mut Criterion) {
    let mut group = c.benchmark_group("core/deadline/cancel");

    for size in DEADLINE_SIZES {
        group.throughput(Throughput::Elements(size as u64));
        for cancelled_percent in CANCELLED_PERCENTS {
            let case = if cancelled_percent == 0 {
                "baseline_no_cancel".to_owned()
            } else {
                format!("cancel_{cancelled_percent}_percent")
            };
            group.bench_with_input(BenchmarkId::new(case, size), &size, |b, &size| {
                b.iter_batched(
                    || deadline_fixture(size),
                    |(mut queue, handles, now)| {
                        let cancelled = size.saturating_mul(cancelled_percent) / 100;
                        for handle in handles.into_iter().take(cancelled) {
                            black_box(queue.cancel(handle));
                        }
                        let due = now + Duration::from_secs(1);
                        while let Some(key) = queue.pop_due(due) {
                            black_box(key);
                        }
                        black_box(queue)
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_supervisor,
    bench_deadline_push_pop,
    bench_deadline_cancel
);
criterion_main!(benches);
