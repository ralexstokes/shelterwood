use std::{hint::black_box, time::Instant};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use shelterwood_core::{
    ChildId, Membership, ScopeIdentity, ScopeState, StopReason,
    engine::{DeadlineHandle, DeadlineQueue, ScopeLifecycle},
    policy::ScopeFlavor,
    supervisor::{self, ChildKey, Effect, Event, SupervisorState},
};

const SCOPE_SIZES: [usize; 4] = [1, 16, 256, 4_096];
const DEADLINE_SIZES: [usize; 3] = [16, 256, 4_096];

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
    debug_assert_eq!(state.lifecycle().state(), ScopeState::Running);
}

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
}

fn bench_supervisor(c: &mut Criterion) {
    for flavor in [ScopeFlavor::Ordered, ScopeFlavor::Dynamic] {
        let flavor_name = match flavor {
            ScopeFlavor::Ordered => "ordered",
            ScopeFlavor::Dynamic => "dynamic",
        };
        let mut group = c.benchmark_group(format!("core/supervisor/{flavor_name}"));

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

            group.bench_with_input(BenchmarkId::new("startup", size), &size, |b, _| {
                b.iter_batched(
                    || populated_state(flavor, &members, ScopeLifecycle::starting()),
                    |(mut state, children)| {
                        let mut effects = Vec::with_capacity(size.saturating_add(1));
                        start_all(&mut state, &children, &mut effects);
                        black_box((state, effects))
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
                        (state, children)
                    },
                    |(mut state, children)| {
                        let mut effects = Vec::with_capacity(size.saturating_add(1));
                        supervisor::step(
                            &mut state,
                            Event::BeginDrain {
                                reason: StopReason::ShutdownRequested,
                            },
                            &mut effects,
                        );
                        match flavor {
                            ScopeFlavor::Ordered => terminate_all(
                                &mut state,
                                children.iter().rev().copied(),
                                &mut effects,
                            ),
                            ScopeFlavor::Dynamic => {
                                terminate_all(&mut state, children.iter().copied(), &mut effects)
                            }
                        }
                        black_box((state, effects))
                    },
                    BatchSize::LargeInput,
                );
            });

            group.bench_with_input(BenchmarkId::new("force", size), &size, |b, _| {
                b.iter_batched(
                    || {
                        let (mut state, children) =
                            populated_state(flavor, &members, ScopeLifecycle::starting());
                        start_all(&mut state, &children, &mut Vec::new());
                        state
                    },
                    |mut state| {
                        let mut effects = Vec::with_capacity(size.saturating_mul(2));
                        black_box(supervisor::force(&mut state, &mut effects));
                        black_box((state, effects))
                    },
                    BatchSize::LargeInput,
                );
            });
        }
        group.finish();
    }
}

fn deadline_fixture(size: usize) -> (DeadlineQueue<usize>, Vec<DeadlineHandle>, Instant) {
    let now = Instant::now();
    let mut queue = DeadlineQueue::default();
    let handles = (0..size)
        .map(|index| {
            let at = now + std::time::Duration::from_nanos((index % 31) as u64);
            queue.push(at, index)
        })
        .collect();
    (queue, handles, now)
}

fn bench_deadline_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("core/deadline");

    for size in DEADLINE_SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("push_pop", size), &size, |b, &size| {
            b.iter_batched(
                || (),
                |()| {
                    let (mut queue, _, now) = deadline_fixture(size);
                    let due = now + std::time::Duration::from_secs(1);
                    for _ in 0..size {
                        black_box(queue.pop_due(due).expect("every deadline is due"));
                    }
                    black_box(queue)
                },
                BatchSize::SmallInput,
            );
        });

        for cancelled_percent in [0_usize, 50, 90] {
            group.bench_with_input(
                BenchmarkId::new(format!("cancel_{cancelled_percent}_percent"), size),
                &size,
                |b, &size| {
                    b.iter_batched(
                        || deadline_fixture(size),
                        |(mut queue, handles, now)| {
                            let cancelled = size.saturating_mul(cancelled_percent) / 100;
                            for handle in handles.into_iter().take(cancelled) {
                                black_box(queue.cancel(handle));
                            }
                            let due = now + std::time::Duration::from_secs(1);
                            while let Some(key) = queue.pop_due(due) {
                                black_box(key);
                            }
                            black_box(queue)
                        },
                        BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_supervisor, bench_deadline_queue);
criterion_main!(benches);
