//! Public mailbox operation benchmarks.
//!
//! # Timing discipline
//!
//! `Mailbox::Queue` is FIFO (`shelterwood_core::policy::Mailbox::Queue`), so a
//! reply barrier enqueued after a batch cannot be answered until the handler
//! has run every message ahead of it. A barrier *inside* a timed region would
//! therefore report `enqueue + handler + wakeups + a full round trip` under
//! the name of a send, and the drain would dominate: the named operation is
//! the smaller half of that sum, and two different send APIs become
//! indistinguishable because they share the same drain.
//!
//! Every batched send case below is written with `iter_custom` so that the
//! clock stops at the last accepted send and the barrier runs outside the
//! measurement. The barrier still runs once per iteration -- it is what
//! guarantees an empty queue at the next iteration's start, which the
//! `try_send` occupancy invariant depends on.

use std::{
    future::Future,
    hint::black_box,
    time::{Duration, Instant},
};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use shelterwood::{
    Actor, ActorDef, ActorRef, Context, ExitError, ExitResult, Mailbox, Reply, System, Tree,
};
use tokio::runtime::{Builder, Runtime};

const BATCH: usize = 256;
const DEADLINE: Duration = Duration::from_secs(30);

/// Sampling budget for the mailbox suite.
///
/// The suite is 48 benchmarks (3 payload sizes x 2 runtimes x 8 cases). At
/// Criterion's defaults that is roughly seven minutes on top of the `core`
/// suite, which is too long for `just bench` to stay an ordinary entry point.
/// These settings keep the whole suite near three minutes; the suite is a
/// coarse regression guardrail, not a publishable baseline.
const SAMPLE_SIZE: usize = 20;
const WARM_UP: Duration = Duration::from_millis(500);
const MEASUREMENT: Duration = Duration::from_secs(2);

/// Change-detection thresholds.
///
/// The backpressured arms are inherently noisy: their timed region contains
/// consumer scheduling, so run-to-run spreads of tens of percent are normal
/// on a shared machine. Criterion's defaults (`significance_level` 0.05,
/// `noise_threshold` 0.01) would report most of that spread as a regression.
/// A tighter significance level and a 10% noise floor keep the reports
/// meaningful.
const SIGNIFICANCE_LEVEL: f64 = 0.01;
const NOISE_THRESHOLD: f64 = 0.10;

enum Message<const N: usize> {
    Data([u8; N]),
    Request([u8; N], Reply<u8>),
    Barrier(Reply<u64>),
}

struct Sink<const N: usize> {
    processed: u64,
}

impl<const N: usize> Actor for Sink<N> {
    type Msg = Message<N>;
    type Args = ();

    async fn init(_args: (), _context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { processed: 0 })
    }

    async fn handle(
        &mut self,
        message: Message<N>,
        _context: &mut Context<'_, Self>,
    ) -> ExitResult {
        match message {
            Message::Data(payload) => {
                black_box(payload);
                self.processed = self.processed.wrapping_add(1);
            }
            Message::Request(payload, reply) => {
                let value = payload.first().copied().unwrap_or_default();
                black_box(payload);
                self.processed = self.processed.wrapping_add(1);
                reply.send(value);
            }
            Message::Barrier(reply) => reply.send(self.processed),
        }
        Ok(())
    }
}

fn payloads<const N: usize>() -> Vec<[u8; N]> {
    (0..BATCH).map(|index| [index as u8; N]).collect()
}

fn partitioned_payloads<const N: usize>(producers: usize) -> Vec<Vec<[u8; N]>> {
    let mut batches: Vec<_> = (0..producers).map(|_| Vec::new()).collect();
    for (index, payload) in payloads().into_iter().enumerate() {
        batches[index % producers].push(payload);
    }
    batches
}

fn start<const N: usize>(runtime: &Runtime, capacity: usize) -> (System, ActorRef<Message<N>>) {
    runtime.block_on(async {
        let mut tree = Tree::new();
        let actor = tree
            .add_actor(
                "sink",
                ActorDef::<Sink<N>>::cloned(())
                    .mailbox(Mailbox::queue(capacity).expect("benchmark capacities are non-zero")),
            )
            .expect("the benchmark tree is valid");
        let system = tree.spawn().expect("the benchmark runtime is active");
        system
            .wait_started()
            .await
            .expect("the benchmark actor starts");
        (system, actor)
    })
}

fn stop(runtime: &Runtime, system: System) {
    runtime.block_on(async {
        system
            .shutdown(DEADLINE)
            .await
            .expect("the benchmark actor shuts down within its generous bound");
    });
}

/// Drains the mailbox and returns the handler's running count.
///
/// The barrier is a real `call`, so it costs a boxed constructor, a reply
/// channel and one armed-and-retired deadline timer. It is never inside a
/// timed region: it runs only between measured batches.
async fn barrier<const N: usize>(actor: &ActorRef<Message<N>>) -> u64 {
    actor
        .call(Message::Barrier, DEADLINE)
        .await
        .expect("the benchmark barrier receives a reply")
        .value
}

/// Runs `iters` measured batches and returns only the batches' own time.
///
/// Each iteration builds its input, times `run`, and then drains the mailbox
/// with an untimed barrier. The drain is what makes the *next* iteration
/// start from an empty queue, so it cannot be hoisted out of the loop even
/// though it is not measured.
async fn timed_batches<const N: usize, I, S, R, F>(
    iters: u64,
    actor: &ActorRef<Message<N>>,
    mut setup: S,
    mut run: R,
) -> Duration
where
    S: FnMut() -> I,
    R: FnMut(I) -> F,
    F: Future<Output = ()>,
{
    let mut elapsed = Duration::ZERO;
    for _ in 0..iters {
        let input = setup();
        let started = Instant::now();
        run(input).await;
        elapsed += started.elapsed();
        black_box(barrier(actor).await);
    }
    elapsed
}

fn bench_payload<const N: usize>(c: &mut Criterion, runtime_name: &str, runtime: &Runtime) {
    let mut group = c.benchmark_group(format!("mailbox/{runtime_name}/payload_{N}"));
    group.throughput(Throughput::Elements(BATCH as u64));
    group.sample_size(SAMPLE_SIZE);
    group.warm_up_time(WARM_UP);
    group.measurement_time(MEASUREMENT);
    group.significance_level(SIGNIFICANCE_LEVEL);
    group.noise_threshold(NOISE_THRESHOLD);

    // Capacity is not a tuning knob over one operation: it selects which
    // operation is measured. Below `BATCH` the batch cannot fit, so sends
    // past the capacity park until the consumer dequeues and are woken by it
    // -- the timed region then legitimately contains consumer scheduling. At
    // or above `BATCH` no send ever parks and the timed region is pure
    // enqueue. The two regimes are named apart so a reader never compares
    // them as one curve.
    for capacity in [1, 64, 1_024] {
        let name = if capacity >= BATCH {
            "send_buffered"
        } else {
            "send_backpressured"
        };
        let (system, actor) = start::<N>(runtime, capacity);
        group.bench_with_input(BenchmarkId::new(name, capacity), &capacity, |b, _| {
            b.to_async(runtime).iter_custom(|iters| {
                let actor = &actor;
                timed_batches(iters, actor, payloads::<N>, move |messages| async move {
                    for payload in messages {
                        actor
                            .send(Message::Data(payload))
                            .await
                            .expect("the live benchmark actor accepts every message");
                    }
                })
            });
        });
        stop(runtime, system);
    }

    // `try_send` never returns `Full` here, and the headroom is exactly one
    // slot. The chain: occupancy is the queue length checked at accept time
    // (`shelterwood::mailbox::cell::accept_locked`), a slot is freed when the
    // driver *dequeues* rather than when the handler finishes
    // (`promote_waiter_queue` recomputes from the queue length), and
    // `timed_batches`' barrier guarantees an empty queue at the start of
    // every iteration. Peak occupancy is therefore `BATCH`, one below
    // `try_capacity`. A restructure that stops draining between iterations
    // invalidates this and must re-derive it.
    let try_capacity = BATCH + 1;
    let (system, actor) = start::<N>(runtime, try_capacity);
    group.bench_function("try_send", |b| {
        b.to_async(runtime).iter_custom(|iters| {
            let actor = &actor;
            timed_batches(iters, actor, payloads::<N>, move |messages| async move {
                for payload in messages {
                    black_box(
                        actor
                            .try_send(Message::Data(payload))
                            .expect("the benchmark batch fits without backpressure"),
                    );
                }
            })
        });
    });
    stop(runtime, system);

    // `call` is measured whole, and the timed region necessarily includes
    // everything the signature implies: the message value is built inside it
    // (only the payload buffers are pre-built), `ActorRef::call` boxes the
    // constructor, a reply channel is created, and a real wheel entry is
    // armed and retired per call because the deadline is non-zero. For
    // `payload_4096` the boxing alone is a 4 KiB allocation and memcpy per
    // call. That is the cost of the operation, not harness overhead, but it
    // is why `call` is not comparable to a `send` figure element for element.
    let (system, actor) = start::<N>(runtime, 64);
    group.bench_function("call", |b| {
        b.to_async(runtime).iter_batched(
            payloads::<N>,
            |messages| {
                let actor = &actor;
                async move {
                    let mut checksum = 0_u64;
                    for payload in messages {
                        let replied = actor
                            .call(move |reply| Message::Request(payload, reply), DEADLINE)
                            .await
                            .expect("the benchmark request receives a reply");
                        checksum = checksum.wrapping_add(u64::from(replied.value));
                    }
                    black_box(checksum)
                }
            },
            BatchSize::PerIteration,
        );
    });
    stop(runtime, system);

    // Capacity 64 is below `BATCH`, so these arms are backpressured too: the
    // timed region is spawn, send-with-parking, and join.
    for producers in [1, 4, 16] {
        let (system, actor) = start::<N>(runtime, 64);
        group.bench_with_input(
            BenchmarkId::new("concurrent_send_tasks", producers),
            &producers,
            |b, &producers| {
                b.to_async(runtime).iter_custom(|iters| {
                    let actor = &actor;
                    timed_batches(
                        iters,
                        actor,
                        || partitioned_payloads::<N>(producers),
                        move |batches: Vec<Vec<[u8; N]>>| async move {
                            let mut tasks = Vec::with_capacity(producers);
                            for messages in batches {
                                let actor = actor.clone();
                                tasks.push(tokio::spawn(async move {
                                    for payload in messages {
                                        actor
                                            .send(Message::Data(payload))
                                            .await
                                            .expect("the live actor accepts every producer batch");
                                    }
                                }));
                            }
                            for task in tasks {
                                task.await.expect("a benchmark producer does not panic");
                            }
                        },
                    )
                });
            },
        );
        stop(runtime, system);
    }

    group.finish();
}

fn run_runtime(c: &mut Criterion, name: &str, runtime: Runtime) {
    bench_payload::<0>(c, name, &runtime);
    bench_payload::<64>(c, name, &runtime);
    bench_payload::<4_096>(c, name, &runtime);
}

fn bench_mailbox(c: &mut Criterion) {
    let current_thread = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the current-thread benchmark runtime builds");
    run_runtime(c, "current_thread", current_thread);

    let multi_thread = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("the multi-thread benchmark runtime builds");
    run_runtime(c, "multi_thread_4", multi_thread);
}

criterion_group!(benches, bench_mailbox);
criterion_main!(benches);
