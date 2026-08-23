use std::{hint::black_box, time::Duration};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use shelterwood::{
    Actor, ActorDef, ActorRef, Context, ExitError, ExitResult, Mailbox, Reply, System, Tree,
};
use tokio::runtime::{Builder, Runtime};

const BATCH: usize = 256;
const DEADLINE: Duration = Duration::from_secs(30);

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

async fn barrier<const N: usize>(actor: &ActorRef<Message<N>>) -> u64 {
    actor
        .call(Message::Barrier, DEADLINE)
        .await
        .expect("the benchmark barrier receives a reply")
        .value
}

fn bench_payload<const N: usize>(c: &mut Criterion, runtime_name: &str, runtime: &Runtime) {
    let mut group = c.benchmark_group(format!("mailbox/{runtime_name}/payload_{N}"));
    group.throughput(Throughput::Elements(BATCH as u64));

    for capacity in [1, 64, 1_024] {
        let (system, actor) = start::<N>(runtime, capacity);
        group.bench_with_input(BenchmarkId::new("send", capacity), &capacity, |b, _| {
            b.to_async(runtime).iter_batched(
                payloads::<N>,
                |messages| {
                    let actor = &actor;
                    async move {
                        for payload in messages {
                            actor
                                .send(Message::Data(payload))
                                .await
                                .expect("the live benchmark actor accepts every message");
                        }
                        black_box(barrier(actor).await)
                    }
                },
                BatchSize::PerIteration,
            );
        });
        stop(runtime, system);
    }

    let try_capacity = BATCH + 1;
    let (system, actor) = start::<N>(runtime, try_capacity);
    group.bench_function("try_send", |b| {
        b.to_async(runtime).iter_batched(
            payloads::<N>,
            |messages| {
                let actor = &actor;
                async move {
                    for payload in messages {
                        black_box(
                            actor
                                .try_send(Message::Data(payload))
                                .expect("the benchmark batch fits without backpressure"),
                        );
                    }
                    black_box(barrier(actor).await)
                }
            },
            BatchSize::PerIteration,
        );
    });
    stop(runtime, system);

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

    for producers in [1, 4, 16] {
        let (system, actor) = start::<N>(runtime, 64);
        group.bench_with_input(
            BenchmarkId::new("concurrent_send_tasks", producers),
            &producers,
            |b, &producers| {
                b.to_async(runtime).iter_batched(
                    || partitioned_payloads::<N>(producers),
                    |batches| {
                        let actor = &actor;
                        async move {
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
                            black_box(barrier(actor).await)
                        }
                    },
                    BatchSize::PerIteration,
                );
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
