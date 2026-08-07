#![cfg(feature = "serde")]

use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use shelterwood::{
    Actor, ActorOnceDef, Backoff, Context, DynamicScopeRef, DynamicTree, ExitError, ExitKind,
    ExitResult, Jitter, KeyedCapacity, Outline, OutlineInterior, Readiness, RemoveOutcome,
    ResolvedMailbox, RestartCondition, RestartPolicy, StopContext, SubtreeOnceDef, TaskDef,
    TaskOnceDef, TaskRef, Tree,
};
use shelterwood_test_support::{ReleaseGate, poll_until};

#[derive(Clone, Default)]
struct DurableFarm {
    runs: Arc<AtomicUsize>,
    completed: Arc<Mutex<Vec<(usize, usize)>>>,
    payloads_consumed: Arc<AtomicUsize>,
    wedged_retired: Arc<AtomicUsize>,
    lease_starts: Arc<AtomicUsize>,
}

struct OwnedJob {
    run: usize,
    job: usize,
    payload: Vec<u8>,
    consumed: Arc<AtomicUsize>,
}

impl OwnedJob {
    fn execute(self, durable: &DurableFarm) -> usize {
        assert_eq!(self.payload, vec![self.job as u8; self.job + 1]);
        self.consumed.fetch_add(1, Ordering::SeqCst);
        durable
            .completed
            .lock()
            .expect("durable completion mutex poisoned")
            .push((self.run, self.job));
        self.job
    }
}

#[derive(Clone)]
struct ProgressUpdate {
    job: String,
    percent: u8,
}

struct ProgressActor {
    block_once: Arc<AtomicBool>,
    entered: Arc<AtomicBool>,
    release: ReleaseGate,
    log: Arc<Mutex<Vec<ProgressUpdate>>>,
}

impl Actor for ProgressActor {
    type Msg = ProgressUpdate;
    type Args = (
        Arc<AtomicBool>,
        Arc<AtomicBool>,
        ReleaseGate,
        Arc<Mutex<Vec<ProgressUpdate>>>,
    );

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            block_once: args.0,
            entered: args.1,
            release: args.2,
            log: args.3,
        })
    }

    async fn handle(&mut self, update: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        if self.block_once.swap(false, Ordering::SeqCst) {
            self.entered.store(true, Ordering::SeqCst);
            self.release.wait().await;
        }
        self.log
            .lock()
            .expect("progress log mutex poisoned")
            .push(update);
        Ok(())
    }

    async fn on_stop(&mut self, _: &mut StopContext<'_, Self>) {}
}

struct FarmCycle {
    tree: Tree,
    outline: Outline,
    workers: DynamicScopeRef,
    progress: shelterwood::ActorRef<ProgressUpdate>,
    progress_entered: Arc<AtomicBool>,
    progress_release: ReleaseGate,
    progress_log: Arc<Mutex<Vec<ProgressUpdate>>>,
    scheduler: TaskRef,
}

fn build_cycle(durable: DurableFarm) -> FarmCycle {
    let mut tree = Tree::new();

    let fail_first_lease = Arc::new(AtomicBool::new(true));
    tree.add_task(
        "lease",
        TaskDef::new({
            let durable = durable.clone();
            let fail_first_lease = Arc::clone(&fail_first_lease);
            move |context| {
                let durable = durable.clone();
                let fail_first_lease = Arc::clone(&fail_first_lease);
                async move {
                    durable.lease_starts.fetch_add(1, Ordering::SeqCst);
                    context.mark_ready();
                    if fail_first_lease.swap(false, Ordering::SeqCst) {
                        return Err(ExitError::message("lease acquisition retry"));
                    }
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        })
        .restart(RestartPolicy::new(
            RestartCondition::OnFailure,
            Backoff::fixed(Duration::from_millis(1), Jitter::None).expect("non-zero lease backoff"),
        ))
        .readiness(Readiness::Manual)
        .expect("manual task readiness is valid"),
    )
    .expect("lease id is valid");

    let workers = tree
        .add_subtree_once("workers", SubtreeOnceDef::new(DynamicTree::new()))
        .expect("dynamic worker scope is declared");

    let progress_entered = Arc::new(AtomicBool::new(false));
    let progress_release = ReleaseGate::default();
    let progress_log = Arc::new(Mutex::new(Vec::new()));
    let progress = tree
        .add_actor_once(
            "progress",
            ActorOnceDef::<ProgressActor>::new((
                Arc::new(AtomicBool::new(true)),
                Arc::clone(&progress_entered),
                progress_release.clone(),
                Arc::clone(&progress_log),
            ))
            .latest_by_key(
                KeyedCapacity::Explicit(NonZeroUsize::new(3).expect("three concurrent job keys")),
                |update| update.job.clone(),
            ),
        )
        .expect("progress id is valid");

    let (scheduler, _) = tree
        .add_task_once(
            "scheduler",
            TaskOnceDef::new({
                let durable = durable.clone();
                let workers = workers.clone();
                let progress = progress.clone();
                move |_| async move {
                    let run = durable.runs.fetch_add(1, Ordering::SeqCst);
                    let mut claims = Vec::new();
                    for job in 0..3 {
                        let owned = OwnedJob {
                            run,
                            job,
                            payload: vec![job as u8; job + 1],
                            consumed: Arc::clone(&durable.payloads_consumed),
                        };
                        let job_id = format!("run-{run}-job-{job}");
                        let progress = progress.clone();
                        let durable_for_job = durable.clone();
                        let (task, claim) = workers
                            .add_task_once(
                                job_id,
                                TaskOnceDef::new(move |_| async move {
                                    let key = format!("run-{run}-job-{job}");
                                    for percent in [0, 50, 100] {
                                        progress
                                            .send(ProgressUpdate {
                                                job: key.clone(),
                                                percent,
                                            })
                                            .await
                                            .map_err(|_| {
                                                ExitError::message(
                                                    "progress mailbox terminated during job",
                                                )
                                            })?;
                                    }
                                    Ok(owned.execute(&durable_for_job))
                                }),
                            )
                            .await
                            .map_err(|error| {
                                ExitError::message(format!("worker admission failed: {error}"))
                            })?
                            .into_handles();
                        claims.push((task, claim));
                    }

                    let wedged_started = ReleaseGate::default();
                    let (wedged, wedged_claim) = workers
                        .add_task_once(
                            format!("run-{run}-wedged"),
                            TaskOnceDef::new({
                                let wedged_started = wedged_started.clone();
                                move |context| async move {
                                    wedged_started.release();
                                    context.shutdown_token().cancelled().await;
                                    Ok::<_, ExitError>(())
                                }
                            }),
                        )
                        .await
                        .map_err(|error| {
                            ExitError::message(format!("wedged admission failed: {error}"))
                        })?
                        .into_handles();
                    wedged_started.wait().await;
                    assert_eq!(workers.remove_task(&wedged).await, RemoveOutcome::Removed);
                    let _ = wedged_claim.wait().await;
                    durable.wedged_retired.fetch_add(1, Ordering::SeqCst);

                    for (task, claim) in claims {
                        let expected = task
                            .id()
                            .as_str()
                            .rsplit('-')
                            .next()
                            .expect("job id has a numeric suffix")
                            .parse::<usize>()
                            .expect("job suffix is numeric");
                        assert_eq!(
                            claim
                                .wait()
                                .await
                                .map_err(|exit| ExitError::message(format!(
                                    "one-shot worker {} failed: {exit:?}",
                                    task.id()
                                )))?,
                            expected
                        );
                    }
                    Ok::<_, ExitError>(())
                }
            }),
        )
        .expect("scheduler id is valid");

    let outline = tree.outline().expect("farm declaration is complete");
    FarmCycle {
        tree,
        outline,
        workers,
        progress,
        progress_entered,
        progress_release,
        progress_log,
        scheduler,
    }
}

async fn run_cycle(durable: DurableFarm) -> Outline {
    let cycle = build_cycle(durable.clone());
    assert_eq!(
        cycle
            .outline
            .root
            .children
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>(),
        ["lease", "workers", "progress", "scheduler"]
    );
    let workers_outline = &cycle.outline.root.children[1];
    assert!(matches!(
        workers_outline.interior,
        Some(OutlineInterior::Recursive(ref scope))
            if scope.kind == shelterwood::ScopeKind::Dynamic
    ));
    assert_eq!(
        cycle.outline.root.children[2].mailbox,
        Some(ResolvedMailbox::LatestByKey {
            capacity: NonZeroUsize::new(3).expect("non-zero capacity"),
        })
    );

    let outline = cycle.outline.clone();
    let system = cycle.tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("farm starts");
    let runner =
        tokio::spawn(system.run_until_all([cycle.scheduler.clone()], Duration::from_secs(1)));

    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            cycle.progress_entered.load(Ordering::SeqCst)
                && cycle.progress.stats().stats.messages_accepted == 9
                && cycle.progress.stats().stats.messages_conflated >= 5
        })
        .await,
        "latest-by-job progress must conflate while its handler is wedged"
    );
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            cycle.workers.snapshot().children.is_empty()
        })
        .await,
        "one-shot workers auto-remove and the wedged worker retires exactly"
    );
    cycle.progress_release.release();
    let result = runner.await.expect("run-until runner joins");
    assert!(result.shutdown.is_ok());
    assert_eq!(result.tasks.len(), 1);
    assert_eq!(result.tasks[0].membership, cycle.scheduler.membership());
    assert!(matches!(result.tasks[0].exit.kind(), ExitKind::Completed));

    let mut latest = BTreeMap::new();
    for update in cycle
        .progress_log
        .lock()
        .expect("progress log mutex poisoned")
        .iter()
    {
        latest.insert(update.job.clone(), update.percent);
    }
    assert_eq!(latest.len(), 3);
    assert!(latest.values().all(|percent| *percent == 100));
    outline
}

#[tokio::test]
async fn build_farm_runs_a_finite_batch_then_warm_rebuilds_from_durable_state() {
    let durable = DurableFarm::default();
    let first = run_cycle(durable.clone()).await;
    let second = run_cycle(durable.clone()).await;

    assert_eq!(
        first, second,
        "fresh declarations have stable resolved outlines"
    );
    assert_eq!(durable.runs.load(Ordering::SeqCst), 2);
    assert_eq!(durable.payloads_consumed.load(Ordering::SeqCst), 6);
    assert_eq!(durable.wedged_retired.load(Ordering::SeqCst), 2);
    assert!(durable.lease_starts.load(Ordering::SeqCst) >= 4);
    let mut completed = durable
        .completed
        .lock()
        .expect("durable completion mutex poisoned")
        .clone();
    completed.sort_unstable();
    assert_eq!(completed, [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2)]);
}
