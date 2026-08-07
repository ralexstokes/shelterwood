use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use shelterwood::{
    Backoff, DynamicTree, ExitError, ExitResult, Mailbox, MailboxShutdown, PolicyError, RawActor,
    RawContext, RawDef, RawOnceDef, Readiness, ReadinessDeadline, RemoveOutcome, RestartCondition,
    RestartPolicy, ScopeDefaults, SendErrorKind, TaskDef, Tree,
};
use shelterwood_test_support::{ConsumeCount, ConsumeGuard, ReleaseGate, poll_until};

fn never() -> RestartPolicy {
    RestartPolicy::new(RestartCondition::Never, Backoff::Immediate)
}

#[derive(Clone, Copy)]
enum ResourceMode {
    Normal,
    ReadinessPanic,
    StartupFailure,
}

struct ResourceActor {
    _guard: ConsumeGuard,
    mode: ResourceMode,
}

impl RawActor for ResourceActor {
    type Msg = ();

    fn readiness(&self) -> Readiness {
        match self.mode {
            ResourceMode::Normal => Readiness::Immediate,
            ResourceMode::ReadinessPanic => panic!("raw readiness panic"),
            ResourceMode::StartupFailure => Readiness::Manual,
        }
    }

    async fn run(&mut self, _context: &mut RawContext<Self::Msg>) -> ExitResult {
        match self.mode {
            ResourceMode::Normal => Ok(()),
            ResourceMode::ReadinessPanic => unreachable!("readiness prevents run"),
            ResourceMode::StartupFailure => {
                Err(ExitError::message("raw actor failed before ready"))
            }
        }
    }
}

fn resource_actor(count: &ConsumeCount, mode: ResourceMode) -> ResourceActor {
    ResourceActor {
        _guard: count.guard(),
        mode,
    }
}

struct FactoryTaskActor {
    factory_task: String,
}

impl RawActor for FactoryTaskActor {
    type Msg = ();

    async fn run(&mut self, _context: &mut RawContext<Self::Msg>) -> ExitResult {
        assert_eq!(self.factory_task, format!("{:?}", tokio::task::id()));
        Ok(())
    }
}

#[tokio::test]
async fn restartable_raw_factory_runs_inside_the_incarnation_task() {
    let mut tree = Tree::new();
    tree.add_raw(
        "factory-task",
        RawDef::factory(|| FactoryTaskActor {
            factory_task: format!("{:?}", tokio::task::id()),
        }),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
}

#[test]
fn raw_readiness_override_rejects_after_init_eagerly() {
    let count = ConsumeCount::default();
    let error = RawOnceDef::new(resource_actor(&count, ResourceMode::Normal))
        .readiness(Readiness::AfterInit)
        .expect_err("raw actors have no init phase");
    assert_eq!(error, PolicyError::UnsupportedReadiness);
}

#[tokio::test]
async fn one_shot_raw_resource_drops_once_on_normal_exit() {
    let count = ConsumeCount::default();
    let mut tree = Tree::new();
    tree.add_raw_once(
        "raw",
        RawOnceDef::new(resource_actor(&count, ResourceMode::Normal)),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    count.assert_once();
}

#[tokio::test]
async fn one_shot_raw_resource_drops_once_on_construction_panic() {
    let count = ConsumeCount::default();
    let mut tree = Tree::new();
    tree.add_raw_once(
        "raw",
        RawOnceDef::new(resource_actor(&count, ResourceMode::ReadinessPanic)),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    assert!(system.wait_started().await.is_err());
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root rolls back");
    count.assert_once();
}

#[tokio::test]
async fn one_shot_raw_resource_drops_once_on_startup_failure() {
    let count = ConsumeCount::default();
    let mut tree = Tree::new();
    tree.add_raw_once(
        "raw",
        RawOnceDef::new(resource_actor(&count, ResourceMode::StartupFailure)),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    assert!(system.wait_started().await.is_err());
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root rolls back");
    count.assert_once();
}

#[tokio::test]
async fn one_shot_raw_resource_drops_once_on_shutdown_before_start() {
    let count = ConsumeCount::default();
    let mut tree = Tree::new();
    tree.add_task(
        "gate",
        TaskDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok(())
        })
        .restart(never())
        .readiness(Readiness::Manual)
        .expect("manual readiness")
        .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid gate");
    tree.add_raw_once(
        "never-spawned",
        RawOnceDef::new(resource_actor(&count, ResourceMode::Normal)),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("shutdown completes");
    count.assert_once();
}

struct ManualActor {
    release_ready: ReleaseGate,
    entered: Arc<AtomicBool>,
    values: Arc<Mutex<Vec<usize>>>,
}

impl RawActor for ManualActor {
    type Msg = usize;

    fn readiness(&self) -> Readiness {
        Readiness::Manual
    }

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        assert_eq!(context.id(), context.myself().id());
        assert_eq!(
            context.incarnation().membership(),
            context.myself().membership()
        );
        self.entered.store(true, Ordering::SeqCst);
        self.release_ready.wait().await;
        context.mark_ready();
        while let Some(value) = context.recv().await {
            self.values
                .lock()
                .expect("manual values mutex poisoned")
                .push(value);
        }
        Ok(())
    }
}

#[tokio::test]
async fn raw_manual_readiness_gates_ordered_startup_but_not_mailbox_acceptance() {
    let release_ready = ReleaseGate::default();
    let entered = Arc::new(AtomicBool::new(false));
    let sibling_started = Arc::new(AtomicBool::new(false));
    let values = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "manual",
            RawOnceDef::new(ManualActor {
                release_ready: release_ready.clone(),
                entered: Arc::clone(&entered),
                values: Arc::clone(&values),
            })
            .readiness_deadline(ReadinessDeadline::Unbounded),
        )
        .expect("valid actor");
    let (_later, _completion) = tree
        .add_task_once(
            "later",
            shelterwood::TaskOnceDef::new({
                let sibling_started = Arc::clone(&sibling_started);
                move |_| async move {
                    sibling_started.store(true, Ordering::SeqCst);
                    Ok::<_, ExitError>(())
                }
            }),
        )
        .expect("valid sibling");
    let system = tree.spawn().expect("runtime is available");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            entered.load(Ordering::SeqCst)
        })
        .await
    );
    assert!(!sibling_started.load(Ordering::SeqCst));
    actor
        .send(7)
        .await
        .expect("readiness does not gate acceptance");
    release_ready.release();
    system.wait_started().await.expect("manual gate releases");
    assert!(sibling_started.load(Ordering::SeqCst));
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            values
                .lock()
                .expect("manual values mutex poisoned")
                .as_slice()
                == [7]
        })
        .await
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}

struct ShutdownActor {
    enter_loop: ReleaseGate,
    values: Arc<Mutex<Vec<usize>>>,
}

impl RawActor for ShutdownActor {
    type Msg = usize;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.enter_loop.wait().await;
        while let Some(value) = context.recv().await {
            self.values
                .lock()
                .expect("shutdown values mutex poisoned")
                .push(value);
        }
        if context.mailbox_shutdown() == MailboxShutdown::Drain {
            while let Some(value) = context.try_recv() {
                self.values
                    .lock()
                    .expect("shutdown values mutex poisoned")
                    .push(value);
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn raw_recv_is_shutdown_biased_and_try_recv_controls_drain_vs_discard() {
    for (shutdown, expected) in [
        (MailboxShutdown::Drain, vec![1, 2]),
        (MailboxShutdown::Discard, vec![]),
    ] {
        let enter_loop = ReleaseGate::default();
        let values = Arc::new(Mutex::new(Vec::new()));
        let mut tree = Tree::new();
        let actor = tree
            .add_raw_once(
                "shutdown",
                RawOnceDef::new(ShutdownActor {
                    enter_loop: enter_loop.clone(),
                    values: Arc::clone(&values),
                })
                .mailbox(Mailbox::queue(2).expect("non-zero capacity"))
                .mailbox_shutdown(shutdown),
            )
            .expect("valid actor");
        let system = tree.spawn().expect("runtime is available");
        system.wait_started().await.expect("actor starts");
        let accepting = actor.try_send(1).expect("one accepts");
        actor.try_send(2).expect("two accepts");
        let shutdown = tokio::spawn(async move { system.shutdown(Duration::from_secs(1)).await });
        assert!(
            poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
                matches!(
                    actor.try_send(3),
                    Err(ref error)
                        if error.kind == SendErrorKind::NotRunning
                            && error.incarnation_observed == Some(accepting)
                )
            })
            .await
        );
        enter_loop.release();
        shutdown
            .await
            .expect("shutdown task joins")
            .expect("shutdown completes");
        assert_eq!(
            *values.lock().expect("shutdown values mutex poisoned"),
            expected
        );
    }
}

struct DynamicActor {
    values: Arc<Mutex<Vec<usize>>>,
}

impl RawActor for DynamicActor {
    type Msg = usize;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        while let Some(value) = context.recv().await {
            self.values
                .lock()
                .expect("dynamic values mutex poisoned")
                .push(value);
        }
        Ok(())
    }
}

#[tokio::test]
async fn dynamic_scope_admits_uses_and_exactly_removes_a_raw_actor() {
    let values = Arc::new(Mutex::new(Vec::new()));
    let tree = DynamicTree::new();
    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("empty dynamic root starts");
    let scope = system.scope();
    let receipt = scope
        .add_raw_once(
            "runtime-raw",
            RawOnceDef::new(DynamicActor {
                values: Arc::clone(&values),
            }),
        )
        .await
        .expect("raw actor is admitted");
    let actor = receipt.into_handles();
    actor.send(9).await.expect("dynamic actor accepts");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            values
                .lock()
                .expect("dynamic values mutex poisoned")
                .as_slice()
                == [9]
        })
        .await
    );
    assert_eq!(scope.remove_actor(&actor).await, RemoveOutcome::Removed);
    let terminal = actor.send(10).await.expect_err("removed actor is terminal");
    assert_eq!(terminal.kind, SendErrorKind::Terminated);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn deferred_queue_capacity_ignores_a_latest_scope_default() {
    let gate = ReleaseGate::default();
    let values = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    tree.defaults(ScopeDefaults {
        mailbox: Some(Mailbox::latest()),
        ..ScopeDefaults::default()
    });
    let actor = tree
        .add_raw_once(
            "queue",
            RawOnceDef::new(ShutdownActor {
                enter_loop: gate.clone(),
                values: Arc::clone(&values),
            })
            .mailbox(Mailbox::queue_inherit()),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.try_send(1).expect("first queue value accepts");
    actor
        .try_send(2)
        .expect("second value proves queue capacity did not become latest");
    gate.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            values
                .lock()
                .expect("default values mutex poisoned")
                .as_slice()
                == [1, 2]
        })
        .await
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}
