mod common;

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{
    ReleaseGate, assert_eventually, assert_quiet, last_panic_message, waiting::task as waiting_task,
};
use shelterwood::{
    DynamicTree, ExitError, ExitResult, Mailbox, MailboxShutdown, PolicyError, RawActor,
    RawContext, RawDef, RawOnceDef, Readiness, ReadinessDeadline, RemoveOutcome, ScopeDefaults,
    SendErrorKind, Shutdown, SubtreeOnceDef, Tree,
};

struct FactoryTaskActor {
    factory_task: tokio::task::Id,
    observations: Arc<Mutex<Vec<(tokio::task::Id, tokio::task::Id)>>>,
}

struct InertRaw;

impl RawActor for InertRaw {
    type Msg = ();

    async fn run(&mut self, _context: &mut RawContext<Self::Msg>) -> ExitResult {
        Ok(())
    }
}

impl RawActor for FactoryTaskActor {
    type Msg = ();

    async fn run(&mut self, _context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.observations
            .lock()
            .expect("task observation mutex poisoned")
            .push((self.factory_task, tokio::task::id()));
        Ok(())
    }
}

#[tokio::test]
async fn restartable_raw_factory_runs_inside_the_incarnation_task() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    tree.add_raw(
        "factory-task",
        RawDef::factory({
            let observations = Arc::clone(&observations);
            move || FactoryTaskActor {
                factory_task: tokio::task::id(),
                observations: Arc::clone(&observations),
            }
        }),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    let observed = observations
        .lock()
        .expect("task observation mutex poisoned");
    assert_eq!(observed.len(), 1, "the one incarnation publishes one pair");
    assert_eq!(
        observed[0].0, observed[0].1,
        "the factory and raw run execute in the same Tokio task"
    );
}

#[test]
fn raw_readiness_override_rejects_after_init_eagerly() {
    let error = RawOnceDef::new(InertRaw)
        .readiness(Readiness::AfterInit)
        .expect_err("raw actors have no init phase");
    assert_eq!(error, PolicyError::UnsupportedReadiness);
}

static STATEFUL_READINESS_CALLS: AtomicUsize = AtomicUsize::new(0);

struct StatefulReadiness;

impl RawActor for StatefulReadiness {
    type Msg = ();

    fn readiness() -> Readiness {
        STATEFUL_READINESS_CALLS.fetch_add(1, Ordering::SeqCst);
        Readiness::Immediate
    }

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        assert_eq!(context.readiness(), Readiness::Immediate);
        Ok(())
    }
}

#[tokio::test]
async fn raw_readiness_is_definition_metadata_evaluated_once() {
    STATEFUL_READINESS_CALLS.store(0, Ordering::SeqCst);
    let mut tree = Tree::new();
    tree.add_raw_once("stateful-readiness", RawOnceDef::new(StatefulReadiness))
        .expect("valid actor");
    assert_eq!(STATEFUL_READINESS_CALLS.load(Ordering::SeqCst), 1);

    let system = tree.spawn().expect("runtime is available");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(STATEFUL_READINESS_CALLS.load(Ordering::SeqCst), 1);
}

static RESTARTING_READINESS_CALLS: AtomicUsize = AtomicUsize::new(0);

struct RestartingReadiness {
    generation: usize,
}

impl RawActor for RestartingReadiness {
    type Msg = ();

    fn readiness() -> Readiness {
        RESTARTING_READINESS_CALLS.fetch_add(1, Ordering::SeqCst);
        Readiness::Immediate
    }

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        assert_eq!(context.readiness(), Readiness::Immediate);
        if self.generation == 0 {
            Err(ExitError::message("restart once"))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn restartable_raw_readiness_is_resolved_once_for_the_definition() {
    RESTARTING_READINESS_CALLS.store(0, Ordering::SeqCst);
    let generations = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    tree.add_raw(
        "restartable-readiness",
        RawDef::factory({
            let generations = Arc::clone(&generations);
            move || RestartingReadiness {
                generation: generations.fetch_add(1, Ordering::SeqCst),
            }
        }),
    )
    .expect("valid actor");

    let system = tree.spawn().expect("runtime is available");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(generations.load(Ordering::SeqCst), 2);
    assert_eq!(RESTARTING_READINESS_CALLS.load(Ordering::SeqCst), 1);
}

static OVERRIDE_READINESS_CALLS: AtomicUsize = AtomicUsize::new(0);

struct OverrideReadiness;

impl RawActor for OverrideReadiness {
    type Msg = ();

    fn readiness() -> Readiness {
        OVERRIDE_READINESS_CALLS.fetch_add(1, Ordering::SeqCst);
        panic!("an effective definition override must bypass actor readiness")
    }

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        assert_eq!(context.readiness(), Readiness::Manual);
        context.mark_ready();
        context.shutdown_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn raw_readiness_override_does_not_evaluate_actor_readiness() {
    OVERRIDE_READINESS_CALLS.store(0, Ordering::SeqCst);
    let mut tree = Tree::new();
    tree.add_raw_once(
        "overridden-readiness",
        RawOnceDef::new(OverrideReadiness)
            .readiness(Readiness::Manual)
            .expect("manual readiness override"),
    )
    .expect("valid actor");

    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor becomes ready");
    assert_eq!(OVERRIDE_READINESS_CALLS.load(Ordering::SeqCst), 0);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops cooperatively");
}

struct ManualActor {
    release_ready: ReleaseGate,
    entered: Arc<AtomicBool>,
    values: Arc<Mutex<Vec<usize>>>,
}

impl RawActor for ManualActor {
    type Msg = usize;

    fn readiness() -> Readiness {
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

#[tokio::test(start_paused = true)]
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
    assert_eventually!(|| entered.load(Ordering::SeqCst)).await;
    assert_quiet(Duration::from_millis(20), || {
        sibling_started.load(Ordering::SeqCst)
    })
    .await;
    actor
        .send(7)
        .await
        .expect("readiness does not gate acceptance");
    release_ready.release();
    system.wait_started().await.expect("manual gate releases");
    assert!(sibling_started.load(Ordering::SeqCst));
    assert_eventually!(|| {
        values
            .lock()
            .expect("manual values mutex poisoned")
            .as_slice()
            == [7]
    })
    .await;
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
        assert_eventually!(|| {
            matches!(
                actor.try_send(3),
                Err(ref error)
                    if error.kind == SendErrorKind::NotRunning
                        && error.incarnation_observed == Some(accepting)
            )
        })
        .await;
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

struct LiveTryRecvActor {
    entered: ReleaseGate,
    release: ReleaseGate,
    observed: Arc<Mutex<Option<usize>>>,
}

impl RawActor for LiveTryRecvActor {
    type Msg = usize;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.entered.release();
        self.release.wait().await;
        *self.observed.lock().expect("observation mutex poisoned") = context.try_recv();
        Ok(())
    }
}

#[tokio::test]
async fn try_recv_delivers_mailbox_input_while_the_incarnation_is_live() {
    let entered = ReleaseGate::default();
    let release = ReleaseGate::default();
    let observed = Arc::new(Mutex::new(None));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "live-try-recv",
            RawOnceDef::new(LiveTryRecvActor {
                entered: entered.clone(),
                release: release.clone(),
                observed: Arc::clone(&observed),
            }),
        )
        .expect("valid raw actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("raw actor starts");
    entered.wait().await;
    actor.try_send(17).expect("live mailbox accepts");
    release.release();

    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(
        *observed.lock().expect("observation mutex poisoned"),
        Some(17)
    );
}

struct TryRecvPanicActor {
    panic_queued: Arc<AtomicBool>,
}

impl RawActor for TryRecvPanicActor {
    type Msg = ();

    fn readiness() -> Readiness {
        Readiness::Manual
    }

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        context.mark_ready();
        let guard = context
            .offload_scoped(
                async { panic!("try_recv retained offload panic") },
                |_| (),
                Duration::MAX,
            )
            .expect("offload accepted");
        guard.finished().await;
        self.panic_queued.store(true, Ordering::SeqCst);
        let _ = context.try_recv();
        unreachable!("live try_recv must resume the retained panic")
    }
}

#[tokio::test]
async fn live_try_recv_resumes_a_retained_offload_panic() {
    let panic_queued = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_raw_once(
        "try-recv-panic",
        RawOnceDef::new(TryRecvPanicActor {
            panic_queued: Arc::clone(&panic_queued),
        }),
    )
    .expect("valid raw actor");
    let system = tree.spawn().expect("runtime is available");
    let mut events = system.scope().subscribe_lifecycle();
    system.wait_started().await.expect("manual readiness fires");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);

    let panic_message = last_panic_message(&mut events, "try-recv-panic").await;
    assert_eq!(
        panic_message.as_deref(),
        Some("try_recv retained offload panic"),
        "the panic exit is observable on the lifecycle stream"
    );
    assert!(panic_queued.load(Ordering::SeqCst));
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
    let actor = scope
        .add_raw_once(
            "runtime-raw",
            RawOnceDef::new(DynamicActor {
                values: Arc::clone(&values),
            }),
        )
        .await
        .expect("raw actor is admitted");
    actor.send(9).await.expect("dynamic actor accepts");
    assert_eventually!(|| {
        values
            .lock()
            .expect("dynamic values mutex poisoned")
            .as_slice()
            == [9]
    })
    .await;
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
    assert_eventually!(|| {
        values
            .lock()
            .expect("default values mutex poisoned")
            .as_slice()
            == [1, 2]
    })
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}

struct DoublePanicActor;

impl RawActor for DoublePanicActor {
    type Msg = ();

    fn readiness() -> Readiness {
        Readiness::Manual
    }

    async fn run(&mut self, _: &mut RawContext<Self::Msg>) -> ExitResult {
        panic!("injected run panic");
    }
}

impl Drop for DoublePanicActor {
    fn drop(&mut self) {
        panic!("injected destructor panic");
    }
}

/// §8's containment boundary at the raw layer: a `run` panic is caught
/// before the actor value is destroyed, so a destructor that also panics is
/// a second contained panic — the process survives and exactly one
/// `Panicked` report publishes, carrying the run panic's payload.
#[tokio::test]
async fn raw_run_panic_with_panicking_destructor_publishes_one_report() {
    let mut tree = Tree::new();
    tree.add_raw_once("double-panic", RawOnceDef::new(DoublePanicActor))
        .expect("valid raw actor");
    let system = tree.spawn().expect("runtime is available");
    let startup = system
        .wait_started()
        .await
        .expect_err("the pre-ready panic aborts startup");
    let shelterwood::StartupError::StartupFailed(failure) = startup else {
        panic!("unexpected startup error: {startup:?}");
    };
    let shelterwood::StartupFailureCause::Child { exit, .. } = &failure.cause else {
        panic!("unexpected failure cause: {:?}", failure.cause);
    };
    let rendered = failure.to_string();
    assert!(
        rendered.contains("child `double-panic` failed during startup: panicked"),
        "startup failure identifies the child's panic: {rendered}"
    );
    assert!(
        rendered.contains("injected run panic"),
        "startup failure preserves the panic payload: {rendered}"
    );
    assert!(
        matches!(
            exit.kind(),
            shelterwood::ExitKind::Panicked { message: Some(message) }
                if message.contains("injected run panic")
        ),
        "the run panic's payload wins: {exit:?}"
    );
}

struct OffloadDoublePanicActor {
    queued: Arc<AtomicBool>,
}

impl RawActor for OffloadDoublePanicActor {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        let guard = context
            .offload_scoped(
                async {
                    panic!("injected offload panic");
                },
                |_| (),
                Duration::MAX,
            )
            .expect("offload accepted");
        guard.finished().await;
        self.queued.store(true, Ordering::SeqCst);
        std::future::pending().await
    }
}

impl Drop for OffloadDoublePanicActor {
    fn drop(&mut self) {
        panic!("injected destructor panic");
    }
}

#[tokio::test]
async fn hard_abort_offload_panic_with_panicking_raw_destructor_is_contained() {
    let queued = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_raw_once(
        "double-panic",
        RawOnceDef::new(OffloadDoublePanicActor {
            queued: Arc::clone(&queued),
        })
        .shutdown(Shutdown::Abort),
    )
    .expect("valid raw actor");
    let system = tree.spawn().expect("runtime is available");
    let mut events = system.scope().subscribe_lifecycle();
    system.wait_started().await.expect("actor starts");
    assert_eventually!(
        || queued.load(Ordering::SeqCst),
        "offload panic is queued before hard abort"
    )
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("the two panics remain contained");

    let panic_message = last_panic_message(&mut events, "double-panic").await;
    assert_eq!(panic_message.as_deref(), Some("injected offload panic"));
}

struct ScopeQuitterRaw;

impl RawActor for ScopeQuitterRaw {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        context
            .recv()
            .await
            .expect("quit trigger arrives before shutdown");
        context.request_scope_shutdown();
        assert!(
            context.recv().await.is_none(),
            "the requested scope shutdown reaches the requester's own token"
        );
        Ok(())
    }
}

/// `RawContext::request_scope_shutdown` targets the supervising scope
/// (§12): the nested scope drains as `ShutdownRequested` while the parent
/// and its other children keep running.
#[tokio::test]
async fn raw_context_scope_shutdown_request_stops_only_the_supervising_scope() {
    let mut nested = Tree::new();
    let quitter = nested
        .add_raw_once("quitter", RawOnceDef::new(ScopeQuitterRaw))
        .expect("valid raw actor");
    let mut root = Tree::new();
    root.add_task("outer-parked", waiting_task())
        .expect("valid parked task");
    let sub = root
        .add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");

    quitter.send(()).await.expect("quitter is live");
    assert_eq!(
        sub.wait_stopped().await,
        shelterwood::StopReason::ShutdownRequested
    );
    // The parent scope survives its child's requested shutdown: the parked
    // sibling is still supervised, and the root still answers its own
    // shutdown cleanly.
    assert!(matches!(
        system
            .scope()
            .snapshot()
            .child("outer-parked")
            .map(|child| child.state.clone()),
        Some(shelterwood::ChildState::Running)
    ));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root shuts down after the nested scope stopped");
}

struct StoppingRejectionRaw {
    checked: Arc<AtomicBool>,
}

impl RawActor for StoppingRejectionRaw {
    type Msg = u8;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        // Live edge cases first: an absent key has nothing to retract, an
        // armed interval is retractable, and a zero-period arm clears its
        // key outright.
        assert!(
            !context.clear_timer(&"absent"),
            "a never-armed key reports no retraction"
        );
        context
            .set_interval("interval", 7u8, Duration::from_secs(600))
            .expect("live interval arms");
        assert!(
            context.clear_timer(&"interval"),
            "an armed interval is retractable"
        );
        context
            .set_interval("cleared-by-zero", 8u8, Duration::from_secs(600))
            .expect("live interval arms");
        context
            .set_interval("cleared-by-zero", 9u8, Duration::ZERO)
            .expect("a zero period clears the key");
        assert!(
            !context.clear_timer(&"cleared-by-zero"),
            "the zero-period arm already cleared the key"
        );

        context.stop();

        // A stopping incarnation rejects new local work and returns every
        // payload whole.
        let timer = context
            .set_timeout("timeout", 1u8, Duration::from_secs(1))
            .expect_err("a stopping context rejects timers");
        assert_eq!(timer.into_inner(), ("timeout", 1u8));
        let interval = context
            .set_interval("interval", 2u8, Duration::from_secs(1))
            .expect_err("a stopping context rejects intervals");
        assert_eq!(interval.into_inner(), ("interval", 2u8));
        let offload = context
            .offload_scoped(
                async { 3u8 },
                |result| result.expect("recovered continuation sees its value"),
                Duration::from_secs(1),
            )
            .expect_err("a stopping context rejects scoped offloads");
        // Recovery is total: the caller still owns the work future and the
        // continuation, and both remain usable outside the offload path.
        let (work, continuation) = offload.into_inner();
        assert_eq!(continuation(Ok(work.await)), 3);

        self.checked.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn stopping_raw_context_rejects_timers_and_offloads_with_payload_recovery() {
    let checked = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_raw_once(
        "stopping-surface",
        RawOnceDef::new(StoppingRejectionRaw {
            checked: Arc::clone(&checked),
        }),
    )
    .expect("valid raw actor");
    let system = tree.spawn().expect("runtime is available");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert!(checked.load(Ordering::SeqCst));
}
