#![cfg(feature = "host")]

use std::{
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use shelterwood::{
    Actor, Context, Exit, ExitError, ExitKind, ExitResult, HostError, HostOptions, Hosted,
    HostedHandle, HostedRaw, HostedTask, LifecycleEventKind, LifecycleItem, Mailbox, PolicyError,
    RawActor, RawContext, RawOnceDef, Readiness, ReadinessDeadline, Shutdown, StopContext, Tree,
};
use shelterwood_test_support::ReleaseGate;

fn assert_clone_eq_debug_send_sync<T: Clone + Eq + Debug + Send + Sync>() {}
fn assert_send<T: Send>() {}

#[test]
fn host_public_data_and_owner_keep_their_trait_contracts() {
    assert_clone_eq_debug_send_sync::<HostOptions>();
    assert_clone_eq_debug_send_sync::<HostError>();
    assert_send::<HostedHandle>();
}

struct StopOnMessage;

impl Actor for StopOnMessage {
    type Msg = ();
    type Args = ();

    async fn init(_: (), _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self)
    }

    async fn handle(&mut self, (): (), context: &mut Context<'_, Self>) -> ExitResult {
        context.stop();
        Ok(())
    }

    async fn on_stop(&mut self, _: &mut StopContext<'_, Self>) {}
}

#[tokio::test]
async fn hosted_actor_uses_real_membership_readiness_and_exit_classification() {
    let options = HostOptions::new()
        .mailbox(Mailbox::queue(4).expect("non-zero mailbox"))
        .readiness_deadline(
            ReadinessDeadline::bounded(Duration::from_secs(1)).expect("non-zero deadline"),
        )
        .shutdown(Shutdown::Graceful {
            grace: Duration::from_millis(50),
        });
    let (actor, handle) = Hosted::<StopOnMessage>::spawn((), options).expect("host starts");
    assert_eq!(actor.id().as_str(), "hosted");
    handle
        .wait_ready()
        .await
        .expect("after-init readiness passes");
    actor.send(()).await.expect("hosted mailbox accepts work");
    let exit = handle.wait().await;
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert!(
        exit.cancelled(),
        "a local actor stop fires its cancellation token"
    );
}

struct FailsInit;

impl Actor for FailsInit {
    type Msg = ();
    type Args = ();

    async fn init(_: (), _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Err(ExitError::message("hosted init failed"))
    }

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult {
        unreachable!("failed initialization never handles messages")
    }

    async fn on_stop(&mut self, _: &mut StopContext<'_, Self>) {}
}

#[tokio::test]
async fn hosted_wait_joins_and_returns_an_initialization_failure() {
    let (_, handle) = Hosted::<FailsInit>::spawn((), HostOptions::new()).expect("host starts");
    let ready = handle
        .wait_ready()
        .await
        .expect_err("initialization never becomes ready");
    let exit = tokio::time::timeout(Duration::from_secs(1), handle.wait())
        .await
        .expect("host wait must not inherit root startup-failure parking");
    assert_eq!(exit, ready.exit);
    assert!(matches!(exit.kind(), ExitKind::Failed(_)));
}

struct CooperativeRaw;

impl RawActor for CooperativeRaw {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        context.shutdown_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn hosted_raw_and_task_share_supervised_shutdown_and_panic_paths() {
    let (actor, raw_handle) =
        HostedRaw::<()>::spawn(CooperativeRaw, HostOptions::new()).expect("raw host starts");
    raw_handle
        .wait_ready()
        .await
        .expect("raw is immediately ready");
    let membership = actor.membership();
    let exit = raw_handle
        .shutdown(Duration::from_secs(1))
        .await
        .expect("cooperative raw host stops");
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert!(exit.cancelled());
    assert_eq!(actor.membership(), membership);

    let (task, task_handle) = HostedTask::spawn(
        |_| async move { panic!("hosted-task-panic") },
        HostOptions::new(),
    )
    .expect("task host starts");
    let membership = task.membership();
    let exit = task_handle.wait().await;
    assert_eq!(task.membership(), membership);
    assert!(matches!(
        exit.kind(),
        ExitKind::Panicked {
            message: Some(message)
        } if message.contains("hosted-task-panic")
    ));
}

#[tokio::test]
async fn hosted_manual_readiness_and_owner_drop_are_structured() {
    let stopped = Arc::new(AtomicBool::new(false));
    let (task, handle) = HostedTask::spawn(
        {
            let stopped = Arc::clone(&stopped);
            move |context| async move {
                context.mark_ready();
                context.shutdown_token().cancelled().await;
                stopped.store(true, Ordering::SeqCst);
                Ok(())
            }
        },
        HostOptions::new()
            .readiness(Readiness::Manual)
            .shutdown_grace(Duration::from_secs(1)),
    )
    .expect("manual task host starts");
    handle
        .wait_ready()
        .await
        .expect("manual readiness is observed");
    drop(handle);
    let exit = task.wait().await;
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert!(exit.cancelled());
    assert!(stopped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn host_options_reject_unresolved_and_child_incompatible_readiness() {
    let unresolved = HostedTask::spawn(
        |_| async { Ok(()) },
        HostOptions::new().readiness_deadline(ReadinessDeadline::Inherit),
    )
    .expect_err("standalone hosting has no inherited default");
    assert_eq!(unresolved, HostError::UnresolvedReadinessDeadline);

    let incompatible = HostedRaw::<()>::spawn(
        CooperativeRaw,
        HostOptions::new().readiness(Readiness::AfterInit),
    )
    .expect_err("after-init is callback-actor-only");
    assert_eq!(
        incompatible,
        HostError::InvalidPolicy(PolicyError::UnsupportedReadiness)
    );

    let task_mailbox = HostedTask::spawn(
        |_| async { Ok(()) },
        HostOptions::new().mailbox(Mailbox::latest()),
    )
    .expect_err("tasks do not have actor mailboxes");
    assert_eq!(task_mailbox, HostError::TaskMailbox);
}

#[test]
fn host_reports_missing_ambient_runtime_without_panicking() {
    let error = HostedTask::spawn(|_| async { Ok(()) }, HostOptions::new())
        .expect_err("plain test has no ambient Tokio runtime");
    assert_eq!(error, HostError::NoRuntime);
}

#[test]
fn hosted_drop_uses_the_originating_runtime_outside_its_entered_context() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime builds");
    let (task, handle) = runtime.block_on(async {
        let hosted = HostedTask::spawn(
            |context| async move {
                context.shutdown_token().cancelled().await;
                Ok(())
            },
            HostOptions::new(),
        )
        .expect("host starts");
        hosted.1.wait_ready().await.expect("task becomes ready");
        hosted
    });
    drop(handle);
    let exit = runtime.block_on(task.wait());
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert!(exit.cancelled());
}

#[derive(Clone)]
enum ParityOutcome {
    Completed,
    Failed(ExitError),
    Panicked,
}

struct ParityRaw {
    release: ReleaseGate,
    outcome: ParityOutcome,
}

impl RawActor for ParityRaw {
    type Msg = ();

    async fn run(&mut self, _: &mut RawContext<Self::Msg>) -> ExitResult {
        self.release.wait().await;
        match &self.outcome {
            ParityOutcome::Completed => Ok(()),
            ParityOutcome::Failed(error) => Err(error.clone()),
            ParityOutcome::Panicked => panic!("shared-runner-parity-panic"),
        }
    }
}

async fn supervised_exit(outcome: ParityOutcome) -> Exit {
    let release = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_raw_once(
        "parity",
        RawOnceDef::new(ParityRaw {
            release: release.clone(),
            outcome,
        }),
    )
    .expect("parity id is valid");
    let system = tree.spawn().expect("runtime is available");
    let mut events = system.scope().subscribe_lifecycle();
    release.release();
    let exit = loop {
        let item = events
            .recv()
            .await
            .expect("lifecycle remains open through exit");
        if let LifecycleItem::Event(event) = item
            && let LifecycleEventKind::Exited { id, exit, .. } = event.kind
            && id.as_str() == "parity"
        {
            break exit;
        }
    };
    let _ = system.wait().await;
    exit
}

#[tokio::test]
async fn hosted_and_supervised_runner_classify_completion_failure_and_panic_identically() {
    let failure = ExitError::message("shared-runner-parity-failure");
    for outcome in [
        ParityOutcome::Completed,
        ParityOutcome::Failed(failure),
        ParityOutcome::Panicked,
    ] {
        let supervised = supervised_exit(outcome.clone()).await;
        let release = ReleaseGate::default();
        let (_, hosted) = HostedRaw::<()>::spawn(
            ParityRaw {
                release: release.clone(),
                outcome,
            },
            HostOptions::new(),
        )
        .expect("host starts");
        release.release();
        let hosted = hosted.wait().await;
        assert_eq!(hosted, supervised);
    }
}
