//! Pins SPEC Appendix A's shipped defaults behaviorally: every other test
//! sets policy explicitly, so without these a default regression passes the
//! whole suite.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{
    POLL_TIMEOUT, ReleaseGate, advance_time, assert_eventually, assert_quiet, poll_once,
};
use shelterwood::{
    Actor, ActorDef, Backoff, Context, DynamicTree, ExitError, ExitKind, ExitResult, GracePhase,
    MailboxShutdown, RawActor, RawContext, RawOnceDef, Readiness, RestartCount, Retention,
    SendErrorKind, StartupError, StopReason, TaskDef, TaskOnceDef, Tree,
};

enum CapacityMessage {
    Block,
    Fill,
}

struct CapacityActor {
    release: ReleaseGate,
    entered: ReleaseGate,
}

impl Actor for CapacityActor {
    type Msg = CapacityMessage;
    type Args = (ReleaseGate, ReleaseGate);

    async fn init(
        (entered, release): Self::Args,
        _: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        Ok(Self { release, entered })
    }

    async fn handle(&mut self, message: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        if let CapacityMessage::Block = message {
            self.entered.release();
            self.release.wait().await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn default_mailbox_is_a_queue_of_sixty_four_messages() {
    let entered = ReleaseGate::default();
    let release = ReleaseGate::default();
    let mut tree = Tree::new();
    let actor = tree
        .add_actor(
            "unconfigured",
            ActorDef::<CapacityActor>::cloned((entered.clone(), release.clone())),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    actor
        .send(CapacityMessage::Block)
        .await
        .expect("the blocking message is accepted");
    entered.wait().await;

    let mut accepted = 0usize;
    let full = loop {
        match actor.try_send(CapacityMessage::Fill) {
            Ok(_) => accepted += 1,
            Err(error) => break error,
        }
        assert!(accepted <= 128, "unbounded acceptance is not a queue");
    };
    assert_eq!(
        accepted, 64,
        "Appendix A ships a bounded queue of 64 messages"
    );
    assert_eq!(full.kind, SendErrorKind::Full);

    release.release();
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("drained actor shuts down");
}

#[tokio::test(start_paused = true)]
async fn default_child_shutdown_grace_is_five_seconds() {
    let mut tree = Tree::new();
    let task = tree
        .add_task("stubborn", TaskDef::new(|_| std::future::pending()))
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("immediate task readiness");

    let shutdown_started = tokio::time::Instant::now();
    let mut shutdown = Box::pin(system.shutdown(Duration::from_secs(60)));
    assert_quiet(Duration::from_millis(50), || {
        poll_once(shutdown.as_mut()).is_ready()
    })
    .await;
    advance_time(
        (shutdown_started + Duration::from_millis(4500))
            .saturating_duration_since(tokio::time::Instant::now()),
    )
    .await;
    // 4.5s elapsed from the captured baseline: still inside the shipped 5s grace.
    assert_quiet(Duration::from_millis(50), || {
        poll_once(shutdown.as_mut()).is_ready()
    })
    .await;
    advance_time(
        (shutdown_started + Duration::from_millis(5100))
            .saturating_duration_since(tokio::time::Instant::now()),
    )
    .await;
    assert_eventually!(
        || poll_once(shutdown.as_mut()).is_ready(),
        "grace elapses at 5s and the abort ladder completes"
    )
    .await;
    drop(shutdown);

    let exit = task.wait().await;
    assert!(
        matches!(
            exit.kind(),
            ExitKind::Aborted {
                phase: GracePhase::AfterGrace
            }
        ),
        "an uncooperative task is aborted after the default grace: {exit:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn default_readiness_deadline_is_thirty_seconds() {
    // The driver arms the deadline from the paused virtual clock, so the
    // baseline must come from the same clock: the real clock has already
    // drifted past virtual t0 by the time the test body runs.
    let before = tokio::time::Instant::now().into_std();
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "gated",
            TaskDef::new(|_| std::future::pending())
                .restart(crate::common::policy::never())
                .readiness(Readiness::Manual)
                .expect("manual readiness"),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");

    let mut started = Box::pin(system.wait_started());
    assert_quiet(Duration::from_millis(50), || {
        poll_once(started.as_mut()).is_ready()
    })
    .await;
    advance_time(Duration::from_secs(29)).await;
    // ~29.1s elapsed: still inside the shipped 30s readiness deadline.
    assert_quiet(Duration::from_millis(50), || {
        poll_once(started.as_mut()).is_ready()
    })
    .await;
    drop(started);
    advance_time(Duration::from_millis(1500)).await;

    let startup = system
        .wait_started()
        .await
        .expect_err("unsignalled manual readiness times out at the shipped 30s deadline");
    assert!(matches!(startup, StartupError::StartupFailed(_)));
    let exit = task.wait().await;
    let ExitKind::ReadinessTimedOut { deadline } = exit.kind() else {
        panic!("expected a typed readiness timeout, got {exit:?}");
    };
    assert!(*deadline >= before + Duration::from_secs(30));
    system
        .shutdown(Duration::ZERO)
        .await
        .expect("terminal child leaves no straggler");
}

#[tokio::test]
async fn default_intensity_allows_five_restarts_before_tripping() {
    let runs = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    tree.add_task(
        "failing",
        TaskDef::new({
            let runs = Arc::clone(&runs);
            move |_| {
                let runs = Arc::clone(&runs);
                async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    Err(ExitError::message("deliberate failure"))
                }
            }
        }),
    )
    .expect("valid task");
    let system = tree.spawn().expect("runtime is available");

    // Immediate task readiness means startup may complete before the failures
    // accumulate, so the trip is asserted on the scope's stop reason rather
    // than the startup result.
    let StopReason::IntensityTripped(trip) = system.wait().await else {
        panic!("repeated failure trips the shipped budget");
    };
    assert_eq!(
        trip.observed_restarts, 6,
        "the shipped budget admits 5 restarts and trips on the 6th"
    );
    assert_eq!(
        runs.load(Ordering::SeqCst),
        6,
        "initial run plus 5 restarts spawn; the tripping restart never does"
    );
}

#[tokio::test]
async fn default_restart_backoff_and_retention_follow_definition_ownership() {
    let runs = Arc::new(AtomicUsize::new(0));
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();

    let restartable = scope
        .add_task(
            "restartable",
            TaskDef::new({
                let runs = Arc::clone(&runs);
                move |context| {
                    let attempt = runs.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if attempt == 0 {
                            Err(ExitError::message("exercise the default restart"))
                        } else {
                            context.shutdown_token().cancelled().await;
                            Ok(())
                        }
                    }
                }
            }),
        )
        .await
        .expect("restartable task is admitted");
    let running = scope
        .as_scope()
        .wait_for_child(
            "restartable",
            |child| {
                child.restart_count == RestartCount::ZERO.bump()
                    && matches!(child.state, shelterwood::ChildState::Running)
            },
            POLL_TIMEOUT,
        )
        .await
        .expect("the immediate default backoff starts the replacement");
    assert_eq!(running.restart_policy.backoff(), Backoff::Immediate);
    assert_eq!(running.retention, Retention::Retain);
    assert_eq!(restartable.membership(), running.membership);

    let (one_shot, completion) = scope
        .add_task_once(
            "one-shot",
            TaskOnceDef::new(|_| async { Ok::<_, ExitError>(()) }),
        )
        .await
        .expect("one-shot task is admitted");
    completion.wait().await.expect("one-shot task completes");
    one_shot.wait().await;
    assert_eventually!(
        || scope.as_scope().child("one-shot").is_none(),
        "the default one-shot retention removes the terminal membership"
    )
    .await;

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("dynamic root stops");
}

struct DefaultDrainActor {
    entered: ReleaseGate,
    values: Arc<AtomicUsize>,
    resolved: Arc<Mutex<Option<MailboxShutdown>>>,
}

impl RawActor for DefaultDrainActor {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.entered.release();
        // A raw loop implements the frozen-prefix policy itself, so the loop
        // must both report the resolved default and obey it: parking until
        // shutdown makes the accepted prefix reachable only through the
        // policy-gated drain below, never through steady-state `recv`.
        context.shutdown_token().cancelled().await;
        let resolved = context.mailbox_shutdown();
        *self
            .resolved
            .lock()
            .expect("resolved policy mutex poisoned") = Some(resolved);
        assert!(
            context.recv().await.is_none(),
            "recv is shutdown-biased once intake is frozen"
        );
        if resolved == MailboxShutdown::Drain {
            while context.try_recv().is_some() {
                self.values.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn default_mailbox_shutdown_drains_the_frozen_prefix() {
    let entered = ReleaseGate::default();
    let values = Arc::new(AtomicUsize::new(0));
    let resolved = Arc::new(Mutex::new(None));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "drain",
            RawOnceDef::new(DefaultDrainActor {
                entered: entered.clone(),
                values: Arc::clone(&values),
                resolved: Arc::clone(&resolved),
            }),
        )
        .expect("valid raw actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("raw actor starts");
    entered.wait().await;
    actor.try_send(()).expect("first message accepts");
    actor.try_send(()).expect("second message accepts");

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("implicit drain completes");
    assert_eq!(
        *resolved.lock().expect("resolved policy mutex poisoned"),
        Some(MailboxShutdown::Drain),
        "MailboxShutdown::Drain is the shipped default"
    );
    assert_eq!(
        values.load(Ordering::SeqCst),
        2,
        "the shipped default delivers the whole frozen accepted prefix"
    );
}

async fn exit_after_default_tidy_cleanup(cleanup: Duration) -> shelterwood::Exit {
    let abort_seen = ReleaseGate::default();
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "tidy",
            TaskDef::new({
                let abort_seen = abort_seen.clone();
                move |context| {
                    let abort_seen = abort_seen.clone();
                    async move {
                        context.abort_token().cancelled().await;
                        abort_seen.release();
                        tokio::time::sleep(cleanup).await;
                        Ok(())
                    }
                }
            }),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("task starts");
    let mut shutdown = Box::pin(system.shutdown(Duration::from_secs(60)));
    assert!(poll_once(shutdown.as_mut()).is_pending());
    advance_time(Duration::from_secs(5)).await;
    abort_seen.wait().await;
    advance_time(cleanup.min(Duration::from_millis(10))).await;
    shutdown.await.expect("default ladder completes");
    task.wait().await
}

#[tokio::test(start_paused = true)]
async fn default_tidy_abort_beat_is_capped_at_ten_milliseconds() {
    let inside = exit_after_default_tidy_cleanup(Duration::from_millis(9)).await;
    assert!(matches!(inside.kind(), ExitKind::Completed));

    let outside = exit_after_default_tidy_cleanup(Duration::from_millis(11)).await;
    assert!(matches!(
        outside.kind(),
        ExitKind::Aborted {
            phase: GracePhase::AfterGrace
        }
    ));
}
