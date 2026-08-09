use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{POLL_TIMEOUT, ReleaseGate, assert_quiet, poll_until};
use shelterwood::{
    Actor, ActorOnceDef, Context, Exit, ExitError, ExitKind, ExitResult, LifecycleEventKind,
    LifecycleEvents, LifecycleItem, Mailbox, MailboxShutdown, SendErrorKind, Shutdown, StopContext,
    Tree,
};

#[derive(Clone, Copy, Debug)]
enum Message {
    Stop,
    Value(u8),
    Deferred,
    Timer,
    Offload,
}

struct Args {
    stop_entered: Arc<AtomicBool>,
    allow_stop: ReleaseGate,
    drain_entered: Arc<AtomicBool>,
    allow_drain: ReleaseGate,
    deferred_ran: Arc<AtomicBool>,
    log: Arc<Mutex<Vec<u8>>>,
}

struct DrainActor(Args);

impl Actor for DrainActor {
    type Msg = Message;
    type Args = Args;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self(args))
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            Message::Stop => {
                assert!(!context.is_draining());
                self.0.log.lock().expect("log mutex poisoned").push(0);
                self.0.stop_entered.store(true, Ordering::SeqCst);
                self.0.allow_stop.wait().await;
                context.stop();
            }
            Message::Value(value) => {
                assert!(context.is_draining());
                self.0.log.lock().expect("log mutex poisoned").push(value);
                if !self.0.drain_entered.swap(true, Ordering::SeqCst) {
                    let continuation = context
                        .continue_with(Message::Deferred)
                        .expect_err("drain rejects continuations");
                    assert!(matches!(continuation.into_inner(), Message::Deferred));

                    let timer = context
                        .set_timeout("timer", Message::Timer, Duration::ZERO)
                        .expect_err("drain rejects timers");
                    assert!(matches!(timer.into_inner().1, Message::Timer));

                    let interval = context
                        .set_interval("interval", Message::Timer, Duration::from_secs(1))
                        .expect_err("drain rejects intervals");
                    let (interval_key, interval_message) = interval.into_inner();
                    assert_eq!(interval_key, "interval");
                    assert!(matches!(interval_message, Message::Timer));

                    context
                        .clear_timer(&"timer")
                        .expect_err("drain rejects timer retraction");

                    let ran = Arc::clone(&self.0.deferred_ran);
                    let offload = context
                        .offload(
                            async move {
                                ran.store(true, Ordering::SeqCst);
                            },
                            |_| Message::Offload,
                            Duration::from_secs(1),
                        )
                        .expect_err("drain rejects offloads");
                    drop(offload);

                    let scoped_ran = Arc::clone(&self.0.deferred_ran);
                    let scoped = context
                        .offload_scoped(
                            async move {
                                scoped_ran.store(true, Ordering::SeqCst);
                            },
                            |_| Message::Offload,
                            Duration::from_secs(1),
                        )
                        .expect_err("drain rejects scoped offloads");
                    // The rejected payload is recovered whole: the work
                    // future is discarded without running and the
                    // continuation still produces its message.
                    let (scoped_work, scoped_continuation) = scoped.into_inner();
                    drop(scoped_work);
                    assert!(matches!(scoped_continuation(Ok(())), Message::Offload));

                    self.0.allow_drain.wait().await;
                }
            }
            Message::Deferred | Message::Timer | Message::Offload => {
                self.0.deferred_ran.store(true, Ordering::SeqCst);
            }
        }
        Ok(())
    }
}

async fn assert_drain_fixture(mailbox: Mailbox, expected: &[u8]) {
    let stop_entered = Arc::new(AtomicBool::new(false));
    let allow_stop = ReleaseGate::default();
    let drain_entered = Arc::new(AtomicBool::new(false));
    let allow_drain = ReleaseGate::default();
    let deferred_ran = Arc::new(AtomicBool::new(false));
    let log = Arc::new(Mutex::new(Vec::new()));

    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "drain",
            ActorOnceDef::<DrainActor>::new(Args {
                stop_entered: Arc::clone(&stop_entered),
                allow_stop: allow_stop.clone(),
                drain_entered: Arc::clone(&drain_entered),
                allow_drain: allow_drain.clone(),
                deferred_ran: Arc::clone(&deferred_ran),
                log: Arc::clone(&log),
            })
            .mailbox(mailbox),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");

    actor.send(Message::Stop).await.expect("stop accepted");
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            stop_entered.load(Ordering::SeqCst)
        })
        .await
    );
    actor
        .send(Message::Value(1))
        .await
        .expect("prefix accepted");
    actor
        .send(Message::Value(2))
        .await
        .expect("prefix accepted");
    allow_stop.release();

    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            drain_entered.load(Ordering::SeqCst)
        })
        .await
    );
    let rejection = actor
        .try_send(Message::Value(3))
        .expect_err("frozen intake rejects fail-fast sends");
    assert_eq!(rejection.kind, SendErrorKind::NotRunning);
    assert!(rejection.incarnation_observed.is_some());

    let parked_actor = actor.clone();
    let parked_started = ReleaseGate::default();
    let parked = tokio::spawn({
        let parked_started = parked_started.clone();
        async move {
            parked_started.release();
            parked_actor.send(Message::Value(4)).await
        }
    });
    parked_started.wait().await;
    assert_quiet(Duration::from_millis(20), || parked.is_finished()).await;

    allow_drain.release();
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    let terminal = parked
        .await
        .expect("send task joins")
        .expect_err("one-shot membership terminalizes");
    assert_eq!(terminal.kind, SendErrorKind::Terminated);
    assert_eq!(*log.lock().expect("log mutex poisoned"), expected);
    assert!(!deferred_ran.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn queue_drain_is_the_exact_frozen_accepted_prefix_and_rejects_deferred_work() {
    assert_drain_fixture(Mailbox::queue(8).expect("valid capacity"), &[0, 1, 2]).await;
}

#[tokio::test(start_paused = true)]
async fn latest_drain_is_the_post_conflation_survivor_and_rejects_deferred_work() {
    assert_drain_fixture(Mailbox::latest(), &[0, 2]).await;
}

#[derive(Clone, Copy, Debug)]
enum FaultMode {
    Discard,
    DrainError,
    DrainPanic,
    StopPanic,
    SharedGrace,
    StopBlocking,
}

#[derive(Clone, Copy)]
enum FaultMessage {
    Hold,
    Fault,
}

struct FaultArgs {
    mode: FaultMode,
    hold_entered: ReleaseGate,
    hold_release: ReleaseGate,
    shutdown_seen: ReleaseGate,
    drain_entered: ReleaseGate,
    stop_entered: Arc<AtomicBool>,
    drained: Arc<AtomicUsize>,
    blocking_result: Arc<AtomicUsize>,
}

struct FaultActor(FaultArgs);

impl Actor for FaultActor {
    type Msg = FaultMessage;
    type Args = FaultArgs;

    async fn init(args: Self::Args, context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        let shutdown = context.shutdown_token();
        let shutdown_seen = args.shutdown_seen.clone();
        tokio::spawn(async move {
            shutdown.cancelled().await;
            shutdown_seen.release();
        });
        Ok(Self(args))
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            FaultMessage::Hold => {
                assert!(!context.is_draining());
                self.0.hold_entered.release();
                self.0.hold_release.wait().await;
                Ok(())
            }
            FaultMessage::Fault => {
                assert!(context.is_draining());
                self.0.drained.fetch_add(1, Ordering::SeqCst);
                self.0.drain_entered.release();
                match self.0.mode {
                    FaultMode::DrainError => Err(ExitError::message("drain error")),
                    FaultMode::DrainPanic => panic!("drain panic"),
                    FaultMode::SharedGrace => {
                        tokio::time::sleep(Duration::from_secs(8)).await;
                        Ok(())
                    }
                    FaultMode::Discard | FaultMode::StopPanic | FaultMode::StopBlocking => Ok(()),
                }
            }
        }
    }

    async fn on_stop(&mut self, context: &mut StopContext<'_, Self>) {
        self.0.stop_entered.store(true, Ordering::SeqCst);
        match self.0.mode {
            FaultMode::StopPanic => panic!("on_stop panic"),
            FaultMode::SharedGrace => tokio::time::sleep(Duration::from_secs(8)).await,
            FaultMode::StopBlocking => {
                let value = context
                    .run_blocking(|shutdown| {
                        assert!(shutdown.is_cancelled());
                        73usize
                    })
                    .await;
                self.0.blocking_result.store(value, Ordering::SeqCst);
            }
            FaultMode::Discard | FaultMode::DrainError | FaultMode::DrainPanic => {}
        }
    }
}

struct FaultOutcome {
    exit: Exit,
    elapsed: Duration,
    stop_entered: bool,
    drained: usize,
    blocking_result: usize,
}

async fn exited_actor(events: &mut LifecycleEvents) -> Exit {
    while let Some(item) = events.recv().await {
        if let LifecycleItem::Event(event) = item
            && let LifecycleEventKind::Exited { id, exit, .. } = event.kind
            && id.as_str() == "actor"
        {
            return exit;
        }
    }
    panic!("actor exit must precede lifecycle closure");
}

async fn run_fault_fixture(
    mode: FaultMode,
    mailbox_shutdown: MailboxShutdown,
    grace: Duration,
    queue_fault: bool,
) -> FaultOutcome {
    let hold_entered = ReleaseGate::default();
    let hold_release = ReleaseGate::default();
    let shutdown_seen = ReleaseGate::default();
    let drain_entered = ReleaseGate::default();
    let stop_entered = Arc::new(AtomicBool::new(false));
    let drained = Arc::new(AtomicUsize::new(0));
    let blocking_result = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "actor",
            ActorOnceDef::<FaultActor>::new(FaultArgs {
                mode,
                hold_entered: hold_entered.clone(),
                hold_release: hold_release.clone(),
                shutdown_seen: shutdown_seen.clone(),
                drain_entered: drain_entered.clone(),
                stop_entered: Arc::clone(&stop_entered),
                drained: Arc::clone(&drained),
                blocking_result: Arc::clone(&blocking_result),
            })
            .mailbox_shutdown(mailbox_shutdown)
            .shutdown(Shutdown::Graceful { grace }),
        )
        .expect("valid fault actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    let mut events = system.scope().subscribe_lifecycle();
    actor
        .send(FaultMessage::Hold)
        .await
        .expect("hold message is accepted");
    hold_entered.wait().await;
    if queue_fault {
        actor
            .send(FaultMessage::Fault)
            .await
            .expect("fault message joins the accepted prefix");
    }

    let started_at = tokio::time::Instant::now();
    let shutdown = tokio::spawn(system.shutdown(Duration::from_secs(30)));
    shutdown_seen.wait().await;
    hold_release.release();
    if matches!(mode, FaultMode::SharedGrace) {
        drain_entered.wait().await;
        assert!(
            poll_until(Duration::from_secs(9), Duration::from_secs(1), || {
                stop_entered.load(Ordering::SeqCst)
            })
            .await,
            "draining leaves part of the shared grace for on_stop"
        );
    }
    shutdown
        .await
        .expect("shutdown task joins")
        .expect("the child policy bounds shutdown");
    let elapsed = tokio::time::Instant::now() - started_at;
    let exit = exited_actor(&mut events).await;
    FaultOutcome {
        exit,
        elapsed,
        stop_entered: stop_entered.load(Ordering::SeqCst),
        drained: drained.load(Ordering::SeqCst),
        blocking_result: blocking_result.load(Ordering::SeqCst),
    }
}

#[tokio::test(start_paused = true)]
async fn handler_discard_skips_the_prefix_and_still_runs_on_stop() {
    let outcome = run_fault_fixture(
        FaultMode::Discard,
        MailboxShutdown::Discard,
        Duration::from_secs(10),
        true,
    )
    .await;
    assert_eq!(outcome.drained, 0);
    assert!(outcome.stop_entered);
    assert!(matches!(outcome.exit.kind(), ExitKind::Completed));
    assert!(outcome.exit.cancelled());
}

#[tokio::test(start_paused = true)]
async fn handler_drain_errors_and_panics_keep_their_authoritative_exit() {
    let error = run_fault_fixture(
        FaultMode::DrainError,
        MailboxShutdown::Drain,
        Duration::from_secs(10),
        true,
    )
    .await;
    assert!(matches!(
        error.exit.kind(),
        ExitKind::Failed(cause) if cause.to_string() == "drain error"
    ));
    assert!(!error.stop_entered);

    let panic = run_fault_fixture(
        FaultMode::DrainPanic,
        MailboxShutdown::Drain,
        Duration::from_secs(10),
        true,
    )
    .await;
    assert!(matches!(
        panic.exit.kind(),
        ExitKind::Panicked { message } if message.as_deref() == Some("drain panic")
    ));
    assert!(!panic.stop_entered);
}

#[tokio::test(start_paused = true)]
async fn handler_on_stop_panic_is_contained_and_classified() {
    let outcome = run_fault_fixture(
        FaultMode::StopPanic,
        MailboxShutdown::Drain,
        Duration::from_secs(10),
        false,
    )
    .await;
    assert!(matches!(
        outcome.exit.kind(),
        ExitKind::Panicked { message } if message.as_deref() == Some("on_stop panic")
    ));
}

#[tokio::test(start_paused = true)]
async fn handler_drain_and_on_stop_share_one_grace_budget() {
    let outcome = run_fault_fixture(
        FaultMode::SharedGrace,
        MailboxShutdown::Drain,
        Duration::from_secs(10),
        true,
    )
    .await;
    assert_eq!(outcome.drained, 1);
    assert!(outcome.stop_entered);
    assert!(matches!(
        outcome.exit.kind(),
        ExitKind::Aborted { after_grace: true }
    ));
    assert!(outcome.elapsed >= Duration::from_secs(10));
    assert!(
        outcome.elapsed < Duration::from_secs(16),
        "drain and on_stop must not receive separate eight-second budgets"
    );
}

#[tokio::test]
async fn stop_context_run_blocking_returns_inside_cooperative_grace() {
    let outcome = run_fault_fixture(
        FaultMode::StopBlocking,
        MailboxShutdown::Drain,
        Duration::from_secs(1),
        false,
    )
    .await;
    assert_eq!(outcome.blocking_result, 73);
    assert!(outcome.stop_entered);
    assert!(matches!(outcome.exit.kind(), ExitKind::Completed));
}
