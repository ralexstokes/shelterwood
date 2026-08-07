use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::common::{ReleaseGate, poll_until};
use shelterwood::{
    Actor, ActorOnceDef, Context, ExitError, ExitResult, Mailbox, SendErrorKind, Tree,
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
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
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
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
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
    let parked = tokio::spawn(async move { parked_actor.send(Message::Value(4)).await });
    tokio::task::yield_now().await;
    assert!(
        !parked.is_finished(),
        "ordinary send parks at frozen intake"
    );

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

#[tokio::test]
async fn queue_drain_is_the_exact_frozen_accepted_prefix_and_rejects_deferred_work() {
    assert_drain_fixture(Mailbox::queue(8).expect("valid capacity"), &[0, 1, 2]).await;
}

#[tokio::test]
async fn latest_drain_is_the_post_conflation_survivor_and_rejects_deferred_work() {
    assert_drain_fixture(Mailbox::latest(), &[0, 2]).await;
}
