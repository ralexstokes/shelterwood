//! Pins SPEC Appendix A's shipped defaults behaviorally: every other test
//! sets policy explicitly, so without these a default regression passes the
//! whole suite.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{POLL_TIMEOUT, ReleaseGate, advance_time, assert_quiet, poll_once, poll_until};
use shelterwood::{
    Actor, ActorDef, Context, ExitError, ExitKind, ExitResult, Readiness, SendErrorKind,
    StartupError, StopReason, TaskDef, Tree,
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

    let mut shutdown = Box::pin(system.shutdown(Duration::from_secs(60)));
    assert_quiet(Duration::from_millis(50), || {
        poll_once(shutdown.as_mut()).is_ready()
    })
    .await;
    advance_time(Duration::from_millis(4500)).await;
    // ~4.55s elapsed: still inside the shipped 5s grace.
    assert_quiet(Duration::from_millis(50), || {
        poll_once(shutdown.as_mut()).is_ready()
    })
    .await;
    advance_time(Duration::from_millis(600)).await;
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            poll_once(shutdown.as_mut()).is_ready()
        })
        .await,
        "grace elapses at 5s and the abort ladder completes"
    );
    drop(shutdown);

    let exit = task.wait().await;
    assert!(
        matches!(exit.kind(), ExitKind::Aborted { after_grace: true }),
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
