use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{
    DestructorBlocker, DestructorGate, ReleaseGate, policy::never, poll_once, poll_until,
};
use shelterwood::{
    Backoff, ExitError, ExitResult, Jitter, Mailbox, RawActor, RawContext, RawDef,
    RestartCondition, RestartPolicy, SendErrorKind, Tree,
};

struct RestartActor {
    generation: usize,
    fail_first: ReleaseGate,
    _blocker: Option<DestructorBlocker>,
    deliveries: Arc<Mutex<Vec<(usize, usize)>>>,
}

impl RawActor for RestartActor {
    type Msg = usize;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        if self.generation == 1 {
            self.fail_first.wait().await;
            return Err(ExitError::message("replace first incarnation"));
        }
        while let Some(message) = context.recv().await {
            self.deliveries
                .lock()
                .expect("deliveries mutex poisoned")
                .push((self.generation, message));
        }
        Ok(())
    }
}

fn restarting_definition(
    factories: &Arc<AtomicUsize>,
    fail_first: &ReleaseGate,
    destructor: &DestructorGate,
    deliveries: &Arc<Mutex<Vec<(usize, usize)>>>,
    block_first: bool,
    restart: RestartPolicy,
) -> RawDef<RestartActor> {
    RawDef::factory({
        let factories = Arc::clone(factories);
        let fail_first = fail_first.clone();
        let destructor = destructor.clone();
        let deliveries = Arc::clone(deliveries);
        move || {
            let generation = factories.fetch_add(1, Ordering::SeqCst) + 1;
            RestartActor {
                generation,
                fail_first: fail_first.clone(),
                _blocker: (block_first && generation == 1).then(|| destructor.blocker()),
                deliveries: Arc::clone(&deliveries),
            }
        }
    })
    .mailbox(Mailbox::queue(1).expect("non-zero capacity"))
    .restart(restart)
}

async fn wait_for_destructor(destructor: &DestructorGate) {
    let destructor = destructor.clone();
    tokio::task::spawn_blocking(move || destructor.wait_entered())
        .await
        .expect("destructor waiter joins");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_rides_the_frozen_destructor_and_rebind_window() {
    let factories = Arc::new(AtomicUsize::new(0));
    let fail_first = ReleaseGate::default();
    let destructor = DestructorGate::default();
    let deliveries = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "worker",
            restarting_definition(
                &factories,
                &fail_first,
                &destructor,
                &deliveries,
                true,
                RestartPolicy::default(),
            ),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("first actor starts");
    let first = actor.try_send(1).expect("first incarnation accepts");
    let mut parked = Box::pin(actor.send(42));
    assert!(poll_once(parked.as_mut()).is_pending());

    fail_first.release();
    wait_for_destructor(&destructor).await;
    let frozen = actor
        .try_send(2)
        .expect_err("frozen intake rejects try_send");
    assert_eq!(frozen.kind, SendErrorKind::NotRunning);
    assert_eq!(frozen.incarnation_observed, Some(first));
    assert!(poll_once(parked.as_mut()).is_pending());

    destructor.release();
    let accepting = parked.await.expect("send rides into replacement");
    assert!(accepting.supersedes(first));
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            deliveries
                .lock()
                .expect("deliveries mutex poisoned")
                .as_slice()
                == [(2, 42)]
        })
        .await
    );
    assert_eq!(
        factories.load(Ordering::SeqCst),
        2,
        "only one replacement starts"
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn never_restart_turns_a_frozen_parked_send_into_terminal() {
    let factories = Arc::new(AtomicUsize::new(0));
    let fail_first = ReleaseGate::default();
    let destructor = DestructorGate::default();
    let deliveries = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "worker",
            restarting_definition(
                &factories,
                &fail_first,
                &destructor,
                &deliveries,
                true,
                never(),
            ),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    let first = actor.try_send(1).expect("first incarnation accepts");
    let mut parked = Box::pin(actor.send(42));
    assert!(poll_once(parked.as_mut()).is_pending());
    fail_first.release();
    wait_for_destructor(&destructor).await;
    assert!(poll_once(parked.as_mut()).is_pending());
    destructor.release();
    let error = parked.await.expect_err("membership terminalizes");
    assert_eq!(error.kind, SendErrorKind::Terminated);
    assert_eq!(error.incarnation_observed, Some(first));
    assert_eq!(factories.load(Ordering::SeqCst), 1);
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
}

#[tokio::test(start_paused = true)]
async fn timed_send_withdraws_while_replacement_is_in_backoff() {
    let factories = Arc::new(AtomicUsize::new(0));
    let fail_first = ReleaseGate::default();
    let destructor = DestructorGate::default();
    let deliveries = Arc::new(Mutex::new(Vec::new()));
    let backoff = Duration::from_secs(30);
    let timeout = Duration::from_secs(10);
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "worker",
            restarting_definition(
                &factories,
                &fail_first,
                &destructor,
                &deliveries,
                false,
                RestartPolicy::new(
                    RestartCondition::OnFailure,
                    Backoff::fixed(backoff, Jitter::None).expect("non-zero backoff"),
                ),
            ),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    let first = actor.try_send(1).expect("first incarnation accepts");
    let timed_actor = actor.clone();
    let timed = tokio::spawn(async move { timed_actor.send_timeout(42, timeout).await });
    tokio::task::yield_now().await;
    fail_first.release();
    let mut observed_rebind = None;
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            match actor.try_send(0) {
                Err(error) if error.kind == SendErrorKind::NotRunning => {
                    observed_rebind = Some(error.incarnation_observed);
                    true
                }
                _ => false,
            }
        })
        .await
    );
    assert_eq!(observed_rebind, Some(None));
    tokio::time::advance(timeout).await;
    let error = timed
        .await
        .expect("timed send task joins")
        .expect_err("backoff outlives timeout");
    assert_eq!(error.kind, SendErrorKind::TimedOut);
    assert_eq!(error.incarnation_observed, Some(first));
    assert_eq!(error.message, 42);
    system.shutdown(Duration::ZERO).await.expect("root stops");
}

/// Dropping a parked send while the mailbox is unbound (the rebind window)
/// withdraws it: the replacement incarnation never sees the message even
/// though promotion runs at the next bind (§5.1's withdrawal rule).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_a_parked_send_in_the_rebind_window_withdraws_it() {
    let factories = Arc::new(AtomicUsize::new(0));
    let fail_first = ReleaseGate::default();
    let destructor = DestructorGate::default();
    let deliveries = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "worker",
            restarting_definition(
                &factories,
                &fail_first,
                &destructor,
                &deliveries,
                true,
                RestartPolicy::default(),
            ),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("first actor starts");
    actor.try_send(1).expect("first incarnation accepts");
    let mut parked = Box::pin(actor.send(42));
    assert!(poll_once(parked.as_mut()).is_pending());

    fail_first.release();
    wait_for_destructor(&destructor).await;
    // Park a second send inside the frozen rebind window, then drop it
    // before the replacement binds.
    let mut doomed = Box::pin(actor.send(43));
    assert!(poll_once(doomed.as_mut()).is_pending());
    drop(doomed);

    destructor.release();
    parked.await.expect("the retained send rides the window");
    actor.send(44).await.expect("replacement accepts");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            deliveries
                .lock()
                .expect("deliveries mutex poisoned")
                .as_slice()
                == [(2, 42), (2, 44)]
        })
        .await,
        "the withdrawn send must never be promoted at bind: {:?}",
        deliveries.lock().expect("deliveries mutex poisoned")
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}
