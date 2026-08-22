mod common;

use std::{
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
    sync::{Arc, mpsc},
    task::{Context as TaskContext, Poll, Waker},
    thread::{self, ThreadId},
    time::{Duration, Instant},
};

use common::{
    DestructorGate, LiveWakerCounter, OrdinalWakerState, POLL_TIMEOUT, counting_waker,
    ordinal_waker as action_ordinal_waker,
};
use shelterwood::{
    Actor, ActorOnceDef, Context, DynamicTree, ExitError, ExitResult, RawActor, RawContext,
    RawOnceDef, ScopeRef, Shutdown, TaskDef, Tree,
};

struct IdleActor;

impl Actor for IdleActor {
    type Msg = ();
    type Args = ();

    async fn init((): (), _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self)
    }

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

fn blocking_drop_waker(gate: &DestructorGate, entered: mpsc::Sender<ThreadId>) -> Waker {
    ordinal_waker(1, gate, entered).0
}

fn ordinal_waker(
    target: usize,
    gate: &DestructorGate,
    entered: mpsc::Sender<ThreadId>,
) -> (Waker, Arc<OrdinalWakerState>) {
    let blocker = gate.blocker();
    action_ordinal_waker(target, move || {
        let _ = entered.send(thread::current().id());
        drop(blocker);
    })
}

fn poll_until_registered<F>(runtime: &tokio::runtime::Runtime, future: Pin<&mut F>, path: &str)
where
    F: Future,
{
    let counter = Arc::new(LiveWakerCounter::default());
    let waker = counting_waker(&counter);
    let deadline = Instant::now() + POLL_TIMEOUT;
    let mut future = future;
    loop {
        {
            let _runtime = runtime.handle().enter();
            assert!(
                future
                    .as_mut()
                    .poll(&mut TaskContext::from_waker(&waker))
                    .is_pending(),
                "{path} must park before its timeout"
            );
        }
        // The caller owns one live handle. Two additional handles prove the
        // operation and timer are both parked; repeatedly replacing one
        // registration can no longer satisfy the preparation condition.
        if counter.live() >= 3 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{path} never reached its timer registration"
        );
        runtime.block_on(tokio::task::yield_now());
    }
}

/// Parks a public future with an ordinal waker, cancels it while the timer's
/// caller-waker destructor blocks, and proves unrelated wheel traffic stays
/// live. Public timeouts poll their operation before their timer, so the final
/// caller-waker clone belongs to the timer even when an in-flight lifecycle
/// edge makes the number of earlier registrations vary.
fn assert_public_timer_cancellation_isolated<F>(
    runtime: &tokio::runtime::Runtime,
    mut future: Pin<Box<F>>,
    path: &str,
) where
    F: Future + Send + 'static,
{
    poll_until_registered(runtime, future.as_mut(), path);

    let gate = DestructorGate::default();
    let (entered_tx, entered_rx) = mpsc::channel();
    // Zero leaves every clone inert until the completed poll identifies the
    // timer's actual ordinal.
    let (hostile, state) = ordinal_waker(0, &gate, entered_tx);
    let hostile = ManuallyDrop::new(hostile);
    {
        let _runtime = runtime.handle().enter();
        assert!(
            future
                .as_mut()
                .poll(&mut TaskContext::from_waker(&hostile))
                .is_pending(),
            "{path} stays pending after replacing its registrations"
        );
    }
    state.target_latest_clone(path);

    let cancel_handle = runtime.handle().clone();
    let (cancelled_tx, cancelled_rx) = mpsc::channel();
    let cancel = thread::Builder::new()
        .name(format!("{path}-cancel"))
        .spawn(move || {
            let _runtime = cancel_handle.enter();
            drop(future);
            let _ = cancelled_tx.send(thread::current().id());
        })
        .expect("cancellation thread starts");

    let destructor_thread = entered_rx
        .recv_timeout(POLL_TIMEOUT)
        .unwrap_or_else(|_| panic!("{path} retires its targeted timer caller waker"));

    let probe_handle = runtime.handle().clone();
    let (probe_tx, probe_rx) = mpsc::channel();
    let probe = thread::Builder::new()
        .name(format!("{path}-unrelated-timer"))
        .spawn(move || {
            let _runtime = probe_handle.enter();
            let mut timer = Box::pin(tokio::time::sleep(Duration::from_secs(30)));
            assert!(
                timer
                    .as_mut()
                    .poll(&mut TaskContext::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(timer);
            let _ = probe_tx.send(thread::current().id());
        })
        .expect("unrelated timer thread starts");

    let probe_thread = probe_rx.recv_timeout(POLL_TIMEOUT).ok();
    let cancel_thread = cancelled_rx.recv_timeout(POLL_TIMEOUT).ok();
    gate.release();
    cancel.join().expect("cancellation thread does not panic");
    probe.join().expect("unrelated timer thread does not panic");

    let probe_thread = probe_thread.unwrap_or_else(|| {
        panic!(
            "{path}: a blocking caller-waker destructor on {destructor_thread:?} held Tokio's time-driver mutex"
        )
    });
    let cancel_thread = cancel_thread
        .unwrap_or_else(|| panic!("{path}: cancellation waited for caller-waker destruction"));
    assert_ne!(destructor_thread, cancel_thread);
    assert_ne!(destructor_thread, probe_thread);
    assert_ne!(destructor_thread, thread::current().id());
}

fn request_shutdown_and_wait_for_draining<F>(
    runtime: &tokio::runtime::Runtime,
    mut future: Pin<&mut F>,
    scope: &ScopeRef,
) where
    F: Future,
{
    {
        let _runtime = runtime.handle().enter();
        assert!(
            future
                .as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop()))
                .is_pending(),
            "the stubborn child keeps shutdown pending"
        );
    }
    runtime
        .block_on(async {
            tokio::time::timeout(POLL_TIMEOUT, async {
                loop {
                    if matches!(scope.snapshot().state, shelterwood::ScopeState::Draining) {
                        return;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
        })
        .expect("the root publishes its Draining edge");
}

/// Cancels a whole public deadline future while one caller-waker destructor
/// blocks, then proves a second thread can still arm and cancel a Tokio timer.
///
/// Tokio 1.53.1 drops a timer's registered waker in `Handle::clear_entry`
/// while holding the global time-driver mutex. Before the proxy, the cancel
/// thread therefore held that mutex for the duration of the hostile
/// destructor and the probe thread could not register. The two-worker runtime
/// bounds the scheduler footprint on shared CI hosts; the relevant work is
/// driven from two named native threads so a blocked worker cannot starve the
/// assertion itself.
#[test]
fn blocking_timer_waker_retirement_does_not_stall_unrelated_timer_traffic() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .expect("test runtime");
    let handle = runtime.handle().clone();

    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once("timer-proxy", ActorOnceDef::<IdleActor>::new(()))
        .expect("valid actor");
    let mut future = Box::pin(actor.send_timeout((), Duration::from_secs(30)));
    let gate = DestructorGate::default();
    let (entered_tx, entered_rx) = mpsc::channel();
    // Only the clones registered by the public future are under test. Keeping
    // the caller-owned handle alive also prevents its own destructor from
    // winning the one-shot blocking gate.
    let hostile = ManuallyDrop::new(blocking_drop_waker(&gate, entered_tx));
    {
        let _runtime = handle.enter();
        assert!(matches!(
            future.as_mut().poll(&mut TaskContext::from_waker(&hostile)),
            Poll::Pending
        ));
    }

    let cancel_handle = handle.clone();
    let (cancelled_tx, cancelled_rx) = mpsc::channel();
    let cancel = thread::Builder::new()
        .name("timer-proxy-cancel".into())
        .spawn(move || {
            let _runtime = cancel_handle.enter();
            drop(future);
            let _ = cancelled_tx.send(thread::current().id());
        })
        .expect("cancellation thread starts");

    let destructor_thread = entered_rx
        .recv_timeout(POLL_TIMEOUT)
        .expect("a framework-owned caller-waker clone reaches retirement");

    let probe_handle = handle.clone();
    let (probe_tx, probe_rx) = mpsc::channel();
    let probe = thread::Builder::new()
        .name("timer-proxy-unrelated".into())
        .spawn(move || {
            let _runtime = probe_handle.enter();
            let mut timer = Box::pin(tokio::time::sleep(Duration::from_secs(30)));
            assert!(
                timer
                    .as_mut()
                    .poll(&mut TaskContext::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(timer);
            let _ = probe_tx.send(thread::current().id());
        })
        .expect("unrelated timer thread starts");

    let probe_thread = probe_rx.recv_timeout(POLL_TIMEOUT).ok();
    let cancel_thread = cancelled_rx.recv_timeout(POLL_TIMEOUT).ok();
    gate.release();
    cancel.join().expect("cancellation thread does not panic");
    probe.join().expect("unrelated timer thread does not panic");

    // Unwrapped rather than compared as options, so the identity assertions
    // below cannot pass merely because a thread never reported in.
    let probe_thread = probe_thread.unwrap_or_else(|| {
        panic!(
            "a blocking caller-waker destructor on {destructor_thread:?} held Tokio's time-driver mutex"
        )
    });
    let cancel_thread =
        cancel_thread.expect("timer cancellation waited for disposal-lane waker destruction");

    // The premise, stated rather than assumed: this test only means anything
    // while `Deadlined::drop` keeps the disposal-lane venue that #398 ruling 3
    // grants drop glue. Were the drop path to adopt the poll path's
    // synchronous containment, the destructor would run on the cancelling
    // thread -- naming the threads it must avoid says so directly instead of
    // leaving it to be inferred from two liveness waits.
    assert_ne!(
        destructor_thread, cancel_thread,
        "a blocking caller-waker destructor must not run on the thread cancelling the future"
    );
    assert_ne!(
        destructor_thread, probe_thread,
        "a blocking caller-waker destructor must not run on an unrelated timer thread"
    );
    assert_ne!(
        destructor_thread,
        thread::current().id(),
        "a blocking caller-waker destructor must not run on the polling thread"
    );
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .expect("test runtime")
}

fn add_stubborn_task(tree: &mut Tree) {
    tree.add_task(
        "stubborn",
        TaskDef::new(|_| std::future::pending::<ExitResult>())
            .shutdown(Shutdown::graceful(Duration::from_secs(60)).expect("grace is non-zero")),
    )
    .expect("valid stubborn task");
}

#[test]
fn wait_for_child_timer_cancellation_does_not_stall_unrelated_timer_traffic() {
    let runtime = runtime();
    let system = {
        let _runtime = runtime.handle().enter();
        DynamicTree::new().spawn().expect("runtime is available")
    };
    runtime
        .block_on(system.wait_started())
        .expect("dynamic root starts");
    let scope = system.scope();
    let waiting_scope = scope.clone();
    let future = Box::pin(async move {
        waiting_scope
            .as_scope()
            .wait_for_child("missing", |_| false, Duration::from_secs(30))
            .await
    });

    assert_public_timer_cancellation_isolated(&runtime, future, "wait-for-child");

    runtime
        .block_on(system.shutdown(Duration::ZERO))
        .expect("dynamic root shuts down");
}

#[test]
fn scope_shutdown_timer_cancellation_does_not_stall_unrelated_timer_traffic() {
    let runtime = runtime();
    let mut tree = Tree::new();
    add_stubborn_task(&mut tree);
    let system = {
        let _runtime = runtime.handle().enter();
        tree.spawn().expect("runtime is available")
    };
    runtime
        .block_on(system.wait_started())
        .expect("stubborn tree starts");
    let scope = system.scope();
    let shutting_scope = scope.clone();
    let mut future = Box::pin(async move {
        shutting_scope
            .shutdown_and_wait(Duration::from_secs(30))
            .await
    });
    request_shutdown_and_wait_for_draining(&runtime, future.as_mut(), &scope);

    assert_public_timer_cancellation_isolated(&runtime, future, "scope-shutdown");

    let _ = runtime.block_on(system.shutdown(Duration::ZERO));
}

#[test]
fn system_shutdown_timer_cancellation_does_not_stall_unrelated_timer_traffic() {
    let runtime = runtime();
    let mut tree = Tree::new();
    add_stubborn_task(&mut tree);
    let system = {
        let _runtime = runtime.handle().enter();
        tree.spawn().expect("runtime is available")
    };
    runtime
        .block_on(system.wait_started())
        .expect("stubborn tree starts");
    let cleanup_scope = system.scope();
    let mut future = Box::pin(async move { system.shutdown(Duration::from_secs(30)).await });
    request_shutdown_and_wait_for_draining(&runtime, future.as_mut(), &cleanup_scope);

    assert_public_timer_cancellation_isolated(&runtime, future, "system-shutdown");

    let _ = runtime.block_on(cleanup_scope.shutdown_and_wait(Duration::ZERO));
}

struct RawRecvTimerProbe {
    hostile: Option<Waker>,
    clones: Arc<OrdinalWakerState>,
    polled: mpsc::Sender<usize>,
    cancelled: mpsc::Sender<ThreadId>,
}

impl RawActor for RawRecvTimerProbe {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        context
            .set_timeout("timer", (), Duration::from_secs(30))
            .expect("live raw context accepts the timer");
        let hostile = self.hostile.take().expect("the probe polls only once");
        let mut receive = Box::pin(context.recv());
        assert!(
            receive
                .as_mut()
                .poll(&mut TaskContext::from_waker(&hostile))
                .is_pending()
        );
        let _ = self.polled.send(self.clones.clones());
        drop(receive);
        let _ = self.cancelled.send(thread::current().id());
        Ok(())
    }
}

#[test]
fn raw_recv_timer_cancellation_does_not_stall_unrelated_timer_traffic() {
    let runtime = runtime();
    let gate = DestructorGate::default();
    let (entered_tx, entered_rx) = mpsc::channel();
    // shutdown, local stop, mailbox, offload event, then the raw timer.
    let (hostile, state) = ordinal_waker(5, &gate, entered_tx);
    let (polled_tx, polled_rx) = mpsc::channel();
    let (cancelled_tx, cancelled_rx) = mpsc::channel();
    let mut tree = Tree::new();
    tree.add_raw_once(
        "raw-timer-proxy",
        RawOnceDef::new(RawRecvTimerProbe {
            hostile: Some(hostile),
            clones: Arc::clone(&state),
            polled: polled_tx,
            cancelled: cancelled_tx,
        }),
    )
    .expect("valid raw actor");
    let system = {
        let _runtime = runtime.handle().enter();
        tree.spawn().expect("runtime is available")
    };

    assert_eq!(
        polled_rx
            .recv_timeout(POLL_TIMEOUT)
            .expect("the raw actor manually parks recv"),
        5,
        "the fifth caller clone belongs to the armed raw timer"
    );
    let destructor_thread = entered_rx
        .recv_timeout(POLL_TIMEOUT)
        .expect("dropping raw recv retires the timer's caller waker");

    let probe_handle = runtime.handle().clone();
    let (probe_tx, probe_rx) = mpsc::channel();
    let probe = thread::Builder::new()
        .name("raw-recv-unrelated-timer".into())
        .spawn(move || {
            let _runtime = probe_handle.enter();
            let mut timer = Box::pin(tokio::time::sleep(Duration::from_secs(30)));
            assert!(
                timer
                    .as_mut()
                    .poll(&mut TaskContext::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(timer);
            let _ = probe_tx.send(thread::current().id());
        })
        .expect("unrelated timer thread starts");

    let probe_thread = probe_rx.recv_timeout(POLL_TIMEOUT).ok();
    let actor_thread = cancelled_rx.recv_timeout(POLL_TIMEOUT).ok();
    gate.release();
    probe.join().expect("unrelated timer thread does not panic");

    let probe_thread = probe_thread.unwrap_or_else(|| {
        panic!(
            "a raw recv caller-waker destructor on {destructor_thread:?} held Tokio's time-driver mutex"
        )
    });
    let actor_thread = actor_thread.expect("raw recv cancellation does not await destruction");
    assert_ne!(destructor_thread, actor_thread);
    assert_ne!(destructor_thread, probe_thread);
    assert_ne!(destructor_thread, thread::current().id());

    assert_eq!(
        runtime.block_on(system.wait()),
        shelterwood::StopReason::Finished
    );
}
