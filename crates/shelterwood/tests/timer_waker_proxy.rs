mod common;

use std::{
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context as TaskContext, Poll, RawWaker, RawWakerVTable, Waker},
    thread::{self, ThreadId},
    time::Duration,
};

use common::{DestructorBlocker, DestructorGate, POLL_TIMEOUT};
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

struct BlockingDropWaker {
    blocked: AtomicBool,
    blocker: Mutex<Option<DestructorBlocker>>,
    entered: mpsc::Sender<ThreadId>,
}

unsafe fn clone_blocking_drop_waker(data: *const ()) -> RawWaker {
    // SAFETY: every pointer using this vtable came from an Arc of the
    // matching type. ManuallyDrop preserves the reference represented by
    // `data`; the returned raw waker owns only the new clone.
    let state = ManuallyDrop::new(unsafe { Arc::<BlockingDropWaker>::from_raw(data.cast()) });
    RawWaker::new(
        Arc::into_raw(Arc::clone(&state)).cast(),
        &BLOCKING_DROP_WAKER_VTABLE,
    )
}

unsafe fn wake_blocking_drop_waker(data: *const ()) {
    // SAFETY: wake consumes the Arc reference represented by this raw waker.
    drop(unsafe { Arc::<BlockingDropWaker>::from_raw(data.cast()) });
}

unsafe fn wake_by_ref_blocking_drop_waker(_data: *const ()) {}

unsafe fn drop_blocking_drop_waker(data: *const ()) {
    // SAFETY: drop consumes the Arc reference represented by this raw waker.
    let state = unsafe { Arc::<BlockingDropWaker>::from_raw(data.cast()) };
    if !state.blocked.swap(true, Ordering::SeqCst) {
        let blocker = state
            .blocker
            .lock()
            .expect("blocking waker mutex poisoned")
            .take()
            .expect("the first framework clone owns the blocker");
        let _ = state.entered.send(thread::current().id());
        drop(blocker);
    }
}

static BLOCKING_DROP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_blocking_drop_waker,
    wake_blocking_drop_waker,
    wake_by_ref_blocking_drop_waker,
    drop_blocking_drop_waker,
);

fn blocking_drop_waker(gate: &DestructorGate, entered: mpsc::Sender<ThreadId>) -> Waker {
    let raw = RawWaker::new(
        Arc::into_raw(Arc::new(BlockingDropWaker {
            blocked: AtomicBool::new(false),
            blocker: Mutex::new(Some(gate.blocker())),
            entered,
        }))
        .cast(),
        &BLOCKING_DROP_WAKER_VTABLE,
    );
    // SAFETY: `raw` owns one Arc reference and its vtable maintains that
    // ownership across clone, wake, and drop.
    unsafe { Waker::from_raw(raw) }
}

#[derive(Default)]
struct CloneCounter(AtomicUsize);

unsafe fn clone_counting_waker(data: *const ()) -> RawWaker {
    // SAFETY: every pointer using this vtable came from an Arc of the
    // matching type. ManuallyDrop preserves the reference represented by
    // `data`; the returned raw waker owns only the new clone.
    let state = ManuallyDrop::new(unsafe { Arc::<CloneCounter>::from_raw(data.cast()) });
    state.0.fetch_add(1, Ordering::SeqCst);
    RawWaker::new(
        Arc::into_raw(Arc::clone(&state)).cast(),
        &COUNTING_WAKER_VTABLE,
    )
}

unsafe fn wake_counting_waker(data: *const ()) {
    // SAFETY: wake consumes the Arc reference represented by this raw waker.
    drop(unsafe { Arc::<CloneCounter>::from_raw(data.cast()) });
}

unsafe fn wake_by_ref_counting_waker(_data: *const ()) {}

unsafe fn drop_counting_waker(data: *const ()) {
    // SAFETY: drop consumes the Arc reference represented by this raw waker.
    drop(unsafe { Arc::<CloneCounter>::from_raw(data.cast()) });
}

static COUNTING_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_counting_waker,
    wake_counting_waker,
    wake_by_ref_counting_waker,
    drop_counting_waker,
);

fn counting_waker(counter: &Arc<CloneCounter>) -> Waker {
    let raw = RawWaker::new(
        Arc::into_raw(Arc::clone(counter)).cast(),
        &COUNTING_WAKER_VTABLE,
    );
    // SAFETY: `raw` owns one Arc reference and its vtable maintains that
    // ownership across clone, wake, and drop.
    unsafe { Waker::from_raw(raw) }
}

struct OrdinalWaker {
    ordinal: usize,
    shared: Arc<OrdinalWakerState>,
}

struct OrdinalWakerState {
    clones: AtomicUsize,
    target: usize,
    blocked: AtomicBool,
    blocker: Mutex<Option<DestructorBlocker>>,
    entered: mpsc::Sender<ThreadId>,
}

unsafe fn clone_ordinal_waker(data: *const ()) -> RawWaker {
    // SAFETY: every pointer using this vtable came from an Arc of the
    // matching type. ManuallyDrop preserves the reference represented by
    // `data`; the returned raw waker owns only the new clone.
    let current = ManuallyDrop::new(unsafe { Arc::<OrdinalWaker>::from_raw(data.cast()) });
    let ordinal = current.shared.clones.fetch_add(1, Ordering::SeqCst) + 1;
    RawWaker::new(
        Arc::into_raw(Arc::new(OrdinalWaker {
            ordinal,
            shared: Arc::clone(&current.shared),
        }))
        .cast(),
        &ORDINAL_WAKER_VTABLE,
    )
}

unsafe fn wake_ordinal_waker(data: *const ()) {
    // SAFETY: wake consumes the Arc reference represented by this raw waker.
    unsafe { drop_ordinal_waker(data) };
}

unsafe fn wake_by_ref_ordinal_waker(_data: *const ()) {}

unsafe fn drop_ordinal_waker(data: *const ()) {
    // SAFETY: drop consumes the Arc reference represented by this raw waker.
    let current = unsafe { Arc::<OrdinalWaker>::from_raw(data.cast()) };
    if current.ordinal == current.shared.target
        && !current.shared.blocked.swap(true, Ordering::SeqCst)
    {
        let blocker = current
            .shared
            .blocker
            .lock()
            .expect("ordinal waker mutex poisoned")
            .take()
            .expect("the targeted framework clone owns the blocker");
        let _ = current.shared.entered.send(thread::current().id());
        drop(blocker);
    }
}

static ORDINAL_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_ordinal_waker,
    wake_ordinal_waker,
    wake_by_ref_ordinal_waker,
    drop_ordinal_waker,
);

fn ordinal_waker(
    target: usize,
    gate: &DestructorGate,
    entered: mpsc::Sender<ThreadId>,
) -> (Waker, Arc<OrdinalWakerState>) {
    let shared = Arc::new(OrdinalWakerState {
        clones: AtomicUsize::new(0),
        target,
        blocked: AtomicBool::new(false),
        blocker: Mutex::new(Some(gate.blocker())),
        entered,
    });
    let raw = RawWaker::new(
        Arc::into_raw(Arc::new(OrdinalWaker {
            ordinal: 0,
            shared: Arc::clone(&shared),
        }))
        .cast(),
        &ORDINAL_WAKER_VTABLE,
    );
    // SAFETY: `raw` owns one Arc reference and its vtable maintains that
    // ownership across clone, wake, and drop.
    (unsafe { Waker::from_raw(raw) }, shared)
}

fn poll_until_registered<F>(
    runtime: &tokio::runtime::Runtime,
    future: Pin<&mut F>,
    minimum_clones: usize,
    path: &str,
) where
    F: Future,
{
    let counter = Arc::new(CloneCounter::default());
    let waker = counting_waker(&counter);
    let mut future = future;
    for _ in 0..100 {
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
        if counter.0.load(Ordering::SeqCst) >= minimum_clones {
            return;
        }
        runtime.block_on(tokio::task::yield_now());
    }
    panic!("{path} never reached its timer registration");
}

/// Parks a public future with an ordinal waker, cancels it while the timer's
/// caller-waker destructor blocks, and proves unrelated wheel traffic stays
/// live. Earlier registrations use a benign counting waker so lifecycle
/// transitions can settle without making the timer's ordinal unstable.
fn assert_public_timer_cancellation_isolated<F>(
    runtime: &tokio::runtime::Runtime,
    mut future: Pin<Box<F>>,
    prepare_clones: usize,
    timer_ordinal: usize,
    path: &str,
) where
    F: Future + Send + 'static,
{
    poll_until_registered(runtime, future.as_mut(), prepare_clones, path);

    let gate = DestructorGate::default();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (hostile, state) = ordinal_waker(timer_ordinal, &gate, entered_tx);
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
    assert_eq!(
        state.clones.load(Ordering::SeqCst),
        timer_ordinal,
        "{path} registers the timer at the documented caller-clone ordinal"
    );

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

    // Snapshot change is clone one; the timer proxy's caller is clone two.
    assert_public_timer_cancellation_isolated(&runtime, future, 2, 2, "wait-for-child");

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

    // At the stable timeout cut the two live registrations are incarnation
    // change, then timer.
    assert_public_timer_cancellation_isolated(&runtime, future, 2, 2, "scope-shutdown");

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

    assert_public_timer_cancellation_isolated(&runtime, future, 2, 2, "system-shutdown");

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
        let _ = self.polled.send(self.clones.clones.load(Ordering::SeqCst));
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
