mod common;

use std::{
    future::Future,
    mem::ManuallyDrop,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::{Context as TaskContext, Poll, RawWaker, RawWakerVTable, Waker},
    thread::{self, ThreadId},
    time::Duration,
};

use common::{DestructorBlocker, DestructorGate, POLL_TIMEOUT};
use shelterwood::{Actor, ActorOnceDef, Context, ExitError, ExitResult, Tree};

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
