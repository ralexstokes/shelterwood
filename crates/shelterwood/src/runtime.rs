//! The only boundary between the library and its async runtime.

use std::{
    future::Future,
    ops::RangeBounds,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{Notify, mpsc, oneshot},
    task, time,
};

#[cfg(test)]
pub(crate) use tokio::test;

pub(crate) fn now() -> std::time::Instant {
    time::Instant::now().into_std()
}

pub(crate) fn is_available() -> bool {
    tokio::runtime::Handle::try_current().is_ok()
}

/// A one-shot, multi-waiter signal backed by Tokio's waiter queue.
///
/// The atomic provides a linearizable, idempotent transition and retains the
/// fired state for future waiters. [`Notify`] wakes the waiters that already
/// exist and removes cancelled waits from its intrusive queue. Creating the
/// notification before rechecking the atomic is essential: Tokio guarantees
/// that such a notification observes a subsequent `notify_waiters`, even when
/// it has not been polled yet.
///
/// This deliberately does not use `tokio_util::sync::CancellationToken`.
/// Shelterwood also uses latches for readiness and completion, needs `fire` to
/// report which caller performed the transition, and keeps parent and local
/// cancellation as distinct shared latches. The Tokio-util cancellation tree
/// would add allocation, locking, and dependencies without replacing those
/// semantics. A Tokio watch channel similarly adds value locking to the hot
/// `is_fired` path.
#[derive(Clone, Debug, Default)]
pub(crate) struct Latch {
    state: Arc<LatchState>,
}

#[derive(Debug, Default)]
struct LatchState {
    fired: AtomicBool,
    notify: Notify,
}

impl Latch {
    pub(crate) fn fire(&self) -> bool {
        if self
            .state
            .fired
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.state.notify.notify_waiters();
            true
        } else {
            false
        }
    }

    pub(crate) fn is_fired(&self) -> bool {
        self.state.fired.load(Ordering::Acquire)
    }

    pub(crate) async fn fired(&self) {
        if self.is_fired() {
            return;
        }

        let notified = self.state.notify.notified();
        if self.is_fired() {
            return;
        }
        notified.await;
        debug_assert!(self.is_fired());
    }
}

/// Sending half of a runtime-backed single-delivery channel.
pub(crate) struct OneShotSender<T>(oneshot::Sender<T>);

/// Receiving half of a runtime-backed single-delivery channel.
pub(crate) struct OneShotReceiver<T>(oneshot::Receiver<T>);

pub(crate) fn oneshot<T>() -> (OneShotSender<T>, OneShotReceiver<T>) {
    let (sender, receiver) = oneshot::channel();
    (OneShotSender(sender), OneShotReceiver(receiver))
}

impl<T> OneShotSender<T> {
    pub(crate) fn send(self, value: T) -> Result<(), T> {
        self.0.send(value)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

impl<T> OneShotReceiver<T> {
    pub(crate) async fn receive(self) -> Option<T> {
        self.0.await.ok()
    }

    #[cfg(test)]
    pub(crate) fn try_receive(&mut self) -> Option<T> {
        self.0.try_recv().ok()
    }
}

/// A spawned operation whose identity is retained through joining.
pub(crate) struct JoinHandle<I, T> {
    id: I,
    inner: task::JoinHandle<T>,
}

#[derive(Clone)]
pub(crate) struct AbortHandle(task::AbortHandle);

impl AbortHandle {
    pub(crate) fn abort(&self) {
        self.0.abort();
    }
}

impl<I, T> JoinHandle<I, T> {
    pub(crate) fn abort_handle(&self) -> AbortHandle {
        AbortHandle(self.inner.abort_handle())
    }
}

pub(crate) enum Either<L, R> {
    Left(L),
    Right(R),
}

pub(crate) async fn select_two<A, B>(left: A, right: B) -> Either<A::Output, B::Output>
where
    A: Future + Send,
    B: Future + Send,
{
    tokio::pin!(left);
    tokio::pin!(right);
    tokio::select! {
        biased;
        value = &mut left => Either::Left(value),
        value = &mut right => Either::Right(value),
    }
}

/// The runtime-level outcome consumed by the exit classifier.
pub(crate) enum JoinOutcome<T> {
    Ok { value: T },
    Panic { message: Option<String> },
    Cancelled,
}

pub(crate) fn spawn<I, F>(id: I, future: F) -> JoinHandle<I, F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    JoinHandle {
        id,
        inner: task::spawn(future),
    }
}

pub(crate) fn spawn_blocking<I, F, T>(id: I, operation: F) -> JoinHandle<I, T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    JoinHandle {
        id,
        inner: task::spawn_blocking(operation),
    }
}

pub(crate) async fn join<I, T>(handle: JoinHandle<I, T>) -> JoinOutcome<T> {
    let JoinHandle { inner, .. } = handle;
    match inner.await {
        Ok(value) => JoinOutcome::Ok { value },
        Err(error) if error.is_panic() => JoinOutcome::Panic {
            message: panic_message(error.into_panic()),
        },
        Err(error) => {
            debug_assert!(error.is_cancelled());
            JoinOutcome::Cancelled
        }
    }
}

pub(crate) async fn join_resuming<I, T>(handle: JoinHandle<I, T>) -> (I, T) {
    let JoinHandle { id, inner } = handle;
    match inner.await {
        Ok(value) => (id, value),
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => {
            debug_assert!(error.is_cancelled());
            panic!("library-owned operation task was unexpectedly cancelled")
        }
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send + 'static>) -> Option<String> {
    if let Some(message) = payload.downcast_ref::<&str>() {
        Some((*message).to_owned())
    } else {
        payload.downcast_ref::<String>().cloned()
    }
}

pub(crate) async fn sleep(duration: Duration) {
    time::sleep(duration).await;
}

#[cfg(test)]
pub(crate) async fn yield_now() {
    task::yield_now().await;
}

pub(crate) async fn sleep_until_std(deadline: std::time::Instant) {
    time::sleep_until(time::Instant::from_std(deadline)).await;
}

pub(crate) enum Timeout<T> {
    Completed(T),
    Elapsed,
}

pub(crate) async fn timeout<F>(duration: Duration, future: F) -> Timeout<F::Output>
where
    F: Future,
{
    match time::timeout(duration, future).await {
        Ok(value) => Timeout::Completed(value),
        Err(_) => Timeout::Elapsed,
    }
}

pub(crate) type MpscSender<T> = mpsc::Sender<T>;
pub(crate) type MpscReceiver<T> = mpsc::Receiver<T>;

pub(crate) fn bounded_mpsc<T>(capacity: usize) -> (MpscSender<T>, MpscReceiver<T>) {
    mpsc::channel(capacity)
}

pub(crate) async fn mpsc_send<T>(sender: &MpscSender<T>, value: T) -> Result<(), T> {
    sender.send(value).await.map_err(|error| error.0)
}

pub(crate) fn mpsc_try_recv<T>(receiver: &mut MpscReceiver<T>) -> Option<T> {
    receiver.try_recv().ok()
}

pub(crate) enum ScopeWake<T> {
    Signal,
    ParentShutdown,
    Message(Option<T>),
    Deadline,
}

pub(crate) async fn wait_scope<S, C, T>(
    signal: S,
    parent_shutdown: C,
    receiver: &mut MpscReceiver<T>,
    deadline: Option<std::time::Instant>,
) -> ScopeWake<T>
where
    S: Future<Output = ()> + Send,
    C: Future<Output = ()> + Send,
{
    tokio::pin!(signal);
    tokio::pin!(parent_shutdown);
    let deadline = async move {
        if let Some(deadline) = deadline {
            sleep_until_std(deadline).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(deadline);
    tokio::select! {
        biased;
        () = &mut signal => ScopeWake::Signal,
        () = &mut parent_shutdown => ScopeWake::ParentShutdown,
        message = receiver.recv() => ScopeWake::Message(message),
        () = &mut deadline => ScopeWake::Deadline,
    }
}

#[derive(Debug)]
pub(crate) struct JitterRng(fastrand::Rng);

impl JitterRng {
    pub(crate) fn from_system_entropy() -> Self {
        Self(fastrand::Rng::with_seed(fastrand::u64(..)))
    }

    pub(crate) fn sample<R>(&mut self, range: R) -> u64
    where
        R: RangeBounds<u64>,
    {
        self.0.u64(range)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Poll,
        time::Duration,
    };

    use super::{JoinOutcome, Latch, Timeout, join, spawn, timeout, yield_now};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn exactly_one_concurrent_fire_performs_the_transition() {
        const FIRERS: usize = 32;

        let latch = Latch::default();
        let ready = Arc::new(AtomicUsize::new(0));
        let mut firers = Vec::with_capacity(FIRERS);
        for _ in 0..FIRERS {
            let latch = latch.clone();
            let ready = Arc::clone(&ready);
            firers.push(spawn((), async move {
                ready.fetch_add(1, Ordering::AcqRel);
                while ready.load(Ordering::Acquire) != FIRERS {
                    yield_now().await;
                }
                latch.fire()
            }));
        }

        let mut transitions = 0;
        for firer in firers {
            let JoinOutcome::Ok { value } = join(firer).await else {
                panic!("latch firer must complete normally");
            };
            transitions += usize::from(value);
        }

        assert_eq!(transitions, 1);
        assert!(latch.is_fired());
        assert!(!latch.fire());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn all_pre_fire_waiters_wake() {
        const WAITERS: usize = 32;

        let latch = Latch::default();
        let parked = Arc::new(AtomicUsize::new(0));
        let mut waiters = Vec::with_capacity(WAITERS);
        for _ in 0..WAITERS {
            let latch = latch.clone();
            let parked = Arc::clone(&parked);
            waiters.push(spawn((), async move {
                let mut fired = Box::pin(latch.fired());
                let first_poll =
                    std::future::poll_fn(|context| Poll::Ready(fired.as_mut().poll(context))).await;
                assert!(first_poll.is_pending());
                parked.fetch_add(1, Ordering::Release);
                fired.await;
            }));
        }

        while parked.load(Ordering::Acquire) != WAITERS {
            yield_now().await;
        }
        assert!(latch.fire());

        for waiter in waiters {
            let result = timeout(Duration::from_secs(1), join(waiter)).await;
            assert!(matches!(
                result,
                Timeout::Completed(JoinOutcome::Ok { value: () })
            ));
        }
    }

    #[tokio::test]
    async fn post_fire_waiters_complete_immediately() {
        let latch = Latch::default();
        assert!(latch.fire());

        assert!(matches!(
            timeout(Duration::from_secs(1), latch.fired()).await,
            Timeout::Completed(())
        ));
    }

    #[tokio::test]
    async fn cancelled_waits_do_not_consume_the_signal() {
        let latch = Latch::default();

        for _ in 0..1_024 {
            let mut fired = Box::pin(latch.fired());
            let first_poll =
                std::future::poll_fn(|context| Poll::Ready(fired.as_mut().poll(context))).await;
            assert!(first_poll.is_pending());
            drop(fired);
        }

        let mut live_waiter = Box::pin(latch.fired());
        let first_poll =
            std::future::poll_fn(|context| Poll::Ready(live_waiter.as_mut().poll(context))).await;
        assert!(first_poll.is_pending());
        assert!(latch.fire());
        assert!(matches!(
            timeout(Duration::from_secs(1), live_waiter).await,
            Timeout::Completed(())
        ));
    }
}
