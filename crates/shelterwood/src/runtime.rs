//! The only boundary between the library and its async runtime.

use std::{future::Future, ops::RangeBounds, time::Duration};

use tokio::{
    sync::{mpsc, oneshot, watch},
    task, time,
};

/// A runtime-owned monotonic instant.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Instant(time::Instant);

impl Instant {
    pub(crate) fn now() -> Self {
        Self(time::Instant::now())
    }

    pub(crate) fn checked_add(self, duration: Duration) -> Option<Self> {
        self.0.checked_add(duration).map(Self)
    }

    pub(crate) fn saturating_duration_since(self, earlier: Self) -> Duration {
        self.0.saturating_duration_since(earlier.0)
    }
}

pub(crate) fn now() -> std::time::Instant {
    time::Instant::now().into_std()
}

pub(crate) fn jitter_sample() -> f64 {
    fastrand::f64()
}

pub(crate) fn is_available() -> bool {
    tokio::runtime::Handle::try_current().is_ok()
}

#[derive(Clone)]
pub(crate) struct RuntimeSpawner(tokio::runtime::Handle);

impl RuntimeSpawner {
    pub(crate) fn current() -> Option<Self> {
        tokio::runtime::Handle::try_current().ok().map(Self)
    }

    pub(crate) fn spawn<I, F>(&self, id: I, future: F) -> JoinHandle<I, F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        JoinHandle {
            id,
            inner: self.0.spawn(future),
        }
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
    pub(crate) fn id(&self) -> &I {
        &self.id
    }

    pub(crate) fn abort(&self) {
        self.inner.abort();
    }

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
pub(crate) enum JoinOutcome<I, T> {
    Ok { id: I, value: T },
    Panic { id: I, message: Option<String> },
    Cancelled { id: I },
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

pub(crate) async fn join<I, T>(handle: JoinHandle<I, T>) -> JoinOutcome<I, T> {
    let JoinHandle { id, inner } = handle;
    match inner.await {
        Ok(value) => JoinOutcome::Ok { id, value },
        Err(error) if error.is_panic() => JoinOutcome::Panic {
            id,
            message: panic_message(error.into_panic()),
        },
        Err(error) => {
            debug_assert!(error.is_cancelled());
            JoinOutcome::Cancelled { id }
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

pub(crate) async fn sleep_until(deadline: Instant) {
    time::sleep_until(deadline.0).await;
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

pub(crate) type WatchSender<T> = watch::Sender<T>;
pub(crate) type WatchReceiver<T> = watch::Receiver<T>;

pub(crate) fn watch_channel<T>(initial: T) -> (WatchSender<T>, WatchReceiver<T>) {
    watch::channel(initial)
}

pub(crate) type OneshotSender<T> = oneshot::Sender<T>;
pub(crate) type OneshotReceiver<T> = oneshot::Receiver<T>;

pub(crate) fn oneshot_channel<T>() -> (OneshotSender<T>, OneshotReceiver<T>) {
    oneshot::channel()
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

#[derive(Clone, Debug, Default)]
pub(crate) struct CancellationToken(tokio_util::sync::CancellationToken);

impl CancellationToken {
    pub(crate) fn new() -> Self {
        Self(tokio_util::sync::CancellationToken::new())
    }

    pub(crate) fn child_token(&self) -> Self {
        Self(self.0.child_token())
    }

    pub(crate) fn cancel(&self) {
        self.0.cancel();
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    pub(crate) async fn cancelled(&self) {
        self.0.cancelled().await;
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

pub(crate) async fn yield_now() {
    task::yield_now().await;
}
