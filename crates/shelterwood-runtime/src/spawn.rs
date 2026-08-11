use std::{any::Any, future::Future, ops::RangeBounds, panic::resume_unwind};

use tokio::{sync::mpsc, task};

use shelterwood_core::exit::JoinVerdict as JoinOutcome;

use super::{PanicPayload, catch_panic, discard_panic, sleep_until_std};

/// Counts the runtime's currently alive spawned tasks, keeping runtime
/// metrics access in this module.
#[cfg(any(test, feature = "test-util"))]
pub fn alive_task_count() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

pub fn is_available() -> bool {
    tokio::runtime::Handle::try_current().is_ok()
}

pub struct ActorWork {
    handle: Option<JoinHandle<()>>,
    abort: AbortHandle,
}

impl ActorWork {
    pub fn abort(&self) {
        self.abort.abort();
    }

    pub async fn join(mut self) -> JoinOutcome<()> {
        let Some(handle) = self.handle.take() else {
            return JoinOutcome::Cancelled;
        };
        join(handle).await
    }
}

impl Drop for ActorWork {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

pub fn spawn_actor_work(future: impl Future<Output = ()> + Send + 'static) -> ActorWork {
    let handle = spawn(future);
    let abort = handle.abort_handle();
    ActorWork {
        handle: Some(handle),
        abort,
    }
}

pub fn spawn_blocking_work<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
) -> impl Future<Output = T> + Send {
    let handle = spawn_blocking(operation);
    async move { join_resuming(handle).await }
}

/// A spawned operation owned by the library.
pub struct JoinHandle<T> {
    inner: task::JoinHandle<T>,
}

#[derive(Clone)]
pub struct AbortHandle(task::AbortHandle);

impl AbortHandle {
    pub fn abort(&self) {
        self.0.abort();
    }
}

impl<T> JoinHandle<T> {
    pub fn abort_handle(&self) -> AbortHandle {
        AbortHandle(self.inner.abort_handle())
    }
}

pub enum Either<L, R> {
    Left(L),
    Right(R),
}

pub async fn select_two<A, B>(left: A, right: B) -> Either<A::Output, B::Output>
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

pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    JoinHandle {
        inner: task::spawn(future),
    }
}

pub fn spawn_blocking<F, T>(operation: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    JoinHandle {
        inner: task::spawn_blocking(operation),
    }
}

pub async fn join<T>(handle: JoinHandle<T>) -> JoinOutcome<T> {
    let JoinHandle { inner } = handle;
    match inner.await {
        Ok(value) => JoinOutcome::Ok { value },
        Err(error) if error.is_panic() => JoinOutcome::Panic {
            message: contain_panic_payload(error.into_panic()),
        },
        Err(error) => {
            debug_assert!(error.is_cancelled());
            JoinOutcome::Cancelled
        }
    }
}

pub async fn join_resuming<T>(handle: JoinHandle<T>) -> T {
    let JoinHandle { inner } = handle;
    match inner.await {
        Ok(value) => value,
        Err(error) if error.is_panic() => resume_unwind(error.into_panic()),
        Err(error) => {
            debug_assert!(error.is_cancelled());
            panic!("library-owned operation task was unexpectedly cancelled")
        }
    }
}

pub(super) fn contain_panic_payload(payload: PanicPayload) -> Option<String> {
    let message = match catch_panic(|| panic_message(payload.as_ref())) {
        Ok(message) => message,
        Err(inspection_panic) => {
            discard_panic(Some(inspection_panic));
            None
        }
    };
    // A custom panic payload is user-owned too. Its destructor may panic, so
    // discard it under a fresh unwind boundary before publishing completion.
    discard_panic(Some(payload));
    message
}

fn panic_message(payload: &(dyn Any + Send + 'static)) -> Option<String> {
    if let Some(message) = payload.downcast_ref::<&str>() {
        Some((*message).to_owned())
    } else {
        payload.downcast_ref::<String>().cloned()
    }
}

pub async fn yield_now() {
    task::yield_now().await;
}

pub type UnboundedMpscSender<T> = mpsc::UnboundedSender<T>;
pub type UnboundedMpscReceiver<T> = mpsc::UnboundedReceiver<T>;

pub fn unbounded_mpsc<T>() -> (UnboundedMpscSender<T>, UnboundedMpscReceiver<T>) {
    mpsc::unbounded_channel()
}

pub fn unbounded_mpsc_send<T>(sender: &UnboundedMpscSender<T>, value: T) -> Result<(), T> {
    sender.send(value).map_err(|error| error.0)
}

pub fn unbounded_mpsc_try_recv<T>(receiver: &mut UnboundedMpscReceiver<T>) -> Option<T> {
    receiver.try_recv().ok()
}

pub enum ScopeWake<T> {
    Signal,
    ParentShutdown,
    Message(Option<T>),
    ControlMessage(Option<T>),
    Deadline,
}

pub struct ScopeWait<S, C> {
    pub signal: S,
    pub parent_shutdown: C,
}

pub async fn wait_scope<S, C, T>(
    wait: ScopeWait<S, C>,
    receiver: &mut UnboundedMpscReceiver<T>,
    control_receiver: Option<&mut UnboundedMpscReceiver<T>>,
    deadline: Option<std::time::Instant>,
) -> ScopeWake<T>
where
    S: Future<Output = ()> + Send,
    C: Future<Output = ()> + Send,
{
    let ScopeWait {
        signal,
        parent_shutdown,
    } = wait;
    tokio::pin!(signal);
    tokio::pin!(parent_shutdown);
    let deadline = async move {
        if let Some(deadline) = deadline {
            sleep_until_std(deadline).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    let control_message = async move {
        if let Some(receiver) = control_receiver {
            receiver.recv().await
        } else {
            std::future::pending().await
        }
    };
    tokio::pin!(deadline);
    tokio::pin!(control_message);
    tokio::select! {
        biased;
        () = &mut signal => ScopeWake::Signal,
        () = &mut parent_shutdown => ScopeWake::ParentShutdown,
        message = receiver.recv() => ScopeWake::Message(message),
        message = &mut control_message => ScopeWake::ControlMessage(message),
        () = &mut deadline => ScopeWake::Deadline,
    }
}

#[derive(Debug)]
pub struct JitterRng(fastrand::Rng);

impl JitterRng {
    pub fn new() -> Self {
        // `fastrand::Rng::new` seeds from the thread-local generator, falling
        // back to a fixed seed if that is unavailable (during TLS teardown).
        // Restart jitter would then be deterministic and correlated across
        // scopes -- degraded spread, never a correctness break.
        Self(fastrand::Rng::new())
    }

    pub fn sample<R>(&mut self, range: R) -> u64
    where
        R: RangeBounds<u64>,
    {
        self.0.u64(range)
    }
}

impl Default for JitterRng {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn scope_wait_prefers_signal_when_both_control_futures_are_ready() {
        let (_sender, mut receiver) = super::unbounded_mpsc::<()>();

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::ready(()),
                parent_shutdown: std::future::ready(()),
            },
            &mut receiver,
            None,
            None,
        )
        .await;

        assert!(matches!(wake, super::ScopeWake::Signal));
    }

    #[tokio::test]
    async fn scope_wait_prefers_a_primary_event_over_a_control_backlog() {
        let (sender, mut receiver) = super::unbounded_mpsc();
        let (control_sender, mut control_receiver) = super::unbounded_mpsc();
        for value in 0..128 {
            assert!(super::unbounded_mpsc_send(&control_sender, value).is_ok());
        }
        assert!(super::unbounded_mpsc_send(&sender, 999).is_ok());

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::pending(),
            },
            &mut receiver,
            Some(&mut control_receiver),
            None,
        )
        .await;

        assert!(matches!(wake, super::ScopeWake::Message(Some(999))));
        assert_eq!(control_receiver.try_recv(), Ok(0));
    }
}
