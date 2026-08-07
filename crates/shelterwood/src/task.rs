//! Supervised task definitions, contexts, and handles.

use std::{
    fmt,
    future::Future,
    hash::{Hash, Hasher},
    pin::Pin,
    sync::{Arc, Mutex},
};

use crate::{
    ChildId, Exit, ExitError, ExitResult, Incarnation, Membership, PolicyError, Readiness,
    ReadinessDeadline, RestartPolicy, Retention, Shutdown,
    driver::{Latch, MemberCell, MemberStage, Signal},
    policy::CommonOptions,
};

pub(crate) type TaskFuture = Pin<Box<dyn Future<Output = ExitResult> + Send + 'static>>;
pub(crate) type TaskFactory = Arc<Mutex<Box<dyn Fn(TaskContext) -> TaskFuture + Send + 'static>>>;

/// A library-owned cancellation token.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    primary: Latch,
    secondary: Option<Latch>,
}

impl CancellationToken {
    pub(crate) fn from_latch(latch: Latch) -> Self {
        Self {
            primary: latch,
            secondary: None,
        }
    }

    pub(crate) fn from_latches(primary: Latch, secondary: Latch) -> Self {
        Self {
            primary,
            secondary: Some(secondary),
        }
    }

    pub(crate) fn child(&self, cancellation: Latch) -> Self {
        debug_assert!(self.secondary.is_none());
        Self::from_latches(self.primary.clone(), cancellation)
    }

    /// Reports whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.primary.is_fired() || self.secondary.as_ref().is_some_and(Latch::is_fired)
    }

    /// Waits until cancellation is requested.
    pub async fn cancelled(&self) {
        if let Some(secondary) = &self.secondary {
            let _ = crate::driver::select(self.primary.fired(), secondary.fired()).await;
        } else {
            self.primary.fired().await;
        }
    }
}

/// Per-incarnation capabilities supplied to a supervised task.
#[derive(Clone, Debug)]
pub struct TaskContext {
    id: ChildId,
    incarnation: Incarnation,
    shutdown: CancellationToken,
    abort: CancellationToken,
    ready: Latch,
}

impl TaskContext {
    pub(crate) fn new(
        id: ChildId,
        incarnation: Incarnation,
        shutdown: Latch,
        abort: Latch,
        ready: Latch,
    ) -> Self {
        Self {
            id,
            incarnation,
            shutdown: CancellationToken::from_latch(shutdown),
            abort: CancellationToken::from_latch(abort),
            ready,
        }
    }

    /// Returns this task's child id.
    #[must_use]
    pub fn id(&self) -> &ChildId {
        &self.id
    }

    /// Returns this task's current incarnation.
    #[must_use]
    pub fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    /// Returns the cooperative shutdown token.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Returns the escalation token.
    #[must_use]
    pub fn abort_token(&self) -> CancellationToken {
        self.abort.clone()
    }

    /// Releases manual readiness. Repeated calls are no-ops.
    pub fn mark_ready(&self) {
        self.ready.fire();
    }
}

/// A repeatable supervised-task definition.
pub struct TaskDef {
    pub(crate) factory: TaskFactory,
    pub(crate) options: CommonOptions,
}

impl fmt::Debug for TaskDef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskDef")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl TaskDef {
    /// Creates a restartable task definition from a repeatable body factory.
    pub fn new<F, Fut>(factory: F) -> Self
    where
        F: Fn(TaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = ExitResult> + Send + 'static,
    {
        Self {
            factory: Arc::new(Mutex::new(Box::new(move |context| {
                Box::pin(factory(context))
            }))),
            options: CommonOptions::default(),
        }
    }

    /// Overrides the restart policy.
    #[must_use]
    pub fn restart(mut self, restart: RestartPolicy) -> Self {
        self.options.restart = Some(restart);
        self
    }

    /// Overrides the shutdown policy.
    #[must_use]
    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.options.shutdown = Some(shutdown);
        self
    }

    /// Overrides task readiness (`Immediate` or `Manual`).
    pub fn readiness(mut self, readiness: Readiness) -> Result<Self, PolicyError> {
        if readiness == Readiness::AfterInit {
            return Err(PolicyError::UnsupportedReadiness);
        }
        self.options.readiness = Some(readiness);
        Ok(self)
    }

    /// Overrides the readiness deadline.
    #[must_use]
    pub fn readiness_deadline(mut self, deadline: ReadinessDeadline) -> Self {
        self.options.readiness_deadline = deadline;
        self
    }

    /// Overrides terminal-membership retention.
    #[must_use]
    pub fn retention(mut self, retention: Retention) -> Self {
        self.options.retention = Some(retention);
        self
    }
}

type OnceTaskFuture<T> = Pin<Box<dyn Future<Output = Result<T, ExitError>> + Send + 'static>>;

/// A consuming one-shot task definition with a typed completion value.
pub struct TaskOnceDef<T> {
    body: Box<dyn FnOnce(TaskContext) -> OnceTaskFuture<T> + Send + 'static>,
    pub(crate) options: CommonOptions,
}

impl<T> fmt::Debug for TaskOnceDef<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskOnceDef")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl<T: Send + 'static> TaskOnceDef<T> {
    /// Creates a one-shot task definition from a consuming body.
    pub fn new<F, Fut>(body: F) -> Self
    where
        F: FnOnce(TaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, ExitError>> + Send + 'static,
    {
        Self {
            body: Box::new(move |context| Box::pin(body(context))),
            options: CommonOptions::default(),
        }
    }

    /// Overrides the shutdown policy.
    #[must_use]
    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.options.shutdown = Some(shutdown);
        self
    }

    /// Overrides task readiness (`Immediate` or `Manual`).
    pub fn readiness(mut self, readiness: Readiness) -> Result<Self, PolicyError> {
        if readiness == Readiness::AfterInit {
            return Err(PolicyError::UnsupportedReadiness);
        }
        self.options.readiness = Some(readiness);
        Ok(self)
    }

    /// Overrides the readiness deadline.
    #[must_use]
    pub fn readiness_deadline(mut self, deadline: ReadinessDeadline) -> Self {
        self.options.readiness_deadline = deadline;
        self
    }

    /// Overrides terminal-membership retention.
    #[must_use]
    pub fn retention(mut self, retention: Retention) -> Self {
        self.options.retention = Some(retention);
        self
    }

    pub(crate) fn erase(self, completion: Arc<Completion<T>>) -> OnceTask {
        let body = self.body;
        OnceTask {
            body: OnceTaskBody::Available(Box::new(move |context| {
                Box::pin(async move {
                    match body(context).await {
                        Ok(value) => {
                            completion.complete(value);
                            Ok(())
                        }
                        Err(error) => Err(error),
                    }
                })
            })),
            options: self.options,
        }
    }
}

pub(crate) struct OnceTask {
    pub(crate) body: OnceTaskBody,
    pub(crate) options: CommonOptions,
}

pub(crate) enum OnceTaskBody {
    Available(Box<dyn FnOnce(TaskContext) -> TaskFuture + Send + 'static>),
    Spent,
}

/// A cheap membership-addressed task handle.
#[derive(Clone)]
pub struct TaskRef {
    pub(crate) cell: Arc<MemberCell>,
}

impl TaskRef {
    pub(crate) fn new(cell: Arc<MemberCell>) -> Self {
        Self { cell }
    }

    /// Returns the task's id.
    #[must_use]
    pub fn id(&self) -> &ChildId {
        self.cell.id()
    }

    /// Returns the task membership identity.
    #[must_use]
    pub fn membership(&self) -> Membership {
        self.cell.membership()
    }

    /// Waits for membership terminality, riding through restarts.
    pub async fn wait(&self) -> Exit {
        self.cell.wait_terminal().await
    }
}

impl fmt::Debug for TaskRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskRef")
            .field("id", &self.id())
            .field("membership", &self.membership())
            .finish()
    }
}

impl PartialEq for TaskRef {
    fn eq(&self, other: &Self) -> bool {
        self.membership() == other.membership()
    }
}

impl Eq for TaskRef {}

impl Hash for TaskRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.membership().hash(state);
    }
}

enum CompletionState<T> {
    Pending,
    Completed(T),
    Discarded,
}

pub(crate) struct Completion<T> {
    state: Mutex<CompletionState<T>>,
    changed: Signal,
}

impl<T> Completion<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(CompletionState::Pending),
            changed: Signal::default(),
        })
    }

    pub(crate) fn signal(&self) -> Signal {
        self.changed.clone()
    }

    fn complete(&self, value: T) {
        let mut state = self.state.lock().expect("completion mutex poisoned");
        if matches!(*state, CompletionState::Pending) {
            *state = CompletionState::Completed(value);
            drop(state);
            self.changed.pulse();
        }
    }
}

/// The sole claim to a one-shot task's typed completion value.
#[must_use]
pub struct OneShotTaskRef<T> {
    completion: Arc<Completion<T>>,
    task: TaskRef,
}

impl<T> fmt::Debug for OneShotTaskRef<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OneShotTaskRef")
            .field("task", &self.task)
            .finish_non_exhaustive()
    }
}

impl<T> OneShotTaskRef<T> {
    pub(crate) fn new(completion: Arc<Completion<T>>, task: TaskRef) -> Self {
        Self { completion, task }
    }

    /// Consumes the claim and waits for the typed value or terminal exit.
    pub async fn wait(self) -> Result<T, Exit> {
        let mut watcher = self.completion.changed.watcher();
        loop {
            {
                let mut state = self
                    .completion
                    .state
                    .lock()
                    .expect("completion mutex poisoned");
                if matches!(*state, CompletionState::Completed(_)) {
                    let CompletionState::Completed(value) =
                        std::mem::replace(&mut *state, CompletionState::Discarded)
                    else {
                        unreachable!()
                    };
                    return Ok(value);
                }
            }
            if let MemberStage::Terminal(exit) = self.task.cell.record().stage {
                return Err(exit);
            }
            watcher.changed().await;
        }
    }
}

impl<T> Drop for OneShotTaskRef<T> {
    fn drop(&mut self) {
        let mut state = self
            .completion
            .state
            .lock()
            .expect("completion mutex poisoned");
        if matches!(*state, CompletionState::Completed(_)) {
            *state = CompletionState::Discarded;
        }
    }
}
