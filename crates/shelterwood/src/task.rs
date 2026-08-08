//! Supervised task definitions, contexts, and handles.

use std::{
    convert::Infallible,
    fmt,
    future::Future,
    hash::{Hash, Hasher},
    pin::Pin,
    sync::Arc,
};

use crate::{
    ChildId, Exit, ExitError, ExitResult, Incarnation, Membership, PolicyError, Readiness,
    ReadinessDeadline, RestartPolicy, Retention, Shutdown,
    cells::MemberCell,
    definition::DefinitionSource,
    policy::CommonOptions,
    runtime::{self, Latch},
};

pub(crate) type TaskFuture = Pin<Box<dyn Future<Output = ExitResult> + Send + 'static>>;
pub(crate) type TaskFactory = Arc<dyn Fn(TaskContext) -> TaskFuture + Send + Sync + 'static>;

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
            let _ = runtime::select_two(self.primary.fired(), secondary.fired()).await;
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
    ///
    /// Once cooperative shutdown or escalation has begun this incarnation is
    /// already stopping, so readiness can no longer be published and the call
    /// is a no-op as well.
    pub fn mark_ready(&self) {
        if !self.is_stopping() {
            self.ready.fire();
        }
    }

    pub(crate) fn is_stopping(&self) -> bool {
        self.shutdown.is_cancelled() || self.abort.is_cancelled()
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
        F: Fn(TaskContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ExitResult> + Send + 'static,
    {
        Self {
            factory: Arc::new(move |context| Box::pin(factory(context))),
            options: CommonOptions::default(),
        }
    }

    common_options_setters!(
        restart,
        shutdown,
        task_readiness,
        task_readiness_deadline,
        retention,
    );
}

type OnceTaskFuture<T> = Pin<Box<dyn Future<Output = Result<T, ExitError>> + Send + 'static>>;

/// A consuming one-shot task definition with a typed completion value.
///
/// “One-shot” means exactly one incarnation because the owned body cannot be
/// invoked again; the task may perform arbitrarily many iterations internally.
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

    common_options_setters!(shutdown, task_readiness, task_readiness_deadline, retention,);

    pub(crate) fn erase(self, completion: runtime::OneShotSender<T>) -> OnceTask {
        let body = self.body;
        OnceTask {
            source: DefinitionSource::OneShot(Box::new(move |context| {
                Box::pin(async move {
                    match body(context).await {
                        Ok(value) => {
                            let _ = completion.send(value);
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
    source: DefinitionSource<Infallible, OnceTaskFactory>,
    pub(crate) options: CommonOptions,
}

type OnceTaskFactory = Box<dyn FnOnce(TaskContext) -> TaskFuture + Send + 'static>;

impl OnceTask {
    pub(crate) fn take_body(&mut self) -> OnceTaskFactory {
        self.source
            .take_one_shot()
            .expect("one-shot task construction invoked more than once")
    }
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

// Handle identity is the slot cell, not the membership token: lowering a
// rebuilt nested declaration rebases the token behind live pre-spawn handles,
// and a token-value hash would strand entries keyed before the rebase.
impl PartialEq for TaskRef {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.cell, &other.cell)
    }
}

impl Eq for TaskRef {}

impl Hash for TaskRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.cell).hash(state);
    }
}

/// The sole claim to a one-shot task's typed completion value.
#[must_use]
pub struct OneShotTaskRef<T> {
    completion: runtime::OneShotReceiver<T>,
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
    pub(crate) fn new(completion: runtime::OneShotReceiver<T>, task: TaskRef) -> Self {
        Self { completion, task }
    }

    /// Consumes the claim and waits for the authoritative terminal verdict.
    ///
    /// The typed value is released only when terminal publication classifies
    /// the membership as [`crate::ExitKind::Completed`]. Any competing
    /// failure, panic, abort, readiness timeout, or never-started verdict wins
    /// even if the task body produced a value first.
    pub async fn wait(self) -> Result<T, Exit> {
        let Self { completion, task } = self;
        let exit = task.wait().await;
        if !matches!(exit.kind(), crate::ExitKind::Completed) {
            return Err(exit);
        }
        // The erased one-shot body sends before returning success, and this
        // receiver is the sole completion claim. A published Completed verdict
        // with a closed, empty channel is therefore a framework invariant
        // violation, not an application exit that can be reported as `Err`.
        Ok(completion
            .receive()
            .await
            .expect("completed one-shot task must publish its typed value"))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use crate::{
        ChildId, Exit, ExitKind,
        cells::MemberCell,
        identity::ScopeIdentity,
        runtime::{self, JoinOutcome, Latch, Timeout},
    };

    use super::{CancellationToken, OneShotTaskRef, TaskContext, TaskRef};

    fn task_context() -> (TaskContext, Latch, Latch, Latch) {
        let id = ChildId::from("task");
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let membership = identity.mint_membership(&id).expect("membership available");
        let mut incarnations = identity.incarnation_counter(membership);
        let incarnation = ScopeIdentity::mint_incarnation(membership, &mut incarnations)
            .expect("incarnation available");
        let shutdown = Latch::default();
        let abort = Latch::default();
        let ready = Latch::default();
        let context = TaskContext::new(
            id,
            incarnation,
            shutdown.clone(),
            abort.clone(),
            ready.clone(),
        );
        (context, shutdown, abort, ready)
    }

    #[test]
    fn live_task_context_can_publish_readiness() {
        let (context, shutdown, abort, ready) = task_context();

        assert!(!context.is_stopping());
        context.mark_ready();

        assert!(ready.is_fired());
        assert!(!shutdown.is_fired());
        assert!(!abort.is_fired());
    }

    #[test]
    fn shutdown_first_prevents_task_readiness_publication() {
        let (context, shutdown, abort, ready) = task_context();

        assert!(shutdown.fire());
        assert!(context.is_stopping());
        context.mark_ready();

        assert!(!ready.is_fired());
        assert!(!abort.is_fired());
    }

    #[test]
    fn abort_first_prevents_task_readiness_publication() {
        let (context, shutdown, abort, ready) = task_context();

        assert!(abort.fire());
        assert!(context.is_stopping());
        context.mark_ready();

        assert!(!ready.is_fired());
        assert!(!shutdown.is_fired());
    }

    fn one_shot_claim<T>() -> (
        runtime::OneShotSender<T>,
        OneShotTaskRef<T>,
        std::sync::Arc<MemberCell>,
    ) {
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let id = ChildId::from("task");
        let membership = identity.mint_membership(&id).expect("membership available");
        let member = MemberCell::new(id, membership);
        let (sender, receiver) = runtime::oneshot();
        let claim = OneShotTaskRef::new(receiver, TaskRef::new(std::sync::Arc::clone(&member)));
        (sender, claim, member)
    }

    #[crate::runtime::test]
    async fn typed_value_waits_for_completed_terminal_publication() {
        let (sender, claim, member) = one_shot_claim();
        sender.send(42_u8).expect("claim remains open");
        let mut waiting = Box::pin(claim.wait());
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
        member.terminalize(Exit::new(ExitKind::Completed, false));
        assert_eq!(waiting.await, Ok(42));
    }

    #[crate::runtime::test]
    async fn non_completed_terminal_publication_hides_a_queued_typed_value() {
        let (sender, claim, member) = one_shot_claim();
        sender.send(42_u8).expect("claim remains open");
        let mut waiting = Box::pin(claim.wait());
        let mut context = Context::from_waker(Waker::noop());
        let exit = Exit::new(ExitKind::Aborted { after_grace: false }, true);

        assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
        member.terminalize(exit.clone());
        assert_eq!(waiting.await, Err(exit));
    }

    #[crate::runtime::test]
    #[should_panic(expected = "completed one-shot task must publish its typed value")]
    async fn completed_terminal_publication_requires_a_typed_value() {
        let (sender, claim, member) = one_shot_claim::<u8>();
        drop(sender);
        member.terminalize(Exit::new(ExitKind::Completed, false));

        let _ = claim.wait().await;
    }

    #[crate::runtime::test]
    async fn local_cancellation_cancels_only_the_derived_token() {
        let primary = Latch::default();
        let local = Latch::default();
        let supervisor = CancellationToken::from_latch(primary.clone());
        let operation = supervisor.child(local.clone());

        assert!(local.fire());
        assert!(matches!(
            runtime::timeout(Duration::from_secs(1), operation.cancelled()).await,
            Timeout::Completed(())
        ));
        assert!(operation.is_cancelled());
        assert!(!supervisor.is_cancelled());
        assert!(!primary.is_fired());
    }

    #[crate::runtime::test]
    async fn supervisor_cancellation_cancels_the_derived_token() {
        let primary = Latch::default();
        let local = Latch::default();
        let supervisor = CancellationToken::from_latch(primary.clone());
        let operation = supervisor.child(local.clone());

        assert!(primary.fire());
        assert!(matches!(
            runtime::timeout(Duration::from_secs(1), operation.cancelled()).await,
            Timeout::Completed(())
        ));
        assert!(supervisor.is_cancelled());
        assert!(operation.is_cancelled());
        assert!(!local.is_fired());
    }

    #[crate::runtime::test(flavor = "multi_thread", worker_threads = 4)]
    async fn simultaneous_supervisor_and_local_cancellation_wake_the_operation() {
        for _ in 0..128 {
            let primary = Latch::default();
            let local = Latch::default();
            let operation = CancellationToken::from_latch(primary.clone()).child(local.clone());
            let waiter = runtime::spawn((), async move {
                operation.cancelled().await;
            });

            let primary_firer = runtime::spawn((), async move {
                runtime::yield_now().await;
                primary.fire()
            });
            let local_firer = runtime::spawn((), async move {
                runtime::yield_now().await;
                local.fire()
            });

            assert!(matches!(
                runtime::join(primary_firer).await,
                JoinOutcome::Ok { value: true }
            ));
            assert!(matches!(
                runtime::join(local_firer).await,
                JoinOutcome::Ok { value: true }
            ));
            assert!(matches!(
                runtime::timeout(Duration::from_secs(1), runtime::join(waiter)).await,
                Timeout::Completed(JoinOutcome::Ok { value: () })
            ));
        }
    }
}
