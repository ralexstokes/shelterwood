//! Supervised task definitions, contexts, and handles.

use std::{
    fmt,
    future::Future,
    hash::{Hash, Hasher},
    pin::Pin,
    sync::Arc,
};

use crate::{
    ChildId, Exit, ExitError, ExitResult, Incarnation, Membership, PolicyError, Readiness,
    ReadinessDeadline, RestartPolicy, Retention, Shutdown,
    cancellation::CancellationToken,
    cells::MemberCell,
    policy::CommonOptions,
    runtime::{self, CompletionGatedLatch, Latch},
};

pub(crate) type TaskFuture = Pin<Box<dyn Future<Output = ExitResult> + Send + 'static>>;
pub(crate) type TaskFactory = Arc<dyn Fn(TaskContext) -> TaskFuture + Send + Sync + 'static>;

pub(crate) struct TaskContextLatches {
    pub(crate) shutdown: Latch,
    pub(crate) abort: Latch,
    pub(crate) ready: CompletionGatedLatch,
}

/// Per-incarnation capabilities supplied to a supervised task.
#[derive(Clone, Debug)]
pub struct TaskContext {
    id: ChildId,
    incarnation: Incarnation,
    shutdown: CancellationToken,
    abort: CancellationToken,
    ready: CompletionGatedLatch,
}

impl TaskContext {
    pub(crate) fn new(id: ChildId, incarnation: Incarnation, latches: TaskContextLatches) -> Self {
        Self {
            id,
            incarnation,
            shutdown: CancellationToken::from_latch(latches.shutdown),
            abort: CancellationToken::from_latch(latches.abort),
            ready: latches.ready,
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
            body: Some(Box::new(move |context| {
                Box::pin(async move {
                    match body(context).await {
                        Ok(value) => {
                            // A rejected send means the completion claim was
                            // abandoned. Destroying the value here would run a
                            // possibly blocking or panicking user destructor
                            // inside the supervised task future, hanging it or
                            // replacing a Completed verdict with Panicked.
                            if let Err(abandoned) = completion.send(value) {
                                runtime::dispose_detached(abandoned);
                            }
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
    body: Option<OnceTaskFactory>,
    pub(crate) options: CommonOptions,
}

type OnceTaskFactory = Box<dyn FnOnce(TaskContext) -> TaskFuture + Send + 'static>;

impl OnceTask {
    pub(crate) fn take_body(&mut self) -> OnceTaskFactory {
        self.body
            .take()
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

// Handle identity is the slot cell, not the membership token: declaration
// lowering can rebase a provisional token behind live pre-spawn handles, and
// a token-value hash would strand entries keyed before that rebase.
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
    completion: runtime::DisposingReceiver<T>,
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

impl<T: Send + 'static> OneShotTaskRef<T> {
    pub(crate) fn new(completion: runtime::OneShotReceiver<T>, task: TaskRef) -> Self {
        Self {
            completion: runtime::DisposingReceiver::new(completion),
            task,
        }
    }

    /// Consumes the claim and waits for the authoritative terminal verdict.
    ///
    /// The typed value is released only when terminal publication classifies
    /// the membership as [`crate::ExitKind::Completed`]. Any competing
    /// failure, panic, abort, readiness timeout, or never-started verdict wins
    /// even if the task body produced a value first. That precedence is
    /// deliberately asymmetric: a body that returned `Err` keeps its
    /// [`crate::ExitKind::Failed`] verdict through a racing forced abort,
    /// while a body that returned `Ok` does not — the value is discarded
    /// through isolated disposal and the claim resolves `Err` with the abort
    /// verdict. An unclaimed stored value is likewise disposed in isolation
    /// when the claim is dropped, so a blocking or panicking destructor never
    /// runs on the dropping thread.
    pub async fn wait(mut self) -> Result<T, Exit> {
        let exit = self.task.wait().await;
        if !matches!(exit.kind(), crate::ExitKind::Completed) {
            return Err(exit);
        }
        // The erased one-shot body sends before returning success, and this
        // receiver is the sole completion claim. A published Completed verdict
        // with a closed, empty channel is therefore a framework invariant
        // violation, not an application exit that can be reported as `Err`.
        Ok(
            std::future::poll_fn(|context| self.completion.poll_receive(context))
                .await
                .expect("completed one-shot task must publish its typed value"),
        )
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
        task::{Context, Poll, Waker},
    };

    use crate::{
        Cancellation, ChildId, Exit, ExitError, ExitKind, GracePhase,
        cells::MemberCell,
        identity::ScopeIdentity,
        runtime::{self, CompletionGatedLatch, Latch},
    };

    use super::{OneShotTaskRef, TaskContext, TaskContextLatches, TaskOnceDef, TaskRef};

    fn task_context() -> (TaskContext, Latch, Latch, CompletionGatedLatch) {
        let id = ChildId::from("task");
        let mut identity = ScopeIdentity::new();
        let (_, mut incarnations) = identity
            .mint_membership(&id)
            .expect("membership available")
            .into_pair();
        let incarnation = incarnations.mint().expect("incarnation available");
        let shutdown = Latch::default();
        let abort = Latch::default();
        let ready = CompletionGatedLatch::default();
        let context = TaskContext::new(
            id,
            incarnation,
            TaskContextLatches {
                shutdown: shutdown.clone(),
                abort: abort.clone(),
                ready: ready.clone(),
            },
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

    #[test]
    fn taking_one_shot_body_moves_its_sole_owner() {
        struct DropProbe(Arc<AtomicUsize>);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let probe = DropProbe(Arc::clone(&drops));
        let (completion, _claim) = runtime::oneshot::<()>();
        let mut task = TaskOnceDef::new(move |_| async move {
            drop(probe);
            Ok::<_, ExitError>(())
        })
        .erase(completion);

        let body = task.take_body();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(task);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(body);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    #[should_panic(expected = "one-shot task construction invoked more than once")]
    fn taking_one_shot_body_twice_preserves_the_invariant_panic() {
        let (completion, _claim) = runtime::oneshot::<()>();
        let mut task = TaskOnceDef::new(|_| async { Ok::<_, ExitError>(()) }).erase(completion);

        drop(task.take_body());
        drop(task.take_body());
    }

    fn one_shot_claim<T: Send + 'static>() -> (
        runtime::OneShotSender<T>,
        OneShotTaskRef<T>,
        std::sync::Arc<MemberCell>,
    ) {
        let mut identity = ScopeIdentity::new();
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
        member.terminalize(
            Exit::new(ExitKind::Completed, Cancellation::NotObserved),
            crate::cells::StartupDisposition::Unchanged,
        );
        assert_eq!(waiting.await, Ok(42));
    }

    #[crate::runtime::test]
    async fn non_completed_terminal_publication_hides_a_queued_typed_value() {
        let (sender, claim, member) = one_shot_claim();
        sender.send(42_u8).expect("claim remains open");
        let mut waiting = Box::pin(claim.wait());
        let mut context = Context::from_waker(Waker::noop());
        let exit = Exit::new(
            ExitKind::Aborted {
                phase: GracePhase::WithinGrace,
            },
            Cancellation::Observed,
        );

        assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
        member.terminalize(exit.clone(), crate::cells::StartupDisposition::Unchanged);
        assert_eq!(waiting.await, Err(exit));
    }

    #[crate::runtime::test]
    #[should_panic(expected = "completed one-shot task must publish its typed value")]
    async fn completed_terminal_publication_requires_a_typed_value() {
        let (sender, claim, member) = one_shot_claim::<u8>();
        drop(sender);
        member.terminalize(
            Exit::new(ExitKind::Completed, Cancellation::NotObserved),
            crate::cells::StartupDisposition::Unchanged,
        );

        let _ = claim.wait().await;
    }
}
