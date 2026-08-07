use std::{
    future::{Future, poll_fn},
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    thread::{self, ThreadId},
    time::Duration,
};

use crate::common::{
    DestructorBlocker, DestructorGate, PanicOnDrop, ReleaseGate, policy::never, poll_until,
};
use shelterwood::{
    Backoff, ChildState, DynamicTree, ExitError, ExitKind, ExitResult, Jitter, LifecycleEventKind,
    LifecycleItem, Mailbox, RawActor, RawContext, RawDef, RawOnceDef, Readiness, RemoveOutcome,
    RestartCondition, RestartPolicy, TaskDef, TaskOnceDef, Tree,
};

struct DropProbe {
    dropped: tokio::sync::mpsc::UnboundedSender<ThreadId>,
    panic: Option<&'static str>,
}

impl DropProbe {
    fn reporting(dropped: tokio::sync::mpsc::UnboundedSender<ThreadId>) -> Self {
        Self {
            dropped,
            panic: None,
        }
    }

    fn panicking(
        dropped: tokio::sync::mpsc::UnboundedSender<ThreadId>,
        message: &'static str,
    ) -> Self {
        Self {
            dropped,
            panic: Some(message),
        }
    }
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        let _ = self.dropped.send(thread::current().id());
        if let Some(message) = self.panic {
            panic!("{message}");
        }
    }
}

struct BlockingDropProbe {
    dropped: tokio::sync::mpsc::UnboundedSender<ThreadId>,
    _blocker: DestructorBlocker,
}

impl BlockingDropProbe {
    fn new(gate: &DestructorGate, dropped: tokio::sync::mpsc::UnboundedSender<ThreadId>) -> Self {
        Self {
            dropped,
            _blocker: gate.blocker(),
        }
    }
}

impl Drop for BlockingDropProbe {
    fn drop(&mut self) {
        let _ = self.dropped.send(thread::current().id());
    }
}

struct Unread<M>(PhantomData<M>);

impl<M> Default for Unread<M> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<M: Send + 'static> RawActor for Unread<M> {
    type Msg = M;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        context.shutdown_token().cancelled().await;
        Ok(())
    }
}

async fn wait_for_destructor(gate: &DestructorGate) {
    let gate = gate.clone();
    tokio::task::spawn_blocking(move || gate.wait_entered())
        .await
        .expect("destructor waiter joins");
}

async fn poll_pending<F: Future>(future: &mut std::pin::Pin<Box<F>>) {
    poll_fn(|context| match future.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("operation completed before its owned disposal"),
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_unread_message_does_not_hold_bounded_shutdown() {
    let gate = DestructorGate::default();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "unread",
            RawOnceDef::new(Unread::<DestructorBlocker>::default()),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor
        .send(gate.blocker())
        .await
        .expect("message is accepted without being read");

    let mut shutdown = tokio::spawn(system.shutdown(Duration::from_secs(1)));
    wait_for_destructor(&gate).await;
    let bounded = tokio::time::timeout(Duration::from_secs(1), &mut shutdown).await;
    gate.release();
    let result = match bounded {
        Ok(joined) => joined.expect("shutdown task joins"),
        Err(_) => {
            let _ = shutdown.await;
            panic!("an unread-message destructor held the scope driver")
        }
    };
    result.expect("scope shuts down while message destruction is blocked");
}

struct CountedPanic {
    drops: Arc<AtomicUsize>,
    message: &'static str,
}

impl CountedPanic {
    fn new(drops: &Arc<AtomicUsize>, message: &'static str) -> Self {
        Self {
            drops: Arc::clone(drops),
            message,
        }
    }
}

impl Drop for CountedPanic {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("{}", self.message);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panicking_unread_messages_are_all_disposed_without_reclassifying_the_actor() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once("unread", RawOnceDef::new(Unread::<CountedPanic>::default()))
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    system.wait_started().await.expect("actor starts");
    let mut lifecycle = scope.subscribe_lifecycle();
    actor
        .send(CountedPanic::new(&drops, "first payload destructor"))
        .await
        .expect("first message is accepted");
    actor
        .send(CountedPanic::new(&drops, "second payload destructor"))
        .await
        .expect("second message is accepted");

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("payload panics do not unwind the scope driver");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            drops.load(Ordering::SeqCst) == 2
        })
        .await,
        "one payload panic must not strand the remaining unread messages"
    );
    let exit = loop {
        let Some(item) = lifecycle.recv().await else {
            panic!("actor exit was not published")
        };
        if let LifecycleItem::Event(event) = item
            && let LifecycleEventKind::Exited { id, exit, .. } = event.kind
            && id.as_str() == "unread"
        {
            break exit;
        }
    };
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert!(exit.cancelled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn factory_capture_destructor_panics_are_isolated_classified_and_independent() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let first = tree
        .add_task(
            "first",
            TaskDef::new({
                let capture = CountedPanic::new(&drops, "first factory destructor");
                move |_| {
                    let _ = &capture;
                    async { Ok(()) }
                }
            })
            .restart(never()),
        )
        .expect("valid first task");
    let second = tree
        .add_task(
            "second",
            TaskDef::new({
                let capture = CountedPanic::new(&drops, "second factory destructor");
                move |_| {
                    let _ = &capture;
                    async { Ok(()) }
                }
            })
            .restart(never()),
        )
        .expect("valid second task");

    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("tasks start");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(drops.load(Ordering::SeqCst), 2);

    let first_exit = first.wait().await;
    assert!(matches!(
        first_exit.kind(),
        ExitKind::Panicked { message } if message.as_deref() == Some("first factory destructor")
    ));
    let second_exit = second.wait().await;
    assert!(matches!(
        second_exit.kind(),
        ExitKind::Panicked { message } if message.as_deref() == Some("second factory destructor")
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn factory_capture_destructor_panic_is_classified_during_shutdown() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "shutdown",
            TaskDef::new({
                let capture = CountedPanic::new(&drops, "shutdown factory destructor");
                move |context| {
                    let _ = &capture;
                    async move {
                        context.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            })
            .restart(never()),
        )
        .expect("valid task");

    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("task starts");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("factory destruction stays off the scope driver");
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let exit = task.wait().await;
    assert!(matches!(
        exit.kind(),
        ExitKind::Panicked { message }
            if message.as_deref() == Some("shutdown factory destructor")
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incarnation_panic_remains_primary_over_factory_destructor_panic() {
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "precedence",
            TaskDef::new({
                let capture = PanicOnDrop::new("secondary factory destructor");
                move |_| {
                    let _ = &capture;
                    async move {
                        let _incarnation = PanicOnDrop::new("primary incarnation destructor");
                        Ok(())
                    }
                }
            })
            .restart(never()),
        )
        .expect("valid task");

    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("task starts");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    let exit = task.wait().await;
    assert!(matches!(
        exit.kind(),
        ExitKind::Panicked { message }
            if message.as_deref() == Some("primary incarnation destructor")
    ));
}

#[test]
fn final_factory_capture_is_destroyed_outside_the_current_thread_driver() {
    let driver_thread = thread::current().id();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime");
    let dropped = runtime.block_on(async {
        let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
        let mut tree = Tree::new();
        tree.add_task(
            "factory",
            TaskDef::new({
                let capture = DropProbe::reporting(dropped);
                move |_| {
                    let _ = &capture;
                    async { Ok(()) }
                }
            })
            .restart(never()),
        )
        .expect("valid task");
        let system = tree.spawn().expect("runtime is available");
        assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
        drops.recv().await.expect("factory capture was destroyed")
    });
    assert_ne!(dropped, driver_thread);
}

#[test]
fn latest_prebind_conflation_is_destroyed_outside_the_current_thread_driver() {
    let driver_thread = thread::current().id();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime");
    let displaced_thread = runtime.block_on(async {
        let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
        let mut tree = Tree::new();
        let actor = tree
            .add_raw_once(
                "latest",
                RawOnceDef::new(Unread::<DropProbe>::default()).mailbox(Mailbox::Latest),
            )
            .expect("valid actor");

        let mut first = Box::pin(actor.send(DropProbe::reporting(dropped.clone())));
        let mut second = Box::pin(actor.send(DropProbe::reporting(dropped)));
        poll_pending(&mut first).await;
        poll_pending(&mut second).await;

        let system = tree.spawn().expect("runtime is available");
        system.wait_started().await.expect("actor starts");
        let displaced_thread = drops
            .recv()
            .await
            .expect("first latest value was displaced");
        assert!(first.await.is_ok());
        assert!(second.await.is_ok());
        system
            .shutdown(Duration::from_secs(1))
            .await
            .expect("latest actor shuts down");
        displaced_thread
    });
    assert_ne!(displaced_thread, driver_thread);
}

#[tokio::test]
async fn non_runtime_reservation_cancellation_contains_destructor_panic() {
    let tree = DynamicTree::new();
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let slot = scope.reserve_task("cancelled").expect("task reservation");
    let task = slot.task_ref();
    let admission = slot.define(TaskDef::new({
        let capture = DropProbe::panicking(dropped, "cancelled definition destructor");
        move |_| {
            let _ = &capture;
            async { Ok(()) }
        }
    }));

    let cancellation = thread::spawn(move || {
        let cancellation_thread = thread::current().id();
        drop(admission);
        cancellation_thread
    });
    let cancellation_thread = cancellation
        .join()
        .expect("cancellation outside a Tokio runtime must not panic");
    let disposal_thread = drops.recv().await.expect("definition was disposed");
    assert_ne!(disposal_thread, cancellation_thread);
    assert!(matches!(task.wait().await.kind(), ExitKind::NeverStarted));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("dynamic root shuts down");
}

#[tokio::test]
async fn unadmitted_removal_completes_after_blocking_definition_disposal() {
    let tree = DynamicTree::new();
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();
    let gate = DestructorGate::default();
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let slot = scope.reserve_task("pending").expect("task reservation");
    let task = slot.task_ref();
    let admission = slot.define(TaskDef::new({
        let capture = BlockingDropProbe::new(&gate, dropped);
        move |_| {
            let _ = &capture;
            async { Ok(()) }
        }
    }));
    let mut removal = Box::pin(scope.remove_task(&task));
    wait_for_destructor(&gate).await;
    assert_ne!(
        drops
            .recv()
            .await
            .expect("definition disposal reports its thread"),
        thread::current().id()
    );
    assert!(matches!(task.wait().await.kind(), ExitKind::NeverStarted));
    poll_pending(&mut removal).await;
    gate.release();
    assert_eq!(removal.await, RemoveOutcome::Removed);
    drop(admission);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("dynamic root shuts down");
}

#[tokio::test]
async fn hard_shutdown_detaches_a_blocking_factory_disposal() {
    let gate = DestructorGate::default();
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "blocked-factory",
            TaskDef::new({
                let capture = BlockingDropProbe::new(&gate, dropped);
                move |context| {
                    let _ = &capture;
                    async move {
                        context.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            }),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("task starts");

    let mut shutdown = tokio::spawn(system.shutdown(Duration::ZERO));
    wait_for_destructor(&gate).await;
    assert_ne!(
        drops
            .recv()
            .await
            .expect("factory disposal reports its thread"),
        thread::current().id()
    );
    let bounded = tokio::time::timeout(Duration::from_secs(1), &mut shutdown).await;
    gate.release();
    let result = bounded
        .expect("hard escalation is not held by factory disposal")
        .expect("shutdown task joins");
    result.expect("post-exit disposal is not an actor straggler");
    let exit = task.wait().await;
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert!(exit.cancelled());
}

#[tokio::test]
async fn startup_rollback_detaches_never_started_one_shot_state_after_escalation() {
    let gate = DestructorGate::default();
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    tree.add_task(
        "fails",
        TaskDef::new(|_| async { Err(ExitError::message("startup failure")) })
            .restart(never())
            .readiness(Readiness::Manual)
            .expect("manual readiness"),
    )
    .expect("valid failing task");
    let (_never_started, completion) = tree
        .add_task_once(
            "never-started",
            TaskOnceDef::new({
                let state = BlockingDropProbe::new(&gate, dropped);
                move |_| {
                    let _ = &state;
                    async { Ok::<_, ExitError>(()) }
                }
            }),
        )
        .expect("valid one-shot suffix");

    let system = tree.spawn().expect("runtime is available");
    let mut rollback = tokio::spawn(system.start_or_shutdown(Duration::ZERO));
    wait_for_destructor(&gate).await;
    assert_ne!(
        drops
            .recv()
            .await
            .expect("one-shot state reports its thread"),
        thread::current().id()
    );
    let bounded = tokio::time::timeout(Duration::from_secs(1), &mut rollback).await;
    gate.release();
    let result = bounded
        .expect("rollback escalation is not held by one-shot state")
        .expect("rollback task joins");
    assert!(result.is_err(), "startup failure is preserved");
    assert!(matches!(
        completion
            .wait()
            .await
            .expect_err("the ordered suffix never ran")
            .kind(),
        ExitKind::NeverStarted
    ));
}

#[tokio::test]
async fn restart_window_cleanup_is_isolated_without_reclassifying_the_recorded_exit() {
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "restarting",
            TaskDef::new({
                let capture = DropProbe::panicking(dropped, "restart-window factory destructor");
                move |_| {
                    let _ = &capture;
                    async { Err(ExitError::message("restart me")) }
                }
            })
            .restart(RestartPolicy::new(
                RestartCondition::OnFailure,
                Backoff::fixed(Duration::from_secs(60), Jitter::None).expect("fixed backoff"),
            )),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    system
        .wait_started()
        .await
        .expect("first incarnation starts");
    scope
        .wait_for_child(
            "restarting",
            |child| matches!(child.state, ChildState::Restarting),
            Duration::from_secs(1),
        )
        .await
        .expect("task enters restart backoff");
    system
        .shutdown(Duration::ZERO)
        .await
        .expect("restart-window cleanup does not become a shutdown straggler");
    assert_ne!(
        drops
            .recv()
            .await
            .expect("restart-window factory was disposed"),
        thread::current().id()
    );
    assert!(matches!(
        task.wait().await.kind(),
        ExitKind::Failed(error) if error.to_string() == "restart me"
    ));
}

struct OffloadPanic {
    run: ReleaseGate,
}

impl RawActor for OffloadPanic {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.run.wait().await;
        let _: () = context
            .run_blocking(|_| -> () { panic!("primary blocking offload panic") })
            .await;
        Ok(())
    }
}

#[tokio::test]
async fn blocking_offload_panic_remains_primary_over_factory_destructor_panic() {
    let run = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_raw(
        "offload-precedence",
        RawDef::factory({
            let capture = PanicOnDrop::new("secondary raw factory destructor");
            let run = run.clone();
            move || {
                let _ = &capture;
                OffloadPanic { run: run.clone() }
            }
        })
        .restart(never()),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    let mut lifecycle = system.scope().subscribe_lifecycle();
    run.release();
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);

    let exit = loop {
        let Some(item) = lifecycle.recv().await else {
            panic!("actor exit was not published")
        };
        if let LifecycleItem::Event(event) = item
            && let LifecycleEventKind::Exited { id, exit, .. } = event.kind
            && id.as_str() == "offload-precedence"
        {
            break exit;
        }
    };
    assert!(matches!(
        exit.kind(),
        ExitKind::Panicked { message }
            if message.as_deref() == Some("primary blocking offload panic")
    ));
}
