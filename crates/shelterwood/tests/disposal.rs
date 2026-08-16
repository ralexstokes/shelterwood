mod common;

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
    DestructorBlocker, DestructorGate, POLL_TIMEOUT, PanicOnDrop, ReleaseGate, assert_eventually,
    policy::never,
};
use shelterwood::{
    Backoff, CallErrorKind, Cancellation, ChildState, DynamicTree, ExitError, ExitKind, ExitResult,
    Jitter, LifecycleEventKind, LifecycleItem, Mailbox, RawActor, RawContext, RawDef, RawOnceDef,
    Readiness, ReadinessDeadline, RemoveOutcome, Reply, ReserveError, RestartCondition,
    RestartPolicy, SubtreeOnceDef, TaskDef, TaskOnceDef, Tree,
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

struct OrderedBlockingDropProbe {
    order: Arc<std::sync::Mutex<Vec<&'static str>>>,
    blocker: Option<DestructorBlocker>,
}

impl OrderedBlockingDropProbe {
    fn new(gate: &DestructorGate, order: Arc<std::sync::Mutex<Vec<&'static str>>>) -> Self {
        Self {
            order,
            blocker: Some(gate.blocker()),
        }
    }
}

impl Drop for OrderedBlockingDropProbe {
    fn drop(&mut self) {
        self.order
            .lock()
            .expect("order mutex is available")
            .push("later-dispose-start");
        drop(self.blocker.take());
        self.order
            .lock()
            .expect("order mutex is available")
            .push("later-dispose-end");
    }
}

struct PanickingPanicPayload;

impl Drop for PanickingPanicPayload {
    fn drop(&mut self) {
        panic!("panic payload destructor");
    }
}

struct PanicAnyOnDrop;

impl Drop for PanicAnyOnDrop {
    fn drop(&mut self) {
        std::panic::panic_any(PanickingPanicPayload);
    }
}

struct NonStringPanicOnDrop;

impl Drop for NonStringPanicOnDrop {
    fn drop(&mut self) {
        std::panic::panic_any(17_u8);
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

async fn assert_disposed_off(
    drops: &mut tokio::sync::mpsc::UnboundedReceiver<ThreadId>,
    forbidden: ThreadId,
    description: &str,
) {
    let actual = drops
        .recv()
        .await
        .unwrap_or_else(|| panic!("{description}"));
    assert_ne!(actual, forbidden, "{description}");
}

async fn assert_disposed_off_current(
    drops: &mut tokio::sync::mpsc::UnboundedReceiver<ThreadId>,
    description: &str,
) {
    let actual = drops
        .recv()
        .await
        .unwrap_or_else(|| panic!("{description}"));
    // The test task may migrate while `recv` is pending, so sample its worker
    // only after the disposal report wakes it.
    assert_ne!(actual, thread::current().id(), "{description}");
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
    let bounded = tokio::time::timeout(POLL_TIMEOUT, &mut shutdown).await;
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
    assert_eventually!(
        || drops.load(Ordering::SeqCst) == 2,
        "one payload panic must not strand the remaining unread messages"
    )
    .await;
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
    assert_eq!(exit.cancellation(), Cancellation::Observed);
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
async fn non_string_factory_destructor_panic_remains_a_panic_exit() {
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "non-string",
            TaskDef::new({
                let capture = NonStringPanicOnDrop;
                move |_| {
                    let _ = &capture;
                    async { Ok(()) }
                }
            })
            .restart(never()),
        )
        .expect("valid task");

    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("task starts");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert!(matches!(
        task.wait().await.kind(),
        ExitKind::Panicked { message: None }
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
    assert_disposed_off(&mut drops, cancellation_thread, "definition was disposed").await;
    assert!(matches!(task.wait().await.kind(), ExitKind::NeverStarted));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("dynamic root shuts down");
}

struct HostileFallbackCapture {
    dropped: tokio::sync::mpsc::UnboundedSender<ThreadId>,
    blocker: Option<DestructorBlocker>,
    panic: Option<&'static str>,
}

impl Drop for HostileFallbackCapture {
    fn drop(&mut self) {
        let _ = self.dropped.send(thread::current().id());
        drop(self.blocker.take());
        if let Some(message) = self.panic {
            panic!("{message}");
        }
    }
}

#[tokio::test]
async fn non_runtime_disposals_run_off_callers_and_contain_panics() {
    const VALUES: usize = 8;

    let tree = DynamicTree::new();
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();
    let gate = DestructorGate::default();
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();

    let mut tasks = Vec::with_capacity(VALUES);
    let mut admissions = Vec::with_capacity(VALUES);
    for index in 0..VALUES {
        let slot = scope
            .reserve_task(format!("hostile-{index}"))
            .expect("task reservation");
        tasks.push(slot.task_ref());
        admissions.push(slot.define(TaskDef::new({
            let capture = HostileFallbackCapture {
                dropped: dropped.clone(),
                // Include a blocking destructor to exercise isolation without
                // requiring any particular disposal-worker topology.
                blocker: (index == 0).then(|| gate.blocker()),
                panic: (index == VALUES - 1).then_some("hostile fallback destructor"),
            };
            move |_| {
                let _ = &capture;
                async { Ok(()) }
            }
        })));
    }
    drop(dropped);

    let dropper = thread::spawn(move || {
        let dropper_thread = thread::current().id();
        drop(admissions);
        dropper_thread
    });
    let dropper_thread = dropper
        .join()
        .expect("dropping definitions outside a Tokio runtime never blocks or panics the caller");

    wait_for_destructor(&gate).await;
    gate.release();

    let mut disposal_threads = Vec::with_capacity(VALUES);
    while disposal_threads.len() < VALUES {
        disposal_threads.push(drops.recv().await.expect("every capture is disposed"));
    }
    assert!(drops.recv().await.is_none());
    assert!(
        disposal_threads
            .iter()
            .all(|thread| *thread != dropper_thread),
        "every disposal runs off the thread that submitted it"
    );
    for task in tasks {
        assert!(matches!(task.wait().await.kind(), ExitKind::NeverStarted));
    }

    // A contained destructor panic must not wedge the shared queue: a later
    // non-runtime disposal still completes off the submitting thread.
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let slot = scope
        .reserve_task("after-panic")
        .expect("task reservation after a contained panic");
    let admission = slot.define(TaskDef::new({
        let capture = HostileFallbackCapture {
            dropped,
            blocker: None,
            panic: None,
        };
        move |_| {
            let _ = &capture;
            async { Ok(()) }
        }
    }));
    let late_dropper = thread::spawn(move || {
        let dropper_thread = thread::current().id();
        drop(admission);
        dropper_thread
    });
    let late_dropper_thread = late_dropper.join().expect("late cancellation never panics");
    assert_disposed_off(
        &mut drops,
        late_dropper_thread,
        "disposal keeps running after a contained destructor panic",
    )
    .await;

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
    assert_disposed_off_current(&mut drops, "definition disposal reports its thread").await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_window_removal_joins_factory_disposal_before_terminality() {
    let gate = DestructorGate::default();
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = DynamicTree::new();
    let task = tree
        .add_task(
            "restarting",
            TaskDef::new({
                let capture = BlockingDropProbe::new(&gate, dropped);
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
        .as_scope()
        .wait_for_child(
            "restarting",
            |child| matches!(child.state, ChildState::Restarting),
            Duration::from_secs(1),
        )
        .await
        .expect("task enters restart backoff");

    let mut removal = Box::pin(scope.remove_task(&task));
    poll_pending(&mut removal).await;
    wait_for_destructor(&gate).await;
    assert_disposed_off_current(&mut drops, "factory disposal reports its thread").await;
    poll_pending(&mut removal).await;
    let mut terminal = Box::pin(task.wait());
    poll_pending(&mut terminal).await;

    gate.release();
    assert!(matches!(
        terminal.await.kind(),
        ExitKind::Failed(error) if error.to_string() == "restart me"
    ));
    assert_eq!(removal.await, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("dynamic root shuts down");
}

#[tokio::test]
async fn unadmitted_removal_completes_when_the_panic_payload_destructor_panics() {
    let tree = DynamicTree::new();
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();
    let slot = scope.reserve_task("pending").expect("task reservation");
    let task = slot.task_ref();
    let admission = slot.define(TaskDef::new({
        let capture = PanicAnyOnDrop;
        move |_| {
            let _ = &capture;
            async { Ok(()) }
        }
    }));

    assert_eq!(
        tokio::time::timeout(POLL_TIMEOUT, scope.remove_task(&task))
            .await
            .expect("panic payload disposal publishes removal completion"),
        RemoveOutcome::Removed
    );
    assert!(matches!(task.wait().await.kind(), ExitKind::NeverStarted));
    drop(admission);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("dynamic root shuts down");
}

#[tokio::test]
async fn ordered_shutdown_waits_for_later_unstarted_definition_disposal() {
    let gate = DestructorGate::default();
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    tree.add_task(
        "earlier",
        TaskDef::new({
            let order = Arc::clone(&order);
            move |context| {
                let shutdown = context.shutdown_token();
                started
                    .send(shutdown.clone())
                    .expect("test observes task startup");
                let order = Arc::clone(&order);
                async move {
                    shutdown.cancelled().await;
                    order
                        .lock()
                        .expect("order mutex is available")
                        .push("earlier-stop");
                    Ok(())
                }
            }
        })
        .readiness(Readiness::Manual)
        .expect("manual readiness")
        .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid earlier task");
    let (_later, _completion) = tree
        .add_task_once(
            "later",
            TaskOnceDef::new({
                let capture = OrderedBlockingDropProbe::new(&gate, Arc::clone(&order));
                move |_| {
                    let _ = &capture;
                    async { Ok::<_, ExitError>(()) }
                }
            }),
        )
        .expect("valid later task");

    let system = tree.spawn().expect("runtime is available");
    let shutdown_token = starts.recv().await.expect("earlier task starts");
    let shutdown = tokio::spawn(system.shutdown(Duration::from_secs(1)));
    wait_for_destructor(&gate).await;
    assert!(
        !shutdown_token.is_cancelled(),
        "earlier child must remain live while later definition disposal is pending"
    );
    assert_eq!(
        *order.lock().expect("order mutex is available"),
        ["later-dispose-start"]
    );

    gate.release();
    shutdown_token.cancelled().await;
    shutdown
        .await
        .expect("shutdown task joins")
        .expect("ordered root shuts down");
    assert_eq!(
        *order.lock().expect("order mutex is available"),
        ["later-dispose-start", "later-dispose-end", "earlier-stop"]
    );
}

#[tokio::test]
async fn zero_shutdown_reports_the_live_child_but_detaches_blocking_factory_disposal() {
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
    assert_disposed_off_current(&mut drops, "factory disposal reports its thread").await;
    let bounded = tokio::time::timeout(POLL_TIMEOUT, &mut shutdown).await;
    gate.release();
    let result = bounded
        .expect("hard escalation is not held by factory disposal")
        .expect("shutdown task joins");
    let timeout = result.expect_err("zero skips the cooperative wait for the live child");
    assert_eq!(timeout.stragglers.len(), 1);
    assert_eq!(timeout.stragglers[0].path[0].as_str(), "blocked-factory");
    let exit = task.wait().await;
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert_eq!(exit.cancellation(), Cancellation::Observed);
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
    assert_disposed_off_current(&mut drops, "one-shot state reports its thread").await;
    let bounded = tokio::time::timeout(POLL_TIMEOUT, &mut rollback).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_rollback_joins_never_started_disposal_before_terminality() {
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
    let mut rollback = Box::pin(system.start_or_shutdown(Duration::from_secs(5)));
    poll_pending(&mut rollback).await;
    wait_for_destructor(&gate).await;
    assert_disposed_off_current(&mut drops, "one-shot state reports its thread").await;
    poll_pending(&mut rollback).await;
    let mut terminal = Box::pin(completion.wait());
    poll_pending(&mut terminal).await;

    gate.release();
    assert!(rollback.await.is_err(), "startup failure is preserved");
    assert!(matches!(
        terminal
            .await
            .expect_err("the ordered suffix never ran")
            .kind(),
        ExitKind::NeverStarted
    ));
}

#[tokio::test]
async fn hard_shutdown_detaches_failed_nested_lowering_disposal() {
    let gate = DestructorGate::default();
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut nested = Tree::new();
    let _undefined = nested.reserve_task("undefined").expect("task reservation");
    let (_task, _completion) = nested
        .add_task_once(
            "blocked-definition",
            TaskOnceDef::new({
                let state = BlockingDropProbe::new(&gate, dropped);
                move |_| {
                    let _ = &state;
                    async { Ok::<_, ExitError>(()) }
                }
            }),
        )
        .expect("valid one-shot definition");
    let mut tree = Tree::new();
    tree.add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid one-shot subtree");

    let system = tree.spawn().expect("runtime is available");
    wait_for_destructor(&gate).await;
    assert_disposed_off_current(&mut drops, "failed definition disposal reports its thread").await;
    let mut shutdown = tokio::spawn(system.shutdown(Duration::ZERO));
    let bounded = tokio::time::timeout(POLL_TIMEOUT, &mut shutdown).await;
    gate.release();
    let result = bounded
        .expect("hard shutdown is not held by failed lowering disposal")
        .expect("shutdown task joins");
    assert!(result.is_err(), "the still-live subtree is a straggler");
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
    assert_disposed_off_current(&mut drops, "restart-window factory was disposed").await;
    assert!(matches!(
        task.wait().await.kind(),
        ExitKind::Failed(error) if error.to_string() == "restart me"
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_one_shot_completion_disposes_blocking_value_off_the_task() {
    let gate = DestructorGate::default();
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let probe = BlockingDropProbe::new(&gate, dropped);
    let (task, completion) = tree
        .add_task_once(
            "abandoned",
            TaskOnceDef::new(move |_| async move { Ok::<_, ExitError>(probe) }),
        )
        .expect("valid task");
    drop(completion);

    let system = tree.spawn().expect("runtime is available");
    wait_for_destructor(&gate).await;
    let bounded = tokio::time::timeout(POLL_TIMEOUT, task.wait()).await;
    gate.release();
    let exit = bounded.expect("a blocked abandoned-claim disposal must not hang the task");
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert_disposed_off_current(&mut drops, "abandoned result reports its disposal thread").await;
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_one_shot_completion_keeps_completed_verdict_through_destructor_panic() {
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let probe = DropProbe::panicking(dropped, "abandoned completion destructor");
    let (task, completion) = tree
        .add_task_once(
            "abandoned",
            TaskOnceDef::new(move |_| async move { Ok::<_, ExitError>(probe) }),
        )
        .expect("valid task");
    drop(completion);

    let system = tree.spawn().expect("runtime is available");
    let exit = task.wait().await;
    assert!(
        matches!(exit.kind(), ExitKind::Completed),
        "an abandoned claim's destructor panic must not reclassify the task: {exit:?}"
    );
    assert_disposed_off_current(&mut drops, "abandoned result reports its disposal thread").await;
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_id_rejection_disposes_blocking_definition_off_the_caller() {
    let gate = DestructorGate::default();
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    tree.add_task("dup", TaskDef::new(|_| async { Ok(()) }).restart(never()))
        .expect("first definition");
    let error = tree
        .add_task(
            "dup",
            TaskDef::new({
                let capture = BlockingDropProbe::new(&gate, dropped);
                move |_| {
                    let _ = &capture;
                    async { Ok(()) }
                }
            }),
        )
        .expect_err("duplicate id is rejected");
    assert!(matches!(error, ReserveError::DuplicateId(id) if id.as_str() == "dup"));

    wait_for_destructor(&gate).await;
    assert_disposed_off_current(
        &mut drops,
        "rejected definition reports its disposal thread",
    )
    .await;
    gate.release();
}

struct HostileCapture {
    _probe: DropProbe,
}

impl RawActor for HostileCapture {
    type Msg = ();

    async fn run(&mut self, _context: &mut RawContext<Self::Msg>) -> ExitResult {
        Ok(())
    }
}

#[tokio::test]
async fn duplicate_id_rejection_contains_definition_destructor_panic() {
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    tree.add_raw_once("dup", RawOnceDef::new(Unread::<()>::default()))
        .expect("first definition");
    let error = tree
        .add_raw_once(
            "dup",
            RawOnceDef::new(HostileCapture {
                _probe: DropProbe::panicking(dropped, "rejected definition destructor"),
            }),
        )
        .expect_err("duplicate id is rejected without unwinding the caller");
    assert!(matches!(error, ReserveError::DuplicateId(id) if id.as_str() == "dup"));
    assert_disposed_off_current(
        &mut drops,
        "rejected definition reports its disposal thread",
    )
    .await;
}

#[tokio::test]
async fn dynamic_duplicate_rejection_disposes_definition_before_admission() {
    let tree = DynamicTree::new();
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();
    let _held = scope.reserve_task("dup").expect("task reservation");
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let admission = scope.add_task(
        "dup",
        TaskDef::new({
            let capture = DropProbe::panicking(dropped, "rejected dynamic definition destructor");
            move |_| {
                let _ = &capture;
                async { Ok(()) }
            }
        }),
    );
    assert_disposed_off_current(
        &mut drops,
        "rejected definition reports its disposal thread",
    )
    .await;
    let error = admission.await.expect_err("duplicate id is rejected");
    assert!(matches!(error, ReserveError::DuplicateId(id) if id.as_str() == "dup"));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("dynamic root shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_send_disposes_blocking_message_off_the_caller() {
    let gate = DestructorGate::default();
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let tree = {
        let mut tree = Tree::new();
        let actor = tree
            .add_raw_once(
                "unread",
                RawOnceDef::new(Unread::<BlockingDropProbe>::default()),
            )
            .expect("valid actor");
        let mut pending = Box::pin(actor.send(BlockingDropProbe::new(&gate, dropped)));
        poll_pending(&mut pending).await;
        drop(pending);
        tree
    };

    wait_for_destructor(&gate).await;
    assert_disposed_off_current(&mut drops, "withdrawn message reports its disposal thread").await;
    gate.release();
    drop(tree);
}

#[tokio::test]
async fn cancelled_send_contains_message_destructor_panic() {
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once("unread", RawOnceDef::new(Unread::<DropProbe>::default()))
        .expect("valid actor");
    let mut pending =
        Box::pin(actor.send(DropProbe::panicking(dropped, "cancelled send destructor")));
    poll_pending(&mut pending).await;
    drop(pending);
    assert_disposed_off_current(&mut drops, "withdrawn message reports its disposal thread").await;
}

struct ReplyThenPark {
    gate: DestructorGate,
    dropped: tokio::sync::mpsc::UnboundedSender<ThreadId>,
    allow_reply: ReleaseGate,
    replied: ReleaseGate,
}

impl RawActor for ReplyThenPark {
    type Msg = Reply<BlockingDropProbe>;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        if let Some(reply) = context.recv().await {
            self.allow_reply.wait().await;
            reply.send(BlockingDropProbe::new(&self.gate, self.dropped.clone()));
            self.replied.release();
        }
        context.shutdown_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_call_disposes_stored_reply_off_the_caller() {
    let gate = DestructorGate::default();
    let allow_reply = ReleaseGate::default();
    let replied = ReleaseGate::default();
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "replier",
            RawOnceDef::new(ReplyThenPark {
                gate: gate.clone(),
                dropped,
                allow_reply: allow_reply.clone(),
                replied: replied.clone(),
            }),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");

    let mut call = Box::pin(actor.call(|reply| reply, Duration::from_secs(5)));
    poll_pending(&mut call).await;
    // The first poll must establish the cancellation window before the actor
    // can publish a reply; otherwise a fast second worker can complete the
    // call in that poll and drop the blocking probe on the test task.
    allow_reply.release();
    replied.wait().await;
    drop(call);
    wait_for_destructor(&gate).await;
    assert_disposed_off_current(&mut drops, "stored reply reports its disposal thread").await;
    gate.release();
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("replier shuts down");
}

#[tokio::test]
async fn dropped_reply_receiver_disposes_stored_value_through_isolated_disposal() {
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once("reply-runtime", RawOnceDef::new(Unread::<()>::default()))
        .expect("valid actor");
    let (reply, receiver) = actor.reply_channel::<DropProbe>();
    reply.send(DropProbe::panicking(dropped, "unclaimed reply destructor"));
    drop(receiver);
    assert_disposed_off_current(&mut drops, "stored reply reports its disposal thread").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unclaimed_completion_value_disposes_blocking_destructor_off_the_claim_holder() {
    let gate = DestructorGate::default();
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let probe = BlockingDropProbe::new(&gate, dropped);
    let (task, completion) = tree
        .add_task_once(
            "unclaimed",
            TaskOnceDef::new(move |_| async move { Ok::<_, ExitError>(probe) }),
        )
        .expect("valid task");

    let system = tree.spawn().expect("runtime is available");
    let exit = task.wait().await;
    assert!(matches!(exit.kind(), ExitKind::Completed));
    drop(completion);
    wait_for_destructor(&gate).await;
    assert_disposed_off_current(
        &mut drops,
        "unclaimed completion reports its disposal thread",
    )
    .await;
    gate.release();
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
}

#[tokio::test]
async fn unclaimed_completion_value_contains_destructor_panic() {
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let probe = DropProbe::panicking(dropped, "unclaimed completion destructor");
    let (task, completion) = tree
        .add_task_once(
            "unclaimed",
            TaskOnceDef::new(move |_| async move { Ok::<_, ExitError>(probe) }),
        )
        .expect("valid task");

    let system = tree.spawn().expect("runtime is available");
    let exit = task.wait().await;
    assert!(matches!(exit.kind(), ExitKind::Completed));
    drop(completion);
    assert_disposed_off_current(
        &mut drops,
        "unclaimed completion reports its disposal thread",
    )
    .await;
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
}

struct CompleteNow<M>(PhantomData<M>);

impl<M> Default for CompleteNow<M> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<M: Send + 'static> RawActor for CompleteNow<M> {
    type Msg = M;

    async fn run(&mut self, _context: &mut RawContext<Self::Msg>) -> ExitResult {
        Ok(())
    }
}

struct ProbedCall<P> {
    _reply: Reply<()>,
    _probe: P,
}

#[tokio::test]
async fn terminated_call_disposes_recovered_message_off_the_caller() {
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "done",
            RawOnceDef::new(CompleteNow::<ProbedCall<DropProbe>>::default()),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);

    let probe = DropProbe::panicking(dropped, "terminated call message destructor");
    let error = actor
        .call(
            move |reply| ProbedCall {
                _reply: reply,
                _probe: probe,
            },
            Duration::from_secs(1),
        )
        .await
        .expect_err("terminated actor rejects the call");
    assert!(matches!(error.kind, CallErrorKind::Terminated));
    assert_disposed_off_current(&mut drops, "recovered message reports its disposal thread").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acceptance_timed_out_call_disposes_recovered_message_off_the_caller() {
    let gate = DestructorGate::default();
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "unread",
            RawOnceDef::new(Unread::<ProbedCall<BlockingDropProbe>>::default()),
        )
        .expect("valid actor");

    let probe = BlockingDropProbe::new(&gate, dropped);
    let error = actor
        .call(
            move |reply| ProbedCall {
                _reply: reply,
                _probe: probe,
            },
            Duration::from_millis(50),
        )
        .await
        .expect_err("unbound mailbox never accepts within the deadline");
    assert!(matches!(error.kind, CallErrorKind::AcceptanceTimedOut));
    wait_for_destructor(&gate).await;
    assert_disposed_off_current(&mut drops, "recovered message reports its disposal thread").await;
    gate.release();
    drop(tree);
}

#[tokio::test]
async fn overdue_call_construction_disposes_message_off_constructor_task_and_contains_panic() {
    let (constructed, mut construction_threads) = tokio::sync::mpsc::unbounded_channel();
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "unread",
            RawOnceDef::new(Unread::<ProbedCall<DropProbe>>::default()),
        )
        .expect("valid actor");

    let probe = DropProbe::panicking(dropped, "overdue call message destructor");
    let error = actor
        .call(
            move |reply| {
                constructed
                    .send(thread::current().id())
                    .expect("construction thread is observed");
                // The deadline is captured immediately before invoking this
                // synchronous constructor, so sleeping here deterministically
                // finishes construction outside the call's total budget.
                thread::sleep(Duration::from_millis(25));
                ProbedCall {
                    _reply: reply,
                    _probe: probe,
                }
            },
            Duration::from_millis(1),
        )
        .await
        .expect_err("overdue construction times out before mailbox submission");
    assert!(matches!(error.kind, CallErrorKind::AcceptanceTimedOut));

    let construction_thread = construction_threads
        .recv()
        .await
        .expect("constructor reports its thread");
    assert_disposed_off(
        &mut drops,
        construction_thread,
        "overdue message destructor runs despite its contained panic",
    )
    .await;
    drop(tree);
}

#[tokio::test]
async fn dropped_unstarted_call_disposes_constructor_off_the_caller() {
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once("unread", RawOnceDef::new(Unread::<()>::default()))
        .expect("valid actor");

    let capture = DropProbe::panicking(dropped, "unstarted call constructor destructor");
    let call = actor.call(
        move |_reply: Reply<()>| {
            let _ = &capture;
        },
        Duration::from_secs(1),
    );
    drop(call);
    assert_disposed_off_current(
        &mut drops,
        "unstarted constructor reports its disposal thread",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_reply_send_disposes_unclaimed_value_off_the_sender() {
    let gate = DestructorGate::default();
    let (dropped, mut drops) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once("reply-runtime", RawOnceDef::new(Unread::<()>::default()))
        .expect("valid actor");
    let (reply, receiver) = actor.reply_channel::<BlockingDropProbe>();
    drop(receiver);
    reply.send(BlockingDropProbe::new(&gate, dropped));
    wait_for_destructor(&gate).await;
    assert_disposed_off_current(&mut drops, "late reply reports its disposal thread").await;
    gate.release();
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
