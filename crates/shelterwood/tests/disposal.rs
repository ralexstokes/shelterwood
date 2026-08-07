use std::{
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{DestructorBlocker, DestructorGate, PanicOnDrop, policy::never, poll_until};
use shelterwood::{
    ExitKind, ExitResult, LifecycleEventKind, LifecycleItem, RawActor, RawContext, RawOnceDef,
    TaskDef, Tree,
};

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
