use super::support::*;
use crate::mailbox::MailboxEffectQueue;

struct ThreadRecordingRaw {
    dropped: crate::runtime::UnboundedMpscSender<std::thread::ThreadId>,
}

impl Drop for ThreadRecordingRaw {
    fn drop(&mut self) {
        let _ = crate::runtime::unbounded_mpsc_send(&self.dropped, std::thread::current().id());
    }
}

impl crate::RawActor for ThreadRecordingRaw {
    type Msg = u8;

    fn readiness() -> Readiness {
        Readiness::Manual
    }

    async fn run(&mut self, _: &mut crate::RawContext<Self::Msg>) -> crate::ExitResult {
        future::pending().await
    }
}

fn one_shot_raw_scope(
    dropped: crate::runtime::UnboundedMpscSender<std::thread::ThreadId>,
) -> (ScopeRuntime, ChildKey, Arc<MemberCell>, ActorRef<u8>) {
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once("worker", RawOnceDef::new(ThreadRecordingRaw { dropped }))
        .expect("the one-shot raw actor is valid");
    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let member = Arc::clone(&plan.children[0].slot.member);
    let epoch = ScopeEpochGuard::begin(&root).expect("the test scope has a live epoch");
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let mut children = ChildArena::default();
    let key = children.insert(child);
    let (events, _event_receiver) = crate::runtime::unbounded_mpsc();
    let scope = ScopeRuntimeBuilder::new(root, epoch, events)
        .with_defaults(plan.defaults.clone())
        .with_lifecycle(ScopeLifecycle::running())
        .with_children(children)
        .with_transferred_plan(plan)
        .build();
    (scope, key, member, actor)
}

async fn disposed_thread(
    receiver: &mut crate::runtime::UnboundedMpscReceiver<std::thread::ThreadId>,
) -> std::thread::ThreadId {
    match crate::runtime::timeout(DRIVER_PROGRESS_WAIT, receiver.recv()).await {
        crate::runtime::Timeout::Completed(Some(thread)) => thread,
        crate::runtime::Timeout::Completed(None) => {
            panic!("the disposal recorder closed before the payload was destroyed")
        }
        crate::runtime::Timeout::Elapsed => panic!("isolated payload disposal did not complete"),
    }
}

fn assert_waker_panic(payload: Box<dyn std::any::Any + Send>, expected: &'static str) {
    assert_eq!(
        payload.downcast_ref::<&'static str>().copied(),
        Some(expected),
        "the hostile observer's panic remains the surfaced diagnostic"
    );
}

#[crate::runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn started_waker_panic_keeps_one_shot_body_isolated() {
    const PANIC: &str = "injected Started observer panic";

    let (dropped, mut drops) = crate::runtime::unbounded_mpsc();
    let (mut scope, key, member, _actor) = one_shot_raw_scope(dropped);
    let mut record = member.record_watcher();
    let mut started = Box::pin(record.changed());
    let hostile = Waker::from(Arc::new(PanicWake(PANIC)));
    assert!(
        started
            .as_mut()
            .poll(&mut Context::from_waker(&hostile))
            .is_pending()
    );

    let driver_thread = std::thread::current().id();
    let result = catch_unwind(AssertUnwindSafe(|| scope.spawn_child(key)));

    let payload = result.expect_err("the hostile Started observer still surfaces");
    assert_waker_panic(payload, PANIC);
    assert_ne!(
        disposed_thread(&mut drops).await,
        driver_thread,
        "the extracted one-shot body must reach isolated disposal, not the driver unwind"
    );
}

#[crate::runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebind_waker_panic_keeps_one_shot_body_isolated() {
    const PANIC: &str = "injected rebind sender panic";

    let (dropped, mut drops) = crate::runtime::unbounded_mpsc();
    let (mut scope, key, _member, actor) = one_shot_raw_scope(dropped);

    // Give the mailbox a prior, fully closed incarnation so the spawn below
    // exercises the restart/rebind edge rather than the first-bind edge.
    let child = scope.children.get_mut(key).expect("the child remains live");
    let prior = child
        .incarnations
        .mint()
        .expect("a prior incarnation is available");
    let token = child
        .mailbox_bind
        .take()
        .expect("configuration supplies the first bind token");
    let mut effects = MailboxEffectQueue::default();
    let mailbox = child.mailbox.as_ref().expect("raw actors own a mailbox");
    mailbox.bind(token, prior, &mut effects);
    mailbox.freeze(prior, &mut effects);
    let close = mailbox
        .close(prior, &mut effects)
        .expect("closing the live prior incarnation returns the next bind token");
    // Hand the rebind token back to the driver before dropping the effect
    // queue, so `spawn_child` below still takes the restart path.
    let (rebind, teardown) = close.into_parts();
    child.mailbox_bind = Some(rebind);
    drop(effects);
    crate::runtime::dispose_detached(teardown);

    let hostile = Waker::from(Arc::new(PanicWake(PANIC)));
    let mut parked = Box::pin(actor.send(1));
    assert!(
        parked
            .as_mut()
            .poll(&mut Context::from_waker(&hostile))
            .is_pending()
    );

    let driver_thread = std::thread::current().id();
    let result = catch_unwind(AssertUnwindSafe(|| scope.spawn_child(key)));

    let payload = result.expect_err("the hostile parked sender still surfaces during rebind");
    assert_waker_panic(payload, PANIC);
    assert_ne!(
        disposed_thread(&mut drops).await,
        driver_thread,
        "the extracted one-shot body must reach isolated disposal, not the rebind unwind"
    );
    assert!(
        matches!(
            parked
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(_))
        ),
        "rebind accepted the parked send before its waker panic resumed"
    );
}
