use std::{cell::Cell, error::Error, hash::Hash, time::Duration};

use shelterwood::{
    Actor, ActorDef, ActorOnceDef, ActorRef, ActorSlot, Admission, Blocking, CallError, CallFuture,
    CancellationToken, Context, DeadlineElapsed, DynamicActorSlot, DynamicScopeRef,
    DynamicSubtreeSlot, DynamicTaskSlot, ExitError, ExitResult, Guard, Handler, Incarnation,
    LifecycleEvents, LifecycleTryRecvError, Membership, OneShotTaskRef, RawActor, RawContext,
    RawDef, RawOnceDef, Removal, Reply, ReplyReceive, ReplyReceiver, ScopeRef, SendError,
    SendFuture, SendTimeout, SnapshotClosed, SnapshotReceiver, SubtreeDef, SubtreeOnceDef,
    SubtreeSlot, System, TaskDef, TaskOnceDef, TaskRef, TaskSlot, Tree, WaitError,
};

fn assert_error<T: Error>() {}
fn assert_send<T: Send>(_: T) {}
fn assert_send_type<T: Send>() {}
fn assert_send_sync<T: Send + Sync>() {}
fn assert_copy_eq_hash_send_sync<T: Copy + Eq + Hash + Send + Sync>() {}
fn assert_clone_eq_hash_send_sync<T: Clone + Eq + Hash + Send + Sync>() {}
fn assert_static<T: 'static>() {}

#[test]
fn documented_identity_handle_token_and_owned_value_bounds_compile() {
    assert_copy_eq_hash_send_sync::<Membership>();
    assert_copy_eq_hash_send_sync::<Incarnation>();

    assert_clone_eq_hash_send_sync::<ActorRef<Cell<()>>>();
    assert_clone_eq_hash_send_sync::<TaskRef>();
    assert_clone_eq_hash_send_sync::<ScopeRef>();
    assert_clone_eq_hash_send_sync::<DynamicScopeRef>();
    assert_send_sync::<SnapshotReceiver>();
    assert_send_sync::<LifecycleEvents>();
    assert_send_sync::<CancellationToken>();
    assert_clone::<CancellationToken>();

    // These owned, at-most-once values promise Send. Deliberately do not
    // turn their current implementation's incidental Sync-ness into API.
    assert_send_type::<System<ScopeRef>>();
    assert_send_type::<ActorSlot<Cell<()>>>();
    assert_send_type::<DynamicActorSlot<Cell<()>>>();
    assert_send_type::<TaskSlot>();
    assert_send_type::<DynamicTaskSlot>();
    assert_send_type::<SubtreeSlot<Tree>>();
    assert_send_type::<DynamicSubtreeSlot<Tree>>();
    assert_send_type::<OneShotTaskRef<Cell<()>>>();
    assert_send_type::<Reply<Cell<()>>>();
    assert_send_type::<ReplyReceiver<Cell<()>>>();
    assert_send_type::<Guard>();

    assert_send_type::<Admission<TaskRef>>();
    assert_send_type::<Removal>();
    assert_send_type::<SendFuture<Cell<()>>>();
    assert_send_type::<SendTimeout<Cell<()>>>();
    assert_send_type::<CallFuture<Cell<()>, Cell<()>>>();
    assert_send_type::<ReplyReceive<Cell<()>>>();

    let _assert_token_future = |token: &CancellationToken| assert_send(token.cancelled());
    let _assert_guard_future = |guard: &Guard| assert_send(guard.finished());
    let _assert_task_future = |task: &TaskRef| assert_send(task.wait());
    let _assert_one_shot_future = |task: OneShotTaskRef<Cell<()>>| assert_send(task.wait());
    let _assert_snapshot_future = |receiver: &mut SnapshotReceiver| assert_send(receiver.changed());
    let _assert_lifecycle_future = |events: &mut LifecycleEvents| assert_send(events.recv());
    let _assert_scope_futures = |scope: &ScopeRef| {
        assert_send(scope.wait_stopped());
        assert_send(scope.shutdown_and_wait(Duration::ZERO));
        assert_send(scope.wait_for_child("child", |_| true, Duration::ZERO));
    };
    let _assert_dynamic_scope_futures = |scope: &DynamicScopeRef| {
        assert_send(scope.wait_stopped());
        assert_send(scope.shutdown_and_wait(Duration::ZERO));
        assert_send(scope.wait_for_child("child", |_| true, Duration::ZERO));
    };
    let _assert_start_future = |system: System<ScopeRef>| {
        assert_send(system.start_or_shutdown(Duration::ZERO));
    };
    let _assert_shutdown_future = |system: System<ScopeRef>| {
        assert_send(system.shutdown(Duration::ZERO));
    };
    let _assert_wait_future = |system: System<ScopeRef>| assert_send(system.wait());
}

fn assert_clone<T: Clone>() {}

struct OpaqueActor {
    _not_sync: Cell<()>,
}

impl Actor for OpaqueActor {
    type Msg = Cell<()>;
    type Args = Cell<()>;

    async fn init(_: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            _not_sync: Cell::new(()),
        })
    }

    async fn handle(&mut self, _: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

fn assert_raw<T: RawActor<Msg = Cell<()>>>() {}

struct ClonedActor;

impl Actor for ClonedActor {
    type Msg = ();
    type Args = ();

    async fn init(_: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self)
    }

    async fn handle(&mut self, _: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

#[test]
fn actor_types_obey_resource_and_payload_trait_contracts() {
    assert_error::<DeadlineElapsed>();
    assert_raw::<Handler<OpaqueActor>>();
    assert_send_type::<Blocking<Cell<()>>>();
    assert_static::<Blocking<Cell<()>>>();
    assert_send_type::<Guard>();

    // The repeatable factory itself is Send + Sync after #37, but its
    // per-incarnation result and the one-shot path remain Send-only.
    let _ = ActorDef::<OpaqueActor>::factory(|| Cell::new(()));
    let _ = ActorDef::<ClonedActor>::cloned(());
    let _ = ActorOnceDef::<OpaqueActor>::new(Cell::new(()));
    let _ = TaskDef::new(|context| async move {
        let send_only_state = Cell::new(());
        context.shutdown_token().cancelled().await;
        send_only_state.set(());
        Ok(())
    });
    let captured = Cell::new(());
    let _ = TaskOnceDef::new(move |_| async move {
        captured.set(());
        Ok::<_, ExitError>(captured)
    });
    let _ = SubtreeDef::factory(Tree::new);
    let _ = SubtreeOnceDef::new(Tree::new());
}

struct OpaqueRaw {
    _not_sync: Cell<()>,
}

impl RawActor for OpaqueRaw {
    type Msg = Cell<()>;

    async fn run(&mut self, _context: &mut RawContext<Self::Msg>) -> ExitResult {
        Ok(())
    }
}

#[test]
fn raw_types_obey_error_and_future_trait_contracts() {
    assert_error::<SendError<Cell<()>>>();
    assert_error::<CallError>();
    assert_send_sync::<ActorRef<Cell<()>>>();

    let _ = RawDef::<OpaqueRaw>::factory(|| OpaqueRaw {
        _not_sync: Cell::new(()),
    });

    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "traits",
            RawOnceDef::new(OpaqueRaw {
                _not_sync: Cell::new(()),
            }),
        )
        .expect("valid actor");
    assert_send(actor.send(Cell::new(())));
    assert_send(actor.send_timeout(Cell::new(()), Duration::from_secs(1)));
    assert_send(actor.call(|_reply: Reply<()>| Cell::new(()), Duration::from_secs(1)));
    let (reply, receiver) = Reply::<Cell<()>>::channel();
    assert_send(reply);
    assert_send(receiver);
}

#[test]
fn observation_types_obey_error_and_thread_safety_contracts() {
    assert_error::<LifecycleTryRecvError>();
    assert_error::<SnapshotClosed>();
    assert_error::<WaitError>();
    assert_send_sync::<LifecycleEvents>();
    assert_send_sync::<SnapshotReceiver>();
}
