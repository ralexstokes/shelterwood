use std::{cell::Cell, error::Error, time::Duration};

use shelterwood::{
    Actor, ActorDef, ActorOnceDef, ActorRef, Blocking, CallError, Context, DeadlineElapsed,
    ExitError, ExitResult, Guard, Handler, LifecycleEvents, LifecycleTryRecvError, RawActor,
    RawContext, RawOnceDef, Reply, SendError, SnapshotClosed, SnapshotReceiver, Tree, WaitError,
};

fn assert_error<T: Error>() {}
fn assert_send<T: Send>(_: T) {}
fn assert_send_type<T: Send>() {}
fn assert_send_sync<T: Send + Sync>() {}
fn assert_static<T: 'static>() {}

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
    assert_send_sync::<Guard>();

    // The repeatable factory itself is Send + Sync after #37, but its
    // per-incarnation result and the one-shot path remain Send-only.
    let _ = ActorDef::<OpaqueActor>::factory(|| Cell::new(()));
    let _ = ActorDef::<ClonedActor>::cloned(());
    let _ = ActorOnceDef::<OpaqueActor>::new(Cell::new(()));
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
