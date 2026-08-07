use std::{error::Error, time::Duration};

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

#[derive(Clone)]
struct OpaqueArgs;

struct OpaqueMessage;

struct OpaqueActor;

impl Actor for OpaqueActor {
    type Msg = OpaqueMessage;
    type Args = OpaqueArgs;

    async fn init(_: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self)
    }

    async fn handle(&mut self, _: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

fn assert_raw<T: RawActor<Msg = OpaqueMessage>>() {}

#[test]
fn actor_types_obey_resource_and_payload_trait_contracts() {
    assert_error::<DeadlineElapsed>();
    assert_raw::<Handler<OpaqueActor>>();
    assert_send_type::<Blocking<OpaqueMessage>>();
    assert_static::<Blocking<OpaqueMessage>>();
    assert_send_sync::<Guard>();

    let _ = ActorDef::<OpaqueActor>::cloned(OpaqueArgs);
    let _ = ActorOnceDef::<OpaqueActor>::new(OpaqueArgs);
}

struct Message;

struct OpaqueRaw;

impl RawActor for OpaqueRaw {
    type Msg = Message;

    async fn run(&mut self, _context: &mut RawContext<Self::Msg>) -> ExitResult {
        Ok(())
    }
}

#[test]
fn raw_types_obey_error_and_future_trait_contracts() {
    assert_error::<SendError<Message>>();
    assert_error::<CallError>();
    assert_send_sync::<ActorRef<Message>>();

    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once("traits", RawOnceDef::new(OpaqueRaw))
        .expect("valid actor");
    assert_send(actor.send(Message));
    assert_send(actor.send_timeout(Message, Duration::from_secs(1)));
    assert_send(actor.call(|_reply: Reply<()>| Message, Duration::from_secs(1)));
    let (reply, receiver) = Reply::<Message>::channel();
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
