use std::{error::Error, fmt::Debug, time::Duration};

use shelterwood::{
    ActorRef, CallError, ExitResult, RawActor, RawContext, RawOnceDef, Replied, Reply,
    ReplyReceiver, SendError, Tree,
};

struct Message;

struct OpaqueRaw;

impl RawActor for OpaqueRaw {
    type Msg = Message;

    async fn run(&mut self, _context: &mut RawContext<Self::Msg>) -> ExitResult {
        Ok(())
    }
}

fn assert_debug<T: Debug>() {}
fn assert_error<T: Error>() {}
fn assert_send<T: Send>(_: T) {}
fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn public_types_obey_the_trait_and_future_matrix_without_payload_debug_bounds() {
    assert_debug::<RawOnceDef<OpaqueRaw>>();
    assert_debug::<SendError<Message>>();
    assert_debug::<Replied<Message>>();
    assert_debug::<Reply<Message>>();
    assert_debug::<ReplyReceiver<Message>>();
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
