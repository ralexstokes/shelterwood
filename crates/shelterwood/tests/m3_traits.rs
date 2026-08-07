use std::{error::Error, fmt::Debug};

use shelterwood::{
    Actor, ActorDef, ActorOnceDef, Blocking, Context, DeadlineElapsed, ExitError, ExitResult,
    Guard, Handler, RawActor, Rejected,
};

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

fn assert_debug<T: Debug>() {}
fn assert_error<T: Error>() {}
fn assert_raw<T: RawActor<Msg = OpaqueMessage>>() {}
fn assert_send<T: Send>() {}
fn assert_send_sync<T: Send + Sync>() {}
fn assert_static<T: 'static>() {}

#[test]
fn m3_public_types_obey_callback_resource_and_payload_trait_contracts() {
    assert_debug::<ActorDef<OpaqueActor>>();
    assert_debug::<ActorOnceDef<OpaqueActor>>();
    assert_debug::<Rejected<OpaqueMessage>>();
    assert_error::<DeadlineElapsed>();
    assert_raw::<Handler<OpaqueActor>>();
    assert_send::<Blocking<OpaqueMessage>>();
    assert_static::<Blocking<OpaqueMessage>>();
    assert_send_sync::<Guard>();

    let _ = ActorDef::<OpaqueActor>::cloned(OpaqueArgs);
    let _ = ActorOnceDef::<OpaqueActor>::new(OpaqueArgs);
}
