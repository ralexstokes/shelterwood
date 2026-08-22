//! Request/reply two ways: a split reply channel awaited separately from
//! its send, and the packaged `call` that bundles the same pieces under one
//! deadline. Both results carry the accepting incarnation as identity
//! evidence.

use std::time::Duration;

use shelterwood::{Actor, ActorDef, Context, ExitError, ExitResult, Reply, Tree};

// ANCHOR: request_reply
// ANCHOR: actor
struct Counter {
    count: u64,
}

enum Msg {
    Add(u64),
    Total(Reply<u64>),
}

impl Actor for Counter {
    type Msg = Msg;
    type Args = ();

    async fn init(_args: (), _context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { count: 0 })
    }

    async fn handle(&mut self, message: Msg, _context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            Msg::Add(n) => self.count += n,
            Msg::Total(reply) => reply.send(self.count),
        }
        Ok(())
    }
}
// ANCHOR_END: actor

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tree = Tree::new();
    let counter = tree.add_actor("counter", ActorDef::<Counter>::cloned(()))?;

    let system = tree.spawn()?;
    system.wait_started().await?;

    counter.send(Msg::Add(40)).await?;

    // ANCHOR: split_reply
    // Split the pieces of a call: embed the `Reply` in a message, submit it
    // fail-fast, and await the receiver under a response-only deadline.
    // Acceptance evidence belongs to the send's own result.
    let (reply, receiver) = counter.reply_channel();
    let accepted = counter.try_send(Msg::Total(reply))?;
    let total = receiver.recv(Duration::from_secs(1)).await?;
    assert_eq!(total, 40);
    // ANCHOR_END: split_reply

    // ANCHOR: call
    // `call` packages construction, acceptance, and response under one
    // deadline, resolving to the value plus the accepting incarnation.
    let replied = counter.call(Msg::Total, Duration::from_secs(1)).await?;
    assert_eq!(replied.value, 40);
    assert_eq!(replied.incarnation, accepted);
    // ANCHOR_END: call

    system.shutdown(Duration::from_secs(5)).await?;
    println!("split reply and call both saw: {total}");
    Ok(())
}
// ANCHOR_END: request_reply
