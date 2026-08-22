//! The front-page quickstart: a counter actor with a request/reply
//! protocol, spawned under an ordered tree inside an ambient Tokio runtime.

use std::time::Duration;

use shelterwood::{Actor, ActorDef, Context, ExitError, ExitResult, Reply, Tree};

// ANCHOR: quickstart
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
    // ANCHOR: run
    let mut tree = Tree::new();
    let counter = tree.add_actor("counter", ActorDef::<Counter>::cloned(()))?;

    let system = tree.spawn()?;
    system.wait_started().await?;

    counter.send(Msg::Add(2)).await?;
    let replied = counter.call(Msg::Total, Duration::from_secs(1)).await?;
    assert_eq!(replied.value, 2);

    system.shutdown(Duration::from_secs(5)).await?;
    // ANCHOR_END: run
    println!("counter total: {}", replied.value);
    Ok(())
}
// ANCHOR_END: quickstart
