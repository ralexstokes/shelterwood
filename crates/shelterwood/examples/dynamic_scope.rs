//! Runtime admission and planned removal under a dynamic scope: admit an
//! actor through the `DynamicScopeRef`, exercise it, then remove exactly the
//! retained membership.

use std::time::Duration;

use shelterwood::{
    Actor, ActorDef, Context, DynamicTree, ExitError, ExitResult, RemoveOutcome, Reply,
};

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
    // ANCHOR: dynamic_scope
    let system = DynamicTree::new().spawn()?;
    system.wait_started().await?;
    let scope = system.scope();

    // ANCHOR: admit
    let counter = scope
        .add_actor("counter", ActorDef::<Counter>::cloned(()))
        .await?;
    // ANCHOR_END: admit

    counter.send(Msg::Add(3)).await?;
    let replied = counter.call(Msg::Total, Duration::from_secs(1)).await?;
    assert_eq!(replied.value, 3);

    // ANCHOR: remove
    // Planned removal: keep the exact handle returned by admission and remove
    // that membership. A same-id successor admitted later is a distinct
    // membership this removal could never touch.
    let outcome = scope.remove_actor(&counter).await;
    assert_eq!(outcome, RemoveOutcome::Removed);
    // ANCHOR_END: remove
    assert!(scope.as_scope().snapshot().child("counter").is_none());

    system.shutdown(Duration::from_secs(5)).await?;
    // ANCHOR_END: dynamic_scope
    println!("admitted, exercised, and removed a runtime member");
    Ok(())
}
