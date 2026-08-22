//! Bounded shutdown of a resource-owning actor and a cooperative task:
//! the frozen mailbox prefix drains under `MailboxShutdown::Drain`, then
//! `on_stop` returns the resource, then the task observes its shutdown
//! token — all inside one `System::shutdown` budget.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use shelterwood::{
    Actor, ActorDef, Context, ExitError, ExitResult, MailboxShutdown, StopContext, TaskDef, Tree,
};

// ANCHOR: graceful_shutdown
// ANCHOR: actor
struct Store {
    handled: Arc<AtomicU64>,
    closed: Arc<AtomicBool>,
}

impl Actor for Store {
    type Msg = u64;
    type Args = (Arc<AtomicU64>, Arc<AtomicBool>);

    async fn init(
        (handled, closed): Self::Args,
        _context: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        Ok(Self { handled, closed })
    }

    async fn handle(&mut self, value: u64, _context: &mut Context<'_, Self>) -> ExitResult {
        self.handled.fetch_add(value, Ordering::SeqCst);
        Ok(())
    }

    // Best-effort resource return: runs after the drained prefix, within
    // the same grace budget.
    async fn on_stop(&mut self, _context: &mut StopContext<'_, Self>) {
        self.closed.store(true, Ordering::SeqCst);
    }
}
// ANCHOR_END: actor

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let handled = Arc::new(AtomicU64::new(0));
    let closed = Arc::new(AtomicBool::new(false));

    // ANCHOR: declare
    let mut tree = Tree::new();
    tree.add_task(
        "worker",
        TaskDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok(())
        }),
    )?;
    let store = tree.add_actor(
        "store",
        ActorDef::<Store>::cloned((Arc::clone(&handled), Arc::clone(&closed)))
            .mailbox_shutdown(MailboxShutdown::Drain),
    )?;
    // ANCHOR_END: declare

    let system = tree.spawn()?;
    system.wait_started().await?;

    // ANCHOR: shutdown
    // Accepted before shutdown, so `Drain` guarantees delivery ahead of
    // `on_stop` even if teardown wins the race to freeze intake.
    store.send(7).await?;

    system.shutdown(Duration::from_secs(5)).await?;

    assert_eq!(handled.load(Ordering::SeqCst), 7);
    assert!(closed.load(Ordering::SeqCst));
    // ANCHOR_END: shutdown
    println!("drained; resource closed");
    Ok(())
}
// ANCHOR_END: graceful_shutdown
