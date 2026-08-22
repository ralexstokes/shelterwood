//! Supervised restart: an actor that fails on demand is restarted by policy,
//! and the same handle keeps answering across the superseding incarnation.

use std::time::Duration;

use shelterwood::{
    Actor, ActorDef, Backoff, Context, ExitError, ExitResult, Jitter, Reply, RestartCondition,
    RestartPolicy, Tree,
};

// ANCHOR: supervision_restart
// ANCHOR: actor
struct Worker;

enum Msg {
    Crash,
    Greet(Reply<&'static str>),
}

impl Actor for Worker {
    type Msg = Msg;
    type Args = ();

    async fn init(_args: (), _context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self)
    }

    async fn handle(&mut self, message: Msg, _context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            Msg::Crash => Err(ExitError::message("crashed on request")),
            Msg::Greet(reply) => {
                reply.send("hello");
                Ok(())
            }
        }
    }
}
// ANCHOR_END: actor

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ANCHOR: policy
    let mut tree = Tree::new();
    let worker = tree.add_actor(
        "worker",
        ActorDef::<Worker>::cloned(()).restart(RestartPolicy::new(
            RestartCondition::OnFailure,
            Backoff::fixed(Duration::from_millis(10), Jitter::None)?,
        )),
    )?;

    let system = tree.spawn()?;
    system.wait_started().await?;
    // ANCHOR_END: policy

    // ANCHOR: restart_wait
    let scope = system.scope();
    let before = scope
        .child("worker")
        .and_then(|child| child.incarnation)
        .expect("a started child has a live incarnation");

    worker.send(Msg::Crash).await?;

    // Snapshot watches conflate, so accept any incarnation at or past the
    // restart edge rather than expecting exactly the next generation.
    let restarted = scope
        .wait_for_child(
            "worker",
            |child| {
                child
                    .incarnation
                    .is_some_and(|incarnation| incarnation.supersedes(before))
            },
            Duration::from_secs(10),
        )
        .await?;
    let after = restarted
        .incarnation
        .expect("the predicate matched a live incarnation");
    assert!(after.supersedes(before));

    let replied = worker.call(Msg::Greet, Duration::from_secs(1)).await?;
    assert_eq!(replied.value, "hello");
    // ANCHOR_END: restart_wait

    system.shutdown(Duration::from_secs(5)).await?;
    println!("worker restarted after failure and answered again");
    Ok(())
}
// ANCHOR_END: supervision_restart
