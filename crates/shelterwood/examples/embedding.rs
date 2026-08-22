//! Host-owned `main`: the host opens process-lifetime state, then runs two
//! complete build/spawn/`start_or_shutdown`/operate/`shutdown` cycles over
//! fresh trees sharing that state.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use shelterwood::{PolicyError, Readiness, TaskDef, Tree};

// ANCHOR: service
fn service(database: Arc<AtomicU64>) -> Result<TaskDef, PolicyError> {
    TaskDef::new(move |context| {
        let database = Arc::clone(&database);
        async move {
            database.fetch_add(1, Ordering::SeqCst);
            // Manual readiness publishes that the resource is usable, not
            // merely that the future was spawned.
            context.mark_ready();
            context.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .readiness(Readiness::Manual)
}
// ANCHOR_END: service

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ANCHOR: embedding
    // Durable host state is what crosses cycles; each tree receives a handle.
    let database = Arc::new(AtomicU64::new(0));

    for cycle in 1..=2u64 {
        // Builders and `System` are single-use: each cycle constructs afresh.
        let mut tree = Tree::new();
        tree.add_task("service", service(Arc::clone(&database))?)?;

        let system = tree.spawn()?;
        // A ready owner on success; on failure the started prefix is rolled
        // back and the startup error returned instead.
        let system = system.start_or_shutdown(Duration::from_secs(5)).await?;

        // Operate: readiness gated on the increment, so it is visible here.
        assert_eq!(database.load(Ordering::SeqCst), cycle);

        // Explicit shutdown joins the root driver before host resources close.
        system.shutdown(Duration::from_secs(5)).await?;
    }

    assert_eq!(database.load(Ordering::SeqCst), 2);
    // ANCHOR_END: embedding
    println!("two host-owned cycles ran over shared state");
    Ok(())
}
