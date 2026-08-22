//! Cyclic wiring: reserving slots splits handle creation from definition, so
//! two actors can hold each other's `ActorRef` before either is defined. A
//! one-round ping-pong proves the cycle carries traffic both ways.

use std::time::Duration;

use shelterwood::{Actor, ActorOnceDef, ActorRef, Context, ExitError, ExitResult, Tree};
use tokio::sync::mpsc;

// ANCHOR: cyclic_wiring
// ANCHOR: actors
enum PingMsg {
    Kick,
    Pong,
}

enum PongMsg {
    Ping,
}

struct Pinger {
    peer: ActorRef<PongMsg>,
    done: mpsc::UnboundedSender<()>,
}

impl Actor for Pinger {
    type Msg = PingMsg;
    type Args = (ActorRef<PongMsg>, mpsc::UnboundedSender<()>);

    async fn init(
        (peer, done): Self::Args,
        _context: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        Ok(Self { peer, done })
    }

    async fn handle(&mut self, message: PingMsg, _context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            PingMsg::Kick => {
                self.peer
                    .send(PongMsg::Ping)
                    .await
                    .map_err(|_| ExitError::message("ponger is unavailable"))?;
                Ok(())
            }
            PingMsg::Pong => {
                let _ = self.done.send(());
                Ok(())
            }
        }
    }
}

struct Ponger {
    peer: ActorRef<PingMsg>,
}

impl Actor for Ponger {
    type Msg = PongMsg;
    type Args = ActorRef<PingMsg>;

    async fn init(peer: Self::Args, _context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { peer })
    }

    async fn handle(&mut self, message: PongMsg, _context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            PongMsg::Ping => {
                self.peer
                    .send(PingMsg::Pong)
                    .await
                    .map_err(|_| ExitError::message("pinger is unavailable"))?;
                Ok(())
            }
        }
    }
}
// ANCHOR_END: actors

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (done_tx, mut done_rx) = mpsc::unbounded_channel();

    // ANCHOR: reserve
    let mut tree = Tree::new();
    let pinger_slot = tree.reserve_actor::<PingMsg>("pinger")?;
    let ponger_slot = tree.reserve_actor::<PongMsg>("ponger")?;

    // Both handles exist before either definition, so each definition can
    // capture the other's.
    let pinger = pinger_slot.actor_ref();
    let ponger = ponger_slot.actor_ref();

    let _ = pinger_slot.define_once(ActorOnceDef::<Pinger>::new((ponger, done_tx)));
    let _ = ponger_slot.define_once(ActorOnceDef::<Ponger>::new(pinger.clone()));
    // ANCHOR_END: reserve

    let system = tree.spawn()?;
    system.wait_started().await?;

    pinger.send(PingMsg::Kick).await?;
    let completed = tokio::time::timeout(Duration::from_secs(5), done_rx.recv()).await?;
    assert_eq!(completed, Some(()));

    system.shutdown(Duration::from_secs(5)).await?;
    println!("ping-pong round trip completed through cyclically wired actors");
    Ok(())
}
// ANCHOR_END: cyclic_wiring
