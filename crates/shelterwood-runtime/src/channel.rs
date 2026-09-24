use std::fmt;

use tokio::sync::{broadcast, mpsc};

/// Result of receiving from a runtime-backed broadcast channel.
pub enum BroadcastReceive<T> {
    Item(T),
    Empty,
    Closed,
    Lagged(u64),
}

/// Publishing half of a bounded runtime-backed broadcast channel.
pub struct BroadcastSender<T>(broadcast::Sender<T>);

/// Per-subscriber receiving half of a bounded runtime-backed broadcast channel.
pub struct BroadcastReceiver<T>(broadcast::Receiver<T>);

pub fn broadcast<T: Clone>(capacity: usize) -> (BroadcastSender<T>, BroadcastReceiver<T>) {
    let (sender, receiver) = broadcast::channel(capacity);
    (BroadcastSender(sender), BroadcastReceiver(receiver))
}

impl<T: Clone> BroadcastSender<T> {
    pub fn subscribe(&self) -> BroadcastReceiver<T> {
        BroadcastReceiver(self.0.subscribe())
    }

    pub fn send(&self, value: T) -> Result<usize, T> {
        self.0.send(value).map_err(|error| error.0)
    }

    pub fn receiver_count(&self) -> usize {
        self.0.receiver_count()
    }
}

impl<T: Clone> BroadcastReceiver<T> {
    pub fn try_receive(&mut self) -> BroadcastReceive<T> {
        match self.0.try_recv() {
            Ok(value) => BroadcastReceive::Item(value),
            Err(broadcast::error::TryRecvError::Empty) => BroadcastReceive::Empty,
            Err(broadcast::error::TryRecvError::Closed) => BroadcastReceive::Closed,
            Err(broadcast::error::TryRecvError::Lagged(dropped)) => {
                BroadcastReceive::Lagged(dropped)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<T> fmt::Debug for BroadcastSender<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BroadcastSender")
            .field("receivers", &self.0.receiver_count())
            .finish()
    }
}

impl<T> fmt::Debug for BroadcastReceiver<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BroadcastReceiver")
            .field("queued", &self.0.len())
            .finish()
    }
}

/// Runtime-neutral publishing half of an unbounded driver event lane.
pub struct UnboundedMpscSender<T>(mpsc::UnboundedSender<T>);

impl<T> Clone for UnboundedMpscSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> UnboundedMpscSender<T> {
    /// Sends one value, returning it when the receive lane is closed.
    pub fn send(&self, value: T) -> Result<(), T> {
        self.0.send(value).map_err(|error| error.0)
    }
}

/// Runtime-neutral receiving half of an unbounded driver event lane.
pub struct UnboundedMpscReceiver<T>(mpsc::UnboundedReceiver<T>);

impl<T> UnboundedMpscReceiver<T> {
    /// Waits for the next value, or returns `None` when every sender is gone.
    pub async fn recv(&mut self) -> Option<T> {
        self.0.recv().await
    }

    /// Receives one immediately available value.
    pub fn try_recv(&mut self) -> Option<T> {
        self.0.try_recv().ok()
    }

    /// Reports whether the receive lane currently contains no values.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub fn unbounded_mpsc<T>() -> (UnboundedMpscSender<T>, UnboundedMpscReceiver<T>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (UnboundedMpscSender(sender), UnboundedMpscReceiver(receiver))
}

#[cfg(test)]
mod tests {
    use crate::{BroadcastReceive, broadcast};

    #[test]
    fn broadcast_wrapper_maps_items_empty_close_and_exact_lag() {
        let (sender, mut receiver) = broadcast(2);
        assert!(matches!(receiver.try_receive(), BroadcastReceive::Empty));
        assert_eq!(sender.send(0_u8), Ok(1));
        assert!(matches!(receiver.try_receive(), BroadcastReceive::Item(0)));

        for value in 1_u8..=5 {
            assert_eq!(sender.send(value), Ok(1));
        }
        assert!(matches!(
            receiver.try_receive(),
            BroadcastReceive::Lagged(3)
        ));
        assert!(matches!(receiver.try_receive(), BroadcastReceive::Item(4)));
        assert!(matches!(receiver.try_receive(), BroadcastReceive::Item(5)));
        assert!(matches!(receiver.try_receive(), BroadcastReceive::Empty));
        drop(sender);
        assert!(matches!(receiver.try_receive(), BroadcastReceive::Closed));
    }

    #[test]
    fn unbounded_sender_returns_the_value_after_receiver_close() {
        let (sender, receiver) = super::unbounded_mpsc();
        drop(receiver);

        assert_eq!(
            sender.send(String::from("returned")),
            Err(String::from("returned"))
        );
    }
}
