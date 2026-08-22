//! Runtime-neutral capabilities shared by the façade and runtime adapter.

use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

/// A type-erased runtime sleep.
pub type BoxedSleep = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// A type-erased value carried through a runtime one-shot.
pub type ErasedValue = Box<dyn Any + Send + 'static>;

/// Runtime-neutral single-delivery send capability.
#[doc(hidden)]
pub trait ErasedOneShotSender: Send {
    fn send(self: Box<Self>, value: ErasedValue) -> Result<(), ErasedValue>;
}

/// Runtime-neutral single-delivery receive capability.
#[doc(hidden)]
pub trait ErasedOneShotReceiver: Send {
    fn poll_receive(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<ErasedValue>>;
    fn close_and_poll_receive(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> ErasedOneShotClose;
    fn close(self: Pin<&mut Self>);
    fn close_and_take(self: Pin<&mut Self>) -> Option<ErasedValue>;
}

/// The result of closing and polling a runtime one-shot receiver.
#[doc(hidden)]
pub enum ErasedOneShotClose {
    Value(ErasedValue),
    SenderClosed,
    Empty,
    Pending,
}

/// Runtime-neutral one-shot change notification.
#[doc(hidden)]
pub trait MailboxSignal: Send + Sync {
    fn pulse(&self);
    fn watcher(&self) -> Box<dyn MailboxSignalWatcher>;
}

/// Runtime-neutral wait side of [`MailboxSignal`].
#[doc(hidden)]
pub trait MailboxSignalWatcher: Send {
    fn changed(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// The runtime capabilities needed by the mailbox shell.
///
/// The public façade installs one object per mailbox. Type erasure keeps the
/// adapter out of `ActorRef`'s type parameters while the capability interface
/// remains free of Tokio and every other concrete executor.
///
/// # Implementation boundary
///
/// This is an implementation seam for Shelterwood's runtime-adapter crate,
/// not a user-supplied executor interface. Foreign implementations and direct
/// construction of mailbox cells are outside the supported façade contract.
#[doc(hidden)]
pub trait MailboxRuntime: Send + Sync {
    fn oneshot(
        &self,
    ) -> (
        Box<dyn ErasedOneShotSender>,
        Pin<Box<dyn ErasedOneShotReceiver>>,
    );
    fn signal(&self) -> Arc<dyn MailboxSignal>;
    fn dispose(&self, value: Box<dyn Send + 'static>);
    fn now(&self) -> Instant;
    fn sleep_until(&self, deadline: Option<Instant>) -> BoxedSleep;
}
