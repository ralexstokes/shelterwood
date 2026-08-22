use shelterwood::CancellationToken;

#[cfg(feature = "exit-new")]
use shelterwood::{Cancellation, Exit, ExitKind};

#[cfg(feature = "lifecycle-capacity")]
use shelterwood::LIFECYCLE_EVENT_CAPACITY;

fn accepts_supported_token(_: &CancellationToken) {}

#[cfg(feature = "installable-seams")]
use shelterwood::{
    ActorIdentity, DynamicRoute, ErasedOneShotClose, ErasedOneShotReceiver, ErasedOneShotSender,
    MailboxCell, MailboxControl, MailboxEffectQueue, MailboxEffectSink, MailboxRuntime,
    MailboxSignal, MailboxSignalWatcher, MailboxTermination, MemberCell, ParentCancellationToken,
    ProxiedPoll, ProxiedSleep, ScopeCell, WakerAction, WakerEffects, WakerProxy, WakerSlot,
    actor_ref_from_parts,
};

fn main() {
    let _ = accepts_supported_token;

    #[cfg(feature = "exit-new")]
    let _ = Exit::new(ExitKind::Completed, Cancellation::NotObserved);

    #[cfg(feature = "from-latch")]
    let _ = CancellationToken::from_latch(todo!());

    #[cfg(feature = "lifecycle-capacity")]
    let _ = LIFECYCLE_EVENT_CAPACITY;
}
