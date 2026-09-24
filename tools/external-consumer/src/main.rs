use shelterwood::CancellationToken;

#[cfg(feature = "exit-new")]
use shelterwood::{Cancellation, Exit, ExitKind};

#[cfg(feature = "lifecycle-capacity")]
use shelterwood::LIFECYCLE_EVENT_CAPACITY;

fn accepts_supported_token(_: &CancellationToken) {}

// A private supertrait's methods are still callable through a public generic
// bound. The conversion must require a capability unavailable to this crate.
#[cfg(feature = "subtree-ref-conversion")]
fn retype_scope<T: shelterwood::Subtree>(scope: shelterwood::ScopeRef) -> T::Ref {
    T::make_ref(scope)
}

#[cfg(feature = "installable-seams")]
use shelterwood::{
    BoxedSleep, DynamicRoute, MailboxCell, MailboxControl, MailboxEffectQueue, MailboxEffectSink,
    MailboxTermination, MemberCell, ParentCancellationToken, ProxiedPoll, ProxiedSleep, ScopeCell,
    WakerAction, WakerEffects, WakerSlot, actor_ref_from_parts,
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
