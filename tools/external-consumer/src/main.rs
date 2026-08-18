use shelterwood::CancellationToken;

fn accepts_supported_token(_: &CancellationToken) {}

#[cfg(feature = "installable-seams")]
use shelterwood::{
    ActorIdentity, DynamicRoute, MailboxCell, MailboxControl, MailboxRuntime, MailboxTermination,
    MemberCell, ParentCancellationToken, ScopeCell, actor_ref_from_parts,
};

#[cfg(feature = "sealed-mailbox-seams")]
mod sealed_mailbox_seams {
    use shelterwood_core::{Incarnation, ResolvedMailbox};
    use shelterwood_mailbox::{MailboxControl, MailboxDisposal, MailboxTermination};

    struct ForeignTermination;

    impl MailboxTermination for ForeignTermination {
        fn finish(self: Box<Self>) -> Option<MailboxDisposal> {
            None
        }
    }

    #[derive(Debug)]
    struct ForeignControl;

    impl MailboxControl for ForeignControl {
        fn configure(&self, _: ResolvedMailbox) {}

        fn bind(&self, _: Incarnation) {}

        fn freeze(&self, _: Incarnation) {}

        fn close(&self, _: Incarnation) -> Option<MailboxDisposal> {
            None
        }

        fn prepare_termination(&self) -> Option<Box<dyn MailboxTermination>> {
            None
        }

        #[cfg(debug_assertions)]
        fn bind_order_valid(&self) -> bool {
            true
        }
    }
}

// The sealed-trait bound above is reported the same way whether or not the
// seal module is reachable. This states the underlying property directly: the
// module holding the private supertraits must not be nameable from outside
// its defining crate.
#[cfg(feature = "private-seal-module")]
use shelterwood_mailbox::private::SealedMailboxControl;

#[cfg(feature = "private-seal-module")]
use shelterwood_mailbox::private::SealedMailboxTermination;

fn main() {
    let _ = accepts_supported_token;

    #[cfg(feature = "from-latch")]
    let _ = CancellationToken::from_latch(todo!());
}
