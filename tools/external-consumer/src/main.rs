use shelterwood::CancellationToken;

fn accepts_supported_token(_: &CancellationToken) {}

#[cfg(feature = "installable-seams")]
use shelterwood::{
    ActorIdentity, DynamicRoute, MailboxControl, MailboxRuntime, MailboxTermination,
};

fn main() {
    let _ = accepts_supported_token;

    #[cfg(feature = "from-latch")]
    let _ = CancellationToken::from_latch(todo!());
}
