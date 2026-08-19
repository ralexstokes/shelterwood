use shelterwood::{ChildId, Exit, StartupError, StartupFailureCause};

pub(crate) fn startup_failed_child(error: StartupError) -> (ChildId, Exit) {
    let StartupError::StartupFailed(failure) = error else {
        panic!("expected structured startup failure, got {error:?}")
    };
    match failure.cause {
        StartupFailureCause::Child { id, exit, .. } => (id, exit),
        cause => panic!("expected child startup failure, got {cause:?}"),
    }
}
