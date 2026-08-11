use shelterwood::{Backoff, RestartCondition, RestartPolicy};

pub(crate) fn never() -> RestartPolicy {
    RestartPolicy::new(RestartCondition::Never, Backoff::Immediate)
}
