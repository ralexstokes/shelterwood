use shelterwood::{Backoff, RestartCondition, RestartPolicy};

pub fn never() -> RestartPolicy {
    RestartPolicy::new(RestartCondition::Never, Backoff::Immediate)
}
