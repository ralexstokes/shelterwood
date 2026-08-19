mod construction;
mod dynamic;
mod events;
mod exhaustion;
mod observation;
mod payload;
mod reports;
mod shutdown;
mod startup;
mod support;
mod terminalization;

pub(crate) use dynamic::exercise_queued_fused_drop_before_exit_dispatch;
