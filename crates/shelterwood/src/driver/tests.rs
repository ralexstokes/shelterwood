mod construction;
mod dynamic;
mod events;
mod observation;
mod payload;
mod reports;
mod retained_outcomes;
mod shutdown;
mod startup;
mod support;
mod system_join;
mod terminalization;
mod wait_scope;

pub(crate) use dynamic::exercise_queued_fused_drop_before_exit_dispatch;
