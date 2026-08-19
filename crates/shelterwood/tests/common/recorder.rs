use shelterwood::{ExitResult, MailboxShutdown, RawActor, RawContext};

pub(crate) trait MessageRecorder: Send + 'static {
    type Message: Send + 'static;

    fn record(&mut self, message: Self::Message);
}

pub(crate) struct GatedRecorder<R> {
    gate: Option<super::ReleaseGate>,
    recorder: R,
    drain: bool,
}

impl<R> GatedRecorder<R> {
    pub(crate) fn new(gate: Option<super::ReleaseGate>, recorder: R) -> Self {
        Self {
            gate,
            recorder,
            drain: false,
        }
    }

    pub(crate) fn drain_on_shutdown(mut self) -> Self {
        self.drain = true;
        self
    }
}

impl<R: MessageRecorder> RawActor for GatedRecorder<R> {
    type Msg = R::Message;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        if let Some(gate) = self.gate.take() {
            gate.wait().await;
        }
        while let Some(message) = context.recv().await {
            self.recorder.record(message);
        }
        if self.drain && context.mailbox_shutdown() == MailboxShutdown::Drain {
            while let Some(message) = context.try_recv() {
                self.recorder.record(message);
            }
        }
        Ok(())
    }
}
