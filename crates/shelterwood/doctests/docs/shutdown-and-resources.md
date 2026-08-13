# Shutdown and resource ownership

Shelterwood uses one escalation ladder everywhere:

```text
cooperative cancellation -> grace expiry -> tidy-abort beat -> hard abort
```

`Shutdown::Abort` is the zero-grace point on that ladder. The shutdown token
still fires strictly before the abort token, and the tidy beat still occurs.
`ExitKind::Aborted { phase }` distinguishes hard abort after an expired
grace (`GracePhase::AfterGrace`) from an abort within grace
(`GracePhase::WithinGrace`). These token transitions are observable inside a
child; lifecycle events report the eventual exit, not separate ladder steps.

## Grace is an upper bound

A graceful child does not receive a guaranteed amount of CPU time after its
shutdown token fires. Its grace is a supervisor-side upper bound. Scheduler
latency and competing work can reduce the time user code observes.

For handler actors, one grace budget covers both delivery of the frozen mailbox
prefix and `on_stop`. With `MailboxShutdown::Drain`, the loop freezes intake,
drains accepted messages, then invokes `on_stop`. With `Discard`, it drops the
prefix and proceeds to `on_stop`. A handler error, panic, or hard abort can
truncate or skip cleanup.

Consequently, `on_stop` is best-effort resource return, not a durability
boundary. Durable correctness must already survive a crash. Size a resource
owner's grace for drain plus close, or choose `Discard` when draining is less
important than allowing the close its full budget.

## Teardown communication uses `try_send`

Ordinary `send` waits through rebind windows. During teardown, that can park
forever against a sibling whose current incarnation has already frozen intake.
Use `try_send` for best-effort shutdown notifications and handle
`NotRunning`, `Full`, and `Terminated` explicitly. Correct teardown must not
depend on such a notification arriving; scope order and cancellation tokens
are the reliable control plane.

Ordered scopes stop children in reverse declaration order, one fully joined
child at a time while the scope driver remains scheduled. Put a slow resource
owner earlier in declaration order so its dependents stop first and its close
runs quiescent. Per-child graces therefore sum in an ordered scope. Dynamic
scopes start all stop ladders together and drain concurrently. The hard-abort
fallback exception is described below.

## Raw actors must implement the drain policy

The framework always freezes raw-actor intake, but the raw loop owns delivery.
It must consult `RawContext::mailbox_shutdown` and use `try_recv` to drain the
frozen prefix when requested:

```rust,no_run
# use shelterwood::{ExitError, ExitResult, MailboxShutdown, RawContext};
# fn handle<T>(_message: T) -> Result<(), ExitError> { Ok(()) }
# async fn drain<M: Send + 'static>(context: &mut RawContext<M>) -> ExitResult {
while let Some(message) = context.recv().await {
    handle(message)?;
}

if context.mailbox_shutdown() == MailboxShutdown::Drain {
    while let Some(message) = context.try_recv() {
        handle(message)?;
    }
}
# Ok(())
# }
```

For a queue mailbox, that prefix is every accepted but undelivered message in
acceptance order. For `latest()`, it is at most the one surviving value.
Draining still shares the child's grace and may be truncated by failure or hard
abort. The high-level `Actor` handler loop performs this protocol itself.

## Choose the resource owner deliberately

An incarnation-owned resource is created by `init` and dropped on restart;
use this when restart should reconnect or repair it. A resource that must
survive restarts belongs outside the incarnation: open it in the host, pass a
cloneable handle through `Args`, resolve system shutdown, then close it in host
code. Host-owned closes run outside child grace budgets.

`run_blocking` is available from normal and stop contexts for blocking work.
The closure receives a cooperative cancellation token. Dropping its returned
future cancels that token, but cannot forcibly stop a thread. After hard abort,
the blocking thread is detached and may continue; Shelterwood never joins it.
A late return value or panic is discarded if its future was dropped. Therefore
blocking operations must be safe to outlive the actor and must not retain
exclusive process resources indefinitely.

If Tokio has already closed its blocking pool during runtime teardown,
Shelterwood reroutes a rejected `run_blocking` operation to a detached
Shelterwood-owned thread. This keeps both the operation and destruction of its
captured state off the submitting runtime thread. If neither Tokio nor a native
fallback thread can start the operation, its captured state still goes through
isolated disposal and awaiting the future reports runtime-teardown cancellation.

Async offloads are different: they are incarnation-owned, are cancelled at the
stop freeze, and never outlive the incarnation. They are intentionally absent
from `StopContext`.

## Owner and runtime lifetime

`System::shutdown(timeout)` consumes the owner, requests teardown, and joins
the root driver before returning. The timeout bounds the cooperative phase;
after it expires Shelterwood escalates stragglers, so destruction of user
futures can make the final return take longer. A zero timeout skips the
cooperative wait but still follows token ordering and the tidy beat.

The budget arms when the targeted incarnation enters its drain, not when the
call is made: a child that cooperates on that first teardown wake, or that is
sitting in a restart backoff window, is not a straggler. An incarnation its
ancestor hard-aborts before it ever reaches drain entry therefore has no
cooperative phase to bound — `shutdown_and_wait` waits on its drop epilogue
and resolves `Ok` rather than reporting a timeout. Neither form of the call
uses this timeout to bound its own return.

Normally each scheduled scope driver joins its children before completing. A
framework driver that misses its abort acknowledgement may instead be
hard-aborted by its ancestor at the tidy-beat backstop. Its synchronous drop
epilogue requests abort for active children but cannot await their join
handles, so deeper task cancellation and user-future destruction may complete
after `System::shutdown` or `shutdown_and_wait` returns. Return guarantees that
the root driver is joined and the target scope epilogue is complete; that
epilogue has requested stop or abort for each directly owned child. It does not
guarantee either a recursive join or completed abort propagation through deeper
fallback boundaries. `run_blocking` threads have the separate detach behavior
described above.

Dropping `System` requests graceful shutdown, but an embedding host should
normally await `shutdown` before tearing down the Tokio runtime. The ambient
runtime must have time enabled. Supervised panic classification additionally
requires `panic = "unwind"`. The standard double-panic exclusion remains: a
destructor that panics while its user future is already unwinding aborts the
process before the supervisor can publish an exit.

Shelterwood's detached disposal is detached from the caller, not from the
runtime's lifetime. On a healthy runtime it uses Tokio's blocking pool, so a
permanently blocked destructor can make the default `Runtime::drop` wait
indefinitely. Embedding hosts that cannot accept that wait must choose Tokio's
`Runtime::shutdown_timeout` or `Runtime::shutdown_background` as a host-level
mitigation, accepting that blocking work may then outlive the runtime. Once
either returns, disposals submitted during teardown may still be in flight on
a Shelterwood-owned thread that nothing joins, so a host that exits the process
immediately afterwards loses those destructors.

A shutdown request through a pre-spawn scope handle is retained as a pending
stop. The first incarnation starts and immediately enters teardown; the timeout
for `shutdown_and_wait` begins when that incarnation starts, because there is
nothing to escalate beforehand. If the membership is terminalized without ever
spawning, the wait resolves as already stopped rather than hanging.
