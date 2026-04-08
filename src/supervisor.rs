//! Task supervisor: spawn a long-running task with panic-catching and
//! automatic restart, so a bug in one table's ingress pipeline can never
//! silently kill it for the rest of the program's life.
//!
//! - The task body is a `Fn` closure that's called once per run, so each
//!   restart starts fresh (reconnect, re-subscribe, etc).
//! - Panics are caught via `FutureExt::catch_unwind` and logged with the
//!   panic payload, then the task is respawned after a backoff.
//! - Backoff doubles up to a 60s cap to absorb crash loops without burning
//!   CPU. If a task runs successfully for 2 minutes before crashing, the
//!   backoff resets — that's a transient failure, not a deterministic bug.
//! - Cooperatively cancellable via `CancellationToken`. Once shutdown is
//!   signalled, no further restart happens and the supervisor exits.

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use futures::FutureExt;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
/// If a task ran successfully for at least this long before crashing, treat
/// the failure as transient and reset the backoff.
const HEALTHY_RUN_DURATION: Duration = Duration::from_secs(120);

/// Spawn a supervised task. The closure is called once per run; on panic
/// or clean return-with-error, the supervisor backs off and calls it again.
/// On a clean return (Ok / unit), the supervisor exits — that means the
/// task chose to stop (e.g. saw the shutdown token).
pub fn supervise<F, Fut>(name: String, shutdown: CancellationToken, body: F)
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let mut backoff = INITIAL_BACKOFF;

        loop {
            if shutdown.is_cancelled() {
                tracing::info!(task = %name, "supervisor: shutdown signalled, exiting");
                return;
            }

            let started_at = Instant::now();
            let outcome = AssertUnwindSafe(body()).catch_unwind().await;
            let ran_for = started_at.elapsed();

            // Long, healthy run → reset the backoff so future failures
            // don't immediately hit the 60s cap.
            if ran_for >= HEALTHY_RUN_DURATION {
                backoff = INITIAL_BACKOFF;
            }

            match outcome {
                Ok(()) => {
                    // Task chose to stop on its own (typically a clean
                    // shutdown). Don't restart.
                    tracing::info!(
                        task = %name,
                        ran_for = ?ran_for,
                        "supervisor: task exited cleanly"
                    );
                    return;
                }
                Err(panic_payload) => {
                    tracing::error!(
                        task = %name,
                        panic = %panic_payload_string(&panic_payload),
                        ran_for = ?ran_for,
                        restart_in = ?backoff,
                        "supervisor: task panicked, will restart"
                    );
                }
            }

            tokio::select! {
                _ = sleep(backoff) => {}
                _ = shutdown.cancelled() => {
                    tracing::info!(task = %name, "supervisor: shutdown signalled during backoff");
                    return;
                }
            }

            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    });
}

fn panic_payload_string(payload: &Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<panic payload not a string>".to_string()
    }
}
