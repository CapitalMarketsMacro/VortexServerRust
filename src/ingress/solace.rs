//! Solace PubSub+ direct-topic ingress.
//!
//! Mirrors the NATS Core ingress pattern. Each Solace-sourced table runs
//! as an independent, supervised task with its own libsolclient session.
//! One TCP connection per table — bulkhead isolation matching the rest
//! of vortex-server's per-table architecture.
//!
//! ## Why a session per table?
//!
//! libsolclient delivers messages on a single context thread per session.
//! Sharing one session across many tables would create a per-process
//! callback path that demuxes by topic — re-introducing the serialization
//! point the per-table model is built to avoid. N TCP connections (one per
//! table) is cheap, bounded by table count, and keeps a slow Perspective
//! writer on one table from stalling another.
//!
//! ## Reconnect strategy
//!
//! libsolclient handles reconnection transparently when we build the
//! session with `reapply_subscriptions(true)` and `reconnect_retries(-1)`,
//! so there is no Rust-side reconnect loop inside the consumer. If
//! libsolclient gives up (a fatal `SessionEvent::DownError`), the task
//! returns an error and [`supervise`] respawns it — which rebuilds the
//! session through [`build_session_with_retry`] using our own backoff.
//!
//! ## Backpressure and the C callback thread
//!
//! The `on_message` callback is invoked on the libsolclient context
//! thread, which must not block. Payloads are copied into a bounded
//! tokio mpsc channel; if the channel is full (slow Perspective writer
//! or stalled table), messages are dropped with a counted warning. Direct
//! delivery has no acks — dropping under sustained backpressure matches
//! Solace's own transient semantics and is preferred to OOMing the process.
//!
//! ## Guaranteed delivery (queue / topic-endpoint binding)
//!
//! Not implemented in this pass. The upstream `solace-rs` crate does not
//! expose `solClient_session_createFlow` at the high-level API yet, so
//! queue binding would require extending `solace-rs-sys` (or declaring
//! the extra `extern "C"` functions locally). Direct topic subscription
//! is the only Solace-side variant currently in [`TableSource`].

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context as _, anyhow};
use perspective::client::Table;
use solace_rs::message::{InboundMessage, Message};
use solace_rs::session::{Session, SessionEvent};
use solace_rs::{Context, SolaceLogLevel};
use solace_rs_sys as ffi;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use crate::config::{PayloadFormat, SolaceConfig, TableSource};
use crate::ingress::apply;
use crate::supervisor::supervise;
use crate::tables::TableSlot;
use crate::transform::RowTransform;

/// How long a per-table consumer waits for its first message before logging
/// a "still waiting" warning. The wait itself is unbounded — the table
/// slot stays reserved until data arrives.
const FIRST_MESSAGE_WARN_AFTER: Duration = Duration::from_secs(60);

/// Per-table capacity of the libsolclient-callback → tokio-task bridge.
/// Sized to absorb seconds of bursty market data before backpressure
/// engages. Larger values trade memory for jitter tolerance.
const MESSAGE_CHANNEL_CAPACITY: usize = 16_384;

/// Capacity of the session-event channel. Lifecycle events arrive at a
/// low rate (connect / reconnect / down) so a small buffer suffices.
const EVENT_CHANNEL_CAPACITY: usize = 64;

/// Emit one "messages dropped" warning per N drops to avoid log floods
/// during sustained backpressure.
const DROP_LOG_EVERY: usize = 1_000;

/// libsolclient connect timeout per attempt. The wrapping retry loop adds
/// its own backoff and attempt cap on top.
const CONNECT_TIMEOUT_MS: u64 = 30_000;

/// libsolclient process-global handle. Cheap to clone (Arc-backed in
/// solace-rs); shared across all per-table tasks. Sessions are still 1:1
/// per table.
#[derive(Clone)]
pub struct SolaceContext {
    cfg: Arc<SolaceConfig>,
    ctx: Context,
}

/// Validate the Solace config block and initialize libsolclient. Returns
/// a [`SolaceContext`] cheaply cloned into each per-table task. Does not
/// open any TCP connection — that happens per-table inside
/// [`build_session_with_retry`].
pub fn connect(cfg: &SolaceConfig) -> anyhow::Result<SolaceContext> {
    if cfg.host.trim().is_empty() {
        return Err(anyhow!("transports.solace.host is empty"));
    }
    if cfg.username.trim().is_empty() {
        return Err(anyhow!("transports.solace.username is empty"));
    }

    let ctx = Context::new(SolaceLogLevel::Warning)
        .map_err(|e| anyhow!("failed to initialize libsolclient: {e:?}"))?;

    tracing::info!(
        host = %cfg.host,
        vpn = %cfg.vpn,
        "libsolclient initialized; sessions will be created per table"
    );

    Ok(SolaceContext {
        cfg: Arc::new(cfg.clone()),
        ctx,
    })
}

/// Fan out a single Solace-sourced table's ingress into a supervised
/// background task. Returns immediately. Per-table panics, errors, and
/// session-down events are handled inside the supervisor and never
/// propagate to the rest of the program.
pub fn start(
    ctx: SolaceContext,
    slot: Arc<TableSlot>,
    transform: RowTransform,
    shutdown: CancellationToken,
) {
    let task_name = format!("solace:{}", slot.name);
    supervise(task_name, shutdown.clone(), move || {
        let ctx = ctx.clone();
        let slot = slot.clone();
        let transform = transform.clone();
        let shutdown = shutdown.clone();
        async move {
            if let Err(error) = run_table(ctx, slot, transform, shutdown).await {
                tracing::error!(%error, "Solace ingress task error");
            }
        }
    });
}

async fn run_table(
    ctx: SolaceContext,
    slot: Arc<TableSlot>,
    transform: RowTransform,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let source = slot
        .config
        .source
        .as_ref()
        .ok_or_else(|| anyhow!("table '{}' has no source", slot.name))?
        .clone();

    let (topic, format) = match source {
        TableSource::Solace { topic, format } => (topic, format),
        _ => {
            return Err(anyhow!(
                "solace::run_table called for non-Solace source on table '{}'",
                slot.name
            ));
        }
    };

    // Build the libsolclient session (with its callbacks bridged into the
    // returned channels) and subscribe to our topic. Reconnect inside the
    // session is handled by libsolclient itself; a fatal DownError
    // bubbles up here and the supervisor restarts us.
    let SessionBundle {
        session,
        mut msg_rx,
        mut event_rx,
        drop_counter,
    } = match build_session_with_retry(&ctx, &slot.name, &shutdown).await? {
        Some(b) => b,
        None => return Ok(()), // shutdown during retry
    };

    session
        .subscribe(topic.as_str())
        .with_context(|| format!("failed to subscribe to '{topic}'"))?;

    tracing::info!(
        table = %slot.name,
        topic = %topic,
        "subscribed; waiting for first message to seed schema"
    );

    let Some(first_payload) =
        wait_for_first_message(&mut msg_rx, &mut event_rx, &slot.name, &topic, &shutdown).await?
    else {
        return Ok(()); // shutdown
    };

    let seed_json = transform.transform_to_json_rows(&first_payload, format)?;
    let table = slot.get_or_create_from_seed(seed_json).await?;

    tracing::info!(
        table = %slot.name,
        topic = %topic,
        "table seeded from first Solace message"
    );

    let outcome = run_consumer(
        &mut msg_rx,
        &mut event_rx,
        &table,
        format,
        &transform,
        &slot,
        &drop_counter,
        &shutdown,
    )
    .await;

    // Explicitly drop the session before returning so the C-side
    // disconnect+destroy happens before supervise() respawns us.
    drop(session);

    match outcome {
        ConsumerOutcome::Shutdown => Ok(()),
        ConsumerOutcome::SessionDown(reason) => Err(anyhow!(
            "Solace session for table '{}' went down: {reason}",
            slot.name
        )),
        ConsumerOutcome::ChannelClosed => Err(anyhow!(
            "Solace message channel for table '{}' closed unexpectedly",
            slot.name
        )),
    }
}

/// Carrier for everything a per-table task needs to consume from a built
/// session. `session` MUST outlive `msg_rx` / `event_rx` — dropping it
/// closes both channels by dropping the C-callback-held Senders.
struct SessionBundle {
    /// Boxed to erase the two closure type parameters so the bundle is a
    /// concrete type. The session is alive as long as this box exists.
    session: Box<dyn SessionHandle>,
    msg_rx: mpsc::Receiver<Vec<u8>>,
    event_rx: mpsc::Receiver<SessionEvent>,
    /// Shared counter of messages dropped by the callback because the
    /// channel was full. Read for periodic warn logs.
    drop_counter: Arc<AtomicUsize>,
}

/// Object-safe slice of the session API we need from the consumer. Lets
/// us hide the two generic closure type parameters behind a `Box<dyn>`.
trait SessionHandle: Send {
    fn subscribe(&self, topic: &str) -> anyhow::Result<()>;
}

impl<M, E> SessionHandle for Session<'static, M, E>
where
    M: FnMut(InboundMessage) + Send + 'static,
    E: FnMut(SessionEvent) + Send + 'static,
{
    fn subscribe(&self, topic: &str) -> anyhow::Result<()> {
        Session::subscribe(self, topic)
            .map_err(|e| anyhow!("solace subscribe('{topic}') failed: {e}"))
    }
}

/// Build (and connect) a libsolclient session for one table, with
/// exponential-backoff retry against `cfg.connect_retry`. Returns
/// `Ok(None)` on shutdown. The returned bundle carries channels into
/// which the session's C callbacks push messages and events.
async fn build_session_with_retry(
    ctx: &SolaceContext,
    table_name: &str,
    shutdown: &CancellationToken,
) -> anyhow::Result<Option<SessionBundle>> {
    let retry = &ctx.cfg.connect_retry;
    let mut backoff = retry.initial_backoff();
    let max_backoff = retry.max_backoff();
    let mut attempt: u32 = 0;

    loop {
        attempt += 1;
        let label = retry.format_attempt(attempt);

        tracing::info!(
            table = %table_name,
            host = %ctx.cfg.host,
            attempt = %label,
            "building Solace session"
        );

        match build_session_once(ctx, table_name) {
            Ok(bundle) => {
                tracing::info!(
                    table = %table_name,
                    host = %ctx.cfg.host,
                    attempt = %label,
                    "Solace session established"
                );
                return Ok(Some(bundle));
            }
            Err(error) => {
                if !retry.is_unlimited() && attempt >= retry.max_attempts {
                    return Err(anyhow!(
                        "Solace session build failed after {} attempts for table '{}': {error}",
                        retry.max_attempts,
                        table_name
                    ));
                }
                tracing::warn!(
                    table = %table_name,
                    host = %ctx.cfg.host,
                    attempt = %label,
                    %error,
                    retry_in = ?backoff,
                    "Solace session build failed, will retry"
                );
            }
        }

        tokio::select! {
            _ = sleep(backoff) => {}
            _ = shutdown.cancelled() => return Ok(None),
        }

        backoff = (backoff * 2).min(max_backoff);
    }
}

/// Single session-build attempt: wires up the C callbacks to bounded
/// mpsc channels and returns the bundle. Blocking work happens inside
/// libsolclient (synchronous TCP connect) — caller may want to wrap in
/// `spawn_blocking` if that becomes a problem, though in practice the
/// connect timeout (`CONNECT_TIMEOUT_MS`) bounds the wait.
fn build_session_once(ctx: &SolaceContext, table_name: &str) -> anyhow::Result<SessionBundle> {
    let cfg = &*ctx.cfg;

    let (msg_tx, msg_rx) = mpsc::channel::<Vec<u8>>(MESSAGE_CHANNEL_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel::<SessionEvent>(EVENT_CHANNEL_CAPACITY);
    let drop_counter = Arc::new(AtomicUsize::new(0));

    let on_message = {
        let msg_tx = msg_tx.clone();
        let drop_counter = drop_counter.clone();
        let table_name = table_name.to_string();
        move |message: InboundMessage| {
            // Copy the payload out of the C-owned message immediately so
            // we can drop the InboundMessage (and its C allocation) as
            // soon as the callback returns. Avoids pinning C memory in
            // the channel queue.
            let payload = match extract_payload(&message) {
                Some(p) => p,
                None => return, // no usable body
            };

            // try_send is non-blocking, which is mandatory here — we're
            // running on the libsolclient context thread and must not
            // park. Drop on Full (no backpressure into the C layer for
            // direct delivery) and surface as a counted warn.
            if let Err(mpsc::error::TrySendError::Full(_)) = msg_tx.try_send(payload) {
                let prev = drop_counter.fetch_add(1, Ordering::Relaxed);
                if prev.is_multiple_of(DROP_LOG_EVERY) {
                    tracing::warn!(
                        table = %table_name,
                        dropped_total = prev + 1,
                        "Solace message dropped: ingress channel full \
                         (downstream Perspective writer is slower than feed)"
                    );
                }
            }
            // Closed sender = consumer task gone; nothing to do, the
            // session will be dropped shortly after.
        }
    };

    let on_event = {
        let event_tx = event_tx.clone();
        let table_name = table_name.to_string();
        move |event: SessionEvent| {
            // Best-effort forward to the consumer task. Lifecycle events
            // are low-volume so Full should be rare; drop silently if it
            // happens — the consumer will see channel closure on the
            // next real failure.
            if let Err(mpsc::error::TrySendError::Full(ev)) = event_tx.try_send(event) {
                tracing::warn!(
                    table = %table_name,
                    event = %ev,
                    "Solace event channel full; dropping event"
                );
            }
        }
    };

    let session = ctx
        .ctx
        .session_builder()
        .host_name(cfg.host.clone())
        .vpn_name(cfg.vpn.clone())
        .username(cfg.username.clone())
        .password(cfg.password.clone())
        .client_name(cfg.client_name.clone())
        .connect_timeout_ms(CONNECT_TIMEOUT_MS)
        // libsolclient-driven reconnect: keep trying forever, with a
        // bounded wait between attempts, and re-apply subscriptions
        // automatically when the link comes back. This is the "solid"
        // bit — transient broker hiccups don't bounce our table.
        .reconnect_retries(-1)
        .reconnect_retry_wait_ms(3_000)
        .reapply_subscriptions(true)
        .tcp_nodelay(true)
        .generate_rcv_timestamps(true)
        .on_message(on_message)
        .on_event(on_event)
        .build()
        .map_err(|e| anyhow!("solace session build failed: {e}"))?;

    Ok(SessionBundle {
        session: Box::new(session),
        msg_rx,
        event_rx,
        drop_counter,
    })
}

/// Wait for the first payload to arrive (so we can infer the table
/// schema), with periodic "still waiting" warnings and event-driven
/// early-exit on a fatal session-down. Returns `Ok(None)` on shutdown.
async fn wait_for_first_message(
    msg_rx: &mut mpsc::Receiver<Vec<u8>>,
    event_rx: &mut mpsc::Receiver<SessionEvent>,
    table_name: &str,
    topic: &str,
    shutdown: &CancellationToken,
) -> anyhow::Result<Option<Vec<u8>>> {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(None),
            event = event_rx.recv() => {
                if let Some(ev) = event {
                    handle_event_for_seed(ev, table_name, topic)?;
                }
                // Channel closed (None) means the session was dropped
                // before delivering anything — let the timeout branch
                // fire or the next loop iteration surface the situation.
            }
            result = timeout(FIRST_MESSAGE_WARN_AFTER, msg_rx.recv()) => {
                match result {
                    Ok(Some(payload)) => return Ok(Some(payload)),
                    Ok(None) => return Err(anyhow!(
                        "Solace message channel for table '{table_name}' \
                         closed before first message on topic '{topic}'"
                    )),
                    Err(_elapsed) => {
                        tracing::warn!(
                            table = %table_name,
                            topic = %topic,
                            "still waiting for first Solace message to seed schema"
                        );
                    }
                }
            }
        }
    }
}

/// Translate session lifecycle events seen while waiting for the seed
/// message. Anything fatal returns an error so the supervisor restarts
/// with a fresh session.
fn handle_event_for_seed(
    event: SessionEvent,
    table_name: &str,
    topic: &str,
) -> anyhow::Result<()> {
    match event {
        SessionEvent::UpNotice
        | SessionEvent::ReconnectedNotice
        | SessionEvent::SubscriptionOk => {
            tracing::info!(table = %table_name, topic = %topic, %event, "Solace session event");
            Ok(())
        }
        SessionEvent::ReconnectingNotice => {
            tracing::warn!(table = %table_name, topic = %topic, "Solace session reconnecting");
            Ok(())
        }
        SessionEvent::DownError | SessionEvent::ConnectFailedError => {
            Err(anyhow!("Solace session fatal event before first message: {event}"))
        }
        other => {
            tracing::debug!(table = %table_name, topic = %topic, event = %other, "Solace event");
            Ok(())
        }
    }
}

enum ConsumerOutcome {
    Shutdown,
    SessionDown(String),
    ChannelClosed,
}

/// Steady-state consumer loop. Multiplexes the message channel, the
/// event channel, and the shutdown token. Exits cleanly on shutdown,
/// or with an error variant when the session is gone (so the caller
/// can rebuild via the supervisor).
#[allow(clippy::too_many_arguments)]
async fn run_consumer(
    msg_rx: &mut mpsc::Receiver<Vec<u8>>,
    event_rx: &mut mpsc::Receiver<SessionEvent>,
    table: &Table,
    format: PayloadFormat,
    transform: &RowTransform,
    slot: &TableSlot,
    drop_counter: &Arc<AtomicUsize>,
    shutdown: &CancellationToken,
) -> ConsumerOutcome {
    let mut last_dropped_logged: usize = 0;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!(table = %slot.name, "shutdown signalled, stopping Solace consumer");
                let total_dropped = drop_counter.load(Ordering::Relaxed);
                if total_dropped > 0 {
                    tracing::warn!(
                        table = %slot.name,
                        dropped_total = total_dropped,
                        "Solace consumer dropped messages over its lifetime due to backpressure"
                    );
                }
                return ConsumerOutcome::Shutdown;
            }
            event = event_rx.recv() => {
                let Some(ev) = event else {
                    // Event channel closed = session destroyed externally.
                    return ConsumerOutcome::ChannelClosed;
                };
                match ev {
                    SessionEvent::DownError | SessionEvent::ConnectFailedError => {
                        tracing::error!(
                            table = %slot.name,
                            event = %ev,
                            "Solace session fatal; will rebuild via supervisor"
                        );
                        return ConsumerOutcome::SessionDown(ev.to_string());
                    }
                    SessionEvent::ReconnectingNotice => {
                        tracing::warn!(table = %slot.name, "Solace session reconnecting");
                    }
                    SessionEvent::ReconnectedNotice => {
                        tracing::info!(
                            table = %slot.name,
                            "Solace session reconnected (subscriptions reapplied)"
                        );
                    }
                    SessionEvent::SubscriptionError => {
                        tracing::error!(
                            table = %slot.name,
                            "Solace subscription error event"
                        );
                    }
                    other => {
                        tracing::debug!(table = %slot.name, event = %other, "Solace event");
                    }
                }
            }
            next = msg_rx.recv() => {
                let Some(payload) = next else {
                    // Message channel closed = session destroyed externally.
                    return ConsumerOutcome::ChannelClosed;
                };

                if let Err(error) = apply(table, &payload, format, transform).await {
                    tracing::error!(
                        table = %slot.name,
                        %error,
                        "failed to apply Solace message"
                    );
                }

                // Surface accumulated drops once every block. Cheap atomic
                // load on the hot path; only emits when drops happened.
                let total_dropped = drop_counter.load(Ordering::Relaxed);
                if total_dropped >= last_dropped_logged + DROP_LOG_EVERY {
                    last_dropped_logged = total_dropped;
                    tracing::warn!(
                        table = %slot.name,
                        dropped_total = total_dropped,
                        "Solace ingress dropping under sustained backpressure"
                    );
                }
            }
        }
    }
}

/// Pull the message body out of an `InboundMessage` regardless of how the
/// publisher framed it. libsolclient stores a "text" attachment (anything
/// set via `solClient_msg_setBinaryAttachmentString`, including everything
/// published through Solace's REST gateway) as a small SDT envelope —
/// `solClient_msg_getBinaryAttachmentPtr` returns the raw envelope bytes,
/// not the underlying string, which then fails to parse as JSON. Calling
/// `solClient_msg_getBinaryAttachmentString` unwraps that envelope to a
/// plain C string; if the attachment isn't an SDT string we fall back to
/// the raw binary path used by clients that publish via
/// `setBinaryAttachmentPtr` (e.g. NATS-style raw JSON bodies).
fn extract_payload(message: &InboundMessage) -> Option<Vec<u8>> {
    // SAFETY: get_raw_message_ptr returns a non-null, owned C pointer
    // that is valid for the duration of `message`. We pass it to two C
    // accessors that only read the message; we do not store the pointer
    // or the returned string pointer past this function.
    unsafe {
        let mut str_ptr: *const std::os::raw::c_char = std::ptr::null();
        let rc =
            ffi::solClient_msg_getBinaryAttachmentString(message.get_raw_message_ptr(), &mut str_ptr);
        if rc == ffi::solClient_returnCode_SOLCLIENT_OK && !str_ptr.is_null() {
            return Some(std::ffi::CStr::from_ptr(str_ptr).to_bytes().to_vec());
        }
    }

    // Not an SDT string — fall back to the raw binary attachment.
    match message.get_payload() {
        Ok(Some(slice)) => Some(slice.to_vec()),
        _ => None,
    }
}
