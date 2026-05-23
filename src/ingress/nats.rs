//! NATS ingress: core pub/sub and JetStream durable pull consumers.
//!
//! Each table runs in a supervised, panic-respawning task tied to its own
//! [`TableSlot`] (and therefore its own isolated Perspective Server). The
//! shared NATS [`Client`] is cheap to clone — every table holds its own
//! handle and subscribes independently.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_nats::jetstream::{self, consumer};
use async_nats::{Client, ConnectOptions, Subscriber};
use futures::StreamExt;
use perspective::client::Table;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use crate::config::{ConnectRetryConfig, NatsConfig, PayloadFormat, TableSource};
use crate::ingress::apply;
use crate::supervisor::supervise;
use crate::tables::TableSlot;
use crate::transform::RowTransform;

/// How long a per-table consumer waits for its first message before logging
/// a "still waiting" warning. The wait itself is unbounded — the table
/// slot stays reserved until data arrives.
const FIRST_MESSAGE_WARN_AFTER: Duration = Duration::from_secs(60);

/// Live NATS connection plus a JetStream context. `Clone` is cheap because
/// the underlying [`Client`] is `Arc`-backed.
#[derive(Clone)]
pub struct NatsContext {
    client: Client,
    jetstream: jetstream::Context,
    /// Retry policy reused for JetStream stream/consumer acquisition so a
    /// table whose stream isn't provisioned yet at startup keeps trying
    /// instead of giving up (same backoff used for the initial connect).
    connect_retry: ConnectRetryConfig,
}

/// Connect to NATS with exponential-backoff retry. Honors
/// `cfg.connect_retry.max_attempts` (`0` = retry forever) and stops early
/// on shutdown.
pub async fn connect(
    cfg: &NatsConfig,
    shutdown: &CancellationToken,
) -> anyhow::Result<NatsContext> {
    if cfg.url.trim().is_empty() {
        return Err(anyhow!("transports.nats.url is empty"));
    }

    let retry = &cfg.connect_retry;
    let mut backoff = retry.initial_backoff();
    let max_backoff = retry.max_backoff();
    let mut attempt: u32 = 0;

    loop {
        attempt += 1;
        let label = retry.format_attempt(attempt);

        tracing::info!(url = %cfg.url, attempt = %label, "connecting to NATS");

        let mut opts = ConnectOptions::new().name(&cfg.name);
        if let Some(creds) = &cfg.credentials_file {
            opts = opts
                .credentials_file(creds)
                .await
                .with_context(|| format!("failed to load NATS credentials file '{creds}'"))?;
        }

        match opts.connect(&cfg.url).await {
            Ok(client) => {
                tracing::info!(
                    server = %client.server_info().server_name,
                    version = %client.server_info().version,
                    "NATS connection established"
                );
                let jetstream = jetstream::new(client.clone());
                return Ok(NatsContext {
                    client,
                    jetstream,
                    connect_retry: cfg.connect_retry.clone(),
                });
            }
            Err(error) => {
                if !retry.is_unlimited() && attempt >= retry.max_attempts {
                    return Err(anyhow!(
                        "NATS connect failed after {} attempts: {}",
                        retry.max_attempts,
                        error
                    ));
                }
                tracing::warn!(
                    url = %cfg.url,
                    attempt = %label,
                    %error,
                    retry_in = ?backoff,
                    "NATS connect failed, will retry"
                );
            }
        }

        tokio::select! {
            _ = sleep(backoff) => {}
            _ = shutdown.cancelled() => {
                return Err(anyhow!("shutdown requested while waiting to retry NATS connect"));
            }
        }

        backoff = (backoff * 2).min(max_backoff);
    }
}

/// Fan out a single table's NATS ingress into a supervised background task.
/// Returns immediately. Per-table panics, errors, and disconnections are
/// handled inside the supervisor and never propagate to the rest of the
/// program.
pub fn start(
    ctx: NatsContext,
    slot: Arc<TableSlot>,
    transform: RowTransform,
    shutdown: CancellationToken,
) {
    let task_name = format!("nats:{}", slot.name);
    supervise(task_name, shutdown.clone(), move || {
        let ctx = ctx.clone();
        let slot = slot.clone();
        let transform = transform.clone();
        let shutdown = shutdown.clone();
        async move {
            if let Err(error) = run_table(ctx, slot, transform, shutdown).await {
                tracing::error!(%error, "NATS ingress task error");
            }
        }
    });
}

async fn run_table(
    ctx: NatsContext,
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
    let format = source.format();

    match source {
        TableSource::NatsCore { subject, .. } => {
            let mut subscriber = ctx
                .client
                .subscribe(subject.clone())
                .await
                .with_context(|| format!("failed to subscribe to '{subject}'"))?;

            tracing::info!(
                table = %slot.name,
                subject = %subject,
                "subscribed; waiting for first message to seed schema"
            );

            let first =
                wait_for_first_core_message(&mut subscriber, &slot.name, &subject, &shutdown)
                    .await?;
            let Some(first) = first else {
                return Ok(()); // shutdown
            };

            let seed_json = transform.transform_to_json_rows(&first.payload, format)?;
            let table = slot.get_or_create_from_seed(seed_json).await?;

            tracing::info!(
                table = %slot.name,
                subject = %subject,
                "table seeded from first NATS message"
            );

            run_core_consumer(subscriber, table, format, transform, &slot, &shutdown).await;
        }
        TableSource::NatsJetstream {
            stream,
            subject,
            consumer: consumer_name,
            ..
        } => {
            // The stream may not exist yet at startup (fresh broker, or a
            // publisher that creates the stream on first publish). Retry with
            // backoff instead of giving up — a Solace/WS table being slow to
            // provision must not permanently kill the Orders table.
            let Some(consumer) = acquire_jetstream_consumer(
                &ctx,
                &stream,
                &subject,
                &consumer_name,
                &slot.name,
                &shutdown,
            )
            .await?
            else {
                return Ok(()); // shutdown during retry
            };

            let mut messages = consumer.messages().await?;

            tracing::info!(
                table = %slot.name,
                stream = %stream,
                subject = %subject,
                "consumer ready; waiting for first message to seed schema"
            );

            let first =
                wait_for_first_jetstream_message(&mut messages, &slot.name, &subject, &shutdown)
                    .await?;
            let Some(first) = first else {
                return Ok(()); // shutdown
            };

            let seed_json = transform.transform_to_json_rows(&first.payload, format)?;
            let table = slot.get_or_create_from_seed(seed_json).await?;

            if let Err(error) = first.ack().await {
                tracing::error!(table = %slot.name, ?error, "failed to ack first message");
            }

            tracing::info!(
                table = %slot.name,
                "table seeded from first JetStream message"
            );

            run_jetstream_consumer(messages, table, format, transform, &slot, &shutdown).await;
        }
        TableSource::Solace { .. } | TableSource::Websocket { .. } => {
            return Err(anyhow!(
                "nats::run_table called for non-NATS source on table '{}'",
                slot.name
            ));
        }
    }

    Ok(())
}

/// Acquire a JetStream pull consumer, retrying transient failures — most
/// importantly the stream not existing yet (a freshly-booted broker, or a
/// publisher that creates the stream lazily on first publish). Uses the same
/// exponential backoff as the initial connect, so a table whose stream isn't
/// provisioned at startup keeps trying instead of silently dying. Returns
/// `Ok(None)` on shutdown, or `Err` only after exhausting `max_attempts`.
async fn acquire_jetstream_consumer(
    ctx: &NatsContext,
    stream: &str,
    subject: &str,
    consumer_name: &str,
    table_name: &str,
    shutdown: &CancellationToken,
) -> anyhow::Result<Option<consumer::Consumer<consumer::pull::Config>>> {
    let retry = &ctx.connect_retry;
    let mut backoff = retry.initial_backoff();
    let max_backoff = retry.max_backoff();
    let mut attempt: u32 = 0;

    loop {
        attempt += 1;
        let label = retry.format_attempt(attempt);

        match try_get_jetstream_consumer(ctx, stream, subject, consumer_name).await {
            Ok(consumer) => {
                tracing::info!(
                    table = %table_name,
                    stream = %stream,
                    attempt = %label,
                    "JetStream consumer ready"
                );
                return Ok(Some(consumer));
            }
            Err(error) => {
                if !retry.is_unlimited() && attempt >= retry.max_attempts {
                    return Err(anyhow!(
                        "JetStream stream/consumer for table '{table_name}' unavailable after \
                         {} attempts: {error}",
                        retry.max_attempts
                    ));
                }
                tracing::warn!(
                    table = %table_name,
                    stream = %stream,
                    attempt = %label,
                    %error,
                    retry_in = ?backoff,
                    "JetStream stream/consumer not ready yet, will retry"
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

/// One attempt to bind the stream and get-or-create the durable consumer.
async fn try_get_jetstream_consumer(
    ctx: &NatsContext,
    stream: &str,
    subject: &str,
    consumer_name: &str,
) -> anyhow::Result<consumer::Consumer<consumer::pull::Config>> {
    let js_stream = ctx
        .jetstream
        .get_stream(stream)
        .await
        .with_context(|| format!("stream '{stream}' not found"))?;

    let consumer_cfg = consumer::pull::Config {
        durable_name: Some(consumer_name.to_string()),
        filter_subject: subject.to_string(),
        ..Default::default()
    };

    js_stream
        .get_or_create_consumer(consumer_name, consumer_cfg)
        .await
        .with_context(|| format!("failed to create consumer '{consumer_name}' on stream '{stream}'"))
}

/// Await the first NATS core message, periodically logging "still waiting"
/// so silent topics are visible. Returns `Ok(None)` on shutdown.
async fn wait_for_first_core_message(
    subscriber: &mut Subscriber,
    table_name: &str,
    subject: &str,
    shutdown: &CancellationToken,
) -> anyhow::Result<Option<async_nats::Message>> {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(None),
            result = timeout(FIRST_MESSAGE_WARN_AFTER, subscriber.next()) => {
                match result {
                    Ok(Some(msg)) => return Ok(Some(msg)),
                    Ok(None) => return Err(anyhow!(
                        "subscriber for '{subject}' closed before first message"
                    )),
                    Err(_elapsed) => {
                        tracing::warn!(
                            table = %table_name,
                            subject = %subject,
                            "still waiting for first message to seed schema"
                        );
                    }
                }
            }
        }
    }
}

async fn wait_for_first_jetstream_message(
    messages: &mut consumer::pull::Stream,
    table_name: &str,
    subject: &str,
    shutdown: &CancellationToken,
) -> anyhow::Result<Option<jetstream::Message>> {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(None),
            result = timeout(FIRST_MESSAGE_WARN_AFTER, messages.next()) => {
                match result {
                    Ok(Some(Ok(msg))) => return Ok(Some(msg)),
                    Ok(Some(Err(e))) => {
                        return Err(anyhow!("failed to receive JetStream message: {e}"));
                    }
                    Ok(None) => {
                        return Err(anyhow!("JetStream consumer closed before first message"));
                    }
                    Err(_elapsed) => {
                        tracing::warn!(
                            table = %table_name,
                            subject = %subject,
                            "still waiting for first JetStream message to seed schema"
                        );
                    }
                }
            }
        }
    }
}

async fn run_core_consumer(
    mut subscriber: Subscriber,
    table: Table,
    format: PayloadFormat,
    transform: RowTransform,
    slot: &TableSlot,
    shutdown: &CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!(table = %slot.name, "shutdown signalled, stopping subscriber");
                return;
            }
            next = subscriber.next() => {
                let Some(message) = next else {
                    tracing::warn!(table = %slot.name, "core subscriber stream ended");
                    return;
                };

                if let Err(error) = apply(&table, &message.payload, format, &transform).await {
                    tracing::error!(table = %slot.name, %error, "failed to apply message");
                }
            }
        }
    }
}

async fn run_jetstream_consumer(
    mut messages: consumer::pull::Stream,
    table: Table,
    format: PayloadFormat,
    transform: RowTransform,
    slot: &TableSlot,
    shutdown: &CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!(table = %slot.name, "shutdown signalled, stopping consumer");
                return;
            }
            next = messages.next() => {
                let Some(message) = next else {
                    tracing::warn!(table = %slot.name, "consumer message stream ended");
                    return;
                };

                let message = match message {
                    Ok(m) => m,
                    Err(error) => {
                        tracing::error!(table = %slot.name, %error, "failed to receive JetStream message");
                        continue;
                    }
                };

                if let Err(error) = apply(&table, &message.payload, format, &transform).await {
                    tracing::error!(table = %slot.name, %error, "failed to apply message");
                    continue;
                }

                if let Err(error) = message.ack().await {
                    tracing::error!(table = %slot.name, ?error, "failed to ack message");
                }
            }
        }
    }
}
