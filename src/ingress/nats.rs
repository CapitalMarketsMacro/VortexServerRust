//! NATS ingress: core pub/sub and JetStream durable pull consumers.

use std::time::Duration;

use anyhow::{Context, anyhow};
use async_nats::jetstream::{self, consumer};
use async_nats::{Client, ConnectOptions, Subscriber};
use futures::StreamExt;
use perspective::client::Table;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::config::{NatsConfig, PayloadFormat, TableConfig, TableSource};
use crate::ingress::apply;
use crate::tables::TableRegistry;
use crate::transform::RowTransform;

/// How long to wait for the first message on a sourced table before giving up
/// and erroring out at startup.
const FIRST_MESSAGE_TIMEOUT: Duration = Duration::from_secs(30);

/// A live NATS connection plus a lazily-created JetStream context. Shared by
/// every NATS-sourced table.
pub struct NatsContext {
    client: Client,
    jetstream: jetstream::Context,
}

pub async fn connect(cfg: &NatsConfig) -> anyhow::Result<NatsContext> {
    if cfg.url.trim().is_empty() {
        return Err(anyhow!("transports.nats.url is empty"));
    }

    tracing::info!(url = %cfg.url, "connecting to NATS");

    let mut opts = ConnectOptions::new().name(&cfg.name);
    if let Some(creds) = &cfg.credentials_file {
        opts = opts
            .credentials_file(creds)
            .await
            .with_context(|| format!("failed to load NATS credentials file '{creds}'"))?;
    }

    let client = opts
        .connect(&cfg.url)
        .await
        .with_context(|| format!("failed to connect to NATS at {}", cfg.url))?;

    tracing::info!(
        server = %client.server_info().server_name,
        version = %client.server_info().version,
        "NATS connection established"
    );

    let jetstream = jetstream::new(client.clone());

    Ok(NatsContext { client, jetstream })
}

/// Set up ingress for a single table. Subscribes (or attaches a JS consumer),
/// awaits the first message synchronously to seed the table, then spawns the
/// per-table consumer task.
pub async fn start(
    ctx: &NatsContext,
    table_cfg: &TableConfig,
    source: &TableSource,
    transform: RowTransform,
    registry: &TableRegistry,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let format = source.format();

    match source {
        TableSource::NatsCore { subject, .. } => {
            let mut subscriber = ctx
                .client
                .subscribe(subject.clone())
                .await
                .with_context(|| format!("failed to subscribe to '{subject}'"))?;

            tracing::info!(
                table = %table_cfg.name,
                subject = %subject,
                "subscribed; waiting for first message to seed schema"
            );

            let first = timeout(FIRST_MESSAGE_TIMEOUT, subscriber.next())
                .await
                .with_context(|| {
                    format!(
                        "timed out after {:?} waiting for first message on '{subject}' \
                         to seed table '{}'",
                        FIRST_MESSAGE_TIMEOUT, table_cfg.name
                    )
                })?
                .ok_or_else(|| {
                    anyhow!("subscriber for '{subject}' closed before first message")
                })?;

            let seed_json = transform.transform_to_json_rows(&first.payload, format)?;
            let table = registry
                .get_or_create_from_seed(table_cfg, seed_json)
                .await?;

            tracing::info!(
                table = %table_cfg.name,
                subject = %subject,
                "table seeded from first NATS message"
            );

            let cfg_clone = table_cfg.clone();
            tokio::spawn(async move {
                if let Err(error) =
                    run_core_consumer(subscriber, table, format, transform, cfg_clone, shutdown)
                        .await
                {
                    tracing::error!(%error, "core NATS consumer task exited with error");
                }
            });
        }
        TableSource::NatsJetstream {
            stream,
            subject,
            consumer: consumer_name,
            ..
        } => {
            let js_stream = ctx
                .jetstream
                .get_stream(stream)
                .await
                .with_context(|| format!("stream '{stream}' not found"))?;

            let consumer_cfg = consumer::pull::Config {
                durable_name: Some(consumer_name.clone()),
                filter_subject: subject.clone(),
                ..Default::default()
            };

            let consumer = js_stream
                .get_or_create_consumer(consumer_name, consumer_cfg)
                .await
                .with_context(|| {
                    format!("failed to create consumer '{consumer_name}' on stream '{stream}'")
                })?;

            let mut messages = consumer.messages().await?;

            tracing::info!(
                table = %table_cfg.name,
                stream = %stream,
                subject = %subject,
                "consumer ready; waiting for first message to seed schema"
            );

            let first_message = timeout(FIRST_MESSAGE_TIMEOUT, messages.next())
                .await
                .with_context(|| {
                    format!(
                        "timed out after {:?} waiting for first JetStream message \
                         to seed table '{}'",
                        FIRST_MESSAGE_TIMEOUT, table_cfg.name
                    )
                })?
                .ok_or_else(|| anyhow!("JetStream consumer closed before first message"))?
                .context("failed to receive first JetStream message")?;

            let seed_json =
                transform.transform_to_json_rows(&first_message.payload, format)?;
            let table = registry
                .get_or_create_from_seed(table_cfg, seed_json)
                .await?;

            if let Err(error) = first_message.ack().await {
                tracing::error!(table = %table_cfg.name, ?error, "failed to ack first message");
            }

            tracing::info!(
                table = %table_cfg.name,
                "table seeded from first JetStream message"
            );

            let cfg_clone = table_cfg.clone();
            tokio::spawn(async move {
                if let Err(error) = run_jetstream_consumer(
                    messages, table, format, transform, cfg_clone, shutdown,
                )
                .await
                {
                    tracing::error!(%error, "JetStream consumer task exited with error");
                }
            });
        }
        TableSource::Solace { .. } | TableSource::Websocket { .. } => {
            unreachable!("dispatcher routes non-NATS sources elsewhere")
        }
    }

    Ok(())
}

async fn run_core_consumer(
    mut subscriber: Subscriber,
    table: Table,
    format: PayloadFormat,
    transform: RowTransform,
    cfg: TableConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!(table = %cfg.name, "shutdown signalled, stopping subscriber");
                return Ok(());
            }
            next = subscriber.next() => {
                let Some(message) = next else {
                    tracing::warn!(table = %cfg.name, "core subscriber stream ended");
                    return Ok(());
                };

                if let Err(error) = apply(&table, &message.payload, format, &transform).await {
                    tracing::error!(table = %cfg.name, %error, "failed to apply message");
                }
                // Core NATS has no ack semantics.
            }
        }
    }
}

async fn run_jetstream_consumer(
    mut messages: consumer::pull::Stream,
    table: Table,
    format: PayloadFormat,
    transform: RowTransform,
    cfg: TableConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!(table = %cfg.name, "shutdown signalled, stopping consumer");
                return Ok(());
            }
            next = messages.next() => {
                let Some(message) = next else {
                    tracing::warn!(table = %cfg.name, "consumer message stream ended");
                    return Ok(());
                };

                let message = message.context("failed to receive JetStream message")?;

                if let Err(error) = apply(&table, &message.payload, format, &transform).await {
                    tracing::error!(table = %cfg.name, %error, "failed to apply message");
                    continue;
                }

                if let Err(error) = message.ack().await {
                    tracing::error!(table = %cfg.name, ?error, "failed to ack message");
                }
            }
        }
    }
}
