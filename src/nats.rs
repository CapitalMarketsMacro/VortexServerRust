use std::time::Duration;

use anyhow::{Context, anyhow};
use async_nats::{ConnectOptions, Subscriber};
use async_nats::jetstream::{self, consumer};
use futures::StreamExt;
use perspective::client::{Table, UpdateData, UpdateOptions};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::config::{NatsConfig, PayloadFormat, TableConfig, TableSource};
use crate::tables::TableRegistry;

/// How long to wait for the first message on a sourced table before giving up
/// and erroring out at startup. Tables are created on the main task using
/// this first message as the schema seed — matching the example pattern.
const FIRST_MESSAGE_TIMEOUT: Duration = Duration::from_secs(30);

/// Connect to NATS, then for every table with a `source` binding, spawn a
/// consumer task — either a core pub/sub subscriber or a JetStream durable
/// pull consumer, depending on `source.transport`.
pub async fn spawn_consumers(
    nats_cfg: &NatsConfig,
    tables: &[TableConfig],
    registry: TableRegistry,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    if nats_cfg.url.trim().is_empty() {
        tracing::warn!("nats.url is empty — skipping NATS subsystem");
        return Ok(());
    }

    tracing::info!(url = %nats_cfg.url, "connecting to NATS");

    let mut opts = ConnectOptions::new().name(&nats_cfg.name);
    if let Some(creds) = &nats_cfg.credentials_file {
        opts = opts
            .credentials_file(creds)
            .await
            .with_context(|| format!("failed to load NATS credentials file '{creds}'"))?;
    }

    let client = opts
        .connect(&nats_cfg.url)
        .await
        .with_context(|| format!("failed to connect to NATS at {}", nats_cfg.url))?;

    tracing::info!(
        server = %client.server_info().server_name,
        version = %client.server_info().version,
        "NATS connection established"
    );

    let bound_tables: Vec<&TableConfig> =
        tables.iter().filter(|t| t.source.is_some()).collect();

    if bound_tables.is_empty() {
        tracing::info!("no tables have a NATS source binding — connection idle");
        return Ok(());
    }

    // Lazily create a JetStream context only if needed.
    let mut js: Option<jetstream::Context> = None;

    for table_cfg in bound_tables {
        let source = table_cfg
            .source
            .as_ref()
            .expect("filtered above; source must be present");

        let transform = RowTransform::from_config(table_cfg);
        let format = source.format();

        match source {
            TableSource::Core { subject, .. } => {
                let mut subscriber = client
                    .subscribe(subject.clone())
                    .await
                    .with_context(|| format!("failed to subscribe to '{subject}'"))?;

                tracing::info!(
                    table = %table_cfg.name,
                    subject = %subject,
                    transport = "core",
                    "subscribed; waiting for first message to seed schema"
                );

                // Receive the first message in the current (main) task so the
                // table is created in the same task context as the rest of
                // startup. Creating tables inside a spawned task and then
                // updating them while the Axum WS handler creates new sessions
                // on the same Server triggers `touching uninited object` /
                // STATUS_ACCESS_VIOLATION in the C++ engine.
                let first_message = timeout(FIRST_MESSAGE_TIMEOUT, subscriber.next())
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

                let seed_json =
                    transform.transform_to_json_rows(&first_message.payload, format)?;
                let table = registry
                    .get_or_create_from_seed(table_cfg, seed_json)
                    .await?;

                tracing::info!(
                    table = %table_cfg.name,
                    subject = %subject,
                    "table seeded from first NATS message"
                );

                let cfg_clone = table_cfg.clone();
                let token = shutdown.clone();
                tokio::spawn(async move {
                    if let Err(error) = run_core_consumer(
                        subscriber, table, format, transform, cfg_clone, token,
                    )
                    .await
                    {
                        tracing::error!(%error, "core consumer task exited with error");
                    }
                });
            }
            TableSource::Jetstream {
                stream,
                subject,
                consumer: consumer_name,
                ..
            } => {
                let js_ctx = js.get_or_insert_with(|| jetstream::new(client.clone()));

                let js_stream = js_ctx
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
                    transport = "jetstream",
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
                let token = shutdown.clone();
                tokio::spawn(async move {
                    if let Err(error) = run_jetstream_consumer(
                        messages, table, format, transform, cfg_clone, token,
                    )
                    .await
                    {
                        tracing::error!(%error, "jetstream consumer task exited with error");
                    }
                });
            }
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

async fn apply(
    table: &Table,
    payload: &[u8],
    format: PayloadFormat,
    transform: &RowTransform,
) -> anyhow::Result<()> {
    let json = transform.transform_to_json_rows(payload, format)?;
    table
        .update(UpdateData::JsonRows(json), UpdateOptions::default())
        .await
        .map_err(|e| anyhow!("table.update failed: {e}"))?;
    Ok(())
}

/// Per-table message transformation pipeline. Stringifies nested columns,
/// then injects the synthetic composite key column.
#[derive(Clone)]
struct RowTransform {
    stringify_columns: Vec<String>,
    composite_index: Option<Vec<String>>,
}

impl RowTransform {
    fn from_config(cfg: &TableConfig) -> Self {
        Self {
            stringify_columns: cfg.stringify_columns.clone(),
            composite_index: cfg.composite_index.clone(),
        }
    }

    fn is_noop(&self) -> bool {
        self.stringify_columns.is_empty() && self.composite_index.is_none()
    }

    /// Run the transform pipeline and return a JSON-rows string ready to be
    /// fed to Perspective. Arrow payloads are not supported here — they bypass
    /// the JSON pipeline entirely and require a separate code path.
    fn transform_to_json_rows(
        &self,
        payload: &[u8],
        format: PayloadFormat,
    ) -> anyhow::Result<String> {
        if matches!(format, PayloadFormat::Arrow) {
            return Err(anyhow!(
                "arrow payloads are not yet supported by the transform pipeline"
            ));
        }

        // Fast path: noop json_rows passthrough — just validate UTF-8.
        if self.is_noop() && matches!(format, PayloadFormat::JsonRows) {
            let s = std::str::from_utf8(payload)
                .map_err(|e| anyhow!("payload is not valid UTF-8: {e}"))?;
            return Ok(s.to_string());
        }

        let value: serde_json::Value =
            serde_json::from_slice(payload).context("payload is not valid JSON")?;

        let rows: Vec<serde_json::Value> = match format {
            PayloadFormat::JsonRow => vec![value],
            PayloadFormat::JsonRows => value
                .as_array()
                .ok_or_else(|| anyhow!("expected a JSON array for json_rows format"))?
                .clone(),
            PayloadFormat::Arrow => unreachable!(),
        };

        let mut transformed = Vec::with_capacity(rows.len());
        for row in rows {
            let mut obj = match row {
                serde_json::Value::Object(map) => map,
                other => {
                    return Err(anyhow!(
                        "row is not a JSON object (got {})",
                        json_kind(&other)
                    ));
                }
            };

            // 1. Stringify any configured nested columns.
            for col in &self.stringify_columns {
                if let Some(v) = obj.get_mut(col) {
                    if matches!(
                        v,
                        serde_json::Value::Object(_) | serde_json::Value::Array(_)
                    ) {
                        let s = serde_json::to_string(v)?;
                        *v = serde_json::Value::String(s);
                    }
                }
            }

            // 2. Inject synthetic composite primary key.
            if let Some(cols) = &self.composite_index {
                let mut parts = Vec::with_capacity(cols.len());
                for col in cols {
                    let v = obj.get(col).ok_or_else(|| {
                        anyhow!("composite key column '{col}' missing from row")
                    })?;
                    parts.push(stringify_scalar(v));
                }
                let pk = parts.join("|");
                obj.insert("_pk".to_string(), serde_json::Value::String(pk));
            }

            transformed.push(serde_json::Value::Object(obj));
        }

        Ok(serde_json::to_string(&serde_json::Value::Array(
            transformed,
        ))?)
    }
}

fn stringify_scalar(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

fn json_kind(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}
