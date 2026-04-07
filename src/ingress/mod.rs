//! Per-table ingress: subscribe to a transport, seed the table from the first
//! message, then keep streaming updates into it.
//!
//! Each transport lives in its own submodule. The dispatcher in this file
//! walks the configured tables, validates that the required transport is
//! configured, and hands each table off to the right submodule.

use anyhow::{Context, anyhow};
use perspective::client::{Table, UpdateData, UpdateOptions};
use tokio_util::sync::CancellationToken;

use crate::config::{AppConfig, PayloadFormat, TableConfig, TableSource};
use crate::tables::TableRegistry;
use crate::transform::RowTransform;

mod nats;
mod solace;
mod websocket;

/// Walk all tables that have an ingress source, validate transport
/// configuration, then for each one: connect, seed the table from the first
/// message on the main task, and spawn a per-table consumer.
pub async fn spawn_consumers(
    config: &AppConfig,
    registry: TableRegistry,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let bound_tables: Vec<&TableConfig> = config
        .tables
        .iter()
        .filter(|t| t.source.is_some())
        .collect();

    if bound_tables.is_empty() {
        tracing::info!("no tables have an ingress source binding");
        return Ok(());
    }

    // Build whichever transport contexts are referenced by at least one table.
    let nats_ctx = if bound_tables.iter().any(|t| {
        matches!(
            t.source.as_ref().unwrap(),
            TableSource::NatsCore { .. } | TableSource::NatsJetstream { .. }
        )
    }) {
        let nats_cfg = config
            .transports
            .nats
            .as_ref()
            .ok_or_else(|| anyhow!("at least one table uses NATS but [transports.nats] is not configured"))?;
        Some(nats::connect(nats_cfg).await?)
    } else {
        None
    };

    if bound_tables
        .iter()
        .any(|t| matches!(t.source.as_ref().unwrap(), TableSource::Solace { .. }))
    {
        let solace_cfg = config
            .transports
            .solace
            .as_ref()
            .ok_or_else(|| anyhow!("at least one table uses Solace but [transports.solace] is not configured"))?;
        // Currently fails fast — see solace.rs.
        solace::validate_config(solace_cfg)?;
    }

    for table_cfg in bound_tables {
        let source = table_cfg.source.as_ref().expect("filtered above");
        let transform = RowTransform::from_config(table_cfg);

        tracing::info!(
            table = %table_cfg.name,
            transport = source.transport_label(),
            "starting ingress for table"
        );

        match source {
            TableSource::NatsCore { .. } | TableSource::NatsJetstream { .. } => {
                let ctx = nats_ctx
                    .as_ref()
                    .expect("nats_ctx checked above");
                nats::start(
                    ctx,
                    table_cfg,
                    source,
                    transform,
                    &registry,
                    shutdown.clone(),
                )
                .await
                .with_context(|| {
                    format!("failed to start NATS ingress for '{}'", table_cfg.name)
                })?;
            }
            TableSource::Solace { topic, .. } => {
                return Err(anyhow!(
                    "table '{}' uses solace transport (topic '{}'), which is not yet \
                     wired up. See src/ingress/solace.rs for the implementation stub.",
                    table_cfg.name,
                    topic
                ));
            }
            TableSource::Websocket { .. } => {
                websocket::start(
                    table_cfg,
                    source,
                    transform,
                    &registry,
                    shutdown.clone(),
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to start WebSocket ingress for '{}'",
                        table_cfg.name
                    )
                })?;
            }
        }
    }

    Ok(())
}

/// Helper used by every transport: feed a transformed payload into the table.
/// Centralized so the call site is identical across NATS, Solace, WebSocket.
pub(crate) async fn apply(
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
