//! Per-table ingress dispatcher.
//!
//! For each table that has an ingress source, hand a clone of its
//! [`TableSlot`] (one isolated Perspective Server per table) to the right
//! transport submodule. The submodule wraps the per-table work in a
//! [`crate::supervisor::supervise`] call so panics auto-restart the task.
//!
//! Per-table failures and transport unavailability are always logged and
//! skipped — never fatal — so vortex-server's HTTP listener always comes up.

use perspective::client::{Table, UpdateData, UpdateOptions};
use tokio_util::sync::CancellationToken;

use crate::config::{AppConfig, PayloadFormat, TableSource};
use crate::tables::TableRegistry;
use crate::transform::RowTransform;

mod nats;
mod solace;
mod websocket;

pub async fn spawn_consumers(
    config: &AppConfig,
    registry: TableRegistry,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let bound_slots: Vec<_> = registry
        .iter()
        .filter_map(|(_, slot)| {
            if slot.config.source.is_some() {
                Some(slot.clone())
            } else {
                None
            }
        })
        .collect();

    if bound_slots.is_empty() {
        tracing::info!("no tables have an ingress source binding");
        return Ok(());
    }

    let needs_nats = bound_slots.iter().any(|s| {
        matches!(
            s.config.source.as_ref().unwrap(),
            TableSource::NatsCore { .. } | TableSource::NatsJetstream { .. }
        )
    });
    let needs_solace = bound_slots
        .iter()
        .any(|s| matches!(s.config.source.as_ref().unwrap(), TableSource::Solace { .. }));

    // NATS context (with retry). None on any failure — affected tables get
    // skipped, vortex-server keeps running.
    let nats_ctx = if needs_nats {
        match config.transports.nats.as_ref() {
            None => {
                tracing::error!(
                    "at least one table uses NATS but [transports.nats] is not configured"
                );
                None
            }
            Some(cfg) => match nats::connect(cfg, &shutdown).await {
                Ok(ctx) => Some(ctx),
                Err(error) => {
                    tracing::error!(
                        %error,
                        "NATS transport unavailable; tables sourced from NATS will be skipped \
                         (vortex-server will keep running)"
                    );
                    None
                }
            },
        }
    } else {
        None
    };

    let solace_ok = if needs_solace {
        match config.transports.solace.as_ref() {
            None => {
                tracing::error!(
                    "at least one table uses Solace but [transports.solace] is not configured"
                );
                false
            }
            Some(cfg) => match solace::validate_config(cfg) {
                Ok(()) => true,
                Err(error) => {
                    tracing::error!(
                        %error,
                        "Solace transport unavailable; tables sourced from Solace will be skipped"
                    );
                    false
                }
            },
        }
    } else {
        false
    };

    // Fan out: one supervised task per table. Each task owns its slot's
    // Perspective Server (and its FFI mutex), so high-rate updates on one
    // table cannot block any other.
    for slot in bound_slots {
        let source = slot.config.source.as_ref().expect("filtered above");
        let transform = RowTransform::from_config(&slot.config);

        tracing::info!(
            table = %slot.name,
            transport = source.transport_label(),
            "starting ingress for table"
        );

        match source {
            TableSource::NatsCore { .. } | TableSource::NatsJetstream { .. } => {
                let Some(ctx) = nats_ctx.clone() else {
                    tracing::warn!(
                        table = %slot.name,
                        "skipping table — NATS transport is unavailable"
                    );
                    continue;
                };
                nats::start(ctx, slot, transform, shutdown.clone());
            }
            TableSource::Solace { .. } => {
                if !solace_ok {
                    tracing::warn!(
                        table = %slot.name,
                        "skipping table — Solace transport is unavailable"
                    );
                    continue;
                }
                tracing::warn!(
                    table = %slot.name,
                    "Solace ingress not yet implemented — table will not receive data"
                );
            }
            TableSource::Websocket { .. } => {
                websocket::start(slot, transform, shutdown.clone());
            }
        }
    }

    Ok(())
}

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
        .map_err(|e| anyhow::anyhow!("table.update failed: {e}"))?;
    Ok(())
}
