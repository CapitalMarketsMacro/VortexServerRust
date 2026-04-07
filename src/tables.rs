use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use perspective::client::{Table, TableInitOptions, UpdateData};
use perspective::server::LocalClient;
use tokio::sync::RwLock;

use crate::config::TableConfig;

/// Holds live `Table` handles. Some are created at startup, others are
/// created lazily by the NATS subsystem when the first message arrives —
/// this lets Perspective infer schema from real data instead of failing
/// on an empty seed when an index column is declared.
///
/// Owns the underlying `LocalClient` (via `Arc`) so it lives for the
/// lifetime of the program. Calling `LocalClient::take()` and dropping the
/// original triggers a C++ access violation when the Axum WS handler later
/// creates new sessions on the same `Server` — keep the `LocalClient`
/// alive instead.
#[derive(Clone)]
pub struct TableRegistry {
    client: Arc<LocalClient>,
    tables: Arc<RwLock<HashMap<String, Table>>>,
}

impl TableRegistry {
    pub fn new(client: LocalClient) -> Self {
        Self {
            client: Arc::new(client),
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Eagerly create any table that has no NATS source — these can be
    /// constructed empty since they don't need a schema yet.
    pub async fn create_static_tables(&self, configs: &[TableConfig]) -> anyhow::Result<()> {
        for cfg in configs.iter().filter(|c| c.source.is_none()) {
            if cfg.composite_index.is_some() {
                return Err(anyhow!(
                    "table '{}' uses composite_index but has no source — \
                     composite_index requires a NATS source so the schema \
                     can be inferred from real data",
                    cfg.name
                ));
            }

            tracing::info!(table = %cfg.name, "creating static table");

            let mut opts = TableInitOptions::default();
            opts.set_name(&cfg.name);
            opts.index = cfg.perspective_index();

            let table = self
                .client
                .table(UpdateData::JsonRows("[]".to_string()).into(), opts)
                .await
                .with_context(|| format!("failed to create table '{}'", cfg.name))?;

            self.tables.write().await.insert(cfg.name.clone(), table);
        }
        Ok(())
    }

    /// Get an existing table, or create one using the supplied JSON-rows
    /// payload as the schema seed. Used by the NATS consumer on its first
    /// message — the first delivered row defines the table schema.
    pub async fn get_or_create_from_seed(
        &self,
        cfg: &TableConfig,
        seed_json_rows: String,
    ) -> anyhow::Result<Table> {
        // Fast path under read lock.
        if let Some(t) = self.tables.read().await.get(&cfg.name) {
            return Ok(t.clone());
        }

        // Slow path: take write lock and recheck.
        let mut tables = self.tables.write().await;
        if let Some(t) = tables.get(&cfg.name) {
            return Ok(t.clone());
        }

        tracing::info!(table = %cfg.name, "creating table from first message seed");

        let mut opts = TableInitOptions::default();
        opts.set_name(&cfg.name);
        opts.index = cfg.perspective_index();

        let table = self
            .client
            .table(UpdateData::JsonRows(seed_json_rows).into(), opts)
            .await
            .with_context(|| format!("failed to create table '{}'", cfg.name))?;

        tables.insert(cfg.name.clone(), table.clone());
        Ok(table)
    }

    pub async fn names(&self) -> Vec<String> {
        self.tables.read().await.keys().cloned().collect()
    }
}
