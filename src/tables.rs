use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use perspective::client::{Table, TableInitOptions, UpdateData};
use perspective::server::{LocalClient, Server};
use tokio::sync::RwLock;

use crate::config::TableConfig;

/// One isolated Perspective instance per table.
///
/// Each slot owns:
/// - its own [`Server`] (one C++ ProtoServer instance, one FFI mutex, one
///   pool, one set of resources). High-rate updates on this slot cannot
///   contend on a lock with any other slot.
/// - its own [`LocalClient`] bound to that Server.
/// - the live [`Table`] handle, created lazily from the first ingress
///   message (or eagerly at startup for static tables).
///
/// This is the bulkhead boundary: stress, panic, or even C++ engine state
/// corruption in one slot can never reach another slot.
pub struct TableSlot {
    pub name: String,
    pub config: TableConfig,
    pub server: Server,
    pub client: Arc<LocalClient>,
    table: RwLock<Option<Table>>,
}

impl TableSlot {
    /// Get the live table handle, creating it from the supplied JSON-rows
    /// payload as the schema seed if it doesn't exist yet. Used by ingress
    /// tasks on their first message.
    pub async fn get_or_create_from_seed(
        &self,
        seed_json_rows: String,
    ) -> anyhow::Result<Table> {
        // Fast path under read lock.
        if let Some(t) = self.table.read().await.as_ref() {
            return Ok(t.clone());
        }

        // Slow path: take write lock and recheck.
        let mut guard = self.table.write().await;
        if let Some(t) = guard.as_ref() {
            return Ok(t.clone());
        }

        tracing::info!(table = %self.name, "creating table from first message seed");

        let mut opts = TableInitOptions::default();
        opts.set_name(&self.name);
        opts.index = self.config.perspective_index();

        let table = self
            .client
            .table(UpdateData::JsonRows(seed_json_rows).into(), opts)
            .await
            .with_context(|| format!("failed to create table '{}'", self.name))?;

        *guard = Some(table.clone());
        Ok(table)
    }

}

/// Registry of every configured table, each in its own isolation slot.
#[derive(Clone)]
pub struct TableRegistry {
    slots: Arc<HashMap<String, Arc<TableSlot>>>,
}

impl TableRegistry {
    /// Build one slot (and one Server) per configured table.
    pub fn build(configs: &[TableConfig]) -> Self {
        let mut slots = HashMap::new();
        for cfg in configs {
            let server = Server::new(None);
            let client = Arc::new(server.new_local_client());
            let slot = Arc::new(TableSlot {
                name: cfg.name.clone(),
                config: cfg.clone(),
                server,
                client,
                table: RwLock::new(None),
            });
            slots.insert(cfg.name.clone(), slot);
        }
        Self {
            slots: Arc::new(slots),
        }
    }

    /// Eagerly create static tables (those without an ingress source).
    /// Tables with a source are seeded lazily from their first message.
    pub async fn create_static_tables(&self) -> anyhow::Result<()> {
        for slot in self.slots.values() {
            if slot.config.source.is_some() {
                continue;
            }
            if slot.config.composite_index.is_some() {
                return Err(anyhow!(
                    "table '{}' uses composite_index but has no source — \
                     composite_index requires an ingress source so the schema \
                     can be inferred from real data",
                    slot.name
                ));
            }

            tracing::info!(table = %slot.name, "creating static table");

            let mut opts = TableInitOptions::default();
            opts.set_name(&slot.name);
            opts.index = slot.config.perspective_index();

            let table = slot
                .client
                .table(UpdateData::JsonRows("[]".to_string()).into(), opts)
                .await
                .with_context(|| format!("failed to create static table '{}'", slot.name))?;

            *slot.table.write().await = Some(table);
        }
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Arc<TableSlot>)> {
        self.slots.iter()
    }

    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.slots.keys().cloned().collect();
        names.sort();
        names
    }
}
