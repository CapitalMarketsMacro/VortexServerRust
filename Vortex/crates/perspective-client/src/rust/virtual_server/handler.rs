// ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
// ┃ ██████ ██████ ██████       █      █      █      █      █ █▄  ▀███ █       ┃
// ┃ ▄▄▄▄▄█ █▄▄▄▄▄ ▄▄▄▄▄█  ▀▀▀▀▀█▀▀▀▀▀ █ ▀▀▀▀▀█ ████████▌▐███ ███▄  ▀█ █ ▀▀▀▀▀ ┃
// ┃ █▀▀▀▀▀ █▀▀▀▀▀ █▀██▀▀ ▄▄▄▄▄ █ ▄▄▄▄▄█ ▄▄▄▄▄█ ████████▌▐███ █████▄   █ ▄▄▄▄▄ ┃
// ┃ █      ██████ █  ▀█▄       █ ██████      █      ███▌▐███ ███████▄ █       ┃
// ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
// ┃ Copyright (c) 2017, the Perspective Authors.                              ┃
// ┃ ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌ ┃
// ┃ This file is part of the Perspective library, distributed under the terms ┃
// ┃ of the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). ┃
// ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

use std::future::Future;
use std::pin::Pin;

use indexmap::IndexMap;

use super::data::VirtualDataSlice;
use super::features::Features;
use crate::config::{ViewConfig, ViewConfigUpdate};
use crate::proto::{ColumnType, HostedTable, TableMakePortReq, ViewPort};

#[cfg(feature = "sendable")]
pub type VirtualServerFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// A boxed future that conditionally implements `Send` based on the target
/// architecture.
///
/// This only compiles on wasm, except for `rust-analyzer` and `metadata`
/// generation, so this type exists to tryck the compiler
#[cfg(not(feature = "sendable"))]
pub type VirtualServerFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Handler trait for implementing virtual server backends.
///
/// This trait defines the interface that must be implemented to provide
/// a custom data source for the Perspective virtual server. Implementors
/// handle table and view operations, translating them to their underlying
/// data store.
pub trait VirtualServerHandler {
    // Required

    /// The error type returned by handler methods.
    #[cfg(not(feature = "sendable"))]
    type Error: std::error::Error + Send + Sync + 'static;

    #[cfg(feature = "sendable")]
    type Error: std::error::Error + 'static;

    /// Returns a list of all tables hosted by this handler.
    fn get_hosted_tables(&self) -> VirtualServerFuture<'_, Result<Vec<HostedTable>, Self::Error>>;

    /// Returns the schema (column names and types) for a table.
    fn table_schema(
        &self,
        table_id: &str,
    ) -> VirtualServerFuture<'_, Result<IndexMap<String, ColumnType>, Self::Error>>;

    /// Returns the number of rows in a table.
    fn table_size(&self, table_id: &str) -> VirtualServerFuture<'_, Result<u32, Self::Error>>;

    /// Creates a new view on a table with the given configuration.
    ///
    /// The handler may modify the configuration to reflect any adjustments
    /// made during view creation.
    fn table_make_view(
        &mut self,
        view_id: &str,
        view_id: &str,
        config: &mut ViewConfigUpdate,
    ) -> VirtualServerFuture<'_, Result<String, Self::Error>>;

    /// Deletes a view and releases its resources.
    fn view_delete(&self, view_id: &str) -> VirtualServerFuture<'_, Result<(), Self::Error>>;

    /// Retrieves data from a view within the specified viewport.
    fn view_get_data(
        &self,
        view_id: &str,
        config: &ViewConfig,
        schema: &IndexMap<String, ColumnType>,
        viewport: &ViewPort,
    ) -> VirtualServerFuture<'_, Result<VirtualDataSlice, Self::Error>>;

    // Optional

    /// Return the column count of a `Table`
    fn table_column_size(
        &self,
        table_id: &str,
    ) -> VirtualServerFuture<'_, Result<u32, Self::Error>> {
        let fut = self.table_schema(table_id);
        Box::pin(async move { Ok(fut.await?.len() as u32) })
    }

    /// Returns the number of rows in a `View`.
    fn view_size(&self, view_id: &str) -> VirtualServerFuture<'_, Result<u32, Self::Error>> {
        Box::pin(self.table_size(view_id))
    }

    /// Return the column count of a `View`
    fn view_column_size(
        &self,
        view_id: &str,
        config: &ViewConfig,
    ) -> VirtualServerFuture<'_, Result<u32, Self::Error>> {
        let fut = self.view_schema(view_id, config);
        Box::pin(async move { Ok(fut.await?.len() as u32) })
    }

    /// Returns the schema of a view after applying its configuration.
    fn view_schema(
        &self,
        view_id: &str,
        _config: &ViewConfig,
    ) -> VirtualServerFuture<'_, Result<IndexMap<String, ColumnType>, Self::Error>> {
        Box::pin(self.table_schema(view_id))
    }

    /// Validates an expression against a table and returns its result type.
    ///
    /// Default implementation returns `Float` for all expressions.
    fn table_validate_expression(
        &self,
        _table_id: &str,
        _expression: &str,
    ) -> VirtualServerFuture<'_, Result<ColumnType, Self::Error>> {
        Box::pin(async { Ok(ColumnType::Float) })
    }

    /// Returns the features supported by this handler.
    ///
    /// Default implementation returns default features.
    fn get_features(&self) -> VirtualServerFuture<'_, Result<Features<'_>, Self::Error>> {
        Box::pin(async { Ok(Features::default()) })
    }

    /// Creates a new input port on a table.
    ///
    /// Default implementation returns port ID 0.
    fn table_make_port(
        &self,
        _req: &TableMakePortReq,
    ) -> VirtualServerFuture<'_, Result<u32, Self::Error>> {
        Box::pin(async { Ok(0) })
    }

    /// Returns the min and max values of a column in a view.
    ///
    /// Default implementation panics with "not implemented".
    fn view_get_min_max(
        &self,
        _view_id: &str,
        _column_name: &str,
        _config: &crate::config::ViewConfig,
    ) -> VirtualServerFuture<'_, Result<(crate::config::Scalar, crate::config::Scalar), Self::Error>>
    {
        Box::pin(async { unimplemented!("view_get_min_max not implemented") })
    }

    // Unused

    /// Creates a new table with the given data.
    ///
    /// Default implementation panics with "not implemented".
    fn make_table(
        &mut self,
        _table_id: &str,
        _data: &crate::proto::MakeTableData,
    ) -> VirtualServerFuture<'_, Result<(), Self::Error>> {
        Box::pin(async { unimplemented!("make_table not implemented") })
    }
}
