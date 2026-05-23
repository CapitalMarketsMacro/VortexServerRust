//! Discovery REST API.
//!
//! `GET /api/tables` returns the list of tables this server exposes, each with
//! the WebSocket path/URL a Perspective client connects to. Browser apps fetch
//! this to enumerate what's available and pick which table(s) to open, instead
//! of hard-coding endpoints.
//!
//! Example response (pretty-printed):
//! ```json
//! {
//!   "tables": [
//!     {
//!       "name": "Orders",
//!       "ws_path": "/ws/Orders",
//!       "url": "ws://host:4000/ws/Orders",
//!       "index": "OrderId",
//!       "transport": "nats_jetstream"
//!     }
//!   ]
//! }
//! ```
//!
//! Client usage:
//! ```js
//! const { tables } = await (await fetch("http://host:4000/api/tables")).json();
//! const meta = tables.find(t => t.name === "Orders");
//! const ws = await perspective.websocket(meta.url);
//! const table = await ws.open_table(meta.name);
//! ```

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, header};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::tables::TableRegistry;

/// Precomputed, request-independent metadata for one table. The absolute `url`
/// is *not* stored here — it's derived per request from the `Host` header so it
/// reflects however the client actually reached the server.
#[derive(Clone)]
struct TableInfo {
    name: String,
    /// Relative WebSocket path, e.g. `/ws/Orders`.
    ws_path: String,
    /// Perspective index column, if any (`_pk` for composite keys).
    index: Option<String>,
    /// Ingress transport, or `"static"` for source-less tables.
    transport: &'static str,
}

struct ApiState {
    tables: Vec<TableInfo>,
}

/// One table entry as serialized in the response, including the absolute `url`
/// resolved from the incoming request.
#[derive(Serialize)]
struct TableEntry<'a> {
    name: &'a str,
    ws_path: &'a str,
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    index: &'a Option<String>,
    transport: &'a str,
}

#[derive(Serialize)]
struct TablesResponse<'a> {
    tables: Vec<TableEntry<'a>>,
}

/// Build the discovery API router, with its table list baked into state. Merge
/// the result into the main app router.
pub fn router(registry: &TableRegistry, ws_base: &str) -> Router {
    let ws_base = ws_base.trim_end_matches('/');

    let mut tables: Vec<TableInfo> = registry
        .iter()
        .map(|(name, slot)| TableInfo {
            name: name.clone(),
            ws_path: format!("{ws_base}/{name}"),
            index: slot.config.perspective_index(),
            transport: slot
                .config
                .source
                .as_ref()
                .map_or("static", |s| s.transport_label()),
        })
        .collect();
    tables.sort_by(|a, b| a.name.cmp(&b.name));

    let state = Arc::new(ApiState { tables });

    Router::new()
        .route("/api/tables", get(list_tables))
        .with_state(state)
}

async fn list_tables(State(state): State<Arc<ApiState>>, headers: HeaderMap) -> impl IntoResponse {
    // Resolve scheme + authority from the request so the returned URL matches
    // however the client reached us, including through a TLS-terminating proxy
    // (X-Forwarded-Proto / X-Forwarded-Host take precedence over Host).
    let secure = header_str(&headers, "x-forwarded-proto")
        .map(|p| p.eq_ignore_ascii_case("https") || p.eq_ignore_ascii_case("wss"))
        .unwrap_or(false);
    let scheme = if secure { "wss" } else { "ws" };
    let host = header_str(&headers, "x-forwarded-host").or_else(|| header_str(&headers, "host"));

    let tables = state
        .tables
        .iter()
        .map(|t| TableEntry {
            name: &t.name,
            ws_path: &t.ws_path,
            // Fall back to the relative path if we can't determine the host.
            url: match host {
                Some(h) => format!("{scheme}://{h}{}", t.ws_path),
                None => t.ws_path.clone(),
            },
            index: &t.index,
            transport: t.transport,
        })
        .collect();

    let mut response = Json(TablesResponse { tables }).into_response();
    // Discovery is public, read-only, and unauthenticated, so allow any origin
    // — a browser app served from a different origin can fetch it directly.
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    response
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}
