use std::path::Path;

use figment::Figment;
use figment::providers::{Env, Format, Json, Serialized};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    #[serde(default)]
    pub transports: TransportsConfig,
    #[serde(default)]
    pub tables: Vec<TableConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
    #[serde(default = "default_ws_path")]
    pub ws_path: String,
}

fn default_ws_path() -> String {
    "/ws".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level filter (e.g. "info", "vortex_server=debug,async_nats=warn").
    /// Overridden by RUST_LOG env if set.
    pub level: String,
    /// Output format: "pretty" or "json".
    #[serde(default = "default_log_format")]
    pub format: LogFormat,
    /// Optional directory for rotating log files. None = stderr only.
    #[serde(default)]
    pub dir: Option<String>,
    /// File prefix when `dir` is set.
    #[serde(default = "default_log_prefix")]
    pub file_prefix: String,
}

fn default_log_format() -> LogFormat {
    LogFormat::Pretty
}

fn default_log_prefix() -> String {
    "vortex-server".to_string()
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Pretty,
    Json,
}

/// Transport-level connection settings shared across all tables that use a
/// given transport. Each transport is optional — only the ones referenced by
/// at least one table need to be configured. WebSocket has no transport-level
/// settings; each WebSocket-sourced table carries its own endpoint URL.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransportsConfig {
    #[serde(default)]
    pub nats: Option<NatsConfig>,
    #[serde(default)]
    pub solace: Option<SolaceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsConfig {
    /// NATS server URL(s), e.g. "nats://localhost:4222". Empty disables NATS.
    pub url: String,
    /// Client connection name reported to the server.
    #[serde(default = "default_nats_name")]
    pub name: String,
    /// Optional credentials file (NATS .creds format).
    #[serde(default)]
    pub credentials_file: Option<String>,
}

fn default_nats_name() -> String {
    "vortex-server".to_string()
}

/// Solace PubSub+ broker connection settings. Currently scaffolded; the actual
/// SMF client binding is not yet wired up — startup fails fast if any table
/// uses the `solace` transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolaceConfig {
    /// Solace broker URL, e.g. "tcps://broker.example.com:55443" or
    /// "tcp://localhost:55555".
    pub host: String,
    /// Message VPN name.
    #[serde(default = "default_solace_vpn")]
    pub vpn: String,
    pub username: String,
    pub password: String,
    #[serde(default = "default_solace_client_name")]
    pub client_name: String,
}

fn default_solace_vpn() -> String {
    "default".to_string()
}

fn default_solace_client_name() -> String {
    "vortex-server".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    /// Table name exposed to clients.
    pub name: String,
    /// Optional single-column index. Mutually exclusive with `composite_index`.
    #[serde(default)]
    pub index: Option<String>,
    /// Optional composite primary key. When set, a synthetic `_pk` column is
    /// derived at ingest time by joining the listed columns with `|`, and
    /// `_pk` becomes the Perspective table index.
    #[serde(default)]
    pub composite_index: Option<Vec<String>>,
    /// Columns whose values are nested objects/arrays — they'll be JSON-stringified
    /// before reaching the Perspective table, since Perspective only supports
    /// scalar columns.
    #[serde(default)]
    pub stringify_columns: Vec<String>,
    /// Optional ingress source. Tables without a source are static (created
    /// empty at startup) and only mutate via direct `Client::table` /
    /// `Table::update` calls from inside vortex-server.
    #[serde(default)]
    pub source: Option<TableSource>,
}

impl TableConfig {
    /// Resolve which column should be passed to Perspective as the index.
    pub fn perspective_index(&self) -> Option<String> {
        if self.composite_index.is_some() {
            Some("_pk".to_string())
        } else {
            self.index.clone()
        }
    }
}

/// Per-table ingress binding. Tagged on `transport`. Each variant carries
/// only the fields meaningful for that transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case")]
pub enum TableSource {
    /// Plain NATS pub/sub. No persistence, no acks, no replay.
    NatsCore {
        /// Subject to subscribe to (supports wildcards).
        subject: String,
        #[serde(default = "default_payload_format")]
        format: PayloadFormat,
    },
    /// JetStream durable pull consumer. Persistent, acked, replayable.
    NatsJetstream {
        /// JetStream stream name.
        stream: String,
        /// Subject filter (supports wildcards).
        subject: String,
        /// Durable consumer name.
        consumer: String,
        #[serde(default = "default_payload_format")]
        format: PayloadFormat,
    },
    /// Solace direct topic subscription.
    Solace {
        /// Solace topic (supports `>` and `*` wildcards).
        topic: String,
        #[serde(default = "default_payload_format")]
        format: PayloadFormat,
    },
    /// Plain WebSocket client — connect to a remote endpoint and consume
    /// messages as table updates. Reconnects with exponential backoff on
    /// disconnect.
    Websocket {
        /// WebSocket endpoint URL (`ws://` or `wss://`).
        endpoint: String,
        /// Optional sub-protocols to request via `Sec-WebSocket-Protocol`.
        #[serde(default)]
        subprotocols: Vec<String>,
        #[serde(default = "default_payload_format")]
        format: PayloadFormat,
    },
}

impl TableSource {
    pub fn format(&self) -> PayloadFormat {
        match self {
            Self::NatsCore { format, .. }
            | Self::NatsJetstream { format, .. }
            | Self::Solace { format, .. }
            | Self::Websocket { format, .. } => *format,
        }
    }

    pub fn transport_label(&self) -> &'static str {
        match self {
            Self::NatsCore { .. } => "nats_core",
            Self::NatsJetstream { .. } => "nats_jetstream",
            Self::Solace { .. } => "solace",
            Self::Websocket { .. } => "websocket",
        }
    }
}

fn default_payload_format() -> PayloadFormat {
    PayloadFormat::JsonRows
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PayloadFormat {
    /// Each message body is a JSON array of row objects.
    JsonRows,
    /// Each message body is a single JSON row object.
    JsonRow,
    /// Each message body is an Arrow IPC stream.
    Arrow,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                bind: "0.0.0.0:4000".to_string(),
                ws_path: default_ws_path(),
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                format: LogFormat::Pretty,
                dir: None,
                file_prefix: default_log_prefix(),
            },
            transports: TransportsConfig::default(),
            tables: Vec::new(),
        }
    }
}

impl AppConfig {
    /// Load config: defaults → JSON file → VORTEX_* env overrides.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let figment = Figment::from(Serialized::defaults(AppConfig::default()))
            .merge(Json::file(path))
            .merge(Env::prefixed("VORTEX_").split("__"));

        let config: AppConfig = figment.extract()?;
        Ok(config)
    }
}
