use std::path::Path;

use figment::Figment;
use figment::providers::{Env, Format, Json, Serialized};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    #[serde(default)]
    pub nats: Option<NatsConfig>,
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
    /// Optional NATS binding — if present, this table is populated from a NATS subject.
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

/// NATS source binding. Tagged on `transport` so the JSON config picks
/// between core pub/sub and JetStream pull consumers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case")]
pub enum TableSource {
    /// Plain NATS pub/sub. No persistence, no acks, no replay.
    Core {
        /// Subject to subscribe to (supports wildcards).
        subject: String,
        #[serde(default = "default_payload_format")]
        format: PayloadFormat,
    },
    /// JetStream durable pull consumer. Persistent, acked, replayable.
    Jetstream {
        /// JetStream stream name.
        stream: String,
        /// Subject filter (supports wildcards).
        subject: String,
        /// Durable consumer name.
        consumer: String,
        #[serde(default = "default_payload_format")]
        format: PayloadFormat,
    },
}

impl TableSource {
    pub fn format(&self) -> PayloadFormat {
        match self {
            Self::Core { format, .. } | Self::Jetstream { format, .. } => *format,
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
            nats: None,
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
