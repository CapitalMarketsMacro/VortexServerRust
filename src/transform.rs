use anyhow::{Context, anyhow};

use crate::config::{PayloadFormat, TableConfig};

/// Per-table message transformation pipeline. Stringifies nested columns,
/// then injects the synthetic composite primary key column. Reused by every
/// transport.
#[derive(Clone)]
pub struct RowTransform {
    stringify_columns: Vec<String>,
    composite_index: Option<Vec<String>>,
}

impl RowTransform {
    pub fn from_config(cfg: &TableConfig) -> Self {
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
    pub fn transform_to_json_rows(
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
