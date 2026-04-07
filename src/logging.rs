use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::config::{LogFormat, LoggingConfig};

/// Initialize the global tracing subscriber.
///
/// Returns a guard that must be kept alive for the lifetime of the program;
/// dropping it flushes any buffered file output.
pub fn init(cfg: &LoggingConfig) -> anyhow::Result<Option<WorkerGuard>> {
    // RUST_LOG env wins over config.level if set.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&cfg.level));

    let registry = tracing_subscriber::registry().with(filter);

    // Stderr layer (always on).
    let stderr_layer = match cfg.format {
        LogFormat::Pretty => tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_target(true)
            .boxed(),
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .with_writer(std::io::stderr)
            .with_current_span(true)
            .with_span_list(false)
            .boxed(),
    };

    // Optional rotating file layer.
    let (file_layer, guard) = if let Some(dir) = cfg.dir.as_deref() {
        let file_appender = tracing_appender::rolling::daily(dir, &cfg.file_prefix);
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        let layer = match cfg.format {
            LogFormat::Pretty => tracing_subscriber::fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_target(true)
                .boxed(),
            LogFormat::Json => tracing_subscriber::fmt::layer()
                .json()
                .with_writer(non_blocking)
                .with_current_span(true)
                .with_span_list(false)
                .boxed(),
        };

        (Some(layer), Some(guard))
    } else {
        (None, None)
    };

    registry.with(stderr_layer).with(file_layer).try_init()?;

    Ok(guard)
}

// Bring the `.boxed()` extension into scope.
use tracing_subscriber::Layer;
