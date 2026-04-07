mod config;
mod logging;
mod nats;
mod tables;

use std::net::SocketAddr;
use std::path::PathBuf;

use axum::Router;
use clap::Parser;
use perspective::server::Server;
use tokio_util::sync::CancellationToken;

use crate::config::AppConfig;
use crate::tables::TableRegistry;

#[derive(Parser, Debug)]
#[command(name = "vortex-server", version, about = "High-performance Perspective server")]
struct Cli {
    /// Path to the JSON configuration file.
    #[arg(short, long, env = "VORTEX_CONFIG", default_value = "config.json")]
    config: PathBuf,
}

// Multi-threaded runtime: safe because `perspective_server::ffi::Server`
// serializes all C++ FFI calls through an internal `async_lock::Mutex`.
// Without that mutex, concurrent FFI calls from different tokio workers
// (NATS consumer vs Axum WS handler) corrupt the engine state because the
// bundled C++ engine is built without `PSP_PARALLEL_FOR` and its internal
// `ServerResources` locks are no-ops.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let app_config = AppConfig::load(&cli.config)?;

    // Keep the guard alive until shutdown so file logs are flushed on exit.
    let _log_guard = logging::init(&app_config.logging)?;

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        config = %cli.config.display(),
        "starting VortexServer"
    );

    // Build Perspective server. Static tables are created eagerly; tables
    // bound to a NATS source are created lazily on the first message so
    // their schema can be inferred from real data.
    let server = Server::new(None);
    let registry = TableRegistry::new(server.new_local_client());
    registry.create_static_tables(&app_config.tables).await?;

    tracing::info!(
        static_tables = ?registry.names().await,
        "static tables ready"
    );

    let shutdown = CancellationToken::new();

    // Start NATS JetStream consumers (no-op if NATS is not configured).
    if let Some(nats_cfg) = &app_config.nats {
        nats::spawn_consumers(
            nats_cfg,
            &app_config.tables,
            registry.clone(),
            shutdown.clone(),
        )
        .await?;
    } else {
        tracing::info!("no [nats] section in config — running without NATS");
    }

    // Build HTTP / WebSocket router.
    let app = Router::new()
        .route(&app_config.server.ws_path, perspective::axum::websocket_handler())
        .with_state(server);

    let listener = tokio::net::TcpListener::bind(&app_config.server.bind).await?;
    tracing::info!(
        bind = %app_config.server.bind,
        ws_path = %app_config.server.ws_path,
        "listening"
    );

    let serve = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal(shutdown.clone()));

    if let Err(error) = serve.await {
        tracing::error!(%error, "http server exited with error");
    }

    shutdown.cancel();
    tracing::info!("VortexServer stopped");
    Ok(())
}

async fn shutdown_signal(token: CancellationToken) {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "failed to install SIGTERM handler");
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received Ctrl+C"),
        _ = terminate => tracing::info!("received SIGTERM"),
    }

    token.cancel();
}
