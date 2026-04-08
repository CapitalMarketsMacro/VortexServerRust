mod config;
mod ingress;
mod logging;
mod supervisor;
mod tables;
mod transform;

use std::net::SocketAddr;
use std::path::PathBuf;

use axum::Router;
use clap::Parser;
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

// Multi-threaded runtime — safe and intentional. Each table runs on its
// own isolated `perspective::server::Server` (one C++ engine instance per
// table, one FFI mutex per table) so high-rate updates on one table cannot
// contend on a lock with any other. Tokio's worker pool spreads the per-
// table tasks across cores; bulkhead isolation is at the Perspective level.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let app_config = AppConfig::load(&cli.config)?;
    let _log_guard = logging::init(&app_config.logging)?;

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        config = %cli.config.display(),
        "starting VortexServer"
    );

    // Build one isolated Perspective Server per configured table.
    let registry = TableRegistry::build(&app_config.tables);
    registry.create_static_tables().await?;

    tracing::info!(tables = ?registry.names(), "table slots ready");

    // Install the shutdown signal handler BEFORE spawning ingress tasks so
    // Ctrl+C / SIGTERM are honored even during transport-connect retry
    // loops at startup.
    let shutdown = CancellationToken::new();
    {
        let token = shutdown.clone();
        tokio::spawn(async move { shutdown_signal(token).await });
    }

    // Start ingress for every table that has a source binding. Each table
    // gets its own supervised, panic-respawning task. Per-table failures
    // never propagate up — the HTTP listener always comes up.
    ingress::spawn_consumers(&app_config, registry.clone(), shutdown.clone()).await?;

    // Build the HTTP router. Every table is exposed under
    // `{ws_path}/{table_name}`, with each route bound to that table's own
    // Server instance — full isolation at the routing layer too.
    let app = build_router(&registry, &app_config.server.ws_path);

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
    .with_graceful_shutdown({
        let token = shutdown.clone();
        async move { token.cancelled().await }
    });

    if let Err(error) = serve.await {
        tracing::error!(%error, "http server exited with error");
    }

    shutdown.cancel();
    tracing::info!("VortexServer stopped");
    Ok(())
}

/// Build a Router that mounts every table's WebSocket endpoint under a
/// distinct path: `{ws_path}/{table_name}`. Each route uses its own
/// per-table Perspective Server, so connections to different tables touch
/// completely independent state.
///
/// Browser code:
/// ```js
/// const ws = new perspective.WebSocketClient("ws://host:4000/ws/Orders");
/// const table = await ws.open_table("Orders");
/// ```
fn build_router(registry: &TableRegistry, ws_base: &str) -> Router {
    let ws_base = ws_base.trim_end_matches('/').to_string();
    let mut app: Router = Router::new();

    for (name, slot) in registry.iter() {
        let path = format!("{ws_base}/{name}");
        let table_router: Router = Router::new()
            .route("/", perspective::axum::websocket_handler())
            .with_state(slot.server.clone());

        app = app.nest(&path, table_router);
        tracing::info!(table = %name, ws_path = %path, "registered WebSocket route");
    }

    app
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
