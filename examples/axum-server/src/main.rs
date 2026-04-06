use std::net::SocketAddr;

use axum::Router;
use perspective::client::{TableInitOptions, UpdateData};
use perspective::server::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let server = Server::new(None);

    // Load data via JSON columns (CSV disabled in this build)
    let client = server.new_local_client();
    let json = r#"{"x":[1,2,3,4],"y":[100,200,300,400],"z":["a","b","c","d"]}"#.to_string();
    let mut opts = TableInitOptions::default();
    opts.set_name("my_table");
    client
        .table(UpdateData::JsonColumns(json).into(), opts)
        .await?;
    client.close().await;

    // Start WebSocket server — connect <perspective-viewer> to ws://localhost:3000/ws
    let app = Router::new()
        .route("/ws", perspective::axum::websocket_handler())
        .with_state(server);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    tracing::info!("listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
