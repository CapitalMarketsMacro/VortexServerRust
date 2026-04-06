use std::net::SocketAddr;

use axum::Router;
use perspective::client::{TableInitOptions, UpdateData};
use perspective::server::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let server = Server::new(None);

    // Load CSV data
    let client = server.new_local_client();
    let csv = "x,y,z\n1,100,a\n2,200,b\n3,300,c\n4,400,d".to_string();
    let mut opts = TableInitOptions::default();
    opts.set_name("my_table");
    client.table(UpdateData::Csv(csv).into(), opts).await?;
    client.close().await;

    // Start WebSocket server
    let app = Router::new()
        .route("/ws", perspective::axum::websocket_handler())
        .with_state(server);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    tracing::info!("listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
