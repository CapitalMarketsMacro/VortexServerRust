//! Probe a running vortex-server over its per-table WebSocket endpoint with
//! a real Perspective `Client`, the same way `<perspective-viewer>` does.
//!
//! Prints the hosted table names, the row count, and the schema of one
//! table, then optionally watches the row count for a few seconds so live
//! ingress is visible. Useful for smoke-testing a deployment without a
//! browser:
//!
//! ```text
//! cargo run --example ws_probe -- ws://127.0.0.1:4000/ws/Orders Orders
//! cargo run --example ws_probe -- ws://host:4000/ws/MarketTicks MarketTicks --watch 5
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use perspective::client::{Client, ClientError, ClientHandler};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone)]
struct WsHandler(Arc<Mutex<WsSink>>);

impl ClientHandler for WsHandler {
    async fn send_request(&self, msg: Vec<u8>) -> Result<(), BoxError> {
        self.0
            .lock()
            .await
            .send(Message::Binary(msg.into()))
            .await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (url, table_name) = match args.as_slice() {
        [url, table, ..] => (url.clone(), table.clone()),
        _ => {
            eprintln!("usage: ws_probe <ws://host:port/ws/<table>> <table> [--watch SECONDS]");
            std::process::exit(2);
        },
    };
    let watch_secs: u64 = args
        .iter()
        .position(|a| a == "--watch")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let (ws, _) = tokio_tungstenite::connect_async(&url).await?;
    let (sink, mut stream) = ws.split();
    let sink = Arc::new(Mutex::new(sink));
    let client = Client::new(None, WsHandler(sink.clone()))?;
    let pump = {
        let client = client.clone();
        tokio::spawn(async move {
            let reason = loop {
                match stream.next().await {
                    Some(Ok(Message::Binary(bytes))) => {
                        if let Err(e) = client.handle_response(&bytes).await {
                            break format!("handle_response failed: {e}");
                        }
                    },
                    Some(Ok(Message::Close(frame))) => break format!("server closed: {frame:?}"),
                    Some(Ok(_)) => {},
                    Some(Err(e)) => break format!("websocket error: {e}"),
                    None => break "websocket stream ended".to_string(),
                }
            };
            let _ = client
                .handle_error(
                    ClientError::TransportError(reason),
                    None::<fn() -> std::future::Ready<Result<(), ClientError>>>,
                )
                .await;
        })
    };

    let run = async {
        println!("connected: {url}");
        println!(
            "hosted tables: {:?}",
            client.get_hosted_table_names().await?
        );
        let table = client.open_table(table_name.clone()).await?;
        println!("{table_name}: {} rows", table.size().await?);
        println!("schema: {}", serde_json::to_string(&table.schema().await?)?);
        if watch_secs > 0 {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(watch_secs);
            while tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_secs(1)).await;
                println!("{table_name}: {} rows", table.size().await?);
            }
        }
        Ok::<(), BoxError>(())
    };

    let result = tokio::time::timeout(Duration::from_secs(30 + watch_secs), run)
        .await
        .map_err(|_| "timed out waiting for the server")?;

    // Proper closing handshake, so the server logs a clean disconnect rather
    // than "Connection reset without closing handshake".
    {
        let mut sink = sink.lock().await;
        let _ = sink.send(Message::Close(None)).await;
        let _ = sink.close().await;
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), pump).await;
    result
}
