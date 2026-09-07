//! End-to-end check of the browser-facing path: a Perspective [`Client`]
//! talking to a [`perspective::axum::websocket_handler`] route over a real
//! TCP WebSocket (tokio-tungstenite, the same crate the WebSocket ingress
//! uses), against a table that is updated from the server side exactly the
//! way the ingress tasks update their `TableSlot`.
//!
//! This exercises the whole stack that `<perspective-viewer>` relies on —
//! protobuf protocol, Axum WS upgrade (nested per-table route, as in
//! `main.rs`), per-session response routing, engine poll/flush after an
//! update — without needing a browser or a broker.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use perspective::client::{
    Client, ClientError, ClientHandler, TableInitOptions, UpdateData, UpdateOptions, ViewWindow,
};
use perspective::server::Server;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;
type TestResult = Result<(), BoxError>;

/// Same shape as `build_router` in `src/main.rs`: `{ws_path}/{table}`.
const WS_PATH: &str = "/ws";
const TABLE: &str = "ticks";

/// Client-side transport: every request the `Client` emits is written to the
/// WebSocket as a binary frame.
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

/// `to_columns` as an `id -> px` map so the assertions don't depend on the
/// engine's row order.
async fn snapshot(view: &perspective::client::View) -> Result<BTreeMap<String, f64>, BoxError> {
    let columns: serde_json::Value =
        serde_json::from_str(&view.to_columns_string(ViewWindow::default()).await?)?;
    let ids = columns["id"].as_array().expect("id column");
    let pxs = columns["px"].as_array().expect("px column");
    Ok(ids
        .iter()
        .zip(pxs)
        .map(|(id, px)| (id.as_str().unwrap().to_string(), px.as_f64().unwrap()))
        .collect())
}

#[tokio::test]
async fn websocket_client_sees_server_side_updates() -> TestResult {
    // Engine/client tracing shows up on failure (`--nocapture` shows it always).
    let _ = tracing_subscriber::fmt()
        .with_env_filter("perspective=debug,perspective_client=debug,perspective_server=debug")
        .with_test_writer()
        .try_init();

    // Every request round-trips through the engine; a transport or protocol
    // regression would otherwise park a oneshot forever and hang the test.
    tokio::time::timeout(Duration::from_secs(30), run())
        .await
        .expect("ws_roundtrip timed out: the server never answered (hang, not a wrong answer)")
}

async fn run() -> TestResult {
    // --- server side: one engine, one named + indexed table (like TableSlot)
    let server = Server::new(None);
    let local = server.new_local_client();
    let mut opts = TableInitOptions::default();
    opts.set_name(TABLE);
    opts.index = Some("id".to_string());
    let table = local
        .table(
            UpdateData::JsonRows(r#"[{"id":"a","px":1.5},{"id":"b","px":2.5}]"#.to_string()).into(),
            opts,
        )
        .await?;

    let table_router: axum::Router = axum::Router::new()
        .route("/", perspective::axum::websocket_handler())
        .with_state(server.clone());
    let app = axum::Router::new().nest(&format!("{WS_PATH}/{TABLE}"), table_router);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let serve = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .expect("axum serve");
    });

    // --- client side: a Perspective Client whose transport is the WebSocket
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}{WS_PATH}/{TABLE}")).await?;
    let (sink, mut stream) = ws.split();
    let client = Client::new(None, WsHandler(Arc::new(Mutex::new(sink))))?;
    let pump = {
        let client = client.clone();
        tokio::spawn(async move {
            // On any termination fail every in-flight request instead of
            // leaving its oneshot pending, so callers see an error promptly.
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
            tracing::error!(%reason, "client pump terminated");
            let _ = client
                .handle_error(
                    ClientError::TransportError(reason),
                    None::<fn() -> std::future::Ready<Result<(), ClientError>>>,
                )
                .await;
        })
    };

    assert_eq!(client.get_hosted_table_names().await?, vec![
        TABLE.to_string()
    ]);
    let remote = client.open_table(TABLE.to_string()).await?;
    assert_eq!(remote.size().await?, 2);
    let view = remote.view(None).await?;
    assert_eq!(
        snapshot(&view).await?,
        BTreeMap::from([("a".to_string(), 1.5), ("b".to_string(), 2.5)])
    );

    // --- server-side update, like an ingress task: upsert `a`, insert `c`
    table
        .update(
            UpdateData::JsonRows(r#"[{"id":"a","px":9.0},{"id":"c","px":3.5}]"#.to_string()),
            UpdateOptions::default(),
        )
        .await?;

    assert_eq!(remote.size().await?, 3);
    assert_eq!(
        snapshot(&view).await?,
        BTreeMap::from([
            ("a".to_string(), 9.0),
            ("b".to_string(), 2.5),
            ("c".to_string(), 3.5)
        ])
    );

    pump.abort();
    serve.abort();
    Ok(())
}
