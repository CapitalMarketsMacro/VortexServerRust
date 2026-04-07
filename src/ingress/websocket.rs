//! Plain WebSocket ingress.
//!
//! Each WebSocket-sourced table opens its own client connection to the
//! configured endpoint. Messages (Text or Binary frames) are passed through
//! the per-table transform pipeline and applied as table updates.
//!
//! On disconnect or connection error the consumer task reconnects with
//! exponential backoff up to 30s, so a flapping upstream feed doesn't
//! permanently take a table offline.

use std::time::Duration;

use anyhow::{Context, anyhow};
use futures::{SinkExt, StreamExt};
use perspective::client::Table;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tokio_util::sync::CancellationToken;

use crate::config::{PayloadFormat, TableConfig, TableSource};
use crate::ingress::apply;
use crate::tables::TableRegistry;
use crate::transform::RowTransform;

/// How long to wait for the first message before failing startup.
const FIRST_MESSAGE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long to wait between reconnect attempts initially.
const INITIAL_RECONNECT_BACKOFF: Duration = Duration::from_secs(1);
/// Cap on the reconnect backoff.
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

pub async fn start(
    table_cfg: &TableConfig,
    source: &TableSource,
    transform: RowTransform,
    registry: &TableRegistry,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let (endpoint, subprotocols, format) = match source {
        TableSource::Websocket {
            endpoint,
            subprotocols,
            format,
        } => (endpoint.clone(), subprotocols.clone(), *format),
        _ => unreachable!("dispatcher routes non-websocket sources elsewhere"),
    };

    tracing::info!(
        table = %table_cfg.name,
        endpoint = %endpoint,
        "connecting WebSocket; waiting for first message to seed schema"
    );

    let mut stream = connect(&endpoint, &subprotocols)
        .await
        .with_context(|| format!("failed to connect to '{endpoint}'"))?;

    let first_payload = timeout(FIRST_MESSAGE_TIMEOUT, recv_data_frame(&mut stream))
        .await
        .with_context(|| {
            format!(
                "timed out after {:?} waiting for first WebSocket message to \
                 seed table '{}'",
                FIRST_MESSAGE_TIMEOUT, table_cfg.name
            )
        })?
        .with_context(|| format!("WebSocket '{endpoint}' closed before first message"))?;

    let seed_json = transform.transform_to_json_rows(&first_payload, format)?;
    let table = registry
        .get_or_create_from_seed(table_cfg, seed_json)
        .await?;

    tracing::info!(
        table = %table_cfg.name,
        endpoint = %endpoint,
        "table seeded from first WebSocket message"
    );

    let cfg_clone = table_cfg.clone();
    tokio::spawn(async move {
        run_consumer(
            stream,
            endpoint,
            subprotocols,
            table,
            format,
            transform,
            cfg_clone,
            shutdown,
        )
        .await;
    });

    Ok(())
}

/// Long-running consumer loop with reconnection. Owns the initial connection
/// (so we don't drop the seed-time stream and have to reconnect immediately)
/// and reopens it with exponential backoff if it fails.
async fn run_consumer(
    mut stream: WsStream,
    endpoint: String,
    subprotocols: Vec<String>,
    table: Table,
    format: PayloadFormat,
    transform: RowTransform,
    cfg: TableConfig,
    shutdown: CancellationToken,
) {
    let mut backoff = INITIAL_RECONNECT_BACKOFF;

    loop {
        // Pump messages on the current stream until it errors or closes.
        let stream_outcome =
            pump_stream(&mut stream, &table, format, &transform, &cfg, &shutdown).await;

        if shutdown.is_cancelled() {
            tracing::info!(table = %cfg.name, "websocket consumer stopping (shutdown)");
            return;
        }

        match stream_outcome {
            StreamOutcome::Closed => {
                tracing::warn!(
                    table = %cfg.name,
                    endpoint = %endpoint,
                    "WebSocket closed by peer; reconnecting"
                );
            }
            StreamOutcome::Error(error) => {
                tracing::warn!(
                    table = %cfg.name,
                    endpoint = %endpoint,
                    %error,
                    "WebSocket consumer error; reconnecting"
                );
            }
        }

        // Reconnect loop with exponential backoff.
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!(table = %cfg.name, "websocket consumer stopping (shutdown)");
                    return;
                }
                _ = sleep(backoff) => {}
            }

            match connect(&endpoint, &subprotocols).await {
                Ok(new_stream) => {
                    tracing::info!(
                        table = %cfg.name,
                        endpoint = %endpoint,
                        "WebSocket reconnected"
                    );
                    stream = new_stream;
                    backoff = INITIAL_RECONNECT_BACKOFF;
                    break;
                }
                Err(error) => {
                    backoff = (backoff * 2).min(MAX_RECONNECT_BACKOFF);
                    tracing::warn!(
                        table = %cfg.name,
                        endpoint = %endpoint,
                        %error,
                        retry_in = ?backoff,
                        "WebSocket reconnect failed"
                    );
                }
            }
        }
    }
}

enum StreamOutcome {
    Closed,
    Error(anyhow::Error),
}

async fn pump_stream(
    stream: &mut WsStream,
    table: &Table,
    format: PayloadFormat,
    transform: &RowTransform,
    cfg: &TableConfig,
    shutdown: &CancellationToken,
) -> StreamOutcome {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return StreamOutcome::Closed,
            next = stream.next() => {
                let Some(message) = next else {
                    return StreamOutcome::Closed;
                };

                let message = match message {
                    Ok(m) => m,
                    Err(e) => return StreamOutcome::Error(anyhow!(e)),
                };

                match message {
                    Message::Text(text) => {
                        if let Err(error) =
                            apply(table, text.as_str().as_bytes(), format, transform).await
                        {
                            tracing::error!(
                                table = %cfg.name,
                                %error,
                                "failed to apply WebSocket text message"
                            );
                        }
                    }
                    Message::Binary(bytes) => {
                        if let Err(error) =
                            apply(table, bytes.as_ref(), format, transform).await
                        {
                            tracing::error!(
                                table = %cfg.name,
                                %error,
                                "failed to apply WebSocket binary message"
                            );
                        }
                    }
                    Message::Ping(payload) => {
                        if let Err(e) = stream.send(Message::Pong(payload)).await {
                            return StreamOutcome::Error(anyhow!(e));
                        }
                    }
                    Message::Pong(_) | Message::Frame(_) => {}
                    Message::Close(frame) => {
                        tracing::info!(
                            table = %cfg.name,
                            ?frame,
                            "WebSocket peer sent Close frame"
                        );
                        return StreamOutcome::Closed;
                    }
                }
            }
        }
    }
}

/// Receive frames until we get a data frame (Text or Binary), discarding
/// control frames. Used during seed-time so the first ping/pong handshake
/// doesn't get mistaken for the seed payload.
async fn recv_data_frame(stream: &mut WsStream) -> anyhow::Result<Vec<u8>> {
    while let Some(message) = stream.next().await {
        match message? {
            Message::Text(text) => return Ok(text.as_str().as_bytes().to_vec()),
            Message::Binary(bytes) => return Ok(bytes.to_vec()),
            Message::Ping(payload) => {
                stream.send(Message::Pong(payload)).await?;
            }
            Message::Pong(_) | Message::Frame(_) => {}
            Message::Close(_) => return Err(anyhow!("WebSocket closed before first data frame")),
        }
    }
    Err(anyhow!("WebSocket stream ended"))
}

async fn connect(endpoint: &str, subprotocols: &[String]) -> anyhow::Result<WsStream> {
    let mut request = endpoint
        .into_client_request()
        .with_context(|| format!("invalid WebSocket URL '{endpoint}'"))?;

    if !subprotocols.is_empty() {
        let header = subprotocols.join(", ");
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str(&header)
                .with_context(|| format!("invalid subprotocol header '{header}'"))?,
        );
    }

    let (stream, _response) = connect_async(request).await?;
    Ok(stream)
}
