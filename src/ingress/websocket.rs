//! Plain WebSocket ingress.
//!
//! Each WebSocket-sourced table runs in a supervised, panic-respawning task
//! tied to its own [`TableSlot`] and its own isolated Perspective Server.
//! Initial connect, first-message seed, and reconnection-after-disconnect
//! all happen inside the spawned task — startup never blocks on a slow or
//! unreachable feed.

use std::sync::Arc;
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

use crate::config::{ConnectRetryConfig, PayloadFormat, TableSource};
use crate::ingress::apply;
use crate::supervisor::supervise;
use crate::tables::TableSlot;
use crate::transform::RowTransform;

const FIRST_MESSAGE_WARN_AFTER: Duration = Duration::from_secs(60);

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Fan out a single WebSocket-sourced table into a supervised background
/// task. Returns immediately.
pub fn start(slot: Arc<TableSlot>, transform: RowTransform, shutdown: CancellationToken) {
    let task_name = format!("websocket:{}", slot.name);
    supervise(task_name, shutdown.clone(), move || {
        let slot = slot.clone();
        let transform = transform.clone();
        let shutdown = shutdown.clone();
        async move {
            if let Err(error) = run_table(slot, transform, shutdown).await {
                tracing::error!(%error, "WebSocket ingress task error");
            }
        }
    });
}

async fn run_table(
    slot: Arc<TableSlot>,
    transform: RowTransform,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let (endpoint, subprotocols, format, retry) = match slot
        .config
        .source
        .as_ref()
        .ok_or_else(|| anyhow!("table '{}' has no source", slot.name))?
        .clone()
    {
        TableSource::Websocket {
            endpoint,
            subprotocols,
            format,
            connect_retry,
        } => (endpoint, subprotocols, format, connect_retry),
        _ => {
            return Err(anyhow!(
                "websocket::run_table called for non-websocket source on table '{}'",
                slot.name
            ));
        }
    };

    tracing::info!(
        table = %slot.name,
        endpoint = %endpoint,
        "connecting WebSocket"
    );

    // Initial connect with retry.
    let mut stream =
        match connect_with_retry(&endpoint, &subprotocols, &retry, &slot.name, &shutdown).await {
            Ok(s) => s,
            Err(ConnectFailure::Shutdown) => return Ok(()),
            Err(ConnectFailure::Exhausted(error)) => {
                return Err(anyhow!(
                    "WebSocket initial connect failed for '{}' ({}): {error}",
                    slot.name,
                    endpoint
                ));
            }
        };

    // Wait for the first data frame to seed the table schema.
    let first_payload = match wait_for_first_data_frame(&mut stream, &slot.name, &shutdown).await {
        Ok(Some(p)) => p,
        Ok(None) => return Ok(()),
        Err(e) => return Err(e),
    };

    let seed_json = transform.transform_to_json_rows(&first_payload, format)?;
    let table = slot.get_or_create_from_seed(seed_json).await?;

    tracing::info!(
        table = %slot.name,
        endpoint = %endpoint,
        "table seeded from first WebSocket message"
    );

    // Long-running consumer with reconnection.
    run_consumer(
        stream,
        endpoint,
        subprotocols,
        retry,
        table,
        format,
        transform,
        slot,
        shutdown,
    )
    .await;

    Ok(())
}

enum ConnectFailure {
    Shutdown,
    Exhausted(anyhow::Error),
}

async fn connect_with_retry(
    endpoint: &str,
    subprotocols: &[String],
    retry: &ConnectRetryConfig,
    table_name: &str,
    shutdown: &CancellationToken,
) -> Result<WsStream, ConnectFailure> {
    let mut backoff = retry.initial_backoff();
    let max_backoff = retry.max_backoff();
    let mut attempt: u32 = 0;

    loop {
        if shutdown.is_cancelled() {
            return Err(ConnectFailure::Shutdown);
        }

        attempt += 1;
        let label = retry.format_attempt(attempt);

        match connect_once(endpoint, subprotocols).await {
            Ok(stream) => {
                tracing::info!(
                    table = %table_name,
                    endpoint = %endpoint,
                    attempt = %label,
                    "WebSocket connected"
                );
                return Ok(stream);
            }
            Err(error) => {
                if !retry.is_unlimited() && attempt >= retry.max_attempts {
                    return Err(ConnectFailure::Exhausted(error));
                }
                tracing::warn!(
                    table = %table_name,
                    endpoint = %endpoint,
                    attempt = %label,
                    %error,
                    retry_in = ?backoff,
                    "WebSocket connect failed, will retry"
                );
            }
        }

        tokio::select! {
            _ = sleep(backoff) => {}
            _ = shutdown.cancelled() => return Err(ConnectFailure::Shutdown),
        }

        backoff = (backoff * 2).min(max_backoff);
    }
}

async fn connect_once(endpoint: &str, subprotocols: &[String]) -> anyhow::Result<WsStream> {
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

async fn wait_for_first_data_frame(
    stream: &mut WsStream,
    table_name: &str,
    shutdown: &CancellationToken,
) -> anyhow::Result<Option<Vec<u8>>> {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(None),
            result = timeout(FIRST_MESSAGE_WARN_AFTER, stream.next()) => {
                match result {
                    Ok(Some(Ok(message))) => match message {
                        Message::Text(text) => return Ok(Some(text.as_str().as_bytes().to_vec())),
                        Message::Binary(bytes) => return Ok(Some(bytes.to_vec())),
                        Message::Ping(payload) => {
                            stream.send(Message::Pong(payload)).await?;
                        }
                        Message::Pong(_) | Message::Frame(_) => {}
                        Message::Close(_) => {
                            return Err(anyhow!("WebSocket closed before first data frame"));
                        }
                    },
                    Ok(Some(Err(e))) => return Err(anyhow!(e)),
                    Ok(None) => return Err(anyhow!("WebSocket stream ended before first data frame")),
                    Err(_elapsed) => {
                        tracing::warn!(
                            table = %table_name,
                            "still waiting for first WebSocket data frame to seed schema"
                        );
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_consumer(
    mut stream: WsStream,
    endpoint: String,
    subprotocols: Vec<String>,
    retry: ConnectRetryConfig,
    table: Table,
    format: PayloadFormat,
    transform: RowTransform,
    slot: Arc<TableSlot>,
    shutdown: CancellationToken,
) {
    loop {
        let outcome = pump_stream(&mut stream, &table, format, &transform, &slot, &shutdown).await;

        if shutdown.is_cancelled() {
            tracing::info!(table = %slot.name, "websocket consumer stopping (shutdown)");
            return;
        }

        match outcome {
            StreamOutcome::Closed => tracing::warn!(
                table = %slot.name,
                endpoint = %endpoint,
                "WebSocket closed by peer; reconnecting"
            ),
            StreamOutcome::Error(error) => tracing::warn!(
                table = %slot.name,
                endpoint = %endpoint,
                %error,
                "WebSocket consumer error; reconnecting"
            ),
        }

        match connect_with_retry(&endpoint, &subprotocols, &retry, &slot.name, &shutdown).await {
            Ok(new_stream) => {
                stream = new_stream;
            }
            Err(ConnectFailure::Shutdown) => return,
            Err(ConnectFailure::Exhausted(error)) => {
                tracing::error!(
                    table = %slot.name,
                    endpoint = %endpoint,
                    %error,
                    "WebSocket reconnect attempts exhausted; consumer task exiting"
                );
                return;
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
    slot: &TableSlot,
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
                                table = %slot.name,
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
                                table = %slot.name,
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
                            table = %slot.name,
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
