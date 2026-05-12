use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use axum::extract::ws::{Message, WebSocket};
use axum::{
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;

use crate::{api::AppState, telemetry};

static ACTIVE_SUBSCRIPTIONS: AtomicUsize = AtomicUsize::new(0);

pub async fn ws_subscribe(
    State(state): State<Arc<AppState>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let mut events = state.pubsub.subscribe();
    telemetry::record_active_subscriptions(
        ACTIVE_SUBSCRIPTIONS.fetch_add(1, Ordering::Relaxed) + 1,
    );

    loop {
        tokio::select! {
            maybe_message = receiver.next() => {
                match maybe_message {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        tracing::warn!(?error, "EVM receipt watcher websocket receive failed");
                        break;
                    }
                }
            }
            event = events.recv() => {
                match event {
                    Ok(event) => {
                        if send_json(&mut sender, &event).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        let _ = send_json(
                            &mut sender,
                            &WsError {
                                error: format!("subscriber lagged by {skipped} events"),
                            },
                        )
                        .await;
                        break;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    let previous = ACTIVE_SUBSCRIPTIONS.fetch_sub(1, Ordering::Relaxed);
    telemetry::record_active_subscriptions(previous.saturating_sub(1));
}

async fn send_json<T: Serialize>(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    value: &T,
) -> Result<(), axum::Error> {
    let text = match serde_json::to_string(value) {
        Ok(text) => text,
        Err(error) => {
            tracing::warn!(?error, "EVM receipt watcher websocket serialization failed");
            return Ok(());
        }
    };
    sender.send(Message::Text(text)).await
}

#[derive(Serialize)]
struct WsError {
    error: String,
}
