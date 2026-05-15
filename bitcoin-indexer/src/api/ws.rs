use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use axum::extract::ws::{Message, WebSocket};
use axum::{
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
};
use bitcoin_indexer_client::TxOutput;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

use crate::api::AppState;

static ACTIVE_SUBSCRIPTIONS: AtomicUsize = AtomicUsize::new(0);

pub async fn ws_subscribe(
    State(state): State<Arc<AppState>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let Some(filter) = read_subscribe_filter(&mut receiver, &mut sender).await else {
        return;
    };
    let mut events = state.pubsub.subscribe();
    ACTIVE_SUBSCRIPTIONS.fetch_add(1, Ordering::Relaxed);

    if send_json(
        &mut sender,
        &ServerMessage::Subscribed {
            subscription_id: None,
        },
    )
    .await
    .is_err()
    {
        ACTIVE_SUBSCRIPTIONS.fetch_sub(1, Ordering::Relaxed);
        return;
    }

    loop {
        tokio::select! {
            maybe_message = receiver.next() => {
                match maybe_message {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        tracing::warn!(?error, "Bitcoin indexer websocket receive failed");
                        break;
                    }
                }
            }
            event = events.recv() => {
                match event {
                    Ok(event) => {
                        if filter.matches(&event)
                            && send_json(&mut sender, &ServerMessage::TxOutput { event }).await.is_err()
                        {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        let _ = send_json(
                            &mut sender,
                            &ServerMessage::Error {
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

    ACTIVE_SUBSCRIPTIONS.fetch_sub(1, Ordering::Relaxed);
}

async fn read_subscribe_filter(
    receiver: &mut futures_util::stream::SplitStream<WebSocket>,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> Option<SubscribeFilter> {
    while let Some(message) = receiver.next().await {
        match message {
            Ok(Message::Text(text)) => match serde_json::from_str::<ClientMessage>(&text) {
                Ok(ClientMessage::Subscribe { filter }) => {
                    match SubscribeFilter::try_from(filter) {
                        Ok(filter) => return Some(filter),
                        Err(error) => {
                            let _ = send_json(sender, &ServerMessage::Error { error }).await;
                            return None;
                        }
                    }
                }
                Err(error) => {
                    let _ = send_json(
                        sender,
                        &ServerMessage::Error {
                            error: error.to_string(),
                        },
                    )
                    .await;
                    return None;
                }
            },
            Ok(Message::Close(_)) | Err(_) => return None,
            Ok(_) => {}
        }
    }
    None
}

async fn send_json<T: Serialize>(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    value: &T,
) -> Result<(), axum::Error> {
    let text = match serde_json::to_string(value) {
        Ok(text) => text,
        Err(error) => {
            tracing::warn!(?error, "Bitcoin indexer websocket serialization failed");
            return Ok(());
        }
    };
    sender.send(Message::Text(text)).await
}

#[derive(Debug)]
struct SubscribeFilter {
    addresses: HashSet<String>,
    min_amount: Option<u64>,
}

impl SubscribeFilter {
    fn matches(&self, event: &TxOutput) -> bool {
        if let Some(min_amount) = self.min_amount {
            if event.amount_sats < min_amount {
                return false;
            }
        }
        self.addresses
            .contains(&event.address.assume_checked_ref().to_string())
    }
}

impl TryFrom<WireSubscribeFilter> for SubscribeFilter {
    type Error = String;

    fn try_from(value: WireSubscribeFilter) -> Result<Self, Self::Error> {
        if value.addresses.is_empty() {
            return Err("addresses must not be empty".to_string());
        }
        Ok(Self {
            addresses: value.addresses.into_iter().collect(),
            min_amount: value.min_amount,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum ClientMessage {
    Subscribe { filter: WireSubscribeFilter },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct WireSubscribeFilter {
    addresses: Vec<String>,
    min_amount: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ServerMessage {
    Subscribed { subscription_id: Option<String> },
    TxOutput { event: TxOutput },
    Error { error: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_filter_matches_address_and_min_amount() {
        let address = "bcrt1q2nfxmhd4n3c8834pj72xagvyr9gl57n5r94fsl";
        let filter = SubscribeFilter {
            addresses: HashSet::from([address.to_string()]),
            min_amount: Some(50_000),
        };
        let event = TxOutput {
            txid: "1111111111111111111111111111111111111111111111111111111111111111"
                .parse()
                .unwrap(),
            vout: 0,
            address: address.parse().unwrap(),
            amount_sats: 50_000,
            block_height: None,
            block_hash: None,
            block_time: None,
            confirmations: 0,
            removed: false,
        };

        assert!(filter.matches(&event));
    }
}
