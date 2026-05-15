use std::{collections::HashMap, sync::Arc};

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::parse_address,
    ingest::{StreamEvent, SubscribeFilter},
    telemetry, AppState,
};

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum ClientMessage {
    Subscribe { filter: WireSubscribeFilter },
    Unsubscribe { subscription_id: Uuid },
}

#[derive(Debug, Deserialize)]
struct WireSubscribeFilter {
    users: Vec<String>,
    #[serde(default)]
    kinds: Vec<crate::ingest::StreamKind>,
}

impl WireSubscribeFilter {
    fn into_filter(self) -> Result<SubscribeFilter, String> {
        let users = self
            .users
            .iter()
            .map(|user| parse_address(user).map_err(|error| error.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        let kinds = if self.kinds.is_empty() {
            vec![
                crate::ingest::StreamKind::Transfers,
                crate::ingest::StreamKind::Orders,
            ]
        } else {
            self.kinds
        };
        Ok(SubscribeFilter { users, kinds })
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ServerMessage {
    Subscribed {
        subscription_id: Uuid,
    },
    Unsubscribed {
        subscription_id: Uuid,
    },
    Transfer {
        event: crate::ingest::HlTransferEvent,
    },
    Order {
        event: crate::ingest::HlOrderEvent,
    },
    Error {
        error: String,
    },
}

pub async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let mut events = state.pubsub.subscribe();
    let mut subscriptions: HashMap<Uuid, SubscribeFilter> = HashMap::new();

    loop {
        tokio::select! {
            maybe_message = receiver.next() => {
                match maybe_message {
                    Some(Ok(Message::Text(text))) => {
                        if !handle_client_message(&state, &mut sender, &mut subscriptions, &text).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        tracing::warn!(?error, "HL shim websocket receive failed");
                        break;
                    }
                }
            }
            event = events.recv() => {
                match event {
                    Ok(event) => {
                        if subscriptions.values().any(|filter| event.matches(filter)) {
                            let outbound = match event {
                                StreamEvent::Transfer { event } => ServerMessage::Transfer { event },
                                StreamEvent::Order { event } => ServerMessage::Order { event },
                            };
                            if send_json(&mut sender, &outbound).await.is_err() {
                                break;
                            }
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

    for filter in subscriptions.values() {
        for user in &filter.users {
            state.scheduler.remove_subscriber(*user).await;
        }
    }
    telemetry::record_active_subscriptions(0);
}

async fn handle_client_message(
    state: &Arc<AppState>,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    subscriptions: &mut HashMap<Uuid, SubscribeFilter>,
    text: &str,
) -> bool {
    let message = match serde_json::from_str::<ClientMessage>(text) {
        Ok(message) => message,
        Err(error) => {
            let _ = send_json(
                sender,
                &ServerMessage::Error {
                    error: format!("invalid subscribe frame: {error}"),
                },
            )
            .await;
            return true;
        }
    };
    match message {
        ClientMessage::Subscribe { filter } => match filter.into_filter() {
            Ok(filter) => {
                let id = Uuid::now_v7();
                for user in &filter.users {
                    state.scheduler.add_subscriber(*user).await;
                }
                subscriptions.insert(id, filter);
                telemetry::record_active_subscriptions(subscriptions.len());
                send_json(
                    sender,
                    &ServerMessage::Subscribed {
                        subscription_id: id,
                    },
                )
                .await
                .is_ok()
            }
            Err(error) => {
                let _ = send_json(sender, &ServerMessage::Error { error }).await;
                true
            }
        },
        ClientMessage::Unsubscribe { subscription_id } => {
            if let Some(filter) = subscriptions.remove(&subscription_id) {
                for user in &filter.users {
                    state.scheduler.remove_subscriber(*user).await;
                }
            }
            telemetry::record_active_subscriptions(subscriptions.len());
            send_json(sender, &ServerMessage::Unsubscribed { subscription_id })
                .await
                .is_ok()
        }
    }
}

async fn send_json<T: Serialize>(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    value: &T,
) -> Result<(), axum::Error> {
    let text = match serde_json::to_string(value) {
        Ok(text) => text,
        Err(error) => {
            tracing::warn!(?error, "HL shim websocket serialization failed");
            return Ok(());
        }
    };
    sender.send(Message::Text(text)).await
}
