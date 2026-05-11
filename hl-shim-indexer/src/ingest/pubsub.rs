use std::sync::Arc;

use tokio::sync::broadcast;

use crate::ingest::schema::{HlOrderEvent, HlTransferEvent, SubscribeFilter};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StreamEvent {
    Transfer { event: HlTransferEvent },
    Order { event: HlOrderEvent },
}

impl StreamEvent {
    #[must_use]
    pub fn matches(&self, filter: &SubscribeFilter) -> bool {
        match self {
            Self::Transfer { event } => filter.matches_transfer(event),
            Self::Order { event } => filter.matches_order(event),
        }
    }
}

#[derive(Clone)]
pub struct PubSub {
    sender: Arc<broadcast::Sender<StreamEvent>>,
}

impl PubSub {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender: Arc::new(sender),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<StreamEvent> {
        self.sender.subscribe()
    }

    pub fn publish(&self, event: StreamEvent) {
        let _ = self.sender.send(event);
    }
}
