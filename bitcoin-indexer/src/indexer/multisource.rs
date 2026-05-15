use std::{
    collections::{HashSet, VecDeque},
    hash::Hash,
    sync::Arc,
};

use tokio::sync::Mutex;

pub trait Source: Copy + Send + Sync + 'static {
    fn name(self) -> &'static str;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupOutcome {
    New,
    Duplicate,
}

#[derive(Clone)]
pub struct MultiSource<E, K, S>
where
    K: Eq + Hash + Clone + Send + 'static,
    S: Source,
{
    sources: Arc<[S]>,
    key_of: fn(&E) -> K,
    recent_seen: Arc<Mutex<RecentSeen<K>>>,
}

impl<E, K, S> MultiSource<E, K, S>
where
    K: Eq + Hash + Clone + Send + 'static,
    S: Source,
{
    pub fn new(sources: Vec<S>, key_of: fn(&E) -> K, max_seen: usize) -> Self {
        Self {
            sources: sources.into(),
            key_of,
            recent_seen: Arc::new(Mutex::new(RecentSeen::new(max_seen))),
        }
    }

    pub async fn observe(&self, source: S, event: &E) -> DedupOutcome {
        debug_assert!(self
            .sources
            .iter()
            .any(|candidate| candidate.name() == source.name()));
        let key = (self.key_of)(event);
        let mut recent_seen = self.recent_seen.lock().await;
        if recent_seen.insert(key) {
            tracing::trace!(source = source.name(), "accepted multisource event");
            DedupOutcome::New
        } else {
            tracing::trace!(source = source.name(), "deduped multisource event");
            DedupOutcome::Duplicate
        }
    }

    #[cfg(test)]
    pub async fn recent_seen_len(&self) -> usize {
        self.recent_seen.lock().await.len()
    }

    #[cfg(test)]
    pub fn source_count(&self) -> usize {
        self.sources.len()
    }
}

struct RecentSeen<K>
where
    K: Eq + Hash + Clone,
{
    max_seen: usize,
    order: VecDeque<K>,
    keys: HashSet<K>,
}

impl<K> RecentSeen<K>
where
    K: Eq + Hash + Clone,
{
    fn new(max_seen: usize) -> Self {
        Self {
            max_seen,
            order: VecDeque::new(),
            keys: HashSet::new(),
        }
    }

    fn insert(&mut self, key: K) -> bool {
        if self.keys.contains(&key) {
            return false;
        }
        self.keys.insert(key.clone());
        self.order.push_back(key);

        while self.keys.len() > self.max_seen {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.keys.remove(&oldest);
        }

        true
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.keys.len()
    }
}
