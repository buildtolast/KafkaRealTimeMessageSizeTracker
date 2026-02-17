use crate::models::HistoryDataPoint;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Maximum per-minute data points per topic (7 days = 10080 minutes).
const MAX_MINUTES: usize = 10080;

/// In-memory time-series storage for per-minute aggregate data points.
#[derive(Clone)]
pub struct TimeSeriesStorage {
    data: Arc<RwLock<HashMap<String, VecDeque<HistoryDataPoint>>>>,
}

impl TimeSeriesStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Append a data point for a topic. Evicts oldest if exceeding MAX_MINUTES.
    pub async fn append(&self, topic: &str, point: HistoryDataPoint) {
        let mut data = self.data.write().await;
        let deque = data
            .entry(topic.to_string())
            .or_insert_with(|| VecDeque::with_capacity(MAX_MINUTES));
        if deque.len() >= MAX_MINUTES {
            deque.pop_front();
        }
        deque.push_back(point);
    }

    /// Query last N minutes of data for a topic.
    pub async fn query(&self, topic: &str, range_minutes: usize) -> Vec<HistoryDataPoint> {
        let data = self.data.read().await;
        match data.get(topic) {
            Some(deque) => {
                let skip = if deque.len() > range_minutes {
                    deque.len() - range_minutes
                } else {
                    0
                };
                deque.iter().skip(skip).cloned().collect()
            }
            None => Vec::new(),
        }
    }

    /// Get all topics that have history data.
    pub async fn get_topics(&self) -> Vec<String> {
        let data = self.data.read().await;
        data.keys().cloned().collect()
    }

    /// Clear all history (e.g., on broker change).
    pub async fn clear(&self) {
        let mut data = self.data.write().await;
        data.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_point(count: u64, avg: f64) -> HistoryDataPoint {
        HistoryDataPoint {
            timestamp: Utc::now(),
            message_count: count,
            avg_bytes: avg,
            min_bytes: 100,
            max_bytes: 1000,
            p95: 900,
            total_bytes: (avg * count as f64) as u64,
        }
    }

    #[tokio::test]
    async fn test_append_and_query() {
        let storage = TimeSeriesStorage::new();
        storage.append("test", make_point(10, 500.0)).await;
        storage.append("test", make_point(20, 600.0)).await;
        storage.append("test", make_point(30, 700.0)).await;

        let result = storage.query("test", 100).await;
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].message_count, 10);
        assert_eq!(result[2].message_count, 30);
    }

    #[tokio::test]
    async fn test_query_range_filtering() {
        let storage = TimeSeriesStorage::new();
        for i in 0..10 {
            storage.append("test", make_point(i, 100.0)).await;
        }

        // Query only last 3 minutes
        let result = storage.query("test", 3).await;
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].message_count, 7);
        assert_eq!(result[2].message_count, 9);
    }

    #[tokio::test]
    async fn test_circular_buffer_eviction() {
        let storage = TimeSeriesStorage::new();
        // Insert MAX_MINUTES + 100 points
        for i in 0..(MAX_MINUTES + 100) as u64 {
            storage.append("test", make_point(i, 100.0)).await;
        }

        let result = storage.query("test", MAX_MINUTES + 200).await;
        assert_eq!(result.len(), MAX_MINUTES);
        // First element should be index 100 (first 100 were evicted)
        assert_eq!(result[0].message_count, 100);
    }

    #[tokio::test]
    async fn test_empty_topic_returns_empty() {
        let storage = TimeSeriesStorage::new();
        let result = storage.query("nonexistent", 100).await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_multiple_topics_independent() {
        let storage = TimeSeriesStorage::new();
        storage.append("a", make_point(1, 100.0)).await;
        storage.append("a", make_point(2, 200.0)).await;
        storage.append("b", make_point(10, 500.0)).await;

        let a = storage.query("a", 100).await;
        let b = storage.query("b", 100).await;
        assert_eq!(a.len(), 2);
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].message_count, 10);
    }

    #[tokio::test]
    async fn test_get_topics() {
        let storage = TimeSeriesStorage::new();
        storage.append("alpha", make_point(1, 100.0)).await;
        storage.append("beta", make_point(1, 100.0)).await;

        let mut topics = storage.get_topics().await;
        topics.sort();
        assert_eq!(topics, vec!["alpha", "beta"]);
    }

    #[tokio::test]
    async fn test_clear() {
        let storage = TimeSeriesStorage::new();
        storage.append("test", make_point(1, 100.0)).await;
        storage.clear().await;
        assert!(storage.query("test", 100).await.is_empty());
        assert!(storage.get_topics().await.is_empty());
    }
}
