use crate::models::{HistoryDataPoint, MessageSizeEvent, TopicStats};
use chrono::Utc;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

const RESERVOIR_SIZE: usize = 1000;
const RATE_WINDOW_SECS: f64 = 10.0;

/// Per-topic accumulator for running statistics.
struct TopicStatsAccumulator {
    message_count: u64,
    total_bytes: u64,
    min_bytes: usize,
    max_bytes: usize,
    /// Reservoir sample of message sizes for percentile estimation (Algorithm R).
    reservoir: Vec<usize>,
    /// Total items seen — used for reservoir sampling probability.
    reservoir_seen: u64,
    /// Rate tracking — sliding window.
    rate_window_start: chrono::DateTime<Utc>,
    rate_window_count: u64,
    rate_window_bytes: u64,
    messages_per_sec: f64,
    bytes_per_sec: f64,
    /// Per-minute snapshot accumulator (reset each minute).
    minute_count: u64,
    minute_bytes: u64,
    minute_min: usize,
    minute_max: usize,
    minute_reservoir: Vec<usize>,
}

impl TopicStatsAccumulator {
    fn new() -> Self {
        Self {
            message_count: 0,
            total_bytes: 0,
            min_bytes: usize::MAX,
            max_bytes: 0,
            reservoir: Vec::with_capacity(RESERVOIR_SIZE),
            reservoir_seen: 0,
            rate_window_start: Utc::now(),
            rate_window_count: 0,
            rate_window_bytes: 0,
            messages_per_sec: 0.0,
            bytes_per_sec: 0.0,
            minute_count: 0,
            minute_bytes: 0,
            minute_min: usize::MAX,
            minute_max: 0,
            minute_reservoir: Vec::with_capacity(RESERVOIR_SIZE),
        }
    }

    fn record(&mut self, size: usize) {
        self.message_count += 1;
        self.total_bytes += size as u64;
        if size < self.min_bytes {
            self.min_bytes = size;
        }
        if size > self.max_bytes {
            self.max_bytes = size;
        }

        // Reservoir sampling (Algorithm R)
        self.reservoir_seen += 1;
        if self.reservoir.len() < RESERVOIR_SIZE {
            self.reservoir.push(size);
        } else {
            let j = rand::thread_rng().gen_range(0..self.reservoir_seen as usize);
            if j < RESERVOIR_SIZE {
                self.reservoir[j] = size;
            }
        }

        // Rate window
        self.rate_window_count += 1;
        self.rate_window_bytes += size as u64;
        let elapsed = (Utc::now() - self.rate_window_start)
            .num_milliseconds() as f64
            / 1000.0;
        if elapsed >= RATE_WINDOW_SECS {
            self.messages_per_sec = self.rate_window_count as f64 / elapsed;
            self.bytes_per_sec = self.rate_window_bytes as f64 / elapsed;
            self.rate_window_start = Utc::now();
            self.rate_window_count = 0;
            self.rate_window_bytes = 0;
        }

        // Per-minute accumulator
        self.minute_count += 1;
        self.minute_bytes += size as u64;
        if size < self.minute_min {
            self.minute_min = size;
        }
        if size > self.minute_max {
            self.minute_max = size;
        }
        // Reservoir for minute p95
        if self.minute_reservoir.len() < RESERVOIR_SIZE {
            self.minute_reservoir.push(size);
        } else {
            let j = rand::thread_rng().gen_range(0..self.minute_count as usize);
            if j < RESERVOIR_SIZE {
                self.minute_reservoir[j] = size;
            }
        }
    }

    fn compute_stats(&self, topic: &str) -> TopicStats {
        let avg = if self.message_count > 0 {
            self.total_bytes as f64 / self.message_count as f64
        } else {
            0.0
        };

        let (p50, p95, p99) = compute_percentiles(&self.reservoir);

        TopicStats {
            topic: topic.to_string(),
            message_count: self.message_count,
            total_bytes: self.total_bytes,
            avg_bytes: avg,
            min_bytes: if self.min_bytes == usize::MAX {
                0
            } else {
                self.min_bytes
            },
            max_bytes: self.max_bytes,
            p50,
            p95,
            p99,
            messages_per_sec: self.messages_per_sec,
            bytes_per_sec: self.bytes_per_sec,
            last_updated: Utc::now(),
        }
    }

    fn snapshot_minute(&mut self) -> Option<HistoryDataPoint> {
        if self.minute_count == 0 {
            return None;
        }

        let avg = self.minute_bytes as f64 / self.minute_count as f64;
        let (_, p95, _) = compute_percentiles(&self.minute_reservoir);

        let point = HistoryDataPoint {
            timestamp: Utc::now(),
            message_count: self.minute_count,
            avg_bytes: avg,
            min_bytes: if self.minute_min == usize::MAX {
                0
            } else {
                self.minute_min
            },
            max_bytes: self.minute_max,
            p95,
            total_bytes: self.minute_bytes,
        };

        // Reset minute accumulators
        self.minute_count = 0;
        self.minute_bytes = 0;
        self.minute_min = usize::MAX;
        self.minute_max = 0;
        self.minute_reservoir.clear();

        Some(point)
    }
}

/// Compute p50, p95, p99 from a reservoir sample.
fn compute_percentiles(reservoir: &[usize]) -> (usize, usize, usize) {
    if reservoir.is_empty() {
        return (0, 0, 0);
    }
    let mut sorted = reservoir.to_vec();
    sorted.sort_unstable();
    let len = sorted.len();
    let p50 = sorted[(len as f64 * 0.50) as usize];
    let p95 = sorted[((len as f64 * 0.95) as usize).min(len - 1)];
    let p99 = sorted[((len as f64 * 0.99) as usize).min(len - 1)];
    (p50, p95, p99)
}

/// Thread-safe stats engine for tracking message sizes across topics.
#[derive(Clone)]
pub struct StatsEngine {
    accumulators: Arc<RwLock<HashMap<String, TopicStatsAccumulator>>>,
}

impl StatsEngine {
    pub fn new() -> Self {
        Self {
            accumulators: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record a message size event, updating running stats and reservoir.
    pub async fn record_event(&self, event: &MessageSizeEvent) {
        let mut map = self.accumulators.write().await;
        let acc = map
            .entry(event.topic.clone())
            .or_insert_with(TopicStatsAccumulator::new);
        acc.record(event.size_bytes);
    }

    /// Get current stats snapshot for a single topic.
    pub async fn get_stats(&self, topic: &str) -> Option<TopicStats> {
        let map = self.accumulators.read().await;
        map.get(topic).map(|acc| acc.compute_stats(topic))
    }

    /// Get current stats for all tracked topics.
    pub async fn get_all_stats(&self) -> Vec<TopicStats> {
        let map = self.accumulators.read().await;
        map.iter()
            .map(|(topic, acc)| acc.compute_stats(topic))
            .collect()
    }

    /// Snapshot per-minute data for all topics and reset minute counters.
    /// Returns a map of topic -> HistoryDataPoint for topics that had activity.
    pub async fn snapshot_and_reset_minute(&self) -> HashMap<String, HistoryDataPoint> {
        let mut map = self.accumulators.write().await;
        let mut result = HashMap::new();
        for (topic, acc) in map.iter_mut() {
            if let Some(point) = acc.snapshot_minute() {
                result.insert(topic.clone(), point);
            }
        }
        result
    }

    /// Clear all stats (e.g., on broker change).
    pub async fn clear(&self) {
        let mut map = self.accumulators.write().await;
        map.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_event(topic: &str, size: usize) -> MessageSizeEvent {
        MessageSizeEvent {
            topic: topic.to_string(),
            partition: 0,
            offset: 0,
            size_bytes: size,
            key_size: None,
            timestamp: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_empty_stats_returns_none() {
        let engine = StatsEngine::new();
        assert!(engine.get_stats("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn test_single_event_stats() {
        let engine = StatsEngine::new();
        engine.record_event(&make_event("test", 1024)).await;

        let stats = engine.get_stats("test").await.unwrap();
        assert_eq!(stats.message_count, 1);
        assert_eq!(stats.total_bytes, 1024);
        assert_eq!(stats.min_bytes, 1024);
        assert_eq!(stats.max_bytes, 1024);
        assert_eq!(stats.avg_bytes, 1024.0);
    }

    #[tokio::test]
    async fn test_running_average() {
        let engine = StatsEngine::new();
        engine.record_event(&make_event("test", 100)).await;
        engine.record_event(&make_event("test", 200)).await;
        engine.record_event(&make_event("test", 300)).await;

        let stats = engine.get_stats("test").await.unwrap();
        assert_eq!(stats.message_count, 3);
        assert_eq!(stats.total_bytes, 600);
        assert!((stats.avg_bytes - 200.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_min_max_tracking() {
        let engine = StatsEngine::new();
        for size in [500, 100, 2000, 50, 1500] {
            engine.record_event(&make_event("test", size)).await;
        }

        let stats = engine.get_stats("test").await.unwrap();
        assert_eq!(stats.min_bytes, 50);
        assert_eq!(stats.max_bytes, 2000);
    }

    #[tokio::test]
    async fn test_percentile_symmetric_distribution() {
        let engine = StatsEngine::new();
        // Insert values 1..=100
        for i in 1..=100 {
            engine.record_event(&make_event("test", i * 10)).await;
        }

        let stats = engine.get_stats("test").await.unwrap();
        // p50 should be around 500 (50th value)
        assert!(stats.p50 >= 400 && stats.p50 <= 600);
        // p95 should be around 950
        assert!(stats.p95 >= 900 && stats.p95 <= 1000);
        // p99 should be around 990
        assert!(stats.p99 >= 980 && stats.p99 <= 1000);
    }

    #[tokio::test]
    async fn test_reservoir_size_bounded() {
        let engine = StatsEngine::new();
        // Insert 5000 events — reservoir should stay at RESERVOIR_SIZE
        for _ in 0..5000 {
            engine.record_event(&make_event("test", 100)).await;
        }

        let map = engine.accumulators.read().await;
        let acc = map.get("test").unwrap();
        assert_eq!(acc.reservoir.len(), RESERVOIR_SIZE);
    }

    #[tokio::test]
    async fn test_multiple_topics_independent() {
        let engine = StatsEngine::new();
        engine.record_event(&make_event("orders", 100)).await;
        engine.record_event(&make_event("orders", 200)).await;
        engine.record_event(&make_event("users", 500)).await;

        let orders = engine.get_stats("orders").await.unwrap();
        let users = engine.get_stats("users").await.unwrap();

        assert_eq!(orders.message_count, 2);
        assert_eq!(users.message_count, 1);
        assert_eq!(orders.total_bytes, 300);
        assert_eq!(users.total_bytes, 500);
    }

    #[tokio::test]
    async fn test_get_all_stats() {
        let engine = StatsEngine::new();
        engine.record_event(&make_event("a", 100)).await;
        engine.record_event(&make_event("b", 200)).await;

        let all = engine.get_all_stats().await;
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn test_snapshot_and_reset_minute() {
        let engine = StatsEngine::new();
        engine.record_event(&make_event("test", 100)).await;
        engine.record_event(&make_event("test", 200)).await;
        engine.record_event(&make_event("test", 300)).await;

        let snapshots = engine.snapshot_and_reset_minute().await;
        assert_eq!(snapshots.len(), 1);
        let point = snapshots.get("test").unwrap();
        assert_eq!(point.message_count, 3);
        assert_eq!(point.total_bytes, 600);
        assert_eq!(point.min_bytes, 100);
        assert_eq!(point.max_bytes, 300);

        // After snapshot, minute counters should be reset
        let snapshots2 = engine.snapshot_and_reset_minute().await;
        assert!(snapshots2.is_empty());
    }

    #[tokio::test]
    async fn test_snapshot_empty_topic_no_point() {
        let engine = StatsEngine::new();
        let snapshots = engine.snapshot_and_reset_minute().await;
        assert!(snapshots.is_empty());
    }

    #[tokio::test]
    async fn test_clear_resets_all() {
        let engine = StatsEngine::new();
        engine.record_event(&make_event("test", 100)).await;
        engine.clear().await;
        assert!(engine.get_stats("test").await.is_none());
        assert!(engine.get_all_stats().await.is_empty());
    }

    #[test]
    fn test_compute_percentiles_empty() {
        let (p50, p95, p99) = compute_percentiles(&[]);
        assert_eq!(p50, 0);
        assert_eq!(p95, 0);
        assert_eq!(p99, 0);
    }

    #[test]
    fn test_compute_percentiles_single() {
        let (p50, p95, p99) = compute_percentiles(&[42]);
        assert_eq!(p50, 42);
        assert_eq!(p95, 42);
        assert_eq!(p99, 42);
    }
}
