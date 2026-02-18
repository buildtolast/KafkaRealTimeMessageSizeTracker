use crate::models::{
    ConsumerProgress, ConsumerProgressResponse, PartitionLag, PartitionProgress,
    ProgressDataPoint, TopicConsumed,
};
use chrono::Utc;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Maximum number of time-series snapshots to keep (12 hours at 10s intervals).
const MAX_TIMESERIES: usize = 4320;

/// Tracks per-partition consumer progress for all topics.
///
/// Called from the event processor for every consumed message, and periodically
/// snapshots progress into a time-series for charting.
#[derive(Clone)]
pub struct ProgressTracker {
    /// Per-topic, per-partition: (first_offset, current_offset, message_count)
    state: Arc<RwLock<HashMap<String, HashMap<i32, (i64, i64, u64)>>>>,
    /// Time-series snapshots of progress over time.
    timeseries: Arc<RwLock<VecDeque<ProgressDataPoint>>>,
}

impl ProgressTracker {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(HashMap::new())),
            timeseries: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    /// Record that a message was consumed at the given offset for topic/partition.
    /// Updates first_offset (if first time), current_offset (always), and increments count.
    pub async fn record_offset(&self, topic: &str, partition: i32, offset: i64) {
        let mut state = self.state.write().await;
        let partitions = state.entry(topic.to_string()).or_default();
        let entry = partitions.entry(partition).or_insert((offset, offset, 0));
        // Update first_offset only if this is earlier
        if offset < entry.0 {
            entry.0 = offset;
        }
        // Always update current_offset to latest
        if offset > entry.1 {
            entry.1 = offset;
        }
        entry.2 += 1;
    }

    /// Get current consumer progress merged with watermark data.
    /// Watermarks provide `log_end_offset` for computing progress percentage.
    pub async fn get_progress(
        &self,
        watermarks: &[PartitionLag],
    ) -> ConsumerProgressResponse {
        let state = self.state.read().await;

        // Build watermark lookup: (topic, partition) -> log_end_offset
        let mut wm_map: HashMap<(&str, i32), i64> = HashMap::new();
        for wm in watermarks {
            wm_map.insert((&wm.topic, wm.partition), wm.log_end_offset);
        }

        let mut topics: Vec<ConsumerProgress> = Vec::new();

        for (topic, partitions) in state.iter() {
            let mut part_progress: Vec<PartitionProgress> = Vec::new();
            let mut total_consumed: u64 = 0;
            let mut weighted_progress_sum: f64 = 0.0;
            let mut total_range: i64 = 0;

            for (&partition, &(first, current, count)) in partitions {
                let log_end = wm_map
                    .get(&(topic.as_str(), partition))
                    .copied()
                    .unwrap_or(current + 1); // fallback if watermark not available

                let range = log_end - first;
                let consumed_range = current - first + 1;
                let pct = if range > 0 {
                    (consumed_range as f64 / range as f64 * 100.0).min(100.0)
                } else {
                    100.0
                };

                part_progress.push(PartitionProgress {
                    partition,
                    first_offset: first,
                    current_offset: current,
                    log_end_offset: log_end,
                    messages_consumed: count,
                    progress_pct: (pct * 100.0).round() / 100.0,
                });

                total_consumed += count;
                weighted_progress_sum += consumed_range as f64;
                total_range += range;
            }

            part_progress.sort_by_key(|p| p.partition);

            let overall_pct = if total_range > 0 {
                (weighted_progress_sum / total_range as f64 * 100.0).min(100.0)
            } else {
                100.0
            };

            topics.push(ConsumerProgress {
                topic: topic.clone(),
                partitions: part_progress,
                total_messages_consumed: total_consumed,
                overall_progress_pct: (overall_pct * 100.0).round() / 100.0,
            });
        }

        topics.sort_by(|a, b| a.topic.cmp(&b.topic));

        ConsumerProgressResponse {
            topics,
            timestamp: Utc::now(),
        }
    }

    /// Capture a time-series snapshot of current progress.
    /// Called from the lag collector background task.
    pub async fn snapshot(&self, watermarks: &[PartitionLag]) {
        let progress = self.get_progress(watermarks).await;

        let topics: Vec<TopicConsumed> = progress
            .topics
            .iter()
            .map(|t| TopicConsumed {
                topic: t.topic.clone(),
                messages_consumed: t.total_messages_consumed,
                progress_pct: t.overall_progress_pct,
            })
            .collect();

        // Only snapshot if there's actual data
        if topics.is_empty() {
            return;
        }

        let point = ProgressDataPoint {
            timestamp: Utc::now(),
            topics,
        };

        let mut ts = self.timeseries.write().await;
        ts.push_back(point);
        while ts.len() > MAX_TIMESERIES {
            ts.pop_front();
        }
    }

    /// Get all time-series snapshots.
    pub async fn get_timeseries(&self) -> Vec<ProgressDataPoint> {
        self.timeseries.read().await.iter().cloned().collect()
    }

    /// Clear all progress state and time-series data.
    pub async fn clear(&self) {
        self.state.write().await.clear();
        self.timeseries.write().await.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::PartitionLag;

    #[tokio::test]
    async fn test_record_offset_basic() {
        let tracker = ProgressTracker::new();
        tracker.record_offset("orders", 0, 100).await;
        tracker.record_offset("orders", 0, 101).await;
        tracker.record_offset("orders", 0, 102).await;

        let state = tracker.state.read().await;
        let (first, current, count) = state["orders"][&0];
        assert_eq!(first, 100);
        assert_eq!(current, 102);
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_record_offset_multiple_partitions() {
        let tracker = ProgressTracker::new();
        tracker.record_offset("orders", 0, 10).await;
        tracker.record_offset("orders", 1, 20).await;
        tracker.record_offset("orders", 0, 11).await;
        tracker.record_offset("orders", 1, 21).await;

        let state = tracker.state.read().await;
        assert_eq!(state["orders"][&0], (10, 11, 2));
        assert_eq!(state["orders"][&1], (20, 21, 2));
    }

    #[tokio::test]
    async fn test_record_offset_multiple_topics() {
        let tracker = ProgressTracker::new();
        tracker.record_offset("orders", 0, 100).await;
        tracker.record_offset("users", 0, 200).await;

        let state = tracker.state.read().await;
        assert_eq!(state["orders"][&0].0, 100);
        assert_eq!(state["users"][&0].0, 200);
    }

    #[tokio::test]
    async fn test_get_progress_with_watermarks() {
        let tracker = ProgressTracker::new();
        tracker.record_offset("orders", 0, 0).await;
        tracker.record_offset("orders", 0, 499).await;

        let watermarks = vec![PartitionLag {
            topic: "orders".into(),
            partition: 0,
            current_offset: 0,
            log_end_offset: 1000,
            lag: 1000,
        }];

        let progress = tracker.get_progress(&watermarks).await;
        assert_eq!(progress.topics.len(), 1);
        assert_eq!(progress.topics[0].topic, "orders");
        assert_eq!(progress.topics[0].partitions[0].first_offset, 0);
        assert_eq!(progress.topics[0].partitions[0].current_offset, 499);
        assert_eq!(progress.topics[0].partitions[0].log_end_offset, 1000);
        assert_eq!(progress.topics[0].partitions[0].messages_consumed, 2);
        assert!(progress.topics[0].partitions[0].progress_pct > 49.0);
        assert!(progress.topics[0].partitions[0].progress_pct < 51.0);
    }

    #[tokio::test]
    async fn test_get_progress_caught_up() {
        let tracker = ProgressTracker::new();
        tracker.record_offset("orders", 0, 999).await;

        let watermarks = vec![PartitionLag {
            topic: "orders".into(),
            partition: 0,
            current_offset: 0,
            log_end_offset: 1000,
            lag: 1000,
        }];

        let progress = tracker.get_progress(&watermarks).await;
        assert!(progress.topics[0].overall_progress_pct >= 99.0);
    }

    #[tokio::test]
    async fn test_snapshot_appends_timeseries() {
        let tracker = ProgressTracker::new();
        tracker.record_offset("orders", 0, 50).await;

        let watermarks = vec![PartitionLag {
            topic: "orders".into(),
            partition: 0,
            current_offset: 0,
            log_end_offset: 100,
            lag: 100,
        }];

        tracker.snapshot(&watermarks).await;
        tracker.snapshot(&watermarks).await;

        let ts = tracker.get_timeseries().await;
        assert_eq!(ts.len(), 2);
        assert_eq!(ts[0].topics[0].topic, "orders");
    }

    #[tokio::test]
    async fn test_snapshot_empty_state_does_not_append() {
        let tracker = ProgressTracker::new();
        tracker.snapshot(&[]).await;

        let ts = tracker.get_timeseries().await;
        assert!(ts.is_empty());
    }

    #[tokio::test]
    async fn test_clear_resets_everything() {
        let tracker = ProgressTracker::new();
        tracker.record_offset("orders", 0, 100).await;

        let watermarks = vec![PartitionLag {
            topic: "orders".into(),
            partition: 0,
            current_offset: 0,
            log_end_offset: 200,
            lag: 200,
        }];
        tracker.snapshot(&watermarks).await;

        tracker.clear().await;

        let state = tracker.state.read().await;
        assert!(state.is_empty());
        drop(state);

        let ts = tracker.get_timeseries().await;
        assert!(ts.is_empty());
    }
}
