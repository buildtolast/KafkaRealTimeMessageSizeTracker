use crate::config::SecurityConfig;
use crate::models::{
    BrokerDiscoveryResponse, BrokerInfo, BrokerMetrics, ClusterInsightsResponse, LatencyWindow,
    PartitionLag, ThroughputRecommendation, TopicThroughput,
};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::statistics::Statistics;
use rdkafka::{ClientConfig, ClientContext};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;

/// Shared statistics snapshot, updated by the rdkafka stats callback.
pub type SharedKafkaStats = Arc<StdRwLock<Option<Statistics>>>;

/// Custom context that captures rdkafka statistics into a shared slot.
pub struct StatsContext {
    stats: SharedKafkaStats,
}

impl ClientContext for StatsContext {
    fn stats(&self, statistics: Statistics) {
        if let Ok(mut guard) = self.stats.write() {
            *guard = Some(statistics);
        }
    }
}

impl ConsumerContext for StatsContext {}

/// Admin consumer type with stats context.
pub type AdminConsumer = BaseConsumer<StatsContext>;

/// Create an AdminConsumer for metadata operations with statistics collection.
pub fn create_admin_consumer(
    brokers: &str,
    stats: SharedKafkaStats,
    security: &SecurityConfig,
) -> Result<AdminConsumer, rdkafka::error::KafkaError> {
    let context = StatsContext { stats };
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("statistics.interval.ms", "5000");
    super::apply_security(&mut config, security);
    super::apply_timeouts(&mut config);
    config.create_with_context(context)
}

/// List all non-internal topics from the Kafka cluster.
pub fn list_topics(consumer: &AdminConsumer) -> Result<Vec<String>, rdkafka::error::KafkaError> {
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(5))?;
    let mut topics: Vec<String> = metadata
        .topics()
        .iter()
        .map(|t| t.name().to_string())
        .filter(|name| !name.starts_with("__"))
        .collect();
    topics.sort();
    Ok(topics)
}

/// Discover brokers in the cluster and return their info + cluster ID.
pub fn discover_brokers(
    consumer: &AdminConsumer,
    current_brokers: &str,
) -> Result<BrokerDiscoveryResponse, rdkafka::error::KafkaError> {
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(5))?;
    let discovered: Vec<BrokerInfo> = metadata
        .brokers()
        .iter()
        .map(|b| BrokerInfo {
            id: b.id(),
            host: b.host().to_string(),
            port: b.port(),
        })
        .collect();

    // rdkafka Metadata doesn't expose cluster_id directly, but we can extract it
    // from the originating_broker's metadata response. For now, use None.
    Ok(BrokerDiscoveryResponse {
        current_brokers: current_brokers.to_string(),
        discovered_brokers: discovered,
        cluster_id: None,
    })
}

/// Fetch consumer lag for tracked topics using watermark offsets.
/// If `topics` is empty, falls back to fetching all non-internal topics.
/// Fetches metadata per-topic (smaller responses) to reduce round-trip time
/// on high-latency connections like Confluent Cloud.
pub fn fetch_consumer_lag(
    consumer: &AdminConsumer,
) -> Result<Vec<PartitionLag>, rdkafka::error::KafkaError> {
    fetch_consumer_lag_for_topics(consumer, &[])
}

/// Fetch consumer lag for specific topics only. When the topic list is non-empty,
/// fetches metadata per-topic instead of a single fetch_metadata(None) for all,
/// which is significantly faster on remote brokers since watermark fetches
/// are limited to the partitions you actually care about.
pub fn fetch_consumer_lag_for_topics(
    consumer: &AdminConsumer,
    topics: &[String],
) -> Result<Vec<PartitionLag>, rdkafka::error::KafkaError> {
    let timeout = Duration::from_secs(5);
    let watermark_timeout = Duration::from_secs(3);
    let mut lags = Vec::new();

    if topics.is_empty() {
        // Fallback: fetch all topics (used by cluster insights)
        let metadata = consumer.fetch_metadata(None, timeout)?;
        for topic_meta in metadata.topics() {
            let topic_name = topic_meta.name();
            if topic_name.starts_with("__") {
                continue;
            }
            fetch_partition_watermarks(consumer, topic_name, topic_meta.partitions(), watermark_timeout, &mut lags);
        }
    } else {
        // Optimized: only fetch metadata + watermarks for tracked topics
        for topic in topics {
            match consumer.fetch_metadata(Some(topic), timeout) {
                Ok(metadata) => {
                    if let Some(topic_meta) = metadata.topics().iter().find(|t| t.name() == topic.as_str()) {
                        fetch_partition_watermarks(consumer, topic, topic_meta.partitions(), watermark_timeout, &mut lags);
                    }
                }
                Err(e) => {
                    log::warn!("Failed to fetch metadata for topic {}: {}", topic, e);
                }
            }
        }
    }

    Ok(lags)
}

/// Helper: fetch watermarks for all partitions of a topic and append to lags.
fn fetch_partition_watermarks(
    consumer: &AdminConsumer,
    topic_name: &str,
    partitions: &[rdkafka::metadata::MetadataPartition],
    timeout: Duration,
    lags: &mut Vec<PartitionLag>,
) {
    for partition in partitions {
        let pid = partition.id();
        match consumer.fetch_watermarks(topic_name, pid, timeout) {
            Ok((low, high)) => {
                lags.push(PartitionLag {
                    topic: topic_name.to_string(),
                    partition: pid,
                    current_offset: low,
                    log_end_offset: high,
                    lag: high - low,
                });
            }
            Err(e) => {
                log::warn!(
                    "Failed to fetch watermarks for {}-{}: {}",
                    topic_name,
                    pid,
                    e
                );
            }
        }
    }
}

/// Convert an rdkafka statistics Window to our LatencyWindow model.
fn to_latency_window(w: &rdkafka::statistics::Window) -> LatencyWindow {
    LatencyWindow {
        min: w.min,
        max: w.max,
        avg: w.avg,
        p50: w.p50,
        p95: w.p95,
        p99: w.p99,
        cnt: w.cnt,
    }
}

/// Extract per-broker metrics from the latest statistics snapshot.
pub fn extract_broker_metrics(stats: &SharedKafkaStats) -> Option<Vec<BrokerMetrics>> {
    let guard = stats.read().ok()?;
    let statistics = guard.as_ref()?;

    let mut metrics: Vec<BrokerMetrics> = statistics
        .brokers
        .values()
        .filter(|b| b.nodeid >= 0) // skip bootstrap/internal nodes
        .map(|b| BrokerMetrics {
            node_id: b.nodeid,
            name: b.name.clone(),
            state: b.state.clone(),
            state_age_secs: b.stateage as f64 / 1_000_000.0,
            tx_bytes: b.txbytes,
            rx_bytes: b.rxbytes,
            tx_errors: b.txerrs,
            rx_errors: b.rxerrs,
            request_timeouts: b.req_timeouts,
            request_retries: b.txretries,
            rtt: b.rtt.as_ref().map(to_latency_window),
            throttle: b.throttle.as_ref().map(to_latency_window),
            outbuf_cnt: b.outbuf_cnt,
            waitresp_cnt: b.waitresp_cnt,
            connects: b.connects,
            disconnects: b.disconnects,
        })
        .collect();

    metrics.sort_by_key(|m| m.node_id);

    if metrics.is_empty() {
        None
    } else {
        Some(metrics)
    }
}

/// Build full cluster insights: brokers, topics, partitions, lag, throughput, recommendations.
/// Accepts pre-fetched consumer lag data to avoid redundant Kafka calls.
pub fn build_cluster_insights(
    consumer: &AdminConsumer,
    all_stats: Vec<crate::models::TopicStats>,
    kafka_stats: &SharedKafkaStats,
    cached_lag: Vec<PartitionLag>,
) -> Result<ClusterInsightsResponse, rdkafka::error::KafkaError> {
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(5))?;

    // Brokers
    let brokers: Vec<BrokerInfo> = metadata
        .brokers()
        .iter()
        .map(|b| BrokerInfo {
            id: b.id(),
            host: b.host().to_string(),
            port: b.port(),
        })
        .collect();

    // Topics + partition count
    let mut total_partitions: i32 = 0;
    let non_internal_topics: Vec<_> = metadata
        .topics()
        .iter()
        .filter(|t| !t.name().starts_with("__"))
        .collect();
    let total_topics = non_internal_topics.len();
    for t in &non_internal_topics {
        total_partitions += t.partitions().len() as i32;
    }

    // Use pre-fetched consumer lag (from background collector cache)
    let consumer_lag = cached_lag;

    // Topic throughput from StatsEngine
    let mut topic_throughput: Vec<TopicThroughput> = Vec::new();
    for stats in &all_stats {
        let partition_count = non_internal_topics
            .iter()
            .find(|t| t.name() == stats.topic)
            .map(|t| t.partitions().len() as i32)
            .unwrap_or(1);

        topic_throughput.push(TopicThroughput {
            topic: stats.topic.clone(),
            partitions: partition_count,
            messages_per_sec: stats.messages_per_sec,
            bytes_per_sec: stats.bytes_per_sec,
            avg_message_size: stats.avg_bytes,
            total_messages: stats.message_count,
            total_bytes: stats.total_bytes,
        });
    }

    // Generate recommendations
    let mut recommendations: Vec<ThroughputRecommendation> = Vec::new();

    for tp in &topic_throughput {
        // Check if topic has only 1 partition but high throughput
        if tp.partitions == 1 && tp.messages_per_sec > 100.0 {
            recommendations.push(ThroughputRecommendation {
                topic: tp.topic.clone(),
                severity: "warning".into(),
                message: format!(
                    "Topic '{}' has only 1 partition but {:.0} msg/sec. Increase partitions to distribute load across consumers.",
                    tp.topic, tp.messages_per_sec
                ),
            });
        }

        // Large average message size
        if tp.avg_message_size > 500_000.0 {
            recommendations.push(ThroughputRecommendation {
                topic: tp.topic.clone(),
                severity: "warning".into(),
                message: format!(
                    "Topic '{}' has a large average message size ({:.0} bytes). Consider compression (lz4/snappy) or splitting payloads.",
                    tp.topic, tp.avg_message_size
                ),
            });
        }

        // Very high throughput — suggest batching
        if tp.bytes_per_sec > 10_000_000.0 {
            recommendations.push(ThroughputRecommendation {
                topic: tp.topic.clone(),
                severity: "info".into(),
                message: format!(
                    "Topic '{}' has high throughput ({:.1} MB/sec). Ensure producers use batch.size and linger.ms for optimal batching.",
                    tp.topic,
                    tp.bytes_per_sec / 1_000_000.0
                ),
            });
        }
    }

    // Check total lag
    let total_lag: i64 = consumer_lag.iter().map(|l| l.lag).sum();
    if total_lag > 100_000 {
        recommendations.push(ThroughputRecommendation {
            topic: "cluster".into(),
            severity: "critical".into(),
            message: format!(
                "Total consumer lag across all partitions is {}. Consumers may be falling behind. Consider adding more consumer instances or increasing partition count.",
                total_lag
            ),
        });
    }

    // Broker count recommendation
    if brokers.len() == 1 && total_topics > 5 {
        recommendations.push(ThroughputRecommendation {
            topic: "cluster".into(),
            severity: "info".into(),
            message: "Running with a single broker. For production workloads with multiple topics, consider adding more brokers for redundancy and throughput.".into(),
        });
    }

    // Broker metrics from rdkafka statistics
    let broker_metrics = extract_broker_metrics(kafka_stats);

    Ok(ClusterInsightsResponse {
        brokers,
        cluster_id: None,
        total_topics,
        total_partitions,
        consumer_lag,
        topic_throughput,
        recommendations,
        broker_metrics,
    })
}
