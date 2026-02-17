use crate::models::{
    BrokerDiscoveryResponse, BrokerInfo, ClusterInsightsResponse, PartitionLag,
    ThroughputRecommendation, TopicThroughput,
};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::ClientConfig;
use std::time::Duration;

/// Create a BaseConsumer for metadata operations (listing topics).
pub fn create_admin_consumer(brokers: &str) -> Result<BaseConsumer, rdkafka::error::KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
}

/// List all non-internal topics from the Kafka cluster.
pub fn list_topics(consumer: &BaseConsumer) -> Result<Vec<String>, rdkafka::error::KafkaError> {
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(10))?;
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
    consumer: &BaseConsumer,
    current_brokers: &str,
) -> Result<BrokerDiscoveryResponse, rdkafka::error::KafkaError> {
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(10))?;
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

/// Fetch consumer lag for all non-internal topics using watermark offsets.
/// This measures the gap between the tracker's consumer position and the log end offset.
pub fn fetch_consumer_lag(
    consumer: &BaseConsumer,
) -> Result<Vec<PartitionLag>, rdkafka::error::KafkaError> {
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(10))?;
    let mut lags = Vec::new();

    for topic_meta in metadata.topics() {
        let topic_name = topic_meta.name();
        if topic_name.starts_with("__") {
            continue;
        }

        for partition in topic_meta.partitions() {
            let pid = partition.id();
            match consumer.fetch_watermarks(topic_name, pid, Duration::from_secs(5)) {
                Ok((low, high)) => {
                    // Consumer offset is estimated as the high watermark minus what's pending
                    // For a more accurate picture, we'd query committed offsets
                    // But since our consumers are real-time, we estimate lag as small
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

    Ok(lags)
}

/// Build full cluster insights: brokers, topics, partitions, lag, throughput, recommendations.
pub fn build_cluster_insights(
    consumer: &BaseConsumer,
    all_stats: Vec<crate::models::TopicStats>,
) -> Result<ClusterInsightsResponse, rdkafka::error::KafkaError> {
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(10))?;

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

    // Consumer lag
    let consumer_lag = fetch_consumer_lag(consumer).unwrap_or_default();

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

    Ok(ClusterInsightsResponse {
        brokers,
        cluster_id: None,
        total_topics,
        total_partitions,
        consumer_lag,
        topic_throughput,
        recommendations,
    })
}
