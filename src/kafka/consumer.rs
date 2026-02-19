use crate::config::SecurityConfig;
use crate::models::{MessageSizeEvent, StartPosition};
use crate::tracker::alerts::AlertEngine;
use chrono::{DateTime, Utc};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::time::Duration;
use tokio::sync::broadcast;

/// Run a consumer that tracks message sizes for a single topic.
/// Extracts only payload length (never materializes message body into String).
/// Reads the current capture threshold from AlertEngine on each message, so
/// threshold changes via the API take effect immediately without restarting consumers.
/// Sends MessageSizeEvent through the broadcast channel.
///
/// `start_position` controls where to start reading:
/// - None / Latest: start from the latest offset (default)
/// - Beginning: start from the earliest offset
/// - Timestamp: seek to a specific timestamp
/// - Offset: seek to a specific partition/offset
pub async fn run_size_tracking_consumer(
    brokers: String,
    topic: String,
    group_id: String,
    tx: broadcast::Sender<MessageSizeEvent>,
    alert_engine: AlertEngine,
    start_position: Option<StartPosition>,
    security: SecurityConfig,
) {
    let auto_offset_reset = match &start_position {
        Some(StartPosition::Beginning) => "earliest",
        _ => "latest",
    };

    // Use a unique consumer group per instance so that previously committed
    // offsets never interfere with the requested start position on reconnection.
    let unique_group = format!("{}-{}-{}", group_id, topic, super::browser::uuid_simple());

    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &brokers)
        .set("group.id", &unique_group)
        .set("auto.offset.reset", auto_offset_reset)
        .set("enable.auto.commit", "false");
    super::apply_security(&mut config, &security);
    super::apply_timeouts(&mut config);

    let consumer: StreamConsumer = match config.create() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("Failed to create consumer for topic {}: {}", topic, e);
            return;
        }
    };

    // For non-latest start positions, use assign() instead of subscribe()
    // to bypass consumer group offset management and get deterministic seeking.
    match &start_position {
        Some(StartPosition::Beginning) => {
            // Use assign() with Beginning offset to bypass committed offsets
            let metadata = match consumer.fetch_metadata(Some(&topic), Duration::from_secs(10)) {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!("Failed to fetch metadata for topic {}: {}", topic, e);
                    return;
                }
            };
            let partition_count = metadata
                .topics()
                .iter()
                .find(|t| t.name() == topic)
                .map(|t| t.partitions().len() as i32)
                .unwrap_or(0);

            if partition_count == 0 {
                tracing::warn!("No partitions found for topic {}", topic);
                return;
            }

            let mut tpl = TopicPartitionList::new();
            for p in 0..partition_count {
                let _ = tpl.add_partition_offset(&topic, p, Offset::Beginning);
            }
            if let Err(e) = consumer.assign(&tpl) {
                tracing::error!("Failed to assign partitions for topic {}: {}", topic, e);
                return;
            }
            tracing::info!("Started size-tracking consumer for topic: {} (from beginning, {} partitions)", topic, partition_count);
        }
        Some(StartPosition::Timestamp { value }) => {
            let ts = match value.parse::<DateTime<Utc>>() {
                Ok(dt) => dt,
                Err(e) => {
                    tracing::error!("Invalid timestamp '{}' for topic {}: {}", value, topic, e);
                    return;
                }
            };
            let ts_millis = ts.timestamp_millis();

            let metadata = match consumer.fetch_metadata(Some(&topic), Duration::from_secs(10)) {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!("Failed to fetch metadata for topic {}: {}", topic, e);
                    return;
                }
            };
            let partition_count = metadata
                .topics()
                .iter()
                .find(|t| t.name() == topic)
                .map(|t| t.partitions().len() as i32)
                .unwrap_or(0);

            if partition_count == 0 {
                tracing::warn!("No partitions found for topic {}", topic);
                return;
            }

            // Build TPL with timestamps for offset lookup
            let mut tpl = TopicPartitionList::new();
            for p in 0..partition_count {
                if let Err(e) = tpl.add_partition_offset(&topic, p, Offset::Offset(ts_millis)) {
                    tracing::error!("Failed to set timestamp offset for topic {}: {}", topic, e);
                    return;
                }
            }

            let resolved = match consumer.offsets_for_times(tpl, Duration::from_secs(10)) {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("Failed to resolve timestamps for topic {}: {}", topic, e);
                    return;
                }
            };

            // Build assignment from resolved offsets
            let mut assign_tpl = TopicPartitionList::new();
            for elem in resolved.elements() {
                match elem.offset() {
                    Offset::Offset(o) if o >= 0 => {
                        let _ = assign_tpl.add_partition_offset(&topic, elem.partition(), Offset::Offset(o));
                    }
                    _ => {
                        // No messages at/after timestamp — start from end
                        let _ = assign_tpl.add_partition_offset(&topic, elem.partition(), Offset::End);
                    }
                }
            }

            if let Err(e) = consumer.assign(&assign_tpl) {
                tracing::error!("Failed to assign partitions for topic {}: {}", topic, e);
                return;
            }
            tracing::info!("Started size-tracking consumer for topic: {} (from timestamp {})", topic, value);
        }
        Some(StartPosition::Offset { partition, offset }) => {
            let mut tpl = TopicPartitionList::new();
            if let Err(e) = tpl.add_partition_offset(&topic, *partition, Offset::Offset(*offset)) {
                tracing::error!("Failed to set offset for topic {}: {}", topic, e);
                return;
            }
            if let Err(e) = consumer.assign(&tpl) {
                tracing::error!("Failed to assign partition for topic {}: {}", topic, e);
                return;
            }
            tracing::info!("Started size-tracking consumer for topic: {} (from partition {} offset {})", topic, partition, offset);
        }
        _ => {
            // Latest / None — use subscribe() for normal consumer group behavior
            if let Err(e) = consumer.subscribe(&[&topic]) {
                tracing::error!("Failed to subscribe to topic {}: {}", topic, e);
                return;
            }
            tracing::info!("Started size-tracking consumer for topic: {} (from latest)", topic);
        }
    }

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let payload_size = msg.payload().map_or(0, |p| p.len());
                let key_size = msg.key().map(|k| k.len());

                // Read the current capture threshold dynamically from AlertEngine
                let capture_threshold = alert_engine.get_capture_threshold(&topic).await;

                // Capture raw bytes only for oversized messages
                let (captured_payload, captured_key) = if payload_size >= capture_threshold {
                    (
                        msg.payload().map(|p| p.to_vec()),
                        msg.key().map(|k| k.to_vec()),
                    )
                } else {
                    (None, None)
                };

                let ts = msg
                    .timestamp()
                    .to_millis()
                    .and_then(|ms| DateTime::from_timestamp_millis(ms))
                    .unwrap_or_else(Utc::now);

                let event = MessageSizeEvent {
                    topic: topic.clone(),
                    partition: msg.partition(),
                    offset: msg.offset(),
                    size_bytes: payload_size,
                    key_size,
                    timestamp: ts,
                    captured_payload,
                    captured_key,
                };

                // Ignore send errors (no active receivers)
                let _ = tx.send(event);
            }
            Err(e) => {
                tracing::error!("Kafka recv error on topic {}: {}", topic, e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
