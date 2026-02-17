use crate::models::MessageSizeEvent;
use chrono::{DateTime, Utc};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use std::time::Duration;
use tokio::sync::broadcast;

/// Run a consumer that tracks message sizes for a single topic.
/// Extracts only payload length (never materializes message body into String).
/// Sends MessageSizeEvent through the broadcast channel.
pub async fn run_size_tracking_consumer(
    brokers: String,
    topic: String,
    group_id: String,
    tx: broadcast::Sender<MessageSizeEvent>,
) {
    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", format!("{}-{}", group_id, topic))
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .create()
    {
        Ok(c) => c,
        Err(e) => {
            log::error!("Failed to create consumer for topic {}: {}", topic, e);
            return;
        }
    };

    if let Err(e) = consumer.subscribe(&[&topic]) {
        log::error!("Failed to subscribe to topic {}: {}", topic, e);
        return;
    }

    log::info!("Started size-tracking consumer for topic: {}", topic);

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let payload_size = msg.payload().map_or(0, |p| p.len());
                let key_size = msg.key().map(|k| k.len());

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
                };

                // Ignore send errors (no active receivers)
                let _ = tx.send(event);
            }
            Err(e) => {
                log::error!("Kafka recv error on topic {}: {}", topic, e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
