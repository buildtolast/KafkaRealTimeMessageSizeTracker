use crate::config::SecurityConfig;
use crate::models::{
    BrowseMessagesResponse, HistoricalMessage, MessageHeader, MessagePayload, PartitionOffset,
};
use chrono::{DateTime, Utc};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Headers;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::collections::HashMap;
use std::time::Duration;

/// Maximum payload preview size (1 KB).
const MAX_PREVIEW_BYTES: usize = 1024;
/// Maximum messages per request.
const MAX_MESSAGES: usize = 100;
/// Consumer poll timeout per iteration.
const POLL_TIMEOUT: Duration = Duration::from_secs(5);
/// Max total poll time before giving up.
const MAX_POLL_DURATION: Duration = Duration::from_secs(10);

/// Encode raw bytes as a MessagePayload.
/// If valid UTF-8, use utf8 encoding; otherwise base64.
pub fn encode_payload(bytes: &[u8], truncate_at: Option<usize>) -> MessagePayload {
    let data_bytes = match truncate_at {
        Some(max) if bytes.len() > max => &bytes[..max],
        _ => bytes,
    };
    match std::str::from_utf8(data_bytes) {
        Ok(s) => MessagePayload {
            encoding: "utf8".into(),
            data: s.to_string(),
        },
        Err(_) => {
            use base64::Engine;
            MessagePayload {
                encoding: "base64".into(),
                data: base64::engine::general_purpose::STANDARD.encode(data_bytes),
            }
        }
    }
}

/// Browse messages starting at a specific offset on a specific partition.
pub fn browse_by_offset(
    brokers: &str,
    topic: &str,
    partition: i32,
    start_offset: i64,
    limit: usize,
    security: &SecurityConfig,
) -> Result<BrowseMessagesResponse, String> {
    let limit = limit.min(MAX_MESSAGES);
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("size-tracker-browser-{}", uuid_simple()))
        .set("enable.auto.commit", "false");
    super::apply_security(&mut config, security);
    super::apply_timeouts(&mut config);
    let consumer: BaseConsumer = config
        .create()
        .map_err(|e| format!("Failed to create consumer: {}", e))?;

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Offset(start_offset))
        .map_err(|e| format!("Failed to set offset: {}", e))?;
    consumer
        .assign(&tpl)
        .map_err(|e| format!("Failed to assign partitions: {}", e))?;

    let mut messages = Vec::new();
    let mut last_offset = start_offset;
    let deadline = std::time::Instant::now() + MAX_POLL_DURATION;

    while messages.len() < limit && std::time::Instant::now() < deadline {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(msg)) => {
                last_offset = msg.offset();
                messages.push(convert_message(&msg, topic));
            }
            Some(Err(e)) => {
                tracing::warn!("Poll error browsing {}: {}", topic, e);
                break;
            }
            None => break,
        }
    }

    let has_more = messages.len() == limit;
    let next_offsets = vec![PartitionOffset {
        partition,
        offset: last_offset + 1,
    }];

    Ok(BrowseMessagesResponse {
        topic: topic.to_string(),
        messages,
        next_offsets,
        has_more,
    })
}

/// Browse messages starting at a timestamp across all partitions.
pub fn browse_by_timestamp(
    brokers: &str,
    topic: &str,
    timestamp: DateTime<Utc>,
    limit: usize,
    security: &SecurityConfig,
) -> Result<BrowseMessagesResponse, String> {
    let limit = limit.min(MAX_MESSAGES);
    let ts_millis = timestamp.timestamp_millis();

    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("size-tracker-browser-{}", uuid_simple()))
        .set("enable.auto.commit", "false");
    super::apply_security(&mut config, security);
    super::apply_timeouts(&mut config);
    let consumer: BaseConsumer = config
        .create()
        .map_err(|e| format!("Failed to create consumer: {}", e))?;

    // Get partition count
    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|e| format!("Failed to fetch metadata: {}", e))?;
    let partition_count = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .map(|t| t.partitions().len() as i32)
        .unwrap_or(0);

    if partition_count == 0 {
        return Ok(BrowseMessagesResponse {
            topic: topic.to_string(),
            messages: vec![],
            next_offsets: vec![],
            has_more: false,
        });
    }

    // Build TPL with timestamps for offset lookup
    let mut tpl = TopicPartitionList::new();
    for p in 0..partition_count {
        tpl.add_partition_offset(topic, p, Offset::Offset(ts_millis))
            .map_err(|e| format!("Failed to set timestamp offset: {}", e))?;
    }

    // Convert timestamps to offsets
    let resolved = consumer
        .offsets_for_times(tpl, Duration::from_secs(10))
        .map_err(|e| format!("Failed to resolve timestamps: {}", e))?;

    // Filter out invalid offsets and assign
    let mut assign_tpl = TopicPartitionList::new();
    for elem in resolved.elements() {
        match elem.offset() {
            Offset::Offset(o) if o >= 0 => {
                assign_tpl
                    .add_partition_offset(topic, elem.partition(), Offset::Offset(o))
                    .map_err(|e| format!("Failed to add partition offset: {}", e))?;
            }
            _ => {
                // Partition had no messages at or after this timestamp, skip it
            }
        }
    }

    if assign_tpl.count() == 0 {
        return Ok(BrowseMessagesResponse {
            topic: topic.to_string(),
            messages: vec![],
            next_offsets: vec![],
            has_more: false,
        });
    }

    consumer
        .assign(&assign_tpl)
        .map_err(|e| format!("Failed to assign partitions: {}", e))?;

    // Poll messages
    let mut messages = Vec::new();
    let mut last_offsets: HashMap<i32, i64> = HashMap::new();
    let deadline = std::time::Instant::now() + MAX_POLL_DURATION;

    while messages.len() < limit && std::time::Instant::now() < deadline {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(msg)) => {
                last_offsets.insert(msg.partition(), msg.offset());
                messages.push(convert_message(&msg, topic));
            }
            Some(Err(e)) => {
                tracing::warn!("Poll error browsing {}: {}", topic, e);
                break;
            }
            None => break,
        }
    }

    // Sort by timestamp then partition
    messages.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then(a.partition.cmp(&b.partition)));

    let has_more = messages.len() == limit;
    let next_offsets: Vec<PartitionOffset> = last_offsets
        .into_iter()
        .map(|(p, o)| PartitionOffset {
            partition: p,
            offset: o + 1,
        })
        .collect();

    Ok(BrowseMessagesResponse {
        topic: topic.to_string(),
        messages,
        next_offsets,
        has_more,
    })
}

/// Browse messages from the very beginning of a topic (all partitions).
pub fn browse_from_beginning(
    brokers: &str,
    topic: &str,
    limit: usize,
    security: &SecurityConfig,
) -> Result<BrowseMessagesResponse, String> {
    let limit = limit.min(MAX_MESSAGES);
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("size-tracker-browser-{}", uuid_simple()))
        .set("enable.auto.commit", "false");
    super::apply_security(&mut config, security);
    super::apply_timeouts(&mut config);
    let consumer: BaseConsumer = config
        .create()
        .map_err(|e| format!("Failed to create consumer: {}", e))?;

    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|e| format!("Failed to fetch metadata: {}", e))?;
    let partition_count = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .map(|t| t.partitions().len() as i32)
        .unwrap_or(0);

    if partition_count == 0 {
        return Ok(BrowseMessagesResponse {
            topic: topic.to_string(),
            messages: vec![],
            next_offsets: vec![],
            has_more: false,
        });
    }

    let mut tpl = TopicPartitionList::new();
    for p in 0..partition_count {
        tpl.add_partition_offset(topic, p, Offset::Beginning)
            .map_err(|e| format!("Failed to set beginning offset: {}", e))?;
    }
    consumer
        .assign(&tpl)
        .map_err(|e| format!("Failed to assign partitions: {}", e))?;

    let mut messages = Vec::new();
    let mut last_offsets: HashMap<i32, i64> = HashMap::new();
    let deadline = std::time::Instant::now() + MAX_POLL_DURATION;

    while messages.len() < limit && std::time::Instant::now() < deadline {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(msg)) => {
                last_offsets.insert(msg.partition(), msg.offset());
                messages.push(convert_message(&msg, topic));
            }
            Some(Err(e)) => {
                tracing::warn!("Poll error browsing {} from beginning: {}", topic, e);
                break;
            }
            None => break,
        }
    }

    messages.sort_by(|a, b| a.offset.cmp(&b.offset).then(a.partition.cmp(&b.partition)));

    let has_more = messages.len() == limit;
    let next_offsets: Vec<PartitionOffset> = last_offsets
        .into_iter()
        .map(|(p, o)| PartitionOffset {
            partition: p,
            offset: o + 1,
        })
        .collect();

    Ok(BrowseMessagesResponse {
        topic: topic.to_string(),
        messages,
        next_offsets,
        has_more,
    })
}

/// Browse the latest messages from a topic (seeks to near the end of each partition).
pub fn browse_latest(
    brokers: &str,
    topic: &str,
    limit: usize,
    security: &SecurityConfig,
) -> Result<BrowseMessagesResponse, String> {
    let limit = limit.min(MAX_MESSAGES);
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("size-tracker-browser-{}", uuid_simple()))
        .set("enable.auto.commit", "false");
    super::apply_security(&mut config, security);
    super::apply_timeouts(&mut config);
    let consumer: BaseConsumer = config
        .create()
        .map_err(|e| format!("Failed to create consumer: {}", e))?;

    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|e| format!("Failed to fetch metadata: {}", e))?;
    let partition_count = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .map(|t| t.partitions().len() as i32)
        .unwrap_or(0);

    if partition_count == 0 {
        return Ok(BrowseMessagesResponse {
            topic: topic.to_string(),
            messages: vec![],
            next_offsets: vec![],
            has_more: false,
        });
    }

    // Get high watermarks and seek to (high - limit_per_partition)
    let per_partition_limit = (limit / partition_count as usize).max(1);
    let mut tpl = TopicPartitionList::new();
    for p in 0..partition_count {
        let (low, high) = consumer
            .fetch_watermarks(topic, p, Duration::from_secs(5))
            .map_err(|e| format!("Failed to fetch watermarks for partition {}: {}", p, e))?;
        let start = (high - per_partition_limit as i64).max(low);
        tpl.add_partition_offset(topic, p, Offset::Offset(start))
            .map_err(|e| format!("Failed to set offset: {}", e))?;
    }

    consumer
        .assign(&tpl)
        .map_err(|e| format!("Failed to assign partitions: {}", e))?;

    let mut messages = Vec::new();
    let mut last_offsets: HashMap<i32, i64> = HashMap::new();
    let deadline = std::time::Instant::now() + MAX_POLL_DURATION;

    while messages.len() < limit && std::time::Instant::now() < deadline {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(msg)) => {
                last_offsets.insert(msg.partition(), msg.offset());
                messages.push(convert_message(&msg, topic));
            }
            Some(Err(e)) => {
                tracing::warn!("Poll error browsing {} latest: {}", topic, e);
                break;
            }
            None => break,
        }
    }

    messages.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then(a.partition.cmp(&b.partition)));

    let has_more = messages.len() == limit;
    let next_offsets: Vec<PartitionOffset> = last_offsets
        .into_iter()
        .map(|(p, o)| PartitionOffset {
            partition: p,
            offset: o + 1,
        })
        .collect();

    Ok(BrowseMessagesResponse {
        topic: topic.to_string(),
        messages,
        next_offsets,
        has_more,
    })
}

/// Fetch a single full message payload for download.
pub fn fetch_full_message(
    brokers: &str,
    topic: &str,
    partition: i32,
    offset: i64,
    security: &SecurityConfig,
) -> Result<Option<Vec<u8>>, String> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("size-tracker-browser-{}", uuid_simple()))
        .set("enable.auto.commit", "false");
    super::apply_security(&mut config, security);
    super::apply_timeouts(&mut config);
    let consumer: BaseConsumer = config
        .create()
        .map_err(|e| format!("Failed to create consumer: {}", e))?;

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Offset(offset))
        .map_err(|e| format!("Failed to set offset: {}", e))?;
    consumer
        .assign(&tpl)
        .map_err(|e| format!("Failed to assign partitions: {}", e))?;

    match consumer.poll(Duration::from_secs(5)) {
        Some(Ok(msg)) if msg.offset() == offset => Ok(msg.payload().map(|p| p.to_vec())),
        Some(Ok(_)) => Ok(None), // Got a different offset
        Some(Err(e)) => Err(format!("Poll error: {}", e)),
        None => Ok(None),
    }
}

/// Convert a BorrowedMessage to HistoricalMessage.
fn convert_message(
    msg: &rdkafka::message::BorrowedMessage<'_>,
    topic: &str,
) -> HistoricalMessage {
    let payload = msg.payload();
    let payload_size = payload.map_or(0, |p| p.len());

    let key = msg.key().map(|k| encode_payload(k, None));
    let value_preview = payload.map(|p| encode_payload(p, Some(MAX_PREVIEW_BYTES)));
    let value_truncated = payload_size > MAX_PREVIEW_BYTES;

    let headers = msg
        .headers()
        .map(|hdrs| {
            (0..hdrs.count())
                .filter_map(|i| {
                    hdrs.get_as::<[u8]>(i).ok().map(|h| {
                        use base64::Engine;
                        MessageHeader {
                            key: h.key.to_string(),
                            value: h.value.map(|v| {
                                base64::engine::general_purpose::STANDARD.encode(v)
                            }),
                        }
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let ts = msg
        .timestamp()
        .to_millis()
        .and_then(|ms| DateTime::from_timestamp_millis(ms))
        .unwrap_or_else(Utc::now);

    HistoricalMessage {
        topic: topic.to_string(),
        partition: msg.partition(),
        offset: msg.offset(),
        timestamp: ts,
        size_bytes: payload_size,
        key,
        value_preview,
        value_truncated,
        value_size: payload_size,
        headers,
    }
}

/// Simple unique-ish ID for consumer groups (avoids needing uuid crate).
pub fn uuid_simple() -> String {
    use std::time::SystemTime;
    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", ts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_payload_utf8() {
        let payload = encode_payload(b"hello world", None);
        assert_eq!(payload.encoding, "utf8");
        assert_eq!(payload.data, "hello world");
    }

    #[test]
    fn test_encode_payload_binary() {
        let payload = encode_payload(&[0xFF, 0xFE, 0x00, 0x01], None);
        assert_eq!(payload.encoding, "base64");
        // Verify it's valid base64
        assert!(!payload.data.is_empty());
    }

    #[test]
    fn test_encode_payload_truncation() {
        let data = vec![b'a'; 2000];
        let payload = encode_payload(&data, Some(1024));
        assert_eq!(payload.data.len(), 1024);
        assert_eq!(payload.encoding, "utf8");
    }

    #[test]
    fn test_encode_payload_no_truncation_under_limit() {
        let data = b"short";
        let payload = encode_payload(data, Some(1024));
        assert_eq!(payload.data, "short");
        assert_eq!(payload.encoding, "utf8");
    }

    #[test]
    fn test_uuid_simple_unique() {
        let a = uuid_simple();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let b = uuid_simple();
        assert_ne!(a, b);
    }
}
