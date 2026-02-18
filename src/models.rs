use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A single message size measurement event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageSizeEvent {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub size_bytes: usize,
    pub key_size: Option<usize>,
    pub timestamp: DateTime<Utc>,
    /// Raw payload bytes captured when message exceeds alert threshold.
    /// Skipped during serialization (never sent over WebSocket).
    #[serde(skip)]
    pub captured_payload: Option<Vec<u8>>,
    /// Raw key bytes captured alongside oversized payload.
    #[serde(skip)]
    pub captured_key: Option<Vec<u8>>,
}

/// Real-time stats for a single topic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicStats {
    pub topic: String,
    pub message_count: u64,
    pub total_bytes: u64,
    pub avg_bytes: f64,
    pub min_bytes: usize,
    pub max_bytes: usize,
    pub p50: usize,
    pub p95: usize,
    pub p99: usize,
    pub messages_per_sec: f64,
    pub bytes_per_sec: f64,
    pub last_updated: DateTime<Utc>,
}

/// Alert severity levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Warning,
    Critical,
}

/// An alert triggered by an oversized message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: u64,
    pub topic: String,
    pub severity: AlertSeverity,
    pub message_size: usize,
    pub threshold: usize,
    pub timestamp: DateTime<Utc>,
    /// If the oversized message payload was captured, this holds the alert ID for download.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_id: Option<u64>,
}

/// A captured oversized message payload available for download.
#[derive(Debug, Clone)]
pub struct CapturedMessage {
    pub alert_id: u64,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub payload: Vec<u8>,
    pub key: Option<Vec<u8>>,
    pub size_bytes: usize,
    pub timestamp: DateTime<Utc>,
}

/// Per-topic alert threshold configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertConfig {
    pub topic: String,
    pub warning_threshold: usize,
    pub critical_threshold: usize,
    pub enabled: bool,
}

/// A single data point in time-series history (per-minute aggregate)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryDataPoint {
    pub timestamp: DateTime<Utc>,
    pub message_count: u64,
    pub avg_bytes: f64,
    pub min_bytes: usize,
    pub max_bytes: usize,
    pub p95: usize,
    pub total_bytes: u64,
}

/// Topics list response
#[derive(Debug, Serialize)]
pub struct TopicsResponse {
    pub topics: Vec<TopicSummary>,
}

/// Summary of a single topic (for the overview grid)
#[derive(Debug, Serialize)]
pub struct TopicSummary {
    pub name: String,
    pub stats: Option<TopicStats>,
    pub alert_status: AlertStatus,
}

/// Alert status for a topic
#[derive(Debug, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AlertStatus {
    Healthy,
    Warning,
    Critical,
    Unknown,
}

/// Broker config request
#[derive(Debug, Deserialize)]
pub struct BrokerRequest {
    pub brokers: String,
}

/// Broker config response
#[derive(Debug, Serialize)]
pub struct BrokerResponse {
    pub brokers: String,
}

/// Alert config update request
#[derive(Debug, Deserialize)]
pub struct AlertConfigRequest {
    pub topic: String,
    pub warning_threshold: Option<usize>,
    pub critical_threshold: Option<usize>,
    pub enabled: Option<bool>,
}

/// Global alert defaults (runtime-mutable)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertDefaultsResponse {
    pub warning_threshold: usize,
    pub critical_threshold: usize,
}

/// Request to update global alert defaults at runtime
#[derive(Debug, Deserialize)]
pub struct AlertDefaultsRequest {
    pub warning_threshold: Option<usize>,
    pub critical_threshold: Option<usize>,
}

/// WebSocket message types sent to the frontend
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum WsMessage {
    #[serde(rename = "size_event")]
    SizeEvent(MessageSizeEvent),
    #[serde(rename = "stats_update")]
    StatsUpdate(TopicStats),
    #[serde(rename = "alert")]
    Alert(Alert),
}

/// WebSocket client command
#[derive(Debug, Deserialize)]
pub struct WsClientMessage {
    pub action: String,
    pub topics: Option<Vec<String>>,
}

// ═══════════════════════════════════════════════════════════════════
//   HISTORICAL MESSAGE BROWSER TYPES
// ═══════════════════════════════════════════════════════════════════

/// A single historical message fetched from Kafka for browsing.
#[derive(Debug, Clone, Serialize)]
pub struct HistoricalMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: DateTime<Utc>,
    pub size_bytes: usize,
    /// Key: auto-detected as UTF-8 or base64.
    pub key: Option<MessagePayload>,
    /// Value preview: first 1KB, auto-detected as UTF-8 or base64.
    pub value_preview: Option<MessagePayload>,
    /// Whether the value was truncated (original > 1KB).
    pub value_truncated: bool,
    /// Full value size in bytes.
    pub value_size: usize,
    /// Kafka message headers.
    pub headers: Vec<MessageHeader>,
}

/// Representation of a message key or value payload with encoding info.
#[derive(Debug, Clone, Serialize)]
pub struct MessagePayload {
    /// "utf8" or "base64"
    pub encoding: String,
    /// The encoded data.
    pub data: String,
}

/// A single Kafka message header.
#[derive(Debug, Clone, Serialize)]
pub struct MessageHeader {
    pub key: String,
    /// Header value (base64-encoded since headers are raw bytes).
    pub value: Option<String>,
}

/// Response from the browse messages endpoint.
#[derive(Debug, Serialize)]
pub struct BrowseMessagesResponse {
    pub topic: String,
    pub messages: Vec<HistoricalMessage>,
    /// Pagination cursor: for each partition, the next offset to fetch.
    pub next_offsets: Vec<PartitionOffset>,
    /// Whether there are potentially more messages to fetch.
    pub has_more: bool,
}

/// Partition + offset pair for pagination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionOffset {
    pub partition: i32,
    pub offset: i64,
}

/// Query parameters for GET /api/browse/{topic}
#[derive(Debug, Deserialize)]
pub struct BrowseQueryParams {
    pub partition: Option<i32>,
    pub offset: Option<i64>,
    /// ISO 8601 timestamp (used if offset is not provided).
    pub timestamp: Option<String>,
    /// Max messages to return (default 50, max 100).
    pub limit: Option<usize>,
    /// Quick preset: "beginning", "1h", "1d", "latest".
    pub preset: Option<String>,
}

/// Per-topic snapshot data point at a historical timestamp.
#[derive(Debug, Serialize)]
pub struct TopicSnapshot {
    pub topic: String,
    /// The closest HistoryDataPoint to the requested timestamp.
    pub data_point: Option<HistoryDataPoint>,
    /// How far (in seconds) the actual data point is from the requested timestamp.
    pub offset_seconds: Option<i64>,
}

/// Response for a global dashboard snapshot.
#[derive(Debug, Serialize)]
pub struct GlobalSnapshotResponse {
    pub requested_timestamp: DateTime<Utc>,
    pub topics: Vec<TopicSnapshot>,
}

/// Query parameters for GET /api/snapshot
#[derive(Debug, Deserialize)]
pub struct SnapshotQueryParams {
    /// ISO 8601 timestamp.
    pub timestamp: String,
}

// ═══════════════════════════════════════════════════════════════════
//   TOPIC SELECTION TYPES
// ═══════════════════════════════════════════════════════════════════

/// Where to start consuming a topic from.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StartPosition {
    Latest,
    Beginning,
    Timestamp { value: String },
    Offset { partition: i32, offset: i64 },
}

/// Request to toggle topics on/off for monitoring.
#[derive(Debug, Deserialize)]
pub struct TopicToggleRequest {
    pub topics: Vec<String>,
    pub enabled: bool,
    #[serde(default)]
    pub start_from: Option<StartPosition>,
}

/// Response with available and tracked topics.
#[derive(Debug, Serialize)]
pub struct AvailableTopicsResponse {
    pub available: Vec<String>,
    pub tracked: Vec<String>,
}

// ═══════════════════════════════════════════════════════════════════
//   BROKER DISCOVERY + CLUSTER INSIGHTS TYPES
// ═══════════════════════════════════════════════════════════════════

/// Information about a single Kafka broker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

/// Broker discovery response.
#[derive(Debug, Serialize)]
pub struct BrokerDiscoveryResponse {
    pub current_brokers: String,
    pub discovered_brokers: Vec<BrokerInfo>,
    pub cluster_id: Option<String>,
}

/// Consumer group lag info for a single topic-partition.
#[derive(Debug, Clone, Serialize)]
pub struct PartitionLag {
    pub topic: String,
    pub partition: i32,
    pub current_offset: i64,
    pub log_end_offset: i64,
    pub lag: i64,
}

/// Per-topic throughput summary.
#[derive(Debug, Clone, Serialize)]
pub struct TopicThroughput {
    pub topic: String,
    pub partitions: i32,
    pub messages_per_sec: f64,
    pub bytes_per_sec: f64,
    pub avg_message_size: f64,
    pub total_messages: u64,
    pub total_bytes: u64,
}

/// Throughput recommendation.
#[derive(Debug, Clone, Serialize)]
pub struct ThroughputRecommendation {
    pub topic: String,
    pub severity: String,
    pub message: String,
}

/// Full cluster insights response.
#[derive(Debug, Serialize)]
pub struct ClusterInsightsResponse {
    pub brokers: Vec<BrokerInfo>,
    pub cluster_id: Option<String>,
    pub total_topics: usize,
    pub total_partitions: i32,
    pub consumer_lag: Vec<PartitionLag>,
    pub topic_throughput: Vec<TopicThroughput>,
    pub recommendations: Vec<ThroughputRecommendation>,
}

/// A single lag time-series data point: per-topic aggregated lag at a moment in time.
#[derive(Debug, Clone, Serialize)]
pub struct LagDataPoint {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Per-topic total lag (summed across all partitions).
    pub topic_lags: Vec<TopicLag>,
    /// Total lag across all topics.
    pub total_lag: i64,
}

/// Per-topic lag aggregate.
#[derive(Debug, Clone, Serialize)]
pub struct TopicLag {
    pub topic: String,
    pub lag: i64,
    pub current_offset: i64,
    pub log_end_offset: i64,
}

/// Response for lag history endpoint.
#[derive(Debug, Serialize)]
pub struct LagHistoryResponse {
    pub points: Vec<LagDataPoint>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ws_message_serialization_size_event() {
        let event = MessageSizeEvent {
            topic: "orders".into(),
            partition: 0,
            offset: 42,
            size_bytes: 1024,
            key_size: Some(8),
            timestamp: Utc::now(),
            captured_payload: None,
            captured_key: None,
        };
        let msg = WsMessage::SizeEvent(event);
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""type":"size_event""#));
        assert!(json.contains(r#""topic":"orders""#));
    }

    #[test]
    fn test_ws_message_serialization_stats_update() {
        let stats = TopicStats {
            topic: "users".into(),
            message_count: 100,
            total_bytes: 50000,
            avg_bytes: 500.0,
            min_bytes: 100,
            max_bytes: 2000,
            p50: 450,
            p95: 1800,
            p99: 1950,
            messages_per_sec: 10.0,
            bytes_per_sec: 5000.0,
            last_updated: Utc::now(),
        };
        let msg = WsMessage::StatsUpdate(stats);
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""type":"stats_update""#));
    }

    #[test]
    fn test_alert_severity_serde() {
        let warning = AlertSeverity::Warning;
        let critical = AlertSeverity::Critical;
        assert_eq!(serde_json::to_string(&warning).unwrap(), r#""warning""#);
        assert_eq!(serde_json::to_string(&critical).unwrap(), r#""critical""#);
    }

    #[test]
    fn test_alert_config_request_deserialization() {
        let json = r#"{"topic":"orders","warning_threshold":500000,"enabled":true}"#;
        let req: AlertConfigRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.topic, "orders");
        assert_eq!(req.warning_threshold, Some(500000));
        assert!(req.critical_threshold.is_none());
        assert_eq!(req.enabled, Some(true));
    }

    #[test]
    fn test_alert_status_serialization() {
        assert_eq!(
            serde_json::to_string(&AlertStatus::Healthy).unwrap(),
            r#""healthy""#
        );
        assert_eq!(
            serde_json::to_string(&AlertStatus::Critical).unwrap(),
            r#""critical""#
        );
    }

    #[test]
    fn test_history_data_point_serde_roundtrip() {
        let point = HistoryDataPoint {
            timestamp: Utc::now(),
            message_count: 60,
            avg_bytes: 1024.5,
            min_bytes: 128,
            max_bytes: 8192,
            p95: 4096,
            total_bytes: 61470,
        };
        let json = serde_json::to_string(&point).unwrap();
        let decoded: HistoryDataPoint = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.message_count, 60);
        assert_eq!(decoded.min_bytes, 128);
    }

    #[test]
    fn test_historical_message_serialization() {
        let msg = HistoricalMessage {
            topic: "orders".into(),
            partition: 0,
            offset: 42,
            timestamp: Utc::now(),
            size_bytes: 1024,
            key: Some(MessagePayload {
                encoding: "utf8".into(),
                data: "order-123".into(),
            }),
            value_preview: Some(MessagePayload {
                encoding: "utf8".into(),
                data: r#"{"id":123}"#.into(),
            }),
            value_truncated: false,
            value_size: 1024,
            headers: vec![MessageHeader {
                key: "source".into(),
                value: Some("cGF5bWVudA==".into()),
            }],
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""partition":0"#));
        assert!(json.contains(r#""offset":42"#));
        assert!(json.contains("order-123"));
        assert!(json.contains("source"));
    }

    #[test]
    fn test_browse_messages_response_serialization() {
        let response = BrowseMessagesResponse {
            topic: "orders".into(),
            messages: vec![],
            next_offsets: vec![PartitionOffset {
                partition: 0,
                offset: 100,
            }],
            has_more: true,
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains(r#""topic":"orders""#));
        assert!(json.contains(r#""has_more":true"#));
        assert!(json.contains(r#""offset":100"#));
    }

    #[test]
    fn test_global_snapshot_response_serialization() {
        let response = GlobalSnapshotResponse {
            requested_timestamp: Utc::now(),
            topics: vec![TopicSnapshot {
                topic: "orders".into(),
                data_point: None,
                offset_seconds: None,
            }],
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains(r#""topic":"orders""#));
        assert!(json.contains(r#""data_point":null"#));
    }

    #[test]
    fn test_message_payload_serialization() {
        let payload = MessagePayload {
            encoding: "utf8".into(),
            data: "hello world".into(),
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains(r#""encoding":"utf8""#));
        assert!(json.contains(r#""data":"hello world""#));
    }

    #[test]
    fn test_topic_toggle_request_deserialization() {
        let json = r#"{"topics":["orders","users"],"enabled":true}"#;
        let req: TopicToggleRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.topics, vec!["orders", "users"]);
        assert!(req.enabled);
    }

    #[test]
    fn test_available_topics_response_serialization() {
        let resp = AvailableTopicsResponse {
            available: vec!["orders".into(), "users".into(), "logs".into()],
            tracked: vec!["orders".into()],
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(r#""available""#));
        assert!(json.contains(r#""tracked""#));
        assert!(json.contains(r#""orders""#));
    }

    #[test]
    fn test_broker_info_serialization() {
        let info = BrokerInfo {
            id: 1,
            host: "kafka-0".into(),
            port: 9092,
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains(r#""id":1"#));
        assert!(json.contains(r#""host":"kafka-0""#));
        assert!(json.contains(r#""port":9092"#));
    }

    #[test]
    fn test_broker_discovery_response_serialization() {
        let resp = BrokerDiscoveryResponse {
            current_brokers: "localhost:9092".into(),
            discovered_brokers: vec![BrokerInfo {
                id: 0,
                host: "localhost".into(),
                port: 9092,
            }],
            cluster_id: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(r#""current_brokers":"localhost:9092""#));
        assert!(json.contains(r#""discovered_brokers""#));
    }

    #[test]
    fn test_cluster_insights_response_serialization() {
        let resp = ClusterInsightsResponse {
            brokers: vec![BrokerInfo {
                id: 0,
                host: "kafka".into(),
                port: 9092,
            }],
            cluster_id: None,
            total_topics: 4,
            total_partitions: 12,
            consumer_lag: vec![PartitionLag {
                topic: "orders".into(),
                partition: 0,
                current_offset: 100,
                log_end_offset: 150,
                lag: 50,
            }],
            topic_throughput: vec![TopicThroughput {
                topic: "orders".into(),
                partitions: 3,
                messages_per_sec: 25.0,
                bytes_per_sec: 50000.0,
                avg_message_size: 2000.0,
                total_messages: 10000,
                total_bytes: 20000000,
            }],
            recommendations: vec![ThroughputRecommendation {
                topic: "orders".into(),
                severity: "info".into(),
                message: "Consider compression".into(),
            }],
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(r#""total_topics":4"#));
        assert!(json.contains(r#""total_partitions":12"#));
        assert!(json.contains(r#""lag":50"#));
        assert!(json.contains(r#""messages_per_sec":25.0"#));
        assert!(json.contains(r#""Consider compression""#));
    }

    #[test]
    fn test_browse_query_params_with_preset() {
        let json = r#"{"preset":"beginning","limit":50}"#;
        let params: BrowseQueryParams = serde_json::from_str(json).unwrap();
        assert_eq!(params.preset, Some("beginning".into()));
        assert_eq!(params.limit, Some(50));
        assert!(params.offset.is_none());
        assert!(params.timestamp.is_none());
    }

    #[test]
    fn test_partition_lag_serialization() {
        let lag = PartitionLag {
            topic: "orders".into(),
            partition: 2,
            current_offset: 500,
            log_end_offset: 550,
            lag: 50,
        };
        let json = serde_json::to_string(&lag).unwrap();
        assert!(json.contains(r#""partition":2"#));
        assert!(json.contains(r#""lag":50"#));
    }

    #[test]
    fn test_topic_throughput_serialization() {
        let tp = TopicThroughput {
            topic: "metrics".into(),
            partitions: 6,
            messages_per_sec: 100.5,
            bytes_per_sec: 200000.0,
            avg_message_size: 1990.0,
            total_messages: 50000,
            total_bytes: 99500000,
        };
        let json = serde_json::to_string(&tp).unwrap();
        assert!(json.contains(r#""partitions":6"#));
        assert!(json.contains(r#""messages_per_sec":100.5"#));
    }

    #[test]
    fn test_alert_defaults_response_serialization() {
        let resp = AlertDefaultsResponse {
            warning_threshold: 1048576,
            critical_threshold: 10485760,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(r#""warning_threshold":1048576"#));
        assert!(json.contains(r#""critical_threshold":10485760"#));
    }

    #[test]
    fn test_alert_defaults_request_deserialization() {
        let json = r#"{"warning_threshold":500000}"#;
        let req: AlertDefaultsRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.warning_threshold, Some(500000));
        assert!(req.critical_threshold.is_none());
    }

    #[test]
    fn test_start_position_latest_serde() {
        let pos = StartPosition::Latest;
        let json = serde_json::to_string(&pos).unwrap();
        assert!(json.contains(r#""type":"latest""#));
        let decoded: StartPosition = serde_json::from_str(&json).unwrap();
        assert!(matches!(decoded, StartPosition::Latest));
    }

    #[test]
    fn test_start_position_beginning_serde() {
        let json = r#"{"type":"beginning"}"#;
        let pos: StartPosition = serde_json::from_str(json).unwrap();
        assert!(matches!(pos, StartPosition::Beginning));
    }

    #[test]
    fn test_start_position_timestamp_serde() {
        let json = r#"{"type":"timestamp","value":"2026-01-15T10:30:00Z"}"#;
        let pos: StartPosition = serde_json::from_str(json).unwrap();
        match pos {
            StartPosition::Timestamp { value } => assert_eq!(value, "2026-01-15T10:30:00Z"),
            _ => panic!("Expected Timestamp variant"),
        }
    }

    #[test]
    fn test_start_position_offset_serde() {
        let json = r#"{"type":"offset","partition":2,"offset":1000}"#;
        let pos: StartPosition = serde_json::from_str(json).unwrap();
        match pos {
            StartPosition::Offset { partition, offset } => {
                assert_eq!(partition, 2);
                assert_eq!(offset, 1000);
            }
            _ => panic!("Expected Offset variant"),
        }
    }

    #[test]
    fn test_topic_toggle_request_with_start_from() {
        let json = r#"{"topics":["orders"],"enabled":true,"start_from":{"type":"beginning"}}"#;
        let req: TopicToggleRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.topics, vec!["orders"]);
        assert!(req.enabled);
        assert!(matches!(req.start_from, Some(StartPosition::Beginning)));
    }

    #[test]
    fn test_topic_toggle_request_without_start_from() {
        let json = r#"{"topics":["orders","users"],"enabled":true}"#;
        let req: TopicToggleRequest = serde_json::from_str(json).unwrap();
        assert!(req.start_from.is_none());
    }
}
