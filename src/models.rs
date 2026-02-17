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
}
