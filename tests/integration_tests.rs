//! Integration tests for KafkaRealTimeMessageSizeTracker.
//!
//! These tests verify cross-module behavior without requiring a live Kafka cluster.
//! They test the internal data flow: events → stats → alerts → storage.

use kafka_message_size_tracker::config::{AppConfig, SecurityConfig};
use kafka_message_size_tracker::models::{
    AlertConfig, AlertSeverity, CapturedMessage, HistoryDataPoint, MessageSizeEvent,
};
use kafka_message_size_tracker::storage::TimeSeriesStorage;
use kafka_message_size_tracker::tracker::alerts::AlertEngine;
use kafka_message_size_tracker::tracker::progress::ProgressTracker;
use kafka_message_size_tracker::tracker::stats::StatsEngine;

use chrono::Utc;

fn make_event(topic: &str, size: usize) -> MessageSizeEvent {
    MessageSizeEvent {
        topic: topic.to_string(),
        partition: 0,
        offset: 0,
        size_bytes: size,
        key_size: None,
        timestamp: Utc::now(),
        captured_payload: None,
        captured_key: None,
    }
}

fn make_event_with_offset(topic: &str, size: usize, partition: i32, offset: i64) -> MessageSizeEvent {
    MessageSizeEvent {
        topic: topic.to_string(),
        partition,
        offset,
        size_bytes: size,
        key_size: Some(10),
        timestamp: Utc::now(),
        captured_payload: None,
        captured_key: None,
    }
}

// ═══════════════════════════════════════════════════════════════════
// Config Validation Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_config_validation_valid() {
    let config = AppConfig {
        kafka_brokers: "localhost:9092".into(),
        server_host: "127.0.0.1".into(),
        server_port: 5688,
        default_warning_threshold: 1_048_576,
        default_critical_threshold: 10_485_760,
        consumer_group: "test-group".into(),
        mongo_uri: None,
        security: SecurityConfig::default(),
    };
    assert!(config.validate().is_ok());
}

#[test]
fn test_config_validation_empty_brokers() {
    let config = AppConfig {
        kafka_brokers: "".into(),
        server_host: "127.0.0.1".into(),
        server_port: 5688,
        default_warning_threshold: 1_048_576,
        default_critical_threshold: 10_485_760,
        consumer_group: "test-group".into(),
        mongo_uri: None,
        security: SecurityConfig::default(),
    };
    let err = config.validate().unwrap_err();
    assert!(err.contains("must not be empty"));
}

#[test]
fn test_config_validation_warning_gte_critical() {
    let config = AppConfig {
        kafka_brokers: "localhost:9092".into(),
        server_host: "127.0.0.1".into(),
        server_port: 5688,
        default_warning_threshold: 10_000_000,
        default_critical_threshold: 1_000_000,
        consumer_group: "test-group".into(),
        mongo_uri: None,
        security: SecurityConfig::default(),
    };
    let err = config.validate().unwrap_err();
    assert!(err.contains("must be less than"));
}

#[test]
fn test_config_validation_zero_warning() {
    let config = AppConfig {
        kafka_brokers: "localhost:9092".into(),
        server_host: "127.0.0.1".into(),
        server_port: 5688,
        default_warning_threshold: 0,
        default_critical_threshold: 10_000_000,
        consumer_group: "test-group".into(),
        mongo_uri: None,
        security: SecurityConfig::default(),
    };
    let err = config.validate().unwrap_err();
    assert!(err.contains("greater than 0"));
}

#[test]
fn test_config_validation_zero_port() {
    let config = AppConfig {
        kafka_brokers: "localhost:9092".into(),
        server_host: "127.0.0.1".into(),
        server_port: 0,
        default_warning_threshold: 1_000_000,
        default_critical_threshold: 10_000_000,
        consumer_group: "test-group".into(),
        mongo_uri: None,
        security: SecurityConfig::default(),
    };
    let err = config.validate().unwrap_err();
    assert!(err.contains("must not be 0"));
}

#[test]
fn test_config_validation_sasl_without_credentials() {
    let config = AppConfig {
        kafka_brokers: "localhost:9092".into(),
        server_host: "127.0.0.1".into(),
        server_port: 5688,
        default_warning_threshold: 1_000_000,
        default_critical_threshold: 10_000_000,
        consumer_group: "test-group".into(),
        mongo_uri: None,
        security: SecurityConfig {
            security_protocol: Some("SASL_SSL".into()),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
        },
    };
    let err = config.validate().unwrap_err();
    assert!(err.contains("sasl_mechanism is required"));
}

#[test]
fn test_config_validation_invalid_protocol() {
    let config = AppConfig {
        kafka_brokers: "localhost:9092".into(),
        server_host: "127.0.0.1".into(),
        server_port: 5688,
        default_warning_threshold: 1_000_000,
        default_critical_threshold: 10_000_000,
        consumer_group: "test-group".into(),
        mongo_uri: None,
        security: SecurityConfig {
            security_protocol: Some("INVALID".into()),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
        },
    };
    let err = config.validate().unwrap_err();
    assert!(err.contains("invalid security_protocol"));
}

#[test]
fn test_config_validation_valid_sasl() {
    let config = AppConfig {
        kafka_brokers: "confluent:9092".into(),
        server_host: "0.0.0.0".into(),
        server_port: 5688,
        default_warning_threshold: 1_000_000,
        default_critical_threshold: 10_000_000,
        consumer_group: "test-group".into(),
        mongo_uri: None,
        security: SecurityConfig {
            security_protocol: Some("SASL_SSL".into()),
            sasl_mechanism: Some("PLAIN".into()),
            sasl_username: Some("key".into()),
            sasl_password: Some("secret".into()),
        },
    };
    assert!(config.validate().is_ok());
}

// ═══════════════════════════════════════════════════════════════════
// Input Validation Helper Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_validate_brokers_valid() {
    assert!(kafka_message_size_tracker::config::validate_brokers("localhost:9092").is_ok());
    assert!(kafka_message_size_tracker::config::validate_brokers("host1:9092,host2:9092").is_ok());
}

#[test]
fn test_validate_brokers_empty() {
    assert!(kafka_message_size_tracker::config::validate_brokers("").is_err());
    assert!(kafka_message_size_tracker::config::validate_brokers("  ").is_err());
}

#[test]
fn test_validate_brokers_no_port() {
    let err = kafka_message_size_tracker::config::validate_brokers("localhost").unwrap_err();
    assert!(err.contains("host:port"));
}

#[test]
fn test_validate_topic_name_valid() {
    assert!(kafka_message_size_tracker::config::validate_topic_name("orders").is_ok());
    assert!(kafka_message_size_tracker::config::validate_topic_name("my-topic.v2").is_ok());
    assert!(kafka_message_size_tracker::config::validate_topic_name("test_topic_123").is_ok());
}

#[test]
fn test_validate_topic_name_empty() {
    assert!(kafka_message_size_tracker::config::validate_topic_name("").is_err());
}

#[test]
fn test_validate_topic_name_invalid_chars() {
    assert!(kafka_message_size_tracker::config::validate_topic_name("topic name").is_err());
    assert!(kafka_message_size_tracker::config::validate_topic_name("topic/path").is_err());
    assert!(kafka_message_size_tracker::config::validate_topic_name("topic@special").is_err());
}

#[test]
fn test_validate_thresholds_valid() {
    assert!(kafka_message_size_tracker::config::validate_thresholds(1000, 10000).is_ok());
}

#[test]
fn test_validate_thresholds_warning_gte_critical() {
    assert!(kafka_message_size_tracker::config::validate_thresholds(10000, 1000).is_err());
    assert!(kafka_message_size_tracker::config::validate_thresholds(1000, 1000).is_err());
}

#[test]
fn test_validate_thresholds_zero_warning() {
    assert!(kafka_message_size_tracker::config::validate_thresholds(0, 1000).is_err());
}

// ═══════════════════════════════════════════════════════════════════
// Full Message Flow Integration: Event → Stats + Alert + Progress
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_full_event_flow_stats_and_alert() {
    let stats = StatsEngine::new();
    let alerts = AlertEngine::new(1000, 10000);

    // Process an event that should trigger a warning
    let event = make_event("orders", 5000);
    stats.record_event(&event).await;
    let alert = alerts.check_event(&event).await;

    // Verify stats were recorded
    let topic_stats = stats.get_stats("orders").await.unwrap();
    assert_eq!(topic_stats.message_count, 1);
    assert_eq!(topic_stats.avg_bytes, 5000.0);
    assert_eq!(topic_stats.min_bytes, 5000);
    assert_eq!(topic_stats.max_bytes, 5000);

    // Verify alert was triggered
    assert!(alert.is_some());
    let alert = alert.unwrap();
    assert_eq!(alert.severity, AlertSeverity::Warning);
    assert_eq!(alert.topic, "orders");
    assert_eq!(alert.message_size, 5000);
}

#[tokio::test]
async fn test_full_event_flow_no_alert_under_threshold() {
    let stats = StatsEngine::new();
    let alerts = AlertEngine::new(1_000_000, 10_000_000);

    let event = make_event("logs", 500);
    stats.record_event(&event).await;
    let alert = alerts.check_event(&event).await;

    // Stats recorded, no alert
    let topic_stats = stats.get_stats("logs").await.unwrap();
    assert_eq!(topic_stats.message_count, 1);
    assert!(alert.is_none());
}

#[tokio::test]
async fn test_full_event_flow_critical_with_capture() {
    let alerts = AlertEngine::new(1000, 10000);

    // Simulate a critical-sized event with captured payload
    let event = MessageSizeEvent {
        topic: "metrics".to_string(),
        partition: 0,
        offset: 42,
        size_bytes: 15000,
        key_size: Some(4),
        timestamp: Utc::now(),
        captured_payload: Some(vec![0xDE, 0xAD]),
        captured_key: Some(vec![0x01, 0x02]),
    };

    let alert = alerts.check_event(&event).await.unwrap();
    assert_eq!(alert.severity, AlertSeverity::Critical);

    // Store the capture (like the event processor does)
    let captured = CapturedMessage {
        alert_id: alert.id,
        topic: event.topic.clone(),
        partition: event.partition,
        offset: event.offset,
        payload: event.captured_payload.unwrap(),
        key: event.captured_key,
        size_bytes: event.size_bytes,
        timestamp: event.timestamp,
    };
    alerts.store_capture(alert.id, captured).await;

    // Verify we can retrieve the capture
    let retrieved = alerts.get_capture(alert.id).await.unwrap();
    assert_eq!(retrieved.payload, vec![0xDE, 0xAD]);
    assert_eq!(retrieved.offset, 42);
}

#[tokio::test]
async fn test_multi_topic_stats_independence() {
    let stats = StatsEngine::new();

    // Record events for different topics
    for i in 0..10 {
        stats.record_event(&make_event("small-topic", 100 + i)).await;
        stats.record_event(&make_event("large-topic", 10000 + i)).await;
    }

    let small = stats.get_stats("small-topic").await.unwrap();
    let large = stats.get_stats("large-topic").await.unwrap();

    assert_eq!(small.message_count, 10);
    assert_eq!(large.message_count, 10);
    assert!(small.avg_bytes < 200.0);
    assert!(large.avg_bytes > 9000.0);
}

// ═══════════════════════════════════════════════════════════════════
// Consumer Progress Tracking Integration
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_progress_tracker_multi_partition_flow() {
    let tracker = ProgressTracker::new();

    // Simulate consuming from 3 partitions
    tracker.record_offset("orders", 0, 100).await;
    tracker.record_offset("orders", 1, 200).await;
    tracker.record_offset("orders", 2, 50).await;

    // Simulate watermark data (log end offsets)
    let watermarks = vec![
        kafka_message_size_tracker::models::PartitionLag {
            topic: "orders".to_string(),
            partition: 0,
            current_offset: 100,
            log_end_offset: 1000,
            lag: 900,
        },
        kafka_message_size_tracker::models::PartitionLag {
            topic: "orders".to_string(),
            partition: 1,
            current_offset: 200,
            log_end_offset: 500,
            lag: 300,
        },
        kafka_message_size_tracker::models::PartitionLag {
            topic: "orders".to_string(),
            partition: 2,
            current_offset: 50,
            log_end_offset: 50,
            lag: 0,
        },
    ];

    // Snapshot with watermarks
    tracker.snapshot(&watermarks).await;

    // Verify cached watermarks
    let cached = tracker.get_cached_watermarks().await;
    assert_eq!(cached.len(), 3);
    let total_lag: i64 = cached.iter().map(|w| w.lag).sum();
    assert_eq!(total_lag, 1200);
}

#[tokio::test]
async fn test_progress_tracker_clear_resets_everything() {
    let tracker = ProgressTracker::new();

    tracker.record_offset("topic1", 0, 100).await;
    tracker.record_offset("topic2", 0, 200).await;

    let watermarks = vec![
        kafka_message_size_tracker::models::PartitionLag {
            topic: "topic1".to_string(),
            partition: 0,
            current_offset: 100,
            log_end_offset: 500,
            lag: 400,
        },
    ];
    tracker.snapshot(&watermarks).await;

    tracker.clear().await;

    let cached = tracker.get_cached_watermarks().await;
    assert!(cached.is_empty());
}

// ═══════════════════════════════════════════════════════════════════
// Time-Series Storage Integration
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_storage_snapshot_and_query_flow() {
    let storage = TimeSeriesStorage::new();
    let stats = StatsEngine::new();

    // Generate some events
    for i in 0..100 {
        stats.record_event(&make_event("orders", 1000 + i * 10)).await;
    }

    // Snapshot (like minute aggregator does)
    let snapshots = stats.snapshot_and_reset_minute().await;
    for (topic, point) in snapshots {
        storage.append(&topic, point).await;
    }

    // Query history
    let history = storage.query("orders", 60).await;
    assert_eq!(history.len(), 1); // One minute bucket
    assert_eq!(history[0].message_count, 100);
    assert!(history[0].avg_bytes > 1000.0);
}

#[tokio::test]
async fn test_storage_multi_topic_isolation() {
    let storage = TimeSeriesStorage::new();

    let point_a = HistoryDataPoint {
        timestamp: Utc::now(),
        avg_bytes: 500.0,
        min_bytes: 100,
        max_bytes: 1000,
        message_count: 50,
        total_bytes: 25000,
        p95: 900,
    };
    let point_b = HistoryDataPoint {
        timestamp: Utc::now(),
        avg_bytes: 5000.0,
        min_bytes: 1000,
        max_bytes: 10000,
        message_count: 10,
        total_bytes: 50000,
        p95: 9000,
    };

    storage.append("small", point_a).await;
    storage.append("large", point_b).await;

    let small = storage.query("small", 60).await;
    let large = storage.query("large", 60).await;

    assert_eq!(small.len(), 1);
    assert_eq!(large.len(), 1);
    assert_eq!(small[0].message_count, 50);
    assert_eq!(large[0].message_count, 10);
}

// ═══════════════════════════════════════════════════════════════════
// Alert Engine Runtime Reconfiguration
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_alert_runtime_threshold_change_flow() {
    let engine = AlertEngine::new(1_000_000, 10_000_000);

    // 5KB message doesn't trigger with default 1MB threshold
    let event = make_event("orders", 5000);
    assert!(engine.check_event(&event).await.is_none());

    // Lower per-topic threshold at runtime
    engine
        .set_config(AlertConfig {
            topic: "orders".into(),
            warning_threshold: 1000,
            critical_threshold: 50000,
            enabled: true,
        })
        .await;

    // Now the same 5KB message triggers a warning
    let alert = engine.check_event(&event).await.unwrap();
    assert_eq!(alert.severity, AlertSeverity::Warning);

    // Disable the topic
    engine
        .set_config(AlertConfig {
            topic: "orders".into(),
            warning_threshold: 1000,
            critical_threshold: 50000,
            enabled: false,
        })
        .await;

    // Same event, no alert (disabled)
    assert!(engine.check_event(&event).await.is_none());
}

#[tokio::test]
async fn test_alert_global_defaults_affect_new_topics() {
    let engine = AlertEngine::new(1_000_000, 10_000_000);

    // Change global defaults
    engine.set_defaults(500, 5000).await;

    // New topic uses updated defaults
    let alert = engine.check_event(&make_event("new-topic", 1000)).await;
    assert!(alert.is_some());
    assert_eq!(alert.unwrap().severity, AlertSeverity::Warning);

    // Per-topic config still overrides
    engine
        .set_config(AlertConfig {
            topic: "new-topic".into(),
            warning_threshold: 2000,
            critical_threshold: 20000,
            enabled: true,
        })
        .await;

    // 1000 bytes no longer triggers (per-topic threshold is 2000)
    assert!(engine.check_event(&make_event("new-topic", 1000)).await.is_none());
}

// ═══════════════════════════════════════════════════════════════════
// End-to-End Stats + Alerts + Storage Pipeline
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_end_to_end_pipeline() {
    let stats = StatsEngine::new();
    let alerts = AlertEngine::new(1000, 10000);
    let storage = TimeSeriesStorage::new();
    let progress = ProgressTracker::new();

    let mut alert_count = 0;

    // Simulate 200 messages across 2 topics
    for i in 0..200 {
        let topic = if i % 2 == 0 { "orders" } else { "users" };
        let size = if i % 50 == 0 { 15000 } else { 500 }; // Every 50th is oversized
        let partition = (i % 3) as i32;

        let event = make_event_with_offset(topic, size, partition, i);

        // Stats
        stats.record_event(&event).await;

        // Progress
        progress.record_offset(&event.topic, event.partition, event.offset).await;

        // Alerts
        if let Some(_alert) = alerts.check_event(&event).await {
            alert_count += 1;
        }
    }

    // Verify stats
    let orders = stats.get_stats("orders").await.unwrap();
    let users = stats.get_stats("users").await.unwrap();
    assert_eq!(orders.message_count, 100);
    assert_eq!(users.message_count, 100);

    // Verify alerts (oversized messages at i=0,50,100,150)
    // i=0 -> orders (15000), i=50 -> orders (15000), i=100 -> orders (15000), i=150 -> orders (15000)
    assert_eq!(alert_count, 4);

    let recent_alerts = alerts.get_alerts(10).await;
    assert_eq!(recent_alerts.len(), 4);

    // Snapshot stats to storage
    let snapshots = stats.snapshot_and_reset_minute().await;
    for (topic, point) in snapshots {
        storage.append(&topic, point).await;
    }

    // Verify storage has data
    let orders_history = storage.query("orders", 60).await;
    let users_history = storage.query("users", 60).await;
    assert_eq!(orders_history.len(), 1);
    assert_eq!(users_history.len(), 1);

    // Clear everything
    stats.clear().await;
    alerts.clear_alerts().await;
    progress.clear().await;
    storage.clear().await;

    assert!(stats.get_stats("orders").await.is_none());
    assert!(alerts.get_alerts(10).await.is_empty());
}

// ═══════════════════════════════════════════════════════════════════
// MongoDB Model Serialization Integration
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_persisted_message_roundtrip() {
    let msg = kafka_message_size_tracker::mongo::PersistedMessage {
        topic: "orders".to_string(),
        partition: 0,
        offset: 42,
        timestamp: Utc::now(),
        size_bytes: 1500,
        key: Some("order-key".to_string()),
        value: Some("base64data==".to_string()),
    };

    let json = serde_json::to_string(&msg).unwrap();
    let decoded: kafka_message_size_tracker::mongo::PersistedMessage =
        serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.topic, "orders");
    assert_eq!(decoded.offset, 42);
    assert_eq!(decoded.size_bytes, 1500);
    assert_eq!(decoded.key, Some("order-key".to_string()));
}

#[test]
fn test_persisted_alert_roundtrip() {
    let alert = kafka_message_size_tracker::mongo::PersistedAlert {
        alert_id: 1,
        topic: "users".to_string(),
        severity: "warning".to_string(),
        message_size: 5000,
        threshold: 1000,
        timestamp: Utc::now(),
    };

    let json = serde_json::to_string(&alert).unwrap();
    let decoded: kafka_message_size_tracker::mongo::PersistedAlert =
        serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.alert_id, 1);
    assert_eq!(decoded.severity, "warning");
    assert_eq!(decoded.threshold, 1000);
}
