use crate::models::{Alert, AlertConfig, AlertSeverity, CapturedMessage, MessageSizeEvent};
use chrono::Utc;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

const MAX_ALERTS: usize = 1000;
const MAX_CAPTURES: usize = 100;

/// Alert engine that checks message sizes against configurable thresholds.
/// All thresholds (per-topic and global defaults) are runtime-mutable.
#[derive(Clone)]
pub struct AlertEngine {
    configs: Arc<RwLock<HashMap<String, AlertConfig>>>,
    alerts: Arc<RwLock<VecDeque<Alert>>>,
    next_id: Arc<RwLock<u64>>,
    captures: Arc<RwLock<HashMap<u64, CapturedMessage>>>,
    /// Ordered list of capture IDs for eviction (oldest first).
    capture_order: Arc<RwLock<VecDeque<u64>>>,
    default_warning: Arc<RwLock<usize>>,
    default_critical: Arc<RwLock<usize>>,
}

impl AlertEngine {
    pub fn new(default_warning: usize, default_critical: usize) -> Self {
        Self {
            configs: Arc::new(RwLock::new(HashMap::new())),
            alerts: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_ALERTS))),
            next_id: Arc::new(RwLock::new(1)),
            captures: Arc::new(RwLock::new(HashMap::new())),
            capture_order: Arc::new(RwLock::new(VecDeque::new())),
            default_warning: Arc::new(RwLock::new(default_warning)),
            default_critical: Arc::new(RwLock::new(default_critical)),
        }
    }

    /// Check if a message size exceeds thresholds. Returns an Alert if triggered.
    pub async fn check_event(&self, event: &MessageSizeEvent) -> Option<Alert> {
        let configs = self.configs.read().await;
        let config = configs.get(&event.topic);

        let dw = *self.default_warning.read().await;
        let dc = *self.default_critical.read().await;

        let (warning_threshold, critical_threshold, enabled) = match config {
            Some(c) => (c.warning_threshold, c.critical_threshold, c.enabled),
            None => (dw, dc, true),
        };

        if !enabled {
            return None;
        }

        let severity = if event.size_bytes >= critical_threshold {
            Some(AlertSeverity::Critical)
        } else if event.size_bytes >= warning_threshold {
            Some(AlertSeverity::Warning)
        } else {
            None
        };

        if let Some(severity) = severity {
            let threshold = match &severity {
                AlertSeverity::Critical => critical_threshold,
                AlertSeverity::Warning => warning_threshold,
            };

            let mut id = self.next_id.write().await;
            let alert = Alert {
                id: *id,
                topic: event.topic.clone(),
                severity,
                message_size: event.size_bytes,
                threshold,
                timestamp: Utc::now(),
                capture_id: None, // Set by caller if payload was captured
            };
            *id += 1;
            drop(id);

            let mut alerts = self.alerts.write().await;
            if alerts.len() >= MAX_ALERTS {
                alerts.pop_front();
            }
            alerts.push_back(alert.clone());

            Some(alert)
        } else {
            None
        }
    }

    /// Get the most recent alerts (up to limit).
    pub async fn get_alerts(&self, limit: usize) -> Vec<Alert> {
        let alerts = self.alerts.read().await;
        alerts.iter().rev().take(limit).cloned().collect()
    }

    /// Get alert config for a specific topic (returns defaults if not configured).
    pub async fn get_config(&self, topic: &str) -> AlertConfig {
        let configs = self.configs.read().await;
        let dw = *self.default_warning.read().await;
        let dc = *self.default_critical.read().await;
        configs.get(topic).cloned().unwrap_or(AlertConfig {
            topic: topic.to_string(),
            warning_threshold: dw,
            critical_threshold: dc,
            enabled: true,
        })
    }

    /// Set alert config for a topic.
    pub async fn set_config(&self, config: AlertConfig) {
        let mut configs = self.configs.write().await;
        configs.insert(config.topic.clone(), config);
    }

    /// Get all configured alert thresholds.
    pub async fn get_all_configs(&self) -> Vec<AlertConfig> {
        let configs = self.configs.read().await;
        configs.values().cloned().collect()
    }

    /// Get the current capture threshold for a topic.
    /// Returns the per-topic warning threshold if configured, otherwise the global default.
    /// Used by the Kafka consumer to decide whether to capture raw message bytes.
    pub async fn get_capture_threshold(&self, topic: &str) -> usize {
        let configs = self.configs.read().await;
        match configs.get(topic) {
            Some(c) if c.enabled => c.warning_threshold,
            _ => *self.default_warning.read().await,
        }
    }

    /// Get the current global default thresholds.
    pub async fn get_defaults(&self) -> (usize, usize) {
        let dw = *self.default_warning.read().await;
        let dc = *self.default_critical.read().await;
        (dw, dc)
    }

    /// Update the global default thresholds at runtime.
    pub async fn set_defaults(&self, warning: usize, critical: usize) {
        *self.default_warning.write().await = warning;
        *self.default_critical.write().await = critical;
        log::info!(
            "Global alert defaults updated: warning={}B, critical={}B",
            warning,
            critical
        );
    }

    /// Store a captured oversized message payload linked to an alert.
    /// Evicts the oldest capture when the store exceeds MAX_CAPTURES.
    pub async fn store_capture(&self, alert_id: u64, msg: CapturedMessage) {
        let mut captures = self.captures.write().await;
        let mut order = self.capture_order.write().await;

        // Evict oldest if at capacity
        while order.len() >= MAX_CAPTURES {
            if let Some(old_id) = order.pop_front() {
                captures.remove(&old_id);
            }
        }

        captures.insert(alert_id, msg);
        order.push_back(alert_id);
    }

    /// Retrieve a captured message by alert ID.
    pub async fn get_capture(&self, alert_id: u64) -> Option<CapturedMessage> {
        let captures = self.captures.read().await;
        captures.get(&alert_id).cloned()
    }

    /// Clear all alerts and captures (e.g., on broker change).
    pub async fn clear_alerts(&self) {
        let mut alerts = self.alerts.write().await;
        alerts.clear();
        let mut captures = self.captures.write().await;
        captures.clear();
        let mut order = self.capture_order.write().await;
        order.clear();
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
            captured_payload: None,
            captured_key: None,
        }
    }

    #[tokio::test]
    async fn test_no_alert_under_threshold() {
        let engine = AlertEngine::new(1_000_000, 10_000_000);
        let result = engine.check_event(&make_event("test", 500)).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_warning_alert_triggered() {
        let engine = AlertEngine::new(1000, 10000);
        let result = engine.check_event(&make_event("test", 5000)).await;
        assert!(result.is_some());
        let alert = result.unwrap();
        assert_eq!(alert.severity, AlertSeverity::Warning);
        assert_eq!(alert.message_size, 5000);
        assert_eq!(alert.threshold, 1000);
    }

    #[tokio::test]
    async fn test_critical_alert_triggered() {
        let engine = AlertEngine::new(1000, 10000);
        let result = engine.check_event(&make_event("test", 15000)).await;
        assert!(result.is_some());
        let alert = result.unwrap();
        assert_eq!(alert.severity, AlertSeverity::Critical);
        assert_eq!(alert.threshold, 10000);
    }

    #[tokio::test]
    async fn test_alert_circular_buffer_eviction() {
        let engine = AlertEngine::new(100, 1000);
        // Fill beyond MAX_ALERTS
        for _ in 0..MAX_ALERTS + 50 {
            engine.check_event(&make_event("test", 500)).await;
        }
        let alerts = engine.get_alerts(MAX_ALERTS + 100).await;
        assert_eq!(alerts.len(), MAX_ALERTS);
    }

    #[tokio::test]
    async fn test_custom_threshold_per_topic() {
        let engine = AlertEngine::new(1_000_000, 10_000_000);
        engine
            .set_config(AlertConfig {
                topic: "small-topic".into(),
                warning_threshold: 100,
                critical_threshold: 500,
                enabled: true,
            })
            .await;

        // 200 bytes exceeds custom warning (100) but not default (1MB)
        let result = engine.check_event(&make_event("small-topic", 200)).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().severity, AlertSeverity::Warning);

        // Same size on unconfigured topic — no alert
        let result2 = engine.check_event(&make_event("other-topic", 200)).await;
        assert!(result2.is_none());
    }

    #[tokio::test]
    async fn test_disabled_topic_no_alerts() {
        let engine = AlertEngine::new(100, 1000);
        engine
            .set_config(AlertConfig {
                topic: "disabled".into(),
                warning_threshold: 100,
                critical_threshold: 1000,
                enabled: false,
            })
            .await;

        let result = engine.check_event(&make_event("disabled", 5000)).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_default_thresholds_used() {
        let engine = AlertEngine::new(500, 5000);
        let config = engine.get_config("unconfigured").await;
        assert_eq!(config.warning_threshold, 500);
        assert_eq!(config.critical_threshold, 5000);
        assert!(config.enabled);
    }

    #[tokio::test]
    async fn test_alert_ids_increment() {
        let engine = AlertEngine::new(100, 1000);
        let a1 = engine.check_event(&make_event("test", 200)).await.unwrap();
        let a2 = engine.check_event(&make_event("test", 300)).await.unwrap();
        assert_eq!(a1.id, 1);
        assert_eq!(a2.id, 2);
    }

    #[tokio::test]
    async fn test_capture_stored_on_alert() {
        let engine = AlertEngine::new(100, 1000);
        let captured = CapturedMessage {
            alert_id: 1,
            topic: "test".into(),
            partition: 0,
            offset: 42,
            payload: vec![0xDE, 0xAD, 0xBE, 0xEF],
            key: None,
            size_bytes: 4,
            timestamp: Utc::now(),
        };
        engine.store_capture(1, captured).await;
        let retrieved = engine.get_capture(1).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().payload, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[tokio::test]
    async fn test_capture_eviction() {
        let engine = AlertEngine::new(100, 1000);
        // Store MAX_CAPTURES + 10 captures
        for i in 0..(super::MAX_CAPTURES + 10) as u64 {
            let captured = CapturedMessage {
                alert_id: i,
                topic: "test".into(),
                partition: 0,
                offset: i as i64,
                payload: vec![i as u8],
                key: None,
                size_bytes: 1,
                timestamp: Utc::now(),
            };
            engine.store_capture(i, captured).await;
        }
        // Oldest 10 should be evicted
        assert!(engine.get_capture(0).await.is_none());
        assert!(engine.get_capture(9).await.is_none());
        // Newest should still be there
        assert!(engine.get_capture(super::MAX_CAPTURES as u64 + 9).await.is_some());
    }

    #[tokio::test]
    async fn test_get_capture_returns_none_for_missing() {
        let engine = AlertEngine::new(100, 1000);
        assert!(engine.get_capture(999).await.is_none());
    }

    #[tokio::test]
    async fn test_set_defaults_changes_runtime_thresholds() {
        let engine = AlertEngine::new(1_000_000, 10_000_000);

        // 5000 bytes doesn't trigger with 1MB default
        let result = engine.check_event(&make_event("test", 5000)).await;
        assert!(result.is_none());

        // Lower defaults at runtime
        engine.set_defaults(1000, 50000).await;

        // Now 5000 bytes triggers a warning
        let result = engine.check_event(&make_event("test", 5000)).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().severity, AlertSeverity::Warning);

        // Verify get_defaults returns new values
        let (dw, dc) = engine.get_defaults().await;
        assert_eq!(dw, 1000);
        assert_eq!(dc, 50000);
    }

    #[tokio::test]
    async fn test_get_capture_threshold_uses_per_topic_config() {
        let engine = AlertEngine::new(1_000_000, 10_000_000);

        // No per-topic config → returns default warning
        assert_eq!(engine.get_capture_threshold("orders").await, 1_000_000);

        // Set per-topic config
        engine
            .set_config(AlertConfig {
                topic: "orders".into(),
                warning_threshold: 500,
                critical_threshold: 5000,
                enabled: true,
            })
            .await;

        // Now returns per-topic warning
        assert_eq!(engine.get_capture_threshold("orders").await, 500);

        // Other topic still returns default
        assert_eq!(engine.get_capture_threshold("users").await, 1_000_000);
    }

    #[tokio::test]
    async fn test_get_capture_threshold_after_set_defaults() {
        let engine = AlertEngine::new(1_000_000, 10_000_000);
        assert_eq!(engine.get_capture_threshold("test").await, 1_000_000);

        // Change defaults
        engine.set_defaults(2000, 20000).await;
        assert_eq!(engine.get_capture_threshold("test").await, 2000);
    }
}
