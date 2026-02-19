use chrono::{DateTime, Utc};
use mongodb::bson::doc;
use mongodb::options::{ClientOptions, IndexOptions};
use mongodb::{Client, Collection, IndexModel};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const DB_NAME: &str = "kafka_size_tracker";
const MESSAGES_COLLECTION: &str = "oversized_messages";
const ALERTS_COLLECTION: &str = "alerts";

/// Persisted oversized message — minimal format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: DateTime<Utc>,
    pub size_bytes: usize,
    /// Key as base64 (if present).
    pub key: Option<String>,
    /// Value as base64.
    pub value: Option<String>,
}

/// Persisted alert record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedAlert {
    pub alert_id: u64,
    pub topic: String,
    pub severity: String,
    pub message_size: usize,
    pub threshold: usize,
    pub timestamp: DateTime<Utc>,
}

/// MongoDB persistent store for oversized messages and alerts.
#[derive(Clone)]
pub struct MongoStore {
    messages: Collection<PersistedMessage>,
    alerts: Collection<PersistedAlert>,
}

impl MongoStore {
    /// Create a new MongoStore, connecting to the given URI.
    /// Creates indexes on (topic + timestamp) for both collections.
    pub async fn new(uri: &str) -> Result<Self, mongodb::error::Error> {
        let opts = ClientOptions::parse(uri).await?;
        let client = Client::with_options(opts)?;
        let db = client.database(DB_NAME);

        let messages: Collection<PersistedMessage> = db.collection(MESSAGES_COLLECTION);
        let alerts: Collection<PersistedAlert> = db.collection(ALERTS_COLLECTION);

        // Create indexes
        let idx = IndexModel::builder()
            .keys(doc! { "topic": 1, "timestamp": -1 })
            .options(IndexOptions::builder().build())
            .build();

        messages.create_index(idx.clone()).await?;
        alerts.create_index(idx).await?;

        tracing::info!("MongoDB connected: {}", uri);

        Ok(Self { messages, alerts })
    }

    /// Persist an oversized message.
    pub async fn persist_message(&self, msg: PersistedMessage) -> Result<(), mongodb::error::Error> {
        self.messages.insert_one(msg).await?;
        Ok(())
    }

    /// Persist an alert.
    pub async fn persist_alert(&self, alert: PersistedAlert) -> Result<(), mongodb::error::Error> {
        self.alerts.insert_one(alert).await?;
        Ok(())
    }

    /// Query persisted oversized messages.
    pub async fn query_messages(
        &self,
        topic: Option<&str>,
        limit: i64,
    ) -> Result<Vec<PersistedMessage>, mongodb::error::Error> {
        use futures_util::TryStreamExt;
        use mongodb::options::FindOptions;

        let filter = match topic {
            Some(t) => doc! { "topic": t },
            None => doc! {},
        };

        let opts = FindOptions::builder()
            .sort(doc! { "timestamp": -1 })
            .limit(limit)
            .build();

        let cursor = self.messages.find(filter).with_options(opts).await?;
        let results: Vec<PersistedMessage> = cursor.try_collect().await?;
        Ok(results)
    }

    /// Query persisted alerts.
    pub async fn query_alerts(
        &self,
        topic: Option<&str>,
        limit: i64,
    ) -> Result<Vec<PersistedAlert>, mongodb::error::Error> {
        use futures_util::TryStreamExt;
        use mongodb::options::FindOptions;

        let filter = match topic {
            Some(t) => doc! { "topic": t },
            None => doc! {},
        };

        let opts = FindOptions::builder()
            .sort(doc! { "timestamp": -1 })
            .limit(limit)
            .build();

        let cursor = self.alerts.find(filter).with_options(opts).await?;
        let results: Vec<PersistedAlert> = cursor.try_collect().await?;
        Ok(results)
    }
}

/// Wrap in Arc for sharing across handlers.
pub type SharedMongoStore = Arc<Option<MongoStore>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_persisted_message_serialization() {
        let msg = PersistedMessage {
            topic: "orders".into(),
            partition: 0,
            offset: 42,
            timestamp: Utc::now(),
            size_bytes: 1500000,
            key: Some("b3JkZXItMTIz".into()),
            value: Some("eyJpZCI6MTIzfQ==".into()),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""topic":"orders""#));
        assert!(json.contains(r#""size_bytes":1500000"#));
    }

    #[test]
    fn test_persisted_alert_serialization() {
        let alert = PersistedAlert {
            alert_id: 1,
            topic: "logs".into(),
            severity: "critical".into(),
            message_size: 15000000,
            threshold: 10000000,
            timestamp: Utc::now(),
        };
        let json = serde_json::to_string(&alert).unwrap();
        assert!(json.contains(r#""severity":"critical""#));
        assert!(json.contains(r#""alert_id":1"#));
    }

    #[test]
    fn test_persisted_message_deserialization() {
        let json = r#"{"topic":"users","partition":1,"offset":100,"timestamp":"2026-02-17T10:00:00Z","size_bytes":2000000,"key":null,"value":"dGVzdA=="}"#;
        let msg: PersistedMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.topic, "users");
        assert_eq!(msg.partition, 1);
        assert_eq!(msg.size_bytes, 2000000);
        assert!(msg.key.is_none());
        assert_eq!(msg.value, Some("dGVzdA==".into()));
    }

    #[test]
    fn test_persisted_alert_deserialization() {
        let json = r#"{"alert_id":5,"topic":"metrics","severity":"warning","message_size":1200000,"threshold":1000000,"timestamp":"2026-02-17T10:00:00Z"}"#;
        let alert: PersistedAlert = serde_json::from_str(json).unwrap();
        assert_eq!(alert.alert_id, 5);
        assert_eq!(alert.severity, "warning");
        assert_eq!(alert.message_size, 1200000);
    }
}
