use clap::Parser;
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Shared security config that can be updated at runtime (e.g., via the web UI).
pub type SharedSecurityConfig = Arc<RwLock<SecurityConfig>>;

#[derive(Parser, Debug)]
#[command(
    name = "kafka-size-tracker",
    about = "Real-time Kafka message size tracking dashboard"
)]
pub struct Args {
    /// Kafka broker addresses (comma-separated)
    #[arg(long, default_value = "localhost:9092")]
    pub brokers: String,

    /// Server port
    #[arg(long, default_value = "5688")]
    pub port: u16,

    /// Server host
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Default warning alert threshold in bytes (1MB)
    #[arg(long, default_value = "1048576")]
    pub threshold: usize,

    /// Critical alert threshold in bytes (10MB)
    #[arg(long, default_value = "10485760")]
    pub critical_threshold: usize,

    /// MongoDB URI for persistent storage (optional, e.g. mongodb://localhost:27017)
    #[arg(long)]
    pub mongo_uri: Option<String>,

    /// Kafka security protocol (e.g. SASL_SSL, SASL_PLAINTEXT, SSL)
    #[arg(long)]
    pub security_protocol: Option<String>,

    /// SASL mechanism (e.g. PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
    #[arg(long)]
    pub sasl_mechanism: Option<String>,

    /// SASL username / API Key
    #[arg(long)]
    pub sasl_username: Option<String>,

    /// SASL password / API Secret
    #[arg(long)]
    pub sasl_password: Option<String>,
}

/// Kafka security/authentication configuration for SASL, SSL, etc.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub security_protocol: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
}

impl SecurityConfig {
    /// Returns true if any security setting is configured.
    pub fn is_configured(&self) -> bool {
        self.security_protocol.is_some()
            || self.sasl_mechanism.is_some()
            || self.sasl_username.is_some()
            || self.sasl_password.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub kafka_brokers: String,
    pub server_host: String,
    pub server_port: u16,
    pub default_warning_threshold: usize,
    pub default_critical_threshold: usize,
    pub consumer_group: String,
    pub mongo_uri: Option<String>,
    pub security: SecurityConfig,
}

impl AppConfig {
    pub fn from_args_and_env(args: Args) -> Self {
        let security = SecurityConfig {
            security_protocol: env::var("KAFKA_SECURITY_PROTOCOL")
                .ok()
                .or(args.security_protocol),
            sasl_mechanism: env::var("KAFKA_SASL_MECHANISM")
                .ok()
                .or(args.sasl_mechanism),
            sasl_username: env::var("KAFKA_SASL_USERNAME")
                .ok()
                .or(args.sasl_username),
            sasl_password: env::var("KAFKA_SASL_PASSWORD")
                .ok()
                .or(args.sasl_password),
        };

        Self {
            kafka_brokers: env::var("KAFKA_BROKERS").unwrap_or(args.brokers),
            server_host: env::var("SERVER_HOST").unwrap_or(args.host),
            server_port: env::var("SERVER_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(args.port),
            default_warning_threshold: env::var("ALERT_THRESHOLD_BYTES")
                .ok()
                .and_then(|t| t.parse().ok())
                .unwrap_or(args.threshold),
            default_critical_threshold: env::var("CRITICAL_THRESHOLD_BYTES")
                .ok()
                .and_then(|t| t.parse().ok())
                .unwrap_or(args.critical_threshold),
            consumer_group: env::var("CONSUMER_GROUP")
                .unwrap_or_else(|_| "kafka-size-tracker".into()),
            mongo_uri: env::var("MONGO_URI").ok().or(args.mongo_uri),
            security,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_values() {
        let args = Args {
            brokers: "localhost:9092".into(),
            port: 5688,
            host: "127.0.0.1".into(),
            threshold: 1048576,
            critical_threshold: 10485760,
            mongo_uri: None,
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
        };
        let config = AppConfig::from_args_and_env(args);
        assert_eq!(config.kafka_brokers, "localhost:9092");
        assert_eq!(config.server_port, 5688);
        assert_eq!(config.server_host, "127.0.0.1");
        assert_eq!(config.default_warning_threshold, 1048576);
        assert_eq!(config.default_critical_threshold, 10485760);
        assert_eq!(config.consumer_group, "kafka-size-tracker");
        assert!(config.mongo_uri.is_none());
        assert!(!config.security.is_configured());
    }

    #[test]
    fn test_args_override() {
        let args = Args {
            brokers: "kafka:9094".into(),
            port: 8080,
            host: "0.0.0.0".into(),
            threshold: 512000,
            critical_threshold: 5000000,
            mongo_uri: Some("mongodb://localhost:27017".into()),
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
        };
        let config = AppConfig::from_args_and_env(args);
        assert_eq!(config.kafka_brokers, "kafka:9094");
        assert_eq!(config.server_port, 8080);
        assert_eq!(config.default_warning_threshold, 512000);
        assert_eq!(
            config.mongo_uri,
            Some("mongodb://localhost:27017".into())
        );
    }

    #[test]
    fn test_security_config_not_configured() {
        let sec = SecurityConfig::default();
        assert!(!sec.is_configured());
    }

    #[test]
    fn test_security_config_configured() {
        let sec = SecurityConfig {
            security_protocol: Some("SASL_SSL".into()),
            sasl_mechanism: Some("PLAIN".into()),
            sasl_username: Some("my-api-key".into()),
            sasl_password: Some("my-api-secret".into()),
        };
        assert!(sec.is_configured());
    }

    #[test]
    fn test_security_config_from_args() {
        let args = Args {
            brokers: "confluent:9092".into(),
            port: 5688,
            host: "127.0.0.1".into(),
            threshold: 1048576,
            critical_threshold: 10485760,
            mongo_uri: None,
            security_protocol: Some("SASL_SSL".into()),
            sasl_mechanism: Some("PLAIN".into()),
            sasl_username: Some("key".into()),
            sasl_password: Some("secret".into()),
        };
        let config = AppConfig::from_args_and_env(args);
        assert!(config.security.is_configured());
        assert_eq!(config.security.security_protocol.as_deref(), Some("SASL_SSL"));
        assert_eq!(config.security.sasl_mechanism.as_deref(), Some("PLAIN"));
        assert_eq!(config.security.sasl_username.as_deref(), Some("key"));
        assert_eq!(config.security.sasl_password.as_deref(), Some("secret"));
    }

    #[test]
    fn test_security_config_serde_roundtrip() {
        let sec = SecurityConfig {
            security_protocol: Some("SASL_SSL".into()),
            sasl_mechanism: Some("PLAIN".into()),
            sasl_username: Some("user".into()),
            sasl_password: Some("pass".into()),
        };
        let json = serde_json::to_string(&sec).unwrap();
        let decoded: SecurityConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.security_protocol, sec.security_protocol);
        assert_eq!(decoded.sasl_mechanism, sec.sasl_mechanism);
        assert_eq!(decoded.sasl_username, sec.sasl_username);
        assert_eq!(decoded.sasl_password, sec.sasl_password);
    }
}
