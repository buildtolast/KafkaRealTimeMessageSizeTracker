use clap::Parser;
use std::env;

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
    #[arg(long, default_value = "3000")]
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
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub kafka_brokers: String,
    pub server_host: String,
    pub server_port: u16,
    pub default_warning_threshold: usize,
    pub default_critical_threshold: usize,
    pub consumer_group: String,
}

impl AppConfig {
    pub fn from_args_and_env(args: Args) -> Self {
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
            port: 3000,
            host: "127.0.0.1".into(),
            threshold: 1048576,
            critical_threshold: 10485760,
        };
        let config = AppConfig::from_args_and_env(args);
        assert_eq!(config.kafka_brokers, "localhost:9092");
        assert_eq!(config.server_port, 3000);
        assert_eq!(config.server_host, "127.0.0.1");
        assert_eq!(config.default_warning_threshold, 1048576);
        assert_eq!(config.default_critical_threshold, 10485760);
        assert_eq!(config.consumer_group, "kafka-size-tracker");
    }

    #[test]
    fn test_args_override() {
        let args = Args {
            brokers: "kafka:9094".into(),
            port: 8080,
            host: "0.0.0.0".into(),
            threshold: 512000,
            critical_threshold: 5000000,
        };
        let config = AppConfig::from_args_and_env(args);
        assert_eq!(config.kafka_brokers, "kafka:9094");
        assert_eq!(config.server_port, 8080);
        assert_eq!(config.default_warning_threshold, 512000);
    }
}
