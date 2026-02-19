use crate::config::{self, SecurityConfig, SharedSecurityConfig};
use crate::kafka::admin;
use crate::kafka::admin::{AdminConsumer, SharedKafkaStats};
use crate::models::{BrokerRequest, BrokerResponse, LagHistoryResponse};
use crate::storage::LagTimeSeries;
use crate::tracker::stats::StatsEngine;
use actix_web::{web, HttpResponse};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// GET /api/broker — get current broker address and auth status.
pub async fn get_broker(
    brokers: web::Data<Arc<RwLock<String>>>,
    security: web::Data<SharedSecurityConfig>,
) -> HttpResponse {
    let brokers = brokers.read().await;
    let sec = security.read().await;
    HttpResponse::Ok().json(BrokerResponse {
        brokers: brokers.clone(),
        auth_configured: sec.is_configured(),
        security_protocol: sec.security_protocol.clone(),
    })
}

/// POST /api/broker — update broker address (and optionally auth) and recreate admin consumer.
pub async fn set_broker(
    body: web::Json<BrokerRequest>,
    brokers: web::Data<Arc<RwLock<String>>>,
    admin_consumer: web::Data<Arc<Mutex<AdminConsumer>>>,
    kafka_stats: web::Data<SharedKafkaStats>,
    security: web::Data<SharedSecurityConfig>,
) -> HttpResponse {
    let req = body.into_inner();
    let new_brokers = req.brokers;

    // Validate broker address
    if let Err(e) = config::validate_brokers(&new_brokers) {
        return HttpResponse::BadRequest().json(serde_json::json!({ "error": e }));
    }

    // Build updated security config from request
    let new_security = SecurityConfig {
        security_protocol: req.security_protocol,
        sasl_mechanism: req.sasl_mechanism,
        sasl_username: req.sasl_username,
        sasl_password: req.sasl_password,
    };

    // Try creating a new admin consumer with the new brokers + security
    match admin::create_admin_consumer(&new_brokers, kafka_stats.get_ref().clone(), &new_security) {
        Ok(new_consumer) => {
            // Update shared state
            let mut brokers_lock = brokers.write().await;
            *brokers_lock = new_brokers.clone();
            drop(brokers_lock);

            let mut consumer_lock = admin_consumer.lock().await;
            *consumer_lock = new_consumer;
            drop(consumer_lock);

            // Update shared security config
            let mut sec_lock = security.write().await;
            *sec_lock = new_security;
            let auth_configured = sec_lock.is_configured();
            let protocol = sec_lock.security_protocol.clone();
            drop(sec_lock);

            tracing::info!(brokers = %new_brokers, auth = auth_configured, "Broker updated");
            HttpResponse::Ok().json(BrokerResponse {
                brokers: new_brokers,
                auth_configured,
                security_protocol: protocol,
            })
        }
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({
            "error": format!("Failed to connect to broker: {}", e)
        })),
    }
}

/// GET /api/broker/discover — discover all brokers in the Kafka cluster.
pub async fn discover_brokers(
    brokers: web::Data<Arc<RwLock<String>>>,
    admin_consumer: web::Data<Arc<Mutex<AdminConsumer>>>,
) -> HttpResponse {
    let current = brokers.read().await.clone();
    let consumer = admin_consumer.clone();

    match web::block(move || {
        let consumer = consumer.blocking_lock();
        admin::discover_brokers(&consumer, &current)
    })
    .await
    {
        Ok(Ok(response)) => HttpResponse::Ok().json(response),
        Ok(Err(e)) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to discover brokers: {}", e)
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Blocking error: {}", e)
        })),
    }
}

/// GET /api/cluster/lag-history — consumer lag time-series data.
pub async fn get_lag_history(lag_timeseries: web::Data<LagTimeSeries>) -> HttpResponse {
    let points = lag_timeseries.get_all().await;
    HttpResponse::Ok().json(LagHistoryResponse { points })
}

/// GET /api/cluster/insights — full cluster insights with lag, throughput, and recommendations.
/// Serves entirely from cached data — no AdminConsumer lock, no Kafka calls.
/// Background lag collector updates the cache every 30 seconds.
pub async fn get_cluster_insights(
    stats_engine: web::Data<StatsEngine>,
    kafka_stats: web::Data<SharedKafkaStats>,
    progress_tracker: web::Data<crate::tracker::progress::ProgressTracker>,
) -> HttpResponse {
    let all_stats = stats_engine.get_all_stats().await;
    let cached_lag = progress_tracker.get_cached_watermarks().await;
    let cached_metadata = progress_tracker.get_cached_metadata().await;

    let response = admin::build_cluster_insights(&cached_metadata, all_stats, &kafka_stats, cached_lag);
    HttpResponse::Ok().json(response)
}
