use crate::kafka::admin;
use crate::kafka::admin::AdminConsumer;
use crate::models::{
    AlertStatus, AvailableTopicsResponse, TopicSummary, TopicToggleRequest, TopicsResponse,
};
use crate::storage::{LagTimeSeries, TimeSeriesStorage};
use crate::tracker::alerts::AlertEngine;
use crate::tracker::progress::ProgressTracker;
use crate::tracker::stats::StatsEngine;
use crate::TopicManager;
use actix_web::{web, HttpResponse};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// GET /api/topics — list all topics with summary stats and alert status.
pub async fn get_topics(
    admin_consumer: web::Data<Arc<Mutex<AdminConsumer>>>,
    stats_engine: web::Data<StatsEngine>,
    alert_engine: web::Data<AlertEngine>,
) -> HttpResponse {
    let consumer = admin_consumer.get_ref().clone();

    let topics = web::block(move || {
        let consumer = consumer.blocking_lock();
        admin::list_topics(&consumer)
    })
    .await;

    match topics {
        Ok(Ok(topic_names)) => {
            let mut summaries = Vec::with_capacity(topic_names.len());
            for name in topic_names {
                let stats = stats_engine.get_stats(&name).await;
                let alert_config = alert_engine.get_config(&name).await;

                let alert_status = match &stats {
                    Some(s) => {
                        if s.max_bytes >= alert_config.critical_threshold {
                            AlertStatus::Critical
                        } else if s.max_bytes >= alert_config.warning_threshold {
                            AlertStatus::Warning
                        } else {
                            AlertStatus::Healthy
                        }
                    }
                    None => AlertStatus::Unknown,
                };

                summaries.push(TopicSummary {
                    name,
                    stats,
                    alert_status,
                });
            }
            HttpResponse::Ok().json(TopicsResponse { topics: summaries })
        }
        Ok(Err(e)) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to list topics: {}", e)
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Blocking error: {}", e)
        })),
    }
}

/// GET /api/topics/available — list all Kafka topics + currently tracked ones.
pub async fn get_available_topics(
    admin_consumer: web::Data<Arc<Mutex<AdminConsumer>>>,
    tracked_topics: web::Data<Arc<RwLock<HashSet<String>>>>,
) -> HttpResponse {
    let consumer = admin_consumer.get_ref().clone();

    let topics = web::block(move || {
        let consumer = consumer.blocking_lock();
        admin::list_topics(&consumer)
    })
    .await;

    match topics {
        Ok(Ok(available)) => {
            let tracked: Vec<String> = tracked_topics.read().await.iter().cloned().collect();
            HttpResponse::Ok().json(AvailableTopicsResponse { available, tracked })
        }
        Ok(Err(e)) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to list topics: {}", e)
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Blocking error: {}", e)
        })),
    }
}

/// POST /api/topics/toggle — enable or disable topics for monitoring.
/// If `start_from` is provided in the request body, immediately subscribes
/// to the topics with the specified start position.
pub async fn toggle_topics(
    body: web::Json<TopicToggleRequest>,
    tracked_topics: web::Data<Arc<RwLock<HashSet<String>>>>,
    topic_manager: web::Data<Arc<TopicManager>>,
) -> HttpResponse {
    let req = body.into_inner();
    let mut tracked = tracked_topics.write().await;

    if req.enabled {
        for topic in &req.topics {
            tracked.insert(topic.clone());
        }
    } else {
        for topic in &req.topics {
            tracked.remove(topic);
        }
    }

    let current: Vec<String> = tracked.iter().cloned().collect();
    drop(tracked);

    log::info!(
        "Topics toggled: enabled={}, topics={:?}, start_from={:?}, current_tracked={:?}",
        req.enabled,
        req.topics,
        req.start_from,
        current
    );

    // If enabling with a start position, trigger immediate subscription
    if req.enabled {
        topic_manager.subscribe_all(&req.topics, req.start_from).await;
    }

    HttpResponse::Ok().json(serde_json::json!({
        "tracked": current
    }))
}

/// POST /api/reset — stop all consumers and clear all stats, alerts, and storage.
/// Used when the dashboard needs a full reset (e.g., re-selecting topics with a new offset).
pub async fn reset_all(
    topic_manager: web::Data<Arc<TopicManager>>,
    stats_engine: web::Data<StatsEngine>,
    alert_engine: web::Data<AlertEngine>,
    progress_tracker: web::Data<ProgressTracker>,
    storage: web::Data<TimeSeriesStorage>,
    lag_timeseries: web::Data<LagTimeSeries>,
    tracked_topics: web::Data<Arc<RwLock<HashSet<String>>>>,
) -> HttpResponse {
    // Stop all consumers
    topic_manager.clear().await;

    // Clear tracked topics set
    tracked_topics.write().await.clear();

    // Clear all backend state
    stats_engine.clear().await;
    alert_engine.clear_alerts().await;
    progress_tracker.clear().await;
    storage.clear().await;
    lag_timeseries.clear().await;

    log::info!("Dashboard reset: all consumers stopped, stats/alerts/storage/progress cleared");
    HttpResponse::Ok().json(serde_json::json!({ "status": "reset" }))
}

/// POST /api/disconnect — stop all consumers without clearing stats.
/// Used to disconnect from the Kafka broker while preserving dashboard data.
pub async fn disconnect(
    topic_manager: web::Data<Arc<TopicManager>>,
    tracked_topics: web::Data<Arc<RwLock<HashSet<String>>>>,
) -> HttpResponse {
    topic_manager.clear().await;
    tracked_topics.write().await.clear();

    log::info!("Disconnected: all consumers stopped");
    HttpResponse::Ok().json(serde_json::json!({ "status": "disconnected" }))
}
