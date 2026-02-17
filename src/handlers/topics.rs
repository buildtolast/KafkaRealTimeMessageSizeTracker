use crate::kafka::admin;
use crate::models::{
    AlertStatus, AvailableTopicsResponse, TopicSummary, TopicToggleRequest, TopicsResponse,
};
use crate::tracker::alerts::AlertEngine;
use crate::tracker::stats::StatsEngine;
use actix_web::{web, HttpResponse};
use rdkafka::consumer::BaseConsumer;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// GET /api/topics — list all topics with summary stats and alert status.
pub async fn get_topics(
    admin_consumer: web::Data<Arc<Mutex<BaseConsumer>>>,
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
    admin_consumer: web::Data<Arc<Mutex<BaseConsumer>>>,
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
pub async fn toggle_topics(
    body: web::Json<TopicToggleRequest>,
    tracked_topics: web::Data<Arc<RwLock<HashSet<String>>>>,
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
    log::info!(
        "Topics toggled: enabled={}, topics={:?}, current_tracked={:?}",
        req.enabled,
        req.topics,
        current
    );

    HttpResponse::Ok().json(serde_json::json!({
        "tracked": current
    }))
}
