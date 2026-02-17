use crate::kafka::admin;
use crate::models::{AlertStatus, TopicSummary, TopicsResponse};
use crate::tracker::alerts::AlertEngine;
use crate::tracker::stats::StatsEngine;
use actix_web::{web, HttpResponse};
use rdkafka::consumer::BaseConsumer;
use std::sync::Arc;
use tokio::sync::Mutex;

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
