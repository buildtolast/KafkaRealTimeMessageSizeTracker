use crate::tracker::stats::StatsEngine;
use actix_web::{web, HttpResponse};

/// GET /api/stats/{topic} — detailed stats for a single topic.
pub async fn get_topic_stats(
    path: web::Path<String>,
    stats_engine: web::Data<StatsEngine>,
) -> HttpResponse {
    let topic = path.into_inner();
    match stats_engine.get_stats(&topic).await {
        Some(stats) => HttpResponse::Ok().json(stats),
        None => HttpResponse::NotFound().json(serde_json::json!({
            "error": format!("No stats available for topic: {}", topic)
        })),
    }
}
