use crate::kafka::browser;
use crate::models::{
    BrowseQueryParams, GlobalSnapshotResponse, SnapshotQueryParams, TopicSnapshot,
};
use crate::storage::TimeSeriesStorage;
use actix_web::{web, HttpResponse};
use chrono::DateTime;
use std::sync::Arc;
use tokio::sync::RwLock;

/// GET /api/browse/{topic}?partition=0&offset=100&limit=50
/// GET /api/browse/{topic}?timestamp=2026-02-17T10:00:00Z&limit=50
/// GET /api/browse/{topic}?preset=beginning|1h|1d|latest&limit=50
pub async fn browse_messages(
    path: web::Path<String>,
    query: web::Query<BrowseQueryParams>,
    brokers: web::Data<Arc<RwLock<String>>>,
) -> HttpResponse {
    let topic = path.into_inner();
    let params = query.into_inner();
    let limit = params.limit.unwrap_or(50).min(100);
    let brokers = brokers.read().await.clone();

    // Check preset first — these are quick buttons
    if let Some(ref preset) = params.preset {
        return match preset.as_str() {
            "beginning" => {
                let topic_clone = topic.clone();
                match web::block(move || {
                    browser::browse_from_beginning(&brokers, &topic_clone, limit)
                })
                .await
                {
                    Ok(Ok(response)) => HttpResponse::Ok().json(response),
                    Ok(Err(e)) => HttpResponse::InternalServerError().json(serde_json::json!({
                        "error": format!("Kafka error: {}", e)
                    })),
                    Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
                        "error": format!("Blocking error: {}", e)
                    })),
                }
            }
            "1h" => {
                let timestamp = chrono::Utc::now() - chrono::Duration::hours(1);
                let topic_clone = topic.clone();
                match web::block(move || {
                    browser::browse_by_timestamp(&brokers, &topic_clone, timestamp, limit)
                })
                .await
                {
                    Ok(Ok(response)) => HttpResponse::Ok().json(response),
                    Ok(Err(e)) => HttpResponse::InternalServerError().json(serde_json::json!({
                        "error": format!("Kafka error: {}", e)
                    })),
                    Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
                        "error": format!("Blocking error: {}", e)
                    })),
                }
            }
            "1d" => {
                let timestamp = chrono::Utc::now() - chrono::Duration::days(1);
                let topic_clone = topic.clone();
                match web::block(move || {
                    browser::browse_by_timestamp(&brokers, &topic_clone, timestamp, limit)
                })
                .await
                {
                    Ok(Ok(response)) => HttpResponse::Ok().json(response),
                    Ok(Err(e)) => HttpResponse::InternalServerError().json(serde_json::json!({
                        "error": format!("Kafka error: {}", e)
                    })),
                    Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
                        "error": format!("Blocking error: {}", e)
                    })),
                }
            }
            "latest" => {
                let topic_clone = topic.clone();
                match web::block(move || {
                    browser::browse_latest(&brokers, &topic_clone, limit)
                })
                .await
                {
                    Ok(Ok(response)) => HttpResponse::Ok().json(response),
                    Ok(Err(e)) => HttpResponse::InternalServerError().json(serde_json::json!({
                        "error": format!("Kafka error: {}", e)
                    })),
                    Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
                        "error": format!("Blocking error: {}", e)
                    })),
                }
            }
            other => HttpResponse::BadRequest().json(serde_json::json!({
                "error": format!("Unknown preset '{}'. Use: beginning, 1h, 1d, latest", other)
            })),
        };
    }

    if let Some(offset) = params.offset {
        // Offset-based browse
        let partition = params.partition.unwrap_or(0);
        let topic_clone = topic.clone();
        match web::block(move || {
            browser::browse_by_offset(&brokers, &topic_clone, partition, offset, limit)
        })
        .await
        {
            Ok(Ok(response)) => HttpResponse::Ok().json(response),
            Ok(Err(e)) => HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Kafka error: {}", e)
            })),
            Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Blocking error: {}", e)
            })),
        }
    } else if let Some(ts_str) = &params.timestamp {
        // Timestamp-based browse
        let timestamp = match DateTime::parse_from_rfc3339(ts_str) {
            Ok(dt) => dt.with_timezone(&chrono::Utc),
            Err(_) => {
                return HttpResponse::BadRequest().json(serde_json::json!({
                    "error": "Invalid timestamp format. Use ISO 8601 (e.g., 2026-02-17T10:00:00Z)"
                }));
            }
        };
        let topic_clone = topic.clone();
        match web::block(move || {
            browser::browse_by_timestamp(&brokers, &topic_clone, timestamp, limit)
        })
        .await
        {
            Ok(Ok(response)) => HttpResponse::Ok().json(response),
            Ok(Err(e)) => HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Kafka error: {}", e)
            })),
            Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Blocking error: {}", e)
            })),
        }
    } else {
        HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Provide 'preset', 'offset' (with optional 'partition'), or 'timestamp' query parameter"
        }))
    }
}

/// GET /api/browse/{topic}/download/{partition}/{offset}
/// Download the full payload of a specific message.
pub async fn download_message(
    path: web::Path<(String, i32, i64)>,
    brokers: web::Data<Arc<RwLock<String>>>,
) -> HttpResponse {
    let (topic, partition, offset) = path.into_inner();
    let brokers = brokers.read().await.clone();
    let topic_clone = topic.clone();

    match web::block(move || {
        browser::fetch_full_message(&brokers, &topic_clone, partition, offset)
    })
    .await
    {
        Ok(Ok(Some(payload))) => HttpResponse::Ok()
            .content_type("application/octet-stream")
            .insert_header((
                "Content-Disposition",
                format!(
                    "attachment; filename=\"msg-{}-p{}-o{}.bin\"",
                    topic, partition, offset
                ),
            ))
            .body(payload),
        Ok(Ok(None)) => HttpResponse::NotFound().json(serde_json::json!({
            "error": "Message not found (may have been compacted or expired)"
        })),
        Ok(Err(e)) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Kafka error: {}", e)
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Blocking error: {}", e)
        })),
    }
}

/// GET /api/snapshot?timestamp=2026-02-17T10:00:00Z
/// Get a global dashboard snapshot at a specific point in time.
pub async fn get_global_snapshot(
    query: web::Query<SnapshotQueryParams>,
    storage: web::Data<TimeSeriesStorage>,
) -> HttpResponse {
    let timestamp = match DateTime::parse_from_rfc3339(&query.timestamp) {
        Ok(dt) => dt.with_timezone(&chrono::Utc),
        Err(_) => {
            return HttpResponse::BadRequest().json(serde_json::json!({
                "error": "Invalid timestamp format. Use ISO 8601 (e.g., 2026-02-17T10:00:00Z)"
            }));
        }
    };

    let snapshots = storage.snapshot_at(timestamp).await;

    let topics: Vec<TopicSnapshot> = snapshots
        .into_iter()
        .map(|(topic, data_point, offset_seconds)| TopicSnapshot {
            topic,
            data_point,
            offset_seconds,
        })
        .collect();

    HttpResponse::Ok().json(GlobalSnapshotResponse {
        requested_timestamp: timestamp,
        topics,
    })
}
