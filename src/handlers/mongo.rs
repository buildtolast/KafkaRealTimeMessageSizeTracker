use crate::mongo::SharedMongoStore;
use actix_web::{web, HttpResponse};
use serde::Deserialize;

/// Query parameters for persisted data endpoints.
#[derive(Debug, Deserialize)]
pub struct PersistedQueryParams {
    pub topic: Option<String>,
    pub limit: Option<i64>,
}

/// GET /api/persisted/messages?topic=X&limit=100
pub async fn get_persisted_messages(
    query: web::Query<PersistedQueryParams>,
    mongo: web::Data<SharedMongoStore>,
) -> HttpResponse {
    let store = match mongo.as_ref().as_ref() {
        Some(s) => s,
        None => {
            return HttpResponse::ServiceUnavailable().json(serde_json::json!({
                "error": "MongoDB not configured. Set MONGO_URI or --mongo-uri to enable persistence."
            }));
        }
    };

    let limit = query.limit.unwrap_or(100).min(1000);
    let topic = query.topic.as_deref();

    match store.query_messages(topic, limit).await {
        Ok(messages) => HttpResponse::Ok().json(messages),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("MongoDB query error: {}", e)
        })),
    }
}

/// GET /api/persisted/alerts?topic=X&limit=100
pub async fn get_persisted_alerts(
    query: web::Query<PersistedQueryParams>,
    mongo: web::Data<SharedMongoStore>,
) -> HttpResponse {
    let store = match mongo.as_ref().as_ref() {
        Some(s) => s,
        None => {
            return HttpResponse::ServiceUnavailable().json(serde_json::json!({
                "error": "MongoDB not configured. Set MONGO_URI or --mongo-uri to enable persistence."
            }));
        }
    };

    let limit = query.limit.unwrap_or(100).min(1000);
    let topic = query.topic.as_deref();

    match store.query_alerts(topic, limit).await {
        Ok(alerts) => HttpResponse::Ok().json(alerts),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("MongoDB query error: {}", e)
        })),
    }
}

/// GET /api/persisted/status — check if MongoDB is configured and connected.
pub async fn get_persistence_status(
    mongo: web::Data<SharedMongoStore>,
) -> HttpResponse {
    let configured = mongo.as_ref().is_some();
    HttpResponse::Ok().json(serde_json::json!({
        "configured": configured,
        "status": if configured { "connected" } else { "not_configured" }
    }))
}
