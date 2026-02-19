use crate::config;
use crate::models::{AlertConfig, AlertConfigRequest, AlertDefaultsRequest, AlertDefaultsResponse};
use crate::tracker::alerts::AlertEngine;
use actix_web::{web, HttpResponse};

/// Maximum limit for alert queries (prevent OOM from huge requests).
const MAX_ALERT_LIMIT: usize = 10_000;

/// GET /api/alerts?limit=100 — get recent alerts.
pub async fn get_alerts(
    query: web::Query<std::collections::HashMap<String, String>>,
    alert_engine: web::Data<AlertEngine>,
) -> HttpResponse {
    let limit: usize = query
        .get("limit")
        .and_then(|l| l.parse().ok())
        .unwrap_or(100)
        .min(MAX_ALERT_LIMIT);

    let alerts = alert_engine.get_alerts(limit).await;
    HttpResponse::Ok().json(alerts)
}

/// GET /api/alerts/config — get all alert threshold configs.
pub async fn get_alert_configs(alert_engine: web::Data<AlertEngine>) -> HttpResponse {
    let configs = alert_engine.get_all_configs().await;
    HttpResponse::Ok().json(configs)
}

/// POST /api/alerts/config — set alert threshold for a topic.
pub async fn set_alert_config(
    body: web::Json<AlertConfigRequest>,
    alert_engine: web::Data<AlertEngine>,
) -> HttpResponse {
    let req = body.into_inner();

    // Validate topic name
    if let Err(e) = config::validate_topic_name(&req.topic) {
        return HttpResponse::BadRequest().json(serde_json::json!({ "error": e }));
    }

    let current = alert_engine.get_config(&req.topic).await;

    let warning = req.warning_threshold.unwrap_or(current.warning_threshold);
    let critical = req.critical_threshold.unwrap_or(current.critical_threshold);

    // Validate thresholds
    if let Err(e) = config::validate_thresholds(warning, critical) {
        return HttpResponse::BadRequest().json(serde_json::json!({ "error": e }));
    }

    let config = AlertConfig {
        topic: req.topic,
        warning_threshold: warning,
        critical_threshold: critical,
        enabled: req.enabled.unwrap_or(current.enabled),
    };

    alert_engine.set_config(config.clone()).await;
    HttpResponse::Ok().json(config)
}

/// GET /api/alerts/defaults — get global default thresholds.
pub async fn get_alert_defaults(alert_engine: web::Data<AlertEngine>) -> HttpResponse {
    let (warning, critical) = alert_engine.get_defaults().await;
    HttpResponse::Ok().json(AlertDefaultsResponse {
        warning_threshold: warning,
        critical_threshold: critical,
    })
}

/// POST /api/alerts/defaults — update global default thresholds at runtime.
pub async fn set_alert_defaults(
    body: web::Json<AlertDefaultsRequest>,
    alert_engine: web::Data<AlertEngine>,
) -> HttpResponse {
    let req = body.into_inner();
    let (current_warning, current_critical) = alert_engine.get_defaults().await;

    let warning = req.warning_threshold.unwrap_or(current_warning);
    let critical = req.critical_threshold.unwrap_or(current_critical);

    if let Err(e) = config::validate_thresholds(warning, critical) {
        return HttpResponse::BadRequest().json(serde_json::json!({ "error": e }));
    }

    alert_engine.set_defaults(warning, critical).await;
    HttpResponse::Ok().json(AlertDefaultsResponse {
        warning_threshold: warning,
        critical_threshold: critical,
    })
}

/// GET /api/alerts/download/{id} — download captured oversized message payload.
pub async fn download_capture(
    path: web::Path<u64>,
    alert_engine: web::Data<AlertEngine>,
) -> HttpResponse {
    let alert_id = path.into_inner();
    match alert_engine.get_capture(alert_id).await {
        Some(msg) => HttpResponse::Ok()
            .content_type("application/octet-stream")
            .insert_header((
                "Content-Disposition",
                format!(
                    "attachment; filename=\"msg-{}-p{}-o{}.json\"",
                    msg.topic, msg.partition, msg.offset
                ),
            ))
            .body(msg.payload),
        None => HttpResponse::NotFound().json(serde_json::json!({
            "error": "Capture not found or expired"
        })),
    }
}
