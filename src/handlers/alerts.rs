use crate::models::{AlertConfig, AlertConfigRequest};
use crate::tracker::alerts::AlertEngine;
use actix_web::{web, HttpResponse};

/// GET /api/alerts?limit=100 — get recent alerts.
pub async fn get_alerts(
    query: web::Query<std::collections::HashMap<String, String>>,
    alert_engine: web::Data<AlertEngine>,
) -> HttpResponse {
    let limit: usize = query
        .get("limit")
        .and_then(|l| l.parse().ok())
        .unwrap_or(100);

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
    let current = alert_engine.get_config(&req.topic).await;

    let config = AlertConfig {
        topic: req.topic,
        warning_threshold: req.warning_threshold.unwrap_or(current.warning_threshold),
        critical_threshold: req.critical_threshold.unwrap_or(current.critical_threshold),
        enabled: req.enabled.unwrap_or(current.enabled),
    };

    alert_engine.set_config(config.clone()).await;
    HttpResponse::Ok().json(config)
}
