use crate::tracker::progress::ProgressTracker;
use actix_web::{web, HttpResponse};

/// GET /api/consumer/progress — current consumer progress for all tracked topics.
/// Uses cached watermarks from the background lag collector — instant, no Kafka call.
pub async fn get_consumer_progress(
    progress: web::Data<ProgressTracker>,
) -> HttpResponse {
    let response = progress.get_progress_cached().await;
    HttpResponse::Ok().json(response)
}

/// GET /api/consumer/progress/timeseries — time-series of consumer progress snapshots.
pub async fn get_progress_timeseries(
    progress: web::Data<ProgressTracker>,
) -> HttpResponse {
    let points = progress.get_timeseries().await;
    HttpResponse::Ok().json(points)
}
