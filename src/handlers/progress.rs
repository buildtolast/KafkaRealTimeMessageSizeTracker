use crate::kafka::admin;
use crate::kafka::admin::AdminConsumer;
use crate::tracker::progress::ProgressTracker;
use actix_web::{web, HttpResponse};
use std::sync::Arc;
use tokio::sync::Mutex;

/// GET /api/consumer/progress — current consumer progress for all tracked topics.
pub async fn get_consumer_progress(
    progress: web::Data<ProgressTracker>,
    admin_consumer: web::Data<Arc<Mutex<AdminConsumer>>>,
) -> HttpResponse {
    let consumer = admin_consumer.get_ref().clone();

    // Fetch watermarks for progress calculation
    let watermarks = match web::block(move || {
        let consumer = consumer.blocking_lock();
        admin::fetch_consumer_lag(&consumer)
    })
    .await
    {
        Ok(Ok(wm)) => wm,
        Ok(Err(_)) => vec![],
        Err(_) => vec![],
    };

    let response = progress.get_progress(&watermarks).await;
    HttpResponse::Ok().json(response)
}

/// GET /api/consumer/progress/timeseries — time-series of consumer progress snapshots.
pub async fn get_progress_timeseries(
    progress: web::Data<ProgressTracker>,
) -> HttpResponse {
    let points = progress.get_timeseries().await;
    HttpResponse::Ok().json(points)
}
