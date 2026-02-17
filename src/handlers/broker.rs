use crate::kafka::admin;
use crate::models::{BrokerRequest, BrokerResponse};
use actix_web::{web, HttpResponse};
use rdkafka::consumer::BaseConsumer;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// GET /api/broker — get current broker address.
pub async fn get_broker(brokers: web::Data<Arc<RwLock<String>>>) -> HttpResponse {
    let brokers = brokers.read().await;
    HttpResponse::Ok().json(BrokerResponse {
        brokers: brokers.clone(),
    })
}

/// POST /api/broker — update broker address and recreate admin consumer.
pub async fn set_broker(
    body: web::Json<BrokerRequest>,
    brokers: web::Data<Arc<RwLock<String>>>,
    admin_consumer: web::Data<Arc<Mutex<BaseConsumer>>>,
) -> HttpResponse {
    let new_brokers = body.into_inner().brokers;

    // Try creating a new admin consumer with the new brokers
    match admin::create_admin_consumer(&new_brokers) {
        Ok(new_consumer) => {
            // Update shared state
            let mut brokers_lock = brokers.write().await;
            *brokers_lock = new_brokers.clone();
            drop(brokers_lock);

            let mut consumer_lock = admin_consumer.lock().await;
            *consumer_lock = new_consumer;
            drop(consumer_lock);

            log::info!("Broker updated to: {}", new_brokers);
            HttpResponse::Ok().json(BrokerResponse {
                brokers: new_brokers,
            })
        }
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({
            "error": format!("Failed to connect to broker: {}", e)
        })),
    }
}
