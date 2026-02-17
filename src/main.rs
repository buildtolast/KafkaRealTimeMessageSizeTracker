mod config;
mod error;
mod frontend;
mod handlers;
mod kafka;
mod models;
mod mongo;
mod storage;
mod tracker;

use crate::config::{AppConfig, Args};
use crate::kafka::admin;
use crate::models::{CapturedMessage, MessageSizeEvent, WsMessage};
use crate::mongo::{MongoStore, PersistedAlert, PersistedMessage, SharedMongoStore};
use crate::storage::TimeSeriesStorage;
use crate::tracker::alerts::AlertEngine;
use crate::tracker::stats::StatsEngine;
use actix_cors::Cors;
use actix_web::{middleware, web, App, HttpResponse, HttpServer};
use clap::Parser;
use rdkafka::consumer::BaseConsumer;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex, RwLock};

/// Manages per-topic Kafka consumers and broadcast channels.
pub struct TopicManager {
    consumers: Arc<RwLock<HashMap<String, broadcast::Sender<MessageSizeEvent>>>>,
    brokers: Arc<RwLock<String>>,
    consumer_group: String,
    stats_engine: StatsEngine,
    alert_engine: AlertEngine,
    ws_broadcast: broadcast::Sender<WsMessage>,
    mongo_store: SharedMongoStore,
}

impl TopicManager {
    fn new(
        brokers: String,
        consumer_group: String,
        stats_engine: StatsEngine,
        alert_engine: AlertEngine,
        ws_broadcast: broadcast::Sender<WsMessage>,
        mongo_store: SharedMongoStore,
    ) -> Self {
        Self {
            consumers: Arc::new(RwLock::new(HashMap::new())),
            brokers: Arc::new(RwLock::new(brokers)),
            consumer_group,
            stats_engine,
            alert_engine,
            ws_broadcast,
            mongo_store,
        }
    }

    /// Subscribe to a topic. Spawns a consumer if this is the first subscription.
    async fn subscribe(&self, topic: &str) -> broadcast::Receiver<MessageSizeEvent> {
        let mut consumers = self.consumers.write().await;

        if let Some(tx) = consumers.get(topic) {
            return tx.subscribe();
        }

        // Create a new broadcast channel for this topic
        let (tx, rx) = broadcast::channel::<MessageSizeEvent>(1024);
        consumers.insert(topic.to_string(), tx.clone());

        // Spawn the consumer task
        let brokers = self.brokers.read().await.clone();
        let topic_owned = topic.to_string();
        let group = self.consumer_group.clone();
        let stats = self.stats_engine.clone();
        let alerts = self.alert_engine.clone();
        let ws_tx = self.ws_broadcast.clone();
        let event_tx = tx.clone();
        let capture_threshold = self.alert_engine.default_warning;
        let mongo = self.mongo_store.clone();

        tokio::spawn(async move {
            let mut event_rx = event_tx.subscribe();

            // Spawn the actual Kafka consumer
            let consumer_topic = topic_owned.clone();
            tokio::spawn(async move {
                crate::kafka::consumer::run_size_tracking_consumer(
                    brokers,
                    consumer_topic,
                    group,
                    event_tx,
                    capture_threshold,
                )
                .await;
            });

            // Process events: feed into stats + alerts + ws broadcast
            loop {
                match event_rx.recv().await {
                    Ok(event) => {
                        // Update stats
                        stats.record_event(&event).await;

                        // Check for alerts
                        if let Some(mut alert) = alerts.check_event(&event).await {
                            // If the event captured the oversized payload, store it
                            if let Some(payload) = &event.captured_payload {
                                let captured = CapturedMessage {
                                    alert_id: alert.id,
                                    topic: event.topic.clone(),
                                    partition: event.partition,
                                    offset: event.offset,
                                    payload: payload.clone(),
                                    key: event.captured_key.clone(),
                                    size_bytes: event.size_bytes,
                                    timestamp: event.timestamp,
                                };
                                alert.capture_id = Some(alert.id);
                                alerts.store_capture(alert.id, captured).await;

                                // Persist oversized message to MongoDB (async, fire-and-forget)
                                if let Some(store) = mongo.as_ref() {
                                    use base64::Engine;
                                    let persisted = PersistedMessage {
                                        topic: event.topic.clone(),
                                        partition: event.partition,
                                        offset: event.offset,
                                        timestamp: event.timestamp,
                                        size_bytes: event.size_bytes,
                                        key: event.captured_key.as_ref().map(|k| {
                                            base64::engine::general_purpose::STANDARD.encode(k)
                                        }),
                                        value: Some(
                                            base64::engine::general_purpose::STANDARD
                                                .encode(payload),
                                        ),
                                    };
                                    let store = store.clone();
                                    tokio::spawn(async move {
                                        if let Err(e) = store.persist_message(persisted).await {
                                            log::warn!("Failed to persist oversized message to MongoDB: {}", e);
                                        }
                                    });
                                }
                            }

                            // Persist alert to MongoDB (async, fire-and-forget)
                            if let Some(store) = mongo.as_ref() {
                                let persisted = PersistedAlert {
                                    alert_id: alert.id,
                                    topic: alert.topic.clone(),
                                    severity: format!("{:?}", alert.severity).to_lowercase(),
                                    message_size: alert.message_size,
                                    threshold: alert.threshold,
                                    timestamp: alert.timestamp,
                                };
                                let store = store.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = store.persist_alert(persisted).await {
                                        log::warn!("Failed to persist alert to MongoDB: {}", e);
                                    }
                                });
                            }

                            let _ = ws_tx.send(WsMessage::Alert(alert));
                        }

                        // Forward size event to WebSocket clients
                        // (captured_payload is #[serde(skip)] so it won't be serialized)
                        let _ = ws_tx.send(WsMessage::SizeEvent(event));
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!(
                            "Event processor lagged, skipped {} events for topic {}",
                            n,
                            topic_owned
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        rx
    }

    /// Subscribe to all topics from the admin consumer.
    async fn subscribe_all(&self, topics: &[String]) {
        for topic in topics {
            self.subscribe(topic).await;
        }
    }

    /// Clear all consumers (e.g., on broker change).
    async fn clear(&self) {
        let mut consumers = self.consumers.write().await;
        consumers.clear();
    }
}

/// Serve the embedded frontend HTML.
async fn index() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(frontend::INDEX_HTML)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let config = AppConfig::from_args_and_env(args);

    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!(
        "KafkaRealTimeMessageSizeTracker starting on {}:{}",
        config.server_host,
        config.server_port
    );
    log::info!("Kafka brokers: {}", config.kafka_brokers);
    log::info!(
        "Alert thresholds: warning={}B, critical={}B",
        config.default_warning_threshold,
        config.default_critical_threshold
    );

    // Create shared state
    let admin_consumer: BaseConsumer = admin::create_admin_consumer(&config.kafka_brokers)
        .expect("Failed to create admin consumer");
    let admin_consumer = Arc::new(Mutex::new(admin_consumer));

    let stats_engine = StatsEngine::new();
    let alert_engine = AlertEngine::new(
        config.default_warning_threshold,
        config.default_critical_threshold,
    );
    let storage = TimeSeriesStorage::new();
    let (ws_tx, _ws_rx) = broadcast::channel::<WsMessage>(4096);

    // Tracked topics set (for topic selection feature)
    let tracked_topics: Arc<RwLock<HashSet<String>>> =
        Arc::new(RwLock::new(HashSet::new()));

    // MongoDB initialization (optional)
    let mongo_store: SharedMongoStore = if let Some(uri) = &config.mongo_uri {
        match MongoStore::new(uri).await {
            Ok(store) => {
                log::info!("MongoDB persistence enabled");
                Arc::new(Some(store))
            }
            Err(e) => {
                log::warn!("Failed to connect to MongoDB ({}): {} — persistence disabled", uri, e);
                Arc::new(None)
            }
        }
    } else {
        log::info!("MongoDB not configured — persistence disabled");
        Arc::new(None)
    };

    let topic_manager = Arc::new(TopicManager::new(
        config.kafka_brokers.clone(),
        config.consumer_group.clone(),
        stats_engine.clone(),
        alert_engine.clone(),
        ws_tx.clone(),
        mongo_store.clone(),
    ));

    let brokers_shared = Arc::new(RwLock::new(config.kafka_brokers.clone()));

    // Background task: stats broadcaster (every 1 second)
    let stats_for_broadcast = stats_engine.clone();
    let ws_tx_stats = ws_tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let all_stats = stats_for_broadcast.get_all_stats().await;
            for stats in all_stats {
                let _ = ws_tx_stats.send(WsMessage::StatsUpdate(stats));
            }
        }
    });

    // Background task: minute aggregator
    let stats_for_agg = stats_engine.clone();
    let storage_for_agg = storage.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let snapshots = stats_for_agg.snapshot_and_reset_minute().await;
            for (topic, point) in snapshots {
                storage_for_agg.append(&topic, point).await;
            }
        }
    });

    // Background task: auto-discover and subscribe to topics (every 30 seconds)
    let tm_for_discover = topic_manager.clone();
    let admin_for_discover = admin_consumer.clone();
    let tracked_for_discover = tracked_topics.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            let consumer = admin_for_discover.clone();
            let topics = web::block(move || {
                let consumer = consumer.blocking_lock();
                admin::list_topics(&consumer)
            })
            .await;

            if let Ok(Ok(topic_list)) = topics {
                let tracked = tracked_for_discover.read().await;
                if tracked.is_empty() {
                    // No selection yet — subscribe to all topics (backward-compatible)
                    drop(tracked);
                    tm_for_discover.subscribe_all(&topic_list).await;
                } else {
                    // Only subscribe to tracked topics
                    let filtered: Vec<String> = topic_list
                        .into_iter()
                        .filter(|t| tracked.contains(t))
                        .collect();
                    drop(tracked);
                    tm_for_discover.subscribe_all(&filtered).await;
                }
            }
        }
    });

    let bind_addr = format!("{}:{}", config.server_host, config.server_port);
    log::info!("Listening on http://{}", bind_addr);

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header();

        App::new()
            .wrap(middleware::Logger::default())
            .wrap(cors)
            .app_data(web::Data::new(admin_consumer.clone()))
            .app_data(web::Data::new(stats_engine.clone()))
            .app_data(web::Data::new(alert_engine.clone()))
            .app_data(web::Data::new(storage.clone()))
            .app_data(web::Data::new(ws_tx.clone()))
            .app_data(web::Data::new(brokers_shared.clone()))
            .app_data(web::Data::new(tracked_topics.clone()))
            .app_data(web::Data::new(mongo_store.clone()))
            .route("/", web::get().to(index))
            .route("/api/topics", web::get().to(handlers::topics::get_topics))
            .route(
                "/api/topics/available",
                web::get().to(handlers::topics::get_available_topics),
            )
            .route(
                "/api/topics/toggle",
                web::post().to(handlers::topics::toggle_topics),
            )
            .route(
                "/api/stats/{topic}",
                web::get().to(handlers::stats::get_topic_stats),
            )
            .route("/api/alerts", web::get().to(handlers::alerts::get_alerts))
            .route(
                "/api/alerts/download/{id}",
                web::get().to(handlers::alerts::download_capture),
            )
            .route(
                "/api/alerts/config",
                web::get().to(handlers::alerts::get_alert_configs),
            )
            .route(
                "/api/alerts/config",
                web::post().to(handlers::alerts::set_alert_config),
            )
            .route(
                "/api/history/{topic}",
                web::get().to(handlers::history::get_history),
            )
            .route("/api/broker", web::get().to(handlers::broker::get_broker))
            .route("/api/broker", web::post().to(handlers::broker::set_broker))
            .route(
                "/api/broker/discover",
                web::get().to(handlers::broker::discover_brokers),
            )
            .route(
                "/api/cluster/insights",
                web::get().to(handlers::broker::get_cluster_insights),
            )
            .route(
                "/api/browse/{topic}",
                web::get().to(handlers::browse::browse_messages),
            )
            .route(
                "/api/browse/{topic}/download/{partition}/{offset}",
                web::get().to(handlers::browse::download_message),
            )
            .route(
                "/api/snapshot",
                web::get().to(handlers::browse::get_global_snapshot),
            )
            .route(
                "/api/persisted/messages",
                web::get().to(handlers::mongo::get_persisted_messages),
            )
            .route(
                "/api/persisted/alerts",
                web::get().to(handlers::mongo::get_persisted_alerts),
            )
            .route(
                "/api/persisted/status",
                web::get().to(handlers::mongo::get_persistence_status),
            )
            .route("/ws/live", web::get().to(handlers::ws::ws_handler))
    })
    .bind(&bind_addr)?
    .run()
    .await
}
