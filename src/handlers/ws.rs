use crate::models::{WsClientMessage, WsMessage};
use actix_web::{web, HttpRequest, HttpResponse};
use actix_ws::Message;
use futures_util::StreamExt;
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const MAX_EVENTS_PER_SEC: usize = 50;

/// GET /ws/live — WebSocket endpoint for real-time size events, stats updates, and alerts.
pub async fn ws_handler(
    req: HttpRequest,
    stream: web::Payload,
    ws_broadcast: web::Data<broadcast::Sender<WsMessage>>,
) -> actix_web::Result<HttpResponse> {
    let (res, session, msg_stream) = actix_ws::handle(&req, stream)?;
    let rx = ws_broadcast.subscribe();

    actix_rt::spawn(ws_session(session, msg_stream, rx));

    Ok(res)
}

async fn ws_session(
    mut session: actix_ws::Session,
    mut msg_stream: actix_ws::MessageStream,
    mut rx: broadcast::Receiver<WsMessage>,
) {
    let mut subscribed_topics: HashSet<String> = HashSet::new();
    let mut subscribe_all = true; // By default, receive everything
    let mut last_heartbeat = Instant::now();
    let mut heartbeat_interval = tokio::time::interval(HEARTBEAT_INTERVAL);
    let mut events_this_second: usize = 0;
    let mut rate_limit_reset = Instant::now();

    loop {
        tokio::select! {
            // Incoming broadcast messages from Kafka
            msg = rx.recv() => {
                match msg {
                    Ok(ws_msg) => {
                        // Rate limiting for size events
                        if matches!(&ws_msg, WsMessage::SizeEvent(_)) {
                            let now = Instant::now();
                            if now.duration_since(rate_limit_reset) >= Duration::from_secs(1) {
                                events_this_second = 0;
                                rate_limit_reset = now;
                            }
                            if events_this_second >= MAX_EVENTS_PER_SEC {
                                continue;
                            }
                            events_this_second += 1;
                        }

                        // Topic filtering
                        if !subscribe_all {
                            let topic = match &ws_msg {
                                WsMessage::SizeEvent(e) => &e.topic,
                                WsMessage::StatsUpdate(s) => &s.topic,
                                WsMessage::Alert(a) => &a.topic,
                            };
                            if !subscribed_topics.contains(topic) {
                                continue;
                            }
                        }

                        if let Ok(json) = serde_json::to_string(&ws_msg) {
                            if session.text(json).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("WebSocket client lagged, skipped {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }

            // Incoming WebSocket messages from client
            Some(msg) = msg_stream.next() => {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Ok(client_msg) = serde_json::from_str::<WsClientMessage>(&text) {
                            match client_msg.action.as_str() {
                                "subscribe" => {
                                    if let Some(topics) = client_msg.topics {
                                        subscribe_all = false;
                                        for t in topics {
                                            subscribed_topics.insert(t);
                                        }
                                    }
                                }
                                "unsubscribe" => {
                                    if let Some(topics) = client_msg.topics {
                                        for t in &topics {
                                            subscribed_topics.remove(t);
                                        }
                                        if subscribed_topics.is_empty() {
                                            subscribe_all = true;
                                        }
                                    }
                                }
                                "subscribe_all" => {
                                    subscribe_all = true;
                                    subscribed_topics.clear();
                                }
                                _ => {}
                            }
                        }
                        last_heartbeat = Instant::now();
                    }
                    Ok(Message::Ping(msg)) => {
                        let _ = session.pong(&msg).await;
                        last_heartbeat = Instant::now();
                    }
                    Ok(Message::Pong(_)) => {
                        last_heartbeat = Instant::now();
                    }
                    Ok(Message::Close(_)) | Err(_) => {
                        break;
                    }
                    _ => {}
                }
            }

            // Heartbeat ping
            _ = heartbeat_interval.tick() => {
                if Instant::now().duration_since(last_heartbeat) > Duration::from_secs(30) {
                    log::info!("WebSocket client timed out");
                    break;
                }
                if session.ping(b"").await.is_err() {
                    break;
                }
            }
        }
    }

    let _ = session.close(None).await;
    log::debug!("WebSocket session closed");
}
