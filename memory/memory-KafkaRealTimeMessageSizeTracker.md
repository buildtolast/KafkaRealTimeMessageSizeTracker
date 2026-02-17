# KafkaRealTimeMessageSizeTracker

## Overview
- **Location**: ~/GIT/KafkaRealTimeMessageSizeTracker
- **Purpose**: Real-time Kafka message size monitoring dashboard with alerts and analytics
- **Stack**: Rust (actix-web 4, rdkafka 0.37, tokio 1) + embedded single-file HTML frontend
- **Edition**: 2024
- **Binary name**: `kafka-message-size-tracker`

## Running
- Dev: `cargo run -- --brokers localhost:9092 --port 3000`
- Full stack Docker: `docker compose -f docker-compose.full.yml up -d`
- App-only Docker: `KAFKA_BROKERS=host:port docker compose up -d`
- Dashboard URL: http://localhost:3000, Kafka external: localhost:9094

## CLI Arguments (clap)
- `--brokers` (default: localhost:9092) — Kafka broker addresses
- `--port` (default: 3000) — Server port
- `--host` (default: 127.0.0.1) — Server host
- `--threshold` (default: 1048576 = 1MB) — Warning alert threshold
- `--critical-threshold` (default: 10485760 = 10MB) — Critical alert threshold

## Environment Variable Overrides
KAFKA_BROKERS, SERVER_HOST, SERVER_PORT, ALERT_THRESHOLD_BYTES, CRITICAL_THRESHOLD_BYTES, CONSUMER_GROUP, RUST_LOG

## Architecture

### Backend Modules
```
src/
├── main.rs              — Server bootstrap, TopicManager, AppState, routes, 3 background tasks
├── config.rs            — Args (clap) + AppConfig (env var fallback)
├── error.rs             — TrackerError (thiserror) with actix ResponseError impl
├── models.rs            — MessageSizeEvent, TopicStats, Alert, AlertConfig, HistoryDataPoint, WsMessage, CapturedMessage
├── kafka/
│   ├── admin.rs         — BaseConsumer for metadata (list_topics)
│   └── consumer.rs      — StreamConsumer per topic, extracts payload.len() + captures raw bytes for oversized msgs
├── tracker/
│   ├── stats.rs         — StatsEngine: running avg/min/max, reservoir sampling (Algorithm R, 1000 samples)
│   └── alerts.rs        — AlertEngine: per-topic thresholds, circular buffer (1000 alerts), capture store (100 max)
├── storage.rs           — TimeSeriesStorage: per-minute aggregates, circular buffer (7 days = 10080 entries)
├── handlers/
│   ├── topics.rs        — GET /api/topics
│   ├── stats.rs         — GET /api/stats/{topic}
│   ├── alerts.rs        — GET /api/alerts, GET /api/alerts/download/{id}, GET/POST /api/alerts/config
│   ├── history.rs       — GET /api/history/{topic}?range=1h|6h|24h|7d
│   ├── broker.rs        — GET/POST /api/broker
│   └── ws.rs            — WebSocket /ws/live (rate-limited 50 events/sec, topic filtering)
└── frontend/
    ├── mod.rs           — include_str!("index.html")
    └── index.html       — Terminal/chalk themed SPA
```

### Key Patterns (from KafkaDashboard)
- **TopicManager**: `Arc<RwLock<HashMap<String, broadcast::Sender<MessageSizeEvent>>>>`, lazy consumer spawn
- **SharedAdminConsumer**: `Arc<Mutex<BaseConsumer>>` with `web::block` for metadata ops
- **WebSocket**: `actix_ws::Message` + `tokio::select!` multiplexing (broadcast rx + client msgs + heartbeat)
- **Feature flags**: `static-kafka` (cmake-build, default) / `dynamic-kafka` (dynamic-linking)

### Background Tasks (3)
1. **Stats broadcaster** (1s): reads all stats → broadcasts WsMessage::StatsUpdate
2. **Minute aggregator** (60s): snapshots stats → appends to TimeSeriesStorage
3. **Topic auto-discovery** (30s): lists topics from admin consumer → subscribes new ones

### Data Flow
```
Kafka Topic → StreamConsumer → payload.len() + optional raw capture (if >= warning threshold)
  → broadcast::Sender<MessageSizeEvent>
  → Event Processor Task:
      → StatsEngine.record_event() (running stats + reservoir)
      → AlertEngine.check_event() (threshold check → WsMessage::Alert)
      → If alert + captured payload → store CapturedMessage, set capture_id on Alert
      → WsMessage::SizeEvent → ws_broadcast → WebSocket clients (captured_payload is #[serde(skip)])
```

## API Routes
| Route | Method | Purpose |
|-------|--------|---------|
| `/` | GET | Serve embedded HTML |
| `/api/topics` | GET | List topics + summary stats + alert status |
| `/api/stats/{topic}` | GET | Detailed TopicStats (count, avg, min, max, p50/p95/p99, rates) |
| `/api/alerts` | GET | Recent alerts (query: ?limit=N) |
| `/api/alerts/download/{id}` | GET | Download captured oversized message payload (.bin) |
| `/api/alerts/config` | GET | All alert threshold configs |
| `/api/alerts/config` | POST | Set threshold for a topic |
| `/api/history/{topic}` | GET | Time-series data (query: ?range=1h\|6h\|24h\|7d) |
| `/api/broker` | GET | Current broker address |
| `/api/broker` | POST | Update broker (hot-swap, clears consumers) |
| `/ws/live` | GET | WebSocket stream (SizeEvent + StatsUpdate + Alert) |

## Frontend (Terminal/Chalk Theme)
- **CSS variables**: --bg:#0d1117, --surface:#161b22, --green:#50fa7b, --red:#ff5555, --yellow:#f1fa8c, --cyan:#8be9fd, --magenta:#ff79c6
- **Font**: JetBrains Mono / Fira Code / monospace
- **Effects**: scanline overlay, CRT text glow (text-shadow), blinking cursor
- **Layout**: Header → Topic Grid → 4-panel (Live Feed | Stats | Alerts | History Chart) → Status Bar
- **Chart**: Lightweight Canvas-based line chart (no external lib), auto-scaling, cyan line + fill
- **WebSocket**: auto-reconnect with exponential backoff

## Tests (40 total)
- `config::tests` (2): default values, args override
- `models::tests` (6): WsMessage serialization, AlertSeverity serde, AlertConfigRequest deser, AlertStatus ser, HistoryDataPoint roundtrip
- `handlers::history::tests` (1): parse_range
- `tracker::stats::tests` (13): empty stats, single event, running average, min/max, percentiles (symmetric), reservoir bounds, multi-topic, get_all, snapshot/reset, empty snapshot, clear, percentiles empty/single
- `tracker::alerts::tests` (11): no alert, warning, critical, circular buffer, custom threshold, disabled, defaults, ID increment, capture stored, capture eviction, capture missing returns none
- `storage::tests` (7): append/query, range filtering, circular eviction, empty topic, multi-topic, get_topics, clear

## Docker
- **Dockerfile**: 2-stage (rust:1.88-bookworm → debian:bookworm-slim)
- **docker-compose.yml**: App only (KAFKA_BROKERS env var)
- **docker-compose.full.yml**: Apache Kafka 3.7.0 (KRaft) + App + seed producer
- **scripts/seed-sizes.sh**: 4 topics (orders/users/logs/metrics), piped stdin approach, alert simulation every 10th/15th/25th iteration (1.2MB/12MB/2MB)

## Dependencies
actix-web 4, actix-ws 0.3, actix-rt 2, actix-cors 0.7, rdkafka 0.37, tokio 1, serde 1, serde_json 1, chrono 0.4, env_logger 0.11, log 0.4, clap 4, thiserror 2, rand 0.8, futures-util 0.3
