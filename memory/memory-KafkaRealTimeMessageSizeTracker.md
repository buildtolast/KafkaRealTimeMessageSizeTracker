# KafkaRealTimeMessageSizeTracker

## Overview
- **Location**: ~/GIT/KafkaRealTimeMessageSizeTracker
- **Purpose**: Real-time Kafka message size monitoring dashboard with alerts, analytics, and MongoDB persistence
- **Stack**: Rust (actix-web 4, rdkafka 0.37, mongodb 3, tokio 1) + embedded single-file HTML frontend
- **Edition**: 2024
- **Binary name**: `kafka-message-size-tracker`

## Running
- Dev: `cargo run -- --brokers localhost:9092 --port 5000`
- Full stack Docker: `docker compose -f docker-compose.full.yml up -d`
- App-only Docker: `KAFKA_BROKERS=host:port docker compose up -d`
- Dashboard URL: http://localhost:5000, Kafka external: localhost:9094

## CLI Arguments (clap)
- `--brokers` (default: localhost:9092) — Kafka broker addresses
- `--port` (default: 5000) — Server port
- `--host` (default: 127.0.0.1) — Server host
- `--threshold` (default: 1048576 = 1MB) — Warning alert threshold
- `--critical-threshold` (default: 10485760 = 10MB) — Critical alert threshold
- `--mongo-uri` (optional) — MongoDB connection URI (enables persistence)

## Environment Variable Overrides
KAFKA_BROKERS, SERVER_HOST, SERVER_PORT, ALERT_THRESHOLD_BYTES, CRITICAL_THRESHOLD_BYTES, CONSUMER_GROUP, MONGO_URI, RUST_LOG

## Architecture

### Backend Modules
```
src/
├── main.rs              — Server bootstrap, TopicManager, AppState, SharedMongoStore, MongoDB init, routes, 3 background tasks, mongo_store passed to TopicManager, MongoDB persist in event pipeline
├── config.rs            — Args (clap) + AppConfig (env var fallback), --mongo-uri / MONGO_URI
├── error.rs             — TrackerError (thiserror) with actix ResponseError impl
├── models.rs            — MessageSizeEvent, TopicStats, Alert, AlertConfig, HistoryDataPoint, WsMessage, CapturedMessage, HistoricalMessage, BrowseMessagesResponse, TopicSnapshot, GlobalSnapshotResponse, TopicToggleRequest, AvailableTopicsResponse, BrokerInfo, BrokerDiscoveryResponse, PartitionLag, TopicThroughput, ThroughputRecommendation, ClusterInsightsResponse, BrowseQueryParams (with preset: Option<String>), AlertDefaultsResponse, AlertDefaultsRequest
├── mongo.rs             — MongoStore with two collections (oversized_messages + alerts), PersistedMessage/PersistedAlert models, persist + query methods
├── kafka/
│   ├── admin.rs         — BaseConsumer for metadata (list_topics), discover_brokers(), fetch_consumer_lag(), build_cluster_insights()
│   ├── consumer.rs      — StreamConsumer per topic, extracts payload.len() + captures raw bytes dynamically (reads threshold from AlertEngine per message)
│   └── browser.rs       — Historical message browsing: seek by offset/timestamp, browse_from_beginning(), browse_latest(), short-lived BaseConsumer per request
├── tracker/
│   ├── stats.rs         — StatsEngine: running avg/min/max, reservoir sampling (Algorithm R, 1000 samples)
│   └── alerts.rs        — AlertEngine: per-topic thresholds, circular buffer (1000 alerts), capture store (100 max), runtime-mutable defaults (Arc<RwLock>), get_capture_threshold() for dynamic consumer capture
├── storage.rs           — TimeSeriesStorage: per-minute aggregates, circular buffer (7 days = 10080 entries), snapshot_at() for time-travel
├── handlers/
│   ├── topics.rs        — GET /api/topics, GET /api/topics/available, POST /api/topics/toggle
│   ├── stats.rs         — GET /api/stats/{topic}
│   ├── alerts.rs        — GET /api/alerts, GET /api/alerts/download/{id}, GET/POST /api/alerts/config
│   ├── history.rs       — GET /api/history/{topic}?range=1h|6h|24h|7d
│   ├── broker.rs        — GET/POST /api/broker, GET /api/broker/discover, GET /api/cluster/insights
│   ├── browse.rs        — GET /api/browse/{topic} (with ?preset=beginning|1h|1d|latest), GET /api/browse/{topic}/download/{partition}/{offset}, GET /api/snapshot
│   ├── mongo.rs         — GET /api/persisted/messages, GET /api/persisted/alerts, GET /api/persisted/status
│   └── ws.rs            — WebSocket /ws/live (rate-limited 50 events/sec, topic filtering)
└── frontend/
    ├── mod.rs           — include_str!("index.html")
    └── index.html       — ~2400-line SPA with 3 themes (Terminal/Chalk, Tokyo Night, Dracula)
```

### Key Patterns (from KafkaDashboard)
- **TopicManager**: `Arc<RwLock<HashMap<String, broadcast::Sender<MessageSizeEvent>>>>`, lazy consumer spawn
- **SharedAdminConsumer**: `Arc<Mutex<BaseConsumer>>` with `web::block` for metadata ops
- **SharedMongoStore**: `Option<Arc<MongoStore>>` — initialized if `--mongo-uri` is provided
- **WebSocket**: `actix_ws::Message` + `tokio::select!` multiplexing (broadcast rx + client msgs + heartbeat)
- **Feature flags**: `static-kafka` (cmake-build, default) / `dynamic-kafka` (dynamic-linking)

### Background Tasks (3)
1. **Stats broadcaster** (1s): reads all stats -> broadcasts WsMessage::StatsUpdate
2. **Minute aggregator** (60s): snapshots stats -> appends to TimeSeriesStorage
3. **Topic auto-discovery** (30s): lists topics from admin consumer -> subscribes new ones

### Data Flow
```
Kafka Topic -> StreamConsumer -> payload.len() + optional raw capture (if >= warning threshold)
  -> broadcast::Sender<MessageSizeEvent>
  -> Event Processor Task:
      -> StatsEngine.record_event() (running stats + reservoir)
      -> AlertEngine.check_event() (threshold check -> WsMessage::Alert)
      -> If alert + captured payload -> store CapturedMessage, set capture_id on Alert
      -> If MongoStore available -> persist oversized message + alert to MongoDB
      -> WsMessage::SizeEvent -> ws_broadcast -> WebSocket clients (captured_payload is #[serde(skip)])
```

## API Routes
| Route | Method | Purpose |
|-------|--------|---------|
| `/` | GET | Serve embedded HTML |
| `/api/topics` | GET | List topics + summary stats + alert status |
| `/api/topics/available` | GET | List all available Kafka topics for selection |
| `/api/topics/toggle` | POST | Enable/disable topic monitoring |
| `/api/stats/{topic}` | GET | Detailed TopicStats (count, avg, min, max, p50/p95/p99, rates) |
| `/api/alerts` | GET | Recent alerts (query: ?limit=N) |
| `/api/alerts/download/{id}` | GET | Download captured oversized message payload (.bin) |
| `/api/alerts/config` | GET | All alert threshold configs |
| `/api/alerts/config` | POST | Set threshold for a topic |
| `/api/alerts/defaults` | GET | Get global default thresholds |
| `/api/alerts/defaults` | POST | Update global default thresholds at runtime |
| `/api/history/{topic}` | GET | Time-series data (query: ?range=1h\|6h\|24h\|7d) |
| `/api/broker` | GET | Current broker address |
| `/api/broker` | POST | Update broker (hot-swap, clears consumers) |
| `/api/broker/discover` | GET | Discover brokers via fetch_metadata().brokers() |
| `/api/browse/{topic}` | GET | Browse messages by offset/timestamp/preset (query: ?partition, ?offset, ?timestamp, ?limit, ?preset=beginning\|1h\|1d\|latest) |
| `/api/browse/{topic}/download/{partition}/{offset}` | GET | Download full message payload at specific offset |
| `/api/snapshot` | GET | Global dashboard snapshot at timestamp (query: ?timestamp) |
| `/api/cluster/insights` | GET | Cluster insights: brokers, topics, partitions, consumer lag, throughput, recommendations |
| `/api/persisted/messages` | GET | Query persisted oversized messages from MongoDB |
| `/api/persisted/alerts` | GET | Query persisted alerts from MongoDB |
| `/api/persisted/status` | GET | MongoDB persistence status (connected, collection counts) |
| `/ws/live` | GET | WebSocket stream (SizeEvent + StatsUpdate + Alert) |

## Frontend
- **3 themes**: Terminal/Chalk (default), Tokyo Night, Dracula — via CSS `[data-theme="..."]` attribute selectors, theme switcher in header, localStorage persistence
- **CSS variables**: --bg:#0d1117, --surface:#161b22, --green:#50fa7b, --red:#ff5555, --yellow:#f1fa8c, --cyan:#8be9fd, --magenta:#ff79c6
- **Font**: JetBrains Mono / Fira Code / monospace
- **Effects**: scanline overlay, CRT text glow (text-shadow), blinking cursor
- **Layout**: Resizable panels — vertical sidebar (default 280px, drag-to-resize) + 2 rows of 2 panels + global panel. All dividers (sidebar/columns/rows) are draggable via `.resize-handle-v`/`.resize-handle-h` with cyan highlight on hover. Structure: `.panels-area` (flex-row) > sidebar + resize-handle + `.panels-wrap` (flex-column) > `.panel-row` containers with column resize handles between left/right panels
- **Topic selection**: Startup picker overlay, localStorage persistence for selected topics
- **Topic sorting**: Sidebar topics sorted by avg_bytes descending (largest first), red blinking border (`priority-alert`) for topics exceeding per-topic or global alert thresholds
- **Enhanced alerts**: Time window filter (5m/15m/1h/all), shows all topics
- **Broker discovery**: Popover UI with click-to-connect
- **Cluster insights**: Full-screen overlay with tables, lag bars, recommendation cards, auto-refresh every 5s while open (real-time consumer lag)
- **History Browser**: Full-screen overlay with 3 tabs (messages browse + global snapshot + persisted data), message detail sidebar, pagination
- **Historical play presets**: `?preset=beginning|1h|1d|latest` param for quick browsing
- **Chart**: Lightweight Canvas-based line chart (no external lib), auto-scaling, cyan line (no area fill)
- **Chart type selector**: All mini-charts have a line/area/bar toggle (`.chart-type-toggle`). State: `feedChartTypeMode` and `alertChartTypeMode` (default: 'line')
- **Live feed mini-chart**: Dual-axis timeseries (avg size cyan line + msg/s magenta dashed line), per-second rolling buffer (120 pts), crosshair hover with tooltip. Switchable modes: live | 1h | 6h | 24h | 7d — historical mode shows multi-topic colored lines from `/api/history/{topic}?range=...`
- **Alerts mini-chart**: Dual-line chart (warnings yellow + criticals red), per-10-second buckets, crosshair hover with tooltip. Switchable modes: live | 1h | 6h | all — historical mode aggregates `state.alerts` into time buckets from `/api/alerts?limit=1000`. Bar mode available via chart type toggle
- **Global history chart**: Dual-axis per topic — left Y: avg_bytes (solid colored lines), right Y: message_count (dashed colored lines). Crosshair hover with per-topic tooltip sorted by avg_bytes descending. Legend indicates solid=size, dashed=count
- **WebSocket**: auto-reconnect with exponential backoff

## MongoDB Persistence
- **Crate**: `mongodb = "3"`
- **Module**: `src/mongo.rs` — MongoStore struct with two collections
- **Collections**: `oversized_messages` (PersistedMessage) + `alerts` (PersistedAlert)
- **Activation**: `--mongo-uri` CLI arg or `MONGO_URI` env var (optional; disabled if not provided)
- **Pipeline integration**: Oversized messages and alerts auto-persisted in event processor
- **Endpoints**: `/api/persisted/messages`, `/api/persisted/alerts`, `/api/persisted/status`
- **Docker**: mongo:7 container with volume in docker-compose.full.yml

## Tests (70 total)
- `config::tests` (2): default values, args override
- `models::tests` (20): WsMessage serialization, AlertSeverity serde, AlertConfigRequest deser, AlertStatus ser, HistoryDataPoint roundtrip, HistoricalMessage ser, BrowseMessagesResponse ser, GlobalSnapshotResponse ser, MessagePayload ser, TopicToggleRequest, AvailableTopicsResponse, BrokerInfo, BrokerDiscoveryResponse, ClusterInsightsResponse, BrowseQueryParams with preset, PartitionLag, TopicThroughput, AlertDefaultsResponse ser, AlertDefaultsRequest deser
- `handlers::history::tests` (1): parse_range
- `kafka::browser::tests` (5): encode_payload_utf8, encode_payload_binary, encode_payload_truncation, encode_payload_no_truncation, uuid_simple_unique
- `tracker::stats::tests` (13): empty stats, single event, running average, min/max, percentiles (symmetric), reservoir bounds, multi-topic, get_all, snapshot/reset, empty snapshot, clear, percentiles empty/single
- `tracker::alerts::tests` (14): no alert, warning, critical, circular buffer, custom threshold, disabled, defaults, ID increment, capture stored, capture eviction, capture missing returns none, set_defaults_changes_runtime, get_capture_threshold_per_topic, get_capture_threshold_after_set_defaults
- `storage::tests` (11): append/query, range filtering, circular eviction, empty topic, multi-topic, get_topics, clear, snapshot_at_exact_match, snapshot_at_finds_closest, snapshot_at_multiple_topics, snapshot_at_no_data
- `mongo::tests` (4): PersistedMessage ser/deser, PersistedAlert ser/deser

## Docker
- **Dockerfile**: 2-stage (rust:1.88-bookworm -> debian:bookworm-slim)
- **docker-compose.yml**: App only (KAFKA_BROKERS env var)
- **docker-compose.full.yml**: Apache Kafka 3.7.0 (KRaft) + App + seed producer + MongoDB 7 (with volume), MONGO_URI env var for tracker
- **scripts/seed-sizes.sh**: 4 topics (orders/users/logs/metrics), piped stdin approach, alert simulation every 10th/15th/25th iteration (1.2MB/12MB/2MB)

## Dependencies
actix-web 4, actix-ws 0.3, actix-rt 2, actix-cors 0.7, rdkafka 0.37, mongodb 3, tokio 1, serde 1, serde_json 1, chrono 0.4, env_logger 0.11, log 0.4, clap 4, thiserror 2, rand 0.8, futures-util 0.3, base64 0.22
