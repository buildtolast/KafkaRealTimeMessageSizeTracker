# KafkaRealTimeMessageSizeTracker

## Overview
- **Location**: ~/GIT/KafkaRealTimeMessageSizeTracker
- **Purpose**: Real-time Kafka message size monitoring dashboard with alerts, analytics, and MongoDB persistence
- **Stack**: Rust (actix-web 4, rdkafka 0.37, mongodb 3, tokio 1) + embedded single-file HTML frontend
- **Edition**: 2024
- **Binary name**: `kafka-message-size-tracker`

## Running
- Dev: `cargo run -- --brokers localhost:9092 --port 5688`
- Full stack Docker: `docker compose -f docker-compose.full.yml up -d`
- App-only Docker: `KAFKA_BROKERS=host:port docker compose up -d`
- Dashboard URL: http://localhost:5688, Kafka external: localhost:9094

## CLI Arguments (clap)
- `--brokers` (default: localhost:9092) — Kafka broker addresses
- `--port` (default: 5688) — Server port
- `--host` (default: 127.0.0.1) — Server host
- `--threshold` (default: 1048576 = 1MB) — Warning alert threshold
- `--critical-threshold` (default: 10485760 = 10MB) — Critical alert threshold
- `--mongo-uri` (optional) — MongoDB connection URI (enables persistence)
- `--security-protocol` (optional) — Kafka security protocol (SASL_SSL, SASL_PLAINTEXT, SSL)
- `--sasl-mechanism` (optional) — SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
- `--sasl-username` (optional) — SASL username / API Key
- `--sasl-password` (optional) — SASL password / API Secret

## Environment Variable Overrides
KAFKA_BROKERS, SERVER_HOST, SERVER_PORT, ALERT_THRESHOLD_BYTES, CRITICAL_THRESHOLD_BYTES, CONSUMER_GROUP, MONGO_URI, KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_MECHANISM, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD, RUST_LOG

## Architecture

### Backend Modules
```
src/
├── main.rs              — Server bootstrap, TopicManager (with StartPosition support), AppState, SharedMongoStore, MongoDB init, routes, 4 background tasks, TopicManager in app_data
├── config.rs            — Args (clap) + AppConfig (env var fallback), SecurityConfig (SASL/SSL auth), SharedSecurityConfig, --mongo-uri / MONGO_URI
├── error.rs             — TrackerError (thiserror) with actix ResponseError impl
├── models.rs            — MessageSizeEvent, TopicStats, Alert, AlertConfig, HistoryDataPoint, WsMessage, CapturedMessage, HistoricalMessage, BrowseMessagesResponse, TopicSnapshot, GlobalSnapshotResponse, TopicToggleRequest (with start_from), StartPosition enum, AvailableTopicsResponse, BrokerInfo, BrokerDiscoveryResponse, BrokerRequest (with auth fields), BrokerResponse (with auth_configured), PartitionLag, TopicThroughput, ThroughputRecommendation, ClusterInsightsResponse (with broker_metrics), BrokerMetrics, LatencyWindow, BrowseQueryParams, AlertDefaultsResponse, AlertDefaultsRequest, PartitionProgress, ConsumerProgress, ConsumerProgressResponse, ProgressDataPoint, TopicConsumed
├── mongo.rs             — MongoStore with two collections (oversized_messages + alerts), PersistedMessage/PersistedAlert models, persist + query methods
├── kafka/
│   ├── mod.rs           — apply_security() + apply_timeouts() helpers: applies SASL/SSL config and connection timeouts (socket 10s, setup 5s, request 10s) to any ClientConfig
│   ├── admin.rs         — AdminConsumer (BaseConsumer<StatsContext>) for metadata (list_topics), discover_brokers(), fetch_consumer_lag()/fetch_consumer_lag_for_topics(scoped), build_cluster_insights(cached_lag), extract_broker_metrics(); SharedKafkaStats for rdkafka statistics callback; accepts SecurityConfig
│   ├── consumer.rs      — StreamConsumer per topic, accepts StartPosition (latest/beginning/timestamp/offset) + SecurityConfig, extracts payload.len() + captures raw bytes dynamically
│   └── browser.rs       — Historical message browsing: seek by offset/timestamp, browse_from_beginning(), browse_latest(), short-lived BaseConsumer per request; all 5 functions accept SecurityConfig
├── tracker/
│   ├── stats.rs         — StatsEngine: running avg/min/max, reservoir sampling (Algorithm R, 1000 samples)
│   ├── alerts.rs        — AlertEngine: per-topic thresholds, circular buffer (1000 alerts), capture store (100 max), runtime-mutable defaults (Arc<RwLock>), get_capture_threshold() for dynamic consumer capture
│   └── progress.rs      — ProgressTracker: per-topic/partition consumer offset tracking, progress percentage calculation (merged with watermarks), time-series snapshots (10s intervals, 12h cap), cached watermarks for instant API responses (no Kafka calls)
├── storage.rs           — TimeSeriesStorage: per-minute aggregates, circular buffer (7 days = 10080 entries), snapshot_at() for time-travel
├── handlers/
│   ├── topics.rs        — GET /api/topics, GET /api/topics/available, POST /api/topics/toggle (with TopicManager + StartPosition)
│   ├── stats.rs         — GET /api/stats/{topic}
│   ├── alerts.rs        — GET /api/alerts, GET /api/alerts/download/{id}, GET/POST /api/alerts/config
│   ├── history.rs       — GET /api/history/{topic}?range=1h|6h|24h|7d
│   ├── broker.rs        — GET/POST /api/broker, GET /api/broker/discover, GET /api/cluster/insights
│   ├── browse.rs        — GET /api/browse/{topic} (with ?preset=beginning|1h|1d|latest), GET /api/browse/{topic}/download/{partition}/{offset}, GET /api/snapshot
│   ├── progress.rs      — GET /api/consumer/progress (cached, no Kafka call), GET /api/consumer/progress/timeseries
│   ├── metrics.rs       — GET /api/metrics/process (CPU%, RSS memory, virtual memory, uptime via sysinfo crate)
│   ├── mongo.rs         — GET /api/persisted/messages, GET /api/persisted/alerts, GET /api/persisted/status
│   └── ws.rs            — WebSocket /ws/live (rate-limited 50 events/sec, topic filtering)
└── frontend/
    ├── mod.rs           — include_str!("index.html")
    └── index.html       — ~2900-line SPA with 4 themes (Terminal/Chalk, Tokyo Night, Dracula, Light)
```

### Key Patterns (from KafkaDashboard)
- **TopicManager**: `Arc<RwLock<HashMap<String, TopicConsumer>>>` (TopicConsumer = tx + JoinHandle), lazy consumer spawn, subscribe() accepts StartPosition, re-creates consumer (abort + respawn) when non-None start_position is provided for existing topic. Has `SharedSecurityConfig` for auth, `ProgressTracker` for consumer progress
- **SharedSecurityConfig**: `Arc<RwLock<SecurityConfig>>` — runtime-mutable via web UI or CLI/env vars; read by TopicManager on subscribe(), passed to all consumer/browser ClientConfig creation points (8 total)
- **SharedAdminConsumer**: `Arc<Mutex<AdminConsumer>>` with `web::block` for metadata ops; AdminConsumer = `BaseConsumer<StatsContext>` with `statistics.interval.ms=5000`
- **SharedMongoStore**: `Option<Arc<MongoStore>>` — initialized if `--mongo-uri` is provided
- **WebSocket**: `actix_ws::Message` + `tokio::select!` multiplexing (broadcast rx + client msgs + heartbeat)
- **Feature flags**: `static-kafka` (cmake-build + ssl, default) / `dynamic-kafka` (dynamic-linking + ssl)

### Background Tasks (5)
1. **Stats broadcaster** (1s): reads all stats -> broadcasts WsMessage::StatsUpdate
2. **Minute aggregator** (60s): snapshots stats -> appends to TimeSeriesStorage
3. **Topic auto-discovery** (60s): lists topics from admin consumer -> subscribes new ones
4. **Lag collector** (30s): fetch_consumer_lag_for_topics (scoped to tracked) -> LagTimeSeries + ProgressTracker.snapshot() (caches watermarks)
5. **Admin consumer poller** (1s): polls AdminConsumer for rdkafka statistics callbacks

### Data Flow
```
Kafka Topic -> StreamConsumer (start position: latest/beginning/timestamp/offset)
  -> payload.len() + optional raw capture (if >= warning threshold)
  -> broadcast::Sender<MessageSizeEvent>
  -> Event Processor Task:
      -> StatsEngine.record_event() (running stats + reservoir)
      -> ProgressTracker.record_offset() (per-partition offset tracking)
      -> AlertEngine.check_event() (threshold check -> WsMessage::Alert)
      -> If alert + captured payload -> store CapturedMessage, set capture_id on Alert
      -> If MongoStore available -> persist oversized message + alert to MongoDB
      -> WsMessage::SizeEvent -> ws_broadcast -> WebSocket clients (captured_payload is #[serde(skip)])
```

### Confluent Cloud / High-Latency Optimizations
- **apply_timeouts()**: socket 10s, connection setup 5s, request 10s, metadata max age 60s (applied to all 8 ClientConfig creation points)
- **Scoped lag fetching**: `fetch_consumer_lag_for_topics()` fetches metadata per-topic instead of all-topics (24 sequential watermark calls → only tracked)
- **Cached watermarks**: Background lag collector caches watermarks in ProgressTracker; consumer progress API serves from cache (zero Kafka calls)
- **Cached cluster metadata**: Background lag collector also caches `CachedClusterMetadata` (brokers, topic/partition counts); cluster insights API serves 100% from cache (no AdminConsumer mutex, no Kafka calls, no `web::block`)
- **Throttled polling**: auto-discovery 60s, lag collector 30s, frontend overlays 15s, global history 60s
- **Topic filtering**: `GET /api/topics` returns only tracked topics from in-memory HashSet (no Kafka metadata fetch when topics selected); WebSocket handlers filter untracked events

## API Routes
| Route | Method | Purpose |
|-------|--------|---------|
| `/` | GET | Serve embedded HTML |
| `/api/topics` | GET | List topics + summary stats + alert status |
| `/api/topics/available` | GET | List all available Kafka topics for selection |
| `/api/topics/toggle` | POST | Enable/disable topic monitoring (with optional `start_from` position) |
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
| `/api/cluster/lag-history` | GET | Consumer lag time-series data (10-sec intervals, 1 hour) |
| `/api/consumer/progress` | GET | Consumer progress per topic/partition (with watermark-based progress %) |
| `/api/consumer/progress/timeseries` | GET | Consumer progress time-series snapshots (10s intervals, 12h) |
| `/api/persisted/messages` | GET | Query persisted oversized messages from MongoDB |
| `/api/persisted/alerts` | GET | Query persisted alerts from MongoDB |
| `/api/persisted/status` | GET | MongoDB persistence status (connected, collection counts) |
| `/api/metrics/process` | GET | Process metrics (CPU%, RSS memory, virtual memory, uptime) |
| `/ws/live` | GET | WebSocket stream (SizeEvent + StatsUpdate + Alert) |

## Frontend
- **4 themes**: Terminal/Chalk (default), Tokyo Night, Dracula, Light — via CSS `[data-theme="..."]` attribute selectors, theme switcher in header, localStorage persistence
- **CSS variables**: --bg:#0d1117, --surface:#161b22, --green:#50fa7b, --red:#ff5555, --yellow:#f1fa8c, --cyan:#8be9fd, --magenta:#ff79c6
- **Font**: JetBrains Mono / Fira Code / monospace
- **Effects**: scanline overlay (disabled in light theme), CRT text glow (text-shadow), blinking cursor
- **Layout**: Resizable panels — vertical sidebar (default 280px, drag-to-resize) + 2 rows of 2 panels + global panel. All dividers (sidebar/columns/rows) are draggable via `.resize-handle-v`/`.resize-handle-h` with cyan highlight on hover
- **App subtitle**: "Real-time message size monitoring & alerting" shown below app name in header
- **Topic selection**: Startup picker overlay with "Start From" position selector (Latest/Beginning/Timestamp/Offset), localStorage persistence for selected topics. Re-openable mid-session via `[topics]` header button — full dashboard reset (backend + frontend) on re-selection
- **Broker connect/disconnect**: Connect button switches to red disconnect button when connected. Disconnect stops all consumers, clears tracked topics, resets dashboard, prevents WebSocket auto-reconnect. Endpoints: `POST /api/reset` (clear all), `POST /api/disconnect` (stop consumers only)
- **Auth UI**: `[auth]` toggle button next to broker input, collapsible auth section with Security Protocol dropdown (None/SASL_SSL/SASL_PLAINTEXT/SSL), SASL Mechanism dropdown (PLAIN/SCRAM-SHA-256/SCRAM-SHA-512), API Key text input, API Secret password input. Green indicator when auth is configured. Auth fields sent with broker POST, populated from GET on init
- **Topic sorting**: Sidebar topics sorted by recent avg descending (rate-window based), 4-state lifecycle: Critical → Warning → Recovered (30s timer) → OK, color-coded borders: green (ok), yellow (warning), red blinking step-end (critical/`priority-alert`), cyan pulse (recovered)
- **Topic names**: Status-based colors — red (critical), yellow (warning), cyan (recovered), green (healthy/ok)
- **Enhanced alerts**: Time window filter (5m/15m/1h/all), shows all topics
- **Broker discovery**: Popover UI with click-to-connect
- **Cluster insights**: Full-screen overlay with tables, lag bars (top panel), recommendation cards, auto-refresh every 5s while open (real-time consumer lag), lag time-series chart (per-topic + total, crosshair hover), broker performance metrics (per-broker cards with state/TX/RX/RTT/errors/throttle/retries/in-flight via rdkafka statistics callback)
- **Health report**: Full-screen overlay with per-topic health cards, plain-English abnormality descriptions (threshold breaches, spike detection, p99 analysis, spread analysis, alert activity), grouped alert summary + individual alert messages table (most recent 20 with download links), auto-refresh every 5s (live) or 30s (historical), severity-sorted (critical first), time-window selection (live/1h/6h/24h/7d) for historical analysis using per-minute aggregate data
- **Consumer progress**: Full-screen overlay showing per-topic consumer progress with visual progress bars (color-coded: red→yellow→cyan→green), per-partition detail tables, offset tracking (first/current/log-end), time-series chart of consumption over time, auto-refresh every 5s
- **History Browser**: Full-screen overlay with 3 tabs (messages browse + global snapshot + persisted data), message detail sidebar, pagination
- **Historical play presets**: `?preset=beginning|1h|1d|latest` param for quick browsing
- **Chart**: Lightweight Canvas-based line chart (no external lib), auto-scaling, cyan line (no area fill)
- **Chart type selector**: All mini-charts have a line/area/bar toggle (`.chart-type-toggle`). State: `feedChartTypeMode` and `alertChartTypeMode` (default: 'line')
- **Live feed mini-chart**: Dual-axis timeseries (avg size cyan line + msg/s magenta dashed line), per-second rolling buffer (120 pts), crosshair hover with tooltip. Switchable modes: live | 1h | 6h | 24h | 7d
- **Alerts mini-chart**: Dual-line chart (warnings yellow + criticals red), per-10-second buckets, crosshair hover. Switchable modes: live | 1h | 6h | all
- **Global history chart**: Dual-axis per topic — left Y: avg_bytes (solid colored lines), right Y: message_count (dashed colored lines). Crosshair hover with per-topic tooltip
- **Process metrics widget**: Header-embedded mini widget showing real-time CPU% (with sparkline), RSS memory (with sparkline), and uptime. Polls `/api/metrics/process` every 3s. Color-coded CPU (green <50%, yellow 50-80%, red >80%). Hover tooltip shows peak CPU/memory and virtual memory
- **What's Happening overlay**: Real-time plain-English system narrative (last 30s client-side buffer), with time range selection (30s/1h/6h/12h) using historical REST API data, performance advisory tips about CPU-intensive overlays
- **WebSocket**: auto-reconnect with exponential backoff

## MongoDB Persistence
- **Crate**: `mongodb = "3"`
- **Module**: `src/mongo.rs` — MongoStore struct with two collections
- **Collections**: `oversized_messages` (PersistedMessage) + `alerts` (PersistedAlert)
- **Activation**: `--mongo-uri` CLI arg or `MONGO_URI` env var (optional; disabled if not provided)
- **Pipeline integration**: Oversized messages and alerts auto-persisted in event processor
- **Endpoints**: `/api/persisted/messages`, `/api/persisted/alerts`, `/api/persisted/status`
- **Docker**: mongo:7 container with volume in docker-compose.full.yml

## Tests (97 total)
- `config::tests` (6): default values, args override, security_config_not_configured, security_config_configured, security_config_from_args, security_config_serde_roundtrip
- `models::tests` (32): WsMessage serialization, AlertSeverity serde, AlertConfigRequest deser, AlertStatus ser, HistoryDataPoint roundtrip, HistoricalMessage ser, BrowseMessagesResponse ser, GlobalSnapshotResponse ser, MessagePayload ser, TopicToggleRequest, AvailableTopicsResponse, BrokerInfo, BrokerDiscoveryResponse, ClusterInsightsResponse, ClusterInsightsResponse with broker_metrics, BrokerMetrics ser, LatencyWindow ser, BrowseQueryParams with preset, PartitionLag, TopicThroughput, AlertDefaultsResponse ser, AlertDefaultsRequest deser, StartPosition latest/beginning/timestamp/offset serde, TopicToggleRequest with/without start_from, PartitionProgress roundtrip, ConsumerProgressResponse ser, ProgressDataPoint ser
- `handlers::history::tests` (1): parse_range
- `kafka::browser::tests` (5): encode_payload_utf8, encode_payload_binary, encode_payload_truncation, encode_payload_no_truncation, uuid_simple_unique
- `tracker::stats::tests` (13): empty stats, single event, running average, min/max, percentiles (symmetric), reservoir bounds, multi-topic, get_all, snapshot/reset, empty snapshot, clear, percentiles empty/single
- `tracker::alerts::tests` (14): no alert, warning, critical, circular buffer, custom threshold, disabled, defaults, ID increment, capture stored, capture eviction, capture missing returns none, set_defaults_changes_runtime, get_capture_threshold_per_topic, get_capture_threshold_after_set_defaults
- `tracker::progress::tests` (8): record_offset_basic, record_offset_multiple_partitions, record_offset_multiple_topics, get_progress_with_watermarks, get_progress_caught_up, snapshot_appends_timeseries, snapshot_empty_state_does_not_append, clear_resets_everything
- `storage::tests` (14): append/query, range filtering, circular eviction, empty topic, multi-topic, get_topics, clear, snapshot_at_exact_match, snapshot_at_finds_closest, snapshot_at_multiple_topics, snapshot_at_no_data, lag_timeseries_append_and_get, lag_timeseries_eviction, lag_timeseries_clear
- `mongo::tests` (4): PersistedMessage ser/deser, PersistedAlert ser/deser

## Docker
- **Dockerfile**: 2-stage (rust:1.88-bookworm -> debian:bookworm-slim)
- **docker-compose.yml**: App only (KAFKA_BROKERS env var)
- **docker-compose.full.yml**: Apache Kafka 3.7.0 (KRaft) + App + seed producer + MongoDB 7 (with volume), MONGO_URI env var for tracker
- **scripts/seed-sizes.sh**: 4 topics (orders/users/logs/metrics), piped stdin approach, alert simulation every 10th/15th/25th iteration (1.2MB/12MB/2MB)

## Dependencies
actix-web 4, actix-ws 0.3, actix-rt 2, actix-cors 0.7, rdkafka 0.37, mongodb 3, sysinfo 0.33, tokio 1, serde 1, serde_json 1, chrono 0.4, env_logger 0.11, log 0.4, clap 4, thiserror 2, rand 0.8, futures-util 0.3, base64 0.22
