# KafkaRealTimeMessageSizeTracker — Implementation Plan

## Context

New Rust project for real-time Kafka message size monitoring. The user needs a dashboard that tracks message payload sizes per topic, alerts on oversized messages, and provides historical size analytics. The backend reuses architectural patterns from the KafkaDashboard project (TopicManager, WebSocket streaming, rdkafka consumer). The frontend is an embedded single-file HTML SPA (like MongoAssist) with a **terminal/chalk theme** — dark background, ANSI-colored monospace text (green=success, red=alert, yellow=warning, cyan=info, magenta=accent).

**Project location:** `/Users/chiya/GIT/KafkaRealTimeMessageSizeTracker`

## File Structure

```
src/
├── main.rs              # Server bootstrap, AppState, TopicManager, routes, background tasks
├── config.rs            # AppConfig (clap CLI + env var fallback)
├── error.rs             # thiserror-based TrackerError
├── models.rs            # MessageSizeEvent, TopicStats, Alert, AlertConfig, HistoryDataPoint, WsMessage
├── kafka/
│   ├── mod.rs
│   ├── consumer.rs      # StreamConsumer per topic — extracts payload size only (not body)
│   └── admin.rs         # BaseConsumer for metadata (list topics)
├── tracker/
│   ├── mod.rs
│   ├── stats.rs         # StatsEngine: running avg/min/max, reservoir sampling for p50/p95/p99
│   └── alerts.rs        # AlertEngine: configurable thresholds, circular alert buffer (1000 max)
├── storage.rs           # TimeSeriesStorage: per-minute aggregates in circular buffer (7 days)
├── handlers/
│   ├── mod.rs
│   ├── topics.rs        # GET /api/topics
│   ├── stats.rs         # GET /api/stats/{topic}
│   ├── alerts.rs        # GET /api/alerts, GET/POST /api/alerts/config
│   ├── history.rs       # GET /api/history/{topic}?range=1h|6h|24h|7d
│   ├── broker.rs        # GET/POST /api/broker
│   └── ws.rs            # WebSocket /ws/live (real-time events + stats + alerts)
└── frontend/
    ├── mod.rs           # pub const INDEX_HTML: &str = include_str!("index.html");
    └── index.html       # Single-file terminal/chalk themed SPA (~3500 lines)

Dockerfile                # 2-stage: rust:1.88-bookworm → debian:bookworm-slim
docker-compose.yml        # App only (external Kafka)
docker-compose.full.yml   # Kafka (KRaft) + App + seed producer
scripts/seed-sizes.sh     # Variable-size message producer for testing
```

## Implementation Steps

### Step 1: Project Scaffold
- `cargo init` at project directory
- `Cargo.toml`: edition 2024, feature flags `static-kafka`/`dynamic-kafka`
- Dependencies: actix-web 4, actix-ws 0.3, actix-rt 2, actix-cors 0.7, rdkafka 0.37, tokio 1, serde 1, serde_json 1, chrono 0.4, env_logger 0.11, log 0.4, clap 4, thiserror 2, rand 0.8, futures-util 0.3

### Step 2: Config + Error (`config.rs`, `error.rs`)
- `Args` struct (clap derive): `--brokers`, `--port`, `--host`, `--threshold`, `--critical-threshold`
- `AppConfig::from_args_and_env()` — CLI args with env var overrides (KAFKA_BROKERS, SERVER_HOST, SERVER_PORT, ALERT_THRESHOLD_BYTES)
- `TrackerError` enum: Kafka, Serialization, Config, TopicNotFound

### Step 3: Models (`models.rs`)
- `MessageSizeEvent` { topic, partition, offset, size_bytes, key_size, timestamp }
- `TopicStats` { topic, message_count, total_bytes, avg/min/max, p50/p95/p99, msg_per_sec, bytes_per_sec }
- `Alert` { id, topic, severity (Warning/Critical), message_size, threshold, timestamp }
- `AlertConfig` { topic, warning_threshold, critical_threshold, enabled }
- `HistoryDataPoint` { timestamp, message_count, avg/min/max, p95, total_bytes }
- `WsMessage` (tagged enum): SizeEvent | StatsUpdate | Alert
- Request/response types: BrokerRequest, AlertConfigRequest, TopicSummary, TopicsResponse

### Step 4: Kafka Module (`kafka/`)
- `admin.rs`: `create_admin_consumer()`, `list_topics()` — direct reuse of KafkaDashboard pattern
- `consumer.rs`: `run_size_tracking_consumer()` — StreamConsumer that extracts `payload.len()` only (no body materialization), sends `MessageSizeEvent` through broadcast channel

### Step 5: Stats Engine (`tracker/stats.rs`)
- `TopicStatsAccumulator`: count, total_bytes, min, max, reservoir sample (1000 items), rate window
- **Reservoir sampling** (Algorithm R): `gen_range(0..n)` to decide replacement at index
- `record_event()` — updates running stats, inserts into reservoir
- `get_stats()` — computes percentiles by sorting reservoir, indexing at `floor(p * len)`
- `snapshot_and_reset_minute()` — produces HistoryDataPoint, resets rate counters
- Rate: 10-second sliding window, compute msg/sec and bytes/sec
- Thread-safe: `Arc<RwLock<HashMap<String, Accumulator>>>`
- **~12 unit tests**: empty stats, single event, running average, min/max, p50/p95/p99 distributions, reservoir bounds, rates, snapshot/reset, multi-topic independence

### Step 6: Alert Engine (`tracker/alerts.rs`)
- `AlertEngine` with per-topic configs (`HashMap<String, AlertConfig>`) and alert history (`VecDeque<Alert>`, max 1000)
- `check_event()` — compares size against thresholds, returns `Option<Alert>`
- `get_alerts()`, `get_config()`, `set_config()`, `get_all_configs()`
- Default thresholds: 1MB warning, 10MB critical
- **~7 unit tests**: no alert under threshold, warning/critical triggers, circular buffer eviction, custom thresholds, disabled topic

### Step 7: Time-Series Storage (`storage.rs`)
- `TimeSeriesStorage`: `HashMap<String, VecDeque<HistoryDataPoint>>`, max 10080 entries (7 days of per-minute data)
- `append()`, `query(topic, range_minutes)`, `get_topics()`
- **~5 unit tests**: append/query, circular eviction, range filtering, empty topic, multi-topic

### Step 8: API Handlers (`handlers/`)
- `topics.rs`: GET /api/topics — list topics via admin consumer + attach current stats/alert status
- `stats.rs`: GET /api/stats/{topic} — detailed TopicStats from StatsEngine
- `alerts.rs`: GET /api/alerts, GET/POST /api/alerts/config
- `history.rs`: GET /api/history/{topic}?range=1h|6h|24h|7d — from TimeSeriesStorage
- `broker.rs`: GET/POST /api/broker — reuse KafkaDashboard pattern (hot-swap, clear topic manager)

### Step 9: WebSocket Handler (`handlers/ws.rs`)
- Single endpoint `/ws/live` — subscribes to global `ws_broadcast` channel
- Client sends `{"action":"subscribe","topics":["orders"]}` to filter
- Server pushes: SizeEvent, StatsUpdate, Alert — filtered by client's subscribed topics
- `tokio::select!` pattern: broadcast rx + client msg stream + 10s heartbeat
- Downsample: max 50 events/sec to browser to prevent flooding

### Step 10: Main Application (`main.rs`)
- **AppState**: TopicManager, StatsEngine, AlertEngine, TimeSeriesStorage, ws_broadcast sender
- **TopicManager** (adapted from KafkaDashboard): `HashMap<String, broadcast::Sender<MessageSizeEvent>>`, lazy consumer spawn
- **Background tasks** (3):
  1. Event processor: receives from TopicManager → feeds StatsEngine + AlertEngine → broadcasts WsMessage
  2. Stats broadcaster: every 1s, reads all stats → broadcasts StatsUpdate
  3. Minute aggregator: every 60s, snapshots stats → appends to TimeSeriesStorage
- Route config: all API + WebSocket + `"/"` serves embedded HTML

### Step 11: Frontend (`frontend/index.html`) — Terminal/Chalk Theme

**CSS Design System (chalk/ANSI colors):**
```css
--bg: #0d1117; --surface: #161b22; --surface2: #1c2333; --border: #30363d;
--text: #f0f6fc; --text-dim: #6b7a8d;
--green: #50fa7b; --red: #ff5555; --yellow: #f1fa8c; --cyan: #8be9fd; --magenta: #ff79c6;
--font: "JetBrains Mono", "Fira Code", monospace;
```

**Terminal-style elements:** prompt headers (`> Stats_`), blinking cursor, box-drawing borders, scanline overlay, CRT text glow

**Layout (4-panel grid):**
1. **Header**: App name with terminal prompt style, connection dot, broker config
2. **Topic grid**: Cards per topic — name, msg/sec, avg size, max, alert status badge
3. **Live feed** (left): Scrolling log (`tail -f` style), color-coded by size vs threshold
4. **Stats detail** (right): Selected topic percentiles table, min/max/avg gauges
5. **Alerts panel** (bottom-left): Active/recent alerts, red/yellow color-coded
6. **History chart** (bottom-right): Canvas-based line chart (avg size over time, auto-scaling axes, `--cyan` line)

**JS architecture:** WebSocket connection with auto-reconnect, state object for topics/stats/alerts/feed, `formatBytes()` helper, lightweight Canvas chart renderer (~150 lines)

### Step 12: Docker Setup
- `Dockerfile`: 2-stage (rust:1.88-bookworm builder + debian:bookworm-slim runtime)
- `docker-compose.yml`: App only, `KAFKA_BROKERS` env var
- `docker-compose.full.yml`: Apache Kafka 3.7.0 (KRaft) + App + seed producer
- `scripts/seed-sizes.sh`: Produces messages of varied sizes (100B–2MB) to 4 topics

### Step 13: Tests
- **Unit tests** (inline): ~24 tests across stats, alerts, storage, models, config
- **Integration tests** (`tests/integration.rs`): ~11 tests for API handlers + WebSocket + HTML serving
- **Target: ~35+ tests total**

## Key Patterns Reused from KafkaDashboard
| Pattern | Source | Adaptation |
|---------|--------|------------|
| TopicManager (broadcast channels) | `main.rs` | Broadcasts `MessageSizeEvent` instead of `KafkaMessage` |
| SharedAdminConsumer | `kafka/admin.rs` | Direct reuse |
| WebSocket with `tokio::select!` | `handlers/ws.rs` | Adds topic filtering + multiplexed message types |
| Feature flags (static/dynamic kafka) | `Cargo.toml` | Direct reuse |
| Multi-stage Docker | `Dockerfile` | 2-stage instead of 3 (no frontend build) |
| Broker hot-swap | `handlers/broker.rs` | Direct reuse |

## Key Differences from KafkaDashboard
- **Embedded HTML** (MongoAssist pattern) vs separate React SPA
- **Stats engine** and **alert engine** — new domain logic
- **Time-series storage** — new in-memory circular buffer
- **Edition 2024** + clap CLI args (MongoAssist pattern)
- **Message body never materialized** — only `payload.len()` extracted for efficiency

## Verification
1. `cargo build` — compiles without errors
2. `cargo test` — all ~35 tests pass
3. `docker compose -f docker-compose.full.yml up -d` — Kafka + app + seed producer start
4. Open `http://localhost:3000` — terminal-themed dashboard loads
5. Verify: topics appear in grid, live feed scrolls with color-coded sizes, stats update in real-time
6. Set a low alert threshold (e.g., 1KB) → verify alerts appear in red
7. Check history chart renders time-series data
8. Change broker via UI → verify reconnection
9. `docker compose down` — clean shutdown
