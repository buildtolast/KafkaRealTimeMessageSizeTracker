# KafkaRealTimeMessageSizeTracker

Real-time Kafka message size monitoring dashboard with configurable alerts, historical analytics, and MongoDB persistence. Built with Rust and an embedded terminal-themed web UI.

## Features

- **Real-time size tracking** per topic: avg, min, max, percentiles (p50/p95/p99 via reservoir sampling), msg/sec, bytes/sec
- **Oversized message alerts** with configurable per-topic warning and critical thresholds, plus **downloadable payload capture** for messages exceeding the threshold
- **Historical message browser** — browse actual Kafka messages by offset, timestamp, or preset (beginning/1h/1d/latest), view key/value/headers, download full payloads
- **Global dashboard snapshot** — "time-travel" to see what all topics' stats looked like at any historical timestamp
- **7-day historical analytics** with per-minute aggregation and interactive time-series charts
- **Live feed** of message sizes with color-coded severity (green/yellow/red)
- **WebSocket streaming** with rate limiting (50 events/sec), topic filtering, and auto-reconnect
- **Auto-discovery** of new Kafka topics every 30 seconds
- **Hot-swappable broker** configuration without restart
- **Topic selection** — startup picker overlay to choose which topics to monitor, with localStorage persistence
- **Priority display** — topics sorted by critical, warning, healthy status with pulse-red animation for threshold-crossing topics
- **Enhanced alerts** — time window filter (5m/15m/1h/all), shows all topics
- **Broker discovery** — discover cluster brokers via metadata API, popover UI with click-to-connect
- **Cluster insights** — full-screen overlay showing brokers, topics, partitions, consumer lag, throughput per topic, and recommendations
- **MongoDB persistence** — optional persistence of oversized messages and alerts to MongoDB, with a dedicated browser tab for querying persisted data
- **3 UI themes** — Terminal/Chalk (default), Tokyo Night, Dracula, with theme switcher and localStorage persistence

## Quick Start

### Docker (Full Stack)

Starts Kafka (KRaft mode, no ZooKeeper), the tracker dashboard, MongoDB, and a seed producer that generates variable-size messages across 4 topics:

```bash
docker compose -f docker-compose.full.yml up -d
```

Open http://localhost:3000

### Docker (App Only)

Connect to an existing Kafka cluster:

```bash
KAFKA_BROKERS=your-kafka:9092 docker compose up -d
```

### From Source

Requires Rust 1.88+ and cmake (for librdkafka static build):

```bash
cargo run -- --brokers localhost:9092 --port 3000
```

With MongoDB persistence:

```bash
cargo run -- --brokers localhost:9092 --port 3000 --mongo-uri mongodb://localhost:27017
```

## CLI Options

```
kafka-message-size-tracker [OPTIONS]

Options:
  --brokers <BROKERS>                Kafka broker addresses [default: localhost:9092]
  --port <PORT>                      Server port [default: 3000]
  --host <HOST>                      Server host [default: 127.0.0.1]
  --threshold <THRESHOLD>            Warning alert threshold in bytes [default: 1048576]
  --critical-threshold <THRESHOLD>   Critical alert threshold in bytes [default: 10485760]
  --mongo-uri <URI>                  MongoDB connection URI (enables persistence)
```

## Environment Variables

All CLI options can be overridden with environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:9092` | Kafka broker addresses |
| `SERVER_HOST` | `127.0.0.1` | Server bind host |
| `SERVER_PORT` | `3000` | Server bind port |
| `ALERT_THRESHOLD_BYTES` | `1048576` (1MB) | Default warning threshold |
| `CRITICAL_THRESHOLD_BYTES` | `10485760` (10MB) | Default critical threshold |
| `CONSUMER_GROUP` | `kafka-size-tracker` | Kafka consumer group prefix |
| `MONGO_URI` | _(none)_ | MongoDB connection URI (enables persistence when set) |
| `RUST_LOG` | `info` | Log level filter |

## Dashboard

The web UI supports **3 themes**, selectable via the theme switcher in the header:

| Theme | Description |
|-------|-------------|
| **Terminal/Chalk** (default) | Dark background (#0d1117), ANSI terminal colors, scanline overlay, CRT text glow |
| **Tokyo Night** | Deep blue tones inspired by the Tokyo Night color scheme |
| **Dracula** | Purple-accented dark theme based on the Dracula palette |

All themes use monospace fonts (JetBrains Mono / Fira Code) and the selected theme persists via localStorage.

### Layout

The dashboard uses a vertical sidebar (280px left) with expandable topic cards and a 5-panel grid:

| Section | Description |
|---------|-------------|
| **Header** | App title with blinking cursor, connection status, theme switcher, broker discovery, cluster insights, `[history_browser]` button |
| **Sidebar** | Topic cards sorted by priority (critical > warning > healthy), expandable with quick stats, topic selection controls |
| **Live Feed** | Scrolling real-time log of message sizes, color-coded by severity |
| **Stats Detail** | Selected topic percentiles table (min, p50, p95, p99, max), rates |
| **Alerts** | Active/recent alerts with severity indicators, time window filter (5m/15m/1h/all) |
| **History Chart** | Canvas-based time-series line chart with selectable range (1h/6h/24h/7d) |
| **Global History** | Spanning bottom panel for global snapshot timeline |
| **Topic Picker** | Startup overlay to select which topics to monitor (persisted to localStorage) |
| **Broker Discovery** | Popover showing discovered cluster brokers with click-to-connect |
| **Cluster Insights** | Full-screen overlay with broker table, topic/partition details, consumer lag bars, throughput metrics, and recommendation cards |
| **History Browser** | Full-screen overlay with 3 tabs: message browsing (by offset/timestamp/preset), global snapshot (time-travel), and persisted data (MongoDB) |
| **Status Bar** | WebSocket status, topic count, event count, alert count |

## API

| Route | Method | Description |
|-------|--------|-------------|
| `/` | GET | Serve dashboard HTML |
| `/api/topics` | GET | List all topics with summary stats and alert status |
| `/api/topics/available` | GET | List all available Kafka topics for selection |
| `/api/topics/toggle` | POST | Enable/disable topic monitoring |
| `/api/stats/{topic}` | GET | Detailed stats: count, avg, min, max, p50, p95, p99, rates |
| `/api/alerts` | GET | Recent alerts (`?limit=N`, default 100) |
| `/api/alerts/download/{id}` | GET | Download captured oversized message payload (binary) |
| `/api/alerts/config` | GET | All alert threshold configurations |
| `/api/alerts/config` | POST | Set threshold for a topic |
| `/api/history/{topic}` | GET | Time-series data (`?range=1h\|6h\|24h\|7d`) |
| `/api/broker` | GET | Current broker address |
| `/api/broker` | POST | Update broker (hot-swaps consumers) |
| `/api/broker/discover` | GET | Discover cluster brokers via metadata API |
| `/api/browse/{topic}` | GET | Browse messages (`?partition=0&offset=100&limit=50` or `?timestamp=ISO8601&limit=50` or `?preset=beginning\|1h\|1d\|latest`) |
| `/api/browse/{topic}/download/{partition}/{offset}` | GET | Download full message payload at a specific partition/offset |
| `/api/snapshot` | GET | Global dashboard snapshot at a timestamp (`?timestamp=ISO8601`) |
| `/api/cluster/insights` | GET | Cluster insights: brokers, topics, partitions, consumer lag, throughput, recommendations |
| `/api/persisted/messages` | GET | Query persisted oversized messages from MongoDB |
| `/api/persisted/alerts` | GET | Query persisted alerts from MongoDB |
| `/api/persisted/status` | GET | MongoDB persistence status (connected, collection counts) |
| `/ws/live` | GET | WebSocket stream (SizeEvent, StatsUpdate, Alert) |

### WebSocket Protocol

Connect to `/ws/live` for real-time updates. Messages are JSON with a `type` field:

```json
{"type": "size_event", "topic": "orders", "partition": 0, "offset": 42, "size_bytes": 1024, "timestamp": "..."}
{"type": "stats_update", "topic": "orders", "message_count": 500, "avg_bytes": 2048.5, "p95": 8192, ...}
{"type": "alert", "id": 1, "topic": "logs", "severity": "critical", "message_size": 15728640, "threshold": 10485760, ...}
```

Clients can filter by topic:

```json
{"action": "subscribe", "topics": ["orders", "users"]}
{"action": "unsubscribe", "topics": ["users"]}
{"action": "subscribe_all"}
```

## Architecture

### Data Flow

```
Kafka Topic
  -> StreamConsumer (extracts payload.len(); captures raw bytes if >= warning threshold)
  -> broadcast::Sender<MessageSizeEvent>
  -> Event Processor:
      -> StatsEngine.record_event()    (running stats + reservoir sampling)
      -> AlertEngine.check_event()     (threshold check -> alert if exceeded)
      -> If alert + captured payload -> store CapturedMessage (max 100, LRU eviction)
      -> If MongoStore available -> persist oversized message + alert to MongoDB
      -> ws_broadcast                  (-> WebSocket clients; captured_payload is #[serde(skip)])
  -> Background Tasks:
      -> Stats Broadcaster (1s)        (all stats -> WebSocket)
      -> Minute Aggregator (60s)       (snapshot -> TimeSeriesStorage)
      -> Topic Auto-Discovery (30s)    (admin consumer -> subscribe new topics)
```

### Project Structure

```
src/
├── main.rs              # Server bootstrap, TopicManager, AppState, SharedMongoStore, routes, background tasks
├── config.rs            # CLI args (clap) + environment variable fallback + --mongo-uri
├── error.rs             # TrackerError with actix ResponseError impl
├── models.rs            # All data structures and serialization (including topic toggle, broker discovery, cluster insights, browse presets)
├── mongo.rs             # MongoStore: two collections (oversized_messages + alerts), PersistedMessage/PersistedAlert models
├── kafka/
│   ├── admin.rs         # BaseConsumer for metadata, discover_brokers(), fetch_consumer_lag(), build_cluster_insights()
│   ├── consumer.rs      # StreamConsumer per topic, size extraction
│   └── browser.rs       # Historical message browsing (seek by offset/timestamp), browse_from_beginning(), browse_latest()
├── tracker/
│   ├── stats.rs         # StatsEngine: reservoir sampling (Algorithm R), percentiles, rates
│   └── alerts.rs        # AlertEngine: per-topic thresholds, circular alert buffer
├── storage.rs           # TimeSeriesStorage: per-minute circular buffer (7 days), snapshot_at() for time-travel
├── handlers/
│   ├── topics.rs        # GET /api/topics, GET /api/topics/available, POST /api/topics/toggle
│   ├── stats.rs         # GET /api/stats/{topic}
│   ├── alerts.rs        # GET /api/alerts, alert config
│   ├── history.rs       # GET /api/history/{topic}
│   ├── broker.rs        # GET/POST /api/broker, GET /api/broker/discover, GET /api/cluster/insights
│   ├── browse.rs        # Message browser (with preset support) + global snapshot
│   ├── mongo.rs         # GET /api/persisted/messages, GET /api/persisted/alerts, GET /api/persisted/status
│   └── ws.rs            # WebSocket /ws/live
└── frontend/
    ├── mod.rs           # include_str! embedding
    └── index.html       # ~2400-line SPA with 3 themes
```

### Key Design Decisions

- **Reservoir sampling (Algorithm R)** with 1000 samples for accurate percentile estimation without unbounded memory growth
- **Message body captured only when oversized** (>= warning threshold); normal messages only extract `payload.len()` for minimal memory overhead. Up to 100 captured payloads stored with LRU eviction
- **Embedded single-file HTML** via `include_str!` for zero-dependency deployment (no static file serving needed)
- **Broadcast channels** per topic for fan-out to multiple WebSocket clients without copying
- **Rate limiting** on WebSocket (50 events/sec) to prevent browser flooding on high-throughput topics
- **Short-lived BaseConsumer** for historical browsing — unique consumer group per request, `enable.auto.commit=false`, dropped after request completes (no interference with real-time StreamConsumer instances)
- **Optional MongoDB persistence** — activated only when `--mongo-uri` or `MONGO_URI` is provided; graceful degradation when unavailable

## MongoDB Persistence

When a MongoDB URI is provided (via `--mongo-uri` or `MONGO_URI`), the tracker automatically persists:

- **Oversized messages** — messages exceeding the warning threshold are stored in the `oversized_messages` collection with topic, partition, offset, size, timestamp, and a preview of the payload
- **Alerts** — all generated alerts are stored in the `alerts` collection with severity, topic, message size, threshold, and timestamp

The persisted data can be queried through the API endpoints (`/api/persisted/messages`, `/api/persisted/alerts`, `/api/persisted/status`) or browsed directly in the History Browser's third tab (Persisted Data).

MongoDB is optional and the application runs fully without it. In the Docker full-stack setup, a mongo:7 container with a persistent volume is included automatically.

## Cluster Insights

The cluster insights feature (`GET /api/cluster/insights`) provides a comprehensive view of the Kafka cluster:

- **Brokers** — list of all brokers with IDs and host:port addresses
- **Topics and Partitions** — topic count, total partitions, partition distribution
- **Consumer Lag** — per-partition lag for tracked consumer groups, visualized as lag bars
- **Throughput** — messages/sec and bytes/sec per topic
- **Recommendations** — actionable suggestions based on lag, throughput patterns, and partition distribution

Accessible via the cluster insights button in the dashboard header, displayed as a full-screen overlay with tables and cards.

## Docker

### Dockerfile

Two-stage build for minimal runtime image:

1. **Builder**: `rust:1.88-bookworm` with cmake (compiles librdkafka from source via `cmake-build` feature)
2. **Runtime**: `debian:bookworm-slim` with only `ca-certificates` and `libssl3`

### Docker Compose Files

| File | Services | Use Case |
|------|----------|----------|
| `docker-compose.yml` | Tracker only | Connect to external Kafka |
| `docker-compose.full.yml` | Kafka + Tracker + Seed Producer + MongoDB | Self-contained demo/testing with persistence |

### Seed Producer

`scripts/seed-sizes.sh` uses piped stdin to long-running `kafka-console-producer` processes for efficient message generation across 4 topics:

| Topic | Normal Size | Alert Simulation | Frequency |
|-------|-------------|-------------------|-----------|
| `orders` | 200-800 B | ~1.2 MB every 10th iter (WARNING) | 1/sec |
| `users` | 100-500 B | ~2 MB every 25th iter (WARNING) | 1/sec |
| `logs` | 500-5000 B | ~12 MB every 15th iter (CRITICAL) | 1/sec |
| `metrics` | 50-200 B | none | 3/sec |

## Build Features

| Feature | Flag | Description |
|---------|------|-------------|
| Static librdkafka | `--features static-kafka` (default) | Compiles librdkafka from C source via cmake |
| Dynamic librdkafka | `--no-default-features --features dynamic-kafka` | Links against system librdkafka (requires >= 2.12.1) |

## Testing

```bash
cargo test
```

**65 tests** across all modules:

| Module | Tests | Coverage |
|--------|-------|----------|
| `config` | 2 | Default values, CLI arg overrides |
| `models` | 18 | Serialization roundtrips, serde attributes, browse/snapshot/toggle/discovery/insights/preset models |
| `handlers::history` | 1 | Range string parsing |
| `kafka::browser` | 5 | Payload encoding (UTF-8/binary/truncation), unique ID generation |
| `tracker::stats` | 13 | Running stats, reservoir sampling, percentiles, rates, snapshots |
| `tracker::alerts` | 11 | Threshold checks, circular buffer, per-topic config, enable/disable, capture store/eviction |
| `storage` | 11 | Circular buffer, range queries, eviction, multi-topic independence, snapshot_at |
| `mongo` | 4 | PersistedMessage ser/deser, PersistedAlert ser/deser |

## Tech Stack

| Component | Technology |
|-----------|------------|
| Web framework | actix-web 4 |
| WebSocket | actix-ws 0.3 |
| Kafka client | rdkafka 0.37 |
| MongoDB | mongodb 3 |
| Async runtime | tokio 1 |
| Serialization | serde + serde_json |
| CLI | clap 4 |
| Error handling | thiserror 2 |
| Timestamps | chrono 0.4 |
| Logging | env_logger 0.11 + log 0.4 |
| Sampling | rand 0.8 (reservoir sampling) |
| Encoding | base64 0.22 (binary payload encoding for browser) |
| Rust edition | 2024 |

## License

MIT
