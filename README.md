# KafkaRealTimeMessageSizeTracker

Real-time Kafka message size monitoring dashboard with configurable alerts and historical analytics. Built with Rust and an embedded terminal-themed web UI.

## Features

- **Real-time size tracking** per topic: avg, min, max, percentiles (p50/p95/p99 via reservoir sampling), msg/sec, bytes/sec
- **Oversized message alerts** with configurable per-topic warning and critical thresholds, plus **downloadable payload capture** for messages exceeding the threshold
- **7-day historical analytics** with per-minute aggregation and interactive time-series charts
- **Live feed** of message sizes with color-coded severity (green/yellow/red)
- **WebSocket streaming** with rate limiting (50 events/sec), topic filtering, and auto-reconnect
- **Auto-discovery** of new Kafka topics every 30 seconds
- **Hot-swappable broker** configuration without restart

## Quick Start

### Docker (Full Stack)

Starts Kafka (KRaft mode, no ZooKeeper), the tracker dashboard, and a seed producer that generates variable-size messages across 4 topics:

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

## CLI Options

```
kafka-message-size-tracker [OPTIONS]

Options:
  --brokers <BROKERS>                Kafka broker addresses [default: localhost:9092]
  --port <PORT>                      Server port [default: 3000]
  --host <HOST>                      Server host [default: 127.0.0.1]
  --threshold <THRESHOLD>            Warning alert threshold in bytes [default: 1048576]
  --critical-threshold <THRESHOLD>   Critical alert threshold in bytes [default: 10485760]
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
| `RUST_LOG` | `info` | Log level filter |

## Dashboard

The web UI uses a **terminal/chalk theme** inspired by ANSI terminal colors:

- Dark background with monospace font (JetBrains Mono / Fira Code)
- Scanline overlay and CRT text glow effects
- Color-coded metrics: green (healthy), yellow (warning), red (critical), cyan (info)

### Layout

| Section | Description |
|---------|-------------|
| **Header** | App title with blinking cursor, connection status, broker configuration |
| **Topic Grid** | Cards per tracked topic showing msg/sec, avg size, max size, alert status |
| **Live Feed** | Scrolling real-time log of message sizes, color-coded by severity |
| **Stats Detail** | Selected topic percentiles table (min, p50, p95, p99, max), rates |
| **Alerts** | Active/recent alerts with severity indicators and threshold details |
| **History Chart** | Canvas-based time-series line chart with selectable range (1h/6h/24h/7d) |
| **Status Bar** | WebSocket status, topic count, event count, alert count |

## API

| Route | Method | Description |
|-------|--------|-------------|
| `/` | GET | Serve dashboard HTML |
| `/api/topics` | GET | List all topics with summary stats and alert status |
| `/api/stats/{topic}` | GET | Detailed stats: count, avg, min, max, p50, p95, p99, rates |
| `/api/alerts` | GET | Recent alerts (`?limit=N`, default 100) |
| `/api/alerts/download/{id}` | GET | Download captured oversized message payload (binary) |
| `/api/alerts/config` | GET | All alert threshold configurations |
| `/api/alerts/config` | POST | Set threshold for a topic |
| `/api/history/{topic}` | GET | Time-series data (`?range=1h\|6h\|24h\|7d`) |
| `/api/broker` | GET | Current broker address |
| `/api/broker` | POST | Update broker (hot-swaps consumers) |
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
      -> ws_broadcast                  (-> WebSocket clients; captured_payload is #[serde(skip)])
  -> Background Tasks:
      -> Stats Broadcaster (1s)        (all stats -> WebSocket)
      -> Minute Aggregator (60s)       (snapshot -> TimeSeriesStorage)
      -> Topic Auto-Discovery (30s)    (admin consumer -> subscribe new topics)
```

### Project Structure

```
src/
├── main.rs              # Server bootstrap, TopicManager, AppState, routes, background tasks
├── config.rs            # CLI args (clap) + environment variable fallback
├── error.rs             # TrackerError with actix ResponseError impl
├── models.rs            # All data structures and serialization
├── kafka/
│   ├── admin.rs         # BaseConsumer for metadata operations
│   └── consumer.rs      # StreamConsumer per topic, size extraction
├── tracker/
│   ├── stats.rs         # StatsEngine: reservoir sampling (Algorithm R), percentiles, rates
│   └── alerts.rs        # AlertEngine: per-topic thresholds, circular alert buffer
├── storage.rs           # TimeSeriesStorage: per-minute circular buffer (7 days)
├── handlers/
│   ├── topics.rs        # GET /api/topics
│   ├── stats.rs         # GET /api/stats/{topic}
│   ├── alerts.rs        # GET /api/alerts, alert config
│   ├── history.rs       # GET /api/history/{topic}
│   ├── broker.rs        # GET/POST /api/broker
│   └── ws.rs            # WebSocket /ws/live
└── frontend/
    ├── mod.rs           # include_str! embedding
    └── index.html       # Terminal/chalk themed SPA
```

### Key Design Decisions

- **Reservoir sampling (Algorithm R)** with 1000 samples for accurate percentile estimation without unbounded memory growth
- **Message body captured only when oversized** (>= warning threshold); normal messages only extract `payload.len()` for minimal memory overhead. Up to 100 captured payloads stored with LRU eviction
- **Embedded single-file HTML** via `include_str!` for zero-dependency deployment (no static file serving needed)
- **Broadcast channels** per topic for fan-out to multiple WebSocket clients without copying
- **Rate limiting** on WebSocket (50 events/sec) to prevent browser flooding on high-throughput topics

## Docker

### Dockerfile

Two-stage build for minimal runtime image:

1. **Builder**: `rust:1.88-bookworm` with cmake (compiles librdkafka from source via `cmake-build` feature)
2. **Runtime**: `debian:bookworm-slim` with only `ca-certificates` and `libssl3`

### Docker Compose Files

| File | Services | Use Case |
|------|----------|----------|
| `docker-compose.yml` | Tracker only | Connect to external Kafka |
| `docker-compose.full.yml` | Kafka + Tracker + Seed Producer | Self-contained demo/testing |

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

**40 tests** across all modules:

| Module | Tests | Coverage |
|--------|-------|----------|
| `config` | 2 | Default values, CLI arg overrides |
| `models` | 6 | Serialization roundtrips, serde attributes |
| `handlers::history` | 1 | Range string parsing |
| `tracker::stats` | 13 | Running stats, reservoir sampling, percentiles, rates, snapshots |
| `tracker::alerts` | 11 | Threshold checks, circular buffer, per-topic config, enable/disable, capture store/eviction |
| `storage` | 7 | Circular buffer, range queries, eviction, multi-topic independence |

## Tech Stack

| Component | Technology |
|-----------|------------|
| Web framework | actix-web 4 |
| WebSocket | actix-ws 0.3 |
| Kafka client | rdkafka 0.37 |
| Async runtime | tokio 1 |
| Serialization | serde + serde_json |
| CLI | clap 4 |
| Error handling | thiserror 2 |
| Timestamps | chrono 0.4 |
| Logging | env_logger 0.11 + log 0.4 |
| Sampling | rand 0.8 (reservoir sampling) |
| Rust edition | 2024 |

## License

MIT
