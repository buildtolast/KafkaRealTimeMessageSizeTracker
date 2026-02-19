# Architecture Review

## Report Card

| Category | Grade | Summary |
|---|---|---|
| **Project Structure** | **A** | Clean module hierarchy, clear separation of concerns |
| **Concurrency** | **A** | Proper async/await, Arc/RwLock/Mutex used correctly, no deadlock risks |
| **API Design** | **A** | RESTful, consistent naming, proper HTTP status codes |
| **Docker** | **A** | Multi-stage build, smart dependency caching, minimal runtime image |
| **Code Quality** | **A** | No magic numbers, well-named constants, minimal duplication |
| **Testing** | **A-** | 97 tests with good edge case coverage, but gaps in integration tests |
| **Performance** | **A-** | Reservoir sampling, efficient hot paths, WebSocket rate limiting |
| **Error Handling** | **B+** | `thiserror` used well, but some silent failures in consumer/background tasks |
| **Configuration** | **B+** | clap + env var fallbacks, but missing startup validation |
| **Security** | **B** | SASL/SSL works, credentials not logged, but stored as plain String in memory |
| **Frontend** | **C+** | Works great but single ~3100-line HTML file is at maintainability limit |
| **Production Readiness** | **C+** | Missing key operational practices |

---

## What's Done Well

### Project Structure
- Clear module hierarchy: Kafka domain logic separate from API handlers
- Tracker subsystem (stats/alerts/progress) well isolated
- Handler layer is thin — mostly routing + data passing
- Domain types centralized in `models.rs`

### Concurrency
- Proper tokio async locks — no blocking in async context
- `RwLock` for read-heavy stats (correct choice over Mutex)
- Arc cloning for task ownership — avoids holding locks across `.await`
- `blocking_lock()` correctly used inside `web::block` context
- Broadcast channel (1024 buffer) balances memory vs lag tolerance

### Error Handling
- Zero `unwrap()` in non-test code — defensive error handling throughout
- `thiserror` used consistently for error derivation
- Proper HTTP status code mapping via `actix_web::ResponseError` impl
- Sensible defaults in query parameter parsing

### Performance
- **Reservoir sampling (Algorithm R)** — O(1) memory for p50/p95/p99 percentiles with fixed 1000-element reservoir
- **Hot path efficiency** — never materializes message body into String; only allocates when exceeding capture threshold
- **Scoped lag fetching** — only tracked topics, not entire cluster (critical for Confluent Cloud)
- **Cached cluster insights** — API serves entirely from cache, zero Kafka calls on request
- **WebSocket rate limiting** — 50 events/sec cap protects the frontend
- **Per-topic metadata fetching** — reduces network round-trips for remote brokers

### API Design
- Consistent RESTful naming (nouns, proper verbs)
- Hierarchical nesting (e.g., `/alerts/config`, `/cluster/insights`)
- Clear separation of concerns (topics, stats, cluster, consumer)
- Proper HTTP semantics (GET for reads, POST for mutations)

### Docker
- Intelligent dependency caching — separates dep compilation from source changes
- Feature flags (`static-kafka` = cmake-build for Docker, `dynamic-kafka` for systems with newer librdkafka)
- Minimal runtime image (debian:bookworm-slim, only binary + ca-certificates + libssl3)
- Dynamic compose generation for configurable multi-node clusters

### Testing
- 97 tests with `#[tokio::test]` for async tests
- Edge case coverage (circular buffer eviction, threshold boundaries, reservoir bounds)
- Good test data builders (helper functions)
- Tests both happy and sad paths
- Config serialization/deserialization roundtrip tests

### Code Quality
- Well-documented constants (no scattered magic numbers): `MAX_ALERTS=1000`, `RESERVOIR_SIZE=1000`, `MAX_EVENTS_PER_SEC=50`, etc.
- Properly wrapped shared state in Arc — no global mutable state
- Feature flags for build flexibility (`static-kafka` / `dynamic-kafka`)

---

## What's Missing

### 1. Graceful Shutdown (Critical)
- No signal handling for SIGTERM/SIGINT
- Background tasks spawn without cancellation tokens
- No coordinated cleanup on shutdown
- Abrupt termination could lose in-flight data

**Recommendation:** Use `tokio::signal` + `CancellationToken` from `tokio-util`, pass cancel token to all background tasks, wait with timeout on shutdown.

### 2. Prometheus Metrics Endpoint
- No `/metrics` endpoint for external monitoring
- No request latency tracking
- No per-endpoint error rates
- No exportable consumer lag metrics
- Only process-level metrics (CPU/memory via sysinfo)

**Recommendation:** Add `prometheus` crate, expose standard counters/histograms/gauges at `/metrics`.

### 3. Input Validation
- No bounds checking on thresholds (could be 0 or MAX)
- No broker string validation (could be empty or excessively long)
- No topic name sanitization
- No API rate limiting
- Startup config doesn't validate `warning < critical`

**Recommendation:** Add `validator` crate with derive macros for request types. Add startup validation in `AppConfig::from_args_and_env()`.

### 4. Structured Logging
- Using `env_logger` (plain text output)
- No per-module log levels
- No JSON output for log aggregation
- No async log flushing (could lose logs on panic)

**Recommendation:** Migrate to `tracing` + `tracing-subscriber` with JSON formatter and `EnvFilter`.

### 5. Integration Tests
97 unit tests are strong, but no tests for:
- Full message flow (Kafka -> consumer -> alert -> WebSocket)
- TopicManager lifecycle (subscribe/unsubscribe coordination)
- Broker failover scenarios
- MongoDB persistence + recovery
- Concurrent topic creation/deletion

---

## Minor Observations

### Error Handling Gaps
- **Silent consumer failures** — if consumer creation fails, the task returns `()` with a log warning; no retry or user notification
- **AdminConsumer startup uses `.expect()`** — will panic if Kafka is unreachable at startup instead of gracefully degrading
- **Browser module uses `Result<..., String>`** — inconsistent with `TrackerError` used elsewhere
- **MongoDB persistence is fire-and-forget** — no retry on failure, no backpressure

### Security
- Credentials stored as plain `String` in memory (consider `zeroize` crate for SASL passwords)
- Environment variables visible in `docker ps`, `ps aux`, `/proc/*/environ` (consider Docker secrets)
- No input sanitization for credentials (null bytes, excessive length)
- Captured message payloads stored as raw bytes with no PII redaction

### Frontend
- Single ~3100-line HTML file is at maintainability limit
- No component structure — all logic in one file, global namespace
- No accessibility attributes (ARIA landmarks, live regions for real-time updates)
- CSS themes are well-organized via `[data-theme]` selectors but duplicated across 4 themes

### Docker
- No `HEALTHCHECK` in Dockerfile (present in compose but not standalone)
- `libssl3` not version-pinned in runtime image
- No SIGTERM propagation guarantee in entrypoint
- `tokio "full"` features could be trimmed to reduce binary size

### Configuration
- No TLS/SASL credential rotation mid-flight
- No connection pooling configuration for MongoDB
- Runtime threshold updates validated but startup thresholds are not

### Performance
- Broadcast channel fixed at 1024 elements for all topics — slow subscriber affects all
- No topic regex filtering support (exact match only)
- `subscribe()` method in TopicManager is 160 lines — could extract StartPosition handling

---

## Top Priorities for Production Readiness

1. **Graceful shutdown** with cancellation tokens and coordinated cleanup
2. **Prometheus `/metrics` endpoint** for external observability
3. **Input validation** on all API endpoints and startup configuration
4. **Structured logging** with `tracing` for JSON output and per-module levels
5. **Integration test suite** covering end-to-end message flow and failure scenarios
