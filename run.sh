#!/usr/bin/env bash
# ============================================================================
# KafkaRealTimeMessageSizeTracker — run.sh
# ============================================================================
# Usage:
#   ./run.sh                      # Run with defaults (localhost:9092, port 5688)
#   ./run.sh --full                # Full stack: Kafka + Tracker + Seed Producer + MongoDB (Docker)
#   ./run.sh --docker              # App-only Docker (connect to external Kafka)
#   ./run.sh --confluent           # Connect to Confluent Cloud (set env vars below)
#   ./run.sh --help                # Show this help
# ============================================================================

set -euo pipefail

# ── Kafka ────────────────────────────────────────────────────────────────────
export KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"

# ── Server ───────────────────────────────────────────────────────────────────
export SERVER_HOST="${SERVER_HOST:-127.0.0.1}"
export SERVER_PORT="${SERVER_PORT:-5688}"

# ── Alert Thresholds ─────────────────────────────────────────────────────────
export ALERT_THRESHOLD_BYTES="${ALERT_THRESHOLD_BYTES:-1048576}"         # 1 MB  (warning)
export CRITICAL_THRESHOLD_BYTES="${CRITICAL_THRESHOLD_BYTES:-10485760}"  # 10 MB (critical)

# ── Consumer ─────────────────────────────────────────────────────────────────
export CONSUMER_GROUP="${CONSUMER_GROUP:-kafka-size-tracker}"

# ── MongoDB (optional — enables persistence) ─────────────────────────────────
# export MONGO_URI="mongodb://localhost:27017"

# ── SASL / SSL Authentication (optional — for Confluent Cloud, etc.) ─────────
# export KAFKA_SECURITY_PROTOCOL="SASL_SSL"        # SASL_SSL | SASL_PLAINTEXT | SSL
# export KAFKA_SASL_MECHANISM="PLAIN"               # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
# export KAFKA_SASL_USERNAME="<your-api-key>"
# export KAFKA_SASL_PASSWORD="<your-api-secret>"

# ── Logging ──────────────────────────────────────────────────────────────────
export RUST_LOG="${RUST_LOG:-info}"

# ============================================================================

print_help() {
    echo "KafkaRealTimeMessageSizeTracker"
    echo ""
    echo "Usage:"
    echo "  ./run.sh                Run from source with cargo (default)"
    echo "  ./run.sh --full         Full stack Docker: Kafka + App + Seed + MongoDB"
    echo "  ./run.sh --docker       App-only Docker (set KAFKA_BROKERS env var)"
    echo "  ./run.sh --confluent    Connect to Confluent Cloud (prompts for credentials)"
    echo "  ./run.sh --help         Show this help"
    echo ""
    echo "Environment Variables:"
    echo "  KAFKA_BROKERS              Kafka broker addresses       (default: localhost:9092)"
    echo "  SERVER_HOST                Server bind host             (default: 127.0.0.1)"
    echo "  SERVER_PORT                Server bind port             (default: 5688)"
    echo "  ALERT_THRESHOLD_BYTES      Warning threshold in bytes   (default: 1048576 = 1MB)"
    echo "  CRITICAL_THRESHOLD_BYTES   Critical threshold in bytes  (default: 10485760 = 10MB)"
    echo "  CONSUMER_GROUP             Kafka consumer group prefix  (default: kafka-size-tracker)"
    echo "  MONGO_URI                  MongoDB URI (enables persist)(default: disabled)"
    echo "  KAFKA_SECURITY_PROTOCOL    Security protocol            (SASL_SSL | SASL_PLAINTEXT | SSL)"
    echo "  KAFKA_SASL_MECHANISM       SASL mechanism               (PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512)"
    echo "  KAFKA_SASL_USERNAME        SASL username / API Key"
    echo "  KAFKA_SASL_PASSWORD        SASL password / API Secret"
    echo "  RUST_LOG                   Log level                    (default: info)"
}

print_config() {
    echo "┌─────────────────────────────────────────────────────────┐"
    echo "│  KafkaRealTimeMessageSizeTracker                        │"
    echo "├─────────────────────────────────────────────────────────┤"
    echo "│  Brokers:       ${KAFKA_BROKERS}"
    echo "│  Server:        ${SERVER_HOST}:${SERVER_PORT}"
    echo "│  Warning:       ${ALERT_THRESHOLD_BYTES} bytes"
    echo "│  Critical:      ${CRITICAL_THRESHOLD_BYTES} bytes"
    echo "│  Consumer Group: ${CONSUMER_GROUP}"
    echo "│  MongoDB:       ${MONGO_URI:-disabled}"
    echo "│  Auth:          ${KAFKA_SECURITY_PROTOCOL:-none}"
    echo "│  Log Level:     ${RUST_LOG}"
    echo "└─────────────────────────────────────────────────────────┘"
    echo ""
    echo "  Dashboard: http://${SERVER_HOST}:${SERVER_PORT}"
    echo ""
}

run_cargo() {
    print_config

    ARGS=(
        "--brokers" "${KAFKA_BROKERS}"
        "--port" "${SERVER_PORT}"
        "--host" "${SERVER_HOST}"
        "--threshold" "${ALERT_THRESHOLD_BYTES}"
        "--critical-threshold" "${CRITICAL_THRESHOLD_BYTES}"
    )

    if [[ -n "${MONGO_URI:-}" ]]; then
        ARGS+=("--mongo-uri" "${MONGO_URI}")
    fi

    if [[ -n "${KAFKA_SECURITY_PROTOCOL:-}" ]]; then
        ARGS+=("--security-protocol" "${KAFKA_SECURITY_PROTOCOL}")
    fi
    if [[ -n "${KAFKA_SASL_MECHANISM:-}" ]]; then
        ARGS+=("--sasl-mechanism" "${KAFKA_SASL_MECHANISM}")
    fi
    if [[ -n "${KAFKA_SASL_USERNAME:-}" ]]; then
        ARGS+=("--sasl-username" "${KAFKA_SASL_USERNAME}")
    fi
    if [[ -n "${KAFKA_SASL_PASSWORD:-}" ]]; then
        ARGS+=("--sasl-password" "${KAFKA_SASL_PASSWORD}")
    fi

    echo "Starting with: cargo run -- ${ARGS[*]}"
    echo ""
    cargo run -- "${ARGS[@]}"
}

run_full_docker() {
    echo "Starting full stack (Kafka + Tracker + Seed Producer + MongoDB)..."
    echo ""
    docker compose -f docker-compose.full.yml up -d
    echo ""
    echo "Dashboard: http://localhost:5688"
    echo "Kafka external: localhost:9094"
    echo ""
    echo "Logs:  docker compose -f docker-compose.full.yml logs -f tracker"
    echo "Stop:  docker compose -f docker-compose.full.yml down"
}

run_docker() {
    echo "Starting app-only Docker (connecting to ${KAFKA_BROKERS})..."
    echo ""
    KAFKA_BROKERS="${KAFKA_BROKERS}" docker compose up -d
    echo ""
    echo "Dashboard: http://localhost:5688"
    echo ""
    echo "Logs:  docker compose logs -f"
    echo "Stop:  docker compose down"
}

run_confluent() {
    if [[ -z "${KAFKA_BROKERS:-}" || "${KAFKA_BROKERS}" == "localhost:9092" ]]; then
        read -rp "Confluent bootstrap server (e.g. pkc-xxxxx.us-east-1.aws.confluent.cloud:9092): " KAFKA_BROKERS
        export KAFKA_BROKERS
    fi
    if [[ -z "${KAFKA_SASL_USERNAME:-}" ]]; then
        read -rp "API Key: " KAFKA_SASL_USERNAME
        export KAFKA_SASL_USERNAME
    fi
    if [[ -z "${KAFKA_SASL_PASSWORD:-}" ]]; then
        read -rsp "API Secret: " KAFKA_SASL_PASSWORD
        echo ""
        export KAFKA_SASL_PASSWORD
    fi

    export KAFKA_SECURITY_PROTOCOL="SASL_SSL"
    export KAFKA_SASL_MECHANISM="PLAIN"

    run_cargo
}

# ── Main ─────────────────────────────────────────────────────────────────────

case "${1:-}" in
    --full)
        run_full_docker
        ;;
    --docker)
        run_docker
        ;;
    --confluent)
        run_confluent
        ;;
    --help|-h)
        print_help
        ;;
    *)
        run_cargo
        ;;
esac
