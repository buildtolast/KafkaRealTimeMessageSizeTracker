#!/usr/bin/env bash
# ============================================================================
# KafkaRealTimeMessageSizeTracker — run.sh
# ============================================================================
# Usage:
#   ./run.sh                      # Run with defaults (localhost:9092, port 5688)
#   ./run.sh --full                # Full stack Docker (default: 1 Kafka + 1 Mongo)
#   ./run.sh --full --kafka-nodes 3 --mongo-nodes 1   # Custom node counts
#   ./run.sh --docker              # App-only Docker (connect to external Kafka)
#   ./run.sh --confluent           # Connect to Confluent Cloud (set env vars below)
#   ./run.sh --stop                # Stop whatever was started (auto-detects mode)
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

# ── Cluster defaults ─────────────────────────────────────────────────────────
KAFKA_NODES="${KAFKA_NODES:-1}"
MONGO_NODES="${MONGO_NODES:-1}"

# ============================================================================

GENERATED_COMPOSE="docker-compose.generated.yml"

print_help() {
    echo "KafkaRealTimeMessageSizeTracker"
    echo ""
    echo "Usage:"
    echo "  ./run.sh                Run from source with cargo (default)"
    echo "  ./run.sh --full         Full stack Docker: Kafka + App + Seed + MongoDB"
    echo "  ./run.sh --docker       App-only Docker (set KAFKA_BROKERS env var)"
    echo "  ./run.sh --confluent    Connect to Confluent Cloud (prompts for credentials)"
    echo "  ./run.sh --stop         Stop whatever was started (auto-detects mode)"
    echo "  ./run.sh --help         Show this help"
    echo ""
    echo "Cluster Options (used with --full):"
    echo "  --kafka-nodes N         Number of Kafka brokers        (default: 1, min: 1)"
    echo "  --mongo-nodes N         Number of MongoDB nodes        (default: 1, options: 1 or 3)"
    echo ""
    echo "Examples:"
    echo "  ./run.sh --full                              # 1 Kafka + 1 Mongo (minimal/dev)"
    echo "  ./run.sh --full --kafka-nodes 3 --mongo-nodes 3   # Medium cluster"
    echo "  ./run.sh --full --kafka-nodes 5 --mongo-nodes 3   # Production-like"
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
    echo "  KAFKA_NODES                Number of Kafka nodes        (default: 5)"
    echo "  MONGO_NODES                Number of Mongo nodes        (default: 3)"
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

# ── Dynamic Compose Generator ─────────────────────────────────────────────────

generate_compose() {
    local kafka_n=$1
    local mongo_n=$2
    local out=$3

    # Determine replication factors
    local rf=1
    local min_isr=1
    if [[ $kafka_n -ge 3 ]]; then
        rf=3
        min_isr=2
    elif [[ $kafka_n -eq 2 ]]; then
        rf=2
        min_isr=1
    fi

    # How many controller nodes (max 3, or kafka_n if fewer)
    local controller_n=$kafka_n
    if [[ $controller_n -gt 3 ]]; then
        controller_n=3
    fi

    # Build quorum voters string
    local quorum_voters=""
    for i in $(seq 1 "$controller_n"); do
        [[ -n "$quorum_voters" ]] && quorum_voters="${quorum_voters},"
        quorum_voters="${quorum_voters}${i}@kafka-${i}:9093"
    done

    # Build broker connection string for the app
    local broker_list=""
    for i in $(seq 1 "$kafka_n"); do
        [[ -n "$broker_list" ]] && broker_list="${broker_list},"
        broker_list="${broker_list}kafka-${i}:9092"
    done

    # Build mongo connection string
    local mongo_uri=""
    if [[ $mongo_n -eq 1 ]]; then
        mongo_uri="mongodb://mongo-1:27017"
    else
        local mongo_hosts=""
        for i in $(seq 1 "$mongo_n"); do
            [[ -n "$mongo_hosts" ]] && mongo_hosts="${mongo_hosts},"
            mongo_hosts="${mongo_hosts}mongo-${i}:27017"
        done
        mongo_uri="mongodb://${mongo_hosts}/?replicaSet=rs0"
    fi

    echo "Generating compose: ${kafka_n} Kafka nodes, ${mongo_n} Mongo nodes (RF=${rf})"

    # Start writing
    cat > "$out" <<'HEADER'
# ════════════════════════════════════════════════════════════════
#  Auto-generated by run.sh — do not edit manually
# ════════════════════════════════════════════════════════════════
services:
HEADER

    # ── Kafka nodes ──
    for i in $(seq 1 "$kafka_n"); do
        local external_port=$((9093 + i))   # 9094, 9095, 9096, ...
        local roles="broker"
        local listeners="PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094"
        local advertised="PLAINTEXT://kafka-${i}:9092,EXTERNAL://localhost:${external_port}"

        if [[ $i -le $controller_n ]]; then
            roles="broker,controller"
            listeners="PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9094"
        fi

        local depends_block=""
        if [[ $i -gt $controller_n ]]; then
            depends_block="    depends_on:
      kafka-1:
        condition: service_healthy"
        fi

        cat >> "$out" <<EOF
  kafka-${i}:
    image: apache/kafka:3.7.0
    container_name: kafka-size-tracker-kafka-${i}
    ports:
      - "${external_port}:9094"
    environment:
      KAFKA_NODE_ID: ${i}
      KAFKA_PROCESS_ROLES: ${roles}
      KAFKA_LISTENERS: ${listeners}
      KAFKA_ADVERTISED_LISTENERS: ${advertised}
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: ${quorum_voters}
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: ${rf}
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: ${rf}
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: ${min_isr}
      KAFKA_DEFAULT_REPLICATION_FACTOR: ${rf}
      KAFKA_MIN_INSYNC_REPLICAS: ${min_isr}
      CLUSTER_ID: "kafka-size-tracker-cluster"
${depends_block:+${depends_block}
}    healthcheck:
      test: /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list || exit 1
      interval: 10s
      timeout: 10s
      retries: 5
      start_period: 30s
    networks:
      - tracker-net

EOF
    done

    # ── MongoDB nodes ──
    if [[ $mongo_n -eq 1 ]]; then
        cat >> "$out" <<'EOF'
  mongo-1:
    image: mongo:7
    container_name: kafka-size-tracker-mongo-1
    volumes:
      - mongo-data-1:/data/db
    healthcheck:
      test: mongosh --eval "db.adminCommand('ping')" --quiet || exit 1
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s
    networks:
      - tracker-net

EOF
    else
        # Replica set nodes
        for i in $(seq 1 "$mongo_n"); do
            local mongo_depends=""
            if [[ $i -gt 1 ]]; then
                mongo_depends="    depends_on:
      mongo-1:
        condition: service_healthy"
            fi
            local hc_block=""
            if [[ $i -eq 1 ]]; then
                hc_block="    healthcheck:
      test: mongosh --eval \"db.adminCommand('ping')\" --quiet || exit 1
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s"
            fi

            cat >> "$out" <<EOF
  mongo-${i}:
    image: mongo:7
    container_name: kafka-size-tracker-mongo-${i}
    command: mongod --replSet rs0 --bind_ip_all
    volumes:
      - mongo-data-${i}:/data/db
${mongo_depends:+${mongo_depends}
}${hc_block:+${hc_block}
}    networks:
      - tracker-net

EOF
        done

        # Replica set init container
        local rs_members=""
        for i in $(seq 1 "$mongo_n"); do
            local priority=1
            [[ $i -eq 1 ]] && priority=2
            [[ -n "$rs_members" ]] && rs_members="${rs_members}, "
            rs_members="${rs_members}{ _id: $((i - 1)), host: \"mongo-${i}:27017\", priority: ${priority} }"
        done

        local mongo_deps_init=""
        for i in $(seq 1 "$mongo_n"); do
            if [[ $i -eq 1 ]]; then
                mongo_deps_init="${mongo_deps_init}      mongo-${i}:
        condition: service_healthy
"
            else
                mongo_deps_init="${mongo_deps_init}      mongo-${i}:
        condition: service_started
"
            fi
        done

        cat >> "$out" <<EOF
  mongo-init:
    image: mongo:7
    container_name: kafka-size-tracker-mongo-init
    depends_on:
${mongo_deps_init}    entrypoint: >
      mongosh --host mongo-1:27017 --eval '
        try {
          rs.status();
          print("Replica set already initialized");
        } catch(e) {
          rs.initiate({
            _id: "rs0",
            members: [${rs_members}]
          });
          print("Replica set initiated");
        }
      '
    restart: "no"
    networks:
      - tracker-net

EOF
    fi

    # ── Tracker app ──
    local tracker_kafka_depends=""
    for i in $(seq 1 "$kafka_n"); do
        # Only wait for controller nodes (they have healthchecks that matter)
        if [[ $i -le $controller_n ]]; then
            tracker_kafka_depends="${tracker_kafka_depends}      kafka-${i}:
        condition: service_healthy
"
        fi
    done

    local tracker_mongo_depends=""
    if [[ $mongo_n -eq 1 ]]; then
        tracker_mongo_depends="      mongo-1:
        condition: service_healthy
"
    else
        tracker_mongo_depends="      mongo-1:
        condition: service_healthy
      mongo-init:
        condition: service_completed_successfully
"
    fi

    cat >> "$out" <<EOF
  tracker:
    build: .
    container_name: kafka-size-tracker
    ports:
      - "5688:5688"
    environment:
      KAFKA_BROKERS: ${broker_list}
      SERVER_HOST: "0.0.0.0"
      SERVER_PORT: "5688"
      RUST_LOG: info
      ALERT_THRESHOLD_BYTES: "1048576"
      CRITICAL_THRESHOLD_BYTES: "10485760"
      MONGO_URI: "${mongo_uri}"
    depends_on:
${tracker_kafka_depends}${tracker_mongo_depends}    networks:
      - tracker-net
    restart: unless-stopped

  seed-producer:
    image: apache/kafka:3.7.0
    container_name: kafka-size-tracker-seed
    depends_on:
      kafka-1:
        condition: service_healthy
    volumes:
      - ./scripts/seed-sizes.sh:/scripts/seed-sizes.sh
    entrypoint: ["/bin/bash", "/scripts/seed-sizes.sh"]
    environment:
      KAFKA_BROKER: kafka-1:9092
      REPLICATION_FACTOR: "${rf}"
    networks:
      - tracker-net

EOF

    # ── Volumes ──
    echo "volumes:" >> "$out"
    for i in $(seq 1 "$mongo_n"); do
        echo "  mongo-data-${i}:" >> "$out"
    done

    # ── Networks ──
    cat >> "$out" <<'EOF'

networks:
  tracker-net:
    driver: bridge
EOF

    echo "Generated: ${out}"
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
    generate_compose "$KAFKA_NODES" "$MONGO_NODES" "$GENERATED_COMPOSE"
    echo ""
    echo "Starting full stack (${KAFKA_NODES} Kafka + ${MONGO_NODES} Mongo)..."
    echo ""
    docker compose -f "$GENERATED_COMPOSE" up -d --build
    echo ""
    echo "Dashboard: http://localhost:5688"
    echo "Kafka external ports: $(seq -s ', ' 9094 $((9093 + KAFKA_NODES)))"
    echo ""
    echo "Logs:  docker compose -f ${GENERATED_COMPOSE} logs -f tracker"
    echo "Stop:  ./run.sh --stop"
}

run_docker() {
    echo "Starting app-only Docker (connecting to ${KAFKA_BROKERS})..."
    echo ""
    KAFKA_BROKERS="${KAFKA_BROKERS}" docker compose up -d --build
    echo ""
    echo "Dashboard: http://localhost:5688"
    echo ""
    echo "Logs:  docker compose logs -f"
    echo "Stop:  ./run.sh --stop"
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

run_stop() {
    local stopped=0

    # Check for generated compose (full stack)
    if [[ -f "$GENERATED_COMPOSE" ]]; then
        local gen_ps
        gen_ps=$(docker compose -f "$GENERATED_COMPOSE" ps -q 2>/dev/null || true)
        if [[ -n "$gen_ps" ]]; then
            echo "Stopping full stack (generated compose)..."
            docker compose -f "$GENERATED_COMPOSE" down
            stopped=1
        fi
    fi

    # Check for static full-stack compose
    local full_ps
    full_ps=$(docker compose -f docker-compose.full.yml ps -q 2>/dev/null || true)
    if [[ -n "$full_ps" ]]; then
        echo "Stopping full stack (Kafka + Tracker + Seed Producer + MongoDB)..."
        docker compose -f docker-compose.full.yml down
        stopped=1
    fi

    # Check for app-only Docker (docker-compose.yml)
    local app_ps
    app_ps=$(docker compose ps -q 2>/dev/null || true)
    if [[ -n "$app_ps" ]]; then
        echo "Stopping app-only Docker..."
        docker compose down
        stopped=1
    fi

    # Check for cargo/binary process on the configured port
    local port_pid
    port_pid=$(lsof -ti :"${SERVER_PORT}" 2>/dev/null || true)
    if [[ -n "$port_pid" ]]; then
        echo "Stopping process on port ${SERVER_PORT} (PID: ${port_pid})..."
        kill $port_pid 2>/dev/null || true
        sleep 2
        # Force kill if still alive
        local remaining
        remaining=$(lsof -ti :"${SERVER_PORT}" 2>/dev/null || true)
        if [[ -n "$remaining" ]]; then
            echo "Force stopping remaining processes..."
            kill -9 $remaining 2>/dev/null || true
        fi
        stopped=1
    fi

    if [[ $stopped -eq 0 ]]; then
        echo "No running instances found."
    else
        echo ""
        echo "Stopped."
    fi
}

# ── Parse arguments ─────────────────────────────────────────────────────────

MODE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --full)       MODE="full"; shift ;;
        --docker)     MODE="docker"; shift ;;
        --confluent)  MODE="confluent"; shift ;;
        --stop)       MODE="stop"; shift ;;
        --help|-h)    MODE="help"; shift ;;
        --kafka-nodes)
            KAFKA_NODES="$2"; shift 2 ;;
        --mongo-nodes)
            MONGO_NODES="$2"; shift 2 ;;
        *)
            echo "Unknown option: $1"
            echo "Run ./run.sh --help for usage"
            exit 1 ;;
    esac
done

# Validate node counts
if [[ "$KAFKA_NODES" -lt 1 ]]; then
    echo "Error: --kafka-nodes must be at least 1"
    exit 1
fi
if [[ "$MONGO_NODES" -ne 1 && "$MONGO_NODES" -ne 3 ]]; then
    echo "Error: --mongo-nodes must be 1 or 3 (standalone or replica set)"
    exit 1
fi

# ── Main ─────────────────────────────────────────────────────────────────────

case "${MODE}" in
    full)       run_full_docker ;;
    docker)     run_docker ;;
    confluent)  run_confluent ;;
    stop)       run_stop ;;
    help)       print_help ;;
    *)          run_cargo ;;
esac
