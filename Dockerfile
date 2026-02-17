# Stage 1: Build
FROM rust:1.88-bookworm AS builder

RUN apt-get update && \
    apt-get install -y cmake libssl-dev pkg-config && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/kafka src/tracker src/handlers src/frontend && \
    echo 'fn main() {}' > src/main.rs && \
    touch src/config.rs src/error.rs src/models.rs src/storage.rs \
          src/kafka/mod.rs src/kafka/consumer.rs src/kafka/admin.rs \
          src/tracker/mod.rs src/tracker/stats.rs src/tracker/alerts.rs \
          src/handlers/mod.rs src/handlers/topics.rs src/handlers/stats.rs \
          src/handlers/alerts.rs src/handlers/history.rs src/handlers/broker.rs \
          src/handlers/ws.rs src/frontend/mod.rs && \
    echo '' > src/frontend/index.html && \
    cargo build --release --no-default-features --features static-kafka 2>/dev/null || true && \
    rm -rf src

# Build actual application
COPY src/ src/
RUN touch src/main.rs && \
    cargo build --release --no-default-features --features static-kafka

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/kafka-message-size-tracker ./

ENV SERVER_HOST=0.0.0.0
ENV SERVER_PORT=5000
ENV RUST_LOG=info

EXPOSE 5000

ENTRYPOINT ["./kafka-message-size-tracker"]
