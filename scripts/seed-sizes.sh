#!/bin/bash
# Seed producer: generates messages of varying sizes to test the tracker.
# Topics: orders, users, logs, metrics
# Sizes: normal (100B-2KB), large (10KB-100KB), oversized (1MB+)

set -e

KAFKA_BROKER="${KAFKA_BROKER:-kafka:9092}"
KAFKA_BIN="/opt/kafka/bin"

echo "[seed] Waiting for Kafka at $KAFKA_BROKER..."
until $KAFKA_BIN/kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" --list > /dev/null 2>&1; do
  echo "[seed] Kafka not ready, retrying in 3s..."
  sleep 3
done
echo "[seed] Kafka is ready!"

# Create topics
for topic in orders users logs metrics; do
  $KAFKA_BIN/kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" \
    --create --topic "$topic" --partitions 3 --replication-factor 1 --if-not-exists 2>/dev/null || true
  echo "[seed] Topic '$topic' ready"
done

echo "[seed] Starting message production..."

# Generate a message of N bytes
generate_payload() {
  local size=$1
  head -c "$size" /dev/urandom | base64 | head -c "$size"
}

i=0
while true; do
  i=$((i + 1))

  # orders: mostly small (200-800 bytes), occasional large (every 20th: 50KB-200KB)
  if [ $((i % 20)) -eq 0 ]; then
    size=$((50000 + RANDOM % 150000))
  else
    size=$((200 + RANDOM % 600))
  fi
  echo "$(generate_payload $size)" | $KAFKA_BIN/kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BROKER" --topic orders 2>/dev/null &

  # users: small (100-500 bytes)
  size=$((100 + RANDOM % 400))
  echo "$(generate_payload $size)" | $KAFKA_BIN/kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BROKER" --topic users 2>/dev/null &

  # logs: medium (500-5000 bytes), occasional oversized (every 50th: 1MB+)
  if [ $((i % 50)) -eq 0 ]; then
    size=$((1048576 + RANDOM % 500000))
  else
    size=$((500 + RANDOM % 4500))
  fi
  echo "$(generate_payload $size)" | $KAFKA_BIN/kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BROKER" --topic logs 2>/dev/null &

  # metrics: tiny (50-200 bytes), high frequency
  for j in 1 2 3; do
    size=$((50 + RANDOM % 150))
    echo "$(generate_payload $size)" | $KAFKA_BIN/kafka-console-producer.sh \
      --bootstrap-server "$KAFKA_BROKER" --topic metrics 2>/dev/null &
  done

  wait

  # Log progress every 100 iterations
  if [ $((i % 100)) -eq 0 ]; then
    echo "[seed] Produced $((i * 6)) messages (iteration $i)"
  fi

  # ~1 second between batches
  sleep 1
done
