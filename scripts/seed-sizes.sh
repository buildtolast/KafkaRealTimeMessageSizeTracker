#!/bin/bash
# Seed producer: generates messages of varying sizes to test the tracker.
# Topics: orders, users, logs, metrics
# Uses piped stdin to a single long-running kafka-console-producer per topic.
#
# Alert simulation schedule:
#   Every 10th iteration: orders sends ~1.2MB (triggers warning at 1MB default)
#   Every 15th iteration: logs   sends ~12MB  (triggers critical at 10MB default)
#   Every 25th iteration: users  sends ~2MB   (triggers warning at 1MB default)

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

# Generate a payload of N bytes (printable characters)
generate_payload() {
  local size=$1
  head -c "$size" /dev/urandom | base64 | head -c "$size"
}

# Create named pipes for each topic
FIFO_DIR=$(mktemp -d)
for topic in orders users logs metrics; do
  mkfifo "$FIFO_DIR/$topic"
done

# Start long-running producers reading from pipes
for topic in orders users logs metrics; do
  $KAFKA_BIN/kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BROKER" --topic "$topic" \
    < "$FIFO_DIR/$topic" 2>/dev/null &
done

# Open file descriptors for writing to the pipes
exec 3>"$FIFO_DIR/orders"
exec 4>"$FIFO_DIR/users"
exec 5>"$FIFO_DIR/logs"
exec 6>"$FIFO_DIR/metrics"

# Cleanup on exit
cleanup() {
  exec 3>&- 4>&- 5>&- 6>&-
  rm -rf "$FIFO_DIR"
  kill $(jobs -p) 2>/dev/null || true
}
trap cleanup EXIT

i=0
while true; do
  i=$((i + 1))

  # ─── orders: normally 200-800 bytes ───
  if [ $((i % 10)) -eq 0 ]; then
    # Oversized: ~1.2MB → triggers WARNING alert
    size=1258291
    echo "[seed] >>> OVERSIZED orders message: ${size} bytes (iteration $i) — should trigger WARNING"
  else
    size=$((200 + RANDOM % 600))
  fi
  echo "$(generate_payload $size)" >&3

  # ─── users: normally 100-500 bytes ───
  if [ $((i % 25)) -eq 0 ]; then
    # Oversized: ~2MB → triggers WARNING alert
    size=2097152
    echo "[seed] >>> OVERSIZED users message: ${size} bytes (iteration $i) — should trigger WARNING"
  else
    size=$((100 + RANDOM % 400))
  fi
  echo "$(generate_payload $size)" >&4

  # ─── logs: normally 500-5000 bytes ───
  if [ $((i % 15)) -eq 0 ]; then
    # Oversized: ~12MB → triggers CRITICAL alert
    size=12582912
    echo "[seed] >>> OVERSIZED logs message: ${size} bytes (iteration $i) — should trigger CRITICAL"
  else
    size=$((500 + RANDOM % 4500))
  fi
  echo "$(generate_payload $size)" >&5

  # ─── metrics: tiny 50-200 bytes, high frequency (3 per iteration) ───
  for j in 1 2 3; do
    size=$((50 + RANDOM % 150))
    echo "$(generate_payload $size)" >&6
  done

  # Log progress every 50 iterations
  if [ $((i % 50)) -eq 0 ]; then
    echo "[seed] Produced ~$((i * 6)) messages (iteration $i)"
  fi

  # ~1 second between batches
  sleep 1
done
