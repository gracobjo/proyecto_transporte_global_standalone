#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
TOPIC="${1:-transporte_filtered}"
CONSUMER_BIN="${KAFKA_CONSOLE_CONSUMER_BIN:-kafka-console-consumer.sh}"
MAX_MESSAGES="${MAX_MESSAGES:-5}"
TIMEOUT_MS="${TIMEOUT_MS:-8000}"

echo "[KAFKA] Topic: ${TOPIC} · bootstrap: ${BOOTSTRAP} · max_messages=${MAX_MESSAGES}"
"${CONSUMER_BIN}" \
  --bootstrap-server "${BOOTSTRAP}" \
  --topic "${TOPIC}" \
  --from-beginning \
  --max-messages "${MAX_MESSAGES}" \
  --timeout-ms "${TIMEOUT_MS}"
