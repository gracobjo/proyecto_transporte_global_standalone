#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
TOPICS_BIN="${KAFKA_TOPICS_BIN:-kafka-topics.sh}"

echo "[KAFKA] Bootstrap: ${BOOTSTRAP}"
"${TOPICS_BIN}" --list --bootstrap-server "${BOOTSTRAP}"
