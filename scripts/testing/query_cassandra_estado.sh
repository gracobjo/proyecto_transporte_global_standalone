#!/usr/bin/env bash
set -euo pipefail

KEYSPACE="${KEYSPACE:-logistica_espana}"
HOST="${CASSANDRA_HOST:-127.0.0.1}"
CQLSH_BIN="${CQLSH_BIN:-cqlsh}"
QUERY="${1:-SELECT COUNT(*) FROM tracking_camiones;}"

echo "[CASSANDRA] Host=${HOST} · keyspace=${KEYSPACE}"
"${CQLSH_BIN}" "${HOST}" -e "USE ${KEYSPACE}; ${QUERY}"
