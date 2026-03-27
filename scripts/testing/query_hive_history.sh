#!/usr/bin/env bash
set -euo pipefail

HIVE_DB="${HIVE_DB:-logistica_espana}"
HIVE_JDBC_URL="${HIVE_JDBC_URL:-jdbc:hive2://127.0.0.1:10000/${HIVE_DB}}"
HIVE_USER="${SIMLOG_HIVE_BEELINE_USER:-${USER:-hadoop}}"
BEELINE_BIN="${HIVE_BEELINE_BIN:-beeline}"

SQL="${1:-SHOW TABLES IN ${HIVE_DB};}"

echo "[HIVE] JDBC=${HIVE_JDBC_URL} · DB=${HIVE_DB}"
"${BEELINE_BIN}" -u "${HIVE_JDBC_URL}" -n "${HIVE_USER}" -e "${SQL}"
