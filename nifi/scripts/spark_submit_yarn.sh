#!/usr/bin/env bash
# NiFi ExecuteProcess → spark-submit (YARN o local).
# Toda la salida de Spark va a un fichero bajo reports/nifi_spark/ (o SIMLOG_NIFI_SPARK_LOG_DIR).
# stderr solo recibe texto si el job falla (resumen + últimas líneas del log), para que NiFi no marque INFO como WARNING.

set -euo pipefail

export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export PROJECT_ROOT="${PROJECT_ROOT:-$HOME/proyecto_transporte_global}"
export SPARK_MASTER="${SPARK_MASTER:-yarn}"
export DEPLOY_MODE="${DEPLOY_MODE:-client}"

LOG_DIR="${SIMLOG_NIFI_SPARK_LOG_DIR:-${PROJECT_ROOT}/reports/nifi_spark}"
mkdir -p "$LOG_DIR"
TS="$(date +%Y%m%d_%H%M%S)_$$"
LOG_FILE="${LOG_DIR}/spark_submit_${TS}.log"

{
  echo "=== SIMLOG spark-submit $(date -Is) ==="
  echo "PROJECT_ROOT=${PROJECT_ROOT} SPARK_MASTER=${SPARK_MASTER} DEPLOY_MODE=${DEPLOY_MODE}"
  echo "---"
} >"$LOG_FILE"

_run_spark_submit() {
  if "${SPARK_HOME}/bin/spark-submit" "$@" >>"$LOG_FILE" 2>&1; then
    return 0
  fi
  local rc=$?
  echo "[SIMLOG] spark-submit falló (código de salida ${rc})." >&2
  echo "[SIMLOG] Log completo: ${LOG_FILE}" >&2
  echo "[SIMLOG] --- Últimas líneas ---" >&2
  tail -n 150 "$LOG_FILE" >&2
  return "$rc"
}

if [[ "${SPARK_MASTER}" == "local" ]]; then
  _run_spark_submit \
    --master local \
    --conf spark.driver.memory=512m \
    --conf spark.driver.host=127.0.0.1 \
    --conf spark.driver.bindAddress=127.0.0.1 \
    --conf spark.cassandra.connection.host="${CASSANDRA_HOST:-127.0.0.1}" \
    "${PROJECT_ROOT}/procesamiento/procesamiento_grafos.py"
  exit 0
fi

_run_spark_submit \
  --master "${SPARK_MASTER}" \
  --deploy-mode "${DEPLOY_MODE}" \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=512m \
  --conf spark.executor.instances=1 \
  --conf spark.cassandra.connection.host="${CASSANDRA_HOST:-127.0.0.1}" \
  --conf spark.yarn.submit.waitAppCompletion=true \
  "${PROJECT_ROOT}/procesamiento/procesamiento_grafos.py"
