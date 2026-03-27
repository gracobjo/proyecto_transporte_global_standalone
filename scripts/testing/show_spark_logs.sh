#!/usr/bin/env bash
set -euo pipefail

LINES="${LINES:-200}"
SPARK_LOG_DIR="${SPARK_LOG_DIR:-${SPARK_HOME:-/opt/spark}/logs}"

echo "[SPARK] Directorio de logs: ${SPARK_LOG_DIR}"
if [[ ! -d "${SPARK_LOG_DIR}" ]]; then
  echo "No existe el directorio de logs."
  exit 1
fi

for f in "${SPARK_LOG_DIR}"/*.out "${SPARK_LOG_DIR}"/*.log; do
  [[ -e "${f}" ]] || continue
  echo "===== ${f} ====="
  tail -n "${LINES}" "${f}" || true
done
