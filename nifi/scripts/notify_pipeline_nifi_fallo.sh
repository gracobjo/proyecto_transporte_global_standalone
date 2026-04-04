#!/usr/bin/env bash
# Llamado por NiFi si spark-submit falla (ExecuteStreamCommand → relación failure).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
PY="${ROOT}/venv_transporte/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="python3"
fi
MSG="${SIMLOG_NIFI_NOTIFY_MSG:-Fallo en spark-submit / procesamiento_grafos desde NiFi. Ver reports/nifi_spark/.}"
exec "$PY" "${ROOT}/scripts/notificar_pipeline_simlog.py" --origen nifi --fallo --mensaje "$MSG"
