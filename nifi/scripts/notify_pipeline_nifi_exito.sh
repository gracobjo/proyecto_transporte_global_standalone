#!/usr/bin/env bash
# Llamado por NiFi tras spark-submit OK (ExecuteStreamCommand).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
PY="${ROOT}/venv_transporte/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="python3"
fi
exec "$PY" "${ROOT}/scripts/notificar_pipeline_simlog.py" --origen nifi --exito \
  --mensaje "Spark/Cassandra/Hive completados desde flujo NiFi (PG_SIMLOG_KDD)."
