#!/usr/bin/env bash
# Arranca todos los servicios del stack SIMLOG en orden secuencial (HDFS → … → NiFi).
# Airflow: api-server + scheduler cuando corresponda (ver servicios/gestion_servicios.py).

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PY="${ROOT}/venv_transporte/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="python3"
fi

exec "$PY" -u "${ROOT}/scripts/simlog_stack.py" start
