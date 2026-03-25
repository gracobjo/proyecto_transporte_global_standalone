#!/usr/bin/env bash
# Detiene todos los servicios del stack en orden secuencial inverso (NiFi → … → HDFS).

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PY="${ROOT}/venv_transporte/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="python3"
fi

exec "$PY" -u "${ROOT}/scripts/simlog_stack.py" stop
