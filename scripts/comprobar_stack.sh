#!/usr/bin/env bash
# Muestra qué servicios del stack responden (puertos / comprobación ligera).

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PY="${ROOT}/venv_transporte/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="python3"
fi

exec "$PY" -u "${ROOT}/scripts/simlog_stack.py" status
