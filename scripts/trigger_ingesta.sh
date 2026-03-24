#!/usr/bin/env bash
# Dispara la ingesta KDD sin PASO_15MIN fijo → paso automático por reloj (ver ingesta/trigger_paso.py).
# Uso: cron cada 15 min en prod, o cada 2 min en demo (ajusta SIMLOG_INGESTA_INTERVAL_MINUTES).

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PY="${ROOT}/venv_transporte/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="python3"
fi

# Opcional: export SIMLOG_INGESTA_INTERVAL_MINUTES=15  # o 2 para presentación
unset PASO_15MIN 2>/dev/null || true
exec "$PY" -m ingesta.ingesta_kdd
