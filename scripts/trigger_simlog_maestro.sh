#!/usr/bin/env bash
# Dispara el DAG simlog_maestro (ingesta + opcionalmente Spark según env).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ -f "$ROOT/venv_transporte/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source "$ROOT/venv_transporte/bin/activate"
elif [[ -f "$ROOT/.venv/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source "$ROOT/.venv/bin/activate"
fi

# shellcheck source=/dev/null
source "$SCRIPT_DIR/airflow_local_env.sh"

exec airflow dags trigger simlog_maestro "$@"
