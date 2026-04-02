#!/usr/bin/env bash
# Arranca Airflow en local (sin Docker) con variables para ingesta SIMLOG.
# Requiere: pip install apache-airflow (mismo venv que el proyecto recomendado).
#
#   ./scripts/run_airflow_local.sh
#
# Luego en otra terminal (mismo venv):
#   ./scripts/trigger_simlog_maestro.sh
#
# UI suele ser http://127.0.0.1:8080 (Airflow 2 standalone). Credenciales en la salida de standalone.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

if [[ -f "$ROOT/venv_transporte/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source "$ROOT/venv_transporte/bin/activate"
elif [[ -f "$ROOT/.venv/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source "$ROOT/.venv/bin/activate"
fi

# shellcheck source=/dev/null
source "$SCRIPT_DIR/airflow_local_env.sh"

bash "$SCRIPT_DIR/setup_airflow_local_dags.sh"

echo "[run-airflow-local] SIMLOG_PROJECT_ROOT=$SIMLOG_PROJECT_ROOT"
echo "[run-airflow-local] SIMLOG_AIRFLOW_LOCAL=$SIMLOG_AIRFLOW_LOCAL (1 = solo ingesta, sin Spark ni comprobaciones de cluster)"
echo "[run-airflow-local] AIRFLOW_HOME=$AIRFLOW_HOME"
echo "[run-airflow-local] Disparar una ejecución: $SCRIPT_DIR/trigger_simlog_maestro.sh"
echo ""

if ! command -v airflow >/dev/null 2>&1; then
  echo "[run-airflow-local] ERROR: no hay 'airflow' en PATH. Activa el venv e instala: pip install 'apache-airflow'"
  exit 1
fi

airflow db migrate

# Preferir standalone (Airflow 2.x); si no existe el subcomando, indica el modo manual.
if airflow standalone 2>&1 | head -n 1 | grep -qi 'unknown\|invalid\|No such command'; then
  echo "[run-airflow-local] Este Airflow no incluye 'standalone'. En terminales separadas (mismo env):"
  echo "  airflow scheduler"
  echo "  airflow api-server -H 0.0.0.0 -p 8080   # o: airflow webserver -p 8080"
  exit 1
fi

# Si llegamos aqui, standalone fallo por otra razon — reintento directo
exec airflow standalone
