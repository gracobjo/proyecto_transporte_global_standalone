#!/usr/bin/env bash
# Enlaza los DAGs mínimos en $AIRFLOW_HOME/dags/simlog/ (import de hdfs_check_airflow).
# Opcional: sincroniza symlinks en la raíz de dags (Airflow 3 / bundles).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow}"
DAGS_SIMLOG="${AIRFLOW_DAGS_SIMLOG:-$AIRFLOW_HOME/dags/simlog}"

mkdir -p "$DAGS_SIMLOG"
ln -sfn "$ROOT/orquestacion/dag_simlog_maestro.py" "$DAGS_SIMLOG/dag_simlog_maestro.py"
ln -sfn "$ROOT/orquestacion/hdfs_check_airflow.py" "$DAGS_SIMLOG/hdfs_check_airflow.py"

export AIRFLOW_DAGS_ROOT="${AIRFLOW_DAGS_ROOT:-$AIRFLOW_HOME/dags}"
if [[ -f "$ROOT/scripts/sync_airflow_dags_root_symlinks.sh" ]]; then
  bash "$ROOT/scripts/sync_airflow_dags_root_symlinks.sh"
fi

echo "[setup-airflow-local] DAGs: $DAGS_SIMLOG -> simlog_maestro + hdfs_check_airflow"
