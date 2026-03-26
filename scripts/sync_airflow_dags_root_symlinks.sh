#!/usr/bin/env bash
# Crea en la raíz de AIRFLOW dags/ enlaces con el mismo nombre que los .py en
# simlog/ y smart_grid/. Airflow 3 puede asociar tareas al bundle «dags-folder»
# con path relativo a esa raíz; sin el enlace, el worker no encuentra el DAG.
#
# No sobrescribe ficheros regulares (p. ej. dag_comprobar_servicios.py shim legacy).
#
# Uso:
#   ./scripts/sync_airflow_dags_root_symlinks.sh
#   AIRFLOW_DAGS_ROOT=/ruta/a/dags ./scripts/sync_airflow_dags_root_symlinks.sh

set -euo pipefail
ROOT="${AIRFLOW_DAGS_ROOT:-$HOME/airflow/dags}"

for sub in simlog smart_grid; do
  dir="$ROOT/$sub"
  [[ -d "$dir" ]] || continue
  for f in "$dir"/*.py; do
    [[ -f "$f" ]] || continue
    base=$(basename "$f")
    target="$ROOT/$base"
    if [[ -e "$target" && ! -L "$target" ]]; then
      echo "SKIP (fichero real, no symlink): $target"
      continue
    fi
    ln -sfn "$f" "$target"
    echo "OK $target -> $f"
  done
done
