"""
SIMLOG — Comprobación de servicios del stack (HDFS, Kafka, Cassandra, Spark, Hive, …).

**dag_id:** `dag_comprobar_servicios_simlog`

Ejecutar manualmente (Trigger DAG). Falla la tarea si algún servicio no responde
según `servicios.gestion_servicios.comprobar_todos()`.

Despliegue (symlink recomendado, como el resto de DAGs):

  ln -sf /ruta/al/proyecto/orquestacion/dag_comprobar_servicios_simlog.py \\
         "$AIRFLOW_HOME/dags/simlog/dag_comprobar_servicios_simlog.py"

Si el .py se copia fuera del repo, definir `SIMLOG_PROJECT_ROOT` con la raíz del proyecto.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

_ORQ = Path(__file__).resolve().parent
_PROJ = _ORQ.parent
if str(_PROJ) not in sys.path:
    sys.path.insert(0, str(_PROJ))
env_root = os.environ.get("SIMLOG_PROJECT_ROOT", "").strip()
if env_root:
    _p = Path(env_root)
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))


def ejecutar_comprobacion(**context) -> dict:
    from servicios.gestion_servicios import comprobar_todos

    resultados = comprobar_todos()
    inactivos = [r for r in resultados if not r.get("activo")]
    if inactivos:
        lines = "\n".join(
            f"- {r.get('nombre', r.get('id'))}: {r.get('detalle', '')}" for r in inactivos
        )
        raise RuntimeError(f"Servicios inactivos ({len(inactivos)}):\n{lines}")
    return {"ok": True, "servicios_comprobados": len(resultados)}


default_args = {
    "owner": "simlog",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dag_comprobar_servicios_simlog",
    default_args=default_args,
    description="SIMLOG — Comprobar que el stack (HDFS, Kafka, Cassandra, Spark, Hive, …) responde",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["simlog", "servicios", "proyecto_simlog", "spain"],
) as dag:
    PythonOperator(
        task_id="comprobar_servicios",
        python_callable=ejecutar_comprobacion,
    )
