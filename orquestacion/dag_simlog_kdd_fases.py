"""
SIMLOG — DAGs por fase KDD (ejecución secuencial encadenada con TriggerDagRunOperator).

Orden: 00_infra → 01_seleccion → 02_preprocesamiento → 03_transformacion → 04_mineria
       → 05_interpretacion → 99_consulta_final

Cada DAG genera informes Markdown/HTML bajo reports/kdd/<run_id>/.

Despliegue: copiar todo el directorio `orquestacion/` (o al menos kdd_*.py, servicios_arranque.py,
kdd_ejecucion.py, kdd_informe.py y este fichero) al path de DAGs de Airflow, con PYTHONPATH
apuntando al proyecto si hace falta.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

_ORQ = Path(__file__).resolve().parent
if str(_ORQ) not in sys.path:
    sys.path.insert(0, str(_ORQ))
_PROJ = _ORQ.parent
if str(_PROJ) not in sys.path:
    sys.path.insert(0, str(_PROJ))

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from kdd_ejecucion import (
    tarea_fase00,
    tarea_fase01,
    tarea_fase02,
    tarea_fase03,
    tarea_fase04,
    tarea_fase05,
    tarea_fase99,
)

_DEFAULT = {
    "owner": "simlog",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

_TAGS = ["simlog", "kdd", "spain"]


def _trigger(dag_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=f"trigger__{dag_id}",
        trigger_dag_id=dag_id,
        wait_for_completion=False,
    )


# --- 00 ---
with DAG(
    dag_id="simlog_kdd_00_infra",
    default_args=_DEFAULT,
    description="SIMLOG KDD Fase 0: arranque servicios + verificación + informe",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=_TAGS,
) as _:
    t0 = PythonOperator(
        task_id="fase_0_infra_informe",
        python_callable=tarea_fase00,
    )
    t0 >> _trigger("simlog_kdd_01_seleccion")


# --- 01 ---
with DAG(
    dag_id="simlog_kdd_01_seleccion",
    default_args=_DEFAULT,
    description="SIMLOG KDD Fase 1: selección (API clima) + informe",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=_TAGS,
) as _:
    t1 = PythonOperator(
        task_id="fase_1_seleccion_informe",
        python_callable=tarea_fase01,
    )
    t1 >> _trigger("simlog_kdd_02_preprocesamiento")


# --- 02 ---
with DAG(
    dag_id="simlog_kdd_02_preprocesamiento",
    default_args=_DEFAULT,
    description="SIMLOG KDD Fase 2: preprocesamiento (simulación, Kafka, HDFS) + informe",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=_TAGS,
) as _:
    t2 = PythonOperator(
        task_id="fase_2_preprocesamiento_informe",
        python_callable=tarea_fase02,
    )
    t2 >> _trigger("simlog_kdd_03_transformacion")


# --- 03 ---
with DAG(
    dag_id="simlog_kdd_03_transformacion",
    default_args=_DEFAULT,
    description="SIMLOG KDD Fase 3: transformación (GraphFrames) + informe",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=_TAGS,
) as _:
    t3 = PythonOperator(
        task_id="fase_3_transformacion_informe",
        python_callable=tarea_fase03,
    )
    t3 >> _trigger("simlog_kdd_04_mineria")


# --- 04 ---
with DAG(
    dag_id="simlog_kdd_04_mineria",
    default_args=_DEFAULT,
    description="SIMLOG KDD Fase 4: minería (PageRank) + informe",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=_TAGS,
) as _:
    t4 = PythonOperator(
        task_id="fase_4_mineria_informe",
        python_callable=tarea_fase04,
    )
    t4 >> _trigger("simlog_kdd_05_interpretacion")


# --- 05 ---
with DAG(
    dag_id="simlog_kdd_05_interpretacion",
    default_args=_DEFAULT,
    description=(
        "SIMLOG KDD Fase 5: Spark interpretación — fase_kdd_spark interpretacion → "
        "procesamiento_grafos.main() (lee JSON en HDFS, Cassandra, Hive con SIMLOG_ENABLE_HIVE). "
        "No ejecutar en paralelo con simlog_maestro (mismo main(); ver procesamiento_singleton). "
        "Un solo task Airflow; no hay sub-tareas separadas HDFS/Hive en el grafo."
    ),
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=_TAGS,
) as _:
    t5 = PythonOperator(
        task_id="fase_5_interpretacion_informe",
        python_callable=tarea_fase05,
    )
    t5 >> _trigger("simlog_kdd_99_consulta_final")


# --- 99 ---
with DAG(
    dag_id="simlog_kdd_99_consulta_final",
    default_args=_DEFAULT,
    description="SIMLOG KDD: consulta final Cassandra + informe (no para servicios)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=_TAGS,
) as _:
    PythonOperator(
        task_id="fase_99_consulta_informe",
        python_callable=tarea_fase99,
    )
