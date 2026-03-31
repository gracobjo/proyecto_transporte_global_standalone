"""
SIMLOG — DAG maestro compacto: ingesta + Spark (Cassandra/Hive).

**dag_id:** `simlog_maestro` (mismo prefijo `simlog_` que el resto de DAGs del proyecto).

**Frente a `simlog_kdd_*`:** la cadena KDD (00→…→99) es **manual** (`schedule=None`): disparas cada fase
cuando quieres. Este DAG está **programado** por defecto **cada 15 minutos**: en cada tick ejecuta
ingesta y luego procesamiento (incluye Hive si el cluster responde). Sirve para demo/operación continua
sin encadenar fases.

**Despliegue Airflow** (symlink recomendado):

  ln -sf /ruta/al/proyecto/orquestacion/dag_simlog_maestro.py \\
         "$AIRFLOW_HOME/dags/simlog/dag_simlog_maestro.py"

Si el .py se copia fuera de `orquestacion/`, definir `SIMLOG_PROJECT_ROOT`.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def _project_base() -> Path:
    here = Path(__file__).resolve()
    if here.parent.name == "orquestacion":
        return here.parent.parent
    env_root = os.environ.get("SIMLOG_PROJECT_ROOT", "").strip()
    if env_root:
        return Path(env_root)
    raise RuntimeError(
        "Este DAG debe vivir en proyecto/.../orquestacion/ (symlink desde Airflow) "
        "o definir la variable de entorno SIMLOG_PROJECT_ROOT con la raíz del repositorio."
    )


BASE = _project_base()


def verificar_hdfs(**context):
    import subprocess

    r = subprocess.run(["hdfs", "dfs", "-ls", "/"], capture_output=True, text=True, timeout=20)
    if r.returncode != 0:
        msg = (r.stderr or r.stdout or "HDFS no disponible").strip()
        raise RuntimeError(f"HDFS no disponible: {msg[:280]}")
    return "OK"


def verificar_kafka(**context):
    import socket

    for srv in ["localhost:9092", "127.0.0.1:9092"]:
        host, port = srv.split(":")[0], int(srv.split(":")[1])
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            sock.connect((host, port))
            sock.close()
            return "OK"
        except Exception:
            continue
    raise RuntimeError("Kafka no disponible en localhost:9092")


def verificar_cassandra(**context):
    import socket

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        sock.connect(("127.0.0.1", 9042))
        sock.close()
        return "OK"
    except Exception as e:
        raise RuntimeError(f"Cassandra no disponible: {e}")


def ejecutar_ingesta(**context):
    import subprocess

    venv_python = BASE / "venv_transporte" / "bin" / "python"
    if not venv_python.exists():
        venv_python = BASE / "venv" / "bin" / "python"
    python_bin = str(venv_python) if venv_python.exists() else "python3"
    r = subprocess.run(
        [python_bin, "-m", "ingesta.ingesta_kdd"],
        env={**os.environ},
        capture_output=True,
        text=True,
        cwd=str(BASE),
        timeout=90,
    )
    if r.returncode != 0:
        raise RuntimeError(f"Ingesta falló: {r.stderr or r.stdout}")


def ejecutar_procesamiento(**context):
    import subprocess

    venv_python = BASE / "venv_transporte" / "bin" / "python"
    if not venv_python.exists():
        venv_python = BASE / "venv" / "bin" / "python"
    python_bin = str(venv_python) if venv_python.exists() else "python3"
    script = BASE / "procesamiento" / "procesamiento_grafos.py"
    r = subprocess.run(
        [python_bin, str(script)],
        env={**os.environ, "SIMLOG_ENABLE_HIVE": "1"},
        capture_output=True,
        text=True,
        cwd=str(BASE),
        timeout=int(os.environ.get("SIMLOG_AIRFLOW_SPARK_TIMEOUT_SEC", "900")),
    )
    if r.returncode != 0:
        raise RuntimeError(f"Procesamiento falló: {r.stderr or r.stdout}")


default_args = {
    "owner": "simlog",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retries": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Programación: cada 15 min (ingesta + Spark en cada ejecución). Para solo manual: schedule=None.
_INGESTA_MIN = int(os.environ.get("SIMLOG_INGESTA_INTERVAL_MINUTES", "15"))
_INGESTA_MIN = max(1, _INGESTA_MIN)

with DAG(
    dag_id="simlog_maestro",
    default_args=default_args,
    description=(
        "SIMLOG — Ingesta (`ingesta_kdd.py`) + Spark (`procesamiento_grafos`, Hive con SIMLOG_ENABLE_HIVE). "
        "Programado cada SIMLOG_INGESTA_INTERVAL_MINUTES (15 por defecto). "
        "Alternativa a la cadena simlog_kdd_00…99 (manual)."
    ),
    schedule=timedelta(minutes=_INGESTA_MIN),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["simlog", "kdd", "transporte", "proyecto_simlog", "spain"],
) as dag:

    check_hdfs = PythonOperator(
        task_id="verificar_hdfs",
        python_callable=verificar_hdfs,
    )
    check_kafka = PythonOperator(
        task_id="verificar_kafka",
        python_callable=verificar_kafka,
    )
    check_cassandra = PythonOperator(
        task_id="verificar_cassandra",
        python_callable=verificar_cassandra,
    )
    ingesta = PythonOperator(
        task_id="ejecutar_ingesta",
        python_callable=ejecutar_ingesta,
    )
    procesamiento = PythonOperator(
        task_id="ejecutar_procesamiento",
        python_callable=ejecutar_procesamiento,
    )

    [check_hdfs, check_kafka, check_cassandra] >> ingesta >> procesamiento
