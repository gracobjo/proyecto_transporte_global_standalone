"""
SIMLOG — DAG maestro compacto: ingesta + Spark (Cassandra/Hive).

**dag_id:** `simlog_maestro` (mismo prefijo `simlog_` que el resto de DAGs del proyecto).

**Frente a `simlog_kdd_*`:** la cadena KDD (00→…→99) es **manual** (`schedule=None`): disparas cada fase
cuando quieres. Este DAG está **programado** por defecto **cada 15 minutos**: en cada tick ejecuta
ingesta y luego procesamiento (incluye Hive si el cluster responde). Sirve para demo/operación continua
sin encadenar fases.

**No solapar con `simlog_kdd_05_interpretacion`:** la tarea ``ejecutar_procesamiento`` llama a
``procesamiento_grafos.main()``, igual que la fase KDD 5 (``fase_kdd_spark interpretacion``). Si ambos
corren a la vez, compiten por Spark, Hive/Metastore y Cassandra y parece que «se quedan colgados».
El repo aplica un **flock** compartido (``orquestacion/procesamiento_singleton.py``) para serializar
ese paso en el mismo host. Aun así, conviene **pausar** uno de los dos DAGs si no necesitas ambos.

**Despliegue Airflow** (symlink recomendado):

  ln -sf /ruta/al/proyecto/orquestacion/dag_simlog_maestro.py \\
         "$AIRFLOW_HOME/dags/simlog/dag_simlog_maestro.py"

Si el .py se copia fuera de `orquestacion/`, definir `SIMLOG_PROJECT_ROOT`.

Modo **local sin Docker** (solo ingesta, sin HDFS/Kafka/Cassandra/Spark):

  export SIMLOG_AIRFLOW_LOCAL=1

Equivale a omitir comprobaciones de servicios y la tarea Spark. Alternativa granular:
`SIMLOG_SKIP_HDFS_CHECK`, `SIMLOG_SKIP_KAFKA_CHECK`, `SIMLOG_SKIP_CASSANDRA_CHECK`,
`SIMLOG_AIRFLOW_SKIP_PROCESAMIENTO`.

Timeout de la tarea Spark: `SIMLOG_AIRFLOW_SPARK_TIMEOUT_SEC` (segundos; por defecto **3600**,
mínimo 300). Si ves `TimeoutExpired` tras 900 s en despliegues antiguos, sube la variable o
actualiza este DAG.
"""
from __future__ import annotations

import os
import sys
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
    docker_mount = Path("/opt/proyecto_transporte_global")
    if docker_mount.exists():
        return docker_mount
    for p in here.parents:
        if (p / "ingesta").is_dir() and (p / "config.py").is_file():
            return p
    raise RuntimeError(
        "No se pudo determinar la raíz del repositorio. "
        "Usa un symlink desde orquestacion/dag_simlog_maestro.py, define SIMLOG_PROJECT_ROOT "
        "o monta el repo en /opt/proyecto_transporte_global."
    )


BASE = _project_base()

_orq = Path(__file__).resolve().parent
if str(_orq) not in sys.path:
    sys.path.insert(0, str(_orq))
from hdfs_check_airflow import verificar_hdfs_airflow as verificar_hdfs  # noqa: E402
from procesamiento_singleton import lock_procesamiento_grafos  # noqa: E402


def _env_yes(name: str) -> bool:
    v = (os.environ.get(name) or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _airflow_local_mode() -> bool:
    return _env_yes("SIMLOG_AIRFLOW_LOCAL")


def verificar_kafka(**context):
    if _airflow_local_mode() or _env_yes("SIMLOG_SKIP_KAFKA_CHECK"):
        return "SKIP (Kafka no requerido en modo local o por SIMLOG_SKIP_KAFKA_CHECK)"

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
    if _airflow_local_mode() or _env_yes("SIMLOG_SKIP_CASSANDRA_CHECK"):
        return "SKIP (Cassandra no requerida en modo local)"

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
    timeout_sec = int(
        os.environ.get(
            "SIMLOG_AIRFLOW_INGESTA_TIMEOUT_SEC",
            os.environ.get("SIMLOG_INGESTA_TIMEOUT_SEC", "180"),
        )
    )
    timeout_sec = max(30, timeout_sec)
    r = subprocess.run(
        [python_bin, "-m", "ingesta.ingesta_kdd"],
        env={**os.environ},
        capture_output=True,
        text=True,
        cwd=str(BASE),
        timeout=timeout_sec,
    )
    if r.returncode != 0:
        raise RuntimeError(f"Ingesta falló: {r.stderr or r.stdout}")


def ejecutar_procesamiento(**context):
    if _airflow_local_mode() or _env_yes("SIMLOG_AIRFLOW_SKIP_PROCESAMIENTO"):
        return "SKIP (Spark/procesamiento omitido en modo local o por SIMLOG_AIRFLOW_SKIP_PROCESAMIENTO)"

    import subprocess

    venv_python = BASE / "venv_transporte" / "bin" / "python"
    if not venv_python.exists():
        venv_python = BASE / "venv" / "bin" / "python"
    python_bin = str(venv_python) if venv_python.exists() else "python3"
    script = BASE / "procesamiento" / "procesamiento_grafos.py"
    # Mismo orden de magnitud que la cadena KDD: Spark+Hive en frío suele superar 15 min.
    try:
        timeout_spark = int(os.environ.get("SIMLOG_AIRFLOW_SPARK_TIMEOUT_SEC", "3600"))
    except ValueError:
        timeout_spark = 3600
    timeout_spark = max(300, timeout_spark)
    with lock_procesamiento_grafos():
        r = subprocess.run(
            [python_bin, str(script)],
            env={**os.environ, "SIMLOG_ENABLE_HIVE": "1"},
            capture_output=True,
            text=True,
            cwd=str(BASE),
            timeout=timeout_spark,
        )
    if r.returncode != 0:
        raise RuntimeError(f"Procesamiento falló: {r.stderr or r.stdout}")


default_args = {
    "owner": "simlog",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
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
