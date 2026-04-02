"""
SIMLOG España — orquestación Airflow (pipeline compacto, legacy)

Preferir el flujo por fases KDD: `dag_simlog_kdd_fases.py` (un DAG por fase, informes y cadena TriggerDagRun).

Este DAG sigue siendo válido para ejecución periódica cada 15 min (ingesta + procesamiento en un solo grafo).

Procesamiento: debe ejecutar `procesamiento/procesamiento_grafos.py` (Spark lee JSON en HDFS, escribe
Cassandra y, con SIMLOG_ENABLE_HIVE=1, Hive en `logistica_espana`). El script suelto `procesamiento_grafos.py`
en la raíz es legacy y no equivale a este pipeline.
"""
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def _project_base() -> Path:
    """
    Devuelve la raíz del repositorio para ejecutar scripts del proyecto.

    Nota: cuando Airflow carga este fichero desde `AIRFLOW_HOME/dags`, `__file__` ya no está en
    `.../orquestacion/` y `parent.parent` apuntaría a `/opt/airflow` (rompiendo rutas relativas).
    """
    here = Path(__file__).resolve()
    if here.parent.name == "orquestacion":
        return here.parent.parent
    env_root = os.environ.get("SIMLOG_PROJECT_ROOT", "").strip()
    if env_root:
        return Path(env_root)
    docker_mount = Path("/opt/proyecto_transporte_global")
    if docker_mount.exists():
        return docker_mount
    raise RuntimeError(
        "No se pudo determinar la raíz del proyecto. "
        "Define SIMLOG_PROJECT_ROOT (ruta al repo) o monta el repo en /opt/proyecto_transporte_global."
    )


BASE = _project_base()

_orq = Path(__file__).resolve().parent
if str(_orq) not in sys.path:
    sys.path.insert(0, str(_orq))
from hdfs_check_airflow import verificar_hdfs_airflow as verificar_hdfs  # noqa: E402

# Verificación de servicios
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
    interval_min = int(os.environ.get("SIMLOG_INGESTA_INTERVAL_MINUTES", "15"))
    ds = context.get("data_interval_start")
    if ds is None:
        ds = datetime.now(timezone.utc)
    elif getattr(ds, "tzinfo", None) is None:
        ds = ds.replace(tzinfo=timezone.utc)
    # Paso alineado con la ventana del DAG (cada intervalo, datos distintos)
    paso_val = int(ds.timestamp() // (max(1, interval_min) * 60))
    r = subprocess.run(
        [f"{BASE}/venv_transporte/bin/python", "-m", "ingesta.ingesta_kdd"],
        env={
            **os.environ,
            "PASO_15MIN": str(paso_val),
            "SIMLOG_INGESTA_CANAL": "airflow",
            "SIMLOG_INGESTA_ORIGEN": "airflow_dag_maestro",
            "SIMLOG_INGESTA_EJECUTOR": "simlog_pipeline_maestro.ejecutar_ingesta",
        },
        capture_output=True,
        text=True,
        cwd=str(BASE),
        timeout=180,
    )
    if r.returncode != 0:
        raise RuntimeError(f"Ingesta falló: {r.stderr or r.stdout}")

def _python_ejecucion() -> str:
    """Python del proyecto (venv si existe) o el mismo intérprete que Airflow."""
    venv_py = BASE / "venv_transporte" / "bin" / "python"
    if venv_py.is_file():
        return str(venv_py)
    return sys.executable


def ejecutar_procesamiento(**context):
    """
    Spark: `procesamiento/procesamiento_grafos.main()` — lee JSON desde HDFS (backup ingesta),
    enriquece con Hive (`nodos_maestro`) si aplica, persiste Cassandra + tablas Hive histórico.
    Requiere SIMLOG_ENABLE_HIVE=1 para enableHiveSupport() (histórico particionado).
    """
    import subprocess

    script = BASE / "procesamiento" / "procesamiento_grafos.py"
    r = subprocess.run(
        [_python_ejecucion(), str(script)],
        env={**os.environ, "SIMLOG_ENABLE_HIVE": "1"},
        capture_output=True,
        text=True,
        cwd=str(BASE),
        timeout=int(os.environ.get("SIMLOG_AIRFLOW_SPARK_TIMEOUT_SEC", "900")),
    )
    if r.returncode != 0:
        raise RuntimeError(f"Procesamiento falló: {r.stderr or r.stdout}")

_INGESTA_INTERVAL_MIN = int(os.environ.get("SIMLOG_INGESTA_INTERVAL_MINUTES", "15"))
_INGESTA_INTERVAL_MIN = max(1, _INGESTA_INTERVAL_MIN)

default_args = {
    "owner": "logistica",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="simlog_pipeline_maestro",
    default_args=default_args,
    description=(
        "SIMLOG — Ingesta + Spark (lee HDFS backup, Cassandra + Hive logistica_espana). "
        "Una sola tarea Python agrupa Spark; no hay sub-tarea separada «Hive» en la UI."
    ),
    schedule=timedelta(minutes=_INGESTA_INTERVAL_MIN),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["simlog", "logistica", "transporte", "spain", "hdfs", "hive", "spark"],
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
        task_id="spark_procesamiento_hdfs_cassandra_hive",
        python_callable=ejecutar_procesamiento,
    )

    [check_hdfs, check_kafka, check_cassandra] >> ingesta >> procesamiento
