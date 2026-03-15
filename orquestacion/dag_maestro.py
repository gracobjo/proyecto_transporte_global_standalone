"""
Sistema de Gemelo Digital Logístico - Orquestación Airflow
DAG que cada 15 minutos:
- Verifica disponibilidad de HDFS, Kafka, Cassandra
- Ejecuta Ingesta -> Spark Processing
- Evita solapamiento de procesos (no agotar RAM 4GB)
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

BASE = Path(__file__).resolve().parent.parent

# Verificación de servicios
def verificar_hdfs(**context):
    import subprocess
    r = subprocess.run(["hdfs", "dfs", "-ls", "/"], capture_output=True)
    if r.returncode != 0:
        raise RuntimeError("HDFS no disponible")
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
    import os
    ds = context.get("data_interval_start") or datetime.utcnow()
    paso_val = ds.hour * 4 + ds.minute // 15
    r = subprocess.run(
        [f"{BASE}/venv_transporte/bin/python", str(BASE / "ingesta_kdd.py")],
        env={**os.environ, "PASO_15MIN": str(paso_val)},
        capture_output=True,
        text=True,
        cwd=str(BASE),
        timeout=60,
    )
    if r.returncode != 0:
        raise RuntimeError(f"Ingesta falló: {r.stderr or r.stdout}")

def ejecutar_procesamiento(**context):
    """Spark debe cerrarse con spark.stop() - no solapar con ingesta."""
    import subprocess
    import os
    r = subprocess.run(
        [f"{BASE}/venv_transporte/bin/python", str(BASE / "procesamiento_grafos.py")],
        env=os.environ,
        capture_output=True,
        text=True,
        cwd=str(BASE),
        timeout=180,
    )
    if r.returncode != 0:
        raise RuntimeError(f"Procesamiento falló: {r.stderr or r.stdout}")

default_args = {
    "owner": "logistica",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_maestro_transporte",
    default_args=default_args,
    description="Gemelo Digital Logístico - Ingesta y Procesamiento cada 15 min",
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["logistica", "transport", "spain"],
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
