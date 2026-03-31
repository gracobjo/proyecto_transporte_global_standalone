"""
SIMLOG Espana - DAG dedicado a ingesta DATEX2 DGT.

El flujo intenta consumir el feed live. Si falla, reutiliza la cache local y,
en ultimo termino, continua solo con la simulacion para no romper la cadena.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


BASE = Path(__file__).resolve().parent.parent


def _python_proyecto() -> str:
    venv = BASE / "venv_transporte" / "bin" / "python"
    if venv.is_file():
        return str(venv)
    return sys.executable


def _socket_check(host: str, port: int, label: str) -> str:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    try:
        sock.connect((host, port))
    except Exception as exc:
        raise RuntimeError(f"{label} no disponible: {exc}") from exc
    finally:
        sock.close()
    return "OK"


def verificar_hdfs(**context):
    r = subprocess.run(["hdfs", "dfs", "-ls", "/"], capture_output=True, text=True, timeout=20)
    if r.returncode != 0:
        msg = (r.stderr or r.stdout or "HDFS no disponible").strip()
        raise RuntimeError(f"HDFS no disponible: {msg[:280]}")
    return "OK"


def verificar_kafka(**context):
    for host in ("127.0.0.1", "localhost"):
        try:
            return _socket_check(host, 9092, "Kafka")
        except Exception:
            continue
    raise RuntimeError("Kafka no disponible en localhost:9092")


def verificar_cassandra(**context):
    return _socket_check("127.0.0.1", 9042, "Cassandra")


def ejecutar_ingesta_dgt(**context):
    interval_min = int(os.environ.get("SIMLOG_INGESTA_INTERVAL_MINUTES", "15"))
    ds = context.get("data_interval_start") or datetime.now(timezone.utc)
    if getattr(ds, "tzinfo", None) is None:
        ds = ds.replace(tzinfo=timezone.utc)
    paso_val = int(ds.timestamp() // (max(1, interval_min) * 60))
    cmd = [str(BASE / "scripts" / "ejecutar_ingesta_dgt.py"), "--paso", str(paso_val), "--skip-processing"]
    r = subprocess.run(
        [_python_proyecto(), *cmd],
        env={
            **os.environ,
            "SIMLOG_INGESTA_CANAL": "airflow",
            "SIMLOG_INGESTA_ORIGEN": "airflow_dag_ingesta_dgt",
            "SIMLOG_INGESTA_EJECUTOR": "dag_ingesta_dgt.ejecutar_ingesta_dgt",
        },
        cwd=str(BASE),
        capture_output=True,
        text=True,
        timeout=240,
    )
    if r.returncode != 0:
        raise RuntimeError(f"Ingesta DGT fallo: {r.stderr or r.stdout}")
    meta_path = BASE / "reports" / "kdd" / "work" / "ultima_ingesta_meta.json"
    meta = {}
    if meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["stdout"] = r.stdout[-4000:]
    return meta


def validar_fuente_dgt(**context):
    ti = context["ti"]
    meta = ti.xcom_pull(task_ids="ejecutar_ingesta_dgt") or {}
    source_mode = meta.get("dgt_source_mode", "disabled")
    incidencias = meta.get("dgt_incidencias_totales", 0)
    if source_mode == "live":
        return {"estado": "ok", "detalle": f"Feed DGT live con {incidencias} incidencias"}
    if source_mode == "cache":
        return {"estado": "degradado", "detalle": f"Feed DGT desde cache con {incidencias} incidencias"}
    return {"estado": "degradado", "detalle": "Feed DGT no disponible; se continua con simulacion"}


def ejecutar_procesamiento(**context):
    script = BASE / "procesamiento" / "procesamiento_grafos.py"
    r = subprocess.run(
        [_python_proyecto(), str(script)],
        env={**os.environ, "SIMLOG_ENABLE_HIVE": "1"},
        cwd=str(BASE),
        capture_output=True,
        text=True,
        timeout=int(os.environ.get("SIMLOG_AIRFLOW_SPARK_TIMEOUT_SEC", "900")),
    )
    if r.returncode != 0:
        raise RuntimeError(f"Procesamiento fallo: {r.stderr or r.stdout}")
    return {"estado": "ok", "stdout": r.stdout[-4000:]}


default_args = {
    "owner": "logistica",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="simlog_ingesta_dgt_datex2",
    default_args=default_args,
    description="SIMLOG - ingesta DATEX2 DGT con cache/fallback y procesamiento Spark posterior",
    schedule=timedelta(minutes=max(1, int(os.environ.get("SIMLOG_INGESTA_INTERVAL_MINUTES", "15")))),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["simlog", "dgt", "datex2", "airflow", "spark"],
) as dag:
    check_hdfs = PythonOperator(task_id="verificar_hdfs", python_callable=verificar_hdfs)
    check_kafka = PythonOperator(task_id="verificar_kafka", python_callable=verificar_kafka)
    check_cassandra = PythonOperator(task_id="verificar_cassandra", python_callable=verificar_cassandra)

    ingesta = PythonOperator(task_id="ejecutar_ingesta_dgt", python_callable=ejecutar_ingesta_dgt)
    validar_dgt = PythonOperator(task_id="validar_fuente_dgt", python_callable=validar_fuente_dgt)
    procesamiento = PythonOperator(task_id="spark_procesamiento_dgt", python_callable=ejecutar_procesamiento)

    [check_hdfs, check_kafka, check_cassandra] >> ingesta >> validar_dgt >> procesamiento

