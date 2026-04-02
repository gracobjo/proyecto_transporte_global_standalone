"""
Airflow DAG: Graph AI Anomaly Detection

Cada 15 minutos:
1) Recupera snapshot del grafo desde Cassandra (`nodos_estado`, `aristas_estado`)
2) Llama al microservicio FastAPI Graph AI: POST `/analyze-graph`
3) Inserta anomalías en Cassandra (`graph_anomalies`)
4) Opcional: publica anomalías en Kafka (`graph_anomalies`)
"""

from __future__ import annotations

import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from pathlib import Path


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
    # Fallback: asumir layout clásico `.../AIRFLOW_HOME/dags/<este_fichero>`
    # y que el repo está junto a AIRFLOW_HOME (solo si existe).
    guess = here.parent.parent
    if (guess / "config.py").exists():
        return guess
    raise RuntimeError(
        "No se pudo determinar la raíz del proyecto para importar `config.py`. "
        "Define SIMLOG_PROJECT_ROOT o monta el repo en /opt/proyecto_transporte_global."
    )


PROJECT_BASE = str(_project_base())
if PROJECT_BASE not in sys.path:
    sys.path.insert(0, PROJECT_BASE)

from config import CASSANDRA_HOST, KEYSPACE


GRAPH_AI_SERVICE_URL = os.environ.get("GRAPH_AI_SERVICE_URL", "http://127.0.0.1:8001")
ANOMALIES_KAFKA_TOPIC = os.environ.get("GRAPH_AI_KAFKA_TOPIC", "graph_anomalies")
PUBLISH_TO_KAFKA = os.environ.get("SIMLOG_PUBLISH_GRAPH_ANOMALIES_KAFKA", "0") == "1"


def _fetch_graph_from_cassandra() -> Dict[str, Any]:
    from cassandra.cluster import Cluster

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)

    nodes: List[Dict[str, str]] = []
    for row in session.execute("SELECT id_nodo FROM nodos_estado"):
        nodes.append({"id": row.id_nodo})

    edges: List[Dict[str, Any]] = []
    for row in session.execute("SELECT src, dst, peso_penalizado, distancia_km FROM aristas_estado"):
        w = row.peso_penalizado if row.peso_penalizado is not None else row.distancia_km
        if w is None:
            w = 1.0
        edges.append({"source": row.src, "target": row.dst, "weight": float(w)})

    cluster.shutdown()
    return {"nodes": nodes, "edges": edges, "directed": True}


def _call_graph_ai_analyze(graph_payload: Dict[str, Any]) -> Dict[str, Any]:
    url = GRAPH_AI_SERVICE_URL.rstrip("/") + "/analyze-graph"
    body = {"graph": graph_payload}
    r = requests.post(url, json=body, timeout=60)
    r.raise_for_status()
    return r.json()


def _insert_anomalies_into_cassandra(analyze_result: Dict[str, Any]) -> None:
    from cassandra.cluster import Cluster

    ts = datetime.now(timezone.utc)
    ts_bucket = int(time.time() // (15 * 60))

    anomalous_nodes = analyze_result.get("anomalous_nodes") or []
    anomaly_scores = analyze_result.get("anomaly_scores") or {}

    if not anomalous_nodes:
        return

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)

    # Asegura tabla si se ejecuta antes de aplicar `cassandra/esquema_logistica.cql`.
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS graph_anomalies (
            id UUID,
            timestamp TIMESTAMP,
            node_id TEXT,
            anomaly_score DOUBLE,
            metric_type TEXT,
            metric_value DOUBLE,
            ts_bucket BIGINT,
            PRIMARY KEY ((ts_bucket, metric_type), timestamp, node_id, id)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
        """
    )

    insert_cql = (
        "INSERT INTO graph_anomalies (id, timestamp, node_id, anomaly_score, metric_type, metric_value, ts_bucket) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    )

    for node_id in anomalous_nodes:
        score = float(anomaly_scores.get(node_id, 0.0))
        session.execute(
            insert_cql,
            (
                uuid.uuid4(),
                ts,
                node_id,
                score,
                "overall",
                score,
                ts_bucket,
            ),
        )

    cluster.shutdown()


def _publish_anomalies_to_kafka(analyze_result: Dict[str, Any]) -> None:
    if not PUBLISH_TO_KAFKA:
        return
    try:
        from kafka import KafkaProducer
    except Exception:
        return

    from config import KAFKA_BOOTSTRAP

    prod = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "anomalous_nodes": analyze_result.get("anomalous_nodes") or [],
            "anomaly_scores": analyze_result.get("anomaly_scores") or {},
        }
        prod.send(ANOMALIES_KAFKA_TOPIC, value=str(payload).encode("utf-8"))
        prod.flush(5)
    finally:
        try:
            prod.close()
        except Exception:
            pass


default_args = {
    "owner": "simlog",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="simlog_graph_ai_anomalias",
    default_args=default_args,
    description="SIMLOG — Graph AI anomaly detection (NetworkX + FastAPI)",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["simlog", "graph_ai", "anomaly", "cassandra"],
) as dag:

    def tarea_fetch_graph(**context) -> Dict[str, Any]:
        return _fetch_graph_from_cassandra()

    def tarea_analyze_and_store(**context) -> Dict[str, Any]:
        graph = context["ti"].xcom_pull(task_ids="fetch_graph")
        result = _call_graph_ai_analyze(graph)
        _insert_anomalies_into_cassandra(result)
        _publish_anomalies_to_kafka(result)
        return result

    fetch_graph = PythonOperator(
        task_id="fetch_graph",
        python_callable=tarea_fetch_graph,
        provide_context=True,
    )

    analyze_and_store = PythonOperator(
        task_id="analyze_and_store",
        python_callable=tarea_analyze_and_store,
        provide_context=True,
    )

    fetch_graph >> analyze_and_store

