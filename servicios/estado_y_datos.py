"""
Estado de infraestructura y lectura de datos Cassandra — sin dependencia de Streamlit.
Compartido entre `app_visualizacion.py` y la API REST.
"""
from __future__ import annotations

import subprocess
from typing import Any, Dict, List

from config import CASSANDRA_HOST, HDFS_BACKUP_PATH, KEYSPACE, TOPIC_TRANSPORTE


def estado_servicios() -> Dict[str, str]:
    """
    Estado del **stack completo** (misma lógica que `comprobar_todos` en gestión de servicios):
    HDFS, Kafka, Cassandra, Spark, Hive, Airflow, NiFi — no solo los tres de ingesta.
    """
    from servicios.gestion_servicios import comprobar_todos

    return {
        s.get("nombre", s["id"]): ("✅ Activo" if s.get("activo") else "❌ Inactivo")
        for s in comprobar_todos()
    }


def verificar_hdfs_ruta(ruta: str) -> str:
    try:
        r = subprocess.run(
            ["hdfs", "dfs", "-ls", ruta],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if r.returncode != 0:
            return f"❌ HDFS `{ruta}`: no accesible o vacío"
        lineas = [l for l in r.stdout.strip().split("\n") if l.strip() and not l.startswith("Found")]
        jsons = [l for l in lineas if ".json" in l]
        return f"✅ HDFS `{ruta}`: {len(jsons)} objeto(s) JSON visibles"
    except FileNotFoundError:
        return "⚠️ Comando `hdfs` no encontrado en PATH"
    except Exception as e:
        return f"⚠️ HDFS: {str(e)[:80]}"


def verificar_kafka_topic(topic: str) -> str:
    try:
        r = subprocess.run(
            ["kafka-topics.sh", "--list", "--bootstrap-server", "localhost:9092"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if r.returncode != 0:
            return f"⚠️ Kafka: {r.stderr[:80] or 'error listando topics'}"
        if topic in r.stdout:
            return f"✅ Topic `{topic}` configurado en el proyecto"
        return f"⚠️ Topic `{topic}` no listado (revisa creación del topic)"
    except FileNotFoundError:
        return "⚠️ `kafka-topics.sh` no encontrado"
    except Exception as e:
        return f"⚠️ Kafka: {str(e)[:80]}"


def verificar_cassandra(query: str, etiqueta: str) -> str:
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(KEYSPACE)
        rows = list(session.execute(query))
        cluster.shutdown()
        n = len(rows)
        if n > 0:
            return f"✅ {etiqueta}: {n} fila(s)"
        return f"⚠️ {etiqueta}: sin datos (ejecuta procesamiento antes)"
    except Exception as e:
        return f"⚠️ {etiqueta}: {str(e)[:70]}"


def _row_a_dict(row: Any) -> Dict[str, Any]:
    if hasattr(row, "_asdict"):
        return row._asdict()
    try:
        return dict(row)
    except Exception:
        names = getattr(row, "_fields", None)
        if names:
            return {n: getattr(row, n) for n in names}
        return {str(i): row[i] for i in range(len(row))}


def _session_cassandra():
    from cassandra.cluster import Cluster

    return Cluster([CASSANDRA_HOST]).connect(KEYSPACE)


def cargar_nodos_cassandra() -> List[Dict[str, Any]]:
    try:
        s = _session_cassandra()
        rows = s.execute(
            "SELECT id_nodo, lat, lon, tipo, estado, motivo_retraso, clima_actual, temperatura FROM nodos_estado"
        )
        s.cluster.shutdown()
        return [_row_a_dict(r) for r in rows]
    except Exception:
        return []


def cargar_aristas_cassandra() -> List[Dict[str, Any]]:
    try:
        s = _session_cassandra()
        rows = s.execute("SELECT src, dst, distancia_km, estado, peso_penalizado FROM aristas_estado")
        s.cluster.shutdown()
        return [_row_a_dict(r) for r in rows]
    except Exception:
        return []


def cargar_tracking_cassandra() -> List[Dict[str, Any]]:
    try:
        s = _session_cassandra()
        rows = s.execute(
            "SELECT id_camion, lat, lon, ruta_origen, ruta_destino, estado_ruta, ruta_sugerida, motivo_retraso FROM tracking_camiones"
        )
        s.cluster.shutdown()
        return [_row_a_dict(r) for r in rows]
    except Exception:
        return []


def cargar_pagerank_cassandra() -> List[Dict[str, Any]]:
    try:
        s = _session_cassandra()
        rows = s.execute("SELECT id_nodo, pagerank FROM pagerank_nodos")
        s.cluster.shutdown()
        return [_row_a_dict(r) for r in rows]
    except Exception:
        return []


def verificacion_tecnica_completa() -> Dict[str, str]:
    """Resumen de comprobaciones (mismo criterio que la pestaña Verificación del dashboard)."""
    return {
        "hdfs_backup": verificar_hdfs_ruta(HDFS_BACKUP_PATH),
        "kafka_topic": verificar_kafka_topic(TOPIC_TRANSPORTE),
        "cassandra_nodos": verificar_cassandra("SELECT id_nodo FROM nodos_estado LIMIT 100", "nodos_estado"),
        "cassandra_tracking": verificar_cassandra("SELECT id_camion FROM tracking_camiones LIMIT 20", "tracking"),
    }
