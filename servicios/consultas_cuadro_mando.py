"""
Consultas supervisadas (whitelist) para Cassandra y Hive — cuadro de mando SIMLOG.

No se ejecuta SQL arbitrario del usuario: solo plantillas aprobadas para supervisión y control.
"""
from __future__ import annotations

import os
import re
import subprocess
from typing import Any, Dict, List, Optional, Tuple

from config import CASSANDRA_HOST, HIVE_DB, HIVE_JDBC_URL, KEYSPACE

# --- Cassandra (CQL) ---

CASSANDRA_CONSULTAS: Dict[str, Dict[str, str]] = {
    "nodos_estado_resumen": {
        "titulo": "Nodos — estado operativo (actual)",
        "cql": (
            "SELECT id_nodo, tipo, estado, motivo_retraso, clima_actual, temperatura, "
            "ultima_actualizacion FROM nodos_estado LIMIT 100"
        ),
    },
    "nodos_hub_congestion": {
        "titulo": "Nodos — solo congestión o bloqueo",
        "cql": (
            "SELECT id_nodo, estado, motivo_retraso, clima_actual FROM nodos_estado "
            "WHERE estado IN ('Congestionado', 'Bloqueado') LIMIT 100 ALLOW FILTERING"
        ),
    },
    "aristas_estado": {
        "titulo": "Aristas — distancias y penalización",
        "cql": "SELECT src, dst, distancia_km, estado, peso_penalizado FROM aristas_estado LIMIT 200",
    },
    "tracking_camiones": {
        "titulo": "Tracking — posición y rutas",
        "cql": (
            "SELECT id_camion, lat, lon, ruta_origen, ruta_destino, estado_ruta, motivo_retraso, "
            "ultima_posicion FROM tracking_camiones LIMIT 50"
        ),
    },
    "pagerank_top": {
        "titulo": "PageRank — criticidad de nodos",
        "cql": "SELECT id_nodo, pagerank, ultima_actualizacion FROM pagerank_nodos LIMIT 100",
    },
    "eventos_recientes": {
        "titulo": "Eventos históricos (Cassandra, ventana TTL)",
        "cql": (
            "SELECT tipo_entidad, id_entidad, estado_anterior, estado_nuevo, motivo, timestamp_evento "
            "FROM eventos_historico LIMIT 80 ALLOW FILTERING"
        ),
    },
}


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if hasattr(row, "_asdict"):
        return row._asdict()
    try:
        return dict(row)
    except Exception:
        names = getattr(row, "_fields", None)
        if names:
            return {n: getattr(row, n) for n in names}
        return {str(i): row[i] for i in range(len(row))}


def ejecutar_cassandra_consulta(codigo: str) -> Tuple[bool, str, List[Dict[str, Any]]]:
    """Ejecuta solo si `codigo` está en CASSANDRA_CONSULTAS."""
    if codigo not in CASSANDRA_CONSULTAS:
        return False, f"Consulta no permitida: {codigo}", []
    cql = CASSANDRA_CONSULTAS[codigo]["cql"]
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(KEYSPACE)
        rows = list(session.execute(cql))
        cluster.shutdown()
        return True, "", [_row_to_dict(r) for r in rows]
    except Exception as e:
        return False, str(e), []


# --- Hive (solo SELECT, whitelist) ---

HIVE_DB_NAME = HIVE_DB or "logistica_espana"

HIVE_CONSULTAS: Dict[str, Dict[str, str]] = {
    "tablas_bd": {
        "titulo": "Listar tablas en la base logística",
        "sql": f"SHOW TABLES IN {HIVE_DB_NAME}",
    },
    "historico_nodos_muestra": {
        "titulo": "Histórico de nodos (muestra; requiere tabla creada por Spark)",
        "sql": f"SELECT * FROM {HIVE_DB_NAME}.historico_nodos LIMIT 50",
    },
    "historico_nodos_conteo": {
        "titulo": "Conteo de registros en histórico de nodos",
        "sql": f"SELECT COUNT(*) AS total FROM {HIVE_DB_NAME}.historico_nodos",
    },
    "nodos_maestro": {
        "titulo": "Maestro de nodos (Hive)",
        "sql": f"SELECT * FROM {HIVE_DB_NAME}.nodos_maestro LIMIT 100",
    },
    "nodos_maestro_conteo": {
        "titulo": "Conteo de nodos maestro",
        "sql": f"SELECT COUNT(*) AS total FROM {HIVE_DB_NAME}.nodos_maestro",
    },
}


def _hive_beeline_cmd() -> List[str]:
    jdbc = os.environ.get("HIVE_JDBC_URL", HIVE_JDBC_URL)
    bin_name = os.environ.get("HIVE_BEELINE_BIN", "beeline")
    return [bin_name, "-u", jdbc, "--silent=true", "--outputformat=tsv2"]


def ejecutar_hive_consulta(codigo: str) -> Tuple[bool, str, str]:
    """
    Ejecuta consulta Hive predefinida. Requiere `beeline` en PATH y HiveServer2.
    Devuelve (ok, mensaje_error, salida_texto).
    """
    if codigo not in HIVE_CONSULTAS:
        return False, f"Consulta no permitida: {codigo}", ""
    sql = HIVE_CONSULTAS[codigo]["sql"].strip()
    if not sql.upper().startswith("SHOW") and not sql.upper().startswith("SELECT"):
        return False, "Solo se permiten SHOW o SELECT", ""

    cmd = _hive_beeline_cmd() + ["-e", sql]
    try:
        r = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            env=os.environ.copy(),
        )
        out = (r.stdout or "") + (r.stderr or "")
        if r.returncode != 0:
            return False, r.stderr or r.stdout or "beeline error", out
        return True, "", out
    except FileNotFoundError:
        return (
            False,
            "Comando `beeline` no encontrado. Instala Hive/Beeline o define HIVE_BEELINE_BIN.",
            "",
        )
    except subprocess.TimeoutExpired:
        return False, "Timeout ejecutando Hive (120s)", ""
    except Exception as e:
        return False, str(e), ""


def listar_claves_cassandra() -> List[str]:
    return list(CASSANDRA_CONSULTAS.keys())


def listar_claves_hive() -> List[str]:
    return list(HIVE_CONSULTAS.keys())


def titulo_cassandra(codigo: str) -> str:
    return CASSANDRA_CONSULTAS.get(codigo, {}).get("titulo", codigo)


def titulo_hive(codigo: str) -> str:
    return HIVE_CONSULTAS.get(codigo, {}).get("titulo", codigo)
