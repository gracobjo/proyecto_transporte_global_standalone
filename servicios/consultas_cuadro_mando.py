"""
Consultas supervisadas (whitelist) para Cassandra y Hive — cuadro de mando SIMLOG.

No se ejecuta SQL arbitrario del usuario: solo plantillas aprobadas para supervisión y control.
"""
from __future__ import annotations

import os
import re
from queue import Queue
from threading import Thread
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from config import (
    CASSANDRA_HOST,
    HIVE_BEELINE_USER,
    HIVE_CONF_DIR,
    HIVE_DB,
    HIVE_JDBC_URL,
    HIVE_METASTORE_URIS,
    KEYSPACE,
)

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
    "tracking_camiones_gemelo": {
        "titulo": "Tracking gemelo digital (columnas estrictas)",
        "cql": "SELECT id_camion, lat, lon, ultima_posicion FROM tracking_camiones",
    },
    "pagerank_top": {
        "titulo": "PageRank — criticidad de nodos",
        "cql": (
            "SELECT id_nodo, pagerank, peso_pagerank, source, estado, ultima_actualizacion "
            "FROM pagerank_nodos LIMIT 100"
        ),
    },
    "eventos_recientes": {
        "titulo": "Eventos históricos (Cassandra, ventana TTL)",
        "cql": (
            "SELECT tipo_entidad, id_entidad, estado_anterior, estado_nuevo, motivo, timestamp_evento "
            "FROM eventos_historico LIMIT 80 ALLOW FILTERING"
        ),
    },
    # --- Gestor (lenguaje natural / asistente; columnas alineadas al esquema real) ---
    "gestor_camiones_mapa": {
        "titulo": "Gestor — ¿Dónde están mis camiones ahora? (tiempo real / mapa)",
        "cql": (
            "SELECT id_camion, lat, lon, ruta_origen, ruta_destino, estado_ruta, motivo_retraso, "
            "ultima_posicion FROM tracking_camiones LIMIT 200"
        ),
    },
    "gestor_ciudades_trafico": {
        "titulo": "Gestor — ¿Qué ciudades/nodos tienen más incidencia? (Bloqueado)",
        "cql": (
            "SELECT id_nodo, tipo, estado, motivo_retraso, clima_actual FROM nodos_estado "
            "WHERE estado = 'Bloqueado' ALLOW FILTERING"
        ),
    },
    "gestor_nodo_critico_pagerank": {
        "titulo": "Gestor — nodo logístico más crítico (PageRank; ordenar en cliente)",
        "cql": (
            "SELECT id_nodo, pagerank, peso_pagerank, source, estado, ultima_actualizacion "
            "FROM pagerank_nodos LIMIT 500"
        ),
    },
    # --- Consultas mejoradas para el Gestor ---
    "gestor_nodos_con_incidencias": {
        "titulo": "Gestor — nodos con incidencias (estado != OK)",
        "cql": (
            "SELECT id_nodo, tipo, estado, motivo_retraso, municipio, provincia, carretera, "
            "descripcion_incidencia, source, severity FROM nodos_estado "
            "WHERE estado != 'OK' LIMIT 100 ALLOW FILTERING"
        ),
    },
    "gestor_nodos_madrid_barcelona": {
        "titulo": "Gestor — estado Madrid y Barcelona (ejes principales)",
        "cql": (
            "SELECT id_nodo, estado, motivo_retraso, clima_actual, temperatura, "
            "municipio, provincia FROM nodos_estado "
            "WHERE id_nodo IN ('Madrid', 'Barcelona') LIMIT 10"
        ),
    },
    "gestor_aristas_bloqueadas": {
        "titulo": "Gestor — rutas bloqueadas entre nodos",
        "cql": (
            "SELECT src, dst, distancia_km, estado, peso_penalizado FROM aristas_estado "
            "WHERE estado = 'Bloqueado' LIMIT 50 ALLOW FILTERING"
        ),
    },
    "gestor_aristas_congestionadas": {
        "titulo": "Gestor — rutas congestionadas",
        "cql": (
            "SELECT src, dst, distancia_km, estado, peso_penalizado FROM aristas_estado "
            "WHERE estado = 'Congestionado' LIMIT 100 ALLOW FILTERING"
        ),
    },
    "gestor_camiones_en_ruta": {
        "titulo": "Gestor — camiones en ruta con progreso",
        "cql": (
            "SELECT id_camion, lat, lon, ruta_origen, ruta_destino, estado_ruta, "
            "motivo_retraso, ultima_posicion FROM tracking_camiones "
            "WHERE estado_ruta = 'En ruta' LIMIT 100 ALLOW FILTERING"
        ),
    },
    "gestor_camiones_bloqueados": {
        "titulo": "Gestor — camiones con retrasos o bloqueados",
        "cql": (
            "SELECT id_camion, lat, lon, ruta_origen, ruta_destino, estado_ruta, "
            "motivo_retraso FROM tracking_camiones "
            "WHERE estado_ruta != 'En ruta' LIMIT 50 ALLOW FILTERING"
        ),
    },
    "gestor_eventos_cambios_estado": {
        "titulo": "Gestor — últimos cambios de estado en la red",
        "cql": (
            "SELECT tipo_entidad, id_entidad, estado_anterior, estado_nuevo, motivo, "
            "timestamp_evento FROM eventos_historico LIMIT 100 ALLOW FILTERING"
        ),
    },
    "gestor_incidencias_por_provincia": {
        "titulo": "Gestor — incidencias agrupadas por provincia",
        "cql": (
            "SELECT provincia, COUNT(*) AS total_incidencias, estado, motivo_retraso "
            "FROM nodos_estado WHERE provincia IS NOT NULL "
            "GROUP BY provincia, estado, motivo_retraso LIMIT 100 ALLOW FILTERING"
        ),
    },
    "gestor_nodos_clima_adverso": {
        "titulo": "Gestor — nodos con clima adverso (nieve, lluvia, niebla)",
        "cql": (
            "SELECT id_nodo, tipo, clima_actual, temperatura, humedad, viento_velocidad, "
            "estado, motivo_retraso FROM nodos_estado "
            "WHERE clima_actual LIKE '%nieve%' OR clima_actual LIKE '%lluvia%' "
            "OR clima_actual LIKE '%niebla%' OR clima_actual LIKE '%hielo%' "
            "LIMIT 100 ALLOW FILTERING"
        ),
    },
    "gestor_nodos_severidad_alta": {
        "titulo": "Gestor — nodos con severidad alta/crítica",
        "cql": (
            "SELECT id_nodo, tipo, estado, severity, motivo_retraso, municipio, provincia, "
            "carretera, descripcion_incidencia, source FROM nodos_estado "
            "WHERE severity IN ('high', 'highest') LIMIT 100 ALLOW FILTERING"
        ),
    },
    "gestor_pagerank_nodos_criticos": {
        "titulo": "Gestor — nodos más críticos según PageRank (top 20)",
        "cql": (
            "SELECT id_nodo, pagerank, peso_pagerank, estado, source, ultima_actualizacion "
            "FROM pagerank_nodos ORDER BY pagerank DESC LIMIT 20"
        ),
    },
    "gestor_tracking_ruta_completa": {
        "titulo": "Gestor — tracking completo con coordenadas",
        "cql": (
            "SELECT id_camion, lat, lon, ruta_origen, ruta_destino, estado_ruta, "
            "motivo_retraso, temperatura AS temp_aprox, ultima_posicion FROM tracking_camiones "
            "LIMIT 200"
        ),
    },
}


# ============================================================================
# Categorías Cassandra para organizar las consultas en el frontend
# ============================================================================
CASSANDRA_CATEGORIAS: Dict[str, Dict[str, List[str]]] = {
    "nodos": {
        "nombre": "Estado de Nodos",
        "descripcion": "Estado actual de hubs y nodos de la red",
        "icono": "📍",
        "consultas": [
            "nodos_estado_resumen",
            "nodos_hub_congestion",
            "gestor_nodos_con_incidencias",
            "gestor_nodos_madrid_barcelona",
            "gestor_nodos_severidad_alta",
            "gestor_nodos_clima_adverso",
        ],
    },
    "aristas": {
        "nombre": "Estado de Rutas",
        "descripcion": "Estado de aristas (conexiones entre nodos)",
        "icono": "🛤️",
        "consultas": [
            "aristas_estado",
            "gestor_aristas_bloqueadas",
            "gestor_aristas_congestionadas",
        ],
    },
    "tracking": {
        "nombre": "Tracking Camiones",
        "descripcion": "Posición GPS actual y estado de la flota",
        "icono": "🚛",
        "consultas": [
            "tracking_camiones",
            "tracking_camiones_gemelo",
            "gestor_camiones_mapa",
            "gestor_camiones_en_ruta",
            "gestor_camiones_bloqueados",
            "gestor_tracking_ruta_completa",
        ],
    },
    "pagerank": {
        "nombre": "PageRank",
        "descripcion": "Criticidad de nodos según algoritmo PageRank",
        "icono": "📊",
        "consultas": [
            "pagerank_top",
            "gestor_nodo_critico_pagerank",
            "gestor_pagerank_nodos_criticos",
        ],
    },
    "eventos": {
        "nombre": "Eventos",
        "descripcion": "Histórico de eventos y cambios de estado",
        "icono": "📋",
        "consultas": [
            "eventos_recientes",
            "gestor_eventos_cambios_estado",
        ],
    },
    "gestor": {
        "nombre": "Gestor",
        "descripcion": "Consultas específicas para el gestor de operaciones",
        "icono": "👤",
        "consultas": [
            "gestor_ciudades_trafico",
            "gestor_incidencias_por_provincia",
        ],
    },
}


def listar_categorias_cassandra() -> List[str]:
    """Lista de claves de categorías Cassandra ordenadas."""
    orden = ["nodos", "aristas", "tracking", "pagerank", "eventos", "gestor"]
    return [c for c in orden if c in CASSANDRA_CATEGORIAS]


def obtener_categoria_cassandra(nombre_categoria: str) -> Dict[str, Any]:
    """Obtiene la información de una categoría Cassandra."""
    return CASSANDRA_CATEGORIAS.get(nombre_categoria, {})


def obtener_consultas_de_categoria_cassandra(nombre_categoria: str) -> List[str]:
    """Obtiene las claves de consultas de una categoría Cassandra."""
    cat = CASSANDRA_CATEGORIAS.get(nombre_categoria, {})
    return cat.get("consultas", [])


def nombre_categoria_cassandra(nombre_categoria: str) -> str:
    """Obtiene el nombre amigable de una categoría Cassandra."""
    cat = CASSANDRA_CATEGORIAS.get(nombre_categoria, {})
    icono = cat.get("icono", "📁")
    nombre = cat.get("nombre", nombre_categoria)
    return f"{icono} {nombre}"


def descripcion_categoria_cassandra(nombre_categoria: str) -> str:
    """Obtiene la descripción de una categoría Cassandra."""
    return CASSANDRA_CATEGORIAS.get(nombre_categoria, {}).get("descripcion", "")


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


def ejecutar_cassandra_cql_seguro(cql: str) -> Tuple[bool, str, List[Dict[str, Any]]]:
    """
    Ejecuta CQL de solo lectura para la UI (copiar/pegar).
    Restringido a SELECT.
    """
    cql = (cql or "").strip().rstrip(";")
    u = cql.upper().lstrip()
    if not u.startswith("SELECT"):
        return False, "Solo se permite SELECT en Cassandra (modo seguro)", []
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(KEYSPACE)
        rows = list(session.execute(cql))
        cluster.shutdown()
        return True, "", [_row_to_dict(r) for r in rows]
    except Exception as e:
        return False, str(e), []


def listar_keyspaces_cassandra() -> Tuple[bool, str, List[str]]:
    """Lista keyspaces Cassandra disponibles."""
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect()
        meta = cluster.metadata
        kss = sorted(list(meta.keyspaces.keys()))
        session.shutdown()
        cluster.shutdown()
        return True, "", kss
    except Exception as e:
        return False, str(e), []


def listar_tablas_cassandra(keyspace: Optional[str] = None) -> Tuple[bool, str, List[str]]:
    """Descubre tablas del keyspace Cassandra indicado (o el configurado)."""
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect()
        meta = cluster.metadata
        ks_name = keyspace or KEYSPACE
        ks = meta.keyspaces.get(ks_name)
        if not ks:
            session.shutdown()
            cluster.shutdown()
            return False, f"Keyspace no encontrado: {ks_name}", []
        tablas = sorted(list(ks.tables.keys()))
        session.shutdown()
        cluster.shutdown()
        return True, "", tablas
    except Exception as e:
        return False, str(e), []


def listar_columnas_cassandra(tabla: str, keyspace: Optional[str] = None) -> Tuple[bool, str, List[str]]:
    """Lista columnas de una tabla Cassandra en el keyspace indicado (o configurado)."""
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect()
        meta = cluster.metadata
        ks_name = keyspace or KEYSPACE
        ks = meta.keyspaces.get(ks_name)
        if not ks:
            session.shutdown()
            cluster.shutdown()
            return False, f"Keyspace no encontrado: {ks_name}", []
        tb = ks.tables.get(tabla)
        if not tb:
            session.shutdown()
            cluster.shutdown()
            return False, f"Tabla no encontrada: {ks_name}.{tabla}", []
        cols = sorted(list(tb.columns.keys()))
        session.shutdown()
        cluster.shutdown()
        return True, "", cols
    except Exception as e:
        return False, str(e), []


def listar_tablas_hive() -> Tuple[bool, str, List[str]]:
    """Lista tablas Hive de la DB configurada."""
    ok, err, out = ejecutar_hive_sql_seguro(f"SHOW TABLES IN {HIVE_DB_NAME}")
    if not ok:
        return False, err or "Error Hive", []
    lineas = [ln.strip() for ln in (out or "").splitlines() if ln.strip()]
    if len(lineas) <= 1:
        return True, "", []
    tablas: List[str] = []
    for ln in lineas[1:]:
        partes = ln.split("\t")
        tablas.append(partes[-1].strip())
    return True, "", sorted(list({t for t in tablas if t}))


def listar_columnas_hive(tabla: str) -> Tuple[bool, str, List[str]]:
    """Lista columnas Hive con DESCRIBE, ignorando metadata extendida."""
    ok, err, out = ejecutar_hive_sql_seguro(f"DESCRIBE {HIVE_DB_NAME}.{tabla}")
    if not ok:
        return False, err or "Error Hive", []
    cols: List[str] = []
    for ln in (out or "").splitlines():
        if not ln.strip() or ln.lower().startswith("col_name"):
            continue
        partes = ln.split("\t")
        c = (partes[0] if partes else "").strip()
        if not c or c.startswith("#"):
            continue
        if c.lower().startswith("detailed table information"):
            break
        cols.append(c)
    return True, "", cols


# --- Hive (solo SELECT, whitelist) ---
# Tablas reales creadas por persistencia_hive.py y procesamiento_grafos.py:
# - eventos_historico: histórico de eventos nodos/aristas
# - clima_historico: histórico climático por ciudad
# - tracking_camiones_historico: tracking histórico de camiones
# - transporte_ingesta_completa: transporte plano para UI
# - rutas_alternativas_historico: rutas alternativas calculadas
# - agg_estadisticas_diarias: agregaciones diarias
# Tablas en HDFS (compatibilidad):
# - nodos_maestro: datos maestros de nodos (CSV en HDFS)

HIVE_DB_NAME = HIVE_DB or "logistica_espana"
_T_EVENTOS = "eventos_historico"
_T_CLIMA = "clima_historico"
_T_TRACKING = "tracking_camiones_historico"
_T_TRANSPORTE = "transporte_ingesta_completa"
_T_RUTAS = "rutas_alternativas_historico"
_T_AGG = "agg_estadisticas_diarias"

HIVE_CONSULTAS: Dict[str, Dict[str, str]] = {
    # --- Diagnóstico ---
    "diag_smoke_hive": {
        "titulo": "Diagnóstico — conexión Hive (SELECT 1; sin tablas)",
        "sql": "SELECT 1 AS ok",
    },
    "tablas_bd": {
        "titulo": "Listar tablas en la base logística",
        "sql": f"SHOW TABLES IN {HIVE_DB_NAME}",
    },
    # --- Eventos histórico (eventos_historico) ---
    "eventos_historico_muestra": {
        "titulo": "Eventos histórico — todos (muestra)",
        "sql": f"""
SELECT timestamp, tipo_evento, id_elemento, tipo_elemento, estado, motivo, hub_asociado, pagerank
FROM {HIVE_DB_NAME}.{_T_EVENTOS}
ORDER BY timestamp DESC
LIMIT 100
        """.strip(),
    },
    "eventos_nodos_24h": {
        "titulo": "Eventos — nodos últimas 24h",
        "sql": f"""
SELECT timestamp, id_elemento, estado, motivo, hub_asociado, pagerank
FROM {HIVE_DB_NAME}.{_T_EVENTOS}
WHERE tipo_evento = 'nodo'
  AND timestamp >= date_sub(current_timestamp(), 1)
ORDER BY timestamp DESC
LIMIT 200
        """.strip(),
    },
    "eventos_bloqueos_24h": {
        "titulo": "Eventos — bloqueos últimas 24h",
        "sql": f"""
SELECT timestamp, id_elemento, estado, motivo, hub_asociado
FROM {HIVE_DB_NAME}.{_T_EVENTOS}
WHERE estado IN ('bloqueado', 'Bloqueado')
  AND timestamp >= date_sub(current_timestamp(), 1)
ORDER BY timestamp DESC
LIMIT 100
        """.strip(),
    },
    "eventos_evolucion_dia": {
        "titulo": "Eventos — evolución por día (últimos 7 días)",
        "sql": f"""
SELECT dia, tipo_evento, estado, COUNT(*) as total
FROM {HIVE_DB_NAME}.{_T_EVENTOS}
WHERE timestamp >= date_sub(current_timestamp(), 7)
GROUP BY dia, tipo_evento, estado
ORDER BY dia DESC, total DESC
LIMIT 200
        """.strip(),
    },
    # --- Clima histórico (clima_historico) ---
    "clima_historico_muestra": {
        "titulo": "Clima histórico — todos (muestra)",
        "sql": f"""
SELECT timestamp, ciudad, temperatura, humedad, descripcion, visibilidad, estado_carretera
FROM {HIVE_DB_NAME}.{_T_CLIMA}
ORDER BY timestamp DESC
LIMIT 100
        """.strip(),
    },
    "clima_historico_hoy": {
        "titulo": "Clima histórico — hoy",
        "sql": f"""
SELECT timestamp, ciudad, temperatura, humedad, descripcion, estado_carretera
FROM {HIVE_DB_NAME}.{_T_CLIMA}
WHERE dia = current_date()
ORDER BY timestamp DESC
LIMIT 50
        """.strip(),
    },
    "clima_estado_carretera": {
        "titulo": "Clima — estado de carreteras por clima",
        "sql": f"""
SELECT estado_carretera, descripcion, COUNT(*) as total
FROM {HIVE_DB_NAME}.{_T_CLIMA}
WHERE timestamp >= date_sub(current_timestamp(), 1)
GROUP BY estado_carretera, descripcion
ORDER BY total DESC
LIMIT 50
        """.strip(),
    },
    # --- Tracking camiones histórico (tracking_camiones_historico) ---
    "tracking_historico_muestra": {
        "titulo": "Tracking histórico — muestra",
        "sql": f"""
SELECT timestamp, id_camion, origen, destino, nodo_actual, lat_actual, lon_actual, progreso_pct, distancia_total_km
FROM {HIVE_DB_NAME}.{_T_TRACKING}
ORDER BY timestamp DESC
LIMIT 100
        """.strip(),
    },
    "tracking_camion_especifico": {
        "titulo": "Tracking — por camión (filtrar en cliente)",
        "sql": f"""
SELECT timestamp, id_camion, origen, destino, nodo_actual, lat_actual, lon_actual, progreso_pct, distancia_total_km, tiene_ruta_alternativa
FROM {HIVE_DB_NAME}.{_T_TRACKING}
WHERE timestamp >= date_sub(current_timestamp(), 7)
ORDER BY timestamp DESC
LIMIT 500
        """.strip(),
    },
    "tracking_ultima_posicion": {
        "titulo": "Tracking — última posición por camión",
        "sql": f"""
SELECT id_camion, origen, destino, nodo_actual, lat_actual, lon_actual, progreso_pct, timestamp
FROM {HIVE_DB_NAME}.{_T_TRACKING}
WHERE timestamp >= date_sub(current_timestamp(), 1)
LIMIT 200
        """.strip(),
    },
    # --- Transporte ingestado (transporte_ingesta_completa) ---
    "transporte_ingesta_real_muestra": {
        "titulo": "Transporte ingestado — muestra",
        "sql": f"""
SELECT timestamp, id_camion, origen, destino, nodo_actual, lat, lon, progreso_pct, estado_ruta, motivo_retraso, ruta
FROM {HIVE_DB_NAME}.{_T_TRANSPORTE}
ORDER BY timestamp DESC
LIMIT 100
        """.strip(),
    },
    "transporte_ingesta_hoy": {
        "titulo": "Transporte ingestado — hoy",
        "sql": f"""
SELECT timestamp, id_camion, origen, destino, estado_ruta, motivo_retraso, progreso_pct
FROM {HIVE_DB_NAME}.{_T_TRANSPORTE}
WHERE dia = current_date()
ORDER BY timestamp DESC
LIMIT 200
        """.strip(),
    },
    "transporte_retrasos_hoy": {
        "titulo": "Transporte — camiones con retrasos hoy",
        "sql": f"""
SELECT timestamp, id_camion, origen, destino, estado_ruta, motivo_retraso
FROM {HIVE_DB_NAME}.{_T_TRANSPORTE}
WHERE estado_ruta != 'En ruta'
  AND dia = current_date()
ORDER BY timestamp DESC
LIMIT 100
        """.strip(),
    },
    "gestor_historial_rutas_camion": {
        "titulo": "Gestor — histórico transporte por camión",
        "sql": f"""
SELECT timestamp, id_camion, origen, destino, nodo_actual, progreso_pct, estado_ruta, motivo_retraso
FROM {HIVE_DB_NAME}.{_T_TRANSPORTE}
WHERE timestamp >= date_sub(current_timestamp(), 7)
ORDER BY id_camion, timestamp DESC
LIMIT 500
        """.strip(),
    },
    # --- Rutas alternativas (rutas_alternativas_historico) ---
    "rutas_alternativas_muestra": {
        "titulo": "Rutas alternativas — muestra",
        "sql": f"""
SELECT timestamp, origen, destino, ruta_original, ruta_alternativa, distancia_original_km, distancia_alternativa_km, motivo_bloqueo, ahorro_km
FROM {HIVE_DB_NAME}.{_T_RUTAS}
ORDER BY timestamp DESC
LIMIT 100
        """.strip(),
    },
    "rutas_alternativas_bloqueos": {
        "titulo": "Rutas alternativas — bloqueos detectados",
        "sql": f"""
SELECT timestamp, origen, destino, ruta_original, motivo_bloqueo, ahorro_km
FROM {HIVE_DB_NAME}.{_T_RUTAS}
WHERE motivo_bloqueo IS NOT NULL
  AND motivo_bloqueo != ''
  AND timestamp >= date_sub(current_timestamp(), 7)
ORDER BY timestamp DESC
LIMIT 100
        """.strip(),
    },
    # --- Agregaciones diarias (agg_estadisticas_diarias) ---
    "agg_estadisticas_diarias": {
        "titulo": "Agregaciones — estadísticas diarias",
        "sql": f"""
SELECT anio, mes, dia, tipo_evento, estado, motivo, contador, pct_total
FROM {HIVE_DB_NAME}.{_T_AGG}
ORDER BY anio DESC, mes DESC, dia DESC, contador DESC
LIMIT 200
        """.strip(),
    },
    "agg_ultima_semana": {
        "titulo": "Agregaciones — última semana",
        "sql": f"""
SELECT anio, mes, dia, tipo_evento, estado, motivo, contador, pct_total
FROM {HIVE_DB_NAME}.{_T_AGG}
WHERE anio = year(current_date())
  AND mes = month(current_date())
  AND dia >= day(current_date()) - 7
ORDER BY dia DESC, contador DESC
LIMIT 200
        """.strip(),
    },
    # --- Consultas específicas para el Gestor ---
    "gestor_eventos_por_hub": {
        "titulo": "Gestor — eventos por hub (últimas 24h)",
        "sql": f"""
SELECT hub_asociado, estado, COUNT(*) as total
FROM {HIVE_DB_NAME}.{_T_EVENTOS}
WHERE timestamp >= date_sub(current_timestamp(), 1)
GROUP BY hub_asociado, estado
ORDER BY total DESC
LIMIT 100
        """.strip(),
    },
    "gestor_clima_afecta_transporte": {
        "titulo": "Gestor — clima adverso que afecta transporte",
        "sql": f"""
SELECT c.timestamp, c.ciudad, c.estado_carretera, c.descripcion, t.estado_ruta, t.motivo_retraso
FROM {HIVE_DB_NAME}.{_T_CLIMA} c
LEFT JOIN {HIVE_DB_NAME}.{_T_TRANSPORTE} t ON c.timestamp = t.timestamp AND c.ciudad = t.hub_actual
WHERE c.timestamp >= date_sub(current_timestamp(), 1)
  AND c.estado_carretera != 'Optimo'
ORDER BY c.timestamp DESC
LIMIT 100
        """.strip(),
    },
    "gestor_incidencias_resumen": {
        "titulo": "Gestor — resumen de incidencias (últimas 24h)",
        "sql": f"""
SELECT estado, COUNT(*) as total, AVG(pagerank) as pagerank_promedio
FROM {HIVE_DB_NAME}.{_T_EVENTOS}
WHERE timestamp >= date_sub(current_timestamp(), 1)
GROUP BY estado
ORDER BY total DESC
LIMIT 20
        """.strip(),
    },
    "gestor_pagerank_historico": {
        "titulo": "Gestor — nodos por PageRank (última semana)",
        "sql": f"""
SELECT id_elemento as nodo, estado, motivo, pagerank, hub_asociado
FROM {HIVE_DB_NAME}.{_T_EVENTOS}
WHERE tipo_evento = 'nodo'
  AND pagerank > 0
  AND timestamp >= date_sub(current_timestamp(), 7)
ORDER BY pagerank DESC
LIMIT 100
        """.strip(),
    },
}


def _parse_hiveserver2_host_port() -> Tuple[str, int]:
    """Host y puerto de HiveServer2 desde JDBC o HIVE_SERVER (p. ej. 127.0.0.1:10000)."""
    jdbc = os.environ.get("HIVE_JDBC_URL", HIVE_JDBC_URL)
    m = re.match(r"jdbc:hive2://([^:/]+)(?::(\d+))?", jdbc.strip())
    if m:
        return m.group(1), int(m.group(2) or "10000")
    server = os.environ.get("HIVE_SERVER", "127.0.0.1:10000")
    if ":" in server:
        h, p = server.rsplit(":", 1)
        return h, int(p)
    return server, 10000


def _ejecutar_hive_pyhive(sql: str) -> Tuple[bool, str, str]:
    """
    Ejecuta HiveQL vía PyHive (Thrift a HiveServer2, puerto 10000).
    Salida en formato TSV con cabecera (similar a beeline tsv2).
    """
    try:
        from pyhive import hive
    except ImportError as e:
        return (
            False,
            f"PyHive no disponible ({e}). Instala: pip install pyhive thrift",
            "",
        )
    host, port = _parse_hiveserver2_host_port()
    db = (HIVE_DB_NAME or "").strip() or None
    auth_pref = os.environ.get("SIMLOG_HIVE_PYHIVE_AUTH", "").strip().upper()
    # NOSASL no requiere thrift_sasl; NONE sí (HiveServer2 con SASL/PLAIN típico).
    modos_auth = [auth_pref] if auth_pref else ["NOSASL", "NONE"]
    conn = None
    ultimo_err: Optional[str] = None
    try:
        for auth in modos_auth:
            try:
                conn = hive.Connection(
                    host=host,
                    port=port,
                    username=HIVE_BEELINE_USER,
                    database=db,
                    auth=auth,
                )
                break
            except Exception as e:
                ultimo_err = str(e)
                continue
        if conn is None:
            return (
                False,
                ultimo_err
                or "No se pudo abrir sesión Hive (prueba SIMLOG_HIVE_PYHIVE_AUTH=NOSASL o NONE; "
                "instala thrift_sasl si usas NONE).",
                "",
            )
        cur = conn.cursor()
        cur.execute(sql)
        if not cur.description:
            return True, "", ""
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        lines = ["\t".join(cols)]
        for row in rows:
            lines.append("\t".join("" if v is None else str(v) for v in row))
        return True, "", "\n".join(lines)
    except Exception as e:
        return False, str(e), ""


def ejecutar_hive_sql_seguro(sql: str) -> Tuple[bool, str, str]:
    """
    Ejecuta HiveQL de solo lectura (SHOW / SELECT / WITH / DESCRIBE) vía PyHive.
    Para integraciones que construyen el SQL en servidor (p. ej. asistente de flota).
    """
    sql = sql.strip()
    u = sql.upper().lstrip()
    if not (
        u.startswith("SHOW")
        or u.startswith("SELECT")
        or u.startswith("WITH")
        or u.startswith("DESCRIBE")
        or u.startswith("DESC ")
    ):
        return False, "Solo se permiten SHOW, SELECT, WITH o DESCRIBE", ""
    timeout = int(os.environ.get("HIVE_QUERY_TIMEOUT_SEC", "300"))
    return _ejecutar_hive_pyhive_con_timeout(sql, timeout=timeout)


def ejecutar_hive_sql_internal(sql: str) -> Tuple[bool, str, str]:
    """
    Ejecuta HiveQL interno (para la UI) vía PyHive.

    Limitado a operaciones controladas: hoy se usa para crear vistas alias
    cuando existen tablas con nombre alternativo (p. ej. *_conteo).
    """
    sql = sql.strip().rstrip(";")
    u = sql.upper().lstrip()
    # Seguridad mínima: solo permitimos CREATE VIEW IF NOT EXISTS ... AS SELECT * FROM ...
    if not u.startswith("CREATE VIEW IF NOT EXISTS"):
        return False, "Solo se permiten CREATE VIEW IF NOT EXISTS para operaciones internas.", ""
    if " AS SELECT * FROM " not in u:
        return False, "CREATE VIEW interno restringido a 'AS SELECT * FROM ...'.", ""
    timeout = int(os.environ.get("HIVE_QUERY_TIMEOUT_SEC", "300"))
    return _ejecutar_hive_pyhive_con_timeout(sql, timeout=timeout)


def ejecutar_hive_consulta(codigo: str) -> Tuple[bool, str, str]:
    """
    Ejecuta consulta Hive predefinida mediante PyHive (HiveServer2 en el puerto 10000).
    Devuelve (ok, mensaje_error, salida_texto).
    """
    if codigo not in HIVE_CONSULTAS:
        return False, f"Consulta no permitida: {codigo}", ""
    sql = HIVE_CONSULTAS[codigo]["sql"].strip()
    if not (
        sql.upper().startswith("SHOW")
        or sql.upper().startswith("SELECT")
        or sql.upper().startswith("WITH")
    ):
        return False, "Solo se permiten SHOW o SELECT", ""

    timeout = int(os.environ.get("HIVE_QUERY_TIMEOUT_SEC", "300"))
    sql_u = sql.upper().lstrip()
    can_fallback = sql_u.startswith("SELECT") or sql_u.startswith("WITH")

    ok, err, out = _ejecutar_hive_pyhive_con_timeout(sql, timeout=timeout)
    timed_out = err.startswith("Timeout PyHive")

    if ok:
        return True, "", out

    if timed_out:
        if can_fallback and os.environ.get("SIMLOG_HIVE_EXEC_FALLBACK_SPARK", "0") == "1":
            ok_s, err_s, out_s = _ejecutar_hive_consulta_spark(sql, max_rows=2000)
            if ok_s:
                return True, "", out_s
            return (
                False,
                f"Timeout PyHive ({timeout}s) y fallback Spark falló. "
                f"Consulta='{codigo}' · SQL='{sql}'. "
                f"fallback_spark_error: {err_s[:400]}.",
                "",
            )
        return (
            False,
            f"Timeout PyHive ({timeout}s). Consulta='{codigo}' · SQL='{sql}'. "
            "Prueba `diag_smoke_hive` (SELECT 1) y sube `HIVE_QUERY_TIMEOUT_SEC` si HS2 va lento.",
            "",
        )

    if err:
        return False, err, out or ""

    return (
        False,
        f"Hive no devolvió resultado. Consulta='{codigo}' · SQL='{sql}'. "
        "Confirma HiveServer2 en el puerto 10000 (`HIVE_JDBC_URL` / `HIVE_SERVER`).",
        out or "",
    )


@lru_cache(maxsize=1)
def _get_spark_hive_session():
    """
    SparkSession con catálogo Hive (metastore local embebido).
    Usa el metastore thrift compartido para no abrir Derby embebido en paralelo.
    """
    from pyspark.sql import SparkSession

    os.environ.setdefault("HIVE_CONF_DIR", HIVE_CONF_DIR)
    os.environ.setdefault("SIMLOG_HIVE_CONF_DIR", HIVE_CONF_DIR)
    os.environ.setdefault("HIVE_METASTORE_URIS", HIVE_METASTORE_URIS)
    return (
        SparkSession.builder.master("local[*]")
        .appName("SIMLOG_HiveQueryUI")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.warehouse.dir", "/user/hive/warehouse")
        .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URIS)
        .config("hive.metastore.uris", HIVE_METASTORE_URIS)
        .enableHiveSupport()
        .getOrCreate()
    )


def _ejecutar_hive_consulta_spark(sql: str, max_rows: int = 2000) -> Tuple[bool, str, str]:
    """
    Ejecuta HiveQL usando Spark SQL como fallback cuando PyHive timeoutea.
    Devuelve (ok, error, salida_tsv).
    """
    try:
        spark = _get_spark_hive_session()
        # Aseguramos que no recolectamos demasiado si la consulta falla al incluir LIMIT.
        df = spark.sql(sql).limit(max_rows)
        cols = df.columns
        rows = df.collect()
        lines: List[str] = ["\t".join(cols)]
        for r in rows:
            # r[col] funciona con Row.
            lines.append("\t".join("" if (r[c] is None) else str(r[c]) for c in cols))
        return True, "", "\n".join(lines)
    except Exception as e:
        msg = str(e)
        if "XSDB6" in msg or "Another instance of Derby" in msg:
            return (
                False,
                "Derby metastore ya en uso (p. ej. HiveServer2 activo). No abrir dos JVM sobre la "
                "misma metastore_db. Opciones: subir HIVE_QUERY_TIMEOUT_SEC y usar solo PyHive; "
                "o SIMLOG_HIVE_EXEC_FALLBACK_SPARK=0 (por defecto); o metastore remoto (MySQL/Postgres).",
                "",
            )
        if "SessionHiveMetaStoreClient" in msg or "Unable to instantiate" in msg:
            return (
                False,
                "Spark no pudo crear el cliente del metastore Hive (HS2 inaccesible, Derby bloqueado "
                "u otra JVM). El fallback Spark no sustituye a PyHive en este modo. "
                "Usa solo PyHive contra HiveServer2, reinicia HS2 o configura metastore remoto. "
                "Mantén SIMLOG_HIVE_EXEC_FALLBACK_SPARK=0.",
                "",
            )
        return False, msg, ""


def _ejecutar_hive_pyhive_con_timeout(sql: str, timeout: int) -> Tuple[bool, str, str]:
    """
    Ejecuta PyHive en un hilo daemon para que un timeout no bloquee la salida del proceso.
    """
    queue: Queue[Tuple[bool, str, str]] = Queue(maxsize=1)

    def _runner() -> None:
        try:
            queue.put(_ejecutar_hive_pyhive(sql))
        except Exception as exc:
            queue.put((False, str(exc), ""))

    thread = Thread(target=_runner, name="simlog-hive-query", daemon=True)
    thread.start()
    try:
        return queue.get(timeout=timeout)
    except Exception:
        return False, f"Timeout PyHive ({timeout}s)", ""


def listar_claves_cassandra() -> List[str]:
    return list(CASSANDRA_CONSULTAS.keys())


def listar_claves_hive() -> List[str]:
    return list(HIVE_CONSULTAS.keys())


def titulo_cassandra(codigo: str) -> str:
    return CASSANDRA_CONSULTAS.get(codigo, {}).get("titulo", codigo)


def titulo_hive(codigo: str) -> str:
    return HIVE_CONSULTAS.get(codigo, {}).get("titulo", codigo)


# ============================================================================
# Categorías para organizar las consultas en el frontend
# ============================================================================
HIVE_CATEGORIAS: Dict[str, Dict[str, List[str]]] = {
    "diagnostico": {
        "nombre": "Diagnóstico",
        "descripcion": "Verificación de conexión y tablas disponibles",
        "icono": "🔧",
        "consultas": ["diag_smoke_hive", "tablas_bd"],
    },
    "eventos": {
        "nombre": "Eventos Histórico",
        "descripcion": "Histórico de eventos de nodos y aristas",
        "icono": "📋",
        "consultas": ["eventos_historico_muestra", "eventos_nodos_24h", "eventos_bloqueos_24h", "eventos_evolucion_dia"],
    },
    "clima": {
        "nombre": "Clima Histórico",
        "descripcion": "Datos climáticos y su impacto en carreteras",
        "icono": "🌤️",
        "consultas": ["clima_historico_muestra", "clima_historico_hoy", "clima_estado_carretera"],
    },
    "tracking": {
        "nombre": "Tracking Camiones",
        "descripcion": "Histórico de posiciones GPS de la flota",
        "icono": "🚛",
        "consultas": ["tracking_historico_muestra", "tracking_camion_especifico", "tracking_ultima_posicion"],
    },
    "transporte": {
        "nombre": "Transporte Ingestado",
        "descripcion": "Datos de ingestión de transporte (pipeline KDD)",
        "icono": "📦",
        "consultas": ["transporte_ingesta_real_muestra", "transporte_ingesta_hoy", "transporte_retrasos_hoy", "gestor_historial_rutas_camion"],
    },
    "rutas": {
        "nombre": "Rutas Alternativas",
        "descripcion": "Rutas alternativas calculadas por el sistema",
        "icono": "🛤️",
        "consultas": ["rutas_alternativas_muestra", "rutas_alternativas_bloqueos"],
    },
    "agregaciones": {
        "nombre": "Agregaciones Diarias",
        "descripcion": "Estadísticas y agregaciones calculadas",
        "icono": "📊",
        "consultas": ["agg_estadisticas_diarias", "agg_ultima_semana"],
    },
    "gestor": {
        "nombre": "Gestor",
        "descripcion": "Consultas específicas para el gestor de operaciones",
        "icono": "👤",
        "consultas": ["gestor_eventos_por_hub", "gestor_clima_afecta_transporte", "gestor_incidencias_resumen", "gestor_pagerank_historico"],
    },
}


def listar_categorias_hive() -> List[str]:
    """Lista de claves de categorías ordenadas."""
    orden = ["diagnostico", "eventos", "clima", "tracking", "transporte", "rutas", "agregaciones", "gestor"]
    return [c for c in orden if c in HIVE_CATEGORIAS]


def obtener_categoria(nombre_categoria: str) -> Dict[str, Any]:
    """Obtiene la información de una categoría."""
    return HIVE_CATEGORIAS.get(nombre_categoria, {})


def obtener_consultas_de_categoria(nombre_categoria: str) -> List[str]:
    """Obtiene las claves de consultas de una categoría."""
    cat = HIVE_CATEGORIAS.get(nombre_categoria, {})
    return cat.get("consultas", [])


def nombre_categoria(nombre_categoria: str) -> str:
    """Obtiene el nombre amigable de una categoría."""
    cat = HIVE_CATEGORIAS.get(nombre_categoria, {})
    icono = cat.get("icono", "📁")
    nombre = cat.get("nombre", nombre_categoria)
    return f"{icono} {nombre}"


def descripcion_categoria(nombre_categoria: str) -> str:
    """Obtiene la descripción de una categoría."""
    return HIVE_CATEGORIAS.get(nombre_categoria, {}).get("descripcion", "")
