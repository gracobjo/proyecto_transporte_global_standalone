"""
Consultas supervisadas (whitelist) para Cassandra y Hive — cuadro de mando SIMLOG.

No se ejecuta SQL arbitrario del usuario: solo plantillas aprobadas para supervisión y control.
"""
from __future__ import annotations

import hashlib
import os
import re
import threading
import time
from collections import OrderedDict
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
    HIVE_TABLE_HISTORICO_NODOS,
    HIVE_TABLE_NODOS_MAESTRO,
    HIVE_TABLE_RED_GEMELO_ARISTAS,
    HIVE_TABLE_RED_GEMELO_NODOS,
    HIVE_TABLE_TRACKING_HIST,
    HIVE_TABLE_TRANSPORTE_HIST,
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
            "FROM pagerank_nodos LIMIT 500"
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
        out = [_row_to_dict(r) for r in rows]

        # Cassandra no soporta ORDER BY global (sin partición restringida).
        # Para "top N" por PageRank, ordenamos en cliente sobre el resultado limitado.
        if codigo in {"gestor_pagerank_nodos_criticos", "gestor_nodo_critico_pagerank"}:
            out = sorted(out, key=lambda d: float(d.get("pagerank") or 0.0), reverse=True)[:20]

        return True, "", out
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
# - transporte (HIVE_TABLE_TRANSPORTE_HIST / SIMLOG_HIVE_TABLA_TRANSPORTE): plano para UI
# - rutas_alternativas_historico: rutas alternativas calculadas
# - agg_estadisticas_diarias: agregaciones diarias
# Tablas en HDFS (compatibilidad):
# - nodos_maestro: datos maestros de nodos (CSV en HDFS)

HIVE_DB_NAME = HIVE_DB or "logistica_espana"
_T_EVENTOS = "eventos_historico"
_T_CLIMA = "clima_historico"
_T_TRACKING = HIVE_TABLE_TRACKING_HIST
_T_TRANSPORTE = HIVE_TABLE_TRANSPORTE_HIST
_T_RUTAS = "rutas_alternativas_historico"
_T_AGG = "agg_estadisticas_diarias"
# Hive 3+: la columna se llama "timestamp" en persistencia_hive.py; el identificador es palabra reservada.
# Usar `timestamp` en HiveQL (no afecta a current_timestamp()).
_HIVE_TS = "`timestamp`"


def _hive_txt_norm(col: str) -> str:
    """
    Expresión HiveQL: trim, minúsculas y sin tildes típicas (á→a, ó→o) para comparar estados
    sin depender de mayúsculas ni de «Óptimo» vs «Optimo». `col` es siempre columna fija del código.
    """
    return (
        "translate(lower(trim(coalesce(cast("
        + col
        + " as string), ''))), 'áéíóúñÁÉÍÓÚÑ', 'aeiouaeioun')"
    )


def _hive_prune_mes() -> str:
    """Poda por partición mes actual (tablas con anio_part/mes_part). Desactivar: SIMLOG_HIVE_PARTITION_PRUNING=0."""
    if os.environ.get("SIMLOG_HIVE_PARTITION_PRUNING", "1").strip() == "0":
        return ""
    return " AND anio_part = year(current_date()) AND mes_part = month(current_date())"


def _hive_prune_7d() -> str:
    """Poda por hasta 2 particiones (mes actual y mes de hace ~6 días). Útil para ventanas 24h/7d."""
    if os.environ.get("SIMLOG_HIVE_PARTITION_PRUNING", "1").strip() == "0":
        return ""
    return (
        " AND ((anio_part = year(current_date()) AND mes_part = month(current_date())) OR "
        "(anio_part = year(date_sub(current_date(), 6)) AND mes_part = month(date_sub(current_date(), 6))))"
    )


# Fragmentos evaluados al import (cambiar SIMLOG_HIVE_PARTITION_PRUNING requiere reiniciar el proceso).
_PM = _hive_prune_mes()
_P7 = _hive_prune_7d()


def _hive_filter_24h_dia() -> str:
    """
    En ventanas «últimas 24h», restringe a hoy y ayer calendario (solo anio/mes/dia).
    Evita make_date(), que en algunos despliegues Hive provoca NPE al resolver UDF.
    Desactivar: SIMLOG_HIVE_DAY_FILTER_24H=0.
    """
    if os.environ.get("SIMLOG_HIVE_DAY_FILTER_24H", "1").strip() == "0":
        return ""
    return (
        " AND ("
        "(anio = year(current_date()) AND mes = month(current_date()) AND dia = day(current_date())) OR "
        "(anio = year(date_sub(current_date(), 1)) AND mes = month(date_sub(current_date(), 1)) "
        "AND dia = day(date_sub(current_date(), 1)))"
        ")"
    )


def _hive_filter_24h_dia_c() -> str:
    """Igual que _hive_filter_24h_dia pero con alias de tabla `c` (JOIN clima)."""
    if os.environ.get("SIMLOG_HIVE_DAY_FILTER_24H", "1").strip() == "0":
        return ""
    return (
        " AND ("
        "(c.anio = year(current_date()) AND c.mes = month(current_date()) AND c.dia = day(current_date())) OR "
        "(c.anio = year(date_sub(current_date(), 1)) AND c.mes = month(date_sub(current_date(), 1)) "
        "AND c.dia = day(date_sub(current_date(), 1)))"
        ")"
    )


def _hive_filter_last_7d_ymd(alias: str = "") -> str:
    """
    Ventana robusta últimos 7 días usando columnas anio/mes/dia.
    Evita comparar `timestamp` STRING con fechas de Hive.
    """
    p = f"{alias}." if alias else ""
    return (
        f" AND ({p}anio * 10000 + {p}mes * 100 + {p}dia) >= "
        "year(date_sub(current_date(), 7)) * 10000 + month(date_sub(current_date(), 7)) * 100 "
        "+ day(date_sub(current_date(), 7))"
        f" AND ({p}anio * 10000 + {p}mes * 100 + {p}dia) <= "
        "year(current_date()) * 10000 + month(current_date()) * 100 + day(current_date())"
    )


_F24 = _hive_filter_24h_dia()
_F24C = _hive_filter_24h_dia_c()
_F7D = _hive_filter_last_7d_ymd()

# Timeout PyHive (segundos). Por defecto 600: HS2 lento o primera consulta suele superar 300s.
_DEFAULT_HIVE_QUERY_TIMEOUT_SEC = 600

# Ejecución previa opcional: SIMLOG_HIVE_SET="SET hive.fetch.task.conversion=more;SET hive.execution.engine=tez"
# (separar con ';'; Tez si existe; en MapReduce el coste fijo del job sigue siendo alto en standalone).

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
    "historico_nodos_muestra": {
        "titulo": "Histórico nodos — muestra",
        "sql": f"SELECT * FROM {HIVE_DB_NAME}.{HIVE_TABLE_HISTORICO_NODOS} LIMIT 100",
    },
    "historico_nodos_conteo": {
        "titulo": "Histórico nodos — conteo",
        "sql": f"SELECT COUNT(*) AS total FROM {HIVE_DB_NAME}.{HIVE_TABLE_HISTORICO_NODOS}",
    },
    "nodos_maestro_muestra": {
        "titulo": "Nodos maestro — muestra",
        "sql": f"SELECT * FROM {HIVE_DB_NAME}.{HIVE_TABLE_NODOS_MAESTRO} LIMIT 100",
    },
    "nodos_maestro_conteo": {
        "titulo": "Nodos maestro — conteo",
        "sql": f"SELECT COUNT(*) AS total FROM {HIVE_DB_NAME}.{HIVE_TABLE_NODOS_MAESTRO}",
    },
    "gemelo_red_nodos": {
        "titulo": "Gemelo digital — red nodos (Hive)",
        "sql": f"""
SELECT id_nodo, tipo, lat, lon, id_capital_ref, nombre
FROM {HIVE_DB_NAME}.{HIVE_TABLE_RED_GEMELO_NODOS}
LIMIT 20000
        """.strip(),
    },
    "gemelo_red_aristas": {
        "titulo": "Gemelo digital — red aristas (Hive)",
        "sql": f"""
SELECT src, dst, distancia_km
FROM {HIVE_DB_NAME}.{HIVE_TABLE_RED_GEMELO_ARISTAS}
LIMIT 100000
        """.strip(),
    },
    # --- Eventos histórico (eventos_historico) ---
    "eventos_historico_muestra": {
        "titulo": "Eventos histórico — todos (muestra)",
        "sql": f"""
SELECT {_HIVE_TS}, tipo_evento, id_elemento, tipo_elemento, estado, motivo, hub_asociado, pagerank
FROM {HIVE_DB_NAME}.{_T_EVENTOS}
WHERE 1=1{_PM}
ORDER BY {_HIVE_TS} DESC
LIMIT 100
        """.strip(),
    },
    "eventos_nodos_24h": {
        "titulo": "Eventos — nodos últimas 24h",
        "sql": f"""
SELECT {_HIVE_TS}, id_elemento, estado, motivo, hub_asociado, pagerank
FROM {HIVE_DB_NAME}.{_T_EVENTOS}
WHERE {_hive_txt_norm("tipo_evento")} = 'nodo'
  AND 1=1{_P7}{_F24}
ORDER BY {_HIVE_TS} DESC
LIMIT 200
        """.strip(),
    },
    "eventos_bloqueos_24h": {
        "titulo": "Eventos — bloqueos últimas 24h",
        "sql": f"""
SELECT {_HIVE_TS}, id_elemento, estado, motivo, hub_asociado
FROM {HIVE_DB_NAME}.{_T_EVENTOS}
WHERE {_hive_txt_norm("estado")} = 'bloqueado'
  AND 1=1{_P7}{_F24}
ORDER BY {_HIVE_TS} DESC
LIMIT 100
        """.strip(),
    },
    "eventos_evolucion_dia": {
        "titulo": "Eventos — evolución por día (últimos 7 días)",
        "sql": f"""
SELECT dia, tipo_evento, estado, COUNT(*) as total
FROM {HIVE_DB_NAME}.{_T_EVENTOS}
WHERE 1=1{_P7}{_F7D}
GROUP BY dia, tipo_evento, estado
ORDER BY dia DESC, total DESC
LIMIT 200
        """.strip(),
    },
    # --- Clima histórico (clima_historico) ---
    "clima_historico_muestra": {
        "titulo": "Clima histórico — todos (muestra)",
        "sql": f"""
SELECT {_HIVE_TS}, ciudad, temperatura, humedad, descripcion, visibilidad, estado_carretera
FROM {HIVE_DB_NAME}.{_T_CLIMA}
WHERE 1=1{_PM}
ORDER BY {_HIVE_TS} DESC
LIMIT 100
        """.strip(),
    },
    "clima_historico_hoy": {
        "titulo": "Clima histórico — hoy",
        "sql": f"""
SELECT {_HIVE_TS}, ciudad, temperatura, humedad, descripcion, estado_carretera
FROM {HIVE_DB_NAME}.{_T_CLIMA}
WHERE anio = year(current_date()) AND mes = month(current_date()) AND dia = day(current_date()){_PM}
ORDER BY {_HIVE_TS} DESC
LIMIT 50
        """.strip(),
    },
    "clima_estado_carretera": {
        "titulo": "Clima — estado de carreteras por clima",
        "sql": f"""
SELECT estado_carretera, descripcion, COUNT(*) as total
FROM {HIVE_DB_NAME}.{_T_CLIMA}
WHERE 1=1{_P7}{_F24}
GROUP BY estado_carretera, descripcion
ORDER BY total DESC
LIMIT 50
        """.strip(),
    },
    # --- Tracking camiones histórico (tracking_camiones_historico) ---
    "tracking_historico_muestra": {
        "titulo": "Tracking histórico — muestra",
        "sql": f"""
SELECT {_HIVE_TS}, id_camion, origen, destino, nodo_actual, lat_actual, lon_actual, progreso_pct, distancia_total_km
FROM {HIVE_DB_NAME}.{_T_TRACKING}
WHERE 1=1{_PM}
ORDER BY {_HIVE_TS} DESC
LIMIT 100
        """.strip(),
    },
    "tracking_camion_especifico": {
        "titulo": "Tracking — por camión (filtrar en cliente)",
        "sql": f"""
SELECT {_HIVE_TS}, id_camion, origen, destino, nodo_actual, lat_actual, lon_actual, progreso_pct, distancia_total_km, tiene_ruta_alternativa
FROM {HIVE_DB_NAME}.{_T_TRACKING}
WHERE 1=1{_P7}{_F7D}
ORDER BY {_HIVE_TS} DESC
LIMIT 200
        """.strip(),
    },
    "tracking_ultima_posicion": {
        "titulo": "Tracking — muestra reciente (24h; últimas filas por tiempo)",
        "sql": f"""
SELECT id_camion, origen, destino, nodo_actual, lat_actual, lon_actual, progreso_pct, {_HIVE_TS}
FROM {HIVE_DB_NAME}.{_T_TRACKING}
WHERE 1=1{_P7}{_F24}
ORDER BY {_HIVE_TS} DESC
LIMIT 200
        """.strip(),
    },
    # --- Transporte ingestado (HIVE_TABLE_TRANSPORTE_HIST) ---
    "transporte_ingesta_real_muestra": {
        "titulo": "Transporte ingestado — muestra",
        "sql": f"""
SELECT {_HIVE_TS}, id_camion, origen, destino, nodo_actual, lat, lon, progreso_pct, estado_ruta, motivo_retraso, ruta
FROM {HIVE_DB_NAME}.{_T_TRANSPORTE}
WHERE 1=1{_PM}
ORDER BY {_HIVE_TS} DESC
LIMIT 100
        """.strip(),
    },
    "transporte_ingesta_hoy": {
        "titulo": "Transporte ingestado — hoy",
        "sql": f"""
SELECT {_HIVE_TS}, id_camion, origen, destino, estado_ruta, motivo_retraso, progreso_pct
FROM {HIVE_DB_NAME}.{_T_TRANSPORTE}
WHERE anio = year(current_date()) AND mes = month(current_date()) AND dia = day(current_date()){_PM}
ORDER BY {_HIVE_TS} DESC
LIMIT 200
        """.strip(),
    },
    "transporte_retrasos_hoy": {
        "titulo": "Transporte — camiones con retrasos hoy",
        "sql": f"""
SELECT {_HIVE_TS}, id_camion, origen, destino, estado_ruta, motivo_retraso
FROM {HIVE_DB_NAME}.{_T_TRANSPORTE}
WHERE estado_ruta IS NOT NULL
  AND {_hive_txt_norm("estado_ruta")} <> 'en ruta'
  AND anio = year(current_date()) AND mes = month(current_date()) AND dia = day(current_date()){_PM}
ORDER BY {_HIVE_TS} DESC
LIMIT 100
        """.strip(),
    },
    "gestor_historial_rutas_camion": {
        "titulo": "Gestor — histórico transporte por camión",
        "sql": f"""
SELECT {_HIVE_TS}, id_camion, origen, destino, nodo_actual, progreso_pct, estado_ruta, motivo_retraso
FROM {HIVE_DB_NAME}.{_T_TRANSPORTE}
WHERE 1=1{_P7}{_F7D}
ORDER BY id_camion, {_HIVE_TS} DESC
LIMIT 300
        """.strip(),
    },
    # --- Rutas alternativas (rutas_alternativas_historico) ---
    "rutas_alternativas_muestra": {
        "titulo": "Rutas alternativas — muestra",
        "sql": f"""
SELECT {_HIVE_TS}, origen, destino, ruta_original, ruta_alternativa, distancia_original_km, distancia_alternativa_km, motivo_bloqueo, ahorro_km
FROM {HIVE_DB_NAME}.{_T_RUTAS}
WHERE 1=1{_PM}
ORDER BY {_HIVE_TS} DESC
LIMIT 100
        """.strip(),
    },
    "rutas_alternativas_bloqueos": {
        "titulo": "Rutas alternativas — bloqueos detectados",
        "sql": f"""
SELECT {_HIVE_TS}, origen, destino, ruta_original, motivo_bloqueo, ahorro_km
FROM {HIVE_DB_NAME}.{_T_RUTAS}
WHERE length(trim(coalesce(cast(motivo_bloqueo as string), ''))) > 0
  AND 1=1{_P7}{_F7D}
ORDER BY {_HIVE_TS} DESC
LIMIT 100
        """.strip(),
    },
    # --- Agregaciones diarias (agg_estadisticas_diarias) ---
    "agg_estadisticas_diarias": {
        "titulo": "Agregaciones — estadísticas diarias",
        "sql": f"""
SELECT anio, mes, dia, tipo_evento, estado, motivo, contador, pct_total
FROM {HIVE_DB_NAME}.{_T_AGG}
WHERE 1=1{_PM}
ORDER BY anio DESC, mes DESC, dia DESC, contador DESC
LIMIT 200
        """.strip(),
    },
    "agg_ultima_semana": {
        "titulo": "Agregaciones — última semana",
        "sql": f"""
SELECT anio, mes, dia, tipo_evento, estado, motivo, contador, pct_total
FROM {HIVE_DB_NAME}.{_T_AGG}
WHERE anio * 10000 + mes * 100 + dia >=
      year(date_sub(current_date(), 7)) * 10000 + month(date_sub(current_date(), 7)) * 100
      + day(date_sub(current_date(), 7))
  AND anio * 10000 + mes * 100 + dia <=
      year(current_date()) * 10000 + month(current_date()) * 100 + day(current_date()){_P7}
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
WHERE 1=1{_P7}{_F24}
GROUP BY hub_asociado, estado
ORDER BY total DESC
LIMIT 100
        """.strip(),
    },
    "gestor_clima_afecta_transporte": {
        "titulo": "Gestor — clima adverso que afecta transporte",
        # Subconsultas con poda de partición y ventana 24h. El JOIN es por ciudad = hub_actual
        # y misma fecha (anio/mes/dia): igualar `timestamp` STRING entre tablas casi nunca coincide.
        # hive.auto.convert.join=false (sesión) evita MapredLocalTask por mapjoin en HS2/MR.
        "sql": f"""
SELECT c.{_HIVE_TS}, c.ciudad, c.estado_carretera, c.descripcion,
       max(t.estado_ruta) AS estado_ruta, max(t.motivo_retraso) AS motivo_retraso
FROM (
  SELECT {_HIVE_TS}, ciudad, estado_carretera, descripcion, anio, mes, dia
  FROM {HIVE_DB_NAME}.{_T_CLIMA}
  WHERE 1=1{_P7}{_F24}
    AND length(trim(coalesce(cast(estado_carretera as string), ''))) > 0
    AND {_hive_txt_norm("estado_carretera")} NOT IN ('optimo', 'optimal', 'ok')
) c
LEFT JOIN (
  SELECT estado_ruta, motivo_retraso, hub_actual, anio, mes, dia
  FROM {HIVE_DB_NAME}.{_T_TRANSPORTE}
  WHERE 1=1{_P7}{_F24}
) t
  ON c.ciudad = t.hub_actual
 AND c.anio = t.anio AND c.mes = t.mes AND c.dia = t.dia
GROUP BY c.{_HIVE_TS}, c.ciudad, c.estado_carretera, c.descripcion
ORDER BY c.{_HIVE_TS} DESC
LIMIT 100
        """.strip(),
    },
    "gestor_incidencias_resumen": {
        "titulo": "Gestor — resumen de incidencias (últimas 24h)",
        "sql": f"""
SELECT estado, COUNT(*) as total, AVG(pagerank) as pagerank_promedio
FROM {HIVE_DB_NAME}.{_T_EVENTOS}
WHERE 1=1{_P7}{_F24}
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
WHERE {_hive_txt_norm("tipo_evento")} = 'nodo'
  AND pagerank > 0
  AND 1=1{_P7}{_F7D}
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


_hive_pyhive_lock = threading.Lock()
_hive_pyhive_conn: Any = None
# Incrementar si cambian los SET por defecto: fuerza re-aplicación en conexiones cacheadas.
_HIVE_SESSION_TUNING_VERSION = "2"


def _hive_default_session_sets() -> List[str]:
    """
    SETs aplicados una vez por conexión (consultas UI con LIMIT).
    Desactivar: SIMLOG_HIVE_APPLY_DEFAULT_SETS=0.
    Sobrescribir lista: SIMLOG_HIVE_DEFAULT_SETS='SET ...;SET ...'
    """
    if os.environ.get("SIMLOG_HIVE_APPLY_DEFAULT_SETS", "1").strip() == "0":
        return []
    custom = os.environ.get("SIMLOG_HIVE_DEFAULT_SETS", "").strip()
    if custom:
        return [s.strip() for s in custom.split(";") if s.strip()]
    # Sin hive.fetch.task.conversion=more aquí: en algunos despliegíos MR interfiere con JOINs
    # y dispara MapredLocalTask (fallo típico HS2). Para SELECT simples rápidos, usa
    # SIMLOG_HIVE_DEFAULT_SETS o SIMLOG_HIVE_SET con SET hive.fetch.task.conversion=more
    return [
        "SET hive.vectorized.execution.enabled=true",
        "SET hive.auto.convert.join=false",
    ]


def _hive_close_pyhive_conn() -> None:
    global _hive_pyhive_conn
    c = _hive_pyhive_conn
    _hive_pyhive_conn = None
    if c is not None:
        try:
            c.close()
        except Exception:
            pass


def _ejecutar_hive_pyhive(sql: str) -> Tuple[bool, str, str]:
    """
    Ejecuta HiveQL vía PyHive (Thrift a HiveServer2, puerto 10000).
    Salida en formato TSV con cabecera (similar a beeline tsv2).

    Por defecto reutiliza una conexión por proceso (SIMLOG_HIVE_REUSE_CONNECTION=1) para que
    HiveServer2/Tez no paguen el arranque de sesión en cada clic del cuadro de mando.
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
    modos_auth = [auth_pref] if auth_pref else ["NOSASL", "NONE"]
    reuse = os.environ.get("SIMLOG_HIVE_REUSE_CONNECTION", "1").strip() != "0"
    global _hive_pyhive_conn

    def _run_query(conn: Any) -> Tuple[bool, str, str]:
        cur = conn.cursor()
        if getattr(conn, "_simlog_hive_tune_ver", None) != _HIVE_SESSION_TUNING_VERSION:
            setattr(conn, "_simlog_hive_warmed", False)
            setattr(conn, "_simlog_hive_tune_ver", _HIVE_SESSION_TUNING_VERSION)
        if not getattr(conn, "_simlog_hive_warmed", False):
            for stmt in _hive_default_session_sets():
                try:
                    cur.execute(stmt)
                except Exception:
                    pass
            setattr(conn, "_simlog_hive_warmed", True)
        extra_sets = os.environ.get("SIMLOG_HIVE_SET", "").strip()
        if extra_sets:
            for stmt in extra_sets.split(";"):
                s = stmt.strip()
                if s:
                    cur.execute(s)
        cur.execute(sql)
        if not cur.description:
            return True, "", ""
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        lines = ["\t".join(cols)]
        for row in rows:
            lines.append("\t".join("" if v is None else str(v) for v in row))
        return True, "", "\n".join(lines)

    def _open_connection() -> Any:
        ultimo_err: Optional[str] = None
        for auth in modos_auth:
            try:
                return hive.Connection(
                    host=host,
                    port=port,
                    username=HIVE_BEELINE_USER,
                    database=db,
                    auth=auth,
                )
            except Exception as e:
                ultimo_err = str(e)
                continue
        raise RuntimeError(
            ultimo_err
            or "No se pudo abrir sesión Hive (prueba SIMLOG_HIVE_PYHIVE_AUTH=NOSASL o NONE; "
            "instala thrift_sasl si usas NONE)."
        )

    created_conn: Any = None
    try:
        with _hive_pyhive_lock:
            if reuse and _hive_pyhive_conn is not None:
                try:
                    return _run_query(_hive_pyhive_conn)
                except Exception:
                    _hive_close_pyhive_conn()
            created_conn = _open_connection()
            if reuse:
                _hive_pyhive_conn = created_conn
            return _run_query(created_conn)
    except Exception as e:
        _hive_close_pyhive_conn()
        return False, str(e), ""
    finally:
        if not reuse and created_conn is not None:
            try:
                created_conn.close()
            except Exception:
                pass


# --- Caché resultados Hive (misma SQL → respuesta rápida dentro del TTL) ---
_hive_sql_cache_lock = threading.Lock()
_hive_sql_cache: "OrderedDict[str, Tuple[float, Tuple[bool, str, str]]]" = OrderedDict()


def _hive_sql_cache_key(sql: str) -> str:
    return hashlib.sha256(sql.strip().encode("utf-8")).hexdigest()


def _hive_sql_cache_ttl_sec() -> float:
    return float(os.environ.get("SIMLOG_HIVE_CACHE_TTL_SEC", "120"))


def _hive_sql_cache_max_entries() -> int:
    return max(4, int(os.environ.get("SIMLOG_HIVE_CACHE_MAX_ENTRIES", "64")))


def _hive_sql_cache_get(sql: str) -> Optional[Tuple[bool, str, str]]:
    ttl = _hive_sql_cache_ttl_sec()
    if ttl <= 0:
        return None
    key = _hive_sql_cache_key(sql)
    now = time.monotonic()
    with _hive_sql_cache_lock:
        ent = _hive_sql_cache.get(key)
        if ent is None:
            return None
        ts, val = ent
        # Caduca si pasó TTL desde la última lectura **o** escritura (según sliding).
        if now - ts > ttl:
            try:
                del _hive_sql_cache[key]
            except KeyError:
                pass
            return None
        _hive_sql_cache.move_to_end(key)
        # Sliding TTL: cada acierto renueva el reloj (si no, a los 120 s de la *primera*
        # ejecución la segunda vuelve a Hive aunque acabes de repetir la consulta).
        if os.environ.get("SIMLOG_HIVE_CACHE_SLIDING", "1").strip() != "0":
            _hive_sql_cache[key] = (now, val)
        return val


def _hive_sql_cache_set(sql: str, result: Tuple[bool, str, str]) -> None:
    ttl = _hive_sql_cache_ttl_sec()
    if ttl <= 0:
        return
    ok, _err, _out = result
    if not ok:
        return
    key = _hive_sql_cache_key(sql)
    now = time.monotonic()
    with _hive_sql_cache_lock:
        _hive_sql_cache[key] = (now, result)
        _hive_sql_cache.move_to_end(key)
        max_e = _hive_sql_cache_max_entries()
        while len(_hive_sql_cache) > max_e:
            _hive_sql_cache.popitem(last=False)


def limpiar_cache_consultas_hive() -> None:
    """Vacía la caché en memoria (útil tras cargas ETL o para forzar lectura fresca)."""
    with _hive_sql_cache_lock:
        _hive_sql_cache.clear()


def ejecutar_hive_sql_seguro(sql: str) -> Tuple[bool, str, str]:
    ok, err, out, _meta = ejecutar_hive_sql_seguro_detalle(sql)
    return ok, err, out


def ejecutar_hive_sql_seguro_detalle(sql: str) -> Tuple[bool, str, str, bool]:
    """
    Como `ejecutar_hive_sql_seguro`, pero el cuarto valor es True si la respuesta salió de caché en memoria.
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
        return False, "Solo se permiten SHOW, SELECT, WITH o DESCRIBE", "", False
    cached = _hive_sql_cache_get(sql)
    if cached is not None:
        return (*cached, True)
    timeout = int(os.environ.get("HIVE_QUERY_TIMEOUT_SEC", str(_DEFAULT_HIVE_QUERY_TIMEOUT_SEC)))
    result = _ejecutar_hive_pyhive_con_timeout(sql, timeout=timeout)
    _hive_sql_cache_set(sql, result)
    return (*result, False)


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
    timeout = int(os.environ.get("HIVE_QUERY_TIMEOUT_SEC", str(_DEFAULT_HIVE_QUERY_TIMEOUT_SEC)))
    return _ejecutar_hive_pyhive_con_timeout(sql, timeout=timeout)


def ejecutar_hive_consulta(codigo: str) -> Tuple[bool, str, str]:
    ok, err, out, _desde_cache = ejecutar_hive_consulta_detalle(codigo)
    return ok, err, out


def ejecutar_hive_consulta_detalle(codigo: str) -> Tuple[bool, str, str, bool]:
    """
    Ejecuta consulta Hive predefinida vía PyHive. Cuarto valor: True si la respuesta vino de caché en memoria.

    TTL con renovación en cada uso: `SIMLOG_HIVE_CACHE_SLIDING=1` (por defecto), ver `_hive_sql_cache_get`.
    """
    if codigo not in HIVE_CONSULTAS:
        return False, f"Consulta no permitida: {codigo}", "", False
    sql = HIVE_CONSULTAS[codigo]["sql"].strip()
    if not (
        sql.upper().startswith("SHOW")
        or sql.upper().startswith("SELECT")
        or sql.upper().startswith("WITH")
    ):
        return False, "Solo se permiten SHOW o SELECT", "", False

    cached = _hive_sql_cache_get(sql)
    if cached is not None:
        return (*cached, True)

    timeout = int(os.environ.get("HIVE_QUERY_TIMEOUT_SEC", str(_DEFAULT_HIVE_QUERY_TIMEOUT_SEC)))
    sql_u = sql.upper().lstrip()
    can_fallback = sql_u.startswith("SELECT") or sql_u.startswith("WITH")

    ok, err, out = _ejecutar_hive_pyhive_con_timeout(sql, timeout=timeout)
    timed_out = err.startswith("Timeout PyHive")

    if ok:
        _hive_sql_cache_set(sql, (True, "", out))
        return True, "", out, False

    if timed_out:
        if can_fallback and os.environ.get("SIMLOG_HIVE_EXEC_FALLBACK_SPARK", "0") == "1":
            ok_s, err_s, out_s = _ejecutar_hive_consulta_spark(sql, max_rows=2000)
            if ok_s:
                _hive_sql_cache_set(sql, (True, "", out_s))
                return True, "", out_s, False
            return (
                False,
                f"Timeout PyHive ({timeout}s) y fallback Spark falló. "
                f"Consulta='{codigo}' · SQL='{sql}'. "
                f"fallback_spark_error: {err_s[:400]}.",
                "",
                False,
            )
        return (
            False,
            f"Timeout PyHive ({timeout}s). Consulta='{codigo}' · SQL='{sql}'. "
            "Prueba `diag_smoke_hive` (SELECT 1). Si HS2 va lento, sube `HIVE_QUERY_TIMEOUT_SEC` "
            f"(por defecto {_DEFAULT_HIVE_QUERY_TIMEOUT_SEC}s) o activa `SIMLOG_HIVE_EXEC_FALLBACK_SPARK=1`.",
            "",
            False,
        )

    if err:
        return False, err, out or "", False

    return (
        False,
        f"Hive no devolvió resultado. Consulta='{codigo}' · SQL='{sql}'. "
        "Confirma HiveServer2 en el puerto 10000 (`HIVE_JDBC_URL` / `HIVE_SERVER`).",
        out or "",
        False,
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
        "consultas": [
            "diag_smoke_hive",
            "tablas_bd",
            "historico_nodos_muestra",
            "historico_nodos_conteo",
            "nodos_maestro_muestra",
            "nodos_maestro_conteo",
            "gemelo_red_nodos",
            "gemelo_red_aristas",
        ],
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
    orden = [
        "diagnostico",
        "eventos",
        "clima",
        "tracking",
        "transporte",
        "rutas",
        "agregaciones",
        "gestor",
    ]
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
