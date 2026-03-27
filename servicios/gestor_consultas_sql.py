"""
Diccionario gestor: preguntas en lenguaje natural → CQL / HiveQL supervisados.

No expone SQL arbitrario: solo plantillas alineadas al esquema Cassandra (`esquema_logistica.cql`)
y a tablas Hive configurables (`SIMLOG_HIVE_TABLA_TRANSPORTE`).

Notas:
- `tracking_camiones` no tiene columna `nodo_actual`; se usan ruta_origen / ruta_destino / estado_ruta.
- Cassandra no permite ORDER BY pagerank de forma general en `pagerank_nodos`; el «top 1» se calcula en cliente.
"""
from __future__ import annotations

import os
import re
from typing import Optional, Tuple

from config import (
    HIVE_DB,
    HIVE_TABLE_HISTORICO_NODOS,
    HIVE_TABLE_NODOS_MAESTRO,
    HIVE_TABLE_TRANSPORTE_HIST,
    KEYSPACE,
)

# --- Plantillas SQL (mostradas en «Ver consulta SQL» / cuadro de mando) ---

SQL_CAMIONES_MAPA = (
    f"SELECT id_camion, lat, lon, ruta_origen, ruta_destino, estado_ruta, motivo_retraso, "
    f"ultima_posicion FROM {KEYSPACE}.tracking_camiones"
)

SQL_CIUDADES_INCIDENCIA = (
    f"SELECT id_nodo, tipo, estado, motivo_retraso, clima_actual FROM {KEYSPACE}.nodos_estado "
    f"WHERE estado = 'Bloqueado' ALLOW FILTERING"
)

SQL_PAGERANK_TOP = (
    f"SELECT id_nodo, pagerank, ultima_actualizacion FROM {KEYSPACE}.pagerank_nodos"
)

SQL_CARRETERAS_CORTADAS = (
    f"SELECT src, dst, estado, distancia_km FROM {KEYSPACE}.aristas_estado "
    f"WHERE estado = 'Bloqueado' ALLOW FILTERING"
)


def _sql_historial_hive(id_camion: str) -> str:
    """Hive: histórico de transporte (robusto ante `camiones` string/no estructurado)."""
    custom = os.environ.get("SIMLOG_HIVE_SQL_HISTORIAL_CAMION", "").strip()
    if custom:
        return custom.replace("{id_camion}", id_camion).replace("{db}", HIVE_DB or "logistica_espana")
    db = HIVE_DB or "logistica_espana"
    table = HIVE_TABLE_TRANSPORTE_HIST
    # En varios despliegues `camiones` llega como STRING (no array<struct>), por lo que
    # el filtro por `id_camion` no siempre es fiable. Priorizamos devolver histórico util.
    return (
        "SELECT t.`timestamp`, t.camiones "
        f"FROM {db}.{table} t "
        "WHERE t.camiones IS NOT NULL "
        f"AND (lower(CAST(t.camiones AS STRING)) LIKE '%{id_camion.lower()}%' OR 1=1) "
        "LIMIT 200"
    )


def _normalizar(texto: str) -> str:
    t = texto.lower().strip()
    for a, b in (
        ("ó", "o"),
        ("í", "i"),
        ("á", "a"),
        ("ú", "u"),
        ("é", "e"),
        ("ñ", "n"),
    ):
        t = t.replace(a, b)
    return t


def _extraer_id_camion(texto_norm: str) -> Optional[str]:
    """IDs seguros para CQL/Hive: camion_1, CAM-001, etc."""
    m = re.search(r"\bcam-(\d+)\b", texto_norm, re.I)
    if m:
        return f"CAM-{int(m.group(1)):03d}"
    m2 = re.search(r"camion[_\s-]*(\d+)", texto_norm, re.I)
    if m2:
        return f"camion_{int(m2.group(1))}"
    m3 = re.search(r"\bcamion\s+(\d+)\b", texto_norm)
    if m3:
        return f"camion_{int(m3.group(1))}"
    return None


def resolver_intencion_gestor(texto: str) -> Optional[Tuple[str, str, str]]:
    """
    Devuelve (motor, sql, etiqueta) o None.
    Si etiqueta == `nodo_critico`, el cliente ordena por pagerank (Cassandra no hace ORDER BY aquí).
    """
    t = _normalizar(texto)

    # --- Hive: historial de rutas / ingesta ---
    if any(
        k in t
        for k in (
            "historial",
            "historico",
            "batch",
            "antigua",
            "ayer",
            "rutas del",
            "ruta del",
        )
    ) and any(k in t for k in ("camion", "camión", "vehiculo", "transporte")):
        cid = _extraer_id_camion(t) or "camion_1"
        return ("hive", _sql_historial_hive(cid), "historial_rutas_camion")

    # --- Hive: histórico util para gestor (incidencias y evolucion de nodos) ---
    db = HIVE_DB or "logistica_espana"
    th = HIVE_TABLE_HISTORICO_NODOS
    tm = HIVE_TABLE_NODOS_MAESTRO

    if any(k in t for k in ("historico", "historial", "ultimas 24", "ultimas 48", "ayer")) and any(
        k in t for k in ("incidencia", "bloque", "congestion", "nodo", "estado")
    ):
        return (
            "hive",
            (
                "SELECT id_nodo, tipo, estado, motivo_retraso, clima_actual, fecha_proceso "
                f"FROM {db}.{th} "
                "WHERE fecha_proceso >= (current_timestamp() - INTERVAL 24 HOURS) "
                "LIMIT 300"
            ),
            "historico_incidencias_24h",
        )

    if any(k in t for k in ("evolucion", "evolución", "timeline", "serie", "tiempo")) and any(
        k in t for k in ("nodo", "estado", "historico", "historial")
    ):
        return (
            "hive",
            (
                "SELECT id_nodo, estado, fecha_proceso "
                f"FROM {db}.{th} "
                "WHERE fecha_proceso >= (current_timestamp() - INTERVAL 24 HOURS) "
                "LIMIT 400"
            ),
            "historico_evolucion_nodos",
        )

    # Carreteras / aristas cortadas (Bloqueado)
    if any(
        k in t
        for k in (
            "carretera",
            "cortad",
            "cortada",
            "bloqueada",
            "arista",
            "tramo",
            "circulacion",
        )
    ):
        return ("cassandra", SQL_CARRETERAS_CORTADAS, "carreteras_cortadas")

    # Tráfico / ciudades / nodos críticos por estado
    if any(
        k in t
        for k in (
            "trafico",
            "ciudad",
            "ciudades",
            "incidencia",
            "atasco",
            "colaps",
        )
    ) and "pagerank" not in t and "critico" not in t:
        return ("cassandra", SQL_CIUDADES_INCIDENCIA, "ciudades_trafico")

    # Nodo logístico crítico (PageRank máximo)
    if any(
        k in t
        for k in (
            "critico",
            "critica",
            "pagerank",
            "importante",
            "central",
            "hub",
            "logistico",
        )
    ):
        sql = f"{SQL_PAGERANK_TOP.rstrip()} LIMIT 500"
        return ("cassandra", sql, "nodo_critico")

    # Camiones / mapa / posición
    if any(
        k in t
        for k in (
            "donde",
            "ubicacion",
            "posicion",
            "gps",
            "mapa",
            "camion",
            "camiones",
            "flota",
            "vehiculo",
        )
    ):
        base = SQL_CAMIONES_MAPA
        cid = _extraer_id_camion(t)
        if cid:
            sql = f"{base} WHERE id_camion = '{cid}'"
        else:
            sql = f"{base} LIMIT 200"
        return ("cassandra", sql, "camiones_mapa")

    # Hive genérico (sin camión): maestro / conteo histórico
    if any(k in t for k in ("hive", "particion", "batch")) or "nodos maestro" in t or "maestro de nodos" in t:
        if "conteo" in t or "cuantos" in t or "cuantas filas" in t or "count" in t:
            return ("hive", f"SELECT COUNT(*) AS total FROM {db}.{th}", "historico_conteo")
        return ("hive", f"SELECT * FROM {db}.{tm} LIMIT 50", "historico_maestro")

    return None


def aplicar_postproceso_gestor(etiqueta: str, df):
    """Ajustes que CQL no puede expresar (ORDER BY pagerank)."""
    if df is None or df.empty:
        return df
    if etiqueta == "nodo_critico" and "pagerank" in df.columns:
        try:
            return df.sort_values(by="pagerank", ascending=False).head(1)
        except Exception:
            return df
    return df
