"""
Consultas supervisadas (whitelist) para Cassandra y Hive — cuadro de mando SIMLOG.

No se ejecuta SQL arbitrario del usuario: solo plantillas aprobadas para supervisión y control.
"""
from __future__ import annotations

import os
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from config import (
    CASSANDRA_HOST,
    HIVE_BEELINE_USER,
    HIVE_DB,
    HIVE_JDBC_URL,
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
        "cql": "SELECT id_nodo, pagerank, ultima_actualizacion FROM pagerank_nodos LIMIT 100",
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
        "cql": "SELECT id_nodo, pagerank, ultima_actualizacion FROM pagerank_nodos LIMIT 500",
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

# Spark escribe `historico_nodos` y `nodos_maestro` en esta BD (ver `procesamiento_grafos.py`).
HIVE_DB_NAME = HIVE_DB or "logistica_espana"

HIVE_CONSULTAS: Dict[str, Dict[str, str]] = {
    "diag_smoke_hive": {
        "titulo": "Diagnóstico — conexión Hive (SELECT 1; sin tablas)",
        "sql": "SELECT 1 AS ok",
    },
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
    "gemelo_red_nodos": {
        "titulo": "Gemelo digital — nodos (red estática)",
        "sql": (
            f"SELECT id_nodo, tipo, lat, lon, id_capital_ref, nombre "
            f"FROM {HIVE_DB_NAME}.red_gemelo_nodos"
        ),
    },
    "gemelo_red_aristas": {
        "titulo": "Gemelo digital — aristas",
        "sql": f"SELECT src, dst, distancia_km FROM {HIVE_DB_NAME}.red_gemelo_aristas",
    },
    "transporte_ingesta_real_muestra": {
        "titulo": "transporte_ingesta_real — camiones (struct)",
        "sql": (
            f"SELECT camiones.id AS id, camiones.posicion_actual.lat AS lat, "
            f"camiones.posicion_actual.lon AS lon, camiones.progreso_pct AS progreso_pct "
            f"FROM {HIVE_DB_NAME}.transporte_ingesta_real LIMIT 500"
        ),
    },
    "gestor_historial_rutas_camion": {
        "titulo": "Gestor — historial de rutas por camión (Hive; tabla configurable)",
        "sql": (
            f"SELECT * FROM {HIVE_DB_NAME}.{HIVE_TABLE_TRANSPORTE_HIST} "
            "WHERE id_camion = 'camion_1' LIMIT 100"
        ),
    },
    "historico_nodos_muestra_24h": {
        "titulo": "Muestra histórico (últimas 24h) — nodos",
        "sql": f"""
SELECT
    h.id_nodo,
    h.estado,
    h.motivo_retraso,
    h.clima_actual,
    h.fecha_proceso
FROM {HIVE_DB_NAME}.historico_nodos h
WHERE h.fecha_proceso >= timestampadd(HOUR, -24, current_timestamp())
LIMIT 20
        """.strip(),
    },
    "severidad_resumen_24h": {
        "titulo": "Resumen severidad (últimas 24h) — estado derivado",
        "sql": f"""
WITH base AS (
    SELECT lower(regexp_replace(COALESCE(h.estado,''), 'ó', 'o')) AS estado_norm
    FROM {HIVE_DB_NAME}.historico_nodos h
    WHERE h.fecha_proceso >= timestampadd(HOUR, -24, current_timestamp())
),
clasif AS (
    SELECT
        CASE
            WHEN estado_norm LIKE '%bloque%' THEN 'Bloqueado'
            WHEN estado_norm LIKE '%congest%' THEN 'Congestionado'
            ELSE 'OK'
        END AS severidad
    FROM base
)
SELECT
    severidad,
    COUNT(*) AS muestras
FROM clasif
GROUP BY severidad
ORDER BY muestras DESC
        """.strip(),
    },
    "diag_fecha_proceso_24h": {
        "titulo": "Diagnóstico — rango y registros (últimas 24h) en historico_nodos",
        "sql": f"""
SELECT
  MIN(h.fecha_proceso) AS min_fecha_proceso,
  MAX(h.fecha_proceso) AS max_fecha_proceso,
  COUNT(*) AS registros_24h
FROM {HIVE_DB_NAME}.historico_nodos h
WHERE h.fecha_proceso >= timestampadd(HOUR, -24, current_timestamp())
        """.strip(),
    },
    "riesgo_hub_24h": {
        "titulo": "Riesgo por hub (últimas 24h) — incidencia derivada",
        "sql": f"""
WITH base AS (
  SELECT
    COALESCE(
      m.hub,
      CASE WHEN lower(COALESCE(h.tipo,'')) = 'hub' THEN h.id_nodo ELSE 'Desconocido' END
    ) AS hub,
    lower(regexp_replace(COALESCE(h.estado,''), 'ó', 'o')) AS estado_norm,
    lower(regexp_replace(COALESCE(h.motivo_retraso,''), 'ó', 'o')) AS motivo_norm,
    lower(regexp_replace(COALESCE(h.clima_actual,''), 'ó', 'o')) AS clima_norm,
    lower(regexp_replace(COALESCE(h.clima_actual,''), 'ó', 'o')) AS clima_desc_norm
  FROM {HIVE_DB_NAME}.historico_nodos h
  LEFT JOIN {HIVE_DB_NAME}.nodos_maestro m ON h.id_nodo = m.id_nodo
  WHERE h.fecha_proceso >= timestampadd(HOUR, -24, current_timestamp())
),
clasif AS (
  SELECT
    hub,
    CASE
      WHEN estado_norm LIKE '%bloque%' THEN 'Bloqueado'
      WHEN estado_norm LIKE '%congest%' THEN 'Congestionado'
      ELSE 'OK'
    END AS severidad,
    CASE
      WHEN estado_norm LIKE '%bloque%' THEN
        CASE
          WHEN motivo_norm LIKE '%accidente%' OR motivo_norm LIKE '%colision%' OR motivo_norm LIKE '%choque%' THEN 'Bloqueo - Accidente'
          WHEN motivo_norm LIKE '%obra%' OR motivo_norm LIKE '%obras%' THEN 'Bloqueo - Obras'
          WHEN clima_norm LIKE '%niebla%' OR clima_norm LIKE '%lluvia%' OR clima_norm LIKE '%nieve%' OR clima_norm LIKE '%tormenta%'
            OR clima_desc_norm LIKE '%niebla%' OR clima_desc_norm LIKE '%lluvia%' OR clima_desc_norm LIKE '%nieve%' OR clima_desc_norm LIKE '%tormenta%' THEN 'Bloqueo - Clima'
          ELSE 'Bloqueo - Otros'
        END
      WHEN estado_norm LIKE '%congest%' THEN
        CASE
          WHEN motivo_norm LIKE '%obra%' OR motivo_norm LIKE '%obras%' THEN 'Congestion - Obras'
          WHEN clima_norm LIKE '%niebla%' OR clima_norm LIKE '%lluvia%' OR clima_norm LIKE '%nieve%' OR clima_norm LIKE '%tormenta%'
            OR clima_desc_norm LIKE '%niebla%' OR clima_desc_norm LIKE '%lluvia%' OR clima_desc_norm LIKE '%nieve%' OR clima_desc_norm LIKE '%tormenta%' THEN 'Congestion - Clima'
          WHEN motivo_norm LIKE '%trafico%' OR motivo_norm LIKE '%tráfico%' THEN 'Congestion - Trafico'
          ELSE 'Congestion - Otros'
        END
      ELSE 'OK'
    END AS tipo_incidencia
  FROM base
),
agg AS (
  SELECT
    hub,
    severidad,
    tipo_incidencia,
    COUNT(*) AS muestras
  FROM clasif
  GROUP BY hub, severidad, tipo_incidencia
),
totales AS (
  SELECT hub, SUM(muestras) AS total_muestras
  FROM agg
  GROUP BY hub
)
SELECT
  a.hub,
  a.severidad,
  a.tipo_incidencia,
  a.muestras,
  ROUND((a.muestras * 100.0) / NULLIF(t.total_muestras, 0), 2) AS pct_muestras,
  CAST(
    ROUND(((a.muestras * 100.0) / NULLIF(t.total_muestras, 0)) / 100.0 * 1440, 0)
    AS INT
  ) AS duracion_aprox_min
FROM agg a
JOIN totales t ON a.hub = t.hub
WHERE a.severidad <> 'OK'
ORDER BY a.hub, a.muestras DESC
        """.strip(),
    },
    "top_causas_24h": {
        "titulo": "Top causas (últimas 24h) — incidencia derivada",
        "sql": f"""
WITH base AS (
  SELECT
    lower(regexp_replace(COALESCE(h.estado,''), 'ó', 'o')) AS estado_norm,
    lower(regexp_replace(COALESCE(h.motivo_retraso,''), 'ó', 'o')) AS motivo_norm,
    lower(regexp_replace(COALESCE(h.clima_actual,''), 'ó', 'o')) AS clima_norm,
    lower(regexp_replace(COALESCE(h.clima_actual,''), 'ó', 'o')) AS clima_desc_norm
  FROM {HIVE_DB_NAME}.historico_nodos h
  WHERE h.fecha_proceso >= timestampadd(HOUR, -24, current_timestamp())
),
clasif AS (
  SELECT
    CASE
      WHEN estado_norm LIKE '%bloque%' THEN 'Bloqueado'
      WHEN estado_norm LIKE '%congest%' THEN 'Congestionado'
      ELSE 'OK'
    END AS severidad,
    CASE
      WHEN estado_norm LIKE '%bloque%' THEN
        CASE
          WHEN motivo_norm LIKE '%accidente%' OR motivo_norm LIKE '%colision%' OR motivo_norm LIKE '%choque%' THEN 'Bloqueo - Accidente'
          WHEN motivo_norm LIKE '%obra%' OR motivo_norm LIKE '%obras%' THEN 'Bloqueo - Obras'
          WHEN clima_norm LIKE '%niebla%' OR clima_norm LIKE '%lluvia%' OR clima_norm LIKE '%nieve%' OR clima_norm LIKE '%tormenta%'
            OR clima_desc_norm LIKE '%niebla%' OR clima_desc_norm LIKE '%lluvia%' OR clima_desc_norm LIKE '%nieve%' OR clima_desc_norm LIKE '%tormenta%' THEN 'Bloqueo - Clima'
          ELSE 'Bloqueo - Otros'
        END
      WHEN estado_norm LIKE '%congest%' THEN
        CASE
          WHEN motivo_norm LIKE '%obra%' OR motivo_norm LIKE '%obras%' THEN 'Congestion - Obras'
          WHEN clima_norm LIKE '%niebla%' OR clima_norm LIKE '%lluvia%' OR clima_norm LIKE '%nieve%' OR clima_norm LIKE '%tormenta%'
            OR clima_desc_norm LIKE '%niebla%' OR clima_desc_norm LIKE '%lluvia%' OR clima_desc_norm LIKE '%nieve%' OR clima_desc_norm LIKE '%tormenta%' THEN 'Congestion - Clima'
          WHEN motivo_norm LIKE '%trafico%' OR motivo_norm LIKE '%tráfico%' THEN 'Congestion - Trafico'
          ELSE 'Congestion - Otros'
        END
      ELSE 'OK'
    END AS tipo_incidencia
  FROM base
),
agg AS (
  SELECT
    severidad,
    tipo_incidencia,
    COUNT(*) AS muestras
  FROM clasif
  WHERE tipo_incidencia <> 'OK'
  GROUP BY severidad, tipo_incidencia
),
totales AS (
  SELECT SUM(muestras) AS total_muestras FROM agg
)
SELECT
  a.severidad,
  a.tipo_incidencia,
  a.muestras,
  ROUND((a.muestras * 100.0) / NULLIF(t.total_muestras, 0), 2) AS pct_muestras,
  CAST(
    ROUND(((a.muestras * 100.0) / NULLIF(t.total_muestras, 0)) / 100.0 * 1440, 0)
    AS INT
  ) AS duracion_aprox_min_24h
FROM agg a
CROSS JOIN totales t
ORDER BY a.muestras DESC
LIMIT 10
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
    Ejecuta HiveQL de solo lectura (SHOW / SELECT / WITH) vía PyHive.
    Para integraciones que construyen el SQL en servidor (p. ej. asistente de flota).
    """
    sql = sql.strip()
    u = sql.upper().lstrip()
    if not (u.startswith("SHOW") or u.startswith("SELECT") or u.startswith("WITH")):
        return False, "Solo se permiten SHOW, SELECT o WITH", ""
    timeout = int(os.environ.get("HIVE_QUERY_TIMEOUT_SEC", "300"))
    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(_ejecutar_hive_pyhive, sql)
        try:
            return fut.result(timeout=timeout)
        except FuturesTimeout:
            return False, f"Timeout PyHive ({timeout}s)", ""


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

    timed_out = False
    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(_ejecutar_hive_pyhive, sql)
        try:
            ok, err, out = fut.result(timeout=timeout)
        except FuturesTimeout:
            timed_out = True
            ok, err, out = False, "", ""

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
    No comparte el proceso con HiveServer2: si Derby ya está abierto por HS2, fallará con XSDB6.
    """
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName("SIMLOG_HiveQueryUI")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
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


def listar_claves_cassandra() -> List[str]:
    return list(CASSANDRA_CONSULTAS.keys())


def listar_claves_hive() -> List[str]:
    return list(HIVE_CONSULTAS.keys())


def titulo_cassandra(codigo: str) -> str:
    return CASSANDRA_CONSULTAS.get(codigo, {}).get("titulo", codigo)


def titulo_hive(codigo: str) -> str:
    return HIVE_CONSULTAS.get(codigo, {}).get("titulo", codigo)
