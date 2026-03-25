"""
Consultas supervisadas (whitelist) para Cassandra y Hive — cuadro de mando SIMLOG.

No se ejecuta SQL arbitrario del usuario: solo plantillas aprobadas para supervisión y control.
"""
from __future__ import annotations

import os
import re
import subprocess
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from config import CASSANDRA_HOST, HIVE_BEELINE_USER, HIVE_DB, HIVE_JDBC_URL, KEYSPACE

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

# Spark escribe `historico_nodos` y `nodos_maestro` en esta BD (ver `procesamiento_grafos.py`).
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


def _hive_beeline_cmd() -> List[str]:
    jdbc = os.environ.get("HIVE_JDBC_URL", HIVE_JDBC_URL)
    bin_name = os.environ.get("HIVE_BEELINE_BIN", "beeline")
    # -n evita sesión «anonymous» y el error de impersonación (hadoop vs anonymous) en HS2+doAs.
    # --fastConnect reduce trabajo de inicialización (tablas/columnas de autocompletado),
    # útil en UI donde queremos respuesta rápida.
    return [
        bin_name,
        "-u",
        jdbc,
        "-n",
        HIVE_BEELINE_USER,
        "--silent=true",
        "--fastConnect=true",
        "--showWarnings=false",
        "--outputformat=tsv2",
    ]


def ejecutar_hive_consulta(codigo: str) -> Tuple[bool, str, str]:
    """
    Ejecuta consulta Hive predefinida. Requiere `beeline` en PATH y HiveServer2.
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

    cmd = _hive_beeline_cmd() + ["-e", sql]
    timeout = int(os.environ.get("HIVE_QUERY_TIMEOUT_SEC", "120"))
    try:
        r = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
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
        cmd_s = " ".join(str(x) for x in cmd)
        # No incluimos credenciales (no se pasan -p) pero sí el JDBC/SQL para diagnóstico.
        # Fallback: ejecutar con Spark SQL si se pudo importar pyspark y la consulta es SELECT/WITH.
        sql_u = sql.upper().lstrip()
        can_fallback = sql_u.startswith("SELECT") or sql_u.startswith("WITH")
        if can_fallback and os.environ.get("SIMLOG_HIVE_EXEC_FALLBACK_SPARK", "1") == "1":
            ok_s, err_s, out_s = _ejecutar_hive_consulta_spark(sql, max_rows=2000)
            if ok_s:
                return True, "", out_s
            # Si falla el fallback, devolvemos diagnóstico beeline (original) + error fallback.
            return (
                False,
                f"Timeout beeline Hive ({timeout}s) y fallback Spark falló. "
                f"beeline: Consulta='{codigo}' · SQL='{sql}'. "
                f"fallback_spark_error: {err_s[:200]}",
                "",
            )

        return (
            False,
            f"Timeout ejecutando Hive ({timeout}s). "
            f"Diagnóstico: beeline no devolvió salida. Consulta='{codigo}' · SQL='{sql}'. "
            f"Ejecutando: {cmd_s}",
            "",
        )
    except Exception as e:
        return False, str(e), ""


@lru_cache(maxsize=1)
def _get_spark_hive_session():
    """Crea (o reutiliza) una SparkSession con soporte Hive."""
    from pyspark.sql import SparkSession

    # Nota: esto no depende de HiveServer2; usa el metastore/warehouse para ejecutar HiveQL via Spark.
    return (
        SparkSession.builder.master("local[*]")
        .appName("SIMLOG_HiveQueryUI")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )


def _ejecutar_hive_consulta_spark(sql: str, max_rows: int = 2000) -> Tuple[bool, str, str]:
    """
    Ejecuta HiveQL usando Spark SQL como fallback cuando beeline timeoutea.
    Devuelve (ok, error, salida_tsv).
    """
    try:
        spark = _get_spark_hive_session()
        # Aseguramos que no recolectamos demasiado si la consulta falla al incluir LIMIT.
        df = spark.sql(sql).limit(max_rows)
        cols = df.columns
        rows = df.collect()
        lines: List[str] = []
        for r in rows:
            # r[col] funciona con Row.
            lines.append("\t".join("" if (r[c] is None) else str(r[c]) for c in cols))
        return True, "", "\n".join(lines)
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
