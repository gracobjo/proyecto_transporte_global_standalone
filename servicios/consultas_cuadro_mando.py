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
    HIVE_TABLE_HISTORICO_NODOS,
    HIVE_TABLE_NODOS_MAESTRO,
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

# Nombres físicos: `SIMLOG_HIVE_TABLE_HISTORICO_NODOS` y `SIMLOG_HIVE_TABLE_NODOS_MAESTRO` (config.py).
HIVE_DB_NAME = HIVE_DB or "logistica_espana"
_T_HIST = HIVE_TABLE_HISTORICO_NODOS
_T_MAESTRO = HIVE_TABLE_NODOS_MAESTRO

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
        "sql": f"SELECT * FROM {HIVE_DB_NAME}.{_T_HIST} LIMIT 50",
    },
    "historico_nodos_conteo": {
        "titulo": "Conteo de registros en histórico de nodos",
        "sql": f"SELECT COUNT(*) AS total FROM {HIVE_DB_NAME}.{_T_HIST}",
    },
    "nodos_maestro": {
        "titulo": "Maestro de nodos (Hive)",
        "sql": f"SELECT * FROM {HIVE_DB_NAME}.{_T_MAESTRO} LIMIT 100",
    },
    "nodos_maestro_conteo": {
        "titulo": "Conteo de nodos maestro",
        "sql": f"SELECT COUNT(*) AS total FROM {HIVE_DB_NAME}.{_T_MAESTRO}",
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
            f"SELECT camiones.id_camion AS id, camiones.posicion_actual.lat AS lat, "
            f"camiones.posicion_actual.lon AS lon, camiones.progreso_pct AS progreso_pct "
            f"FROM {HIVE_DB_NAME}.transporte_ingesta_real LIMIT 500"
        ),
    },
    "gestor_historial_rutas_camion": {
        "titulo": "Gestor — histórico transporte (Hive; robusto con camiones raw)",
        # En varios entornos `camiones` llega como STRING y no como array<struct>.
        # Esta versión devuelve histórico util sin depender del parseo estructurado.
        "sql": os.environ.get("SIMLOG_HIVE_SQL_HISTORIAL_CAMION", "").strip()
        or f"""
SELECT
  t.`timestamp`,
  t.camiones
FROM {HIVE_DB_NAME}.{HIVE_TABLE_TRANSPORTE_HIST} t
WHERE t.camiones IS NOT NULL
LIMIT 200
        """.strip(),
    },
    "gestor_historico_incidencias_24h": {
        "titulo": "Gestor — histórico incidencias nodos (24h)",
        "sql": f"""
SELECT
  h.id_nodo,
  h.tipo,
  h.estado,
  h.motivo_retraso,
  h.clima_actual,
  h.fecha_proceso
FROM {HIVE_DB_NAME}.{_T_HIST} h
WHERE h.fecha_proceso >= (current_timestamp() - INTERVAL 24 HOURS)
LIMIT 300
        """.strip(),
    },
    "gestor_historico_evolucion_nodos_24h": {
        "titulo": "Gestor — evolución estados por nodo (24h)",
        "sql": f"""
SELECT
  h.id_nodo,
  h.estado,
  h.fecha_proceso
FROM {HIVE_DB_NAME}.{_T_HIST} h
WHERE h.fecha_proceso >= (current_timestamp() - INTERVAL 24 HOURS)
LIMIT 400
        """.strip(),
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
FROM {HIVE_DB_NAME}.{_T_HIST} h
WHERE h.fecha_proceso >= (current_timestamp() - INTERVAL 24 HOURS)
LIMIT 20
        """.strip(),
    },
    "severidad_resumen_24h": {
        "titulo": "Resumen severidad (últimas 24h) — estado derivado",
        # En entornos standalone (sin Tez/YARN) las agregaciones suelen disparar MapReduce y fallar.
        # Devolvemos una muestra "segura" para que la UI no reviente.
        "sql": f"""
SELECT
  h.estado,
  h.motivo_retraso,
  h.clima_actual,
  h.fecha_proceso
FROM {HIVE_DB_NAME}.{_T_HIST} h
WHERE h.fecha_proceso >= (current_timestamp() - INTERVAL 24 HOURS)
LIMIT 200
        """.strip(),
    },
    "diag_fecha_proceso_24h": {
        "titulo": f"Diagnóstico — rango y registros (últimas 24h) en {_T_HIST}",
        # Evitar MIN/MAX/COUNT (agregación) por el mismo motivo que arriba.
        "sql": f"""
SELECT
  h.id_nodo,
  h.fecha_proceso
FROM {HIVE_DB_NAME}.{_T_HIST} h
WHERE h.fecha_proceso >= (current_timestamp() - INTERVAL 24 HOURS)
LIMIT 200
        """.strip(),
    },
    "riesgo_hub_24h": {
        "titulo": "Riesgo por hub (últimas 24h) — incidencia derivada",
        # Versión "safe": evita CTEs + agregaciones (MapReduce) y devuelve filas por hub para que
        # el dashboard pueda mostrar/filtrar en cliente.
        "sql": f"""
SELECT
  h.id_nodo,
  h.tipo,
  h.estado,
  h.motivo_retraso,
  h.clima_actual,
  h.fecha_proceso
FROM {HIVE_DB_NAME}.{_T_HIST} h
WHERE h.fecha_proceso >= (current_timestamp() - INTERVAL 24 HOURS)
LIMIT 500
        """.strip(),
    },
    "top_causas_24h": {
        "titulo": "Top causas (últimas 24h) — incidencia derivada",
        # Versión "safe": sin agregaciones.
        "sql": f"""
SELECT
  h.estado,
  h.motivo_retraso,
  h.clima_actual,
  h.fecha_proceso
FROM {HIVE_DB_NAME}.{_T_HIST} h
WHERE h.fecha_proceso >= (current_timestamp() - INTERVAL 24 HOURS)
LIMIT 200
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
    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(_ejecutar_hive_pyhive, sql)
        try:
            return fut.result(timeout=timeout)
        except FuturesTimeout:
            return False, f"Timeout PyHive ({timeout}s)", ""


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
