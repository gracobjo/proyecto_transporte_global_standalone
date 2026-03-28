"""
Capa de Persistencia Relacional Histórica - Hive
SIMLOG España — persistencia Hive

Hive almacena el histórico completo cada 15 minutos.
Permite análisis de tendencias futuro (ej. "¿En qué meses hay más incendios?")
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import os

from config import (
    HDFS_NAMENODE,
    HIVE_BEELINE_USER,
    HIVE_CONF_DIR,
    HIVE_DB,
    HIVE_METASTORE_URIS,
    HIVE_TABLE_TRANSPORTE_HIST,
)

TABLA_EVENTOS = "eventos_historico"
TABLA_CAMIONES = "tracking_camiones_historico"
TABLA_CLIMA = "clima_historico"
TABLA_RUTAS = "rutas_alternativas_historico"
TABLA_AGG = "agg_estadisticas_diarias"
TABLA_TRANSPORTE = HIVE_TABLE_TRANSPORTE_HIST
TABLA_ALERTAS_HIST = "alertas_historicas"
TABLA_EVENTOS_GRAFO = "eventos_grafo"
HIVE_COMPAT_BASE = os.environ.get("SIMLOG_HIVE_COMPAT_BASE", "/user/hadoop/simlog_hive")
CSV_SERDE = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
HDFS_DEFAULT_FS = os.environ.get(
    "SIMLOG_HDFS_DEFAULT_FS",
    os.environ.get(
        "HDFS_DEFAULT_FS",
        "hdfs://nodo1:9000" if HDFS_NAMENODE.startswith(("127.", "localhost")) else f"hdfs://{HDFS_NAMENODE}",
    ),
)

TABLE_PATHS = {
    TABLA_EVENTOS: f"{HIVE_COMPAT_BASE}/{TABLA_EVENTOS}",
    TABLA_CLIMA: f"{HIVE_COMPAT_BASE}/{TABLA_CLIMA}",
    TABLA_CAMIONES: f"{HIVE_COMPAT_BASE}/{TABLA_CAMIONES}",
    TABLA_RUTAS: f"{HIVE_COMPAT_BASE}/{TABLA_RUTAS}",
    TABLA_AGG: f"{HIVE_COMPAT_BASE}/{TABLA_AGG}",
    TABLA_TRANSPORTE: f"{HIVE_COMPAT_BASE}/{TABLA_TRANSPORTE}",
    TABLA_ALERTAS_HIST: f"{HIVE_COMPAT_BASE}/{TABLA_ALERTAS_HIST}",
    TABLA_EVENTOS_GRAFO: f"{HIVE_COMPAT_BASE}/{TABLA_EVENTOS_GRAFO}",
}

TABLE_SCHEMAS: Dict[str, List[Tuple[str, str]]] = {
    TABLA_EVENTOS: [
        ("timestamp", "STRING"),
        ("anio", "INT"),
        ("mes", "INT"),
        ("dia", "INT"),
        ("hora", "INT"),
        ("minuto", "INT"),
        ("dia_semana", "STRING"),
        ("tipo_evento", "STRING"),
        ("id_elemento", "STRING"),
        ("tipo_elemento", "STRING"),
        ("estado", "STRING"),
        ("motivo", "STRING"),
        ("pagerank", "DOUBLE"),
        ("distancia_km", "DOUBLE"),
        ("hub_asociado", "STRING"),
        ("anio_part", "INT"),
        ("mes_part", "INT"),
    ],
    TABLA_CLIMA: [
        ("timestamp", "STRING"),
        ("anio", "INT"),
        ("mes", "INT"),
        ("dia", "INT"),
        ("ciudad", "STRING"),
        ("temperatura", "DOUBLE"),
        ("humedad", "INT"),
        ("descripcion", "STRING"),
        ("visibilidad", "INT"),
        ("estado_carretera", "STRING"),
        ("anio_part", "INT"),
        ("mes_part", "INT"),
    ],
    TABLA_CAMIONES: [
        ("timestamp", "STRING"),
        ("anio", "INT"),
        ("mes", "INT"),
        ("dia", "INT"),
        ("id_camion", "STRING"),
        ("origen", "STRING"),
        ("destino", "STRING"),
        ("nodo_actual", "STRING"),
        ("lat_actual", "DOUBLE"),
        ("lon_actual", "DOUBLE"),
        ("progreso_pct", "DOUBLE"),
        ("distancia_total_km", "DOUBLE"),
        ("tiene_ruta_alternativa", "BOOLEAN"),
        ("distancia_alternativa_km", "DOUBLE"),
        ("anio_part", "INT"),
        ("mes_part", "INT"),
    ],
    TABLA_TRANSPORTE: [
        ("timestamp", "STRING"),
        ("anio", "INT"),
        ("mes", "INT"),
        ("dia", "INT"),
        ("hora", "INT"),
        ("minuto", "INT"),
        ("id_camion", "STRING"),
        ("origen", "STRING"),
        ("destino", "STRING"),
        ("nodo_actual", "STRING"),
        ("lat", "DOUBLE"),
        ("lon", "DOUBLE"),
        ("progreso_pct", "DOUBLE"),
        ("distancia_total_km", "DOUBLE"),
        ("estado_ruta", "STRING"),
        ("motivo_retraso", "STRING"),
        ("ruta", "STRING"),
        ("ruta_sugerida", "STRING"),
        ("hub_actual", "STRING"),
        ("anio_part", "INT"),
        ("mes_part", "INT"),
    ],
    TABLA_RUTAS: [
        ("timestamp", "STRING"),
        ("anio", "INT"),
        ("mes", "INT"),
        ("dia", "INT"),
        ("origen", "STRING"),
        ("destino", "STRING"),
        ("ruta_original", "STRING"),
        ("ruta_alternativa", "STRING"),
        ("distancia_original_km", "DOUBLE"),
        ("distancia_alternativa_km", "DOUBLE"),
        ("motivo_bloqueo", "STRING"),
        ("ahorro_km", "DOUBLE"),
        ("anio_part", "INT"),
        ("mes_part", "INT"),
    ],
    TABLA_AGG: [
        ("anio", "INT"),
        ("mes", "INT"),
        ("dia", "INT"),
        ("tipo_evento", "STRING"),
        ("estado", "STRING"),
        ("motivo", "STRING"),
        ("contador", "INT"),
        ("pct_total", "DOUBLE"),
        ("anio_part", "INT"),
        ("mes_part", "INT"),
    ],
    TABLA_ALERTAS_HIST: [
        ("timestamp_inicio", "STRING"),
        ("timestamp_fin", "STRING"),
        ("alerta_id", "STRING"),
        ("tipo_alerta", "STRING"),
        ("entidad_id", "STRING"),
        ("severidad", "STRING"),
        ("causa", "STRING"),
        ("mensaje", "STRING"),
        ("estado_resolucion", "STRING"),
        ("duracion_segundos", "INT"),
        ("ruta_original", "STRING"),
        ("ruta_alternativa", "STRING"),
        ("anio_part", "INT"),
        ("mes_part", "INT"),
    ],
    TABLA_EVENTOS_GRAFO: [
        ("timestamp", "STRING"),
        ("tipo_evento", "STRING"),
        ("entidad_tipo", "STRING"),
        ("entidad_id", "STRING"),
        ("estado_anterior", "STRING"),
        ("estado_nuevo", "STRING"),
        ("causa", "STRING"),
        ("detalles", "STRING"),
        ("anio_part", "INT"),
        ("mes_part", "INT"),
    ],
}


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalizar_clima(datos: Dict) -> List[Dict]:
    clima = datos.get("clima")
    if isinstance(clima, list) and clima:
        return clima
    clima_hubs = datos.get("clima_hubs") or {}
    out = []
    for ciudad, item in clima_hubs.items():
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "ciudad": ciudad,
                "temperatura": item.get("temperatura", item.get("temp")),
                "humedad": item.get("humedad"),
                "descripcion": item.get("descripcion", ""),
                "visibilidad": item.get("visibilidad"),
                "estado_carretera": item.get("estado_carretera"),
                "timestamp": datos.get("timestamp"),
            }
        )
    return out


def _normalizar_camiones(datos: Dict) -> List[Dict]:
    out = []
    for camion in datos.get("camiones", []):
        ruta = camion.get("ruta") or camion.get("ruta_sugerida") or []
        pos = camion.get("posicion_actual") if isinstance(camion.get("posicion_actual"), dict) else {}
        lat = camion.get("lat", pos.get("lat"))
        lon = camion.get("lon", pos.get("lon"))
        origen = camion.get("ruta_origen") or (ruta[0] if ruta else "")
        destino = camion.get("ruta_destino") or (ruta[-1] if ruta else "")
        ruta_sugerida = camion.get("ruta_sugerida") or ruta
        out.append(
            {
                "id_camion": camion.get("id_camion") or camion.get("id") or "UNKNOWN",
                "origen": origen,
                "destino": destino,
                "nodo_actual": camion.get("nodo_actual") or camion.get("origen_tramo") or origen,
                "lat_actual": _safe_float(lat),
                "lon_actual": _safe_float(lon),
                "progreso_pct": _safe_float(camion.get("progreso_pct", camion.get("progreso", 0.0))),
                "distancia_total_km": _safe_float(camion.get("distancia_total_km")),
                "tiene_ruta_alternativa": bool(ruta_sugerida and ruta_sugerida != ruta),
                "distancia_alternativa_km": _safe_float(camion.get("distancia_alternativa_km")),
                "estado_ruta": camion.get("estado_ruta", "En ruta"),
                "motivo_retraso": camion.get("motivo_retraso", ""),
                "ruta": ruta,
                "ruta_sugerida": ruta_sugerida,
            }
        )
    return out


def _hdfs_uri(path: str) -> str:
    if path.startswith("hdfs://"):
        return path
    return f"{HDFS_DEFAULT_FS}{path}"


def _hive_connect(database: str | None = None):
    from pyhive import hive

    host, port = "127.0.0.1", 10000
    last_error = None
    for auth in ("NOSASL", "NONE"):
        try:
            kwargs = dict(
                host=host,
                port=port,
                username=HIVE_BEELINE_USER,
                auth=auth,
            )
            if database:
                kwargs["database"] = database
            return hive.Connection(**kwargs)
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"No se pudo conectar a HiveServer2: {last_error}")


def _ejecutar_hive_ddl(sql: str, *, database: str | None = None) -> None:
    conn = _hive_connect(database=database)
    cur = conn.cursor()
    try:
        cur.execute(sql)
    finally:
        cur.close()
        conn.close()


def _columnas_actuales(tabla: str) -> List[str]:
    conn = _hive_connect(database=HIVE_DB)
    cur = conn.cursor()
    try:
        cur.execute(f"DESCRIBE {HIVE_DB}.{tabla}")
        out = []
        for row in cur.fetchall():
            name = (row[0] or "").strip()
            if not name or name.startswith("#"):
                continue
            out.append(name)
        return out
    except Exception:
        return []
    finally:
        cur.close()
        conn.close()


def _asegurar_tabla_externa_csv(tabla: str, *, replace: bool = False) -> None:
    cols = TABLE_SCHEMAS[tabla]
    location = _hdfs_uri(TABLE_PATHS[tabla])
    expected_cols = [name for name, _ in cols]
    current_cols = _columnas_actuales(tabla)
    if replace or (current_cols and current_cols != expected_cols):
        _ejecutar_hive_ddl(f"DROP TABLE IF EXISTS {HIVE_DB}.{tabla}", database=HIVE_DB)

    cols_sql = ",\n            ".join(f"`{name}` {dtype}" for name, dtype in cols)
    ddl = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_DB}.{tabla} (
            {cols_sql}
        )
        ROW FORMAT SERDE '{CSV_SERDE}'
        WITH SERDEPROPERTIES (
            "separatorChar"=",",
            "quoteChar"="\\"",
            "escapeChar"="\\\\"
        )
        STORED AS TEXTFILE
        LOCATION '{location}'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """
    _ejecutar_hive_ddl(ddl, database=HIVE_DB)


def _append_external_csv(df, tabla: str) -> None:
    ordered_cols = [name for name, _ in TABLE_SCHEMAS[tabla]]
    (
        df.select(*ordered_cols)
        .coalesce(1)
        .write.mode("append")
        .option("header", "true")
        .csv(_hdfs_uri(TABLE_PATHS[tabla]))
    )


def crear_spark_hive(app_name: str = "HivePersistence") -> SparkSession:
    """Crea sesión Spark enfocada a escribir datasets compatibles con Hive 4."""
    os.environ.setdefault("HIVE_CONF_DIR", HIVE_CONF_DIR)
    os.environ.setdefault("SIMLOG_HIVE_CONF_DIR", HIVE_CONF_DIR)
    os.environ.setdefault("HIVE_METASTORE_URIS", HIVE_METASTORE_URIS)
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .getOrCreate()


def inicializar_esquema_hive(spark: SparkSession) -> None:
    """Inicializa la base y tablas externas compatibles con Hive 4."""
    _ejecutar_hive_ddl(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
    for tabla in TABLE_SCHEMAS:
        _asegurar_tabla_externa_csv(tabla, replace=(tabla == TABLA_TRANSPORTE))


def parsear_timestamp(ts: str) -> Dict:
    """Extrae componentes de timestamp para particiones."""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        dias = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
        return {
            "anio": dt.year,
            "mes": dt.month,
            "dia": dt.day,
            "hora": dt.hour,
            "minuto": dt.minute,
            "dia_semana": dias[dt.weekday()]
        }
    except:
        return {
            "anio": datetime.now().year,
            "mes": datetime.now().month,
            "dia": datetime.now().day,
            "hora": datetime.now().hour,
            "minuto": datetime.now().minute,
            "dia_semana": "Desconocido"
        }


def determinar_hub_asociado(nodo: str, nodos_info: Dict) -> str:
    """Determina el hub asociado a un nodo."""
    if nodo in ["Madrid", "Barcelona", "Bilbao", "Vigo", "Sevilla"]:
        return nodo
    return nodos_info.get(nodo, {}).get("hub", "Desconocido")


def persistir_eventos_historico(
    spark: SparkSession,
    datos: Dict,
    pagerank: Dict,
    nodos_info: Dict
) -> None:
    """Persiste eventos de nodos y aristas en Hive histórico."""
    
    ts = datos.get("timestamp", datetime.now().isoformat())
    componentes = parsear_timestamp(ts)
    anio = componentes["anio"]
    mes = componentes["mes"]
    
    eventos = []
    
    for nodo, estado_info in datos.get("estados_nodos", {}).items():
        eventos.append({
            "timestamp": ts,
            "anio": componentes["anio"],
            "mes": componentes["mes"],
            "dia": componentes["dia"],
            "hora": componentes["hora"],
            "minuto": componentes["minuto"],
            "dia_semana": componentes["dia_semana"],
            "tipo_evento": "nodo",
            "id_elemento": nodo,
            "tipo_elemento": nodos_info.get(nodo, {}).get("tipo", "secundario"),
            "estado": estado_info.get("estado", "ok"),
            "motivo": estado_info.get("motivo", ""),
            "pagerank": pagerank.get(nodo, {}).get("pagerank", 0.0),
            "distancia_km": 0.0,
            "hub_asociado": determinar_hub_asociado(nodo, nodos_info),
            "anio_part": anio,
            "mes_part": mes
        })
    
    for arista, estado_info in datos.get("estados_aristas", {}).items():
        src, dst = arista.split("|")
        eventos.append({
            "timestamp": ts,
            "anio": componentes["anio"],
            "mes": componentes["mes"],
            "dia": componentes["dia"],
            "hora": componentes["hora"],
            "minuto": componentes["minuto"],
            "dia_semana": componentes["dia_semana"],
            "tipo_evento": "arista",
            "id_elemento": arista,
            "tipo_elemento": "arista",
            "estado": estado_info.get("estado", "ok"),
            "motivo": estado_info.get("motivo", ""),
            "pagerank": 0.0,
            "distancia_km": estado_info.get("distancia_km", 0.0),
            "hub_asociado": determinar_hub_asociado(src, nodos_info),
            "anio_part": anio,
            "mes_part": mes
        })
    
    if eventos:
        df = spark.createDataFrame(eventos)
        _append_external_csv(df, TABLA_EVENTOS)
        print(f"Insertados {len(eventos)} eventos en {TABLA_EVENTOS}")


def persistir_clima_historico(spark: SparkSession, datos: Dict) -> None:
    """Persiste datos climáticos en Hive."""
    
    ts = datos.get("timestamp", datetime.now().isoformat())
    componentes = parsear_timestamp(ts)
    anio = componentes["anio"]
    mes = componentes["mes"]
    
    registros = []
    
    for clima in _normalizar_clima(datos):
        estado_carretera = clima.get("estado_carretera") or "Optimo"
        desc = clima.get("descripcion", "").lower()
        if "niebla" in desc or "niebla" in desc:
            estado_carretera = "Niebla"
        elif "lluvia" in desc or "rain" in desc:
            estado_carretera = "Lluvia"
        elif "nieve" in desc or "snow" in desc:
            estado_carretera = "Nieve"
        elif "tormenta" in desc or "storm" in desc:
            estado_carretera = "Tormenta"
        
        registros.append({
            "timestamp": ts,
            "anio": componentes["anio"],
            "mes": componentes["mes"],
            "dia": componentes["dia"],
            "ciudad": clima.get("ciudad", "Unknown"),
            "temperatura": _safe_float(clima.get("temperatura", clima.get("temp"))),
            "humedad": _safe_int(clima.get("humedad")),
            "descripcion": clima.get("descripcion", ""),
            "visibilidad": _safe_int(clima.get("visibilidad"), 10000),
            "estado_carretera": estado_carretera,
            "anio_part": anio,
            "mes_part": mes
        })
    
    if registros:
        schema = StructType([
            StructField("timestamp", StringType()),
            StructField("anio", IntegerType()),
            StructField("mes", IntegerType()),
            StructField("dia", IntegerType()),
            StructField("ciudad", StringType()),
            StructField("temperatura", DoubleType()),
            StructField("humedad", IntegerType()),
            StructField("descripcion", StringType()),
            StructField("visibilidad", IntegerType()),
            StructField("estado_carretera", StringType()),
            StructField("anio_part", IntegerType()),
            StructField("mes_part", IntegerType()),
        ])
        df = spark.createDataFrame(registros, schema=schema)
        _append_external_csv(df, TABLA_CLIMA)
        print(f"Insertados {len(registros)} registros climáticos en {TABLA_CLIMA}")


def persistir_camiones_historico(
    spark: SparkSession,
    datos: Dict,
    rutas_alternativas: Dict
) -> None:
    """Persiste tracking de camiones en Hive."""
    
    ts = datos.get("timestamp", datetime.now().isoformat())
    componentes = parsear_timestamp(ts)
    anio = componentes["anio"]
    mes = componentes["mes"]
    
    registros = []
    
    for camion in _normalizar_camiones(datos):
        cam_id = camion.get("id_camion", "UNKNOWN")
        ruta_alt = rutas_alternativas.get(cam_id, {})

        ruta = camion.get("ruta", [])
        registros.append({
            "timestamp": ts,
            "anio": componentes["anio"],
            "mes": componentes["mes"],
            "dia": componentes["dia"],
            "id_camion": cam_id,
            "origen": camion.get("origen") or (ruta[0] if ruta else ""),
            "destino": camion.get("destino") or (ruta[-1] if len(ruta) > 1 else ""),
            "nodo_actual": camion.get("nodo_actual", ""),
            "lat_actual": camion.get("lat_actual", 0.0),
            "lon_actual": camion.get("lon_actual", 0.0),
            "progreso_pct": camion.get("progreso_pct", 0.0),
            "distancia_total_km": camion.get("distancia_total_km", 0.0),
            "tiene_ruta_alternativa": ruta_alt.get("ruta") is not None,
            "distancia_alternativa_km": ruta_alt.get("distancia", 0.0),
            "anio_part": anio,
            "mes_part": mes
        })
    
    if registros:
        df = spark.createDataFrame(registros)
        _append_external_csv(df, TABLA_CAMIONES)
        print(f"Insertados {len(registros)} registros de camiones en {TABLA_CAMIONES}")


def persistir_transporte_historico(spark: SparkSession, datos: Dict, nodos_info: Dict) -> None:
    """Persiste una tabla plana para la UI y el gemelo digital."""

    ts = datos.get("timestamp", datetime.now().isoformat())
    componentes = parsear_timestamp(ts)
    anio = componentes["anio"]
    mes = componentes["mes"]

    registros = []
    for camion in _normalizar_camiones(datos):
        nodo_actual = camion.get("nodo_actual", "")
        hub_actual = nodos_info.get(nodo_actual, {}).get("hub")
        if not hub_actual and nodos_info.get(nodo_actual, {}).get("tipo") == "hub":
            hub_actual = nodo_actual
        registros.append({
            "timestamp": ts,
            "anio": componentes["anio"],
            "mes": componentes["mes"],
            "dia": componentes["dia"],
            "hora": componentes["hora"],
            "minuto": componentes["minuto"],
            "id_camion": camion.get("id_camion", "UNKNOWN"),
            "origen": camion.get("origen", ""),
            "destino": camion.get("destino", ""),
            "nodo_actual": nodo_actual,
            "lat": camion.get("lat_actual", 0.0),
            "lon": camion.get("lon_actual", 0.0),
            "progreso_pct": camion.get("progreso_pct", 0.0),
            "distancia_total_km": camion.get("distancia_total_km", 0.0),
            "estado_ruta": camion.get("estado_ruta", "En ruta"),
            "motivo_retraso": camion.get("motivo_retraso", ""),
            "ruta": "->".join(camion.get("ruta", [])),
            "ruta_sugerida": "->".join(camion.get("ruta_sugerida", [])),
            "hub_actual": hub_actual or "Desconocido",
            "anio_part": anio,
            "mes_part": mes,
        })

    if registros:
        df = spark.createDataFrame(registros)
        _append_external_csv(df, TABLA_TRANSPORTE)
        print(f"Insertados {len(registros)} registros en {TABLA_TRANSPORTE}")


def persistir_rutas_alternativas(
    spark: SparkSession,
    datos: Dict,
    rutas_alternativas: Dict
) -> None:
    """Persiste histórico de rutas alternativas calculadas."""
    
    ts = datos.get("timestamp", datetime.now().isoformat())
    componentes = parsear_timestamp(ts)
    anio = componentes["anio"]
    mes = componentes["mes"]
    
    registros = []
    
    for cam_id, ruta_info in rutas_alternativas.items():
        ruta_orig = None
        dist_orig = 0.0
        for cam in datos.get("camiones", []):
            if cam.get("id") == cam_id:
                ruta_orig = cam.get("ruta", [])
                dist_orig = cam.get("distancia_total_km", 0.0)
                break
        
        if ruta_orig and len(ruta_orig) >= 2:
            registros.append({
                "timestamp": ts,
                "anio": componentes["anio"],
                "mes": componentes["mes"],
                "dia": componentes["dia"],
                "origen": ruta_orig[0],
                "destino": ruta_orig[-1],
                "ruta_original": "->".join(ruta_orig),
                "ruta_alternativa": "->".join(ruta_info.get("ruta", [])) if ruta_info.get("ruta") else "",
                "distancia_original_km": dist_orig,
                "distancia_alternativa_km": ruta_info.get("distancia", 0.0),
                "motivo_bloqueo": ruta_info.get("motivo", ""),
                "ahorro_km": dist_orig - ruta_info.get("distancia", 0.0),
                "anio_part": anio,
                "mes_part": mes
            })
    
    if registros:
        df = spark.createDataFrame(registros)
        _append_external_csv(df, TABLA_RUTAS)
        print(f"Insertadas {len(registros)} rutas alternativas en {TABLA_RUTAS}")


def persistir_alertas_historicas(spark: SparkSession, datos: Dict) -> None:
    """Persiste alertas resueltas de reconfiguración en Hive."""
    alertas = datos.get("alertas_historicas_resueltas") or []
    if not alertas:
        return
    registros = []
    for alerta in alertas:
        componentes = parsear_timestamp(alerta.get("timestamp_fin") or alerta.get("timestamp_inicio") or datetime.now().isoformat())
        registros.append(
            {
                "timestamp_inicio": alerta.get("timestamp_inicio", ""),
                "timestamp_fin": alerta.get("timestamp_fin", ""),
                "alerta_id": alerta.get("alerta_id", ""),
                "tipo_alerta": alerta.get("tipo_alerta", ""),
                "entidad_id": alerta.get("entidad_id", ""),
                "severidad": alerta.get("severidad", ""),
                "causa": alerta.get("causa", ""),
                "mensaje": alerta.get("mensaje", ""),
                "estado_resolucion": alerta.get("estado_resolucion", "RESUELTA"),
                "duracion_segundos": int(alerta.get("duracion_segundos", 0) or 0),
                "ruta_original": alerta.get("ruta_original", ""),
                "ruta_alternativa": alerta.get("ruta_alternativa", ""),
                "anio_part": componentes["anio"],
                "mes_part": componentes["mes"],
            }
        )
    df = spark.createDataFrame(registros)
    _append_external_csv(df, TABLA_ALERTAS_HIST)
    print(f"Insertadas {len(registros)} alertas históricas en {TABLA_ALERTAS_HIST}")


def persistir_eventos_grafo_historico(spark: SparkSession, datos: Dict) -> None:
    """Persiste cambios del grafo de reconfiguración en Hive."""
    eventos = datos.get("eventos_grafo") or []
    if not eventos:
        return
    registros = []
    for evento in eventos:
        componentes = parsear_timestamp(evento.get("timestamp") or datetime.now().isoformat())
        detalles = evento.get("detalles", {})
        registros.append(
            {
                "timestamp": evento.get("timestamp", ""),
                "tipo_evento": evento.get("tipo_evento", ""),
                "entidad_tipo": evento.get("entidad_tipo", ""),
                "entidad_id": evento.get("entidad_id", ""),
                "estado_anterior": evento.get("estado_anterior", ""),
                "estado_nuevo": evento.get("estado_nuevo", ""),
                "causa": evento.get("causa", ""),
                "detalles": str(detalles),
                "anio_part": componentes["anio"],
                "mes_part": componentes["mes"],
            }
        )
    df = spark.createDataFrame(registros)
    _append_external_csv(df, TABLA_EVENTOS_GRAFO)
    print(f"Insertados {len(registros)} eventos de grafo en {TABLA_EVENTOS_GRAFO}")


def calcular_estadisticas_diarias(spark: SparkSession) -> None:
    """Calcula agregaciones diarias leyendo el histórico externo ya escrito en HDFS."""
    path = _hdfs_uri(TABLE_PATHS[TABLA_EVENTOS])
    try:
        df = spark.read.option("header", "true").csv(path, inferSchema=True)
    except Exception:
        return
    if df.rdd.isEmpty():
        return

    agg = (
        df.filter(F.col("anio_part").isNotNull())
        .groupBy("anio", "mes", "dia", "tipo_evento", "estado", "motivo", "anio_part", "mes_part")
        .agg(F.count("*").alias("contador"))
    )
    window_cols = ["anio", "mes", "dia"]
    total = agg.groupBy(*window_cols).agg(F.sum("contador").alias("total_dia"))
    agg = (
        agg.join(total, on=window_cols, how="left")
        .withColumn("pct_total", F.round(F.col("contador") * F.lit(100.0) / F.col("total_dia"), 2))
        .select(
            "anio",
            "mes",
            "dia",
            "tipo_evento",
            "estado",
            "motivo",
            "contador",
            "pct_total",
            "anio_part",
            "mes_part",
        )
    )
    _append_external_csv(agg, TABLA_AGG)


def ejecutar_persistencia_hive(
    spark: SparkSession,
    datos: Dict,
    pagerank: Dict,
    rutas_alternativas: Dict,
    nodos_info: Dict
) -> None:
    """Ejecuta toda la persistencia en Hive."""
    
    print("Inicializando esquema Hive...")
    inicializar_esquema_hive(spark)
    
    print("Persistiendo eventos históricos...")
    persistir_eventos_historico(spark, datos, pagerank, nodos_info)
    
    print("Persistiendo clima histórico...")
    persistir_clima_historico(spark, datos)
    
    print("Persistiendo tracking de camiones...")
    persistir_camiones_historico(spark, datos, rutas_alternativas)

    print("Persistiendo transporte analítico...")
    persistir_transporte_historico(spark, datos, nodos_info)
    
    print("Persistiendo rutas alternativas...")
    persistir_rutas_alternativas(spark, datos, rutas_alternativas)

    print("Persistiendo alertas históricas...")
    persistir_alertas_historicas(spark, datos)

    print("Persistiendo eventos de grafo...")
    persistir_eventos_grafo_historico(spark, datos)
    
    print("Calculando estadísticas diarias...")
    calcular_estadisticas_diarias(spark)
    
    print("Persistencia Hive completada ✓")


def consulta_ejemplo_tendencias(spark: SparkSession) -> None:
    """Ejemplo de consulta para análisis de tendencias."""
    
    print("\n" + "="*60)
    print("EJEMPLO: Análisis de Tendencias")
    print("="*60)
    
    print("\n1. Incidentes por mes:")
    spark.sql(f"""
        SELECT mes, estado, COUNT(*) as total
        FROM {TABLA_EVENTOS}
        WHERE tipo_evento = 'nodo'
        GROUP BY mes, estado
        ORDER BY mes, total DESC
    """).show()
    
    print("\n2. Motivos de bloqueo más frecuentes:")
    spark.sql(f"""
        SELECT motivo, COUNT(*) as veces
        FROM {TABLA_EVENTOS}
        WHERE estado = 'bloqueado'
        GROUP BY motivo
        ORDER BY veces DESC
        LIMIT 10
    """).show()
    
    print("\n3. Rutas con más bloqueos:")
    spark.sql(f"""
        SELECT origen, destino, COUNT(*) as bloqueos
        FROM {TABLA_RUTAS}
        WHERE motivo_bloqueo != 'OK'
        GROUP BY origen, destino
        ORDER BY bloqueos DESC
        LIMIT 10
    """).show()
    
    print("\n4. Impacto del clima en estados de las carreteras:")
    spark.sql(f"""
        SELECT c.estado_carretera, e.estado, COUNT(*) as incidentes
        FROM {TABLA_CLIMA} c
        JOIN {TABLA_EVENTOS} e ON c.timestamp = e.timestamp AND c.ciudad = e.hub_asociado
        GROUP BY c.estado_carretera, e.estado
    """).show()


if __name__ == "__main__":
    from config_nodos import get_nodos
    
    spark = crear_spark_hive("HivePersistence")
    
    nodos = get_nodos()
    nodos_info = {n: {"tipo": d["tipo"], "hub": d.get("hub", "")} for n, d in nodos.items()}
    
    datos_ejemplo = {
        "timestamp": datetime.now().isoformat(),
        "estados_nodos": {"Madrid": {"estado": "ok", "motivo": "Tráfico fluido"}},
        "estados_aristas": {},
        "clima": [],
        "camiones": []
    }
    
    pagerank_ejemplo = {"Madrid": {"pagerank": 1.0}}
    rutas_ejemplo = {}
    
    ejecutar_persistencia_hive(spark, datos_ejemplo, pagerank_ejemplo, rutas_ejemplo, nodos_info)
    
    spark.stop()
