#!/usr/bin/env python3
"""
SIMLOG España — Fase II/III: Minería de grafos
- Construcción de grafo con GraphFrames
- Autosanación: eliminar rutas Bloqueadas, penalizar Niebla/Lluvia
- ShortestPath dinámico
- Persistencia: Hive (histórico) + Cassandra (nodos_estado, tracking_camiones)
- IMPORTANTE: spark.stop() al final (4GB RAM)
"""
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, current_timestamp
from graphframes import GraphFrame

from config_nodos import get_nodos, get_aristas
from config import (
    JAR_GRAPHFRAMES,
    JAR_CASSANDRA,
    JAR_KAFKA,
    CASSANDRA_HOST,
    KEYSPACE,
    KAFKA_BOOTSTRAP,
    TOPIC_TRANSPORTE,
    HIVE_DB,
    HIVE_CONF_DIR,
    HIVE_METASTORE_URIS,
)
from procesamiento.reconfiguracion_grafo import (
    STATUS_ACTIVE,
    STATUS_DOWN,
    aplicar_reconfiguracion_logistica,
)

# Pesos de penalización por clima/estado
PESO_CONGESTIONADO = 1.5  # Niebla, Tráfico, Lluvia
PESO_BLOQUEADO = 9999  # Se elimina del grafo
HIVE_COMPAT_SEED_BASE = os.environ.get("SIMLOG_HIVE_SEED_BASE", "/user/hadoop/simlog_seed")
HIVE_COMPAT_NODOS_MAESTRO_PATH = os.environ.get(
    "SIMLOG_HIVE_NODOS_MAESTRO_PATH",
    f"{HIVE_COMPAT_SEED_BASE}/nodos_maestro",
)
HIVE_COMPAT_HISTORICO_NODOS_PATH = os.environ.get(
    "SIMLOG_HIVE_HISTORICO_NODOS_PATH",
    f"{HIVE_COMPAT_SEED_BASE}/historico_nodos",
)


def cargar_estados_desde_kafka(spark):
    """Leer último batch de Kafka o generar datos de simulación si no hay Kafka."""
    try:
        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", TOPIC_TRANSPORTE)
            .option("startingOffsets", "latest")
            .load()
        )
        # Streaming: para batch usamos read en lugar de readStream
        pass
    except Exception:
        pass

    # Fallback: leer de HDFS backup o generar simulación
    from config import HDFS_BACKUP_PATH
    try:
        df = spark.read.json(f"{HDFS_BACKUP_PATH}/*.json")
        return df
    except Exception:
        pass

    return None


def crear_spark():
    """Spark con Cassandra; Hive queda en modo opt-in para evitar locks del metastore."""
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("HIVE_CONF_DIR", HIVE_CONF_DIR)
    os.environ.setdefault("SIMLOG_HIVE_CONF_DIR", HIVE_CONF_DIR)
    os.environ.setdefault("HIVE_METASTORE_URIS", HIVE_METASTORE_URIS)
    master = os.environ.get("SPARK_MASTER", "local")
    base_builder = (
        SparkSession.builder
        .appName("SIMLOG_TransporteEspaña")
        .master(master)
        .config("spark.driver.memory", "512m")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", os.environ.get("SIMLOG_SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.default.parallelism", os.environ.get("SIMLOG_SPARK_DEFAULT_PARALLELISM", "8"))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.timeoutMS", "30000")
        .config("spark.cassandra.connection.keepAliveMS", "30000")
        .config("spark.jars", f"{JAR_GRAPHFRAMES},{JAR_CASSANDRA}")
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
        .config("spark.eventLog.enabled", "false")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "false")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.warehouse.dir", "/user/hive/warehouse")
    )
    if (HIVE_METASTORE_URIS or "").strip():
        uri = HIVE_METASTORE_URIS.strip()
        base_builder = (
            base_builder
            .config("spark.hadoop.hive.metastore.uris", uri)
            .config("hive.metastore.uris", uri)
        )
    enable_hive = os.environ.get("SIMLOG_ENABLE_HIVE", "0") == "1"
    if not enable_hive:
        return base_builder.getOrCreate()
    try:
        return base_builder.enableHiveSupport().getOrCreate()
    except Exception as e:
        print(f"[SPARK] Hive no disponible, continúo sin HiveSupport: {e}")
        return base_builder.getOrCreate()


def construir_grafo_base(spark, nodos, aristas, estados_nodos=None):
    """Crear GraphFrame con vértices enriquecidos con severidad/fuente DGT."""
    v_data = []
    for nid, datos in nodos.items():
        est = (estados_nodos or {}).get(nid, {})
        v_data.append(
            (
                nid,
                datos["lat"],
                datos["lon"],
                datos["tipo"],
                est.get("source", "simulacion"),
                est.get("severity", "low"),
                float(est.get("peso_pagerank", 1.0) or 1.0),
                est.get("estado", "OK"),
            )
        )
    v = spark.createDataFrame(v_data, ["id", "lat", "lon", "tipo", "source", "severity", "peso_pagerank", "estado"])

    e_data = [(a, b, float(d)) for a, b, d in aristas]
    e = spark.createDataFrame(e_data, ["src", "dst", "distancia_km"])
    return GraphFrame(v, e)


def aplicar_autosanacion(g, estados_aristas, estados_nodos):
    """
    1. Eliminar aristas Bloqueadas del grafo
    2. Aumentar peso (distancia) para aristas Congestionadas (Niebla/Lluvia)
    """
    aristas_list = get_aristas()
    # Crear nuevo grafo filtrado
    aristas_filtradas = []
    for src, dst, dist in aristas_list:
        key = f"{src}|{dst}"
        key_inv = f"{dst}|{src}"
        estado_info = estados_aristas.get(key) or estados_aristas.get(key_inv)
        if estado_info:
            if estado_info.get("estado") == "Bloqueado":
                continue  # Eliminar arista bloqueada
            motivo = estado_info.get("motivo") or ""
            peso = float(dist)
            if estado_info.get("estado") == "Congestionado" or motivo in ("Niebla", "Lluvia"):
                peso *= PESO_CONGESTIONADO
            aristas_filtradas.append((src, dst, peso))
        else:
            aristas_filtradas.append((src, dst, float(dist)))

    if not aristas_filtradas:
        return g  # Sin cambios

    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    v = g.vertices
    e = spark.createDataFrame(aristas_filtradas, ["src", "dst", "peso_penalizado"])
    return GraphFrame(v, e)


def shortest_paths(g, origen, destino):
    """Calcular shortest path entre origen y destino."""
    try:
        paths = g.shortestPaths(landmarks=[destino])
        row = paths.filter(col("id") == origen).first()
        if row and row.get("distances") and destino in row["distances"]:
            return row["distances"][destino]
    except Exception:
        pass
    return None


def enriquecer_desde_hive(spark, estados_nodos, estados_aristas, camiones):
    """
    Enriquecimiento desde datos maestros en Hive (PDF Fase II).
    Lee tabla nodos_maestro (id_nodo, lat, lon, tipo, hub) y añade hub a estados_nodos.
    En Hive 4 usamos el path externo publicado por HS2 para evitar incompatibilidades
    del cliente de metastore de Spark con el Thrift de Hive.
    """
    nodos = get_nodos()

    rows = []
    for nid, datos in nodos.items():
        hub = datos.get("hub", nid if datos.get("tipo") == "hub" else "")
        rows.append((nid, float(datos["lat"]), float(datos["lon"]), datos["tipo"], hub))

    try:
        try:
            df_master = spark.read.option("header", "true").csv(HIVE_COMPAT_NODOS_MAESTRO_PATH)
            if df_master.rdd.isEmpty():
                raise ValueError("nodos_maestro vacío")
        except Exception:
            df_master = spark.createDataFrame(rows, ["id_nodo", "lat", "lon", "tipo", "hub"])
            (
                df_master.coalesce(1)
                .write.mode("overwrite")
                .option("header", "true")
                .csv(HIVE_COMPAT_NODOS_MAESTRO_PATH)
            )
        df_maestro = df_master.select("id_nodo", "hub")
        maestro = {r.id_nodo: r.hub for r in df_maestro.collect()}
        for nid in list(estados_nodos.keys()):
            if isinstance(estados_nodos.get(nid), dict) and nid in maestro:
                estados_nodos[nid]["hub"] = maestro[nid]
    except Exception as e:
        print(f"[ENRIQUECIMIENTO HIVE] No disponible: {e}")
    return estados_nodos, estados_aristas, camiones


def limpiar_datos_antes_cassandra(estados_nodos, estados_aristas, camiones):
    """
    Gestión de calidad de datos antes de escribir en Cassandra:
    - Rellenar nulos en campos clave (lat/lon, estado, motivo).
    - Eliminar duplicados por id_camion (quedarse con el primero).
    - Normalizar valores de estado (OK, Congestionado, Bloqueado).
    Devuelve (estados_nodos, estados_aristas, camiones) ya limpios.
    """
    # Normalizar estado a valores canónicos
    def norm_estado(v):
        if v is None or (isinstance(v, str) and not v.strip()):
            return "OK"
        v = str(v).strip().lower().replace("ó", "o")
        if v in ("ok", "fluido"):
            return "OK"
        if v in ("congestionado", "congestion"):
            return "Congestionado"
        if v in ("bloqueado", "blocked"):
            return "Bloqueado"
        return "OK"

    # Limpiar nodos: nulos y normalizar estado
    nodos_limpios = {}
    for nid, d in (estados_nodos or {}).items():
        if not nid:
            continue
        estado = norm_estado(d.get("estado"))
        nodos_limpios[nid] = {
            "estado": estado,
            "motivo": d.get("motivo") or "",
            "clima_desc": d.get("clima_desc"),
            "temp": d.get("temp"),
            "humedad": d.get("humedad"),
            "viento": d.get("viento"),
            "source": d.get("source") or "simulacion",
            "severity": d.get("severity") or ("high" if estado == "Bloqueado" else ("medium" if estado == "Congestionado" else "low")),
            "peso_pagerank": float(d.get("peso_pagerank", 1.0) or 1.0),
            "id_incidencia": d.get("id_incidencia"),
            "carretera": d.get("carretera"),
            "municipio": d.get("municipio"),
            "provincia": d.get("provincia"),
            "descripcion": d.get("descripcion"),
        }

    # Limpiar aristas: nulos y normalizar estado
    aristas_limpias = {}
    for k, d in (estados_aristas or {}).items():
        if not k or "|" not in k:
            continue
        aristas_limpias[k] = {
            "estado": norm_estado(d.get("estado")),
            "motivo": d.get("motivo") or "",
            "distancia_km": float(d.get("distancia_km", 0)) if d.get("distancia_km") is not None else 0.0,
        }

    # Limpiar camiones: nulos en lat/lon, duplicados por id_camion
    seen_ids = set()
    camiones_limpios = []
    for c in (camiones or []):
        cid = c.get("id_camion") or c.get("id")
        if not cid or cid in seen_ids:
            continue
        seen_ids.add(cid)
        pos = c.get("posicion_actual") if isinstance(c.get("posicion_actual"), dict) else {}
        lat = c.get("lat", pos.get("lat"))
        lon = c.get("lon", pos.get("lon"))
        if lat is None:
            lat = 40.4
        if lon is None:
            lon = -3.7
        try:
            lat, lon = float(lat), float(lon)
        except (TypeError, ValueError):
            lat, lon = 40.4, -3.7
        if not (35 <= lat <= 44 and -10 <= lon <= 5):
            continue  # Fuera de España, filtrar
        ruta = c.get("ruta") or c.get("ruta_sugerida") or []
        ruta = ruta if isinstance(ruta, list) else []
        ruta_sugerida = c.get("ruta_sugerida") or ruta
        ruta_sugerida = ruta_sugerida if isinstance(ruta_sugerida, list) else ruta
        try:
            progreso_pct = float(c.get("progreso_pct", c.get("progreso", 0.0)) or 0.0)
        except (TypeError, ValueError):
            progreso_pct = 0.0
        try:
            distancia_total_km = float(c.get("distancia_total_km", 0.0) or 0.0)
        except (TypeError, ValueError):
            distancia_total_km = 0.0
        camiones_limpios.append({
            "id_camion": cid,
            "id": cid,
            "lat": lat,
            "lon": lon,
            "posicion_actual": {"lat": lat, "lon": lon},
            "ruta": ruta,
            "ruta_origen": c.get("ruta_origen") or (ruta[0] if ruta else ""),
            "ruta_destino": c.get("ruta_destino") or (ruta[-1] if ruta else ""),
            "ruta_sugerida": ruta_sugerida,
            "estado_ruta": c.get("estado_ruta") or "En ruta",
            "motivo_retraso": c.get("motivo_retraso") or "",
            "progreso_pct": progreso_pct,
            "distancia_total_km": distancia_total_km,
            "nodo_actual": c.get("nodo_actual") or c.get("origen_tramo") or (ruta[0] if ruta else ""),
            "origen_tramo": c.get("origen_tramo") or (ruta[0] if ruta else ""),
            "destino_tramo": c.get("destino_tramo") or (ruta[1] if len(ruta) > 1 else (ruta[0] if ruta else "")),
            "distancia_alternativa_km": c.get("distancia_alternativa_km"),
        })
    return nodos_limpios, aristas_limpias, camiones_limpios


def _distancia_ruta_km(ruta):
    if not ruta or len(ruta) < 2:
        return 0.0
    nodos = get_nodos()
    distancias = {}
    for src, dst, dist in get_aristas():
        distancias[(src, dst)] = float(dist)
        distancias[(dst, src)] = float(dist)
    total = 0.0
    for src, dst in zip(ruta, ruta[1:]):
        tramo = distancias.get((src, dst))
        if tramo is None:
            d1 = nodos.get(src, {"lat": 40.4, "lon": -3.7})
            d2 = nodos.get(dst, {"lat": 40.4, "lon": -3.7})
            tramo = (((d1["lat"] - d2["lat"]) ** 2 + (d1["lon"] - d2["lon"]) ** 2) ** 0.5) * 111.0
        total += float(tramo)
    return round(total, 2)


def _persistir_historico_nodos_hive_compatible(spark, df_nodos):
    """
    Mantiene `historico_nodos` vía la ruta externa que Hive ya expone en HDFS.
    Evita `saveAsTable()` contra Hive 4 desde Spark.
    """
    df_hist = (
        df_nodos.withColumn(
            "motivo_retraso",
            coalesce(col("motivo_retraso"), lit("")),
        )
        .withColumn("clima_actual", col("clima_desc"))
        .withColumn("temperatura", col("temp"))
        .withColumn("viento_velocidad", col("viento"))
        .select(
            "id_nodo",
            "lat",
            "lon",
            "tipo",
            "estado",
            "motivo_retraso",
            "clima_actual",
            "temperatura",
            "humedad",
            "viento_velocidad",
            "ultima_actualizacion",
            "fecha_proceso",
        )
    )
    (
        df_hist.coalesce(1)
        .write.mode("append")
        .option("header", "true")
        .csv(HIVE_COMPAT_HISTORICO_NODOS_PATH)
    )


def _ruta_alternativa_minima(ruta, nodos):
    if not ruta or len(ruta) < 2:
        return ruta or []

    def hub_de(nodo):
        info = nodos.get(nodo, {})
        if info.get("tipo") == "hub":
            return nodo
        return info.get("hub") or "Madrid"

    origen = ruta[0]
    destino = ruta[-1]
    hub_origen = hub_de(origen)
    hub_destino = hub_de(destino)
    propuesta = []
    for nodo in (origen, hub_origen, "Madrid", hub_destino, destino):
        if nodo and (not propuesta or propuesta[-1] != nodo):
            propuesta.append(nodo)
    return propuesta


def _construir_rutas_alternativas(camiones, estados_aristas):
    nodos = get_nodos()
    rutas = {}
    bloqueadas = set()
    for key, info in (estados_aristas or {}).items():
        if (info or {}).get("estado") == "Bloqueado" and "|" in key:
            src, dst = key.split("|", 1)
            bloqueadas.add((src, dst))
            bloqueadas.add((dst, src))

    for camion in camiones:
        ruta = camion.get("ruta") or []
        ruta_sugerida = camion.get("ruta_sugerida") or ruta
        necesita_alternativa = ruta_sugerida != ruta
        if not necesita_alternativa and len(ruta) >= 2:
            necesita_alternativa = any((src, dst) in bloqueadas for src, dst in zip(ruta, ruta[1:]))
            if necesita_alternativa:
                ruta_sugerida = _ruta_alternativa_minima(ruta, nodos)
        if not necesita_alternativa:
            continue
        rutas[camion["id_camion"]] = {
            "ruta": ruta_sugerida,
            "distancia": _distancia_ruta_km(ruta_sugerida),
            "motivo": camion.get("motivo_retraso") or "Ruta alternativa calculada",
        }
    return rutas


def _cargar_estado_reconfiguracion_actual():
    estado_nodos = {}
    estado_rutas = {}
    alertas = {}
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(KEYSPACE)
        try:
            for row in session.execute("SELECT id_nodo, status, cause, updated_at, last_event FROM estado_nodos"):
                estado_nodos[row.id_nodo] = {
                    "status": row.status or STATUS_ACTIVE,
                    "cause": row.cause or "",
                    "updated_at": row.updated_at,
                    "last_event": row.last_event or "",
                }
        except Exception:
            estado_nodos = {}
        try:
            for row in session.execute(
                "SELECT src, dst, status, cause, updated_at, last_event, manual_down FROM estado_rutas"
            ):
                estado_rutas[f"{row.src}|{row.dst}"] = {
                    "status": row.status or STATUS_ACTIVE,
                    "cause": row.cause or "",
                    "updated_at": row.updated_at,
                    "last_event": row.last_event or "",
                    "manual_down": bool(getattr(row, "manual_down", False)),
                }
        except Exception:
            estado_rutas = {}
        try:
            for row in session.execute(
                """
                SELECT alerta_id, tipo_alerta, entidad_id, severidad, mensaje, causa,
                       timestamp_inicio, timestamp_ultima_actualizacion, estado,
                       ruta_original, ruta_alternativa
                FROM alertas_activas
                """
            ):
                alertas[row.alerta_id] = {
                    "alerta_id": row.alerta_id,
                    "tipo_alerta": row.tipo_alerta,
                    "entidad_id": row.entidad_id,
                    "severidad": row.severidad,
                    "mensaje": row.mensaje,
                    "causa": row.causa,
                    "timestamp_inicio": row.timestamp_inicio,
                    "timestamp_ultima_actualizacion": row.timestamp_ultima_actualizacion,
                    "estado": row.estado,
                    "ruta_original": row.ruta_original,
                    "ruta_alternativa": row.ruta_alternativa,
                }
        except Exception:
            alertas = {}
        cluster.shutdown()
    except Exception:
        pass
    return estado_nodos, estado_rutas, alertas


def _aplicar_reconfiguracion_a_estados(estados_nodos, estados_aristas, reconfig):
    nodos_out = {nid: dict(info or {}) for nid, info in (estados_nodos or {}).items()}
    aristas_out = {eid: dict(info or {}) for eid, info in (estados_aristas or {}).items()}

    for nid, overlay in (reconfig.get("estado_nodos") or {}).items():
        base = nodos_out.setdefault(nid, {})
        base["graph_status"] = overlay.get("status", STATUS_ACTIVE)
        base["graph_cause"] = overlay.get("cause", "")
        if overlay.get("status") == STATUS_DOWN:
            base["estado"] = "Bloqueado"
            base["motivo"] = overlay.get("cause") or base.get("motivo") or "Nodo caído"
            base["source"] = base.get("source") or "reconfiguracion"
            base["severity"] = "highest"
            base["peso_pagerank"] = max(float(base.get("peso_pagerank", 1.0) or 1.0), 3.0)

    for edge_id, overlay in (reconfig.get("estado_rutas") or {}).items():
        src, dst = edge_id.split("|", 1)
        key = edge_id if edge_id in aristas_out else (f"{dst}|{src}" if f"{dst}|{src}" in aristas_out else edge_id)
        base = aristas_out.setdefault(
            key,
            {
                "estado": "OK",
                "motivo": "",
                "distancia_km": overlay.get("distancia_km", 0.0),
            },
        )
        base["graph_status"] = overlay.get("status", STATUS_ACTIVE)
        base["graph_cause"] = overlay.get("cause", "")
        if overlay.get("status") == STATUS_DOWN:
            base["estado"] = "Bloqueado"
            base["motivo"] = overlay.get("cause") or base.get("motivo") or "Ruta desactivada"
    return nodos_out, aristas_out


def _aplicar_rutas_reconfiguradas(camiones, rutas_reconfiguradas):
    for camion in camiones:
        cid = camion.get("id_camion") or camion.get("id")
        if not cid or cid not in (rutas_reconfiguradas or {}):
            continue
        ruta_info = rutas_reconfiguradas[cid]
        ruta_alt = ruta_info.get("ruta") or []
        if not ruta_alt:
            continue
        camion["ruta_sugerida"] = ruta_alt
        camion["estado_ruta"] = "Reconfigurada"
        camion["motivo_retraso"] = ruta_info.get("motivo") or "Reconfiguración automática por evento"
        camion["distancia_alternativa_km"] = _distancia_ruta_km(ruta_alt)
    return camiones


def _persistir_reconfiguracion_cassandra(spark, reconfig):
    from pyspark.sql.types import BooleanType, StructField, StructType, StringType, TimestampType

    now = datetime.now(timezone.utc)
    nodos_rows = []
    for nid, info in (reconfig.get("estado_nodos") or {}).items():
        nodos_rows.append(
            (
                nid,
                info.get("status", STATUS_ACTIVE),
                info.get("cause", ""),
                _parse_ts_local(info.get("updated_at")) if info.get("updated_at") else now,
                info.get("last_event", ""),
            )
        )
    if nodos_rows:
        schema = StructType(
            [
                StructField("id_nodo", StringType()),
                StructField("status", StringType()),
                StructField("cause", StringType()),
                StructField("updated_at", TimestampType()),
                StructField("last_event", StringType()),
            ]
        )
        spark.createDataFrame(nodos_rows, schema).write.format("org.apache.spark.sql.cassandra").options(
            table="estado_nodos", keyspace=KEYSPACE
        ).mode("append").save()

    rutas_rows = []
    for route_id, info in (reconfig.get("estado_rutas") or {}).items():
        src, dst = route_id.split("|", 1)
        rutas_rows.append(
            (
                src,
                dst,
                info.get("status", STATUS_ACTIVE),
                info.get("cause", ""),
                _parse_ts_local(info.get("updated_at")) if info.get("updated_at") else now,
                info.get("last_event", ""),
                bool(info.get("manual_down", False)),
            )
        )
    if rutas_rows:
        schema = StructType(
            [
                StructField("src", StringType()),
                StructField("dst", StringType()),
                StructField("status", StringType()),
                StructField("cause", StringType()),
                StructField("updated_at", TimestampType()),
                StructField("last_event", StringType()),
                StructField("manual_down", BooleanType()),
            ]
        )
        spark.createDataFrame(rutas_rows, schema).write.format("org.apache.spark.sql.cassandra").options(
            table="estado_rutas", keyspace=KEYSPACE
        ).mode("append").save()

    alertas_rows = []
    for alerta in (reconfig.get("alertas_activas") or {}).values():
        alertas_rows.append(
            (
                alerta.get("alerta_id"),
                alerta.get("tipo_alerta"),
                alerta.get("entidad_id"),
                alerta.get("severidad"),
                alerta.get("mensaje"),
                alerta.get("causa"),
                _parse_ts_local(alerta.get("timestamp_inicio")) if alerta.get("timestamp_inicio") else now,
                _parse_ts_local(alerta.get("timestamp_ultima_actualizacion")) if alerta.get("timestamp_ultima_actualizacion") else now,
                alerta.get("estado", "ACTIVA"),
                alerta.get("ruta_original", ""),
                alerta.get("ruta_alternativa", ""),
            )
        )
    if alertas_rows:
        schema = StructType(
            [
                StructField("alerta_id", StringType()),
                StructField("tipo_alerta", StringType()),
                StructField("entidad_id", StringType()),
                StructField("severidad", StringType()),
                StructField("mensaje", StringType()),
                StructField("causa", StringType()),
                StructField("timestamp_inicio", TimestampType()),
                StructField("timestamp_ultima_actualizacion", TimestampType()),
                StructField("estado", StringType()),
                StructField("ruta_original", StringType()),
                StructField("ruta_alternativa", StringType()),
            ]
        )
        spark.createDataFrame(alertas_rows, schema).write.format("org.apache.spark.sql.cassandra").options(
            table="alertas_activas", keyspace=KEYSPACE
        ).mode("append").save()

    resueltas = reconfig.get("alertas_historicas") or []
    if not resueltas:
        return
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(KEYSPACE)
        stmt = session.prepare("DELETE FROM alertas_activas WHERE alerta_id = ?")
        for alerta in resueltas:
            if alerta.get("alerta_id"):
                session.execute(stmt, (alerta["alerta_id"],))
        cluster.shutdown()
    except Exception as e:
        print(f"[CASSANDRA] No se pudieron eliminar alertas resueltas: {e}")


def _parse_ts_local(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return datetime.now(timezone.utc)
    return datetime.now(timezone.utc)


def _persistir_eventos_historico_cassandra(estados_nodos, estados_aristas):
    try:
        import uuid
        from cassandra.cluster import Cluster

        nodos = get_nodos()
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(KEYSPACE)
        stmt = session.prepare(
            """
            INSERT INTO eventos_historico (
                id_evento, tipo_entidad, id_entidad, estado_anterior, estado_nuevo,
                motivo, lat, lon, timestamp_evento
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        now = datetime.now(timezone.utc)
        for nid, info in (estados_nodos or {}).items():
            meta = nodos.get(nid, {})
            session.execute(
                stmt,
                (
                    uuid.uuid4(),
                    "nodo",
                    nid,
                    None,
                    info.get("estado", "OK"),
                    info.get("motivo", ""),
                    float(meta.get("lat", 0.0)),
                    float(meta.get("lon", 0.0)),
                    now,
                ),
            )
        for edge_id, info in (estados_aristas or {}).items():
            if "|" not in edge_id:
                continue
            src, _ = edge_id.split("|", 1)
            meta = nodos.get(src, {})
            session.execute(
                stmt,
                (
                    uuid.uuid4(),
                    "arista",
                    edge_id,
                    None,
                    info.get("estado", "OK"),
                    info.get("motivo", ""),
                    float(meta.get("lat", 0.0)),
                    float(meta.get("lon", 0.0)),
                    now,
                ),
            )
        cluster.shutdown()
    except Exception as e:
        print(f"[CASSANDRA] eventos_historico no disponible: {e}")


def _evaluar_alerta_bloqueos(estados_nodos):
    bloqueados = [nid for nid, info in (estados_nodos or {}).items() if (info or {}).get("estado") == "Bloqueado"]
    total = len(estados_nodos or {})
    ratio = (len(bloqueados) / total) if total else 0.0
    if ratio >= 0.20 or len(bloqueados) >= 5:
        return {"nivel": "critica", "bloqueados": len(bloqueados), "ratio": round(ratio, 3), "nodos": bloqueados}
    if len(bloqueados) >= 3:
        return {"nivel": "alta", "bloqueados": len(bloqueados), "ratio": round(ratio, 3), "nodos": bloqueados}
    return {"nivel": "normal", "bloqueados": len(bloqueados), "ratio": round(ratio, 3), "nodos": bloqueados}


def procesar_y_persistir(spark, estados_nodos, estados_aristas, camiones, *, eventos_grafo=None, timestamp=None):
    """Ejecutar minería de grafos y persistir en Hive + Cassandra."""
    nodos = get_nodos()
    aristas = get_aristas()

    estado_nodos_prev, estado_rutas_prev, alertas_previas = _cargar_estado_reconfiguracion_actual()
    reconfig = aplicar_reconfiguracion_logistica(
        spark,
        nodos,
        aristas,
        camiones,
        eventos=eventos_grafo,
        estado_nodos_previo=estado_nodos_prev,
        estado_rutas_previo=estado_rutas_prev,
        alertas_previas=alertas_previas,
        timestamp=timestamp,
    )
    estados_nodos, estados_aristas = _aplicar_reconfiguracion_a_estados(estados_nodos, estados_aristas, reconfig)
    camiones = _aplicar_rutas_reconfiguradas(camiones, reconfig.get("rutas_alternativas"))

    g0 = construir_grafo_base(spark, nodos, aristas, estados_nodos=estados_nodos)
    g = aplicar_autosanacion(g0, estados_aristas, estados_nodos)

    # PageRank para identificar nodos críticos
    pr = g.pageRank(resetProbability=0.15, maxIter=10)
    pagerank_df = (
        pr.vertices.select("id", "source", "severity", "peso_pagerank", "estado", col("pagerank").alias("base_pagerank"))
        .withColumn("pagerank_val", col("base_pagerank") * col("peso_pagerank"))
    )

    # Preparar nodos_estado para Cassandra
    from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
    rows_nodos = []
    for nid, datos in nodos.items():
        est = estados_nodos.get(nid, {})
        rows_nodos.append((
            nid,
            float(datos["lat"]),
            float(datos["lon"]),
            datos["tipo"],
            est.get("estado", "OK"),
            est.get("motivo"),
            est.get("clima_desc", "N/A"),
            float(est.get("temp") or 0),
            float(est.get("humedad") or 0),
            float(est.get("viento") or 0),
            est.get("source", "simulacion"),
            est.get("severity", "low"),
            est.get("id_incidencia"),
            est.get("carretera"),
            est.get("municipio"),
            est.get("provincia"),
            est.get("descripcion"),
            datetime.now(timezone.utc),
        ))

    schema_nodos = StructType([
        StructField("id_nodo", StringType()),
        StructField("lat", FloatType()),
        StructField("lon", FloatType()),
        StructField("tipo", StringType()),
        StructField("estado", StringType()),
        StructField("motivo_retraso", StringType()),
        StructField("clima_actual", StringType()),
        StructField("temperatura", FloatType()),
        StructField("humedad", FloatType()),
        StructField("viento_velocidad", FloatType()),
        StructField("source", StringType()),
        StructField("severity", StringType()),
        StructField("id_incidencia", StringType()),
        StructField("carretera", StringType()),
        StructField("municipio", StringType()),
        StructField("provincia", StringType()),
        StructField("descripcion_incidencia", StringType()),
        StructField("ultima_actualizacion", TimestampType()),
    ])
    df_nodos = spark.createDataFrame(rows_nodos, schema_nodos)

    # Escribir nodos_estado en Cassandra (overwrite por id)
    df_nodos.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="nodos_estado", keyspace=KEYSPACE) \
        .mode("append") \
        .save()

    # aristas_estado (estado y peso penalizado)
    aristas_list = get_aristas()
    rows_aristas = []
    for src, dst, dist in aristas_list:
        key = f"{src}|{dst}"
        key_inv = f"{dst}|{src}"
        est = estados_aristas.get(key) or estados_aristas.get(key_inv) or {}
        estado = est.get("estado", "OK")
        motivo = est.get("motivo", "")
        peso = float(dist)
        if estado == "Congestionado" or motivo in ("Niebla", "Lluvia"):
            peso *= PESO_CONGESTIONADO
        if estado != "Bloqueado":  # Solo las que siguen en el grafo
            rows_aristas.append((src, dst, float(dist), estado, peso))
    if rows_aristas:
        df_aristas = spark.createDataFrame(
            rows_aristas,
            ["src", "dst", "distancia_km", "estado", "peso_penalizado"]
        )
        df_aristas.write.format("org.apache.spark.sql.cassandra") \
            .options(table="aristas_estado", keyspace=KEYSPACE) \
            .mode("append").save()

    # tracking_camiones
    rows_camiones = []
    for c in camiones:
        ruta_sug = c.get("ruta_sugerida") or c.get("ruta", [])
        rows_camiones.append((
            c.get("id_camion", "camion_1"),
            float(c.get("lat", 40.4)),
            float(c.get("lon", -3.7)),
            c.get("ruta_origen", ""),
            c.get("ruta_destino", ""),
            ruta_sug,
            c.get("estado_ruta", "En ruta"),
            c.get("motivo_retraso"),
            datetime.now(timezone.utc),
        ))

    from pyspark.sql.types import ArrayType
    schema_camiones = StructType([
        StructField("id_camion", StringType()),
        StructField("lat", FloatType()),
        StructField("lon", FloatType()),
        StructField("ruta_origen", StringType()),
        StructField("ruta_destino", StringType()),
        StructField("ruta_sugerida", ArrayType(StringType())),
        StructField("estado_ruta", StringType()),
        StructField("motivo_retraso", StringType()),
        StructField("ultima_posicion", TimestampType()),
    ])
    rows_cam2 = []
    for c in camiones:
        ruta = c.get("ruta") or c.get("ruta_sugerida") or []
        ruta_sug = ruta if isinstance(ruta, list) else []
        rows_cam2.append((
            c.get("id_camion", "camion_1"),
            float(c.get("lat", 40.4)),
            float(c.get("lon", -3.7)),
            ruta_sug[0] if ruta_sug else "",
            ruta_sug[-1] if ruta_sug else "",
            ruta_sug,
            c.get("estado_ruta", "En ruta"),
            c.get("motivo_retraso"),
            datetime.now(timezone.utc),
        ))
    df_camiones = spark.createDataFrame(rows_cam2, schema_camiones)

    df_camiones.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="tracking_camiones", keyspace=KEYSPACE) \
        .mode("append") \
        .save()

    # PageRank a Cassandra
    pr_rows = pagerank_df.collect()
    pr_data = [
        (
            r["id"],
            float(r["pagerank_val"]),
            float(r["peso_pagerank"] or 1.0),
            r["source"] or "simulacion",
            r["estado"] or "OK",
            datetime.now(timezone.utc),
        )
        for r in pr_rows
    ]
    df_pr = spark.createDataFrame(
        pr_data,
        ["id_nodo", "pagerank", "peso_pagerank", "source", "estado", "ultima_actualizacion"],
    )
    df_pr.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="pagerank_nodos", keyspace=KEYSPACE) \
        .mode("append") \
        .save()

    _persistir_eventos_historico_cassandra(estados_nodos, estados_aristas)
    _persistir_reconfiguracion_cassandra(spark, reconfig)

    pagerank_dict = {r["id"]: {"pagerank": float(r["pagerank_val"])} for r in pr_rows}
    rutas_alternativas = _construir_rutas_alternativas(camiones, estados_aristas)
    clima_hubs = {}
    clima = []
    for nid, info in (estados_nodos or {}).items():
        meta = nodos.get(nid, {})
        hub = meta.get("hub") or (nid if meta.get("tipo") == "hub" else None)
        if not hub or hub in clima_hubs:
            continue
        clima_hubs[hub] = {
            "descripcion": info.get("clima_desc", "N/A"),
            "temp": info.get("temp"),
            "humedad": info.get("humedad"),
            "viento": info.get("viento"),
        }
        clima.append(
            {
                "ciudad": hub,
                "temperatura": info.get("temp"),
                "humedad": info.get("humedad"),
                "descripcion": info.get("clima_desc", "N/A"),
                "visibilidad": None,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
    datos_hive = {
        "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        "estados_nodos": estados_nodos,
        "estados_aristas": estados_aristas,
        "camiones": camiones,
        "clima_hubs": clima_hubs,
        "clima": clima,
        "alerta_bloqueos": _evaluar_alerta_bloqueos(estados_nodos),
        "eventos_grafo": reconfig.get("eventos_grafo", []),
        "alertas_historicas_resueltas": reconfig.get("alertas_historicas", []),
    }

    # Hive: histórico de eventos (si Hive está disponible)
    try:
        from persistencia_hive import ejecutar_persistencia_hive

        ejecutar_persistencia_hive(spark, datos_hive, pagerank_dict, rutas_alternativas, nodos)
    except Exception as e:
        print(f"[HIVE] Persistencia principal no disponible: {e}")
    else:
        try:
            _persistir_historico_nodos_hive_compatible(
                spark,
                df_nodos.withColumn("fecha_proceso", current_timestamp()),
            )
            print(
                "[HIVE] Modo compatibilidad activo: Spark mantiene histórico Hive "
                "mediante rutas externas compatibles con Hive 4."
            )
        except Exception as e:
            print(f"[HIVE] Histórico de nodos no disponible: {e}")

    alerta = _evaluar_alerta_bloqueos(estados_nodos)
    if alerta["nivel"] != "normal":
        print(f"[ALERTA] Bloqueos de red detectados: {alerta}")

    return g, pagerank_df


def _cassandra_disponible():
    """Comprobar si Cassandra acepta conexiones en 9042."""
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((CASSANDRA_HOST, 9042))
        s.close()
        return True
    except Exception:
        return False


def main():
    """Orquestar procesamiento: simular datos si no hay Kafka."""
    import subprocess
    from config import HDFS_BACKUP_PATH
    if not _cassandra_disponible():
        print(f"[ERROR] Cassandra no está disponible en {CASSANDRA_HOST}:9042. Arranca con: ~/proyecto_transporte_global/cassandra/bin/cassandra")
        sys.exit(1)
    # Crear carpeta HDFS antes de Spark (evita timeout con Spark ya levantado)
    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_BACKUP_PATH], capture_output=True, timeout=30)
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
        pass

    spark = crear_spark()
    try:
        import json
        # Intentar leer desde HDFS solo si hay ficheros (evita FileNotFoundException en análisis)
        payload = None
        try:
            r = subprocess.run(
                ["hdfs", "dfs", "-ls", HDFS_BACKUP_PATH],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if r.returncode == 0 and ".json" in (r.stdout or ""):
                df = spark.read.json(f"{HDFS_BACKUP_PATH}/*.json")
                rows = df.collect()
                if rows:
                    last = rows[-1]
                    payload = last.asDict() if hasattr(last, "asDict") else dict(last)
        except Exception:
            payload = None

        if payload is None:
            # Generar datos de simulación
            from config_nodos import get_nodos, get_aristas
            import random
            nodos = get_nodos()
            aristas = get_aristas()
            estados_nodos = {
                n: {
                    "estado": random.choice(["OK", "Congestionado", "Bloqueado"]),
                    "motivo": None,
                    "source": "simulacion",
                    "severity": "low",
                    "peso_pagerank": 1.0,
                }
                for n in nodos
            }
            for n in list(estados_nodos.keys())[:5]:
                if estados_nodos[n]["estado"] == "Congestionado":
                    estados_nodos[n]["motivo"] = random.choice(["Niebla", "Tráfico", "Lluvia"])
                    estados_nodos[n]["severity"] = "medium"
                elif estados_nodos[n]["estado"] == "Bloqueado":
                    estados_nodos[n]["motivo"] = random.choice(["Incendio", "Nieve", "Accidente"])
                    estados_nodos[n]["severity"] = "high"
            estados_aristas = {}
            for a, b, d in aristas:
                key = f"{a}|{b}"
                est = random.choice(["OK", "Congestionado", "Bloqueado"])
                motivo = random.choice(["Niebla", "Tráfico", "Lluvia"]) if est == "Congestionado" else (random.choice(["Incendio", "Nieve"]) if est == "Bloqueado" else None)
                estados_aristas[key] = {
                    "estado": est,
                    "motivo": motivo,
                    "distancia_km": d,
                    "source": "simulacion",
                    "severity": "high" if est == "Bloqueado" else ("medium" if est == "Congestionado" else "low"),
                }
            clima_hubs = {
                hub: {
                    "descripcion": random.choice(["despejado", "lluvia ligera", "niebla"]),
                    "temp": round(random.uniform(6, 28), 1),
                    "humedad": random.randint(35, 90),
                    "viento": round(random.uniform(1, 12), 1),
                }
                for hub, info in nodos.items()
                if info.get("tipo") == "hub"
            }
            camiones = [
                {
                    "id_camion": f"camion_{i}",
                    "id": f"camion_{i}",
                    "lat": 40.4 + i * 0.1,
                    "lon": -3.7,
                    "posicion_actual": {"lat": 40.4 + i * 0.1, "lon": -3.7},
                    "ruta": ["Madrid", "Barcelona"],
                    "ruta_sugerida": ["Madrid", "Bilbao", "Barcelona"],
                    "ruta_origen": "Madrid",
                    "ruta_destino": "Barcelona",
                    "nodo_actual": "Madrid",
                    "progreso_pct": float(i * 15),
                    "distancia_total_km": _distancia_ruta_km(["Madrid", "Barcelona"]),
                    "estado_ruta": "En ruta",
                    "motivo_retraso": "Ruta alternativa calculada" if i % 2 == 0 else "",
                }
                for i in range(1, 6)
            ]
            for hub, c in clima_hubs.items():
                if hub in estados_nodos:
                    estados_nodos[hub]["clima_desc"] = c.get("descripcion", "N/A")
                    estados_nodos[hub]["temp"] = c.get("temp")
                    estados_nodos[hub]["humedad"] = c.get("humedad")
                    estados_nodos[hub]["viento"] = c.get("viento")
            eventos_grafo = []
            ts_payload = datetime.now(timezone.utc).isoformat()
        else:
            # El JSON de ingesta usa "estados_nodos" y "estados_aristas"
            estados_nodos = payload.get("estados_nodos", payload.get("nodos_estado", {}))
            estados_aristas = payload.get("estados_aristas", payload.get("aristas_estado", {}))
            camiones = payload.get("camiones", [])
            eventos_grafo = payload.get("eventos_grafo", [])
            ts_payload = payload.get("timestamp") or datetime.now(timezone.utc).isoformat()
            # Normalizar formato camiones (ingesta usa "id", "posicion_actual", "ruta")
            camiones = [
                {
                    "id_camion": c.get("id") or c.get("id_camion"),
                    "lat": c.get("posicion_actual", {}).get("lat") if isinstance(c.get("posicion_actual"), dict) else c.get("lat"),
                    "lon": c.get("posicion_actual", {}).get("lon") if isinstance(c.get("posicion_actual"), dict) else c.get("lon"),
                    "posicion_actual": c.get("posicion_actual"),
                    "ruta": c.get("ruta", []),
                    "ruta_sugerida": c.get("ruta_sugerida", c.get("ruta", [])),
                    "ruta_origen": c.get("ruta_origen") or (c.get("ruta") or [None])[0],
                    "ruta_destino": c.get("ruta_destino") or (c.get("ruta") or [None])[-1],
                    "nodo_actual": c.get("nodo_actual", c.get("origen_tramo")),
                    "estado_ruta": c.get("estado_ruta", "En ruta"),
                    "motivo_retraso": c.get("motivo_retraso"),
                    "progreso_pct": c.get("progreso_pct", c.get("progreso", 0.0)),
                    "distancia_total_km": c.get("distancia_total_km", _distancia_ruta_km(c.get("ruta", []))),
                }
                for c in camiones
            ]
            # Enriquecer clima en nodos hub
            clima_list = payload.get("clima", [])
            clima = {c.get("ciudad"): c for c in clima_list if c.get("ciudad")} if isinstance(clima_list, list) else payload.get("clima_hubs", {})
            for hub, c in (clima or {}).items():
                if hub in estados_nodos:
                    estados_nodos[hub]["clima_desc"] = c.get("descripcion", "N/A")
                    estados_nodos[hub]["temp"] = c.get("temperatura", c.get("temp"))
                    estados_nodos[hub]["humedad"] = c.get("humedad")
                    estados_nodos[hub]["viento"] = c.get("viento")

        # Enriquecimiento desde Hive (datos maestros: nodos_maestro)
        estados_nodos, estados_aristas, camiones = enriquecer_desde_hive(
            spark, estados_nodos, estados_aristas, camiones
        )
        # Calidad de datos antes de Cassandra: nulos, duplicados, normalización
        estados_nodos, estados_aristas, camiones = limpiar_datos_antes_cassandra(
            estados_nodos, estados_aristas, camiones
        )
        procesar_y_persistir(
            spark,
            estados_nodos,
            estados_aristas,
            camiones,
            eventos_grafo=eventos_grafo,
            timestamp=ts_payload,
        )
        print("[PROCESAMIENTO] OK - Cassandra y Hive actualizados")
    finally:
        spark.stop()
        print("[PROCESAMIENTO] Spark detenido (spark.stop())")


if __name__ == "__main__":
    main()
