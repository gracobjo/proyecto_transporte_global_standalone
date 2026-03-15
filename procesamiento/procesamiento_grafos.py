#!/usr/bin/env python3
"""
Sistema de Gemelo Digital Logístico - Fase II/III: Minería de Grafos
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
from pyspark.sql.functions import col, lit, current_timestamp
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
)

# Pesos de penalización por clima/estado
PESO_CONGESTIONADO = 1.5  # Niebla, Tráfico, Lluvia
PESO_BLOQUEADO = 9999  # Se elimina del grafo


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
    """Spark con Cassandra, JARs configurados. master=local por defecto; usar SPARK_MASTER=yarn para YARN."""
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    master = os.environ.get("SPARK_MASTER", "local")
    return (
        SparkSession.builder
        .appName("MineriaTransporteEspaña")
        .master(master)
        .config("spark.driver.memory", "512m")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.timeoutMS", "30000")
        .config("spark.cassandra.connection.keepAliveMS", "30000")
        .config("spark.jars", f"{JAR_GRAPHFRAMES},{JAR_CASSANDRA}")
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
        .config("spark.eventLog.enabled", "false")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "false")
        .enableHiveSupport()
        .getOrCreate()
    )


def construir_grafo_base(spark, nodos, aristas):
    """Crear GraphFrame con vértices y aristas."""
    v_data = [(nid, datos["lat"], datos["lon"], datos["tipo"]) for nid, datos in nodos.items()]
    v = spark.createDataFrame(v_data, ["id", "lat", "lon", "tipo"])

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
    Si la tabla no existe o está vacía, asegura crearla y poblarla desde config_nodos.
    """
    nodos = get_nodos()
    db = HIVE_DB if HIVE_DB else "logistica_espana"
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        spark.sql(f"USE {db}")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS nodos_maestro (
                id_nodo STRING, lat DOUBLE, lon DOUBLE, tipo STRING, hub STRING
            ) STORED AS PARQUET
        """)
        # Poblar desde config_nodos si la tabla está vacía
        df_master = spark.sql("SELECT * FROM nodos_maestro LIMIT 1")
        if df_master.count() == 0:
            rows = []
            for nid, datos in nodos.items():
                hub = datos.get("hub", nid if datos.get("tipo") == "hub" else "")
                rows.append((nid, float(datos["lat"]), float(datos["lon"]), datos["tipo"], hub))
            from pyspark.sql import Row
            spark.createDataFrame(rows, ["id_nodo", "lat", "lon", "tipo", "hub"]).write.mode("overwrite").saveAsTable(f"{db}.nodos_maestro")
        # Leer maestros y enriquecer
        df_maestro = spark.sql(f"SELECT id_nodo, hub FROM {db}.nodos_maestro")
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
        nodos_limpios[nid] = {
            "estado": norm_estado(d.get("estado")),
            "motivo": d.get("motivo") or "",
            "clima_desc": d.get("clima_desc"),
            "temp": d.get("temp"),
            "humedad": d.get("humedad"),
            "viento": d.get("viento"),
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
        lat = c.get("lat")
        lon = c.get("lon")
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
        camiones_limpios.append({
            "id_camion": cid,
            "lat": lat,
            "lon": lon,
            "ruta_origen": c.get("ruta_origen") or (c.get("ruta") or [None])[0] or "",
            "ruta_destino": c.get("ruta_destino") or (c.get("ruta") or [None])[-1] or "",
            "ruta_sugerida": c.get("ruta_sugerida") or c.get("ruta") or [],
            "estado_ruta": c.get("estado_ruta") or "En ruta",
            "motivo_retraso": c.get("motivo_retraso"),
        })
    return nodos_limpios, aristas_limpias, camiones_limpios


def procesar_y_persistir(spark, estados_nodos, estados_aristas, camiones):
    """Ejecutar minería de grafos y persistir en Hive + Cassandra."""
    nodos = get_nodos()
    aristas = get_aristas()

    g0 = construir_grafo_base(spark, nodos, aristas)
    g = aplicar_autosanacion(g0, estados_aristas, estados_nodos)

    # PageRank para identificar nodos críticos
    pr = g.pageRank(resetProbability=0.15, maxIter=10)
    pagerank_df = pr.vertices.select("id", col("pagerank").alias("pagerank_val"))

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
    from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
    pr_schema = StructType([
        StructField("id_nodo", StringType()),
        StructField("pagerank", FloatType()),
        StructField("ultima_actualizacion", TimestampType()),
    ])
    pr_data = [(r["id"], float(r["pagerank_val"]), datetime.now(timezone.utc)) for r in pr_rows]
    df_pr = spark.createDataFrame(pr_data, ["id_nodo", "pagerank", "ultima_actualizacion"])
    df_pr.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="pagerank_nodos", keyspace=KEYSPACE) \
        .mode("append") \
        .save()

    # Hive: histórico de eventos (si Hive está disponible)
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS logistica_espana")
        spark.sql("USE logistica_espana")
        df_nodos.withColumn("fecha_proceso", current_timestamp()).write \
            .format("hive") \
            .mode("append") \
            .saveAsTable("logistica_espana.historico_nodos")
    except Exception as e:
        print(f"[HIVE] Opcional - No disponible: {e}")

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
            estados_nodos = {n: {"estado": random.choice(["OK", "Congestionado", "Bloqueado"]), "motivo": None} for n in nodos}
            for n in list(estados_nodos.keys())[:5]:
                if estados_nodos[n]["estado"] == "Congestionado":
                    estados_nodos[n]["motivo"] = random.choice(["Niebla", "Tráfico", "Lluvia"])
                elif estados_nodos[n]["estado"] == "Bloqueado":
                    estados_nodos[n]["motivo"] = random.choice(["Incendio", "Nieve", "Accidente"])
            estados_aristas = {}
            for a, b, d in aristas:
                key = f"{a}|{b}"
                est = random.choice(["OK", "Congestionado", "Bloqueado"])
                motivo = random.choice(["Niebla", "Tráfico", "Lluvia"]) if est == "Congestionado" else (random.choice(["Incendio", "Nieve"]) if est == "Bloqueado" else None)
                estados_aristas[key] = {"estado": est, "motivo": motivo, "distancia_km": d}
            camiones = [
                {"id_camion": f"camion_{i}", "lat": 40.4 + i * 0.1, "lon": -3.7, "ruta": ["Madrid", "Barcelona"], "ruta_sugerida": ["Madrid", "Toledo", "Cuenca", "Barcelona"]}
                for i in range(1, 6)
            ]
        else:
            # El JSON de ingesta usa "estados_nodos" y "estados_aristas"
            estados_nodos = payload.get("estados_nodos", payload.get("nodos_estado", {}))
            estados_aristas = payload.get("estados_aristas", payload.get("aristas_estado", {}))
            camiones = payload.get("camiones", [])
            # Normalizar formato camiones (ingesta usa "id", "posicion_actual", "ruta")
            camiones = [
                {
                    "id_camion": c.get("id") or c.get("id_camion"),
                    "lat": c.get("posicion_actual", {}).get("lat") if isinstance(c.get("posicion_actual"), dict) else c.get("lat"),
                    "lon": c.get("posicion_actual", {}).get("lon") if isinstance(c.get("posicion_actual"), dict) else c.get("lon"),
                    "ruta": c.get("ruta", []),
                    "ruta_sugerida": c.get("ruta_sugerida", c.get("ruta", [])),
                    "ruta_origen": (c.get("ruta") or [None])[0],
                    "ruta_destino": (c.get("ruta") or [None])[-1],
                    "estado_ruta": c.get("estado_ruta", "En ruta"),
                    "motivo_retraso": c.get("motivo_retraso"),
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
        procesar_y_persistir(spark, estados_nodos, estados_aristas, camiones)
        print("[PROCESAMIENTO] OK - Cassandra y Hive actualizados")
    finally:
        spark.stop()
        print("[PROCESAMIENTO] Spark detenido (spark.stop())")


if __name__ == "__main__":
    main()
