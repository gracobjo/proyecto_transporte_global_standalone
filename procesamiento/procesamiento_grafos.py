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
    """Spark con Cassandra, JARs configurados. master=local (1 JVM, sin executor separado) para evitar RpcEndpointNotFoundException."""
    # Evitar que hostname (nodo1.home) rompa RPC; usar localhost
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    # GraphFrames JAR es para Spark 3.5 (Scala 2.12). Usar Spark del sistema (/opt/spark) con PySpark 3.5.
    # No quitar SPARK_HOME para evitar NoClassDefFoundError: scala/Serializable con PySpark 4.x
    return (
        SparkSession.builder
        .appName("MineriaTransporteEspaña")
        .master("local")
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
            estados_nodos = payload.get("nodos_estado", {})
            estados_aristas = payload.get("aristas_estado", {})
            camiones = payload.get("camiones", [])
            # Enriquecer clima en nodos hub
            clima = payload.get("clima_hubs", {})
            for hub, c in clima.items():
                if hub in estados_nodos:
                    estados_nodos[hub]["clima_desc"] = c.get("descripcion", "N/A")
                    estados_nodos[hub]["temp"] = c.get("temp")
                    estados_nodos[hub]["humedad"] = c.get("humedad")
                    estados_nodos[hub]["viento"] = c.get("viento")

        procesar_y_persistir(spark, estados_nodos, estados_aristas, camiones)
        print("[PROCESAMIENTO] OK - Cassandra y Hive actualizados")
    finally:
        spark.stop()
        print("[PROCESAMIENTO] Spark detenido (spark.stop())")


if __name__ == "__main__":
    main()
