"""
SIMLOG España — Fase II y III: Procesamiento de grafos
Ruta base: ~/proyecto_transporte_global/
"""

import json
import math
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, MapType
from graphframes import GraphFrame

from config import CASSANDRA_HOST, KEYSPACE
from config_nodos import RED, get_nodos, get_aristas

SPARK_MASTER = "local[*]"

HIVE_DB = "logistica_db"
HIVE_TABLE_EVENTOS = "eventos_historico"
HIVE_TABLE_NODOS = "nodos_estado"


def crear_spark_session(app_name: str = "ProcesamientoGrafos") -> SparkSession:
    """Crea sesión Spark optimizada para 4GB de RAM con Hive y Cassandra."""
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(SPARK_MASTER) \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.session.timeZone", "Europe/Madrid") \
        .config("spark.jars", "/home/hadoop/proyecto_transporte_global/herramientas/graphframes-0.8.3-spark3.5-s_2.12.jar") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .enableHiveSupport()
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def cargar_datos_kafka(spark: SparkSession, topic: str = "transporte_status") -> Optional[Dict]:
    """Carga datos del topic Kafka."""
    try:
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
        
        if df.isEmpty():
            print("No hay datos en Kafka")
            return None
        
        json_str = df.select(col("value").cast("string")).orderBy(col("timestamp").desc()).first()[0]
        return json.loads(json_str)
        
    except Exception as e:
        print(f"Error leyendo de Kafka: {e}")
        return None


def construir_grafo(
    spark: SparkSession,
    estados_nodos: Dict,
    estados_aristas: Dict
) -> GraphFrame:
    """
    Construye GraphFrame a partir de estados de nodos y aristas.
    Aplica lógica de autosanación: elimina aristas Bloqueadas.
    """
    nodos = get_nodos()
    aristas = get_aristas()
    
    vertices_data = []
    for nombre, datos in nodos.items():
        estado = estados_nodos.get(nombre, {}).get("estado", "ok")
        motivo = estados_nodos.get(nombre, {}).get("motivo", "")
        vertices_data.append({
            "id": nombre,
            "lat": datos["lat"],
            "lon": datos["lon"],
            "tipo": datos["tipo"],
            "estado": estado,
            "motivo": motivo
        })
    
    vertices_df = spark.createDataFrame(vertices_data)
    
    edges_data = []
    for src, dst, dist in aristas:
        edge_id = f"{src}|{dst}"
        estado = estados_aristas.get(edge_id, {}).get("estado", "ok")
        motivo = estados_aristas.get(edge_id, {}).get("motivo", "")
        
        if estado == "bloqueado":
            continue
        
        peso_base = dist
        if estado == "congestionado":
            if "Niebla" in motivo or "Lluvia" in motivo:
                peso_base *= 2.0
            else:
                peso_base *= 1.5
        
        edges_data.append({
            "src": src,
            "dst": dst,
            "distancia_km": dist,
            "peso": peso_base,
            "estado": estado,
            "motivo": motivo
        })
    
    edges_df = spark.createDataFrame(edges_data)
    
    g = GraphFrame(vertices_df, edges_df)
    return g


def calcular_pagerank(g: GraphFrame, tol: float = 0.001) -> Dict:
    """Calcula PageRank para identificar nodos críticos."""
    pr = g.pageRank(tol=tol, resetProbability=0.15)
    
    resultados = pr.vertices.orderBy(col("pagerank").desc()).collect()
    
    pagerank_dict = {}
    for row in resultados:
        pagerank_dict[row["id"]] = {
            "pagerank": round(row["pagerank"], 4),
            "tipo": row["tipo"],
            "estado": row["estado"]
        }
    
    return pagerank_dict


def calcular_shortest_path(
    g: GraphFrame,
    origen: str,
    destino: str
) -> Tuple[Optional[List[str]], float]:
    """
    Calcula shortest path dinámico considerando pesos de aristas.
    Retorna (ruta, distancia_total)
    """
    try:
        ssp = g.shortestPaths(landmarks=[destino])
        
        resultados = ssp.vertices.filter(col("id") == origen).collect()
        
        if not resultados:
            return None, 0.0
        
        distances = resultados[0]["distances"]
        
        if destino not in distances:
            return None, 0.0
        
        ruta = [origen]
        aristas_df = g.edges.filter(col("src") == origen).collect()
        
        current = origen
        distancia_total = 0.0
        
        for _ in range(50):
            if current == destino:
                break
            
            candidates = g.edges.filter(col("src") == current).collect()
            if not candidates:
                break
            
            next_edge = min(candidates, key=lambda e: e["peso"])
            next_node = next_edge["dst"]
            
            if next_node in ruta:
                break
            
            ruta.append(next_node)
            distancia_total += next_edge["peso"]
            current = next_node
        
        if ruta[-1] != destino:
            return None, 0.0
        
        return ruta, round(distancia_total, 2)
        
    except Exception as e:
        print(f"Error calculando shortest path: {e}")
        return None, 0.0


def buscar_rutas_alternativas(
    g: GraphFrame,
    origen: str,
    destino: str,
    ruta_original: List[str]
) -> Tuple[Optional[List[str]], float, str]:
    """
    Busca ruta alternativa si la original está bloqueada.
    Retorna (ruta_alternativa, distancia, motivo_bloqueo)
    """
    ruta, distancia = calcular_shortest_path(g, origen, destino)
    
    if ruta is None:
        return None, 0.0, "Sin ruta disponible"
    
    if ruta == ruta_original:
        return ruta, distancia, "OK"
    
    return ruta, distancia, "Ruta alternativa"


def persistir_hive(
    spark: SparkSession,
    datos: Dict,
    pagerank: Dict
) -> None:
    """Persiste histórico de eventos en Hive."""
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
        spark.sql(f"USE {HIVE_DB}")
        
        eventos_data = []
        timestamp = datos.get("timestamp", datetime.now().isoformat())
        
        for nodo, estado_info in datos.get("estados_nodos", {}).items():
            eventos_data.append({
                "timestamp": timestamp,
                "tipo_evento": "nodo",
                "id_elemento": nodo,
                "estado": estado_info.get("estado", "ok"),
                "motivo": estado_info.get("motivo", ""),
                "pagerank": pagerank.get(nodo, {}).get("pagerank", 0.0)
            })
        
        for arista, estado_info in datos.get("estados_aristas", {}).items():
            src, dst = arista.split("|")
            eventos_data.append({
                "timestamp": timestamp,
                "tipo_evento": "arista",
                "id_elemento": arista,
                "estado": estado_info.get("estado", "ok"),
                "motivo": estado_info.get("motivo", ""),
                "distancia_km": estado_info.get("distancia_km", 0.0)
            })
        
        eventos_df = spark.createDataFrame(eventos_data)
        
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {HIVE_TABLE_EVENTOS} (
                timestamp STRING,
                tipo_evento STRING,
                id_elemento STRING,
                estado STRING,
                motivo STRING,
                pagerank DOUBLE,
                distancia_km DOUBLE
            )
            USING parquet
        """)
        
        eventos_df.write.mode("append").insertInto(HIVE_TABLE_EVENTOS)
        
        print(f"Insertados {len(eventos_data)} eventos en Hive")
        
    except Exception as e:
        print(f"Error persistiendo en Hive: {e}")


def persistir_cassandra(
    spark: SparkSession,
    datos: Dict,
    pagerank: Dict,
    rutas_alternativas: Dict
) -> None:
    """
    Persiste estado en Cassandra según `cassandra/esquema_logistica.cql`.
    Columnas: `lat`/`lon`/`ultima_posicion` (no latitud/longitud/progreso_pct en tracking_camiones).
    """
    try:
        from datetime import timezone

        from cassandra.cluster import Cluster

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(KEYSPACE)

        ts = datetime.now(timezone.utc)
        nodos_map = get_nodos()

        for nodo, estado_info in datos.get("estados_nodos", {}).items():
            nd = nodos_map.get(nodo, {})
            session.execute(
                """
                INSERT INTO nodos_estado (
                    id_nodo, lat, lon, tipo, estado, motivo_retraso, clima_actual,
                    temperatura, humedad, viento_velocidad, ultima_actualizacion
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    nodo,
                    float(nd.get("lat", 0.0)),
                    float(nd.get("lon", 0.0)),
                    nd.get("tipo", "secundario"),
                    estado_info.get("estado", "ok"),
                    estado_info.get("motivo", ""),
                    None,
                    None,
                    None,
                    None,
                    ts,
                ),
            )

        for camion in datos.get("camiones", []):
            cam_id = camion.get("id_camion") or camion.get("id", "UNKNOWN")
            pos = camion.get("posicion_actual") or {}
            lat = float(pos.get("lat", camion.get("lat", 0.0)))
            lon = float(pos.get("lon", camion.get("lon", 0.0)))
            ruta = camion.get("ruta") or camion.get("ruta_sugerida") or []
            ruta_list = ruta if isinstance(ruta, list) else []

            session.execute(
                """
                INSERT INTO tracking_camiones (
                    id_camion, lat, lon, ruta_origen, ruta_destino, ruta_sugerida,
                    estado_ruta, motivo_retraso, ultima_posicion
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    cam_id,
                    lat,
                    lon,
                    ruta_list[0] if ruta_list else "",
                    ruta_list[-1] if ruta_list else "",
                    ruta_list,
                    camion.get("estado_ruta", "En ruta"),
                    camion.get("motivo_retraso"),
                    ts,
                ),
            )

        cluster.shutdown()
        print("Actualizado estado en Cassandra")

    except Exception as e:
        print(f"Error persistiendo en Cassandra: {e}")


def ejecutar_procesamiento(datos: Dict = None) -> Dict:
    """
    Ejecuta el pipeline completo de procesamiento de grafos.
    """
    print("=" * 60)
    print("FASE II y III: PROCESAMIENTO DE GRAFOS")
    print("=" * 60)
    
    spark = crear_spark_session()
    
    try:
        if datos is None:
            print("\n[1/6] Cargando datos de Kafka...")
            datos = cargar_datos_kafka(spark)
            if datos is None:
                print("No hay datos para procesar")
                return {}
        else:
            print("\n[1/6] Usando datos proporcionados...")
        
        print("\n[2/6] Construyendo grafo con GraphFrames...")
        g = construir_grafo(spark, datos["estados_nodos"], datos["estados_aristas"])
        
        print(f"  - Vértices: {g.vertices.count()}")
        print(f"  - Aristas: {g.edges.count()}")
        
        print("\n[3/6] Calculando PageRank...")
        pagerank = calcular_pagerank(g)
        print("  - Top 5 nodos críticos:")
        for i, (nodo, info) in enumerate(list(pagerank.items())[:5]):
            print(f"    {i+1}. {nodo}: {info['pagerank']}")
        
        print("\n[4/6] Buscando rutas alternativas para camiones...")
        rutas_alternativas = {}
        for camion in datos.get("camiones", []):
            cam_id = camion["id"]
            ruta = camion["ruta"]
            
            if len(ruta) >= 2:
                origen = ruta[0]
                destino = ruta[-1]
                
                ruta_alt, dist_alt, motivo = buscar_rutas_alternativas(
                    g, origen, destino, ruta
                )
                
                rutas_alternativas[cam_id] = {
                    "ruta": ruta_alt,
                    "distancia": dist_alt,
                    "motivo": motivo
                }
                
                print(f"  - {cam_id}: {motivo}")
        
        print("\n[5/6] Persistiendo en Hive (histórico)...")
        persistir_hive(spark, datos, pagerank)
        
        print("\n[6/6] Persistiendo en Cassandra (estado actual)...")
        persistir_cassandra(spark, datos, pagerank, rutas_alternativas)
        
        print("\n" + "=" * 60)
        print("PROCESAMIENTO COMPLETADO")
        print("=" * 60)
        
        return {
            "pagerank": pagerank,
            "rutas_alternativas": rutas_alternativas,
            "timestamp": datos.get("timestamp", datetime.now().isoformat())
        }
        
    finally:
        spark.stop()
        print("Spark session closed")


if __name__ == "__main__":
    resultado = ejecutar_procesamiento()
    print(f"\nPageRank calculado para {len(resultado.get('pagerank', {}))} nodos")
    print(f"Rutas alternativas calculadas para {len(resultado.get('rutas_alternativas', {}))} camiones")
