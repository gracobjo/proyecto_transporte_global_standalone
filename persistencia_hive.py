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
from typing import Dict, List, Optional

HIVE_DB = "logistica_db"

TABLA_EVENTOS = "eventos_historico"
TABLA_CAMIONES = "tracking_camiones_historico"
TABLA_CLIMA = "clima_historico"
TABLA_RUTAS = "rutas_alternativas_historico"
TABLA_AGG = "agg_estadisticas_diarias"


def crear_spark_hive(app_name: str = "HivePersistence") -> SparkSession:
    """Crea sesión Spark con soporte Hive y Cassandra."""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .enableHiveSupport() \
        .getOrCreate()


def inicializar_esquema_hive(spark: SparkSession) -> None:
    """Inicializa la base de datos y tablas Hive con esquema optimizado."""
    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
    spark.sql(f"USE {HIVE_DB}")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLA_EVENTOS} (
            timestamp STRING,
            anio INT,
            mes INT,
            dia INT,
            hora INT,
            minuto INT,
            dia_semana STRING,
            tipo_evento STRING,
            id_elemento STRING,
            tipo_elemento STRING,
            estado STRING,
            motivo STRING,
            pagerank DOUBLE,
            distancia_km DOUBLE,
            hub_asociado STRING
        )
        PARTITIONED BY (anio_part INT, mes_part INT)
        STORED AS parquet
        TBLPROPERTIES ('parquet.compression'='SNAPPY')
    """)
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLA_CLIMA} (
            timestamp STRING,
            anio INT,
            mes INT,
            dia INT,
            ciudad STRING,
            temperatura DOUBLE,
            humedad INT,
            descripcion STRING,
            visibilidad INT,
            estado_carretera STRING
        )
        PARTITIONED BY (anio_part INT, mes_part INT)
        STORED AS parquet
    """)
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLA_CAMIONES} (
            timestamp STRING,
            anio INT,
            mes INT,
            dia INT,
            id_camion STRING,
            origen STRING,
            destino STRING,
            nodo_actual STRING,
            lat_actual DOUBLE,
            lon_actual DOUBLE,
            progreso_pct DOUBLE,
            distancia_total_km DOUBLE,
            tiene_ruta_alternativa BOOLEAN,
            distancia_alternativa_km DOUBLE
        )
        PARTITIONED BY (anio_part INT, mes_part INT)
        STORED AS parquet
    """)
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLA_RUTAS} (
            timestamp STRING,
            anio INT,
            mes INT,
            dia INT,
            origen STRING,
            destino STRING,
            ruta_original STRING,
            ruta_alternativa STRING,
            distancia_original_km DOUBLE,
            distancia_alternativa_km DOUBLE,
            motivo_bloqueo STRING,
            ahorro_km DOUBLE
        )
        PARTITIONED BY (anio_part INT, mes_part INT)
        STORED AS parquet
    """)
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLA_AGG} (
            anio INT,
            mes INT,
            dia INT,
            tipo_evento STRING,
            estado STRING,
            motivo STRING,
            contador INT,
            pct_total DOUBLE
        )
        PARTITIONED BY (anio_part INT, mes_part INT)
        STORED AS parquet
    """)


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
        df.write.mode("append").partitionBy("anio_part", "mes_part").insertInto(TABLA_EVENTOS)
        print(f"Insertados {len(eventos)} eventos en {TABLA_EVENTOS}")


def persistir_clima_historico(spark: SparkSession, datos: Dict) -> None:
    """Persiste datos climáticos en Hive."""
    
    ts = datos.get("timestamp", datetime.now().isoformat())
    componentes = parsear_timestamp(ts)
    anio = componentes["anio"]
    mes = componentes["mes"]
    
    registros = []
    
    for clima in datos.get("clima", []):
        estado_carretera = "Óptimo"
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
            "temperatura": clima.get("temperatura"),
            "humedad": clima.get("humedad"),
            "descripcion": clima.get("descripcion", ""),
            "visibilidad": clima.get("visibilidad", 10000),
            "estado_carretera": estado_carretera,
            "anio_part": anio,
            "mes_part": mes
        })
    
    if registros:
        df = spark.createDataFrame(registros)
        df.write.mode("append").partitionBy("anio_part", "mes_part").insertInto(TABLA_CLIMA)
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
    
    for camion in datos.get("camiones", []):
        cam_id = camion.get("id", "UNKNOWN")
        ruta_alt = rutas_alternativas.get(cam_id, {})
        
        pos = camion.get("posicion_actual", {})
        ruta = camion.get("ruta", [])
        
        registros.append({
            "timestamp": ts,
            "anio": componentes["anio"],
            "mes": componentes["mes"],
            "dia": componentes["dia"],
            "id_camion": cam_id,
            "origen": ruta[0] if ruta else "",
            "destino": ruta[-1] if len(ruta) > 1 else "",
            "nodo_actual": camion.get("nodo_actual", ""),
            "lat_actual": pos.get("lat", 0.0),
            "lon_actual": pos.get("lon", 0.0),
            "progreso_pct": camion.get("progreso_pct", 0.0),
            "distancia_total_km": camion.get("distancia_total_km", 0.0),
            "tiene_ruta_alternativa": ruta_alt.get("ruta") is not None,
            "distancia_alternativa_km": ruta_alt.get("distancia", 0.0),
            "anio_part": anio,
            "mes_part": mes
        })
    
    if registros:
        df = spark.createDataFrame(registros)
        df.write.mode("append").partitionBy("anio_part", "mes_part").insertInto(TABLA_CAMIONES)
        print(f"Insertados {len(registros)} registros de camiones en {TABLA_CAMIONES}")


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
        df.write.mode("append").partitionBy("anio_part", "mes_part").insertInto(TABLA_RUTAS)
        print(f"Insertadas {len(registros)} rutas alternativas en {TABLA_RUTAS}")


def calcular_estadisticas_diarias(spark: SparkSession) -> None:
    """Calcula agregaciones diarias para análisis de tendencias."""
    
    spark.sql(f"""
        INSERT OVERWRITE TABLE {TABLA_AGG} PARTITION(anio_part, mes_part)
        SELECT 
            anio,
            mes,
            dia,
            tipo_evento,
            estado,
            motivo,
            COUNT(*) as contador,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY anio, mes, dia), 2) as pct_total,
            anio as anio_part,
            mes as mes_part
        FROM {TABLA_EVENTOS}
        WHERE anio_part IS NOT NULL
        GROUP BY anio, mes, dia, tipo_evento, estado, motivo, anio, mes
    """)


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
    
    print("Persistiendo rutas alternativas...")
    persistir_rutas_alternativas(spark, datos, rutas_alternativas)
    
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
