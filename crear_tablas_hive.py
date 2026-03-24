from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "/user/hadoop/proyecto_transporte_global/spark-warehouse") \
    .config("spark.driver.memory", "1g") \
    .enableHiveSupport() \
    .getOrCreate()

print("Creando base de datos logistica_db...")
spark.sql("CREATE DATABASE IF NOT EXISTS logistica_db")
spark.sql("USE logistica_db")

print("Creando tabla eventos_historico...")
spark.sql("""
CREATE TABLE IF NOT EXISTS eventos_historico (
    timestamp STRING, anio INT, mes INT, dia INT, hora INT, minuto INT,
    tipo_evento STRING, id_elemento STRING, estado STRING, motivo STRING,
    pagerank DOUBLE, distancia_km DOUBLE
) STORED AS PARQUET
""")

print("Creando tabla metricas_nodos_hist...")
spark.sql("""
CREATE TABLE IF NOT EXISTS metricas_nodos_hist (
    id_nodo STRING, tipo_nodo STRING, pagerank_score FLOAT,
    conectividad_grado INT, fecha_proceso STRING
) STORED AS PARQUET
""")

print("Creando tabla clima_historico...")
spark.sql("""
CREATE TABLE IF NOT EXISTS clima_historico (
    hub_nombre STRING, temperatura FLOAT, humedad INT,
    descripcion STRING, visibilidad INT, fecha_captura STRING
) STORED AS PARQUET
""")

print("Creando tabla tracking_camiones_hist...")
spark.sql("""
CREATE TABLE IF NOT EXISTS tracking_camiones_hist (
    camion_id STRING, origen STRING, destino STRING, nodo_actual STRING,
    lat_actual FLOAT, lon_actual FLOAT, progreso_pct FLOAT,
    distancia_total_km FLOAT, timestamp_posicion STRING
) STORED AS PARQUET
""")

print("\nTablas creadas:")
spark.sql("SHOW TABLES").show()

spark.stop()
print("Done!")
