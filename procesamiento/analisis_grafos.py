from pyspark.sql import SparkSession
from graphframes import GraphFrame
from pyspark.sql.functions import current_timestamp, lit

# 1. Inicializamos Spark con la configuración de Cassandra
spark = SparkSession.builder \
    .appName("Analisis_Rutas_Transporte") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .getOrCreate()

# 2. Definición de Vértices y Aristas (Tus datos originales)
v = spark.createDataFrame([
  ("A", "Almacen_Central", "Madrid"),
  ("B", "Almacen_Puerto", "Valencia"),
  ("C", "Centro_Distribucion", "Barcelona")
], ["id", "nombre", "ciudad"])

e = spark.createDataFrame([
  ("A", "B", "Ruta_Sur"),
  ("B", "C", "Ruta_Costa"),
  ("A", "C", "Ruta_Norte")
], ["src", "dst", "tipo_ruta"])

# 3. Crear el Grafo
g = GraphFrame(v, e)

# 4. MINERÍA: Calculamos la importancia (degrees)
# Transformamos los nombres de columnas para que coincidan con Cassandra
resultados = g.degrees.withColumnRenamed("id", "id_almacen") \
                      .withColumnRenamed("degree", "grado_importancia")

# 5. ACCIÓN: Enriquecemos los datos antes de persistir
# Añadimos nombre genérico y la marca de tiempo actual
final_df = resultados.withColumn("nombre_almacen", lit("Nodo_Logistico_España")) \
                     .withColumn("fecha_proceso", current_timestamp())

# 6. PERSISTENCIA: Guardar en Cassandra
print(">>> Iniciando persistencia en Cassandra (Capa de Servicio)...")

final_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="analisis_red", keyspace="logistica_stats") \
    .mode("append") \
    .save()

print(">>> ¡Éxito! Datos guardados en logistica_stats.analisis_red")

spark.stop()
