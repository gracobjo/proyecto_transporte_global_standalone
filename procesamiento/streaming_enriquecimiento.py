from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Iniciar sesión con soporte Hive
spark = SparkSession.builder \
    .appName("Enriquecimiento_Transporte") \
    .enableHiveSupport() \
    .getOrCreate()

# 2. Definir el esquema del GPS que viene de Kafka
schema = StructType([
    StructField("id_vehiculo", StringType()),
    StructField("latitud", DoubleType()),
    StructField("longitud", DoubleType()),
    StructField("timestamp", StringType())
])

# 3. Leer de Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic-hadoop") \
    .load()

# 4. Parsear el JSON
gps_df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Cargar Datos Maestros desde Hive (Caché para velocidad)
maestro_df = spark.table("logistica_db.maestro_vehiculos")

# 6. ENRIQUECIMIENTO (Join)
enriquecido_df = gps_df.join(maestro_df, "id_vehiculo", "inner")

# 7. Escribir resultado en consola (para probar) y en HDFS
query = enriquecido_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
