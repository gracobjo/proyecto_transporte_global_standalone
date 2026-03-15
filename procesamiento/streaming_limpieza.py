from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Spark 3.5.x setup [cite: 14]
spark = SparkSession.builder \
    .appName("TransporteGlobal_Limpieza") \
    .config("spark.sql.streaming.checkpointLocation", "/user/hadoop/checkpoints/audit") \
    .enableHiveSupport() \
    .getOrCreate()

# Fase I: Esquema de sensores GPS [cite: 19]
gpsSchema = StructType() \
    .add("id_vehiculo", StringType()) \
    .add("latitud", DoubleType()) \
    .add("longitud", DoubleType()) \
    .add("timestamp", TimestampType()) \
    .add("velocidad", DoubleType())

# Ingesta desde Kafka [cite: 20]
rawStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "topic-hadoop") \
    .load()

# Fase II: Limpieza y Normalización [cite: 23]
cleanDF = rawStream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), gpsSchema).as("data")) \
    .select("data.*") \
    .filter(col("id_vehiculo").isNotNull())

# Almacenamiento en HDFS para Auditoría [cite: 21]
query = cleanDF.writeStream \
    .format("parquet") \
    .option("path", "hdfs://127.0.0.1:9000/user/hadoop/kafka_data/audit") \
    .start()

query.awaitTermination()
