"""
Sistema de Gemelo Digital Logístico - Configuración central
Paths de JARs y parámetros configurables
"""
import os

BASE_PATH = os.path.expanduser("~/proyecto_transporte_global")

# JARs - Configurables según instalación
JAR_GRAPHFRAMES = os.environ.get(
    "JAR_GRAPHFRAMES",
    os.path.join(BASE_PATH, "herramientas/graphframes-0.8.3-spark3.5-s_2.12.jar")
)
JAR_CASSANDRA = os.environ.get(
    "JAR_CASSANDRA",
    "/home/hadoop/.ivy2/cache/com.datastax.spark/spark-cassandra-connector_2.12/jars/spark-cassandra-connector_2.12-3.5.0.jar"
)
JAR_KAFKA = os.environ.get(
    "JAR_KAFKA",
    os.path.join(BASE_PATH, "spark-sql-kafka-0-10_2.12-3.5.1.jar.1")
)

# Fallback: si herramienta tiene extensión .1, usar la del proyecto
if not os.path.exists(JAR_GRAPHFRAMES):
    alt = os.path.join(BASE_PATH, "graphframes-0.8.3-spark3.5-s_2.12.jar.1")
    if os.path.exists(alt):
        JAR_GRAPHFRAMES = alt

# API Weather
API_WEATHER_KEY = "735df735c4780d78a550d6bb6b52dfd7"
API_WEATHER_BASE = "https://api.openweathermap.org/data/2.5/weather"

# Kafka
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_TRANSPORTE = "transporte_status"

# Cassandra
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
KEYSPACE = "logistica_espana"

# HDFS
HDFS_BACKUP_PATH = "/user/hadoop/transporte_backup"

# Hive (para histórico)
HIVE_DB = "logistica_espana"
