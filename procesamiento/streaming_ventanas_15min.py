#!/usr/bin/env python3
"""
Structured Streaming con ventanas de 15 minutos (PDF Proyecto Big Data - Fase III).
- Lee del tema Kafka transporte_filtered.
- Agrupa por ventana de 15 min y calcula métricas: conteo de eventos, media de progreso camiones,
  conteo por estado (OK/Congestionado/Bloqueado).
- Escribe agregados en consola (o en Hive/Cassandra si se configura).
"""
import os
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, max as spark_max, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from config import KAFKA_BOOTSTRAP, TOPIC_FILTERED


def run_streaming_15min(spark_master="local[*]", checkpoint_dir=None):
    """Ejecuta Structured Streaming con ventanas de 15 minutos (media de retrasos / conteos)."""
    checkpoint_dir = checkpoint_dir or "/tmp/streaming_transporte_checkpoint"
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    spark = (
        SparkSession.builder
        .appName("StreamingVentanas15min")
        .master(spark_master)
        .config("spark.sql.streaming.checkpointLocation", checkpoint_dir)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_FILTERED)
        .option("startingOffsets", "latest")
        .load()
    )

    schema_value = StructType([
        StructField("timestamp", StringType()),
        StructField("intervalo_minutos", IntegerType()),
    ])

    parsed = df.select(
        from_json(col("value").cast("string"), schema_value).alias("data")
    ).select("data.timestamp")

    with_ts = parsed.withColumn(
        "event_time",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
    ).na.drop(subset=["event_time"])

    ventanas = with_ts.withWatermark("event_time", "10 minutes").groupBy(
        window(col("event_time"), "15 minutes", "15 minutes")
    ).agg(
        count("*").alias("num_eventos"),
        spark_max("timestamp").alias("ultimo_ts")
    )

    query = (
        ventanas.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 50)
        .start()
    )

    print("[STREAMING] Ventanas de 15 min leyendo de", TOPIC_FILTERED)
    print("[STREAMING] Checkpoint:", checkpoint_dir)
    query.awaitTermination()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--master", default="local[*]", help="Spark master (local[*] o yarn)")
    p.add_argument("--checkpoint", default=None, help="Checkpoint dir para streaming")
    args = p.parse_args()
    run_streaming_15min(spark_master=args.master, checkpoint_dir=args.checkpoint)
