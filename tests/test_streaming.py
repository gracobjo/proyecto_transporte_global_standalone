from __future__ import annotations

from pyspark.sql import functions as F

from procesamiento.testing_kdd_utils import calcular_metricas_ventana_15min_df


def test_streaming_window_aggregates_route_delay_metrics(spark, load_json_fixture):
    eventos = load_json_fixture("streaming_extremos.json")["eventos"]
    df = spark.createDataFrame(eventos).withColumn("event_time", F.to_timestamp("event_time"))

    metricas = calcular_metricas_ventana_15min_df(df)
    rows = {row["ruta"]: row for row in metricas.collect()}

    assert "Madrid->Barcelona" in rows
    madrid = rows["Madrid->Barcelona"]
    assert round(madrid["retraso_medio_min"], 2) == round((12.0 + 48.0 + 55.0) / 3.0, 2)
    assert madrid["vehiculos_reportando"] == 3
    assert madrid["vehiculos_congestionados"] == 2


def test_streaming_window_keeps_independent_routes_separated(spark, load_json_fixture):
    eventos = load_json_fixture("streaming_extremos.json")["eventos"]
    df = spark.createDataFrame(eventos).withColumn("event_time", F.to_timestamp("event_time"))

    metricas = calcular_metricas_ventana_15min_df(df)

    assert metricas.select("ruta").distinct().count() == 2
