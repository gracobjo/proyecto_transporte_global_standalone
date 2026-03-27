from __future__ import annotations

from pyspark.sql import functions as F

from procesamiento.testing_kdd_utils import limpiar_registros_gps_df


def test_cleaning_removes_nulls_duplicates_and_invalid_coordinates(spark, load_json_fixture):
    raw = load_json_fixture("ingesta_errores.json")["registros_gps"]
    df = spark.createDataFrame(raw).withColumn("event_time", F.to_timestamp("event_time"))

    cleaned = limpiar_registros_gps_df(df)
    rows = cleaned.collect()

    assert len(rows) == 1
    assert rows[0]["id_vehiculo"] == "veh_1"
    assert rows[0]["velocidad"] > 0


def test_cleaning_keeps_only_spain_bounded_points(spark):
    rows = [
        {"id_vehiculo": "ok_1", "lat": 40.4, "lon": -3.7, "velocidad": 55.0, "event_time": "2026-03-26T10:00:00"},
        {"id_vehiculo": "bad_1", "lat": 60.0, "lon": 12.0, "velocidad": 55.0, "event_time": "2026-03-26T10:01:00"},
    ]
    df = spark.createDataFrame(rows).withColumn("event_time", F.to_timestamp("event_time"))

    cleaned = limpiar_registros_gps_df(df)

    assert cleaned.count() == 1
    assert cleaned.first()["id_vehiculo"] == "ok_1"
