from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from procesamiento.testing_kdd_utils import enriquecer_con_maestros_df


def test_enrichment_joins_master_route_and_hub(spark):
    schema = StructType(
        [
            StructField("id_vehiculo", StringType()),
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType()),
            StructField("velocidad", DoubleType()),
            StructField("event_time", StringType()),
            StructField("ruta", StringType()),
            StructField("hub_logistico", StringType()),
        ]
    )
    gps_rows = [("veh_1", 40.4, -3.7, 63.0, "2026-03-26T10:00:00", None, None)]
    maestros = [
        {
            "id_vehiculo": "veh_1",
            "origen_almacen": "Madrid",
            "destino_almacen": "Barcelona",
            "hub_logistico": "Madrid",
        }
    ]

    df_gps = spark.createDataFrame(gps_rows, schema=schema).withColumn("event_time", F.to_timestamp("event_time"))
    df_maestro = spark.createDataFrame(maestros)

    enriquecido = enriquecer_con_maestros_df(df_gps, df_maestro).first()

    assert enriquecido["origen_almacen"] == "Madrid"
    assert enriquecido["destino_almacen"] == "Barcelona"
    assert enriquecido["hub_logistico"] == "Madrid"
    assert enriquecido["ruta"] == "Madrid->Barcelona"


def test_enrichment_preserves_route_when_already_present(spark):
    gps_rows = [
        {
            "id_vehiculo": "veh_2",
            "lat": 41.3,
            "lon": 2.1,
            "velocidad": 70.0,
            "event_time": "2026-03-26T10:00:00",
            "ruta": "Bilbao->Vigo",
            "hub_logistico": "Bilbao",
        }
    ]
    maestros = [{"id_vehiculo": "veh_2", "origen_almacen": "Bilbao", "destino_almacen": "Vigo", "hub_logistico": "Bilbao"}]

    df_gps = spark.createDataFrame(gps_rows).withColumn("event_time", F.to_timestamp("event_time"))
    df_maestro = spark.createDataFrame(maestros)

    enriquecido = enriquecer_con_maestros_df(df_gps, df_maestro).first()

    assert enriquecido["ruta"] == "Bilbao->Vigo"
    assert enriquecido["hub_logistico"] == "Bilbao"
