"""
Utilidades de testing para el ciclo KDD.

Estas funciones encapsulan transformaciones reproducibles usadas por el plan de
pruebas sin depender de servicios externos.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def limpiar_registros_gps_df(df: DataFrame) -> DataFrame:
    """
    Limpia registros GPS con reglas de calidad alineadas al proyecto:
    - elimina nulos en identificador / coordenadas / velocidad
    - elimina duplicados por vehículo y timestamp
    - filtra coordenadas fuera de España
    - exige velocidad positiva
    """

    return (
        df.dropna(subset=["id_vehiculo", "lat", "lon", "velocidad", "event_time"])
        .dropDuplicates(["id_vehiculo", "event_time"])
        .filter(F.col("lat").between(35.0, 44.5))
        .filter(F.col("lon").between(-10.0, 5.0))
        .filter(F.col("velocidad") > 0)
    )


def enriquecer_con_maestros_df(df_gps: DataFrame, df_maestro: DataFrame) -> DataFrame:
    """
    Enriquecimiento por JOIN con datos maestros de rutas y almacenes.
    """
    maestro = df_maestro
    if "hub_logistico" in maestro.columns:
        maestro = maestro.withColumnRenamed("hub_logistico", "hub_logistico_maestro")
    return (
        df_gps.join(maestro, on="id_vehiculo", how="left")
        .withColumn(
            "ruta",
            F.when(
                F.col("ruta").isNull() | (F.trim(F.col("ruta")) == ""),
                F.concat_ws("->", F.col("origen_almacen"), F.col("destino_almacen")),
            ).otherwise(F.col("ruta")),
        )
        .withColumn(
            "hub_logistico",
            F.coalesce(
                F.when(F.trim(F.col("hub_logistico")) == "", F.lit(None)).otherwise(F.col("hub_logistico")),
                F.col("hub_logistico_maestro"),
                F.col("origen_almacen"),
            ),
        )
        .drop("hub_logistico_maestro")
    )


def calcular_metricas_ventana_15min_df(df: DataFrame) -> DataFrame:
    """
    Agrega métricas de operación por ventana de 15 minutos y ruta.
    Espera columnas:
    - id_vehiculo
    - event_time
    - delay_minutes
    - estado_evento
    - ruta
    """

    return (
        df.withWatermark("event_time", "5 minutes")
        .groupBy(F.window("event_time", "15 minutes", "15 minutes"), F.col("ruta"))
        .agg(
            F.avg("delay_minutes").alias("retraso_medio_min"),
            F.max("delay_minutes").alias("retraso_max_min"),
            F.countDistinct("id_vehiculo").alias("vehiculos_reportando"),
            F.sum(F.when(F.col("estado_evento") == "Congestionado", 1).otherwise(0)).alias("vehiculos_congestionados"),
            F.sum(
                F.when(F.col("estado_evento").isin("Perdido", "SinSenal", "Sin señal"), 1).otherwise(0)
            ).alias("vehiculos_perdidos"),
        )
        .withColumn("inicio_ventana", F.col("window.start"))
        .withColumn("fin_ventana", F.col("window.end"))
        .drop("window")
    )


def detectar_eventos_criticos_df(
    metricas_df: DataFrame,
    *,
    retraso_umbral_min: float = 30.0,
    congestion_umbral: int = 2,
    cascada_umbral: int = 3,
) -> DataFrame:
    """
    Detecta eventos operativos críticos a partir de métricas agregadas.
    """

    base = (
        metricas_df.withColumn(
            "tipo_evento",
            F.when(F.col("vehiculos_perdidos") >= cascada_umbral, F.lit("fallo_cascada"))
            .when(F.col("vehiculos_perdidos") >= 1, F.lit("perdida_vehiculo"))
            .when(F.col("vehiculos_congestionados") >= congestion_umbral, F.lit("congestion"))
            .when(F.col("retraso_medio_min") >= retraso_umbral_min, F.lit("retraso_elevado"))
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "severidad",
            F.when(F.col("tipo_evento") == "fallo_cascada", F.lit("critica"))
            .when(F.col("tipo_evento") == "perdida_vehiculo", F.lit("alta"))
            .when(F.col("tipo_evento") == "congestion", F.lit("alta"))
            .when(F.col("tipo_evento") == "retraso_elevado", F.lit("media"))
            .otherwise(F.lit(None)),
        )
    )
    return base.filter(F.col("tipo_evento").isNotNull())
