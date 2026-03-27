from __future__ import annotations

from pyspark.sql import functions as F

from graph_ai.graph_processing import AnomalyParams, compare_graphs
from graph_ai.models import GraphPayload
from procesamiento.testing_kdd_utils import calcular_metricas_ventana_15min_df, detectar_eventos_criticos_df


def test_detects_high_delay_and_congestion_events(spark, load_json_fixture):
    eventos = load_json_fixture("streaming_extremos.json")["eventos"]
    df = spark.createDataFrame(eventos).withColumn("event_time", F.to_timestamp("event_time"))

    metricas = calcular_metricas_ventana_15min_df(df)
    eventos_detectados = detectar_eventos_criticos_df(metricas, retraso_umbral_min=30.0, congestion_umbral=2)
    tipos = {row["tipo_evento"] for row in eventos_detectados.collect()}

    assert "retraso_elevado" in tipos or "congestion" in tipos


def test_detects_cascade_failure_when_multiple_vehicles_are_lost(spark, load_json_fixture):
    eventos = load_json_fixture("fallo_cascada.json")["eventos"]
    df = spark.createDataFrame(eventos).withColumn("event_time", F.to_timestamp("event_time"))

    metricas = calcular_metricas_ventana_15min_df(df)
    eventos_detectados = detectar_eventos_criticos_df(metricas, cascada_umbral=3)
    rows = eventos_detectados.collect()

    assert rows
    assert any(row["tipo_evento"] == "fallo_cascada" for row in rows)
    assert any(row["severidad"] == "critica" for row in rows)


def test_graph_ai_detects_structural_change_anomaly(load_json_fixture):
    fixture = load_json_fixture("fallo_cascada.json")
    current_graph = GraphPayload(**fixture["graph_current"])
    previous_graph = GraphPayload(**fixture["graph_previous"])

    _, anomaly_scores, anomalous_nodes, structural_summary, _ = compare_graphs(
        current_graph,
        previous_graph,
        AnomalyParams(anomaly_score_threshold=1.0, structural_change_threshold=0.5),
    )

    assert structural_summary["edges_current"] < structural_summary["edges_previous"]
    assert anomalous_nodes
    assert any(anomaly_scores[node] >= 1.0 for node in anomalous_nodes)
