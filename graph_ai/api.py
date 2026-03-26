from __future__ import annotations

import os
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException

from graph_ai.graph_processing import AnomalyParams, analyze_graph_payload, compare_graphs
from graph_ai.models import AnalyzeGraphRequest, AnalyzeGraphResponse, CompareGraphsRequest, CompareGraphsResponse

app = FastAPI(title="SIMLOG Graph AI", version="1.0")


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True}


@app.post("/analyze-graph", response_model=AnalyzeGraphResponse)
def analyze_graph(req: AnalyzeGraphRequest) -> AnalyzeGraphResponse:
    params = AnomalyParams(
        degree_z_threshold=req.degree_z_threshold,
        edge_z_threshold=req.edge_z_threshold,
        structural_change_threshold=req.structural_change_threshold,
        anomaly_score_threshold=req.anomaly_score_threshold,
    )
    try:
        centrality_metrics, anomaly_scores, anomalous_nodes, debug = analyze_graph_payload(
            req.graph, params, previous_graph=req.previous_graph
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    summary = {"num_nodes": len(centrality_metrics), "num_edges": len(req.graph.edges), "debug": debug}

    # Convert to pydantic models (centrality_metrics already includes anomaly_score)
    response = AnalyzeGraphResponse(
        centrality_metrics={
            node_id: {
                "degree_centrality": float(m["degree_centrality"]),
                "betweenness_centrality": float(m["betweenness_centrality"]),
                "pagerank": float(m["pagerank"]),
                "anomaly_score": float(m["anomaly_score"]),
            }
            for node_id, m in centrality_metrics.items()
        },
        anomaly_scores={k: float(v) for k, v in anomaly_scores.items()},
        anomalous_nodes=anomalous_nodes,
        summary=summary,
    )
    return response


@app.post("/compare-graphs", response_model=CompareGraphsResponse)
def compare_graphs_endpoint(req: CompareGraphsRequest) -> CompareGraphsResponse:
    params = AnomalyParams(
        degree_z_threshold=req.degree_z_threshold,
        edge_z_threshold=req.edge_z_threshold,
        structural_change_threshold=req.structural_change_threshold,
        anomaly_score_threshold=req.anomaly_score_threshold,
    )

    try:
        centrality_metrics_current, anomaly_scores, anomalous_nodes, structural_summary, debug = compare_graphs(
            current=req.current_graph,
            previous=req.previous_graph,
            params=params,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    return CompareGraphsResponse(
        centrality_metrics_current={
            node_id: {
                "degree_centrality": float(m["degree_centrality"]),
                "betweenness_centrality": float(m["betweenness_centrality"]),
                "pagerank": float(m["pagerank"]),
                "anomaly_score": float(m["anomaly_score"]),
            }
            for node_id, m in centrality_metrics_current.items()
        },
        anomaly_scores={k: float(v) for k, v in anomaly_scores.items()},
        anomalous_nodes=anomalous_nodes,
        structural_summary={**structural_summary, "debug": debug},
    )

