from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class GraphNode(BaseModel):
    id: str


class GraphEdge(BaseModel):
    source: str
    target: str
    weight: float = Field(..., description="Edge weight (e.g., penalized distance)")


class GraphPayload(BaseModel):
    nodes: List[GraphNode]
    edges: List[GraphEdge]
    directed: bool = True


class AnalyzeGraphRequest(BaseModel):
    graph: GraphPayload

    # Optional parameters for anomaly sensitivity
    degree_z_threshold: float = 2.0
    edge_z_threshold: float = 2.0
    structural_change_threshold: float = 2.0
    anomaly_score_threshold: float = 2.5

    # If provided, enables structural-change comparison
    previous_graph: Optional[GraphPayload] = None


class AnalyzeNodeMetrics(BaseModel):
    degree_centrality: float
    betweenness_centrality: float
    pagerank: float
    anomaly_score: float


class AnalyzeGraphResponse(BaseModel):
    centrality_metrics: Dict[str, AnalyzeNodeMetrics]
    anomaly_scores: Dict[str, float]
    anomalous_nodes: List[str]
    summary: Dict[str, Any]


class CompareGraphsRequest(BaseModel):
    current_graph: GraphPayload
    previous_graph: GraphPayload

    degree_z_threshold: float = 2.0
    edge_z_threshold: float = 2.0
    structural_change_threshold: float = 2.0
    anomaly_score_threshold: float = 2.5


class CompareGraphsResponse(BaseModel):
    centrality_metrics_current: Dict[str, AnalyzeNodeMetrics]
    anomaly_scores: Dict[str, float]
    anomalous_nodes: List[str]
    structural_summary: Dict[str, Any]

