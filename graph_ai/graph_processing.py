from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import networkx as nx

from graph_ai.models import GraphPayload


@dataclass(frozen=True)
class AnomalyParams:
    degree_z_threshold: float = 2.0
    edge_z_threshold: float = 2.0
    structural_change_threshold: float = 2.0
    anomaly_score_threshold: float = 2.5


def _pagerank_fallback(g: nx.DiGraph | nx.Graph, alpha: float = 0.85, max_iter: int = 100, tol: float = 1e-6) -> Dict[str, float]:
    nodes = list(g.nodes())
    if not nodes:
        return {}
    n = len(nodes)
    rank = {node: 1.0 / n for node in nodes}
    if g.number_of_edges() == 0:
        return rank

    successors = {}
    if g.is_directed():
        for node in nodes:
            successors[node] = list(g.successors(node))
    else:
        for node in nodes:
            successors[node] = list(g.neighbors(node))

    for _ in range(max_iter):
        prev = rank.copy()
        dangling_sum = sum(prev[node] for node in nodes if not successors[node])
        for node in nodes:
            incoming = g.predecessors(node) if g.is_directed() else g.neighbors(node)
            score = (1.0 - alpha) / n
            score += alpha * dangling_sum / n
            for src in incoming:
                out_degree = len(successors[src]) or n
                score += alpha * prev[src] / out_degree
            rank[node] = score
        err = sum(abs(rank[node] - prev[node]) for node in nodes)
        if err < tol:
            break
    return {k: float(v) for k, v in rank.items()}


def build_nx_graph(payload: GraphPayload) -> nx.DiGraph | nx.Graph:
    if payload.directed:
        g: nx.DiGraph | nx.Graph = nx.DiGraph()
    else:
        g = nx.Graph()

    for n in payload.nodes:
        g.add_node(n.id)

    for e in payload.edges:
        g.add_edge(e.source, e.target, weight=float(e.weight))

    return g


def _safe_mean_std(values: List[float]) -> Tuple[float, float]:
    if not values:
        return 0.0, 0.0
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / len(values)
    std = math.sqrt(var)
    return mean, std


def _z_scores(values: List[float]) -> Dict[int, float]:
    mean, std = _safe_mean_std(values)
    if std == 0.0:
        return {i: 0.0 for i in range(len(values))}
    return {i: (v - mean) / std for i, v in enumerate(values)}


def compute_centralities(g: nx.DiGraph | nx.Graph) -> Dict[str, Dict[str, float]]:
    # Degree centrality (normalizado por definición de NetworkX)
    degree_cent = nx.degree_centrality(g)

    # Betweenness centrality puede ser costoso: usar aproximación con k cuando el grafo crece.
    n = g.number_of_nodes()
    if n <= 200:
        betweenness = nx.betweenness_centrality(g, weight="weight", normalized=True)
    else:
        k = min(50, n)
        betweenness = nx.betweenness_centrality(
            g,
            k=k,
            weight="weight",
            normalized=True,
            seed=42,
        )

    if g.number_of_edges() > 0:
        try:
            pagerank = nx.pagerank(g, weight="weight")
        except ModuleNotFoundError:
            # Fallback sin scipy para entornos ligeros de pruebas/desarrollo.
            pagerank = _pagerank_fallback(g)
    else:
        pagerank = {k: 0.0 for k in g.nodes()}

    out: Dict[str, Dict[str, float]] = {}
    for node in g.nodes():
        out[node] = {
            "degree_centrality": float(degree_cent.get(node, 0.0)),
            "betweenness_centrality": float(betweenness.get(node, 0.0)),
            "pagerank": float(pagerank.get(node, 0.0)),
        }
    return out


def detect_anomalies(
    g: nx.DiGraph | nx.Graph,
    centralities: Dict[str, Dict[str, float]],
    params: AnomalyParams,
    previous_g: Optional[nx.DiGraph | nx.Graph] = None,
) -> Tuple[Dict[str, float], Dict[str, Any]]:
    """
    Returns:
      - anomaly_scores: node_id -> score
      - debug: detalles de qué disparó cada anomalía (útil para trazas)
    """
    node_ids = list(g.nodes())
    anomaly_scores: Dict[str, float] = {n: 0.0 for n in node_ids}
    debug: Dict[str, Any] = {"isolated": [], "edge_weight_outliers": [], "degree_outliers": [], "structural_changes": []}

    # --- Aislados ---
    for n in node_ids:
        # En DiGraph, grado total = in_degree + out_degree
        if g.degree(n) == 0:
            anomaly_scores[n] += 5.0
            debug["isolated"].append(n)

    # --- Outliers por grado (centralidad) ---
    degrees = [centralities[n]["degree_centrality"] for n in node_ids]
    z_degree_by_idx = _z_scores(degrees)
    for idx, n in enumerate(node_ids):
        z = z_degree_by_idx[idx]
        if abs(z) >= params.degree_z_threshold:
            anomaly_scores[n] += abs(z) * 2.0
            debug["degree_outliers"].append({"node": n, "z": z, "degree_centrality": degrees[idx]})

    # --- Outliers por peso en aristas ---
    edges = list(g.edges(data=True))
    weights = [float(d.get("weight", 1.0)) for _, _, d in edges]
    if weights:
        z_edge = _z_scores(weights)
        # Mapear anomalías de arista a nodos incidentes
        for idx, (u, v, d) in enumerate(edges):
            z = z_edge[idx]
            if abs(z) >= params.edge_z_threshold:
                w = weights[idx]
                debug["edge_weight_outliers"].append({"edge": [u, v], "z": z, "weight": w})
                anomaly_scores[u] += abs(z)
                anomaly_scores[v] += abs(z)

    # --- Cambios estructurales (comparar con snapshot anterior) ---
    if previous_g is not None:
        prev_centralities = compute_centralities(previous_g)

        # Delta por nodo (usamos degree centrality + pagerank)
        degree_deltas: List[float] = []
        for n in node_ids:
            degree_deltas.append(centralities[n]["degree_centrality"] - prev_centralities.get(n, {}).get("degree_centrality", 0.0))

        z_delta_degree_by_idx = _z_scores(degree_deltas)
        for idx, n in enumerate(node_ids):
            z = z_delta_degree_by_idx[idx]
            if abs(z) >= params.structural_change_threshold:
                anomaly_scores[n] += abs(z) * 2.0
                debug["structural_changes"].append(
                    {
                        "node": n,
                        "z_delta_degree": z,
                        "delta_degree_centrality": degree_deltas[idx],
                    }
                )

    # Post-ajuste: normalizar un poco para evitar scores enormes por grafos grandes
    # (sin alterar ranking relativo de anomalía)
    # sqrt() comprime valores altos.
    for n in anomaly_scores:
        anomaly_scores[n] = float(math.sqrt(max(0.0, anomaly_scores[n])))

    anomalous_nodes = [n for n, score in anomaly_scores.items() if score >= params.anomaly_score_threshold]
    anomalous_nodes.sort(key=lambda n: anomaly_scores[n], reverse=True)

    debug["anomalous_nodes"] = anomalous_nodes
    return anomaly_scores, debug


def analyze_graph_payload(request_graph: GraphPayload, params: AnomalyParams, previous_graph: Optional[GraphPayload] = None):
    g = build_nx_graph(request_graph)
    centralities = compute_centralities(g)
    previous_g_nx = build_nx_graph(previous_graph) if previous_graph is not None else None

    anomaly_scores, debug = detect_anomalies(g, centralities, params, previous_g=previous_g_nx)

    anomalous_nodes = [n for n, score in anomaly_scores.items() if score >= params.anomaly_score_threshold]
    anomalous_nodes.sort(key=lambda n: anomaly_scores[n], reverse=True)

    centrality_metrics: Dict[str, Dict[str, float]] = {}
    for node_id, c in centralities.items():
        centrality_metrics[node_id] = {**c, "anomaly_score": float(anomaly_scores.get(node_id, 0.0))}

    return centrality_metrics, anomaly_scores, anomalous_nodes, debug


def compare_graphs(
    current: GraphPayload,
    previous: GraphPayload,
    params: AnomalyParams,
):
    current_g = build_nx_graph(current)
    previous_g = build_nx_graph(previous)

    current_c = compute_centralities(current_g)
    previous_c = compute_centralities(previous_g)

    anomaly_scores, debug = detect_anomalies(current_g, current_c, params, previous_g=previous_g)

    anomalous_nodes = [n for n, score in anomaly_scores.items() if score >= params.anomaly_score_threshold]
    anomalous_nodes.sort(key=lambda n: anomaly_scores[n], reverse=True)

    centrality_metrics_current: Dict[str, Dict[str, float]] = {}
    for node_id, c in current_c.items():
        centrality_metrics_current[node_id] = {**c, "anomaly_score": float(anomaly_scores.get(node_id, 0.0))}

    # Resumen de cambios de estructura (rápido, no ML)
    structural_summary = {
        "nodes_current": current_g.number_of_nodes(),
        "nodes_previous": previous_g.number_of_nodes(),
        "edges_current": current_g.number_of_edges(),
        "edges_previous": previous_g.number_of_edges(),
    }

    # Delta de centralidades: solo resumen (por coste)
    pagerank_deltas = [current_c[n]["pagerank"] - previous_c.get(n, {}).get("pagerank", 0.0) for n in current_g.nodes()]
    if pagerank_deltas:
        structural_summary["pagerank_delta_abs_mean"] = float(sum(abs(x) for x in pagerank_deltas) / len(pagerank_deltas))

    return centrality_metrics_current, anomaly_scores, anomalous_nodes, structural_summary, debug

