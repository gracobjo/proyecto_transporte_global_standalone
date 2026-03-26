"""
Rutas más cortas (Dijkstra) sobre la red gemelo con pesos dinámicos.
Spark GraphFrames puede usarse en batch; en el dashboard se usa NetworkX (baja latencia).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import networkx as nx

from servicios.gemelo_digital_grafo import haversine_km


def construir_grafo_ponderado(
    aristas: List[Dict[str, Any]],
    factor_atasco: float,
    factor_clima: float,
    distancias_por_par: Optional[Dict[Tuple[str, str], float]] = None,
) -> nx.Graph:
    """
    Peso efectivo = distancia_km * factor_atasco * factor_clima (mismos factores globales en todas las aristas).
    """
    G = nx.Graph()
    f = float(factor_atasco) * float(factor_clima)
    for e in aristas:
        a, b = e["src"], e["dst"]
        d = float(e.get("distancia_km") or 0.0)
        if distancias_por_par is not None:
            k = (a, b) if a < b else (b, a)
            d = float(distancias_por_par.get(k, d))
        w = max(d * f, 1e-9)
        G.add_edge(a, b, weight=w, distancia_km=d)
    return G


def grafo_sin_nodo(G: nx.Graph, nodo_bloqueado: str) -> nx.Graph:
    H = G.copy()
    if nodo_bloqueado in H:
        H.remove_node(nodo_bloqueado)
    return H


def dijkstra_ruta(
    G: nx.Graph,
    origen: str,
    destino: str,
) -> Tuple[List[str], float, float]:
    """
    Devuelve (nodos_path, coste_ponderado, km_geográficos).
    """
    if origen not in G or destino not in G:
        return [], float("inf"), float("inf")
    try:
        path = nx.shortest_path(G, origen, destino, weight="weight")
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        return [], float("inf"), float("inf")
    coste = 0.0
    km = 0.0
    for i in range(len(path) - 1):
        u, v = path[i], path[i + 1]
        ed = G[u][v]
        coste += float(ed.get("weight", 0.0))
        km += float(ed.get("distancia_km", 0.0))
    return path, coste, km


def nodo_mas_cercano(lat: float, lon: float, nodos: List[Dict[str, Any]]) -> str:
    best, best_id = float("inf"), ""
    for n in nodos:
        d = haversine_km(lat, lon, float(n["lat"]), float(n["lon"]))
        if d < best:
            best, best_id = d, n["id_nodo"]
    return best_id


def comparar_original_vs_bloqueo(
    G: nx.Graph,
    origen: str,
    destino: str,
    nodo_bloqueado: Optional[str],
) -> Dict[str, Any]:
    """
    Calcula la ruta mínima con pesos actuales; si hay capital bloqueada y cae en la ruta (o es intermedio),
    recalcula sin ese nodo y devuelve métricas comparadas.
    """
    p0, c0, km0 = dijkstra_ruta(G, origen, destino)
    out: Dict[str, Any] = {
        "ruta_original": p0,
        "coste_original": c0,
        "km_original": km0,
        "ruta_activa": p0,
        "coste_activo": c0,
        "km_activo": km0,
        "recalculado": False,
        "ruta_alternativa": [],
        "coste_alternativa": float("inf"),
        "km_alternativa": float("inf"),
    }
    if not nodo_bloqueado or not p0:
        return out

    if nodo_bloqueado not in p0:
        return out

    H = grafo_sin_nodo(G, nodo_bloqueado)
    p1, c1, km1 = dijkstra_ruta(H, origen, destino)
    out["recalculado"] = True
    out["ruta_alternativa"] = p1
    out["coste_alternativa"] = c1
    out["km_alternativa"] = km1
    out["ruta_activa"] = p1 if p1 else p0
    out["coste_activo"] = c1 if p1 else c0
    out["km_activo"] = km1 if p1 else km0
    return out


def tiempo_estimado_horas(km_geo: float, coste_ponderado: float, velocidad_kmh: float = 75.0) -> float:
    """
    Tiempo base por km geográfico, escalado por el ratio coste_ponderado / km_geo cuando km_geo > 0.
    Refleja que mayor peso (atasco+clima) implica más tiempo efectivo.
    """
    if km_geo <= 0:
        return 0.0
    ratio = coste_ponderado / km_geo if coste_ponderado > 0 else 1.0
    return (km_geo * ratio) / max(velocidad_kmh, 1e-6)
