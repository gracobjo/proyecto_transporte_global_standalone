"""
Vista de grafo para fases KDD de procesamiento (3–5): topología lógica, no mapa geográfico.
Usa la misma red que `config_nodos` / GraphFrames; opcionalmente colorea nodos con Cassandra.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import altair as alt
import pandas as pd

from config_nodos import get_aristas, get_nodos


def _estado_nodo_cassandra(nodos_c: List[Dict[str, Any]], nid: str) -> str:
    for r in nodos_c:
        if r.get("id_nodo") == nid:
            return str(r.get("estado") or "—")
    return "—"


def _pagerank_por_nodo(pr_rows: List[Dict[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for r in pr_rows:
        nid = r.get("id_nodo")
        if nid is None:
            continue
        try:
            v = float(r.get("pagerank") or 0.0)
        except (TypeError, ValueError):
            v = 0.0
        out[str(nid)] = v
    return out


def construir_chart_grafo_topologia(
    *,
    nodos_cassandra: Optional[List[Dict[str, Any]]] = None,
    pagerank_rows: Optional[List[Dict[str, Any]]] = None,
    titulo: str = "Topología de red (vista grafo)",
    ancho: int = 720,
    alto: int = 460,
) -> Tuple[Optional[alt.Chart], str]:
    """
    Devuelve (chart Altair o None, mensaje si no se pudo construir).
    """
    try:
        import networkx as nx
    except ImportError:
        return None, "Instala **networkx** (`pip install networkx`) para la vista grafo."

    nodos = get_nodos()
    aristas = get_aristas()
    nodos_c = nodos_cassandra or []
    pr_map = _pagerank_por_nodo(pagerank_rows or [])

    G = nx.Graph()
    for nid in nodos:
        G.add_node(nid)
    for src, dst, _ in aristas:
        if src in nodos and dst in nodos:
            G.add_edge(src, dst)

    if G.number_of_nodes() == 0:
        return None, "No hay nodos en la topología configurada."

    pos = nx.spring_layout(G, seed=42, k=1.8 / max(1, (G.number_of_nodes() ** 0.5)), iterations=55)

    rows_e: List[Dict[str, Any]] = []
    for ei, (u, v) in enumerate(G.edges()):
        eid = f"e{ei}"
        rows_e.append({"eid": eid, "x": float(pos[u][0]), "y": float(pos[u][1]), "ord": 0})
        rows_e.append({"eid": eid, "x": float(pos[v][0]), "y": float(pos[v][1]), "ord": 1})
    df_e = pd.DataFrame(rows_e)

    rows_n: List[Dict[str, Any]] = []
    for nid in G.nodes():
        info = nodos[nid]
        est = _estado_nodo_cassandra(nodos_c, nid)
        pr = pr_map.get(nid, 0.0)
        rows_n.append(
            {
                "id": nid,
                "x": float(pos[nid][0]),
                "y": float(pos[nid][1]),
                "tipo": info.get("tipo", "—"),
                "estado_op": est,
                "pagerank": pr,
            }
        )
    df_n = pd.DataFrame(rows_n)

    base = alt.Chart(df_e).mark_line(color="#94a3b8", strokeWidth=1.4, opacity=0.85).encode(
        x=alt.X("x:Q", axis=None, scale=alt.Scale(zero=False)),
        y=alt.Y("y:Q", axis=None, scale=alt.Scale(zero=False)),
        detail="eid:N",
    )

    pr_max = float(df_n["pagerank"].max()) if len(df_n) else 0.0
    use_pr_size = bool(pagerank_rows) and pr_max > 1e-12

    enc: Dict[str, Any] = {
        "x": alt.X("x:Q", axis=None),
        "y": alt.Y("y:Q", axis=None),
        "color": alt.Color("estado_op:N", legend=alt.Legend(title="Estado nodo")),
        "tooltip": ["id", "tipo", "estado_op", alt.Tooltip("pagerank:Q", format=".4f")],
    }
    if use_pr_size:
        enc["size"] = alt.Size(
            "pagerank:Q",
            scale=alt.Scale(range=[140, 820]),
            legend=alt.Legend(title="PageRank"),
        )

    nodos_mrk = alt.Chart(df_n).mark_circle(
        stroke="white",
        strokeWidth=2,
        **({} if use_pr_size else {"size": 300}),
    ).encode(**enc)

    etiquetas = (
        alt.Chart(df_n)
        .mark_text(dy=-14, fontSize=9, fontWeight="normal", color="#1e293b")
        .encode(x="x:Q", y="y:Q", text="id:N")
    )

    chart = (
        (base + nodos_mrk + etiquetas)
        .configure_view(stroke=None)
        .properties(width=ancho, height=alto, title=titulo)
        .configure_title(fontSize=16, anchor="start")
    )

    return chart, ""


def render_bloque_grafo_fases_spark(
    st: Any,
    *,
    orden_fase: int,
    nodos_cassandra: Optional[List[Dict[str, Any]]] = None,
    pagerank_rows: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """Llamar desde Streamlit cuando la fase es transformación / minería / interpretación."""
    st.subheader("Topología lógica de la red (una sola figura para fases 3–5)")
    st.caption(
        "Es la **misma** red en fases 3, 4 y 5: layout **topológico** (no coordenadas reales). "
        "**GraphFrames** en Spark usa estos mismos nodos y aristas. Color = estado en Cassandra si existe; "
        "tamaño del nodo = **PageRank** solo cuando hay datos (típ. tras fase 4+). "
        "Mapas **geográficos** de la misma red: pestañas **Mapa y métricas** y **Rutas híbridas**."
    )

    # Un título de gráfico estable: no sugerir tres “grafos distintos” al cambiar de fase.
    chart, err = construir_chart_grafo_topologia(
        nodos_cassandra=nodos_cassandra,
        pagerank_rows=pagerank_rows if orden_fase >= 4 else None,
        titulo="Red logística — topología (KDD 3–5, alineada con GraphFrames)",
    )
    if chart is None:
        st.warning(err)
        return
    st.altair_chart(chart, width="stretch")
