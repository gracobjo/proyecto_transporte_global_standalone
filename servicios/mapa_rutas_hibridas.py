"""
Mapa Folium: red completa + ruta principal + rutas alternativas (colores distintos).
"""
from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

import folium

from config_nodos import get_aristas, get_nodos


def _lineas_desde_ruta(nodos: dict, ruta: Sequence[str]) -> List[Tuple[float, float]]:
    pts: List[Tuple[float, float]] = []
    for nid in ruta:
        info = nodos.get(nid)
        if not info:
            continue
        pts.append((float(info["lat"]), float(info["lon"])))
    return pts


def crear_mapa_planificacion_rutas(
    ruta_principal: Optional[Sequence[str]],
    alternativas: List[Tuple[List[str], str]],
    *,
    mostrar_red_completa: bool = True,
    max_alternativas: int = 5,
    zoom_start: int = 6,
) -> folium.Map:
    """
    Capas (Leyenda en control de capas):
    - Red completa: aristas grises (todas las conexiones definidas).
    - Ruta principal: azul sólido.
    - Alternativas: tonos naranja/ámbar, trazo discontinuo.
    """
    nodos = get_nodos()
    m = folium.Map(location=[40.35, -3.7], zoom_start=zoom_start, tiles="OpenStreetMap")

    fg_red = folium.FeatureGroup(name="Red: todas las conexiones", show=True)
    if mostrar_red_completa:
        vistos = set()
        for src, dst, _ in get_aristas():
            key = tuple(sorted([src, dst]))
            if key in vistos:
                continue
            vistos.add(key)
            if src not in nodos or dst not in nodos:
                continue
            p1 = [nodos[src]["lat"], nodos[src]["lon"]]
            p2 = [nodos[dst]["lat"], nodos[dst]["lon"]]
            folium.PolyLine(
                [p1, p2],
                color="#94a3b8",
                weight=2,
                opacity=0.5,
                tooltip=f"{src} ↔ {dst}",
            ).add_to(fg_red)
    fg_red.add_to(m)

    fg_nodos = folium.FeatureGroup(name="Nodos (hubs y subnodos)", show=True)
    for nid, info in nodos.items():
        lat, lon = float(info["lat"]), float(info["lon"])
        es_hub = info.get("tipo") == "hub"
        folium.CircleMarker(
            location=[lat, lon],
            radius=8 if es_hub else 5,
            color="#1e40af" if es_hub else "#475569",
            fill=True,
            fill_color="#60a5fa" if es_hub else "#e2e8f0",
            fill_opacity=0.92,
            weight=2,
            tooltip=f"{'Hub' if es_hub else 'Subnodo'}: {nid}",
        ).add_to(fg_nodos)
    fg_nodos.add_to(m)

    fg_main = folium.FeatureGroup(name="Ruta principal (azul)", show=True)
    if ruta_principal and len(ruta_principal) >= 2:
        pts = _lineas_desde_ruta(nodos, ruta_principal)
        if len(pts) >= 2:
            folium.PolyLine(
                pts,
                color="#1d4ed8",
                weight=7,
                opacity=0.95,
                tooltip="Ruta principal — menor número de saltos (BFS)",
            ).add_to(fg_main)
        o, d = ruta_principal[0], ruta_principal[-1]
        if o in nodos:
            folium.Marker(
                [nodos[o]["lat"], nodos[o]["lon"]],
                tooltip=f"Origen: {o}",
                icon=folium.Icon(color="green", icon="arrow-up"),
            ).add_to(fg_main)
        if d in nodos:
            folium.Marker(
                [nodos[d]["lat"], nodos[d]["lon"]],
                tooltip=f"Destino: {d}",
                icon=folium.Icon(color="red", icon="arrow-down"),
            ).add_to(fg_main)
    fg_main.add_to(m)

    fg_alt = folium.FeatureGroup(name="Rutas alternativas (naranja, discontinuo)", show=True)
    colores_alt = ["#c2410c", "#ea580c", "#f97316", "#fb923c", "#fdba74"]
    for i, (ruta_alt, motivo) in enumerate(alternativas[:max_alternativas]):
        if len(ruta_alt) < 2:
            continue
        pts = _lineas_desde_ruta(nodos, ruta_alt)
        if len(pts) < 2:
            continue
        color = colores_alt[i % len(colores_alt)]
        folium.PolyLine(
            pts,
            color=color,
            weight=5,
            opacity=0.9,
            dash_array="12, 10",
            tooltip=f"Alt. {i + 1}: {motivo[:120]}",
        ).add_to(fg_alt)
    fg_alt.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    return m
