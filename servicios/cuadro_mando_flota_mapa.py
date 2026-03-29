"""
Mapa operativo multi-camión para el cuadro de mando: rutas origen→destino y estado (retrasos).
"""
from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import folium

from config_nodos import get_nodos

# Colores por índice de asignación (línea planificada)
_COLORES_RUTA = (
    "#2563eb",
    "#059669",
    "#d97706",
    "#7c3aed",
    "#db2777",
    "#0e7490",
    "#b45309",
)


def _desplazar_pin_si_superpuesto(
    lat: float,
    lon: float,
    indice_mismo_lugar: int,
) -> Tuple[float, float]:
    """
    Separa ligeramente marcadores que comparten casi las mismas coordenadas
    (varios camiones en el mismo hub): si no, solo se ve un pin en Folium.
    """
    if indice_mismo_lugar <= 0:
        return lat, lon
    km = 0.28 * float(indice_mismo_lugar)
    ang = math.radians((indice_mismo_lugar * 67) % 360)
    cos_lat = max(0.25, abs(math.cos(math.radians(lat))))
    dlat = (km / 111.0) * math.cos(ang)
    dlon = (km / (111.0 * cos_lat)) * math.sin(ang)
    return lat + dlat, lon + dlon


def clasificar_camion(row: Dict[str, Any]) -> Tuple[str, str]:
    """
    Devuelve (nivel, detalle) con nivel en 'ok' | 'aviso' | 'retraso'.
    """
    motivo = (row.get("motivo_retraso") or "").strip()
    est = (row.get("estado_ruta") or "").strip().lower()
    if motivo:
        return "retraso", motivo
    if not est:
        return "ok", ""
    if est in ("en ruta", "ok", "operativo", "finalizada"):
        return "ok", ""
    if "retras" in est or "bloque" in est or "incid" in est:
        return "retraso", est
    if "congest" in est:
        return "aviso", est
    return "aviso", est


def construir_mapa_flota_asignaciones(
    asignaciones: List[Dict[str, Any]],
    tracking: List[Dict[str, Any]],
    *,
    nodos_static: Optional[Dict[str, Any]] = None,
) -> folium.Map:
    """
    `asignaciones`: lista de {camion, origen, destino, id?}.
    `tracking`: filas Cassandra tracking_camiones (id_camion, lat, lon, ...).
    Dibuja una polilínea origen→destino por asignación y marcadores de camión con color según retraso.
    """
    nodos = nodos_static or get_nodos()
    by_id = {str(t.get("id_camion")): t for t in tracking if t.get("id_camion")}

    m = folium.Map(location=[40.4, -3.7], zoom_start=6, tiles="CartoDB positron")
    fg_rutas = folium.FeatureGroup(name="Rutas planificadas (origen → destino)", show=True)
    fg_cam = folium.FeatureGroup(name="Camiones (posición)", show=True)
    fg_ley = folium.FeatureGroup(name="Origen / destino planificados", show=True)

    puntos_fit: List[List[float]] = []
    # Contador por celda ~11 m para separar pins superpuestos (mismo origen / misma posición)
    contador_pos: Dict[Tuple[int, int], int] = defaultdict(int)

    for i, a in enumerate(asignaciones):
        cid = str(a.get("camion") or "").strip()
        o, d = str(a.get("origen") or "").strip(), str(a.get("destino") or "").strip()
        col = _COLORES_RUTA[i % len(_COLORES_RUTA)]
        row_tr = by_id.get(cid) if cid else None
        ruta_nodos: List[str] = []
        if row_tr and isinstance(row_tr.get("ruta_sugerida"), (list, tuple)):
            ruta_nodos = [str(x) for x in row_tr["ruta_sugerida"] if x]
        coords_ruta: List[List[float]] = []
        if len(ruta_nodos) >= 2:
            for nodo in ruta_nodos:
                if nodo in nodos:
                    coords_ruta.append([float(nodos[nodo]["lat"]), float(nodos[nodo]["lon"])])
        elif o in nodos and d in nodos:
            coords_ruta = [
                [float(nodos[o]["lat"]), float(nodos[o]["lon"])],
                [float(nodos[d]["lat"]), float(nodos[d]["lon"])],
            ]
        if coords_ruta:
            la_o, lo_o = coords_ruta[0][0], coords_ruta[0][1]
            la_d, lo_d = coords_ruta[-1][0], coords_ruta[-1][1]
            folium.PolyLine(
                coords_ruta,
                color=col,
                weight=4,
                opacity=0.75,
                dash_array="10, 12",
                tooltip=f"{cid or '?'}: {o} → {d}" + (f" ({len(ruta_nodos)} nodos)" if len(ruta_nodos) >= 2 else ""),
            ).add_to(fg_rutas)
            folium.CircleMarker(
                [la_o, lo_o],
                radius=7,
                color=col,
                fill=True,
                fill_opacity=0.9,
                popup=f"<b>Origen</b> {cid}<br>{o}",
            ).add_to(fg_ley)
            folium.CircleMarker(
                [la_d, lo_d],
                radius=7,
                color=col,
                fill=True,
                fill_opacity=0.5,
                popup=f"<b>Destino</b> {cid}<br>{d}",
            ).add_to(fg_ley)
            puntos_fit.extend(coords_ruta)

        row = by_id.get(cid) if cid else None
        if row:
            lat, lon = row.get("lat"), row.get("lon")
            if lat is not None and lon is not None:
                nivel, detalle = clasificar_camion(row)
                if nivel == "retraso":
                    color_icon = "red"
                elif nivel == "aviso":
                    color_icon = "orange"
                else:
                    color_icon = "green"
                la_f, lo_f = float(lat), float(lon)
                cell = (round(la_f * 9000), round(lo_f * 9000))
                idx_spot = contador_pos[cell]
                contador_pos[cell] += 1
                la_m, lo_m = _desplazar_pin_si_superpuesto(la_f, lo_f, idx_spot)
                popup_html = (
                    f"<b>{cid}</b><br>"
                    f"Plan: {o} → {d}<br>"
                    f"Estado ruta: {row.get('estado_ruta') or '—'}<br>"
                    f"Motivo: {detalle or '—'}<br>"
                    f"GPS (Cassandra): {la_f:.5f}, {lo_f:.5f}"
                    + (f"<br><small>Pin desplazado si varios camiones comparten posición.</small>" if idx_spot > 0 else "")
                    + "<br>"
                    f"Origen/destino (Cassandra): {row.get('ruta_origen') or '—'} → {row.get('ruta_destino') or '—'}"
                )
                folium.Marker(
                    [la_m, lo_m],
                    icon=folium.Icon(color=color_icon, icon="info-sign"),
                    popup=folium.Popup(popup_html, max_width=320),
                    tooltip=f"{cid} ({la_f:.4f}, {lo_f:.4f})",
                ).add_to(fg_cam)
                puntos_fit.append([la_m, lo_m])
        elif cid and o in nodos:
            # Sin GPS: marcar aviso en origen planificado
            la_f = float(nodos[o]["lat"])
            lo_f = float(nodos[o]["lon"])
            cell = (round(la_f * 9000), round(lo_f * 9000))
            idx_spot = contador_pos[cell]
            contador_pos[cell] += 1
            la_m, lo_m = _desplazar_pin_si_superpuesto(la_f, lo_f, idx_spot)
            folium.Marker(
                [la_m, lo_m],
                icon=folium.Icon(color="gray", icon="question-sign"),
                popup=f"Sin posición en tracking para <b>{cid}</b><br>Ruta plan: {o} → {d}",
                tooltip=cid,
            ).add_to(fg_cam)
            puntos_fit.append([la_m, lo_m])

    fg_rutas.add_to(m)
    fg_ley.add_to(m)
    fg_cam.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)

    if len(puntos_fit) >= 1:
        lats = [p[0] for p in puntos_fit]
        lons = [p[1] for p in puntos_fit]
        pad = 0.6
        m.fit_bounds(
            [[min(lats) - pad, min(lons) - pad], [max(lats) + pad, max(lons) + pad]],
            padding=(24, 24),
        )

    return m


def tabla_incidencias_rows(asignaciones: List[Dict[str, Any]], tracking: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filas para st.dataframe: camión, plan, estado, motivo, nivel."""
    by_id = {str(t.get("id_camion")): t for t in tracking if t.get("id_camion")}
    out: List[Dict[str, Any]] = []
    for a in asignaciones:
        cid = str(a.get("camion") or "").strip()
        row = by_id.get(cid)
        if row:
            nivel, detalle = clasificar_camion(row)
        else:
            nivel, detalle = "aviso", "sin telemetría en Cassandra"
        out.append(
            {
                "camión": cid,
                "origen → destino (plan)": f"{a.get('origen')} → {a.get('destino')}",
                "estado_ruta": row.get("estado_ruta") or "—",
                "motivo_retraso": row.get("motivo_retraso") or detalle,
                "nivel": nivel,
            }
        )
    return out


def texto_resumen_correo(asignaciones: List[Dict[str, Any]], tracking: List[Dict[str, Any]]) -> str:
    lines = ["SIMLOG — Resumen flota y rutas", ""]
    for a in asignaciones:
        cid = str(a.get("camion") or "").strip()
        row = next((t for t in tracking if str(t.get("id_camion")) == cid), None)
        lines.append(f"Camión: {cid}")
        lines.append(f"  Ruta planificada: {a.get('origen')} → {a.get('destino')}")
        if row:
            nivel, detalle = clasificar_camion(row)
            lines.append(f"  Estado: {nivel} | estado_ruta: {row.get('estado_ruta')}")
            lines.append(f"  Motivo: {detalle or row.get('motivo_retraso') or '—'}")
        else:
            lines.append("  (Sin fila en tracking_cassandra)")
        lines.append("")
    return "\n".join(lines)
