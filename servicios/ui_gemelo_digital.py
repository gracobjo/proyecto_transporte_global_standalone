"""
Gemelo digital logístico — mapa Folium, incidencias (sliders), Dijkstra, ingesta CSV.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import folium
import pandas as pd
import streamlit as st
from folium.plugins import AntPath
from streamlit_folium import st_folium

from servicios.gemelo_digital_datos import (
    cargar_red_gemelo,
    cargar_tracking_gemelo_cassandra,
    cargar_transporte_ingesta_real_hive,
    cargar_historial_tracking_hive,
    cargar_trayectorias_por_camion,
)
from servicios.gemelo_digital_rutas import (
    comparar_original_vs_bloqueo,
    construir_grafo_ponderado,
    nodo_mas_cercano,
    tiempo_estimado_horas,
)
from servicios.red_hibrida_rutas import estimar_retraso_tramo, COSTE_EURO_MINUTO_RETASO
from servicios.ingesta_csv_gemelo import ingerir_csv_gemelo


def _safe_float_coord(v: Any) -> float:
    if v is None or v == "":
        return float("nan")
    try:
        return float(v)
    except (TypeError, ValueError):
        return float("nan")


def _dataframe_arrow_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Evita ArrowTypeError por columnas object con int/float/str mezclados (p. ej. lat)."""
    out = df.copy()
    for c in out.columns:
        if out[c].dtype == object:
            num = pd.to_numeric(out[c], errors="coerce")
            if num.notna().any():
                out[c] = num
    return out


def _cargar_retrasos_cassandra(
    nodos: List[Dict[str, Any]],
    aristas: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Carga nodos/aristas desde Cassandra y calcula retrasos por tramo."""
    try:
        from cassandra.cluster import Cluster
        from config import CASSANDRA_HOST, KEYSPACE

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(KEYSPACE)

        rows_nodos = list(session.execute("SELECT id_nodo, estado, motivo_retraso FROM nodos_estado"))
        rows_aristas = list(session.execute("SELECT src, dst, estado FROM aristas_estado"))

        cluster.shutdown()

        nodos_map = {}
        for r in rows_nodos:
            d = dict(r._asdict()) if hasattr(r, "_asdict") else dict(r)
            nodos_map[d["id_nodo"]] = d

        aristas_map = {}
        for r in rows_aristas:
            d = dict(r._asdict()) if hasattr(r, "_asdict") else dict(r)
            key = (d.get("src", ""), d.get("dst", ""))
            aristas_map[key] = d

        retrasos = {}
        for e in aristas:
            src, dst = e.get("src"), e.get("dst")
            if not src or not dst:
                continue
            key = f"{src}|{dst}"
            resultado = estimar_retraso_tramo(src, dst, nodos_map, aristas_map, None)
            retrasos[key] = resultado

        return retrasos
    except Exception:
        return {}


def render_gemelo_digital_sidebar() -> None:
    """Controles de incidencias (sidebar global)."""
    with st.expander("Gemelo digital — incidencias", expanded=False):
        st.slider(
            "Nivel de atasco (× coste arista)",
            min_value=1.0,
            max_value=5.0,
            value=1.0,
            step=0.1,
            key="gemelo_slider_atasco",
            help="Multiplica la distancia efectiva (congestión).",
        )
        st.slider(
            "Inclemencia climática (× coste arista)",
            min_value=1.0,
            max_value=3.0,
            value=1.0,
            step=0.1,
            key="gemelo_slider_clima",
            help="Penalización global por condiciones meteorológicas.",
        )
        st.checkbox(
            "Bloqueo de capital (simular caída de una capital)",
            value=False,
            key="gemelo_bloqueo_activo",
        )
        st.caption(
            "Peso arista = distancia × atasco × clima. "
            "El recálculo de ruta (Dijkstra) usa **NetworkX** en memoria "
            "(equivalente funcional a un job Spark/GraphX por lotes)."
        )


def _crear_mapa_gemelo(
    nodos: List[Dict[str, Any]],
    aristas: List[Dict[str, Any]],
    tracking: List[Dict[str, Any]],
    ruta1: Optional[List[str]] = None,
    ruta2: Optional[List[str]] = None,
    retrasos_tramos: Optional[Dict[str, Dict[str, Any]]] = None,
    trayectorias: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    camion_seleccionado: Optional[str] = None,
    *,
    animar_trayectorias: bool = False,
) -> folium.Map:
    m = folium.Map(location=[40.2, -3.8], zoom_start=6, tiles="CartoDB positron")
    fg_nodos = folium.FeatureGroup(name="Nodos", show=True)
    fg_aristas = folium.FeatureGroup(name="Aristas (carga de red)", show=False)
    fg_rutas_plan = folium.FeatureGroup(name="Rutas planificadas (Dijkstra)", show=True)
    fg_tray = folium.FeatureGroup(name="Trayectorias históricas", show=True)
    fg_cam = folium.FeatureGroup(name="Camiones (posición)", show=True)
    pos: Dict[str, Tuple[float, float]] = {}
    for n in nodos:
        nid = str(n.get("id_nodo", ""))
        if not nid:
            continue
        lat, lon = float(n["lat"]), float(n["lon"])
        pos[nid] = (lat, lon)
        color = "red" if (n.get("tipo") or "").lower() == "capital" else "blue"
        popup_texto = nid
        if n.get("estado") and n.get("estado") != "OK":
            popup_texto = f"{nid}\nEstado: {n.get('estado')}\nMotivo: {n.get('motivo_retraso', 'N/A')}"
        folium.CircleMarker(
            location=[lat, lon],
            radius=5 if color == "blue" else 7,
            color=color,
            fill=True,
            fill_opacity=0.85,
            popup=folium.Popup(popup_texto, max_width=300),
            tooltip=nid,
        ).add_to(fg_nodos)

    retrasos = retrasos_tramos or {}
    for e in aristas:
        a, b = e.get("src"), e.get("dst")
        if not a or not b or a not in pos or b not in pos:
            continue
        key = f"{a}|{b}"
        r = retrasos.get(key, {})
        minutos = r.get("minutos", 0)
        coste_eur = r.get("coste_eur")
        motivos = r.get("motivos", [])
        
        if minutos > 0 and minutos < 1000:
            color_arista = "orange" if minutos < 30 else "red"
            peso = 3 if minutos < 30 else 5
            tooltip_parts = [f"{a} → {b}", f"Retraso: ~{minutos:.0f} min"]
            if coste_eur is not None:
                tooltip_parts.append(f"Coste: {coste_eur:.2f} €")
            if motivos:
                tooltip_parts.append(f"Motivo: {'; '.join(motivos[:2])}")
            tooltip_texto = "\n".join(tooltip_parts)
        elif minutos >= 1000:
            color_arista = "darkred"
            peso = 5
            tooltip_texto = f"{a} → {b}\nBLOQUEADO"
        else:
            color_arista = "gray"
            peso = 1
            distancia = e.get("distancia_km", 0)
            tooltip_texto = f"{a} → {b}\n{distancia:.1f} km"
        
        folium.PolyLine(
            [list(pos[a]), list(pos[b])],
            color=color_arista,
            weight=peso,
            opacity=0.7,
            tooltip=tooltip_texto,
        ).add_to(fg_aristas)

    def dibujar_ruta(ruta: List[str], color: str, label: str, mostrar_coste: bool = False) -> None:
        if len(ruta) < 2:
            return
        pts = [[pos[x][0], pos[x][1]] for x in ruta if x in pos]
        if len(pts) < 2:
            return
        tooltip_label = label
        if mostrar_coste and retrasos:
            total_min = 0.0
            total_eur = 0.0
            for i in range(len(ruta) - 1):
                key = f"{ruta[i]}|{ruta[i+1]}"
                r = retrasos.get(key, {})
                total_min += r.get("minutos", 0) or 0
                ce = r.get("coste_eur")
                if ce is not None:
                    total_eur += ce
            if total_min > 0:
                tooltip_label = f"{label}\nRetraso total: ~{total_min:.0f} min"
                if total_eur > 0:
                    tooltip_label += f" | Coste: {total_eur:.2f} €"
        folium.PolyLine(pts, color=color, weight=4, opacity=0.85, tooltip=tooltip_label).add_to(fg_rutas_plan)

    if ruta1:
        dibujar_ruta(ruta1, "green", "Ruta referencia", mostrar_coste=True)
    if ruta2:
        dibujar_ruta(ruta2, "orange", "Ruta alternativa", mostrar_coste=True)

    colores_trayectoria = ["blue", "purple", "darkgreen", "darkorange", "darkblue", "pink", "brown", "gray"]
    tray = trayectorias or {}
    mostrar_todas = not camion_seleccionado
    
    for idx, (cid, puntos) in enumerate(tray.items()):
        if not mostrar_todas and cid != camion_seleccionado:
            continue
        if len(puntos) < 2:
            continue
        
        color_track = colores_trayectoria[idx % len(colores_trayectoria)]
        pts_track = [[p["lat"], p["lon"]] for p in puntos if p.get("lat") and p.get("lon")]
        
        if len(pts_track) >= 2:
            tooltip_track = f"Trayectoria {cid}"
            if puntos[0].get("origen") and puntos[-1].get("destino"):
                tooltip_track += f"\n{puntos[0]['origen']} → {puntos[-1]['destino']}"
            tooltip_track += f"\n{puntos[0]['timestamp'][:19] if puntos[0].get('timestamp') else '?'} (inicio)"
            
            if animar_trayectorias and len(pts_track) >= 2:
                AntPath(
                    pts_track,
                    color=color_track,
                    weight=4,
                    opacity=0.75,
                    delay=500,
                    dash_array=[8, 14],
                    tooltip=tooltip_track,
                ).add_to(fg_tray)
            else:
                folium.PolyLine(
                    pts_track,
                    color=color_track,
                    weight=3,
                    opacity=0.8,
                    tooltip=tooltip_track,
                    dash_array="5, 10" if cid == camion_seleccionado else None,
                ).add_to(fg_tray)

            inicio = pts_track[0]
            fin = pts_track[-1]

            folium.Marker(
                inicio,
                icon=folium.DivIcon(html=f'<div style="font-size:12px;color:{color_track}">▶ {cid}</div>'),
                tooltip=f"{cid} - Inicio",
            ).add_to(fg_tray)

            folium.Marker(
                fin,
                icon=folium.DivIcon(html=f'<div style="font-size:12px;color:{color_track}">■ {cid}</div>'),
                tooltip=f"{cid} - Fin (actual)",
            ).add_to(fg_tray)

    for t in tracking:
        lat, lon = t.get("lat"), t.get("lon")
        if lat is None or lon is None:
            continue
        cid = str(t.get("id_camion", "?"))
        estado = t.get("estado_ruta", "")
        tooltip_camion = f"Camión {cid}"
        if estado and estado != "En ruta":
            tooltip_camion += f"\nEstado: {estado}"
        if t.get("motivo_retraso"):
            tooltip_camion += f"\nMotivo: {t['motivo_retraso']}"
        
        es_seleccionado = cid == camion_seleccionado
        color_icono = "red" if es_seleccionado else "blue"
        
        folium.Marker(
            [float(lat), float(lon)],
            icon=folium.DivIcon(html=f'<div style="font-size:24px;color:{color_icono}" title="{tooltip_camion}">🚛</div>'),
            tooltip=tooltip_camion,
        ).add_to(fg_cam)

    fg_aristas.add_to(m)
    fg_rutas_plan.add_to(m)
    fg_tray.add_to(m)
    fg_nodos.add_to(m)
    fg_cam.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)
    return m


def render_gemelo_digital_tab() -> None:
    st.subheader("Gemelo digital logístico (España)")
    st.caption(
        "Red estática (Hive/HDFS o generador local), flota en **Cassandra** "
        "(`id_camion`, `lat`, `lon`, `ultima_posicion`), histórico Hive `transporte_ingesta_real`."
    )

    nodos, aristas, fuente = cargar_red_gemelo()
    st.caption(f"Fuente de red: **{fuente}** · Nodos: **{len(nodos)}** · Aristas: **{len(aristas)}**")

    atasco = float(st.session_state.get("gemelo_slider_atasco", 1.0))
    clima = float(st.session_state.get("gemelo_slider_clima", 1.0))
    bloqueo_on = bool(st.session_state.get("gemelo_bloqueo_activo", False))

    G = construir_grafo_ponderado(aristas, atasco, clima)

    caps = [n for n in nodos if (n.get("tipo") or "").lower() in ("capital", "hub")]
    cap_ids = sorted([str(c["id_nodo"]) for c in caps])

    tracking = cargar_tracking_gemelo_cassandra()

    retrasos_tramos = _cargar_retrasos_cassandra(nodos, aristas)

    historial = cargar_historial_tracking_hive(limite=500)
    trayectorias = cargar_trayectorias_por_camion(historial)
    ids_camiones_hist = sorted(trayectorias.keys())

    col_a, col_b = st.columns([2, 1])
    with col_a:
        st.caption(
            "**Mapa:** la capa **Aristas (carga de red)** está **desactivada** por defecto para evitar "
            "saturar España de líneas; actívala en el **control de capas** del mapa si la necesitas."
        )
        anim_tray = st.checkbox(
            "Animar trayectorias históricas (efecto movimiento, AntPath)",
            value=False,
            key="gemelo_anim_trayectorias",
        )
        st.markdown("**Planificación de ruta (Dijkstra / NetworkX)**")
        id_camiones = [str(t.get("id_camion")) for t in tracking if t.get("id_camion")]
        
        all_camion_options = ["(ninguno)"] + sorted(set(id_camiones + ids_camiones_hist))
        sel_camion = st.selectbox(
            "Ver trayectoria de camión (desde Hive)",
            options=all_camion_options,
            key="gemelo_sel_camion",
        )
        camion_seleccionado = sel_camion if sel_camion != "(ninguno)" else None
        
        if camion_seleccionado and camion_seleccionado in trayectorias:
            pts = trayectorias[camion_seleccionado]
            st.caption(f"📍 {camion_seleccionado}: **{len(pts)}** posiciones en historial (ordenadas cronológicamente)")
            if len(pts) >= 2:
                st.caption(f"   Trayectoria: {pts[0].get('origen', '?')} → {pts[-1].get('destino', '?')}")
                st.caption(f"   Período: {pts[0].get('timestamp', '?')[:19]} → {pts[-1].get('timestamp', '?')[:19]}")
        
        destino = st.selectbox("Destino (nodo capital)", options=cap_ids or ["—"], key="gemelo_destino")

        origen_manual = st.selectbox(
            "Origen (nodo)",
            options=cap_ids or ["—"],
            key="gemelo_origen_manual",
            help="Puedes fijar manualmente la capital de origen.",
        )

        usar_gps = False
        origen = origen_manual
        if tracking and id_camiones and sel_camion in id_camiones:
            tc = next((x for x in tracking if str(x.get("id_camion")) == sel_camion), None)
            if tc and tc.get("lat") is not None:
                origen_calc = nodo_mas_cercano(float(tc["lat"]), float(tc["lon"]), nodos)
                st.caption(f"Nodo más cercano al GPS del camión: `{origen_calc}`")
                usar_gps = st.checkbox("Usar ese nodo como origen", value=True, key="gemelo_usar_gps")
                origen = origen_calc if usar_gps else origen_manual

        nodo_bloqueado: Optional[str] = None
        if bloqueo_on and cap_ids:
            nodo_bloqueado = st.selectbox(
                "Capital bloqueada",
                options=cap_ids,
                key="gemelo_capital_bloqueada",
            )

        cmp_out: Optional[Dict[str, Any]] = None
        r1: List[str] = []
        r_alt: List[str] = []
        km0 = km1 = 0.0
        t0 = t1 = 0.0

        if cap_ids and destino != "—" and origen != "—":
            cmp_out = comparar_original_vs_bloqueo(
                G,
                origen,
                destino,
                nodo_bloqueado if bloqueo_on else None,
            )
            r1 = cmp_out.get("ruta_original") or []
            r_alt = cmp_out.get("ruta_alternativa") or []
            km0 = float(cmp_out.get("km_original") or 0.0)
            c0 = float(cmp_out.get("coste_original") or 0.0)
            t0 = tiempo_estimado_horas(km0, c0)
            if cmp_out.get("recalculado") and r_alt:
                km1 = float(cmp_out.get("km_alternativa") or 0.0)
                c1 = float(cmp_out.get("coste_alternativa") or 0.0)
                t1 = tiempo_estimado_horas(km1, c1)
            else:
                km1, t1 = km0, t0

        if cmp_out:
            if r1:
                st.markdown("**Resultado**")
                st.write(
                    f"Ruta referencia: `{' → '.join(r1)}` — **{km0:.1f} km** · **{t0 * 60:.1f} min** (estim.)"
                )
                if cmp_out.get("recalculado"):
                    if r_alt:
                        st.warning(
                            f"Bloqueo afecta la ruta. Alternativa: `{' → '.join(r_alt)}` — "
                            f"**{km1:.1f} km** · **{t1 * 60:.1f} min**"
                        )
                        st.info(
                            f"Δ km: **{km1 - km0:+.1f}** · Δ tiempo: **{(t1 - t0) * 60:+.1f} min**"
                        )
                    else:
                        st.error("No hay ruta alternativa sin el nodo bloqueado.")
                mapa = _crear_mapa_gemelo(
                    nodos,
                    aristas,
                    tracking,
                    ruta1=r1,
                    ruta2=r_alt if cmp_out.get("recalculado") and r_alt else None,
                    retrasos_tramos=retrasos_tramos,
                    trayectorias=trayectorias,
                    camion_seleccionado=camion_seleccionado,
                    animar_trayectorias=anim_tray,
                )
            else:
                st.warning("No hay ruta entre origen y destino con los pesos actuales.")
                mapa = _crear_mapa_gemelo(
                    nodos,
                    aristas,
                    tracking,
                    retrasos_tramos=retrasos_tramos,
                    trayectorias=trayectorias,
                    camion_seleccionado=camion_seleccionado,
                    animar_trayectorias=anim_tray,
                )
        else:
            st.info("Selecciona origen y destino válidos en la red.")
            mapa = _crear_mapa_gemelo(
                nodos,
                aristas,
                tracking,
                retrasos_tramos=retrasos_tramos,
                trayectorias=trayectorias,
                camion_seleccionado=camion_seleccionado,
                animar_trayectorias=anim_tray,
            )

        st_folium(mapa, width=None, height=520, returned_objects=[], key="folium_gemelo")

        if ids_camiones_hist:
            with st.expander("📊 Historial de posiciones (últimas 500)", expanded=False):
                for cid in ids_camiones_hist[:5]:
                    pts = trayectorias.get(cid, [])
                    if pts:
                        df_hist = pd.DataFrame([
                            {
                                "timestamp": p.get("timestamp", "")[:19] if p.get("timestamp") else "",
                                "lat": _safe_float_coord(p.get("lat")),
                                "lon": _safe_float_coord(p.get("lon")),
                                "nodo": p.get("nodo_actual", ""),
                                "progreso": f"{p.get('progreso_pct', 0):.1f}%",
                            }
                            for p in pts[:20]
                        ])
                        st.markdown(f"**{cid}** ({len(pts)} posiciones)")
                        st.dataframe(
                            _dataframe_arrow_safe(df_hist),
                            hide_index=True,
                            width="stretch",
                        )

    with col_b:
        st.markdown("**Hive — transporte_ingesta_real**")
        hive_rows = cargar_transporte_ingesta_real_hive()
        if hive_rows:
            st.dataframe(
                _dataframe_arrow_safe(pd.DataFrame(hive_rows[:30])),
                hide_index=True,
                width="stretch",
            )
        else:
            st.caption("Sin filas o tabla no creada (`sql/hive_gemelo_digital.hql`).")

        st.divider()
        st.markdown("**Ingesta explorador (CSV)**")
        st.caption("Columnas: `id_camion`, `lat`, `lon`, `origen`, `destino`")
        up = st.file_uploader("CSV", type=["csv"], key="gemelo_csv_up")
        if up is not None:
            raw = up.read().decode("utf-8", errors="replace")
            if st.button("Ingerir a Kafka + HDFS", key="gemelo_ingest_btn"):
                res = ingerir_csv_gemelo(raw)
                if res.get("ok"):
                    st.success(
                        f"Ingeridas **{res['n']}** filas. Kafka: {'✅' if res['kafka'] else '❌'} · "
                        f"HDFS: `{res.get('hdfs_path', '—')}`"
                    )
                else:
                    st.error(res.get("error", "Error"))
