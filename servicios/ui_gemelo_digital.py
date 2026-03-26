"""
Gemelo digital logístico — mapa Folium, incidencias (sliders), Dijkstra, ingesta CSV.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import folium
import streamlit as st
from streamlit_folium import st_folium

from servicios.gemelo_digital_datos import (
    cargar_red_gemelo,
    cargar_tracking_gemelo_cassandra,
    cargar_transporte_ingesta_real_hive,
)
from servicios.gemelo_digital_rutas import (
    comparar_original_vs_bloqueo,
    construir_grafo_ponderado,
    nodo_mas_cercano,
    tiempo_estimado_horas,
)
from servicios.ingesta_csv_gemelo import ingerir_csv_gemelo


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
) -> folium.Map:
    m = folium.Map(location=[40.2, -3.8], zoom_start=6, tiles="OpenStreetMap")
    pos: Dict[str, Tuple[float, float]] = {}
    for n in nodos:
        nid = str(n.get("id_nodo", ""))
        if not nid:
            continue
        lat, lon = float(n["lat"]), float(n["lon"])
        pos[nid] = (lat, lon)
        color = "red" if (n.get("tipo") or "").lower() == "capital" else "blue"
        folium.CircleMarker(
            location=[lat, lon],
            radius=5 if color == "blue" else 7,
            color=color,
            fill=True,
            fill_opacity=0.85,
            popup=nid,
        ).add_to(m)

    for e in aristas:
        a, b = e.get("src"), e.get("dst")
        if not a or not b or a not in pos or b not in pos:
            continue
        folium.PolyLine(
            [list(pos[a]), list(pos[b])],
            color="gray",
            weight=1,
            opacity=0.35,
        ).add_to(m)

    def dibujar_ruta(ruta: List[str], color: str, label: str) -> None:
        if len(ruta) < 2:
            return
        pts = [[pos[x][0], pos[x][1]] for x in ruta if x in pos]
        if len(pts) < 2:
            return
        folium.PolyLine(pts, color=color, weight=4, opacity=0.85, tooltip=label).add_to(m)

    if ruta1:
        dibujar_ruta(ruta1, "green", "Ruta referencia")
    if ruta2:
        dibujar_ruta(ruta2, "orange", "Ruta alternativa")

    for t in tracking:
        lat, lon = t.get("lat"), t.get("lon")
        if lat is None or lon is None:
            continue
        cid = str(t.get("id_camion", "?"))
        folium.Marker(
            [float(lat), float(lon)],
            icon=folium.DivIcon(html=f'<div style="font-size:22px" title="{cid}">🚛</div>'),
            tooltip=f"Camión {cid}",
        ).add_to(m)

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

    caps = [n for n in nodos if (n.get("tipo") or "").lower() == "capital"]
    cap_ids = sorted([str(c["id_nodo"]) for c in caps])

    tracking = cargar_tracking_gemelo_cassandra()

    col_a, col_b = st.columns([2, 1])
    with col_a:
        st.markdown("**Planificación de ruta (Dijkstra / NetworkX)**")
        id_camiones = [str(t.get("id_camion")) for t in tracking if t.get("id_camion")]
        sel_camion = st.selectbox(
            "Camión (referencia)",
            options=(id_camiones if id_camiones else ["(sin datos Cassandra)"]),
            key="gemelo_sel_camion",
        )

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
                )
            else:
                st.warning("No hay ruta entre origen y destino con los pesos actuales.")
                mapa = _crear_mapa_gemelo(nodos, aristas, tracking)
        else:
            st.info("Selecciona origen y destino válidos en la red.")
            mapa = _crear_mapa_gemelo(nodos, aristas, tracking)

        st_folium(mapa, width=None, height=520, returned_objects=[], key="folium_gemelo")

    with col_b:
        st.markdown("**Hive — transporte_ingesta_real**")
        hive_rows = cargar_transporte_ingesta_real_hive()
        if hive_rows:
            st.dataframe(hive_rows[:30], use_container_width=True)
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
