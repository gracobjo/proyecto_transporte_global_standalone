#!/usr/bin/env python3
"""
Dashboard Streamlit — SIMLOG España
Organización por fases del ciclo KDD y ejecución alineada con el código real:
  ingesta/ingesta_kdd.py  →  procesamiento/procesamiento_grafos.py
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import folium
import streamlit as st
from streamlit_folium import st_folium

BASE = Path(__file__).resolve().parent
LOGO_PATH = BASE / "logo.png"
sys.path.insert(0, str(BASE))

from config_nodos import get_nodos, get_aristas
from config import (
    CASSANDRA_HOST,
    HDFS_BACKUP_PATH,
    TOPIC_TRANSPORTE,
    PROJECT_DISPLAY_NAME,
    PROJECT_TAGLINE,
    PROJECT_DESCRIPTION,
)

from servicios.estado_y_datos import (
    estado_servicios,
    verificar_hdfs_ruta,
    verificar_kafka_topic,
    verificar_cassandra,
    cargar_nodos_cassandra,
    cargar_aristas_cassandra,
    cargar_tracking_cassandra,
    cargar_pagerank_cassandra,
)
from servicios.kdd_fases import FaseKDD, FASES_KDD
from servicios.ejecucion_pipeline import (
    ejecutar_ingesta,
    ejecutar_procesamiento,
    ejecutar_fase_kdd,
)
from servicios.cuadro_mando_ui import render_cuadro_mando_tab
from servicios.ui_gestion_servicios import render_panel_gestion_servicios
from servicios.ui_servicios_web import render_sidebar_enlaces_ui
from servicios.gestion_servicios import arrancar_stack_basico, arrancar_todos_servicios
from servicios.ui_rutas_hibridas import render_rutas_hibridas_tab
from servicios.ui_pipeline_resultados import render_pipeline_resultados_tab
from servicios.ui_asistente_flota import render_asistente_flota_tab
from servicios.ui_gemelo_digital import render_gemelo_digital_sidebar, render_gemelo_digital_tab
from servicios.mapa_rutas_hibridas import crear_mapa_planificacion_rutas
from servicios.kdd_vista_grafo import render_bloque_grafo_fases_spark
from servicios.kdd_reglas_ui import render_panel_reglas_grafo
from servicios.kdd_vista_ficheros import render_vista_previa_ficheros_fase

COLORES_ESTADO = {
    "ok": "green",
    "OK": "green",
    "congestionado": "orange",
    "Congestionado": "orange",
    "bloqueado": "red",
    "Bloqueado": "red",
    "BLOQUEADO": "red",
}


def crear_mapa(
    nodos_cassandra: List[Dict],
    aristas_cassandra: List[Dict],
    tracking: List[Dict],
) -> folium.Map:
    """Mapa Folium: nodos y aristas desde Cassandra; fallback a topología estática si no hay datos."""
    nodos_static = get_nodos()
    m = folium.Map(location=[40.4, -3.7], zoom_start=6, tiles="OpenStreetMap")

    # Mapa id -> posición y estado
    pos: Dict[str, Dict[str, Any]] = {}
    for row in nodos_cassandra:
        nid = row.get("id_nodo")
        if nid:
            pos[nid] = {
                "lat": row.get("lat"),
                "lon": row.get("lon"),
                "tipo": row.get("tipo") or nodos_static.get(nid, {}).get("tipo", "secundario"),
                "estado": (row.get("estado") or "OK"),
            }
    for nid, info in nodos_static.items():
        if nid not in pos:
            pos[nid] = {
                "lat": info["lat"],
                "lon": info["lon"],
                "tipo": info.get("tipo", "secundario"),
                "estado": "OK",
            }

    # Aristas
    if aristas_cassandra:
        for r in aristas_cassandra:
            src, dst = r.get("src"), r.get("dst")
            if not src or not dst or src not in pos or dst not in pos:
                continue
            est = (r.get("estado") or "OK").lower()
            if "bloque" in est:
                color = "red"
            elif "congest" in est or "congestion" in est:
                color = "orange"
            else:
                color = "green"
            folium.PolyLine(
                [[pos[src]["lat"], pos[src]["lon"]], [pos[dst]["lat"], pos[dst]["lon"]]],
                color=color,
                weight=2,
                opacity=0.65,
                tooltip=f"{src} → {dst}",
            ).add_to(m)
    else:
        for src, dst, _ in get_aristas():
            if src in pos and dst in pos:
                folium.PolyLine(
                    [[pos[src]["lat"], pos[src]["lon"]], [pos[dst]["lat"], pos[dst]["lon"]]],
                    color="gray",
                    weight=1,
                    opacity=0.35,
                ).add_to(m)

    # Nodos
    for nid, p in pos.items():
        est = p.get("estado", "OK")
        color = COLORES_ESTADO.get(est, COLORES_ESTADO.get(str(est).lower(), "blue"))
        folium.CircleMarker(
            location=[p["lat"], p["lon"]],
            radius=10 if p.get("tipo") == "hub" else 6,
            color=color,
            fill=True,
            fill_opacity=0.85,
            popup=f"<b>{nid}</b><br>{p.get('tipo')}<br>Estado: {est}",
        ).add_to(m)

    # Camiones
    for t in tracking:
        lat, lon = t.get("lat"), t.get("lon")
        if lat is None or lon is None:
            continue
        cid = t.get("id_camion", "?")
        folium.Marker(
            [lat, lon],
            icon=folium.Icon(color="blue", icon="info-sign"),
            popup=f"Camión {cid}<br>{t.get('estado_ruta', '')}",
        ).add_to(m)

    return m


def _kdd_sync_idx() -> tuple[int, int, list[str]]:
    """Índice acotado, n y etiquetas de fase."""
    n = len(FASES_KDD)
    if "kdd_fase_idx" not in st.session_state:
        st.session_state.kdd_fase_idx = 0
    idx = max(0, min(n - 1, int(st.session_state.kdd_fase_idx)))
    st.session_state.kdd_fase_idx = idx
    labels = [f"{f.orden}. {f.titulo}" for f in FASES_KDD]
    return idx, n, labels


def _kdd_on_fase_select_change() -> None:
    """Sincroniza índice lógico solo cuando el usuario cambia el desplegable (no machaca ◀ ▶)."""
    st.session_state.kdd_fase_idx = int(st.session_state.kdd_select_widget)


def _render_kdd_title_and_selector() -> None:
    """Fila superior: título a la izquierda, «Ir a fase» + desplegable a la derecha (misma banda visual)."""
    idx, n, labels = _kdd_sync_idx()
    c_title, c_sel = st.columns([3, 2])
    with c_title:
        st.subheader("Fases del ciclo KDD en este proyecto")
    with c_sel:
        st.markdown("**Ir a fase**")
        # El valor del widget debe seguir a kdd_fase_idx tras ◀/▶; si además comparamos sel != idx aquí,
        # un desfase del selectbox devolvía siempre fase 1 o bloqueaba en fase 2.
        if st.session_state.get("kdd_select_widget") != idx:
            st.session_state.kdd_select_widget = idx
        st.selectbox(
            "Ir a fase",
            options=list(range(n)),
            format_func=lambda i: labels[i],
            key="kdd_select_widget",
            label_visibility="collapsed",
            on_change=_kdd_on_fase_select_change,
        )


def _render_kdd_prev_next() -> int:
    """Solo ◀ / ▶ en una fila; caption de posición. Devuelve índice 0..n-1."""
    idx, n, labels = _kdd_sync_idx()
    b1, b2 = st.columns([1, 1])
    with b1:
        if st.button(
            "◀ Anterior",
            disabled=idx <= 0,
            key="kdd_pag_prev",
            width="stretch",
            help="Fase anterior del ciclo KDD",
        ):
            st.session_state.kdd_fase_idx = idx - 1
            st.rerun()
    with b2:
        if st.button(
            "Siguiente ▶",
            disabled=idx >= n - 1,
            key="kdd_pag_next",
            width="stretch",
            help="Siguiente fase del ciclo KDD",
        ):
            st.session_state.kdd_fase_idx = idx + 1
            st.rerun()

    k = int(st.session_state.kdd_fase_idx)
    st.caption(f"**Fase {k + 1} de {n}** — {labels[k]}")
    return k


def _render_fase_kdd_card(
    f: FaseKDD,
    *,
    widget_scope: str = "kdd_principal",
    mostrar_vista_previa: bool = True,
) -> None:
    with st.container(border=True):
        st.markdown(f"### {f.orden}. {f.titulo}")
        st.caption(f.resumen)
        st.markdown("**Actividades**")
        for a in f.actividades:
            st.markdown(f"- {a}")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Entradas**")
            for x in f.datos_entrada:
                st.markdown(f"- `{x}`")
        with c2:
            st.markdown("**Salidas**")
            for x in f.datos_salida:
                st.markdown(f"- `{x}`")
        st.markdown("**Stack / script**")
        _cap_stack = list(f.stack)
        if f.script and f.script.strip():
            sc = f.script.strip()
            if sc not in _cap_stack:
                _cap_stack.append(f"`{sc}`")
        st.caption(" · ".join(_cap_stack))
        if mostrar_vista_previa:
            render_vista_previa_ficheros_fase(st, f, BASE, widget_scope=widget_scope)
        else:
            st.caption(
                "Vista previa de ficheros, simulación por paso, GPS y OpenWeather: "
                "elige esta fase con **◀ / ▶** o «Ir a fase» en la cabecera de la pestaña."
            )


def main() -> None:
    page_icon = str(LOGO_PATH) if LOGO_PATH.exists() else "🚛"
    st.set_page_config(
        page_title=f"{PROJECT_DISPLAY_NAME} — KDD",
        page_icon=page_icon,
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # --- session state ---
    if "paso_15min" not in st.session_state:
        st.session_state.paso_15min = 0
    if "timeline" not in st.session_state:
        st.session_state.timeline = []

    h1, h2 = st.columns([1, 8])
    with h1:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=96)
    with h2:
        st.title(PROJECT_DISPLAY_NAME)
    st.caption(PROJECT_TAGLINE)
    st.markdown(
        f"{PROJECT_DESCRIPTION} Ciclo **KDD** con stack Apache: **ingesta**, "
        "**Spark + GraphFrames**, **interpretación** (Cassandra + este dashboard)."
    )

    # --- Sidebar: servicios + paso temporal + acciones ---
    with st.sidebar:
        st.subheader("Estado de servicios")
        estado_stack = estado_servicios()
        for nombre, etiqueta in estado_stack.items():
            st.markdown(f"{etiqueta} **{nombre}**")

        todos_ok = all(
            v.startswith("✅") or "Activo" in v for v in estado_stack.values()
        )

        st.caption(
            "Resumen **stack completo** (7 componentes). "
            f"Puertos típicos: `9870` HDFS · `9092` Kafka · `9042` Cassandra (`{CASSANDRA_HOST}`) · "
            "`7077` Spark · `10000` Hive · `8088` Airflow · `8443`/`8080` NiFi."
        )
        st.caption("Controles por servicio (iniciar/parar) en la pestaña **Servicios**.")

        if st.button("🔄 Actualizar estado", width="stretch", key="btn_refresh_estado"):
            st.rerun()

        if todos_ok:
            st.success("Stack en marcha: todos los servicios responden.")
            with st.expander("Reiniciar arranque (solo si lo necesitas)", expanded=False):
                st.caption(
                    "No hace falta usar esto si el pipeline ya funciona. "
                    "Solo tras reinicio del sistema o si un servicio se ha caído."
                )
                if st.button(
                    "▶ Arrancar base (HDFS + Cassandra + Kafka)",
                    help="Orden mínimo para ingesta. Luego «Actualizar estado».",
                    width="stretch",
                    type="secondary",
                    key="btn_arrancar_stack_basico",
                ):
                    with st.spinner("Arrancando HDFS, Cassandra y Kafka…"):
                        msgs = arrancar_stack_basico()
                    st.session_state["last_arranque_msgs"] = msgs
                    st.rerun()

                st.caption("Stack completo: HDFS → Cassandra → Kafka → Spark → Hive → Airflow → NiFi.")
                if st.button("Ejecutar arranque completo (7 servicios)", key="btn_arrancar_todos"):
                    with st.spinner("Arrancando todos los servicios (puede tardar varios minutos)…"):
                        msgs = arrancar_todos_servicios()
                    st.session_state["last_arranque_msgs"] = msgs
                    st.rerun()
        else:
            st.warning("Algunos servicios no responden — revisa la lista o arranca el stack.")
            if st.button(
                "▶ Arrancar base (HDFS + Cassandra + Kafka)",
                help="Primero el núcleo de ingesta. Luego «Actualizar estado».",
                width="stretch",
                type="primary",
                key="btn_arrancar_stack_basico",
            ):
                with st.spinner("Arrancando HDFS, Cassandra y Kafka…"):
                    msgs = arrancar_stack_basico()
                st.session_state["last_arranque_msgs"] = msgs
                st.rerun()

            with st.expander("Arrancar stack completo (7 servicios)", expanded=False):
                st.caption("Orden: HDFS → Cassandra → Kafka → Spark → Hive → Airflow → NiFi.")
                if st.button("Ejecutar arranque completo", key="btn_arrancar_todos"):
                    with st.spinner("Arrancando todos los servicios (puede tardar varios minutos)…"):
                        msgs = arrancar_todos_servicios()
                    st.session_state["last_arranque_msgs"] = msgs
                    st.rerun()

        if st.session_state.get("last_arranque_msgs"):
            with st.expander("Último resultado de arranque", expanded=True):
                for line in st.session_state["last_arranque_msgs"]:
                    st.caption(line)
                st.caption("Espera **30–90 s** (sobre todo Cassandra) y pulsa **Actualizar estado**.")

        render_sidebar_enlaces_ui()

        render_gemelo_digital_sidebar()

        st.divider()
        st.subheader("Línea temporal (simulación 15 min)")
        st.checkbox(
            "Paso automático (reloj, alineado con SIMLOG_INGESTA_INTERVAL_MINUTES)",
            help="Sin PASO_15MIN fijo: la simulación usa la ventana de tiempo actual (como cron/Airflow).",
            key="ingesta_paso_automatico",
        )
        st.session_state.paso_15min = st.number_input(
            "Paso actual (ventana)",
            min_value=0,
            max_value=96,
            value=int(st.session_state.paso_15min),
            step=1,
            disabled=st.session_state.ingesta_paso_automatico,
            help="Manual: se envía como PASO_15MIN. Desactivado si usas paso automático.",
        )
        st.checkbox(
            "Simular incidencias de tráfico (nodos/aristas)",
            value=bool(st.session_state.get("simlog_simular_incidencias", True)),
            key="simlog_simular_incidencias",
            help=(
                "Si lo desmarcas, la ingesta generará estados operativos estables (todo OK) "
                "para nodos y aristas."
            ),
        )

        if st.button("Ejecutar ingesta (fases 1–2 KDD)", type="primary", width="stretch"):
            with st.spinner("Ingesta: clima, incidentes, GPS, Kafka, HDFS…"):
                if st.session_state.ingesta_paso_automatico:
                    code, out, err = ejecutar_ingesta(
                        None,
                        simular_incidencias=bool(st.session_state.get("simlog_simular_incidencias", True)),
                    )
                else:
                    code, out, err = ejecutar_ingesta(
                        int(st.session_state.paso_15min),
                        simular_incidencias=bool(st.session_state.get("simlog_simular_incidencias", True)),
                    )
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
            if code == 0:
                if st.session_state.ingesta_paso_automatico:
                    st.session_state.timeline.append(f"{ts} — Ingesta OK (paso automático)")
                else:
                    st.session_state.timeline.append(f"{ts} — Ingesta OK (paso {st.session_state.paso_15min})")
                st.success("Ingesta terminada.")
            else:
                st.error(f"Código {code}")
                st.code(err[-2000:] or out[-2000:])
            st.session_state.last_ingesta_out = out
            st.session_state.last_ingesta_err = err

        if st.button("Ejecutar procesamiento Spark (fases 3–5 KDD)", width="stretch"):
            with st.spinner("Spark: grafo, PageRank, Cassandra…"):
                code, out, err = ejecutar_procesamiento()
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
            if code == 0:
                st.session_state.timeline.append(f"{ts} — Procesamiento OK")
                st.success("Procesamiento terminado.")
            else:
                st.error(f"Código {code}")
                st.code(err[-2000:] or out[-2000:])
            st.session_state.last_proc_out = out
            st.session_state.last_proc_err = err

        if st.button("Avanzar paso + ingesta + procesamiento", width="stretch"):
            p = int(st.session_state.paso_15min)
            with st.spinner("Pipeline completo…"):
                c1, o1, e1 = ejecutar_ingesta(
                    p,
                    simular_incidencias=bool(st.session_state.get("simlog_simular_incidencias", True)),
                )
                c2, o2, e2 = ejecutar_procesamiento()
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
            if c1 == 0 and c2 == 0:
                st.session_state.paso_15min = p + 1
                st.session_state.timeline.append(f"{ts} — Pipeline completo (ingesta paso {p})")
                st.success("Pipeline OK.")
            else:
                st.error(f"Ingesta {c1} | Procesamiento {c2}")
                st.code((e1 or o1) + "\n---\n" + (e2 or o2))
            st.rerun()

        if st.session_state.timeline:
            st.divider()
            st.caption("Últimos eventos")
            for ev in reversed(st.session_state.timeline[-8:]):
                st.caption(ev)

    (
        tab_kdd,
        tab_resultados,
        tab_cuadro,
        tab_asistente,
        tab_rutas,
        tab_gemelo,
        tab_servicios,
        tab_mapa,
        tab_verif,
    ) = st.tabs(
        [
            "Ciclo KDD",
            "Resultados pipeline",
            "Cuadro de mando",
            "Asistente flota",
            "Rutas híbridas",
            "Gemelo digital",
            "Servicios",
            "Mapa y métricas",
            "Verificación técnica",
        ]
    )

    with tab_kdd:
        _render_kdd_title_and_selector()
        st.info(
            "Las fases **1–2** se realizan en `ingesta/ingesta_kdd.py`. "
            "Las fases **3–5** se concentran en `procesamiento/procesamiento_grafos.py` (Spark)."
        )
        idx = _render_kdd_prev_next()
        fase_actual = FASES_KDD[idx]

        paso = int(st.session_state.paso_15min)
        st.caption(
            f"Paso temporal (sidebar): **{paso}** · Script: `{fase_actual.script or '—'}`"
        )

        if fase_actual.orden in (1, 2):
            st.warning(
                "Las fases **1** y **2** comparten el mismo script de ingesta; "
                "**Ejecutar esta fase** lanza la ingesta completa (selección + preprocesamiento)."
            )
        elif fase_actual.orden in (3, 4):
            st.info(
                "Fases **3** y **4** ejecutan solo la parte Spark indicada (`fase_kdd_spark`). "
                "Para persistir en Cassandra y ver el mapa actualizado, usa la **fase 5** o el botón de procesamiento completo en la barra lateral."
            )
        elif fase_actual.orden == 5:
            st.info(
                "La **fase 5** persiste en Cassandra y opcionalmente en Hive. "
                "El mapa geográfico está en **Mapa y métricas**; la figura de **topología** (abajo) es la misma para fases 3–5."
            )

        if fase_actual.orden in (3, 4, 5):
            render_panel_reglas_grafo(st, fase_actual.orden)
            pr_rows = cargar_pagerank_cassandra() if fase_actual.orden >= 4 else []
            render_bloque_grafo_fases_spark(
                st,
                orden_fase=fase_actual.orden,
                nodos_cassandra=cargar_nodos_cassandra(),
                pagerank_rows=pr_rows,
            )

        c_run, c_spacer = st.columns([2, 3])
        with c_run:
            if st.button(
                f"Ejecutar fase {fase_actual.orden}: {fase_actual.titulo}",
                type="primary",
                key=f"kdd_run_fase_{fase_actual.orden}",
                width="stretch",
            ):
                with st.spinner(f"Ejecutando fase {fase_actual.orden}…"):
                    code, out, err = ejecutar_fase_kdd(
                        fase_actual.orden,
                        paso,
                        simular_incidencias=bool(st.session_state.get("simlog_simular_incidencias", True)),
                    )
                ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
                if code == 0:
                    st.session_state.timeline.append(
                        f"{ts} — Fase {fase_actual.orden} ({fase_actual.codigo}) OK"
                    )
                    st.success("Ejecución terminada correctamente.")
                else:
                    st.error(f"Código de salida {code}")
                    st.code(err[-4000:] or out[-4000:])
                st.session_state.last_fase_kdd_out = out
                st.session_state.last_fase_kdd_err = err

        _render_fase_kdd_card(fase_actual)

        with st.expander("Ver todas las fases (lista completa)", expanded=False):
            for f in FASES_KDD:
                _render_fase_kdd_card(
                    f,
                    widget_scope=f"kdd_lista_f{f.orden}",
                    mostrar_vista_previa=False,
                )

    with tab_resultados:
        render_pipeline_resultados_tab()

    with tab_cuadro:
        render_cuadro_mando_tab()

    with tab_asistente:
        render_asistente_flota_tab()

    with tab_rutas:
        render_rutas_hibridas_tab()

    with tab_gemelo:
        render_gemelo_digital_tab()

    with tab_servicios:
        render_panel_gestion_servicios()

    with tab_mapa:
        modo_mapa = st.radio(
            "Vista del mapa",
            options=["operativo", "planificacion"],
            format_func=lambda x: (
                "Operativo — Cassandra (nodos, aristas, tracking)"
                if x == "operativo"
                else "Planificación — última ruta (Rutas híbridas)"
            ),
            horizontal=True,
            key="tab_mapa_modo",
            help="El modo planificación reutiliza el último cálculo de la pestaña «Rutas híbridas».",
        )

        col_m1, col_m2 = st.columns([2, 1])
        with col_m1:
            if modo_mapa == "operativo":
                st.subheader("Mapa operativo")
                nodos_c = cargar_nodos_cassandra()
                aristas_c = cargar_aristas_cassandra()
                track_c = cargar_tracking_cassandra()
                if not nodos_c:
                    st.warning(
                        "No hay datos en `nodos_estado` (o Cassandra no responde). "
                        "Ejecuta el procesamiento tras la ingesta."
                    )
                mapa = crear_mapa(nodos_c, aristas_c, track_c)
                st_folium(mapa, width=None, height=480, returned_objects=[], key="folium_operativo")
            else:
                st.subheader("Mapa de planificación (red + ruta principal + alternativas)")
                rh = st.session_state.get("rh_resultado")
                if rh and rh.get("ok"):
                    ruta = rh["ruta"]
                    alts = rh.get("alternativas") or []
                    mostrar_red = st.checkbox(
                        "Mostrar toda la red de fondo (conexiones grises)",
                        value=bool(st.session_state.get("rh_mapa_red", True)),
                        key="tab_mapa_mostrar_red_completa",
                        help="Misma opción que en Rutas híbridas; aquí puedes cambiarla sin cambiar de pestaña.",
                    )
                    st.caption(
                        f"**Ruta actual:** `{' → '.join(ruta)}` · **{rh.get('num_saltos', 0)}** salto(s) · "
                        f"{len(alts)} alternativa(s) listada(s)."
                    )
                    mapa_plan = crear_mapa_planificacion_rutas(
                        ruta,
                        alts,
                        mostrar_red_completa=mostrar_red,
                        max_alternativas=6,
                    )
                    st_folium(mapa_plan, width=None, height=480, returned_objects=[], key="folium_planif")
                else:
                    st.info(
                        "Aún no hay una ruta calculada. Ve a la pestaña **Rutas híbridas**, "
                        "elige **origen** y **destino** y pulsa **Calcular ruta y mostrar en mapa**; "
                        "luego vuelve aquí para ver el mismo trazado en grande."
                    )
                    st.caption("Puedes seguir viendo el **mapa operativo** cambiando la vista de arriba.")

        with col_m2:
            if modo_mapa == "operativo":
                st.subheader("PageRank (muestra)")
                pr = cargar_pagerank_cassandra()
                if pr:
                    pr_sorted = sorted(pr, key=lambda x: float(x.get("pagerank") or 0), reverse=True)[:12]
                    for row in pr_sorted:
                        st.metric(
                            label=str(row.get("id_nodo", "")),
                            value=f"{float(row.get('pagerank') or 0):.6f}",
                        )
                else:
                    st.caption("Sin datos de PageRank. Ejecuta procesamiento Spark.")

                st.divider()
                st.caption("Leyenda aristas: verde OK · naranja congestión · rojo bloqueo")
            else:
                st.subheader("Resumen de la última ruta")
                rh = st.session_state.get("rh_resultado")
                if rh and rh.get("ok"):
                    st.metric("Saltos", f"{rh.get('num_saltos', 0)}")
                    st.metric("Min. retraso estimados (total)", f"{rh.get('minutos_totales_estimados', '—')}")
                    st.metric("Coste estimado (€)", f"{rh.get('coste_total_eur', '—')}")
                    alts = rh.get("alternativas") or []
                    st.caption(f"Alternativas calculadas: **{len(alts)}**")
                    with st.expander("Secuencia de nodos", expanded=False):
                        st.code(" → ".join(rh["ruta"]), language="text")
                else:
                    st.caption("Sin datos hasta calcular una ruta en **Rutas híbridas**.")

    with tab_verif:
        st.subheader("Comprobaciones rápidas")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**HDFS** (backup ingesta)")
            st.info(verificar_hdfs_ruta(HDFS_BACKUP_PATH))
        with c2:
            st.markdown("**Kafka**")
            st.info(verificar_kafka_topic(TOPIC_TRANSPORTE))

        c3, c4 = st.columns(2)
        with c3:
            st.markdown("**Cassandra — nodos**")
            st.info(verificar_cassandra("SELECT id_nodo FROM nodos_estado LIMIT 100", "nodos_estado"))
        with c4:
            st.markdown("**Cassandra — tracking**")
            st.info(verificar_cassandra("SELECT id_camion FROM tracking_camiones LIMIT 20", "tracking"))

        st.markdown("**Hive**")
        st.caption(
            "Si ves `spark.sql.catalogImplementation` o errores de metastore, el histórico Hive es opcional; "
            "el núcleo del proyecto valida con Cassandra."
        )


if __name__ == "__main__":
    main()
