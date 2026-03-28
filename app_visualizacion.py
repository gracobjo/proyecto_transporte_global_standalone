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
from folium.plugins import AntPath
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
    cargar_estado_nodos_reconfiguracion,
    cargar_estado_rutas_reconfiguracion,
    cargar_alertas_activas_cassandra,
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
from servicios.gestion_servicios import ORDEN_SERVICIOS, PORT_AIRFLOW, PORT_API, PORT_CASSANDRA, PORT_FAQ_IA, PORT_HDFS, PORT_HIVE, PORT_KAFKA, PORT_NIFI_HTTP, PORT_NIFI_HTTPS, PORT_SPARK_MASTER
from servicios.ui_rutas_hibridas import render_rutas_hibridas_tab
from servicios.ui_pipeline_resultados import render_pipeline_resultados_tab
from servicios.ui_asistente_flota import render_asistente_flota_tab
from servicios.ui_gemelo_digital import render_gemelo_digital_sidebar, render_gemelo_digital_tab
from servicios.mapa_rutas_hibridas import crear_mapa_planificacion_rutas
from servicios.kdd_vista_grafo import render_bloque_grafo_fases_spark
from servicios.kdd_reglas_ui import render_panel_reglas_grafo
from servicios.kdd_vista_ficheros import render_vista_previa_ficheros_fase
from servicios.ui_faq_ia import render_faq_ia_panel
from servicios.ui_pruebas_ingesta import render_pruebas_ingesta_tab
from servicios.ui_nifi_flujo import render_nifi_flujo_tab
from servicios.pruebas_ingesta import registrar_prueba_ingesta
from servicios.pipeline_verificacion import leer_ultima_ingesta

COLORES_ESTADO = {
    "ok": "green",
    "OK": "green",
    "active": "green",
    "ACTIVE": "green",
    "congestionado": "orange",
    "Congestionado": "orange",
    "bloqueado": "red",
    "Bloqueado": "red",
    "BLOQUEADO": "red",
    "down": "red",
    "DOWN": "red",
}


TAB_LABELS = [
    "Ciclo KDD",
    "Resultados pipeline",
    "Pruebas",
    "Flujo NiFi",
    "Cuadro de mando",
    "Asistente flota",
    "Rutas híbridas",
    "Gemelo digital",
    "Servicios",
    "Mapa y métricas",
    "Verificación técnica",
]


def _render_resumen_dgt_ui() -> None:
    ing = leer_ultima_ingesta()
    if not ing.get("disponible"):
        st.caption("Sin snapshot local todavía para mostrar el estado DGT.")
        return

    alerta = ing.get("alerta_bloqueos") or {}
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Modo DGT", str(ing.get("dgt_source_mode") or "disabled"))
    c2.metric("Incidencias DGT", str(ing.get("dgt_incidencias_totales", 0)))
    c3.metric("Nodos DGT", str(ing.get("dgt_nodos_afectados", 0)))
    c4.metric("Bloqueos", str(alerta.get("bloqueados", 0)))

    if alerta:
        ratio = alerta.get("ratio_bloqueados", alerta.get("ratio", 0))
        msg = (
            f"Alerta operativa: `{alerta.get('nivel', 'normal')}` · "
            f"bloqueados `{alerta.get('bloqueados', 0)}` · ratio `{ratio}`"
        )
        if str(alerta.get("nivel", "")).lower() in ("alta", "critica"):
            st.warning(msg)
        else:
            st.info(msg)


def _render_resumen_reconfiguracion_ui(alertas_activas: List[Dict[str, Any]], nodos_rt: List[Dict[str, Any]], rutas_rt: List[Dict[str, Any]]) -> None:
    down_nodes = [n for n in (nodos_rt or []) if str(n.get("status") or "").upper() == "DOWN"]
    down_routes = [r for r in (rutas_rt or []) if str(r.get("status") or "").upper() == "DOWN"]
    c1, c2, c3 = st.columns(3)
    c1.metric("Nodos caídos", str(len(down_nodes)))
    c2.metric("Rutas desactivadas", str(len(down_routes)))
    c3.metric("Alertas activas", str(len(alertas_activas or [])))
    if alertas_activas:
        with st.expander("Alertas activas de reconfiguración", expanded=False):
            for alerta in alertas_activas[:12]:
                st.markdown(
                    f"- `{alerta.get('tipo_alerta')}` · `{alerta.get('entidad_id')}` · "
                    f"{alerta.get('mensaje') or alerta.get('causa') or 'sin detalle'}"
                )


def _buscar_semantico_ui(query: str) -> List[Dict[str, str]]:
    q = (query or "").strip().lower()
    if not q:
        return []
    catalogo = [
        {"tab": "Servicios", "titulo": "Levantar/parar stack y Swagger", "keywords": "servicios iniciar parar swagger api faq ia airflow nifi kafka hive cassandra spark hdfs"},
        {"tab": "Cuadro de mando", "titulo": "Consultas supervisadas Cassandra/Hive", "keywords": "consulta sql cql hive cassandra dashboard cuadro mando"},
        {"tab": "Cuadro de mando", "titulo": "Informes a medida + PDF", "keywords": "informe pdf plantilla campos select tabla where order"},
        {"tab": "Asistente flota", "titulo": "Preguntas en lenguaje natural", "keywords": "asistente flota lenguaje natural camion rutas"},
        {"tab": "Servicios", "titulo": "FAQ IA para dudas rápidas", "keywords": "faq ia preguntas frecuentes ayuda soporte"},
        {"tab": "Rutas híbridas", "titulo": "Planificación origen-destino y alternativas", "keywords": "ruta hibrida alternativa origen destino bfs"},
        {"tab": "Mapa y métricas", "titulo": "Mapa operativo y PageRank", "keywords": "mapa metrica pagerank nodos aristas tracking reconfiguracion nodo down ruta alternativa alertas activas"},
        {"tab": "Resultados pipeline", "titulo": "Resultado de fases y persistencia", "keywords": "pipeline resultado fases ingesta spark"},
        {"tab": "Resultados pipeline", "titulo": "Estado DGT y alertas de bloqueos", "keywords": "dgt datex2 live cache disabled alertas bloqueos provenance"},
        {"tab": "Pruebas", "titulo": "Registro de pruebas y trazabilidad", "keywords": "pruebas test evidencias nifi airflow script ingesta historico"},
        {"tab": "Flujo NiFi", "titulo": "Canvas, procesadores y relaciones NiFi", "keywords": "nifi procesadores relaciones provenance canvas openweather dgt kafka hdfs"},
        {"tab": "Verificación técnica", "titulo": "Checks rápidos HDFS/Kafka/Cassandra", "keywords": "verificacion tecnica hdfs kafka cassandra checks"},
        {"tab": "Ciclo KDD", "titulo": "Fases KDD y ejecución por fase", "keywords": "kdd fases seleccion preprocesamiento transformacion mineria interpretacion"},
        {"tab": "Gemelo digital", "titulo": "Visualización del gemelo y red", "keywords": "gemelo digital red nodos aristas"},
    ]
    q_tokens = [t for t in q.replace("—", " ").replace("-", " ").split() if t]
    res: List[Dict[str, str]] = []
    for it in catalogo:
        text = f"{it['titulo']} {it['keywords']} {it['tab']}".lower()
        score = 0
        for t in q_tokens:
            if t in text:
                score += 1
        if q in text:
            score += 2
        if score > 0:
            res.append({**it, "score": str(score)})
    res.sort(key=lambda x: int(x["score"]), reverse=True)
    return res[:6]


def crear_mapa(
    nodos_cassandra: List[Dict],
    aristas_cassandra: List[Dict],
    tracking: List[Dict],
    nodos_reconfig: List[Dict] | None = None,
    rutas_reconfig: List[Dict] | None = None,
    *,
    animar_rutas_sugeridas: bool = False,
) -> folium.Map:
    """Mapa Folium: nodos y aristas desde Cassandra; fallback a topología estática si no hay datos.

    Capas (LayerControl): aristas y rutas sugeridas empiezan ocultas para un mapa más legible.
    ``animar_rutas_sugeridas`` usa AntPath (efecto de flujo) en las polilíneas de ruta por camión.
    """
    nodos_static = get_nodos()
    m = folium.Map(
        location=[40.4, -3.7],
        zoom_start=6,
        tiles="CartoDB positron",
    )
    fg_aristas = folium.FeatureGroup(name="Red: todas las aristas", show=False)
    fg_nodos = folium.FeatureGroup(name="Nodos", show=True)
    fg_camiones = folium.FeatureGroup(name="Camiones (posición actual)", show=True)
    fg_rutas = folium.FeatureGroup(name="Rutas sugeridas (por camión)", show=False)

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

    nodos_rt = {row.get("id_nodo"): row for row in (nodos_reconfig or []) if row.get("id_nodo")}
    rutas_rt = {}
    for row in rutas_reconfig or []:
        src, dst = row.get("src"), row.get("dst")
        if src and dst:
            rutas_rt[f"{src}|{dst}"] = row
            rutas_rt[f"{dst}|{src}"] = row

    # Aristas
    if aristas_cassandra:
        for r in aristas_cassandra:
            src, dst = r.get("src"), r.get("dst")
            if not src or not dst or src not in pos or dst not in pos:
                continue
            rt = rutas_rt.get(f"{src}|{dst}", {})
            rt_status = str(rt.get("status") or "").upper()
            est = (r.get("estado") or "OK").lower()
            if rt_status == "DOWN":
                color = "gray"
                dash_array = "8, 8"
                tooltip = f"{src} → {dst} · desactivada"
            elif "bloque" in est:
                color = "red"
                dash_array = None
                tooltip = f"{src} → {dst}"
            elif "congest" in est or "congestion" in est:
                color = "orange"
                dash_array = None
                tooltip = f"{src} → {dst}"
            else:
                color = "green"
                dash_array = None
                tooltip = f"{src} → {dst}"
            folium.PolyLine(
                [[pos[src]["lat"], pos[src]["lon"]], [pos[dst]["lat"], pos[dst]["lon"]]],
                color=color,
                weight=3 if rt_status == "DOWN" else 2,
                opacity=0.65,
                dash_array=dash_array,
                tooltip=tooltip,
            ).add_to(fg_aristas)
    else:
        for src, dst, _ in get_aristas():
            if src in pos and dst in pos:
                folium.PolyLine(
                    [[pos[src]["lat"], pos[src]["lon"]], [pos[dst]["lat"], pos[dst]["lon"]]],
                    color="gray",
                    weight=1,
                    opacity=0.35,
                ).add_to(fg_aristas)

    # Nodos
    for nid, p in pos.items():
        rt = nodos_rt.get(nid, {})
        rt_status = str(rt.get("status") or "").upper()
        est = p.get("estado", "OK")
        color = COLORES_ESTADO.get(est, COLORES_ESTADO.get(str(est).lower(), "blue"))
        popup = f"<b>{nid}</b><br>{p.get('tipo')}<br>Estado: {est}"
        if rt_status == "DOWN":
            folium.Marker(
                location=[p["lat"], p["lon"]],
                icon=folium.DivIcon(
                    html=(
                        "<div style='font-size: 18px; color: white; background: #c62828; "
                        "border-radius: 50%; width: 22px; height: 22px; text-align: center; "
                        "line-height: 22px; border: 2px solid white;'>X</div>"
                    )
                ),
                popup=f"{popup}<br>Reconfiguración: DOWN<br>{rt.get('cause') or ''}",
            ).add_to(fg_nodos)
        else:
            folium.CircleMarker(
                location=[p["lat"], p["lon"]],
                radius=10 if p.get("tipo") == "hub" else 6,
                color=color,
                fill=True,
                fill_opacity=0.85,
                popup=popup,
            ).add_to(fg_nodos)

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
        ).add_to(fg_camiones)
        ruta_sugerida = t.get("ruta_sugerida") or []
        if isinstance(ruta_sugerida, list) and len(ruta_sugerida) >= 2:
            coords = []
            for nodo in ruta_sugerida:
                if nodo in pos:
                    coords.append([pos[nodo]["lat"], pos[nodo]["lon"]])
            if len(coords) >= 2:
                tip = f"Ruta sugerida {cid}"
                if animar_rutas_sugeridas:
                    AntPath(
                        coords,
                        color="#2563eb",
                        weight=4,
                        opacity=0.65,
                        delay=600,
                        dash_array=[12, 18],
                        tooltip=tip,
                    ).add_to(fg_rutas)
                else:
                    folium.PolyLine(
                        coords,
                        color="blue",
                        weight=4,
                        opacity=0.55,
                        tooltip=tip,
                    ).add_to(fg_rutas)

    fg_aristas.add_to(m)
    fg_rutas.add_to(m)
    fg_nodos.add_to(m)
    fg_camiones.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)

    # Encuadre sobre la flota visible (evita península entera si hay posiciones GPS).
    pts_fit: List[List[float]] = []
    for t in tracking:
        la, lo = t.get("lat"), t.get("lon")
        if la is not None and lo is not None:
            pts_fit.append([float(la), float(lo)])
    if len(pts_fit) >= 1:
        lats = [p[0] for p in pts_fit]
        lons = [p[1] for p in pts_fit]
        pad = 0.75
        m.fit_bounds(
            [[min(lats) - pad, min(lons) - pad], [max(lats) + pad, max(lons) + pad]],
            padding=(28, 28),
        )

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

    h1, h2, h3 = st.columns([1, 4, 3])
    with h1:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=88)
    with h2:
        st.title(PROJECT_DISPLAY_NAME)
        st.caption(PROJECT_TAGLINE)
    with h3:
        with st.container(border=True):
            st.markdown("**Ir rápido a una sección**")
            q = st.text_input(
                "Buscar sección o función",
                value="",
                placeholder="Ej.: servicios, hive, informe, rutas",
                key="ui_sem_search",
            ).strip()
            st.caption("Atajos útiles: `servicios`, `hive`, `informes`, `rutas`, `swagger`.")
            hits = _buscar_semantico_ui(q)
            if hits:
                for i, h in enumerate(hits[:3]):
                    if st.button(f"{h['tab']} · {h['titulo']}", key=f"hit_{i}_{h['tab']}", width="stretch"):
                        st.session_state["quick_open_tab"] = h["tab"]
                        st.rerun()
            elif q:
                st.caption("Sin coincidencias. Prueba con términos más cortos.")

    st.caption(PROJECT_DESCRIPTION)
    st.caption("Flujo principal: ingesta, Spark/GraphFrames y visualización operativa en el dashboard.")

    # --- Sidebar: servicios + paso temporal + acciones ---
    with st.sidebar:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=56)
        st.markdown(f"**{PROJECT_DISPLAY_NAME}**")
        st.caption(PROJECT_TAGLINE)
        st.divider()
        st.subheader("Estado de servicios")
        estado_stack = estado_servicios()
        for nombre, etiqueta in estado_stack.items():
            st.markdown(f"{etiqueta} **{nombre}**")

        todos_ok = all(
            v.startswith("✅") or "Activo" in v for v in estado_stack.values()
        )

        st.caption(
            f"Resumen **stack completo** ({len(ORDEN_SERVICIOS)} componentes). "
            f"Puertos típicos: `{PORT_HDFS}` HDFS · `{PORT_KAFKA}` Kafka · "
            f"`{PORT_CASSANDRA}` Cassandra (`{CASSANDRA_HOST}`) · `{PORT_SPARK_MASTER}` Spark · "
            f"`{PORT_HIVE}` Hive · `{PORT_AIRFLOW}` Airflow · `{PORT_API}` Swagger API · "
            f"`{PORT_FAQ_IA}` FAQ IA API · "
            f"`{PORT_NIFI_HTTPS}`/`{PORT_NIFI_HTTP}` NiFi."
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

                st.caption("Stack completo: HDFS → Cassandra → Kafka → Spark → Hive → Airflow → API → FAQ IA → NiFi.")
                if st.button(f"Ejecutar arranque completo ({len(ORDEN_SERVICIOS)} servicios)", key="btn_arrancar_todos"):
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

            with st.expander(f"Arrancar stack completo ({len(ORDEN_SERVICIOS)} servicios)", expanded=False):
                st.caption("Orden: HDFS → Cassandra → Kafka → Spark → Hive → Airflow → API → FAQ IA → NiFi.")
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
                    detalle_prueba = "Ingesta ejecutada desde Streamlit con paso automático."
                else:
                    st.session_state.timeline.append(f"{ts} — Ingesta OK (paso {st.session_state.paso_15min})")
                    detalle_prueba = f"Ingesta ejecutada desde Streamlit en el paso {st.session_state.paso_15min}."
                registrar_prueba_ingesta(
                    canal="Frontend Streamlit",
                    ejecutor="Sidebar Streamlit",
                    resultado="OK",
                    detalle=detalle_prueba,
                )
                st.success("Ingesta terminada.")
            else:
                registrar_prueba_ingesta(
                    canal="Frontend Streamlit",
                    ejecutor="Sidebar Streamlit",
                    resultado="FAIL",
                    detalle=f"Ingesta lanzada desde Streamlit terminó con código {code}.",
                    observaciones=(err[-1000:] or out[-1000:]),
                )
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
                registrar_prueba_ingesta(
                    canal="Frontend Streamlit",
                    ejecutor="Sidebar Streamlit",
                    resultado="OK",
                    detalle=f"Pipeline completo desde Streamlit con ingesta en paso {p} y procesamiento Spark posterior.",
                )
                st.success("Pipeline OK.")
            else:
                registrar_prueba_ingesta(
                    canal="Frontend Streamlit",
                    ejecutor="Sidebar Streamlit",
                    resultado="FAIL",
                    detalle=f"Pipeline completo desde Streamlit falló (ingesta {c1} | procesamiento {c2}).",
                    observaciones=((e1 or o1) + "\n---\n" + (e2 or o2))[-1500:],
                )
                st.error(f"Ingesta {c1} | Procesamiento {c2}")
                st.code((e1 or o1) + "\n---\n" + (e2 or o2))
            st.rerun()

        if st.session_state.timeline:
            st.divider()
            st.caption("Últimos eventos")
            for ev in reversed(st.session_state.timeline[-8:]):
                st.caption(ev)

    pref_tab = st.session_state.get("quick_open_tab")
    if "active_tab" not in st.session_state:
        st.session_state["active_tab"] = TAB_LABELS[0]
    if pref_tab in TAB_LABELS:
        st.session_state["active_tab"] = pref_tab
        st.session_state["quick_open_tab"] = None

    active_tab = st.radio(
        "Navegación",
        options=TAB_LABELS,
        horizontal=True,
        key="active_tab",
        label_visibility="collapsed",
    )

    if active_tab == "Ciclo KDD":
        _render_kdd_title_and_selector()
        st.info(
            "Las fases **1–2** se realizan en `ingesta/ingesta_kdd.py`. "
            "Las fases **3–5** se concentran en `procesamiento/procesamiento_grafos.py` (Spark)."
        )
        with st.container(border=True):
            st.markdown("**Estado actual de la fuente DGT**")
            _render_resumen_dgt_ui()
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

    if active_tab == "Resultados pipeline":
        render_pipeline_resultados_tab()

    if active_tab == "Pruebas":
        render_pruebas_ingesta_tab()

    if active_tab == "Flujo NiFi":
        render_nifi_flujo_tab()

    if active_tab == "Cuadro de mando":
        render_cuadro_mando_tab()

    if active_tab == "Asistente flota":
        render_asistente_flota_tab()

    if active_tab == "Rutas híbridas":
        render_rutas_hibridas_tab()

    if active_tab == "Gemelo digital":
        render_gemelo_digital_tab()

    if active_tab == "Servicios":
        render_panel_gestion_servicios()
        st.divider()
        render_faq_ia_panel()

    if active_tab == "Mapa y métricas":
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
                nodos_rt = cargar_estado_nodos_reconfiguracion()
                rutas_rt = cargar_estado_rutas_reconfiguracion()
                if not nodos_c:
                    st.warning(
                        "No hay datos en `nodos_estado` (o Cassandra no responde). "
                        "Ejecuta el procesamiento tras la ingesta."
                    )
                st.caption(
                    "**Mapa legible:** la red de **aristas** y las **rutas sugeridas** vienen "
                    "**desactivadas** por defecto. Usa el control de **capas** en el mapa (icono de capas) "
                    "para mostrarlas. Opcional: animación tipo «flujo» en las rutas."
                )
                anim_rutas = st.checkbox(
                    "Animar rutas sugeridas (efecto movimiento, AntPath)",
                    value=False,
                    key="mapa_operativo_anim_rutas",
                )
                mapa = crear_mapa(
                    nodos_c,
                    aristas_c,
                    track_c,
                    nodos_rt,
                    rutas_rt,
                    animar_rutas_sugeridas=anim_rutas,
                )
                st_folium(mapa, width=None, height=480, returned_objects=[], key="folium_operativo")
            else:
                st.subheader("Mapa de planificación (red + ruta principal + alternativas)")
                rh = st.session_state.get("rh_resultado")
                if rh and rh.get("ok"):
                    ruta = rh["ruta"]
                    alts = rh.get("alternativas") or []
                    mostrar_red = st.checkbox(
                        "Mostrar toda la red de fondo (conexiones grises)",
                        value=bool(st.session_state.get("rh_mapa_red", False)),
                        key="tab_mapa_mostrar_red_completa",
                        help="Por defecto desactivado: la malla completa satura el mapa. Misma opción que en Rutas híbridas.",
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
                alertas_rt = cargar_alertas_activas_cassandra()
                nodos_rt = cargar_estado_nodos_reconfiguracion()
                rutas_rt = cargar_estado_rutas_reconfiguracion()
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
                st.markdown("**Reconfiguración logística**")
                _render_resumen_reconfiguracion_ui(alertas_rt, nodos_rt, rutas_rt)
                st.divider()
                st.markdown("**Estado DGT y alertas**")
                _render_resumen_dgt_ui()
                st.divider()
                st.caption("Leyenda nodos: verde activo · rojo con X nodo caído · aristas grises desactivadas · rutas alternativas azules")
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

    if active_tab == "Verificación técnica":
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
