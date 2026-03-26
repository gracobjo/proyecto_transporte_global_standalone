"""
Widgets Streamlit: cuadro de mando, consultas supervisadas y slides de clima/retrasos.
"""
from __future__ import annotations

import time

from datetime import datetime, timezone
from typing import List, Tuple

import pandas as pd
import streamlit as st

from servicios.clima_retrasos import (
    evaluar_retraso_integrado,
    obtener_clima_todos_hubs_completo,
)
from servicios.consultas_cuadro_mando import (
    CASSANDRA_CONSULTAS,
    HIVE_CONSULTAS,
    ejecutar_cassandra_consulta,
    ejecutar_hive_consulta,
    listar_claves_cassandra,
    listar_claves_hive,
    titulo_cassandra,
    titulo_hive,
)
from servicios.ejecucion_pipeline import ejecutar_ingesta, ejecutar_procesamiento
from servicios.estado_y_datos import cargar_nodos_cassandra, verificar_kafka_topic
from servicios.kdd_operaciones import OPERACIONES_POR_FASE
from config import PROJECT_DISPLAY_NAME, TOPIC_TRANSPORTE


def _badge_tipo(tipo: str) -> str:
    colors = {
        "ingesta": "🟦",
        "spark": "🟧",
        "consulta_cassandra": "🟩",
        "consulta_hive": "🟪",
        "clima": "🌤️",
        "verificacion": "🔧",
    }
    return colors.get(tipo, "⬜")


def render_operaciones_por_fase() -> None:
    st.subheader("Operaciones por fase KDD")
    st.caption(
        "Supervisión del modelo de negocio: qué hace cada fase y qué consultas "
        "encajan con Cassandra (tiempo real) o Hive (histórico)."
    )
    for orden, ops in OPERACIONES_POR_FASE:
        with st.expander(f"**Fase {orden}** — { _titulo_fase(orden) }", expanded=(orden == 1)):
            for op in ops:
                c1, c2 = st.columns([1, 4])
                with c1:
                    st.markdown(_badge_tipo(op.tipo))
                    st.caption(op.tipo.replace("_", " "))
                with c2:
                    st.markdown(f"**{op.titulo}**")
                    st.caption(op.descripcion)


def _titulo_fase(orden: int) -> str:
    titulos = {
        1: "Selección",
        2: "Preprocesamiento",
        3: "Transformación",
        4: "Minería",
        5: "Interpretación",
    }
    return titulos.get(orden, f"Fase {orden}")


def render_acciones_rapidas() -> None:
    st.subheader("Acciones rápidas")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Ejecutar ingesta (fases 1–2)", key="cm_ingesta", width="stretch"):
            auto = bool(st.session_state.get("ingesta_paso_automatico", False))
            paso_arg = None if auto else int(st.session_state.get("paso_15min", 0))
            with st.spinner("Ingesta…"):
                code, out, err = ejecutar_ingesta(paso_arg)
            if code == 0:
                st.success("Ingesta OK.")
                st.session_state.timeline = st.session_state.get("timeline", []) + [
                    f"{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')} — Ingesta (cuadro)"
                ]
            else:
                st.error(f"Código {code}")
                st.code(err[-2500:] or out[-2500:])
    with c2:
        if st.button("Ejecutar Spark (fases 3–5)", key="cm_spark", width="stretch"):
            with st.spinner("Spark…"):
                code, out, err = ejecutar_procesamiento()
            if code == 0:
                st.success("Procesamiento OK.")
                st.session_state.timeline = st.session_state.get("timeline", []) + [
                    f"{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')} — Spark (cuadro)"
                ]
            else:
                st.error(f"Código {code}")
                st.code(err[-2500:] or out[-2500:])
    with c3:
        if st.button("Refrescar datos clima (API)", key="cm_clima", width="stretch"):
            st.session_state["clima_refresh"] = datetime.now(timezone.utc).isoformat()
            # Forzamos recarga de slides (OpenWeather) en el siguiente render
            st.session_state["slides_clima_ready"] = False
            st.rerun()


def render_consultas_cassandra() -> None:
    st.subheader("Consultas supervisadas — Cassandra")
    st.caption(
        "Solo consultas **aprobadas** (lectura). Para ver el estado actual de la red, nodos, "
        "aristas, tracking y PageRank."
    )
    claves = listar_claves_cassandra()
    etiquetas = {k: f"{titulo_cassandra(k)} (`{k}`)" for k in claves}
    sel = st.selectbox("Consulta", options=claves, format_func=lambda k: etiquetas[k], key="cql_sel")
    if st.button("Ejecutar consulta Cassandra", key="btn_cql", type="primary"):
        ok, err, rows = ejecutar_cassandra_consulta(sel)
        if ok and rows:
            st.dataframe(pd.DataFrame(rows), width="stretch")
            st.caption(f"{len(rows)} fila(s).")
        elif ok:
            st.info("Consulta vacía (sin filas o tablas sin datos).")
        else:
            st.error(err)
    with st.expander("CQL ejecutado (referencia)"):
        st.code(CASSANDRA_CONSULTAS[sel]["cql"], language="sql")


def render_consultas_hive() -> None:
    st.subheader("Consultas supervisadas — Hive (histórico)")
    st.caption(
        "Requiere **HiveServer2** (puerto **10000**). Las consultas usan **PyHive** (Thrift), no `beeline`. "
        "Variables: `HIVE_JDBC_URL` o `HIVE_SERVER` (ej. `jdbc:hive2://localhost:10000`), "
        "`SIMLOG_HIVE_BEELINE_USER` / usuario efectivo (`hadoop`). "
        "Timeout: `HIVE_QUERY_TIMEOUT_SEC=300`. Si va lento, prueba `diag_smoke_hive` (SELECT 1). "
        "El fallback Spark tras timeout suele fallar con metastore Derby; deja `SIMLOG_HIVE_EXEC_FALLBACK_SPARK=0`. "
        "Por defecto se consultan `historico_nodos` y `nodos_maestro` (como escribe Spark). "
        "Si tus tablas tienen otro nombre, define `SIMLOG_HIVE_TABLE_HISTORICO_NODOS` y "
        "`SIMLOG_HIVE_TABLE_NODOS_MAESTRO` en el entorno o en `.env`."
    )
    claves = listar_claves_hive()
    # Para evitar que el usuario no encuentre el diagnóstico (por scroll/visibilidad),
    # priorizamos estas consultas 24h al principio de la lista.
    claves_rapidas = [
        "diag_smoke_hive",
        "diag_fecha_proceso_24h",
        "historico_nodos_muestra_24h",
        "severidad_resumen_24h",
    ]
    claves_ordenadas = [k for k in claves_rapidas if k in claves] + [k for k in claves if k not in claves_rapidas]
    etiquetas = {k: f"{titulo_hive(k)} (`{k}`)" for k in claves}
    # `st.selectbox` (dropdown) a veces no se navega bien con teclado/flechas.
    # Usamos `st.radio` para una lista visible y accesible.
    label_a_clave = {etiquetas[k]: k for k in claves_ordenadas}
    sel_label = st.radio("Consulta", options=list(label_a_clave.keys()), key="hive_sel_radio")
    sel = label_a_clave[sel_label]
    if st.button("Ejecutar consulta Hive", key="btn_hive", type="primary"):
        t0 = time.monotonic()
        with st.spinner("Ejecutando Hive (PyHive)…"):
            ok, err, out = ejecutar_hive_consulta(sel)
        elapsed = time.monotonic() - t0
        if ok:
            out_s = out or ""
            st.caption(f"Tiempo Hive: {elapsed:.1f}s")
            st.caption(f"Salida TSV: {len(out_s)} caracteres.")
            if not out_s.strip():
                st.caption("La consulta se ejecutó pero Hive devolvió salida TSV vacía (posible: 0 filas en las últimas 24h).")
                st.caption("Sugerencia: prueba `Resumen severidad (últimas 24h) — estado derivado` o `Muestra histórico (últimas 24h) — nodos`.")
            else:
                st.text_area("Salida (TSV)", value=out_s, height=280)
                st.caption(f"Preview: {out_s[:300].replace(chr(10),' ')}")
        else:
            st.error(err)
            if out:
                st.text_area("Detalle", value=out, height=160)
    with st.expander("SQL ejecutado"):
        st.code(HIVE_CONSULTAS[sel]["sql"], language="sql")


def render_slides_clima_retrasos() -> None:
    st.subheader("Slides — Clima y anticipación de retrasos")
    st.info(
        "Esta vista resume **riesgo por hub**. Para la **planificación de transporte** en la red completa "
        "(todas las conexiones hub/subnodo, formulario origen→destino, **pasos**, **mapa** con ruta "
        "principal y **alternativas** en otro color), abre la pestaña **Rutas híbridas**."
    )
    st.caption(
        "Cada **slide** corresponde a un hub. Se combinan variables OpenWeather (tormenta, nieve, "
        "lluvia, niebla, viento, visibilidad) con el **estado operativo** en Cassandra (atascos, "
        "obras, bloqueos simulados) para estimar un margen de retraso orientativo."
    )

    # Evita que cada recarga del dashboard haga 5 llamadas HTTP + lecturas Cassandra.
    # Esto hace que el tab "Cuadro de mando" sea rápido incluso si OpenWeather/Hive/Cassandra están lentos.
    if not st.session_state.get("slides_clima_ready"):
        st.caption(
            "Para generar las slides (requiere OpenWeather y Cassandra), pulsa el botón. "
            "Así la app carga rápido y no se bloquea al refrescar la página."
        )
        if st.button("Cargar clima y generar slides", key="slides_clima_load"):
            st.session_state["slides_clima_ready"] = True
            st.rerun()
        return

    nodos = cargar_nodos_cassandra()
    clima = obtener_clima_todos_hubs_completo()
    hubs_order: List[str] = ["Madrid", "Barcelona", "Bilbao", "Vigo", "Sevilla"]

    slides: List[Tuple[str, dict, dict]] = []
    for h in hubs_order:
        if h not in clima:
            continue
        raw = clima[h]
        ev = evaluar_retraso_integrado(raw, nodos)
        slides.append((h, raw, ev))

    if not slides:
        st.warning("No se pudo obtener clima para los hubs.")
        return

    # Slide de síntesis
    totales = [s[2].get("minutos_estimados", 0) for s in slides]
    max_hub = max(slides, key=lambda x: x[2].get("minutos_estimados", 0))[0]
    slides.append(
        (
            "Síntesis red nacional",
            {"hub": "Síntesis", "descripcion": "Agregado orientativo 5 hubs"},
            {
                "hub": "Síntesis",
                "nivel_riesgo": "alto" if max(totales) >= 45 else ("medio" if max(totales) >= 15 else "bajo"),
                "minutos_estimados": int(sum(totales) / max(len(totales), 1)),
                "factores": [
                    f"Hub con mayor margen estimado: **{max_hub}**",
                    "Comparar con tracking en Cassandra y con histórico Hive para validar tendencias.",
                ],
                "desglose": {},
            },
        )
    )

    n_slides = len(slides)
    idx = st.slider("Slide (hub)", min_value=0, max_value=n_slides - 1, value=0, key="slide_clima")
    hub, raw, ev = slides[idx]

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Hub", hub)
        st.metric("Riesgo operativo (heurístico)", str(ev.get("nivel_riesgo", "—")).upper())
    with c2:
        st.metric("Margen de retraso estimado (min)", f"{ev.get('minutos_estimados', 0)}")
        if ev.get("categoria"):
            st.caption(f"Categoría OWM: **{ev['categoria']}**")
    with c3:
        if raw.get("temp") is not None:
            st.metric("Temperatura (°C)", f"{raw['temp']:.1f}")
        if raw.get("viento_vel") is not None:
            st.metric("Viento (m/s)", f"{raw['viento_vel']:.1f}")

    if hub != "Síntesis red nacional" and not raw.get("error"):
        st.markdown("**Variables meteorológicas (OpenWeather)**")
        mcols = st.columns(4)
        with mcols[0]:
            st.write("Descripción:", raw.get("descripcion", "—"))
        with mcols[1]:
            st.write("Humedad %:", raw.get("humedad", "—"))
        with mcols[2]:
            st.write("Visibilidad (m):", raw.get("visibilidad_m", "—"))
        with mcols[3]:
            st.write("Nubosidad %:", raw.get("nubosidad_pct", "—"))
        if raw.get("lluvia_1h_mm") is not None:
            st.caption(f"Lluvia 1h: {raw['lluvia_1h_mm']} mm")
        if raw.get("nieve_1h_mm") is not None:
            st.caption(f"Nieve 1h: {raw['nieve_1h_mm']} mm")

    st.markdown("**Factores de retraso (clima + operación)**")
    for f in ev.get("factores", []):
        st.markdown(f"- {f}")
    if ev.get("desglose"):
        st.caption(
            f"Desglose min: clima {ev['desglose'].get('clima_base_min', 0)} · "
            f"físicos {ev['desglose'].get('fisicos_min', 0)} · "
            f"operativo {ev['desglose'].get('operativo_min', 0)}"
        )

    st.markdown("---")
    st.caption(
        f"{PROJECT_DISPLAY_NAME} — estimación no contractual. "
        "Incluye analogías de incidencias: tormenta, granizo, nieve, niebla, atascos, obras, etc., "
        "según códigos OWM y estado de nodos en Cassandra."
    )


def render_verificacion_kafka_cuadro() -> None:
    st.caption("Kafka (topic del proyecto)")
    st.info(verificar_kafka_topic(TOPIC_TRANSPORTE))


def render_cuadro_mando_tab() -> None:
    """Contenido completo de la pestaña Cuadro de mando."""
    st.header("Cuadro de mando")
    st.markdown(
        "Vista operativa del ciclo KDD: **tareas por fase**, consultas a **Cassandra** (datos en vivo) "
        "y **Hive** (histórico), y **slides** de anticipación de retrasos por clima e incidencias."
    )
    render_operaciones_por_fase()
    render_acciones_rapidas()
    render_verificacion_kafka_cuadro()
    st.divider()
    render_consultas_cassandra()
    st.divider()
    render_consultas_hive()
    st.divider()
    render_slides_clima_retrasos()
