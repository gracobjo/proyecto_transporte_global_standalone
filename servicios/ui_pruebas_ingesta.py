"""
Pestaña Streamlit: trazabilidad y registro de pruebas de ingesta.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from servicios.pruebas_ingesta import (
    REGISTRO_PATH,
    capturar_snapshot_pruebas,
    describir_canales_ingesta,
    leer_registro_pruebas,
    registrar_prueba_ingesta,
    resumen_tabular_pruebas,
)


def _valor(snapshot: dict, *keys):
    cur = snapshot
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def render_pruebas_ingesta_tab() -> None:
    st.subheader("Pruebas de ingesta y trazabilidad")
    st.caption(
        "Aquí queda constancia de qué pruebas se han hecho, por qué canal de ingesta se ejecutaron "
        "y cuál fue el resultado. En una ingesta es normal que crezcan los históricos "
        "(HDFS JSON, `eventos_historico`, `historico_nodos`, `transporte_ingesta_completa`)."
    )
    st.caption("Plan completo de pruebas: `docs/PLAN_PRUEBAS_KDD.md`.")

    st.markdown("**Canales de ingesta contemplados**")
    cols = st.columns(3)
    for col, canal in zip(cols, describir_canales_ingesta()):
        with col:
            with st.container(border=True):
                st.markdown(f"**{canal['canal']}**")
                st.caption(canal["ejecucion"])
                st.write(canal["detalle"])
                st.caption(canal["evidencia"])

    c1, c2 = st.columns([1, 2])
    with c1:
        if st.button("Actualizar snapshot de pruebas", type="primary", key="btn_snapshot_pruebas"):
            st.session_state["snapshot_pruebas_ingesta"] = capturar_snapshot_pruebas()
    with c2:
        st.caption(f"Registro persistente: `{REGISTRO_PATH}`")

    snapshot = st.session_state.get("snapshot_pruebas_ingesta")
    if snapshot is None:
        snapshot = capturar_snapshot_pruebas()
        st.session_state["snapshot_pruebas_ingesta"] = snapshot

    st.markdown("**Snapshot actual**")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("HDFS JSON", _valor(snapshot, "hdfs", "total_json") or 0)
    m2.metric("Eventos Cassandra", _valor(snapshot, "cassandra", "eventos_historico") or 0)
    m3.metric("Hive histórico nodos", _valor(snapshot, "hive", "historico_nodos") or "—")
    m4.metric("Hive transporte", _valor(snapshot, "hive", "transporte_ingesta_completa") or "—")

    c_ev1, c_ev2, c_ev3 = st.columns(3)
    with c_ev1:
        with st.container(border=True):
            st.markdown("**Última ingesta local**")
            ing = snapshot.get("ingesta_local", {})
            if ing.get("disponible"):
                meta = ing.get("meta", {})
                st.caption(f"Origen: `{meta.get('origen', 'desconocido')}`")
                st.caption(f"Canal: `{meta.get('canal_ingesta', '—')}`")
                st.caption(f"Timestamp: `{ing.get('timestamp', '—')}`")
                st.caption(f"Camiones: `{ing.get('camiones', '—')}`")
            else:
                st.caption(ing.get("mensaje", "Sin evidencias locales."))
    with c_ev2:
        with st.container(border=True):
            st.markdown("**Evidencia Airflow**")
            air = snapshot.get("airflow", {})
            if air.get("disponible"):
                st.caption(f"Run: `{air.get('run_id', '—')}`")
                st.caption(f"Informe: `{air.get('archivo', '—')}`")
                st.caption(f"UTC: `{air.get('modificado', '—')}`")
            else:
                st.caption(air.get("mensaje", "Sin informes Airflow todavía."))
    with c_ev3:
        with st.container(border=True):
            st.markdown("**Estado NiFi**")
            nifi = snapshot.get("nifi", {})
            st.caption(f"Activo: `{'sí' if nifi.get('activo') else 'no'}`")
            st.caption(nifi.get("detalle", "Sin detalle"))

    with st.expander("Registrar una prueba", expanded=True):
        canal = st.selectbox(
            "Canal probado",
            options=["NiFi", "Airflow", "Script sin GUI", "Frontend Streamlit"],
            key="prueba_canal_sel",
        )
        ejecutor_default = {
            "NiFi": "PG_SIMLOG_KDD",
            "Airflow": "simlog_pipeline_maestro / simlog_kdd_*",
            "Script sin GUI": "venv_transporte/bin/python -m ingesta.ingesta_kdd",
            "Frontend Streamlit": "Sidebar Streamlit",
        }[canal]
        ejecutor = st.text_input("Medio / ejecutor", value=ejecutor_default, key="prueba_ejecutor_txt")
        resultado = st.selectbox("Resultado", options=["OK", "WARN", "FAIL"], key="prueba_resultado_sel")
        detalle = st.text_input(
            "Resumen del resultado",
            value="La prueba deja evidencia y los contadores históricos son coherentes.",
            key="prueba_detalle_txt",
        )
        observaciones = st.text_area(
            "Observaciones",
            value="",
            placeholder="Ej.: Airflow generó informe y el total de eventos históricos aumentó.",
            key="prueba_obs_txt",
        )
        if st.button("Guardar prueba en registro", key="btn_guardar_prueba_ingesta"):
            entry = registrar_prueba_ingesta(
                canal=canal,
                ejecutor=ejecutor,
                resultado=resultado,
                detalle=detalle,
                observaciones=observaciones,
            )
            st.session_state["snapshot_pruebas_ingesta"] = entry["snapshot"]
            st.success("Prueba registrada.")

    filas = resumen_tabular_pruebas()
    st.markdown("**Histórico de pruebas registradas**")
    if filas:
        st.dataframe(pd.DataFrame(filas), use_container_width=True, hide_index=True)
    else:
        st.info("Todavía no hay pruebas registradas.")

    registro = leer_registro_pruebas()
    if registro:
        with st.expander("Detalle de las últimas pruebas", expanded=False):
            for item in reversed(registro[-5:]):
                st.markdown(
                    f"**{item.get('timestamp', '')} · {item.get('canal', '')} · {item.get('resultado', '')}**"
                )
                st.caption(f"Ejecutor: `{item.get('ejecutor', '')}`")
                st.caption(item.get("detalle", ""))
                if item.get("observaciones"):
                    st.write(item["observaciones"])
                deltas = item.get("deltas", {})
                st.caption(
                    "Incrementos: "
                    f"HDFS `{deltas.get('hdfs_json')}` · "
                    f"eventos Cassandra `{deltas.get('cassandra_eventos')}` · "
                    f"Hive histórico `{deltas.get('hive_historico_nodos')}` · "
                    f"Hive transporte `{deltas.get('hive_transporte')}`"
                )
