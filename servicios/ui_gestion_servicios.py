"""Panel Streamlit: iniciar, comprobar y parar servicios del stack."""
from __future__ import annotations

import streamlit as st

from servicios.gestion_servicios import ORDEN_SERVICIOS, ejecutar_comprobar, ejecutar_iniciar, ejecutar_parar
from servicios.ui_servicios_web import render_bloque_interfaz_web


def render_panel_gestion_servicios() -> None:
    st.header("Gestión de servicios del stack")
    st.markdown(
        "**HDFS**, **Kafka**, **Cassandra**, **Spark** (master opcional), **HiveServer2**, "
        "**Airflow** (api-server) y **NiFi**. Rutas típicas: `HADOOP_HOME`, `KAFKA_HOME`, "
        "`SPARK_HOME`, `HIVE_HOME`, `NIFI_HOME`; `AIRFLOW_HOME` para Airflow. "
        "En cada bloque, **Interfaz web** enlaza la consola HTTP (URL, puerto, usuario/contraseña vía variables `SIMLOG_UI_*`)."
    )
    st.warning(
        "⚠️ **Parar** puede interrumpir trabajos y datos en memoria. "
        "**Comprobar** solo lee puertos TCP locales."
    )

    confirm = st.checkbox(
        "Habilitar botones **Parar** (confirmo que entiendo el riesgo)",
        value=False,
        key="svc_confirm_stop",
    )

    if st.button("Comprobar todos los servicios", type="secondary", use_container_width=True):
        for sid in ORDEN_SERVICIOS:
            r = ejecutar_comprobar(sid)
            estado = "✅" if r.get("activo") else "❌"
            st.markdown(f"{estado} **{r.get('nombre', sid)}** — {r.get('detalle', '')}")

    st.divider()

    for sid in ORDEN_SERVICIOS:
        res = ejecutar_comprobar(sid)
        nombre = res.get("nombre", sid)
        activo = res.get("activo", False)
        detalle = res.get("detalle", "")

        with st.container(border=True):
            c0, c1, c2, c3, c4 = st.columns([2, 2, 1, 1, 1])
            with c0:
                st.markdown(f"### {nombre}")
            with c1:
                if activo:
                    st.success("Activo")
                else:
                    st.error("Inactivo / no responde")
                st.caption(detalle[:280] + ("…" if len(detalle) > 280 else ""))
            with c2:
                if st.button("Iniciar", key=f"svc_start_{sid}", use_container_width=True):
                    msg = ejecutar_iniciar(sid)
                    st.info(msg)
            with c3:
                if st.button("Comprobar", key=f"svc_check_{sid}", use_container_width=True):
                    r = ejecutar_comprobar(sid)
                    st.info(f"{r.get('nombre')}: {'OK' if r.get('activo') else 'no responde'} — {r.get('detalle', '')}")
            with c4:
                if st.button(
                    "Parar",
                    key=f"svc_stop_{sid}",
                    use_container_width=True,
                    disabled=not confirm,
                    type="secondary",
                ):
                    msg = ejecutar_parar(sid)
                    st.warning(msg)

            render_bloque_interfaz_web(sid)
