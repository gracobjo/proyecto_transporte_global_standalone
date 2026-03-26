"""
Asistente de flota — lenguaje natural → consultas supervisadas (Cassandra / Hive).

Diccionario principal: `servicios/gestor_consultas_sql.py` (preguntas tipo gestor).
"""
from __future__ import annotations

import io
from typing import Optional, Tuple

import pandas as pd
import streamlit as st

from servicios.consultas_cuadro_mando import ejecutar_hive_sql_seguro
from servicios.gestor_consultas_sql import aplicar_postproceso_gestor, resolver_intencion_gestor


def _cassandra_a_dataframe(cql: str) -> Tuple[pd.DataFrame, str]:
    try:
        from cassandra.cluster import Cluster
        from config import CASSANDRA_HOST

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect()
        rows = list(session.execute(cql))
        cluster.shutdown()
        if not rows:
            return pd.DataFrame(), ""
        first = rows[0]
        if hasattr(first, "_asdict"):
            data = [r._asdict() for r in rows]
        else:
            data = [dict(r) for r in rows]
        return pd.DataFrame(data), ""
    except Exception as e:
        return pd.DataFrame(), str(e)


def _hive_tsv_a_dataframe(tsv: str) -> pd.DataFrame:
    if not (tsv or "").strip():
        return pd.DataFrame()
    return pd.read_csv(io.StringIO(tsv), sep="\t")


def ejecutar_consulta_asistente(
    motor: str, sql: str, etiqueta: str
) -> Tuple[Optional[pd.DataFrame], str]:
    """Devuelve (DataFrame o None, mensaje_error)."""
    if motor == "cassandra":
        df, err = _cassandra_a_dataframe(sql)
        if err:
            return None, err
        df = aplicar_postproceso_gestor(etiqueta, df)
        return df, ""
    if motor == "hive":
        ok, err, out = ejecutar_hive_sql_seguro(sql)
        if not ok:
            return None, err or "Error Hive"
        return _hive_tsv_a_dataframe(out or ""), ""
    return None, f"Motor desconocido: {motor}"


def render_asistente_flota_tab() -> None:
    st.subheader("Asistente de flota")
    st.caption(
        "Preguntas en **lenguaje natural** se traducen a consultas **supervisadas** "
        "(Cassandra tiempo real · Hive histórico). "
        "Ejemplos: «¿Dónde están mis camiones ahora?», «¿Qué carreteras están cortadas?», "
        "«¿Cuál es el nodo más crítico?», «Historial del camión 1»."
    )
    with st.expander("Diccionario gestor → SQL", expanded=False):
        st.markdown(
            "| Pregunta tipo | Origen | Consulta |\n"
            "|---|---|---|\n"
            "| ¿Dónde están mis camiones? / GPS / mapa | Cassandra | `tracking_camiones` (sin `nodo_actual` en esquema: se usan rutas y posición) |\n"
            "| Tráfico / ciudades con incidencia | Cassandra | `nodos_estado` con `Bloqueado` |\n"
            "| Nodo logístico crítico (PageRank) | Cassandra | `pagerank_nodos` + **top 1 en la app** |\n"
            "| Carreteras cortadas | Cassandra | `aristas_estado` con `Bloqueado` |\n"
            "| Historial de rutas de un camión | Hive | `SELECT *` sobre tabla `SIMLOG_HIVE_TABLA_TRANSPORTE` |\n"
        )
        st.caption(
            "Hive: ajusta `SIMLOG_HIVE_TABLA_TRANSPORTE` y opcionalmente "
            "`SIMLOG_HIVE_SQL_HISTORIAL_CAMION` (plantilla con `{id_camion}`) si tu DDL difiere."
        )

    if "asistente_flota_hist" not in st.session_state:
        st.session_state.asistente_flota_hist = []

    c_clear, _ = st.columns([1, 4])
    with c_clear:
        if st.session_state.asistente_flota_hist and st.button(
            "Limpiar conversación", key="asistente_flota_clear_top"
        ):
            st.session_state.asistente_flota_hist = []
            st.rerun()

    for i, turn in enumerate(st.session_state.asistente_flota_hist):
        with st.chat_message("user"):
            st.write(turn["pregunta"])
        with st.chat_message("assistant"):
            if turn.get("error"):
                st.error(turn["error"])
            elif turn.get("df") is not None:
                if turn["df"].empty:
                    st.info("Consulta correcta pero sin filas.")
                else:
                    st.dataframe(turn["df"], width="stretch")
            if turn.get("sql"):
                if st.toggle("Ver consulta SQL", key=f"asistente_flota_ver_sql_{i}"):
                    st.code(turn["sql"], language="sql")
            if turn.get("motor"):
                st.caption(f"Motor: **{turn.get('motor', '—')}** · intención: `{turn.get('intencion', '')}`")

    prompt = st.chat_input("Ej.: ¿Dónde están mis camiones ahora?")
    if prompt:
        resuelto = resolver_intencion_gestor(prompt)
        if not resuelto:
            st.session_state.asistente_flota_hist.append(
                {
                    "pregunta": prompt,
                    "error": "No reconozco la intención. Prueba las preguntas del diccionario (arriba).",
                    "df": None,
                    "sql": None,
                    "motor": None,
                    "intencion": None,
                }
            )
        else:
            motor, sql, etiqueta = resuelto
            df, err = ejecutar_consulta_asistente(motor, sql, etiqueta)
            st.session_state.asistente_flota_hist.append(
                {
                    "pregunta": prompt,
                    "error": err or None,
                    "df": df if not err else None,
                    "sql": sql,
                    "motor": motor,
                    "intencion": etiqueta,
                }
            )
        if len(st.session_state.asistente_flota_hist) > 24:
            st.session_state.asistente_flota_hist = st.session_state.asistente_flota_hist[-24:]
        st.rerun()
