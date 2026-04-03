"""
Pestaña Streamlit: integración KNIME ↔ Hive (modo ligero, poca RAM en el cluster).

KNIME Analytics Platform se ejecuta en el puesto del analista; el servidor solo debe
exponer HiveServer2 (+ HDFS/metastore) cuando se extraen datos.
"""
from __future__ import annotations

from pathlib import Path

import streamlit as st

from config import HIVE_DB, HIVE_JDBC_URL, HIVE_SERVER, HIVE_TABLE_TRANSPORTE_HIST

ROOT = Path(__file__).resolve().parents[1]
DOC_KNIME = ROOT / "docs" / "INTEGRACION_KNIME_HIVE.md"
SQL_DATASET = ROOT / "sql" / "hive_dataset_entrenamiento_knime.hql"
JDBC_EXAMPLE = ROOT / "docs" / "knime" / "jdbc_connection_template.properties.example"


def render_knime_tab() -> None:
    st.header("KNIME / IA avanzada (analítica sobre Hive)")
    st.caption(
        "Integración **off-cluster**: KNIME en tu PC lee datos vía **JDBC Hive**. "
        "En equipos con **poca RAM** no se instala KNIME en el servidor Hadoop; solo mantén "
        "los servicios mínimos cuando vayas a entrenar o exportar features."
    )

    st.info(
        "**Servicios mínimos recomendados** para usar KNIME contra este proyecto: "
        "**HDFS** (si el warehouse está en HDFS) + **Metastore** + **HiveServer2**. "
        "No son necesarios NiFi, Spark ni Airflow solo para consultar Hive desde KNIME."
    )

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Base Hive (`HIVE_DB`)", HIVE_DB)
    with c2:
        st.metric("Tabla transporte (config)", HIVE_TABLE_TRANSPORTE_HIST)

    jdbc = (HIVE_JDBC_URL or "").strip().rstrip("/")
    if "jdbc:hive2://" in jdbc and not jdbc.endswith(f"/{HIVE_DB}"):
        jdbc_display = f"{jdbc}/{HIVE_DB}"
    else:
        jdbc_display = jdbc or f"jdbc:hive2://127.0.0.1:10000/{HIVE_DB}"

    st.subheader("Conexión JDBC (KNIME Database Connector)")
    st.code(jdbc_display, language="text")
    st.caption(f"Origen: `HIVE_JDBC_URL` / `HIVE_SERVER` → `{HIVE_SERVER}`")

    with st.expander("Plantilla de propiedades JDBC (ejemplo)", expanded=False):
        if JDBC_EXAMPLE.exists():
            st.code(JDBC_EXAMPLE.read_text(encoding="utf-8"), language="properties")
        else:
            st.caption("No se encontró `docs/knime/jdbc_connection_template.properties.example`.")

    with st.expander("Consulta Hive — dataset de entrenamiento (KNIME)", expanded=True):
        st.markdown(
            f"Fichero versionado: `{SQL_DATASET.relative_to(ROOT)}` (ajusta nombre de tabla "
            f"si `SIMLOG_HIVE_TABLA_TRANSPORTE` ≠ `transporte_ingesta_completa`)."
        )
        if SQL_DATASET.exists():
            st.code(SQL_DATASET.read_text(encoding="utf-8"), language="sql")
        else:
            st.warning("No se encontró el fichero SQL.")

    with st.expander("Documentación técnica completa (pipeline KNIME, PMML, FastAPI)", expanded=False):
        if DOC_KNIME.exists():
            st.markdown(DOC_KNIME.read_text(encoding="utf-8"))
        else:
            st.error(f"No se encontró {DOC_KNIME}")

    st.subheader("Resumen rápido del workflow")
    st.markdown(
        """
1. **DB Connector** → JDBC Hive (URL de arriba).
2. **DB Reader** → pega la consulta del `.hql` (añade `LIMIT` en desarrollo).
3. Limpieza + **Column Filter** + **Normalizer** (opcional).
4. **Partitioning** train/test estratificado sobre `congestion`.
5. **Random Forest Learner** + **Predictor** + **Scorer** (accuracy, F1, …).
6. Exportar: **PMML** o modelo KNIME; scoring en producción vía **FastAPI** (ver `servicios/api_simlog.py` como patrón).
        """
    )

    st.divider()
    st.caption(
        "Para equipos con muy poca RAM: reduce filas en Hive (`LIMIT`), usa árboles poco profundos "
        "y cierra el stack pesado (Spark, Airflow) cuando solo necesites Hive + KNIME."
    )
