"""
Pestaña Streamlit: comprobación visual del pipeline (ingesta → Kafka/HDFS → Spark → Cassandra/Hive).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from config import (
    CASSANDRA_HOST,
    HDFS_BACKUP_PATH,
    HIVE_DB,
    KAFKA_BOOTSTRAP,
    KEYSPACE,
    TOPIC_RAW,
    TOPIC_TRANSPORTE,
)
from servicios.consultas_cuadro_mando import titulo_hive
from servicios.pipeline_verificacion import (
    hive_resumen,
    kafka_crear_topic_si_falta,
    obtener_snapshot_pipeline,
)


def render_pipeline_resultados_tab() -> None:
    st.subheader("Resultados del pipeline KDD")
    st.caption(
        "Comprueba **fase a fase** que la ingesta generó datos, que hay copia en **HDFS**, "
        "que **Kafka** tiene el topic activo, que **Spark** ha dejado estado en **Cassandra** "
        "y que el **histórico Hive** está accesible (si HiveServer2 está en marcha; la UI usa PyHive al puerto 10000)."
    )

    c_btn, c_topics = st.columns([1, 2])
    with c_btn:
        refresh = st.button("🔄 Actualizar comprobaciones", type="primary", key="btn_refresh_pipeline")
    with c_topics:
        crear_topics = st.button(
            "Crear temas Kafka (raw + filtered) si faltan",
            key="btn_kafka_create_topics",
            help="Ejecuta kafka-topics --create (usa KAFKA_HOME si está definido).",
        )

    if crear_topics:
        with st.spinner("Creando topics en Kafka…"):
            r1 = kafka_crear_topic_si_falta(KAFKA_BOOTSTRAP, TOPIC_RAW)
            r2 = kafka_crear_topic_si_falta(KAFKA_BOOTSTRAP, TOPIC_TRANSPORTE)
        for label, r in (("transporte_raw", r1), ("transporte_filtered", r2)):
            if r.get("ok"):
                st.success(f"{label}: {r.get('mensaje', 'OK')}")
            else:
                st.error(f"{label}: {r.get('mensaje', 'error')}")

    if refresh:
        with st.spinner("Consultando servicios y ficheros…"):
            st.session_state["pipeline_snapshot_kdd"] = obtener_snapshot_pipeline(
                hdfs_path=HDFS_BACKUP_PATH,
                kafka_bootstrap=KAFKA_BOOTSTRAP,
                topic=TOPIC_TRANSPORTE,
                cassandra_host=CASSANDRA_HOST,
                keyspace=KEYSPACE,
                cassandra_modo="rapido",
                incluir_hive=False,
                kafka_modo="rapido",
                hdfs_max_items=3,
                hdfs_timeout=28,
                kafka_timeout_describe=25,
                kafka_timeout_list=22,
            )

    snap = st.session_state.get("pipeline_snapshot_kdd")
    if snap is None:
        st.info(
            "Pulsa `Actualizar comprobaciones` para consultar servicios y ficheros. "
            "Esto puede tardar (HDFS/Kafka/Cassandra/Hive)."
        )
        return

    # --- 1–2 Ingesta ---
    with st.expander("**1–2 · Ingesta** (clima, simulación, payload JSON)", expanded=True):
        ing = snap["ingesta_local"]
        if ing.get("disponible"):
            st.success("Última ingesta registrada en disco (vista compatible con Spark `fase_kdd_spark`).")
            c1, c2, c3, c4 = st.columns(4)
            if ing.get("timestamp"):
                c1.metric("Timestamp payload", str(ing["timestamp"])[:19])
            if ing.get("paso_15min") is not None:
                c2.metric("Paso 15 min", str(ing["paso_15min"]))
            c3.metric("Hubs con clima", str(ing.get("hubs_clima", "—")))
            c4.metric("Camiones simulados", str(ing.get("camiones", "—")))
            if "meta" in ing:
                m = ing["meta"]
                st.write(
                    f"**Kafka publicado:** {'✅' if m.get('ok_kafka') else '❌'} · "
                    f"**HDFS backup:** {'✅' if m.get('ok_hdfs') else '❌'}"
                )
            p = Path(ing["ruta_payload"])
            if p.exists():
                with st.expander("Vista previa JSON (truncada)", expanded=False):
                    raw = p.read_text(encoding="utf-8")[:8000]
                    st.code(raw, language="json")
        else:
            st.warning(ing.get("mensaje", "Sin datos de ingesta local."))
            st.caption(f"Ruta esperada: `{ing.get('ruta_payload')}`")

    # --- Kafka + HDFS (ingesta escribe ambos) ---
    with st.expander("**Kafka + HDFS** (mensajes y backup JSON)", expanded=True):
        k = snap["kafka"]
        st.markdown(f"**Bootstrap:** `{k['bootstrap']}` · **Topic:** `{k['topic']}`")
        if k.get("kafka_topics_bin"):
            st.caption(f"Cliente: `{k['kafka_topics_bin']}`")
        if k.get("topic_existe"):
            st.success("Topic presente en el clúster Kafka (describe o listado).")
        else:
            st.warning(
                "No se confirmó el topic (describe/list fallaron o hicieron timeout). "
                "Pulsa «Crear temas Kafka» arriba o `bash sql/crear_temas_kafka.sh`."
            )
        if k.get("describe_ok"):
            st.text_area("Describe topic", k.get("describe_salida", ""), height=120, disabled=True, key="kafka_desc")
        if k.get("offsets"):
            st.text_area("Offsets / particiones", k["offsets"], height=140, disabled=True, key="kafka_off")
        elif k.get("nota_offsets"):
            st.info(k["nota_offsets"])

        st.divider()
        h = snap["hdfs_backup"]
        st.markdown(f"**Ruta HDFS ingesta:** `{HDFS_BACKUP_PATH}`")
        if h.get("ok"):
            st.success(f"HDFS accesible — JSON listados: **{h.get('total_json_listados', 0)}**")
            ult = h.get("ultimos") or []
            if ult:
                df = pd.DataFrame(ult)
                st.dataframe(df, width="stretch", hide_index=True)
            else:
                st.caption("No se encontraron ficheros `.json` en esa ruta (¿primera ingesta?)")
        else:
            det = h.get("detalle", "Error HDFS")
            st.error(det)
            if "timeout" in str(det).lower():
                st.info(
                    "Si HDFS responde lento, sube el tiempo de espera en código "
                    "(`hdfs_timeout` en `obtener_snapshot_pipeline`) o revisa el NameNode."
                )

    # --- Spark → Cassandra ---
    with st.expander("**3–5 · Spark → Cassandra** (estado actual del gemelo)", expanded=True):
        st.caption(
            "Tras **procesamiento Spark** (`procesamiento_grafos`), las tablas operativas se rellenan en "
            f"`{KEYSPACE}`."
        )
        cas = snap["cassandra"]
        if cas.get("ok"):
            rows = []
            for nombre, info in (cas.get("tablas") or {}).items():
                if info.get("ok"):
                    rows.append({"tabla": nombre, "filas": info.get("filas", 0), "estado": "✅"})
                else:
                    rows.append({"tabla": nombre, "filas": "—", "estado": f"⚠️ {info.get('error', '')}"})
            if rows:
                st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
            st.success("Lectura Cassandra OK.")
        else:
            st.error(cas.get("error", "No se pudo conectar a Cassandra."))

    # --- Hive histórico ---
    with st.expander("**Hive** (histórico particionado — opcional)", expanded=False):
        st.caption(
            f"Base esperada: **`{HIVE_DB}`**. Requiere HiveServer2 y `beeline` en PATH "
            "(o variable `HIVE_BEELINE_BIN`)."
        )
        hv = snap["hive"]
        # En modo rápido omitimos Hive para no bloquear la UI por HiveServer2.
        if hv.get("omitido_modo_rapido"):
            st.warning(hv.get("error_tablas", "Hive omitido en modo rápido."))
            if st.button("Cargar Hive (histórico)", key="btn_load_hive_fast", type="secondary"):
                with st.spinner("Consultando Hive (PyHive)…"):
                    snap["hive"] = hive_resumen()
                    st.session_state["pipeline_snapshot_kdd"] = snap
                    st.rerun()
        else:
            if hv.get("show_tables_ok"):
                st.success("SHOW TABLES ejecutado.")
                st.text_area("Tablas", hv.get("tablas") or "", height=180, disabled=True, key="hive_tabs")
            else:
                st.warning(hv.get("error_tablas", "Hive no disponible o error de conexión (PyHive)."))

            for codigo, bloque in (hv.get("conteos") or {}).items():
                st.markdown(f"**{titulo_hive(codigo)}** (`{codigo}`)")
                if bloque.get("ok"):
                    st.code(bloque.get("salida", "")[:1200], language="text")
                else:
                    st.caption(f"⚠️ {bloque.get('error', 'error')}")
            if hv.get("hint"):
                st.info(hv["hint"])
            if hv.get("hint_alias_hive"):
                st.warning(hv["hint_alias_hive"])

    st.divider()
    st.markdown(
        "**Flujo de referencia:** `ingesta` → Kafka + HDFS → Spark lee HDFS → "
        "Cassandra (tiempo casi real) + Hive (histórico si el metastore responde)."
    )


