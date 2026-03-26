"""
Widgets Streamlit: cuadro de mando, consultas supervisadas y slides de clima/retrasos.
"""
from __future__ import annotations

import io
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
    ejecutar_cassandra_cql_seguro,
    ejecutar_hive_consulta,
    ejecutar_hive_sql_seguro,
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
                code, out, err = ejecutar_ingesta(
                    paso_arg,
                    simular_incidencias=bool(st.session_state.get("simlog_simular_incidencias", True)),
                )
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

    with st.expander("CQL (copiar / pegar / ejecutar)", expanded=False):
        st.caption("Ejecuta CQL pegado aquí. Por seguridad solo se permite `SELECT`.")
        cql_default = (CASSANDRA_CONSULTAS.get(sel, {}) or {}).get("cql", "").strip()
        cql_edit = st.text_area(
            "CQL",
            value=cql_default,
            height=140,
            key=f"cql_edit_{sel}",
        ).strip()
        c1, c2 = st.columns([1, 3])
        with c1:
            run_cql = st.button("Ejecutar CQL", key=f"btn_cql_run_{sel}", type="secondary")
        with c2:
            st.caption("Sugerencia: añade `LIMIT` si no lo tiene.")

        if run_cql:
            with st.spinner("Ejecutando Cassandra…"):
                ok, err, rows = ejecutar_cassandra_cql_seguro(cql_edit)
            if ok and rows:
                st.dataframe(pd.DataFrame(rows), width="stretch")
                st.caption(f"{len(rows)} fila(s).")
            elif ok:
                st.info("Consulta correcta pero sin filas.")
            else:
                st.error(err or "Error Cassandra")


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
    # Descubrir tablas realmente existentes para evitar “solo funciona la primera”.
    # (En muchos entornos local/dev no se generan `historico_nodos`/`nodos_maestro` hasta que Spark corre con Hive.)
    tablas_en_hive: set[str] = set()
    ok_tabs, err_tabs, out_tabs = ejecutar_hive_consulta("tablas_bd")
    if ok_tabs and out_tabs:
        lineas = [l for l in out_tabs.splitlines() if l.strip()]
        for ln in lineas[1:]:
            partes = ln.split("\t")
            tablas_en_hive.add(partes[-1].strip())

    # Requisitos por consulta (tablas físicas).
    requiere: dict[str, set[str]] = {
        "diag_smoke_hive": set(),
        "tablas_bd": set(),
        "historico_nodos_muestra": {"historico_nodos"},
        "historico_nodos_conteo": {"historico_nodos"},
        "historico_nodos_muestra_24h": {"historico_nodos"},
        "diag_fecha_proceso_24h": {"historico_nodos"},
        "severidad_resumen_24h": {"historico_nodos"},
        "riesgo_hub_24h": {"historico_nodos", "nodos_maestro"},
        "top_causas_24h": {"historico_nodos"},
        "nodos_maestro": {"nodos_maestro"},
        "nodos_maestro_conteo": {"nodos_maestro"},
        "gemelo_red_nodos": {"red_gemelo_nodos"},
        "gemelo_red_aristas": {"red_gemelo_aristas"},
        "transporte_ingesta_real_muestra": {"transporte_ingesta_real"},
        # Tabla configurable (por defecto `transporte_ingesta_completa`, que sí suele existir)
        "gestor_historial_rutas_camion": set(),
    }

    def _disponible(codigo: str) -> bool:
        req = requiere.get(codigo)
        if req is None:
            # Si no conocemos los requisitos, no la ocultamos.
            return True
        return req.issubset(tablas_en_hive)

    claves = listar_claves_hive()
    # Orden “rápido” (lo más útil al principio)
    claves_rapidas = ["diag_smoke_hive", "tablas_bd", "transporte_ingesta_real_muestra"]
    claves_ordenadas = [k for k in claves_rapidas if k in claves] + [k for k in claves if k not in claves_rapidas]

    # Por defecto, solo las que tienen pinta de funcionar con las tablas actuales.
    disponibles = [k for k in claves_ordenadas if _disponible(k)]
    no_disp = [k for k in claves_ordenadas if k not in disponibles]

    if not ok_tabs:
        st.warning(
            "Hive responde a `SELECT 1` pero falló al listar tablas (`SHOW TABLES`). "
            f"Detalle: {err_tabs or 'error'}. Si HS2 va lento, sube `HIVE_QUERY_TIMEOUT_SEC`."
        )
    elif tablas_en_hive:
        st.caption(f"Tablas detectadas en Hive: {len(tablas_en_hive)}")

    if no_disp:
        faltan = sorted({t for k in no_disp for t in requiere.get(k, set()) if t not in tablas_en_hive})
        if faltan:
            st.info(
                "Algunas consultas no están disponibles porque faltan tablas en Hive: "
                + ", ".join(f"`{t}`" for t in faltan)
                + ". Esto se arregla ejecutando la fase Spark que escribe Hive (con `SIMLOG_ENABLE_HIVE=1`)."
            )
        ver_todo = st.toggle("Mostrar también consultas que ahora fallarán", value=False, key="hive_show_all")
    else:
        ver_todo = False

    claves_para_ui = claves_ordenadas if ver_todo else disponibles
    if not claves_para_ui:
        # Mínimo: siempre dejar el diagnóstico.
        claves_para_ui = ["diag_smoke_hive"]

    def _label(k: str) -> str:
        base = f"{titulo_hive(k)} (`{k}`)"
        if ver_todo and (k in no_disp):
            req = sorted(requiere.get(k, set()))
            if req:
                base += " — no disponible (falta: " + ", ".join(req) + ")"
            else:
                base += " — puede fallar"
        return base

    etiquetas = {k: _label(k) for k in claves_para_ui}
    label_a_clave = {etiquetas[k]: k for k in claves_para_ui}
    sel_label = st.radio("Consulta", options=list(label_a_clave.keys()), key="hive_sel_radio")
    sel = label_a_clave[sel_label]

    def _hive_tsv_a_dataframe(tsv: str) -> pd.DataFrame:
        if not (tsv or "").strip():
            return pd.DataFrame()
        df = pd.read_csv(io.StringIO(tsv), sep="\t")
        # Quitar prefijos tipo "h.id_nodo" o "tabla.col"
        df.columns = [c.split(".")[-1].strip() for c in df.columns]
        return df

    def _recortar_texto(v: object, max_len: int = 120) -> str:
        if v is None:
            return ""
        s = str(v)
        if len(s) <= max_len:
            return s
        return s[: max_len - 1] + "…"

    def _limpiar_clima(v: object) -> str:
        if v is None:
            return ""
        s = str(v)
        if s.startswith("Error: HTTPSConnectionPool") or "Failed to resolve" in s:
            return "Sin conexión a OpenWeather"
        return _recortar_texto(s, max_len=120)

    def _render_df_usuario(df: pd.DataFrame) -> None:
        if df.empty:
            st.info("Consulta correcta pero sin filas.")
            return

        ren = {
            "id_nodo": "Nodo",
            "tipo": "Tipo",
            "estado": "Estado",
            "motivo_retraso": "Motivo",
            "clima_actual": "Clima",
            "fecha_proceso": "Fecha",
        }
        df2 = df.copy()
        for c in list(df2.columns):
            if c in ren:
                df2.rename(columns={c: ren[c]}, inplace=True)

        # Limpieza: clima muy largo cuando no hay DNS
        if "Clima" in df2.columns:
            df2["Clima"] = df2["Clima"].map(_limpiar_clima)

        # Filtros rápidos
        c1, c2, c3 = st.columns([2, 2, 3])
        with c1:
            solo_hubs = st.checkbox("Solo hubs", value=False, key="hive_f_solo_hubs")
        with c2:
            estados = sorted({str(x) for x in df2.get("Estado", pd.Series(dtype=str)).dropna().unique()})
            estados_sel = st.multiselect("Estado", options=estados, default=estados, key="hive_f_estados")
        with c3:
            q = st.text_input("Buscar nodo", value="", key="hive_f_buscar").strip().lower()

        if solo_hubs and "Tipo" in df2.columns:
            df2 = df2[df2["Tipo"].astype(str).str.lower() == "hub"]
        if "Estado" in df2.columns and estados_sel:
            df2 = df2[df2["Estado"].astype(str).isin(estados_sel)]
        if q and "Nodo" in df2.columns:
            df2 = df2[df2["Nodo"].astype(str).str.lower().str.contains(q, na=False)]

        if "Estado" in df2.columns and "Nodo" in df2.columns:
            df2 = df2.sort_values(["Estado", "Nodo"], ascending=[True, True])

        st.dataframe(df2, width="stretch", hide_index=True)
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
                df = _hive_tsv_a_dataframe(out_s)
                _render_df_usuario(df)
                with st.expander("Ver salida raw (TSV)"):
                    st.text_area("Salida (TSV)", value=out_s, height=220)
        else:
            st.error(err)
            if "Table not found" in (err or "") or "tabla no encontrada" in (err or "").lower():
                st.info(
                    "Pista: esa consulta requiere tablas que aún no existen en el metastore de Hive. "
                    "Ejecuta Spark con Hive habilitado (`SIMLOG_ENABLE_HIVE=1`) para crear/poblar "
                    "`historico_nodos` y/o `nodos_maestro`, o ajusta `SIMLOG_HIVE_TABLE_*` si tus nombres difieren."
                )
            if out:
                st.text_area("Detalle", value=out, height=160)

    with st.expander("SQL (copiar / pegar / ejecutar)", expanded=False):
        st.caption("Ejecuta SQL pegado aquí. Por seguridad solo se permiten `SHOW`, `SELECT` o `WITH`.")
        sql_default = (HIVE_CONSULTAS.get(sel, {}) or {}).get("sql", "").strip()
        sql_edit = st.text_area(
            "SQL",
            value=sql_default,
            height=160,
            key=f"hive_sql_edit_{sel}",
        ).strip()
        c_sql1, c_sql2 = st.columns([1, 3])
        with c_sql1:
            run_sql = st.button("Ejecutar SQL", key=f"btn_hive_sql_{sel}", type="secondary")
        with c_sql2:
            st.caption("Sugerencia: añade `LIMIT` si la tabla es grande.")

        if run_sql:
            t0 = time.monotonic()
            with st.spinner("Ejecutando SQL en Hive (PyHive)…"):
                ok, err, out = ejecutar_hive_sql_seguro(sql_edit)
            elapsed = time.monotonic() - t0
            if ok:
                out_s = out or ""
                st.caption(f"Tiempo Hive: {elapsed:.1f}s")
                st.caption(f"Salida TSV: {len(out_s)} caracteres.")
                if not out_s.strip():
                    st.info("Consulta correcta pero sin filas.")
                else:
                    df = _hive_tsv_a_dataframe(out_s)
                    _render_df_usuario(df)
                    with st.expander("Ver salida raw (TSV)"):
                        st.text_area("Salida (TSV)", value=out_s, height=220)
            else:
                st.error(err or "Error Hive")


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
