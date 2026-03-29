"""
Widgets Streamlit: cuadro de mando, consultas supervisadas y slides de clima/retrasos.
"""
from __future__ import annotations

import io
import json
import os
import time

from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import streamlit as st

from servicios.clima_retrasos import (
    evaluar_retraso_integrado,
    obtener_clima_todos_hubs_completo,
)
from config import HIVE_TABLE_TRANSPORTE_HIST
from servicios.consultas_cuadro_mando import (
    CASSANDRA_CONSULTAS,
    CASSANDRA_CATEGORIAS,
    HIVE_CONSULTAS,
    HIVE_CATEGORIAS,
    ejecutar_cassandra_consulta,
    ejecutar_cassandra_cql_seguro,
    ejecutar_hive_consulta,
    ejecutar_hive_consulta_detalle,
    ejecutar_hive_sql_seguro,
    ejecutar_hive_sql_seguro_detalle,
    limpiar_cache_consultas_hive,
    listar_categorias_cassandra,
    listar_categorias_hive,
    listar_columnas_cassandra,
    listar_columnas_hive,
    listar_claves_cassandra,
    listar_claves_hive,
    listar_keyspaces_cassandra,
    listar_tablas_cassandra,
    listar_tablas_hive,
    nombre_categoria,
    nombre_categoria_cassandra,
    descripcion_categoria,
    descripcion_categoria_cassandra,
    obtener_categoria_cassandra,
    obtener_consultas_de_categoria,
    obtener_consultas_de_categoria_cassandra,
    titulo_cassandra,
    titulo_hive,
)
from servicios.ejecucion_pipeline import ejecutar_ingesta, ejecutar_procesamiento
from servicios.estado_y_datos import cargar_nodos_cassandra, verificar_kafka_topic
from servicios.kdd_operaciones import OPERACIONES_POR_FASE
from config import PROJECT_DISPLAY_NAME, TOPIC_TRANSPORTE

REPORT_TEMPLATES_PATH = Path(__file__).resolve().parents[1] / "servicios" / "report_templates.json"
LOGO_PATH = Path(__file__).resolve().parents[1] / "logo.png"


def _cargar_plantillas_usuario() -> dict:
    if not REPORT_TEMPLATES_PATH.exists():
        return {}
    try:
        return json.loads(REPORT_TEMPLATES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _guardar_plantillas_usuario(plantillas: dict) -> tuple[bool, str]:
    try:
        REPORT_TEMPLATES_PATH.write_text(json.dumps(plantillas, ensure_ascii=True, indent=2), encoding="utf-8")
        return True, ""
    except Exception as e:
        return False, str(e)


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
    st.subheader("Consultas supervisadas — Cassandra (tiempo real)")
    st.caption(
        "Solo consultas **aprobadas** (lectura). Datos en **tiempo real** desde Cassandra. "
        "Tablas: `nodos_estado`, `aristas_estado`, `tracking_camiones`, `pagerank_nodos`, `eventos_historico`."
    )

    # Estado para guardar la consulta seleccionada
    if "cass_categoria_seleccionada" not in st.session_state:
        st.session_state["cass_categoria_seleccionada"] = "nodos"
    if "cass_consulta_seleccionada" not in st.session_state:
        st.session_state["cass_consulta_seleccionada"] = "nodos_estado_resumen"

    # Selector de categoría
    categorias = listar_categorias_cassandra()
    cat_seleccionada = st.selectbox(
        "Categoría",
        options=categorias,
        format_func=lambda c: nombre_categoria_cassandra(c),
        index=categorias.index(st.session_state["cass_categoria_seleccionada"])
            if st.session_state["cass_categoria_seleccionada"] in categorias else 0,
        key="cass_cat_select",
    )
    st.session_state["cass_categoria_seleccionada"] = cat_seleccionada

    # Obtener consultas de la categoría
    consultas_categoria = obtener_consultas_de_categoria_cassandra(cat_seleccionada)
    opciones_consultas = [
        titulo_cassandra(c) for c in consultas_categoria
    ]
    
    # Texto informativo (no es un checkbox; antes el emoji 📋 confundía con un control)
    st.markdown(
        f"**Descripción de la categoría:** {descripcion_categoria_cassandra(cat_seleccionada)}"
    )

    # Selector de consulta
    consulta_seleccionada = st.selectbox(
        "Consulta",
        options=consultas_categoria,
        format_func=lambda c: titulo_cassandra(c),
        key="cass_consulta_select",
    )
    st.session_state["cass_consulta_seleccionada"] = consulta_seleccionada

    # Ejecutar consulta
    if st.button("Ejecutar consulta Cassandra", key="btn_cql", type="primary"):
        with st.spinner("Ejecutando Cassandra…"):
            ok, err, rows = ejecutar_cassandra_consulta(consulta_seleccionada)
        if ok and rows:
            st.dataframe(pd.DataFrame(rows), width="stretch")
            st.caption(f"✅ {len(rows)} fila(s) devueltas.")
        elif ok:
            st.info("Consulta correcta pero sin filas (tabla vacía).")
        else:
            st.error(f"Error Cassandra: {err}")

    # CQL personalizado
    with st.expander("CQL personalizado (copiar / pegar / ejecutar)", expanded=False):
        st.caption("Ejecuta CQL pegado aquí. Solo se permite `SELECT`.")
        cql_default = (CASSANDRA_CONSULTAS.get(consulta_seleccionada, {}) or {}).get("cql", "").strip()
        cql_edit = st.text_area(
            "CQL",
            value=cql_default,
            height=140,
            key="cql_edit_custom",
        ).strip()
        c1, c2 = st.columns([1, 3])
        with c1:
            run_cql = st.button("Ejecutar CQL", key="btn_cql_run_custom", type="secondary")
        with c2:
            st.caption("💡 Sugerencia: usa `ALLOW FILTERING` con moderación.")

        if run_cql and cql_edit:
            with st.spinner("Ejecutando Cassandra…"):
                ok, err, rows = ejecutar_cassandra_cql_seguro(cql_edit)
            if ok and rows:
                st.dataframe(pd.DataFrame(rows), width="stretch")
                st.caption(f"✅ {len(rows)} fila(s).")
            elif ok:
                st.info("Consulta correcta pero sin filas.")
            else:
                st.error(f"Error: {err or 'Error desconocido'}")
        elif run_cql and not cql_edit:
            st.warning("Introduce una consulta CQL primero.")


def render_consultas_hive() -> None:
    st.subheader("Consultas supervisadas — Hive (histórico)")
    st.caption(
        "Requiere **HiveServer2** (puerto **10000**). Las consultas usan **PyHive** (Thrift), no `beeline`. "
        "Timeout por defecto: 600 s (`HIVE_QUERY_TIMEOUT_SEC`). "
        "Las respuestas **correctas** se guardan en **caché en memoria** (`SIMLOG_HIVE_CACHE_TTL_SEC`, por defecto **120 s**); "
        "**la misma consulta** repetida debería mostrar un tiempo mucho menor (⚡ en el resultado). "
        "Tablas típicas: `eventos_historico`, `clima_historico`, `tracking_camiones_historico`, "
        f"`{HIVE_TABLE_TRANSPORTE_HIST}` (SIMLOG_HIVE_TABLA_TRANSPORTE), `rutas_alternativas_historico`, `agg_estadisticas_diarias`."
    )

    # Tablas Hive: opcional. Antes se llamaba `tablas_bd` aquí y podía bloquear minutos la página
    # (solo se veía la fila de caché). Se rellena con el botón «Verificar tablas» o primera ejecución.
    if "hive_tablas_en_hive" not in st.session_state:
        st.session_state.hive_tablas_en_hive = set()
    if "hive_tablas_err" not in st.session_state:
        st.session_state.hive_tablas_err = ""

    tablas_en_hive: set[str] = st.session_state.hive_tablas_en_hive

    # Requisitos por consulta (tablas físicas)
    requiere: dict[str, set[str]] = {
        "diag_smoke_hive": set(),
        "tablas_bd": set(),
        "historico_nodos_muestra": {"historico_nodos"},
        "historico_nodos_conteo": {"historico_nodos"},
        "nodos_maestro_muestra": {"nodos_maestro"},
        "nodos_maestro_conteo": {"nodos_maestro"},
        "gemelo_red_nodos": {"red_gemelo_nodos"},
        "gemelo_red_aristas": {"red_gemelo_aristas"},
        "eventos_historico_muestra": {"eventos_historico"},
        "eventos_nodos_24h": {"eventos_historico"},
        "eventos_bloqueos_24h": {"eventos_historico"},
        "eventos_evolucion_dia": {"eventos_historico"},
        "clima_historico_muestra": {"clima_historico"},
        "clima_historico_hoy": {"clima_historico"},
        "clima_estado_carretera": {"clima_historico"},
        "tracking_historico_muestra": {"tracking_camiones_historico"},
        "tracking_camion_especifico": {"tracking_camiones_historico"},
        "tracking_ultima_posicion": {"tracking_camiones_historico"},
        "transporte_ingesta_real_muestra": {"transporte_ingesta_completa"},
        "transporte_ingesta_hoy": {"transporte_ingesta_completa"},
        "transporte_retrasos_hoy": {"transporte_ingesta_completa"},
        "gestor_historial_rutas_camion": {"transporte_ingesta_completa"},
        "rutas_alternativas_muestra": {"rutas_alternativas_historico"},
        "rutas_alternativas_bloqueos": {"rutas_alternativas_historico"},
        "agg_estadisticas_diarias": {"agg_estadisticas_diarias"},
        "agg_ultima_semana": {"agg_estadisticas_diarias"},
        "gestor_eventos_por_hub": {"eventos_historico"},
        "gestor_clima_afecta_transporte": {"clima_historico", HIVE_TABLE_TRANSPORTE_HIST},
        "gestor_incidencias_resumen": {"eventos_historico"},
        "gestor_pagerank_historico": {"eventos_historico"},
    }

    def _disponible(codigo: str) -> bool:
        req = requiere.get(codigo)
        if req is None:
            return True
        if not tablas_en_hive:
            # Sin verificación aún: mostrar todas las consultas (fallará Hive si falta la tabla)
            return True
        return req.issubset(tablas_en_hive)

    c_ver, c_hint = st.columns([1, 3])
    with c_ver:
        if st.button("Verificar tablas en Hive", key="btn_hive_list_tablas", type="secondary"):
            with st.spinner("Listando tablas (SHOW TABLES)…"):
                ok_tabs, err_tabs, out_tabs = ejecutar_hive_consulta("tablas_bd")
            if ok_tabs and out_tabs:
                lineas = [l for l in out_tabs.splitlines() if l.strip()]
                nuevas: set[str] = set()
                for ln in lineas[1:]:
                    partes = ln.split("\t")
                    nuevas.add(partes[-1].strip())
                st.session_state.hive_tablas_en_hive = nuevas
                st.session_state.hive_tablas_err = ""
                st.success(f"Detectadas **{len(nuevas)}** tablas.")
            else:
                st.session_state.hive_tablas_err = err_tabs or "error"
                st.warning(f"No se pudieron listar tablas: {err_tabs}")
            st.rerun()
    with c_hint:
        if tablas_en_hive:
            st.caption(f"Tablas conocidas en Hive: **{len(tablas_en_hive)}** (pulsa verificar tras crear tablas con Spark).")
        elif st.session_state.hive_tablas_err:
            st.caption(f"Último error al listar: {st.session_state.hive_tablas_err[:120]}…")
        else:
            st.caption(
                "Opcional: **Verificar tablas** para ocultar consultas cuyas tablas aún no existen. "
                "Puedes ejecutar consultas igualmente (p. ej. **Diagnóstico — SELECT 1**)."
            )

    # ==========================================================================
    # Interfaz por categorías con expanders
    # ==========================================================================
    
    # Estado para guardar la consulta seleccionada actualmente
    if "hive_categoria_seleccionada" not in st.session_state:
        st.session_state["hive_categoria_seleccionada"] = "diagnostico"
    if "hive_consulta_seleccionada" not in st.session_state:
        st.session_state["hive_consulta_seleccionada"] = "diag_smoke_hive"

    # Selector de categoría principal
    categorias = listar_categorias_hive()
    opciones_categorias = [nombre_categoria(c) for c in categorias]
    cat_seleccionada = st.selectbox(
        "Categoría",
        options=categorias,
        format_func=lambda c: nombre_categoria(c),
        index=categorias.index(st.session_state["hive_categoria_seleccionada"]) 
            if st.session_state["hive_categoria_seleccionada"] in categorias else 0,
        key="hive_cat_select",
    )
    st.session_state["hive_categoria_seleccionada"] = cat_seleccionada

    # Obtener consultas de la categoría seleccionada
    consultas_categoria = obtener_consultas_de_categoria(cat_seleccionada)
    disponibles_categoria = [c for c in consultas_categoria if _disponible(c)]
    no_disp_categoria = [c for c in consultas_categoria if c not in disponibles_categoria]
    
    # Texto informativo (no es un checkbox; el emoji 📋 parecía un control deshabilitado)
    st.markdown(f"**Descripción de la categoría:** {descripcion_categoria(cat_seleccionada)}")

    # Selector de consulta dentro de la categoría
    if disponibles_categoria:
        opciones_consultas = [
            titulo_hive(c) for c in disponibles_categoria
        ]
        consulta_seleccionada = st.selectbox(
            "Consulta",
            options=disponibles_categoria,
            format_func=lambda c: titulo_hive(c),
            key="hive_consulta_select",
        )
        st.session_state["hive_consulta_seleccionada"] = consulta_seleccionada
    elif no_disp_categoria:
        # Mostrar consultas no disponibles
        st.warning("Esta categoría requiere tablas que aún no existen en Hive.")
        opciones_consultas = [titulo_hive(c) + " ⚠️" for c in no_disp_categoria]
        st.selectbox(
            "Consultas (no disponibles)",
            options=no_disp_categoria,
            format_func=lambda c: titulo_hive(c) + " ⚠️",
            disabled=True,
            key="hive_consulta_disabled",
        )
        consulta_seleccionada = None
    else:
        st.info("No hay consultas definidas para esta categoría.")
        consulta_seleccionada = None

    # Si hay no disponibles, mostrar toggle para verlas
    if no_disp_categoria:
        ver_no_disponibles = st.checkbox(
            f"Mostrar {len(no_disp_categoria)} consulta(s) no disponible(s)",
            value=False,
            key="hive_ver_no_disp",
        )
        if ver_no_disponibles:
            st.caption("⚠️ Estas consultas requieren tablas que se crean ejecutando Spark con `SIMLOG_ENABLE_HIVE=1`")

    sel = st.session_state["hive_consulta_seleccionada"] if consulta_seleccionada else None

    # ==========================================================================
    # Funciones auxiliares para renderizar resultados
    # ==========================================================================

    def _hive_tsv_a_dataframe(tsv: str) -> pd.DataFrame:
        if not (tsv or "").strip():
            return pd.DataFrame()
        df = pd.read_csv(io.StringIO(tsv), sep="\t")
        df.columns = [c.split(".")[-1].strip() for c in df.columns]
        return df

    def _recortar_texto(v: object, max_len: int = 120) -> str:
        if v is None:
            return ""
        s = str(v)
        return s if len(s) <= max_len else s[: max_len - 1] + "…"

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
            "timestamp": "Fecha/Hora",
            "id_camion": "Camión",
            "origen": "Origen",
            "destino": "Destino",
            "lat_actual": "Lat",
            "lon_actual": "Lon",
            "progreso_pct": "Progreso %",
        }
        df2 = df.copy()
        for c in list(df2.columns):
            if c in ren:
                df2.rename(columns={c: ren[c]}, inplace=True)

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
            q = st.text_input("Buscar", value="", key="hive_f_buscar").strip().lower()

        if solo_hubs and "Tipo" in df2.columns:
            df2 = df2[df2["Tipo"].astype(str).str.lower() == "hub"]
        if "Estado" in df2.columns and estados_sel:
            df2 = df2[df2["Estado"].astype(str).isin(estados_sel)]
        if q:
            for col in ["Nodo", "Camión", "Origen", "Destino"]:
                if col in df2.columns:
                    df2 = df2[df2[col].astype(str).str.lower().str.contains(q, na=False)]
                    break

        st.dataframe(df2, width="stretch", hide_index=True)

    # ==========================================================================
    # Ejecutar consulta seleccionada
    # ==========================================================================
    
    if sel and st.button("Ejecutar consulta Hive", key="btn_hive", type="primary"):
        t0 = time.monotonic()
        with st.spinner("Ejecutando Hive (PyHive)…"):
            ok, err, out, desde_cache = ejecutar_hive_consulta_detalle(sel)
        elapsed = time.monotonic() - t0
        if ok:
            out_s = out or ""
            origen = (
                "**Caché en memoria** (misma SQL; TTL renovado en cada uso)"
                if desde_cache
                else "**HiveServer2** (consulta ejecutada en cluster)"
            )
            st.caption(
                f"⏱️ Tiempo: **{elapsed:.1f}s** | Resultado: **{len(out_s)}** caracteres · Origen: {origen}"
            )
            if not out_s.strip():
                st.info("Consulta correcta pero sin filas para los filtros actuales.")
            else:
                df = _hive_tsv_a_dataframe(out_s)
                _render_df_usuario(df)
                with st.expander("Ver salida raw (TSV)"):
                    st.text_area("Salida (TSV)", value=out_s, height=220)
        else:
            st.error(f"Error Hive: {err}")
            if "Table not found" in (err or ""):
                st.info(
                    "La tabla no existe. Ejecuta Spark con `SIMLOG_ENABLE_HIVE=1` para crear las tablas."
                )

    # ==========================================================================
    # SQL personalizado
    # ==========================================================================
    with st.expander("SQL personalizado (copiar / pegar / ejecutar)", expanded=False):
        st.caption("Ejecuta SQL pegado aquí. Solo se permiten `SHOW`, `SELECT` o `WITH`.")
        sql_default = (HIVE_CONSULTAS.get(sel, {}) or {}).get("sql", "").strip() if sel else ""
        sql_edit = st.text_area(
            "SQL",
            value=sql_default,
            height=160,
            key="hive_sql_edit_custom",
        ).strip()
        
        c_sql1, c_sql2 = st.columns([1, 3])
        with c_sql1:
            run_sql = st.button("Ejecutar SQL", key="btn_hive_sql_custom", type="secondary")
        with c_sql2:
            st.caption("💡 Sugerencia: añade `LIMIT 100` si la tabla es grande.")

        if run_sql and sql_edit:
            t0 = time.monotonic()
            with st.spinner("Ejecutando SQL en Hive…"):
                ok, err, out, desde_cache = ejecutar_hive_sql_seguro_detalle(sql_edit)
            elapsed = time.monotonic() - t0
            if ok:
                out_s = out or ""
                origen = "**Caché**" if desde_cache else "**HiveServer2**"
                st.caption(
                    f"⏱️ Tiempo: **{elapsed:.1f}s** | Resultado: **{len(out_s)}** caracteres · Origen: {origen}"
                )
                if not out_s.strip():
                    st.info("Consulta correcta pero sin filas.")
                else:
                    df = _hive_tsv_a_dataframe(out_s)
                    _render_df_usuario(df)
            else:
                st.error(f"Error: {err or 'Error desconocido'}")
        elif run_sql and not sql_edit:
            st.warning("Introduce una consulta SQL primero.")

    with st.expander("Caché Hive y TTL", expanded=False):
        st.caption(
            f"TTL actual: **{os.environ.get('SIMLOG_HIVE_CACHE_TTL_SEC', '120')}** s · "
            "`0` = desactivar caché. **Renovación en cada uso** (`SIMLOG_HIVE_CACHE_SLIDING=1` por defecto): "
            "el plazo cuenta desde la **última** vez que acertaste esa SQL, no solo desde la primera."
        )
        st.caption(
            "Si el tiempo es alto y el origen indica **HiveServer2**, no hubo acierto de caché "
            "(consulta distinta, caché vaciada o proceso Streamlit reiniciado)."
        )
        if st.button("Vaciar caché Hive", key="btn_hive_cache_clear"):
            limpiar_cache_consultas_hive()
            st.success("Caché vaciada; la próxima ejecución irá a HiveServer2.")


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


def render_informes_a_medida() -> None:
    st.subheader("Informes a medida (plantillas + PDF)")
    st.caption(
        "Selecciona BD, tabla y campos para generar un informe personalizado. "
        "Incluye vista previa y descarga en PDF."
    )

    plantillas_base = {
        "operativa": {
            "label": "Operativa (tabla + resumen corto)",
            "default_limit": 200,
            "default_where": "",
            "default_order_by": "",
            "aliases": {},
        },
        "ejecutiva": {
            "label": "Ejecutiva (KPIs + tabla)",
            "default_limit": 120,
            "default_where": "",
            "default_order_by": "",
            "aliases": {},
        },
        "auditoria": {
            "label": "Auditoría (detalle completo)",
            "default_limit": 500,
            "default_where": "",
            "default_order_by": "",
            "aliases": {},
        },
        "administrativa": {
            "label": "Administrativa (solo metadatos de emisión/firma)",
            "default_limit": 1,
            "default_where": "",
            "default_order_by": "",
            "aliases": {},
        },
    }
    plantillas_usuario = _cargar_plantillas_usuario()
    plantillas = {**plantillas_base, **plantillas_usuario}
    plantilla = st.selectbox(
        "Plantilla",
        options=list(plantillas.keys()),
        format_func=lambda x: plantillas.get(x, {}).get("label", x),
        key="rep_tpl",
    )
    tpl_cfg = plantillas.get(plantilla, {})
    tpl_prev = st.session_state.get("rep_tpl_prev")
    if tpl_prev != plantilla:
        # Al cambiar de plantilla, aplicar defaults completos (motor/tabla/campos/filtros).
        if tpl_cfg.get("default_motor") in ("cassandra", "hive"):
            st.session_state["rep_motor"] = tpl_cfg.get("default_motor")
        st.session_state["rep_tabla"] = tpl_cfg.get("default_table", "")
        st.session_state["rep_keyspace"] = tpl_cfg.get("default_keyspace", st.session_state.get("rep_keyspace", "logistica_espana"))
        st.session_state["rep_campos"] = list(tpl_cfg.get("default_fields", []) or [])
        st.session_state["rep_where"] = str(tpl_cfg.get("default_where", ""))
        st.session_state["rep_order"] = str(tpl_cfg.get("default_order_by", ""))
        st.session_state["rep_limit"] = int(tpl_cfg.get("default_limit", 200))
        st.session_state["rep_tpl_prev"] = plantilla

    st.markdown("**Datos administrativos del informe**")
    c_meta1, c_meta2, c_meta3 = st.columns([1, 1, 1])
    with c_meta1:
        fecha_emision = st.text_input(
            "Fecha de emisión",
            value=st.session_state.get("rep_fecha_emision", datetime.now().strftime("%Y-%m-%d")),
            key="rep_fecha_emision",
        ).strip()
    with c_meta2:
        firmante = st.text_input(
            "Firmante",
            value=st.session_state.get("rep_firmante", ""),
            key="rep_firmante",
        ).strip()
    with c_meta3:
        cargo = st.text_input(
            "Cargo",
            value=st.session_state.get("rep_cargo", ""),
            key="rep_cargo",
        ).strip()
    organismo = st.text_input(
        "Organismo / Área",
        value=st.session_state.get("rep_organismo", PROJECT_DISPLAY_NAME),
        key="rep_organismo",
    ).strip()
    observaciones = st.text_area(
        "Observaciones / alcance (texto libre)",
        value=st.session_state.get("rep_observaciones", ""),
        height=90,
        key="rep_observaciones",
    ).strip()
    modo_formulario = st.checkbox(
        "Generar en formato formulario (imprimible)",
        value=bool(st.session_state.get("rep_modo_formulario", False)),
        key="rep_modo_formulario",
        help="Dibuja campos tipo ficha para completar/firmar. Si dejas valores vacíos, se mantienen líneas en blanco.",
    )

    admin_meta = {
        "fecha_emision": fecha_emision,
        "firmante": firmante,
        "cargo": cargo,
        "organismo": organismo,
        "observaciones": observaciones,
        "modo_formulario": modo_formulario,
    }

    if plantilla == "administrativa":
        st.info("Esta plantilla permite generar un PDF administrativo sin extraer datos de tablas.")
        if st.button("Generar PDF administrativo", key="rep_pdf_admin", type="primary"):
            st.session_state["rep_last_df"] = pd.DataFrame()
            st.session_state["rep_last_sql"] = "(sin consulta: plantilla administrativa)"
            st.session_state["rep_last_ctx"] = {
                "motor": "documental",
                "tabla": "administrativa",
                "plantilla": plantilla,
                "meta": admin_meta,
            }

    motor = st.radio(
        "Base de datos",
        options=["cassandra", "hive"],
        horizontal=True,
        format_func=lambda x: "Cassandra (operativo)" if x == "cassandra" else "Hive (histórico)",
        key="rep_motor",
    )

    if motor == "cassandra":
        ok_ks, err_ks, keyspaces = listar_keyspaces_cassandra()
        if not ok_ks:
            st.error(err_ks or "No se pudieron listar keyspaces.")
            return
        if not keyspaces:
            st.info("No hay keyspaces disponibles en Cassandra.")
            return
        ks_default = "logistica_espana" if "logistica_espana" in keyspaces else keyspaces[0]
        ks_actual = st.session_state.get("rep_keyspace", ks_default)
        if ks_actual not in keyspaces:
            ks_actual = ks_default
            st.session_state["rep_keyspace"] = ks_actual
        keyspace_sel = st.selectbox("Keyspace", options=keyspaces, key="rep_keyspace")
        if keyspace_sel != "logistica_espana":
            st.caption(f"Usando keyspace `{keyspace_sel}` (el configurado por defecto es `logistica_espana`).")
        ok_t, err_t, tablas = listar_tablas_cassandra(keyspace=keyspace_sel)
    else:
        ok_t, err_t, tablas = listar_tablas_hive()
    if not ok_t:
        st.error(err_t or "No se pudieron listar tablas.")
        return
    if not tablas:
        st.info("No hay tablas disponibles para este motor.")
        return

    tabla_actual = st.session_state.get("rep_tabla", "")
    if tabla_actual not in tablas:
        st.session_state["rep_tabla"] = tablas[0]
    tabla = st.selectbox("Tabla", options=tablas, key="rep_tabla")
    if motor == "cassandra":
        keyspace_sel = st.session_state.get("rep_keyspace", "logistica_espana")
        ok_c, err_c, columnas = listar_columnas_cassandra(tabla, keyspace=keyspace_sel)
    else:
        ok_c, err_c, columnas = listar_columnas_hive(tabla)
    if not ok_c:
        st.error(err_c or "No se pudieron listar columnas.")
        return
    if not columnas:
        st.info("La tabla no expone columnas seleccionables.")
        return

    modo_select = st.radio(
        "Modo de SELECT",
        options=["custom", "all"],
        horizontal=True,
        format_func=lambda x: "Campos seleccionados" if x == "custom" else "SELECT * (exploración)",
        key="rep_select_mode",
    )
    campos: List[str] = []
    if modo_select == "custom":
        campos_actuales = st.session_state.get("rep_campos", [])
        if not isinstance(campos_actuales, list):
            campos_actuales = []
        campos_validos = [c for c in campos_actuales if c in columnas]
        if not campos_validos:
            campos_validos = columnas[: min(6, len(columnas))]
        st.session_state["rep_campos"] = campos_validos
        campos = st.multiselect("Campos (1 o más)", options=columnas, key="rep_campos")
        if not campos:
            st.warning("Selecciona al menos un campo.")
            return
    else:
        st.info("Modo exploración activo: se ejecutará `SELECT *` sobre la tabla elegida.")

    if "rep_where" not in st.session_state:
        st.session_state["rep_where"] = str(tpl_cfg.get("default_where", ""))
    if "rep_limit" not in st.session_state:
        st.session_state["rep_limit"] = int(tpl_cfg.get("default_limit", 200))
    if "rep_order" not in st.session_state:
        st.session_state["rep_order"] = str(tpl_cfg.get("default_order_by", ""))

    c1, c2 = st.columns([2, 1])
    with c1:
        where_txt = st.text_input(
            "Filtro WHERE (opcional, sin escribir WHERE)",
            key="rep_where",
            help="Ejemplo: estado = 'Bloqueado' o fecha_proceso >= (current_timestamp() - INTERVAL 24 HOURS)",
        ).strip()
    with c2:
        limit_n = int(
            st.number_input(
                "Límite",
                min_value=1,
                max_value=5000,
                step=50,
                key="rep_limit",
            )
        )
    order_by = st.text_input(
        "ORDER BY (opcional, sin escribir ORDER BY)",
        key="rep_order",
    ).strip()
    sin_limit = st.checkbox(
        "Traer todas las filas (sin LIMIT)",
        value=False,
        key="rep_no_limit",
        help="Puede tardar y consumir memoria. Recomendado solo para tablas pequeñas.",
    )

    campos_sql = "*" if modo_select == "all" else ", ".join(campos)
    if motor == "cassandra":
        keyspace_sel = st.session_state.get("rep_keyspace", "logistica_espana")
        tabla_ref = f"{keyspace_sel}.{tabla}"
        cql = f"SELECT {campos_sql} FROM {tabla_ref}"
        if where_txt:
            cql += f" WHERE {where_txt}"
        if order_by:
            cql += f" ORDER BY {order_by}"
        if not sin_limit:
            cql += f" LIMIT {limit_n}"
        if st.button("Previsualizar informe", key=f"rep_run_cql_{tabla}", type="primary"):
            ok, err, rows = ejecutar_cassandra_cql_seguro(cql)
            if not ok:
                st.error(err or "Error Cassandra")
                return
            df = pd.DataFrame(rows)
            st.session_state["rep_last_df"] = df
            st.session_state["rep_last_sql"] = cql
            st.session_state["rep_last_ctx"] = {
                "motor": motor,
                "tabla": tabla,
                "plantilla": plantilla,
                "meta": admin_meta,
            }
    else:
        sql = f"SELECT {campos_sql} FROM {tabla}"
        if "." not in tabla:
            sql = f"SELECT {campos_sql} FROM {tabla}"
        if where_txt:
            sql += f" WHERE {where_txt}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        if not sin_limit:
            sql += f" LIMIT {limit_n}"
        if st.button("Previsualizar informe", key=f"rep_run_sql_{tabla}", type="primary"):
            ok, err, out = ejecutar_hive_sql_seguro(sql)
            if not ok:
                st.error(err or "Error Hive")
                return
            df = pd.read_csv(io.StringIO(out), sep="\t") if (out or "").strip() else pd.DataFrame()
            st.session_state["rep_last_df"] = df
            st.session_state["rep_last_sql"] = sql
            st.session_state["rep_last_ctx"] = {
                "motor": motor,
                "tabla": tabla,
                "plantilla": plantilla,
                "meta": admin_meta,
            }

    df = st.session_state.get("rep_last_df")
    ctx = st.session_state.get("rep_last_ctx") or {}
    if isinstance(df, pd.DataFrame):
        aliases = tpl_cfg.get("aliases", {})
        if aliases and not df.empty:
            ren = {c: aliases[c] for c in df.columns if c in aliases}
            if ren:
                df = df.rename(columns=ren)
                st.session_state["rep_last_df"] = df
        st.markdown("**Vista previa**")
        if df.empty:
            st.info("Consulta correcta pero sin filas.")
        else:
            st.dataframe(df, width="stretch", hide_index=True)
            st.caption(f"{len(df)} fila(s) · {len(df.columns)} columna(s)")
        with st.expander("Consulta generada", expanded=False):
            st.code(st.session_state.get("rep_last_sql", ""), language="sql")

        try:
            pdf_bytes = _generar_pdf_informe(df, ctx=ctx)
            st.download_button(
                "Descargar PDF",
                data=pdf_bytes,
                file_name=f"informe_{ctx.get('motor','db')}_{ctx.get('tabla','tabla')}.pdf",
                mime="application/pdf",
                key="rep_dl_pdf",
            )
        except Exception as e:
            st.warning(
                "No se pudo generar PDF en este entorno. "
                f"Detalle: {e}. Instala `reportlab` para habilitarlo."
            )

    with st.expander("Gestionar plantillas personalizadas", expanded=False):
        st.caption("Guarda configuración reutilizable: campos, alias, filtros, orden y límite.")
        tpl_name = st.text_input("Nombre plantilla nueva", value="", key="rep_tpl_new_name").strip()
        alias_txt = st.text_area(
            "Alias de columnas (JSON opcional)",
            value=json.dumps(tpl_cfg.get("aliases", {}), ensure_ascii=True),
            height=100,
            key="rep_tpl_aliases",
            help='Ejemplo: {"id_nodo":"Nodo","fecha_proceso":"Fecha"}',
        ).strip()
        ctp1, ctp2 = st.columns([1, 1])
        with ctp1:
            if st.button("Guardar plantilla actual", key="rep_tpl_save"):
                if not tpl_name:
                    st.warning("Indica un nombre de plantilla.")
                else:
                    try:
                        aliases = json.loads(alias_txt) if alias_txt else {}
                        if not isinstance(aliases, dict):
                            raise ValueError("aliases debe ser objeto JSON")
                    except Exception as e:
                        st.error(f"Alias JSON no válido: {e}")
                        return
                    plantillas_usuario[tpl_name] = {
                        "label": tpl_name,
                        "default_limit": limit_n,
                        "default_where": where_txt,
                        "default_order_by": order_by,
                        "aliases": aliases,
                        "default_fields": campos,
                        "default_motor": motor,
                        "default_keyspace": st.session_state.get("rep_keyspace", "logistica_espana"),
                        "default_table": tabla,
                    }
                    ok_g, err_g = _guardar_plantillas_usuario(plantillas_usuario)
                    if ok_g:
                        st.success(f"Plantilla guardada: {tpl_name}")
                    else:
                        st.error(f"No se pudo guardar plantilla: {err_g}")
        with ctp2:
            if st.button("Eliminar plantilla seleccionada", key="rep_tpl_delete"):
                if plantilla in plantillas_base:
                    st.warning("No se puede eliminar una plantilla base.")
                elif plantilla in plantillas_usuario:
                    plantillas_usuario.pop(plantilla, None)
                    ok_g, err_g = _guardar_plantillas_usuario(plantillas_usuario)
                    if ok_g:
                        st.success(f"Plantilla eliminada: {plantilla}")
                    else:
                        st.error(f"No se pudo eliminar plantilla: {err_g}")


def _generar_pdf_informe(df: pd.DataFrame, *, ctx: dict) -> bytes:
    from io import BytesIO
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_JUSTIFY
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph
    from reportlab.pdfgen import canvas

    class NumberedCanvas(canvas.Canvas):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._saved_page_states = []

        def showPage(self):
            self._saved_page_states.append(dict(self.__dict__))
            self._startPage()

        def save(self):
            total = len(self._saved_page_states)
            for state in self._saved_page_states:
                self.__dict__.update(state)
                self._draw_footer(total)
                super().showPage()
            super().save()

        def _draw_footer(self, total_pages: int):
            self.setFont("Helvetica", 8)
            txt = f"{self._pageNumber} de {total_pages}"
            self.drawRightString(w - 1.2 * cm, 0.9 * cm, txt)

    buff = BytesIO()
    w, h = landscape(A4)
    cv = NumberedCanvas(buff, pagesize=landscape(A4))

    def _draw_header(y_top: float) -> float:
        x_left = 1.2 * cm
        if LOGO_PATH.exists():
            try:
                cv.drawImage(str(LOGO_PATH), x_left, y_top - 1.5 * cm, width=1.3 * cm, height=1.3 * cm, preserveAspectRatio=True, mask="auto")
            except Exception:
                pass
        cv.setFont("Helvetica-Bold", 12)
        cv.drawString(x_left + 1.6 * cm, y_top - 0.5 * cm, PROJECT_DISPLAY_NAME)
        cv.setFont("Helvetica", 9)
        cv.drawString(x_left + 1.6 * cm, y_top - 1.0 * cm, "Informe generado desde Cuadro de mando")
        return y_top - 1.8 * cm

    def _draw_paragraph_justified(text: str, x: float, y: float, width: float, font_size: int = 9) -> float:
        style = ParagraphStyle(
            "justified",
            fontName="Helvetica",
            fontSize=font_size,
            leading=font_size + 3,
            alignment=TA_JUSTIFY,
        )
        p = Paragraph(text, style=style)
        _, ph = p.wrap(width, h)
        p.drawOn(cv, x, y - ph)
        return y - ph

    def _draw_form_field(label: str, value: str, x: float, y: float, width: float) -> float:
        cv.setFont("Helvetica-Bold", 9)
        cv.drawString(x, y, label)
        y_line = y - 0.18 * cm
        cv.line(x + 3.2 * cm, y_line, x + width, y_line)
        cv.setFont("Helvetica", 9)
        if value:
            cv.drawString(x + 3.3 * cm, y, str(value)[:120])
        return y - 0.7 * cm

    meta = ctx.get("meta", {}) if isinstance(ctx, dict) else {}
    titulo = f"Informe {ctx.get('plantilla', 'operativa').title()} - {ctx.get('motor', '').upper()}::{ctx.get('tabla', '')}"
    y = _draw_header(h - 1.0 * cm)
    cv.setFont("Helvetica-Bold", 14)
    cv.drawString(1.2 * cm, y, titulo)
    cv.setFont("Helvetica", 9)
    y -= 0.55 * cm
    cv.drawString(1.2 * cm, y, f"Generado: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    y -= 0.45 * cm
    cv.drawString(1.2 * cm, y, f"Fecha emisión: {meta.get('fecha_emision', '')}    Firmante: {meta.get('firmante', '')}")
    y -= 0.45 * cm
    cv.drawString(1.2 * cm, y, f"Cargo: {meta.get('cargo', '')}    Organismo: {meta.get('organismo', '')}")
    y -= 0.55 * cm
    cv.drawString(1.2 * cm, y, f"Filas: {len(df)}  Columnas: {len(df.columns)}")

    obs = str(meta.get("observaciones", "") or "").strip()
    modo_form = bool(meta.get("modo_formulario", False))
    if modo_form:
        y -= 0.9 * cm
        cv.setFont("Helvetica-Bold", 10)
        cv.drawString(1.2 * cm, y, "Formulario de emisión")
        y -= 0.2 * cm
        cv.rect(1.1 * cm, y - 3.0 * cm, w - 2.2 * cm, 3.2 * cm)
        y -= 0.5 * cm
        y = _draw_form_field("Fecha emisión", str(meta.get("fecha_emision", "")), 1.4 * cm, y, w - 3.0 * cm)
        y = _draw_form_field("Firmante", str(meta.get("firmante", "")), 1.4 * cm, y, w - 3.0 * cm)
        y = _draw_form_field("Cargo", str(meta.get("cargo", "")), 1.4 * cm, y, w - 3.0 * cm)
        y = _draw_form_field("Organismo", str(meta.get("organismo", "")), 1.4 * cm, y, w - 3.0 * cm)
        y -= 0.1 * cm
        cv.setFont("Helvetica-Bold", 9)
        cv.drawString(1.4 * cm, y, "Firma")
        cv.line(3.2 * cm, y - 0.18 * cm, w - 2.0 * cm, y - 0.18 * cm)
        y -= 0.8 * cm

    if obs:
        y -= 0.7 * cm
        cv.setFont("Helvetica-Bold", 10)
        cv.drawString(1.2 * cm, y, "Observaciones")
        y -= 0.25 * cm
        y = _draw_paragraph_justified(obs, 1.2 * cm, y, w - 2.4 * cm, font_size=9)

    y -= 0.8 * cm
    x0 = 1.2 * cm
    max_cols = min(len(df.columns), 8)
    cols = list(df.columns[:max_cols])
    if cols:
        col_w = (w - 2.4 * cm) / max(max_cols, 1)
        cv.setFont("Helvetica-Bold", 8)
        for i, c in enumerate(cols):
            cv.drawString(x0 + i * col_w, y, str(c)[:28])
        y -= 0.45 * cm
        cv.setFont("Helvetica", 8)
        max_rows = min(len(df), 120)
        for _, row in df.head(max_rows).iterrows():
            if y < 1.8 * cm:
                cv.showPage()
                y = _draw_header(h - 1.0 * cm) - 0.2 * cm
                cv.setFont("Helvetica-Bold", 8)
                for i, c in enumerate(cols):
                    cv.drawString(x0 + i * col_w, y, str(c)[:28])
                y -= 0.45 * cm
                cv.setFont("Helvetica", 8)
            for i, c in enumerate(cols):
                cv.drawString(x0 + i * col_w, y, str(row.get(c, ""))[:28])
            y -= 0.42 * cm
    else:
        cv.setFont("Helvetica-Oblique", 10)
        cv.drawString(1.2 * cm, y, "Informe sin tabla de datos (plantilla administrativa).")

    cv.showPage()
    cv.save()
    return buff.getvalue()


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
    render_informes_a_medida()
    st.divider()
    render_slides_clima_retrasos()
