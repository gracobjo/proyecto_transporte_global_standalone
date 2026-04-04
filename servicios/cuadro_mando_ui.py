"""
Widgets Streamlit: cuadro de mando, consultas supervisadas y slides de clima/retrasos.
"""
from __future__ import annotations

import io
import json
import os
import time
import uuid

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

from config_nodos import get_nodos
from servicios.clima_retrasos import evaluar_retraso_integrado, obtener_clima_completo_hub
from config import HIVE_DB, HIVE_TABLE_TRANSPORTE_HIST, KEYSPACE
from servicios.consultas_cuadro_mando import (
    CASSANDRA_CONSULTAS,
    CASSANDRA_CATEGORIAS,
    HIVE_CONSULTAS,
    HIVE_CATEGORIAS,
    cassandra_tablas_de_consulta,
    ejecutar_cassandra_consulta,
    ejecutar_cassandra_cql_seguro,
    ejecutar_hive_consulta,
    ejecutar_hive_consulta_detalle,
    ejecutar_hive_sql_seguro,
    ejecutar_hive_sql_seguro_detalle,
    hive_tablas_de_consulta,
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
from servicios.cuadro_mando_analisis_hive import ejecutar_paquete_analisis_hive
from servicios.cuadro_mando_modelo_datos import (
    markdown_contexto_cassandra,
    markdown_contexto_hive,
)
from servicios.asignaciones_ruta_cassandra import (
    listar_asignaciones_dia,
    persistir_asignacion_y_tracking,
)
from servicios.cuadro_mando_flota_mapa import (
    construir_mapa_flota_asignaciones,
    tabla_incidencias_rows,
    texto_resumen_correo,
)
from servicios.ejecucion_pipeline import ejecutar_ingesta, ejecutar_procesamiento
from servicios.estado_y_datos import (
    cargar_nodos_cassandra,
    cargar_tracking_cassandra,
    verificar_kafka_topic,
)
from servicios.notificaciones_correo import enviar_correo_texto, smtp_configurado
from servicios.notificaciones_telegram import notificar_cuadro_mando, probar_telegram_desde_ui
from servicios.simulacion_movimiento_flota import (
    iniciar_estado_simulacion_para_asignaciones,
    sincronizar_posiciones_desde_reloj,
    texto_alerta_ruta_finalizada,
    texto_toast_ruta_finalizada,
)
from servicios.kdd_operaciones import get_operaciones_por_fase
from config import PROJECT_DISPLAY_NAME, SMTP_HOST, TOPIC_TRANSPORTE

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
    for orden, ops in get_operaciones_por_fase():
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


def _render_panel_modelo_cassandra(codigo: str) -> None:
    """Muestra la plantilla CQL y la descripción de negocio del esquema tocado."""
    tablas = cassandra_tablas_de_consulta(codigo)
    cql = (CASSANDRA_CONSULTAS.get(codigo) or {}).get("cql", "").strip()
    with st.expander("Consulta CQL y modelo de datos (Cassandra)", expanded=True):
        st.markdown("#### Plantilla CQL supervisada")
        st.code(cql or "(sin plantilla)", language="sql")
        st.caption(f"Keyspace configurado: `{KEYSPACE}` · Tablas en esta consulta: **{len(tablas)}**")
        if tablas:
            st.markdown("#### Descripción del modelo")
            st.markdown(markdown_contexto_cassandra(tablas))
        if tablas and st.button(
            "Leer columnas desde Cassandra (metadatos)",
            key=f"cass_meta_cols_{codigo}",
            help="Una petición de metadatos por tabla; requiere nodo CQL accesible.",
        ):
            for t in tablas:
                ok_c, err_c, cols = listar_columnas_cassandra(t)
                if ok_c and cols:
                    st.markdown(f"**`{t}`** ({len(cols)} columnas)")
                    st.code(", ".join(cols), language="text")
                else:
                    st.warning(f"No se pudieron listar columnas de `{t}`: {err_c}")


def _render_panel_modelo_hive(codigo: str) -> None:
    """Plantilla SQL Hive + tablas tocadas + descripción analítica."""
    tablas = sorted(hive_tablas_de_consulta(codigo))
    sql = (HIVE_CONSULTAS.get(codigo) or {}).get("sql", "").strip()
    with st.expander("Consulta SQL y modelo de datos (Hive)", expanded=True):
        st.markdown("#### Plantilla SQL supervisada")
        st.code(sql or "(sin plantilla)", language="sql")
        st.caption(
            f"Base de datos Hive: `{HIVE_DB}` · Tablas requeridas: **{len(tablas)}** "
            f"(nombre físico transporte: `{HIVE_TABLE_TRANSPORTE_HIST}`)"
        )
        if tablas:
            st.markdown("#### Descripción del modelo")
            st.markdown(markdown_contexto_hive(tablas, HIVE_TABLE_TRANSPORTE_HIST))
        if tablas and st.button(
            "Leer columnas desde Hive (DESCRIBE)",
            key=f"hive_meta_cols_{codigo}",
        ):
            for t in tablas:
                ok_h, err_h, cols = listar_columnas_hive(t)
                if ok_h and cols:
                    st.markdown(f"**`{t}`** ({len(cols)} columnas)")
                    st.code(", ".join(cols), language="text")
                else:
                    st.warning(f"No se pudieron listar columnas de `{t}`: {err_h}")


def render_analisis_hive_ia() -> None:
    """Estadísticas y proyecciones simples sobre consultas históricas aprobadas."""
    st.subheader("Análisis asistido sobre histórico (Hive)")
    st.caption(
        "Combina varias **plantillas Hive aprobadas** (agregaciones, evolución de eventos, resumen de incidencias). "
        "Incluye `describe` sobre columnas numéricas, gráficos de serie y **proyecciones heurísticas** "
        "(regresión lineal sobre el índice temporal de la muestra). No es un modelo de IA entrenado ni sustituye "
        "a un motor de ML en producción."
    )
    if st.button("Ejecutar paquete de análisis y predicciones simples", key="btn_hive_ia_pack", type="secondary"):
        with st.spinner("Consultando Hive (3 plantillas + cálculos en memoria)…"):
            pack = ejecutar_paquete_analisis_hive()
        if pack.get("errores"):
            for e in pack["errores"]:
                st.error(e)
        if pack.get("ok_general") is False and not pack.get("errores"):
            st.warning("Algunas consultas no devolvieron datos.")
        if pack.get("predicciones_md"):
            st.markdown("#### Síntesis y predicciones heurísticas")
            st.markdown(pack["predicciones_md"])
        if pack.get("tablas_resumen_md"):
            with st.expander("Estadísticas numéricas (`describe`)", expanded=False):
                st.markdown(pack["tablas_resumen_md"])
        df_serie = pack.get("dfs", {}).get("_serie_agg_diaria")
        if df_serie is not None and not df_serie.empty and "contador" in df_serie.columns:
            st.markdown("#### Serie diaria — suma de `contador` (agregaciones)")
            tmp = df_serie.copy()
            tmp["fecha"] = tmp["fecha"].astype(str)
            st.line_chart(tmp.set_index("fecha")["contador"])
        df_ev = pack.get("dfs", {}).get("_eventos_total_por_dia")
        if df_ev is not None and not df_ev.empty and "total" in df_ev.columns:
            st.markdown("#### Eventos agregados por `dia` (evolución)")
            st.line_chart(df_ev.set_index("dia")["total"])


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
                notificar_cuadro_mando("Ingesta (fases 1–2)", True, f"returncode={code}")
                st.session_state.timeline = st.session_state.get("timeline", []) + [
                    f"{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')} — Ingesta (cuadro)"
                ]
            else:
                st.error(f"Código {code}")
                st.code(err[-2500:] or out[-2500:])
                notificar_cuadro_mando(
                    "Ingesta (fases 1–2)",
                    False,
                    (err or out or "")[-600:],
                )
    with c2:
        if st.button("Ejecutar Spark (fases 3–5)", key="cm_spark", width="stretch"):
            with st.spinner("Spark…"):
                code, out, err = ejecutar_procesamiento()
            if code == 0:
                st.success("Procesamiento OK.")
                notificar_cuadro_mando("Spark (fases 3–5)", True, f"returncode={code}")
                st.session_state.timeline = st.session_state.get("timeline", []) + [
                    f"{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')} — Spark (cuadro)"
                ]
            else:
                st.error(f"Código {code}")
                st.code(err[-2500:] or out[-2500:])
                notificar_cuadro_mando(
                    "Spark (fases 3–5)",
                    False,
                    (err or out or "")[-600:],
                )
    with c3:
        if st.button("Refrescar datos clima (API)", key="cm_clima", width="stretch"):
            st.session_state["clima_refresh"] = datetime.now(timezone.utc).isoformat()
            # Forzamos recarga de slides (API clima) en el siguiente render
            st.session_state["slides_clima_ready"] = False
            notificar_cuadro_mando(
                "Refrescar datos clima (API)",
                True,
                "Sesión marcada para recargar datos de clima en la vista de slides.",
            )
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

    _render_panel_modelo_cassandra(consulta_seleccionada)

    # Ejecutar consulta
    if st.button("Ejecutar consulta Cassandra", key="btn_cql", type="primary"):
        with st.spinner("Ejecutando Cassandra…"):
            ok, err, rows = ejecutar_cassandra_consulta(consulta_seleccionada)
        if ok and rows:
            st.dataframe(pd.DataFrame(rows), width="stretch")
            st.caption(f"✅ {len(rows)} fila(s) devueltas.")
            notificar_cuadro_mando(
                f"Consulta Cassandra: {titulo_cassandra(consulta_seleccionada)}",
                True,
                f"{len(rows)} fila(s)",
            )
        elif ok:
            st.info("Consulta correcta pero sin filas (tabla vacía).")
            notificar_cuadro_mando(
                f"Consulta Cassandra: {titulo_cassandra(consulta_seleccionada)}",
                True,
                "Sin filas",
            )
        else:
            st.error(f"Error Cassandra: {err}")
            notificar_cuadro_mando(
                f"Consulta Cassandra: {titulo_cassandra(consulta_seleccionada)}",
                False,
                str(err or "")[:500],
            )

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
                notificar_cuadro_mando("CQL personalizado (Cassandra)", True, f"{len(rows)} fila(s)")
            elif ok:
                st.info("Consulta correcta pero sin filas.")
                notificar_cuadro_mando("CQL personalizado (Cassandra)", True, "Sin filas")
            else:
                st.error(f"Error: {err or 'Error desconocido'}")
                notificar_cuadro_mando("CQL personalizado (Cassandra)", False, str(err or "")[:500])
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

    def _disponible(codigo: str) -> bool:
        req = hive_tablas_de_consulta(codigo)
        if not req:
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
                notificar_cuadro_mando("Verificar tablas en Hive", True, f"{len(nuevas)} tablas")
            else:
                st.session_state.hive_tablas_err = err_tabs or "error"
                st.warning(f"No se pudieron listar tablas: {err_tabs}")
                notificar_cuadro_mando("Verificar tablas en Hive", False, str(err_tabs or "")[:500])
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

    if sel:
        _render_panel_modelo_hive(sel)

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
            return "Sin conexión a la API de clima"
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
                notificar_cuadro_mando(
                    f"Consulta Hive: {titulo_hive(sel)}",
                    True,
                    f"{elapsed:.1f}s · sin filas · caché={'sí' if desde_cache else 'no'}",
                )
            else:
                df = _hive_tsv_a_dataframe(out_s)
                _render_df_usuario(df)
                with st.expander("Ver salida raw (TSV)"):
                    st.text_area("Salida (TSV)", value=out_s, height=220)
                notificar_cuadro_mando(
                    f"Consulta Hive: {titulo_hive(sel)}",
                    True,
                    f"{elapsed:.1f}s · {len(df)} fila(s) · caché={'sí' if desde_cache else 'no'}",
                )
        else:
            st.error(f"Error Hive: {err}")
            notificar_cuadro_mando(
                f"Consulta Hive: {titulo_hive(sel)}",
                False,
                str(err or "")[:500],
            )
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
                    notificar_cuadro_mando("SQL personalizado (Hive)", True, f"{elapsed:.1f}s · sin filas")
                else:
                    df = _hive_tsv_a_dataframe(out_s)
                    _render_df_usuario(df)
                    notificar_cuadro_mando(
                        "SQL personalizado (Hive)",
                        True,
                        f"{elapsed:.1f}s · {len(df)} fila(s)",
                    )
            else:
                st.error(f"Error: {err or 'Error desconocido'}")
                notificar_cuadro_mando("SQL personalizado (Hive)", False, str(err or "")[:500])
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
            notificar_cuadro_mando("Vaciar caché Hive", True, "Caché de consultas en memoria limpiada.")


def render_slides_clima_retrasos() -> None:
    st.subheader("Slides — Clima y anticipación de retrasos")
    st.info(
        "Esta vista resume **riesgo por hub**. Para la **planificación de transporte** en la red completa "
        "(todas las conexiones hub/subnodo, formulario origen→destino, **pasos**, **mapa** con ruta "
        "principal y **alternativas** en otro color), abre la pestaña **Rutas híbridas**."
    )
    st.caption(
        "Cada **slide** corresponde a un hub. Se combinan variables meteorológicas (tormenta, nieve, "
        "lluvia, niebla, viento, visibilidad) con el **estado operativo** en Cassandra (atascos, "
        "obras, bloqueos simulados) para estimar un margen de retraso orientativo."
    )

    # Evita que cada recarga del dashboard haga 5 llamadas HTTP + lecturas Cassandra.
    # Esto hace que el tab "Cuadro de mando" sea rápido incluso si la API de clima/Hive/Cassandra están lentos.
    if not st.session_state.get("slides_clima_ready"):
        st.caption(
            "Para generar las slides (requiere API de clima y Cassandra), pulsa el botón. "
            "Así la app carga rápido y no se bloquea al refrescar la página."
        )
        if st.button("Cargar clima y generar slides", key="slides_clima_load"):
            st.session_state["slides_clima_ready"] = True
            st.session_state["tg_slides_pending"] = True
            st.rerun()
        return

    nodos = cargar_nodos_cassandra()
    # Solo 5 capitales de referencia (evita decenas de llamadas HTTP al cargar slides).
    hubs_order: List[str] = ["Madrid", "Barcelona", "Bilbao", "Sevilla", "Valencia"]
    nodos_cfg = get_nodos()
    clima: Dict[str, Dict[str, Any]] = {}
    for h in hubs_order:
        if h not in nodos_cfg:
            continue
        m = nodos_cfg[h]
        clima[h] = obtener_clima_completo_hub(h, float(m["lat"]), float(m["lon"]))

    slides: List[Tuple[str, dict, dict]] = []
    for h in hubs_order:
        if h not in clima:
            continue
        raw = clima[h]
        ev = evaluar_retraso_integrado(raw, nodos)
        slides.append((h, raw, ev))

    if not slides:
        st.warning("No se pudo obtener clima para los hubs.")
        if st.session_state.pop("tg_slides_pending", False):
            notificar_cuadro_mando(
                "Slides clima y retrasos",
                False,
                "Sin datos de clima para los hubs.",
            )
        return

    # Slide de síntesis
    totales = [s[2].get("minutos_estimados", 0) for s in slides]
    max_hub = max(slides, key=lambda x: x[2].get("minutos_estimados", 0))[0]
    slides.append(
        (
            "Síntesis red nacional",
            {"hub": "Síntesis", "descripcion": "Agregado orientativo (muestra de capitales)"},
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

    if st.session_state.pop("tg_slides_pending", False):
        notificar_cuadro_mando(
            "Slides clima y retrasos",
            True,
            f"{len(slides)} slide(s) (incl. síntesis).",
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
            st.caption(f"Categoría (heurística): **{ev['categoria']}**")
    with c3:
        if raw.get("temp") is not None:
            st.metric("Temperatura (°C)", f"{raw['temp']:.1f}")
        if raw.get("viento_vel") is not None:
            st.metric("Viento (m/s)", f"{raw['viento_vel']:.1f}")

    if hub != "Síntesis red nacional" and not raw.get("error"):
        st.markdown("**Variables meteorológicas (API clima)**")
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
        "según heurísticas de condición y estado de nodos en Cassandra."
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
            notificar_cuadro_mando(
                "Informe PDF administrativo",
                True,
                "Vista previa lista; descarga desde «Descargar PDF».",
            )

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
                notificar_cuadro_mando(
                    f"Previsualizar informe (Cassandra · {tabla})",
                    False,
                    str(err or "")[:500],
                )
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
            notificar_cuadro_mando(
                f"Previsualizar informe (Cassandra · {tabla})",
                True,
                f"{len(df)} fila(s)",
            )
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
                notificar_cuadro_mando(
                    f"Previsualizar informe (Hive · {tabla})",
                    False,
                    str(err or "")[:500],
                )
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
            notificar_cuadro_mando(
                f"Previsualizar informe (Hive · {tabla})",
                True,
                f"{len(df)} fila(s)",
            )

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
                        notificar_cuadro_mando("Guardar plantilla informes", True, tpl_name)
                    else:
                        st.error(f"No se pudo guardar plantilla: {err_g}")
                        notificar_cuadro_mando("Guardar plantilla informes", False, str(err_g)[:500])
        with ctp2:
            if st.button("Eliminar plantilla seleccionada", key="rep_tpl_delete"):
                if plantilla in plantillas_base:
                    st.warning("No se puede eliminar una plantilla base.")
                elif plantilla in plantillas_usuario:
                    plantillas_usuario.pop(plantilla, None)
                    ok_g, err_g = _guardar_plantillas_usuario(plantillas_usuario)
                    if ok_g:
                        st.success(f"Plantilla eliminada: {plantilla}")
                        notificar_cuadro_mando("Eliminar plantilla informes", True, str(plantilla))
                    else:
                        st.error(f"No se pudo eliminar plantilla: {err_g}")
                        notificar_cuadro_mando("Eliminar plantilla informes", False, str(err_g)[:500])


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


def _asignaciones_desde_tabla_cuadro(filas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convierte filas de `listar_asignaciones_dia()` al formato del mapa ({id, camion, origen, destino, id_ruta})."""
    out: List[Dict[str, Any]] = []
    for r in filas:
        cid = str(r.get("id_camion") or "").strip()
        o = str(r.get("origen") or "").strip()
        d = str(r.get("destino") or "").strip()
        ir = str(r.get("id_ruta") or "").strip()
        if not cid or not o or not d:
            continue
        out.append(
            {
                "id": f"cuadro_{ir}_{cid}"[:48],
                "camion": cid,
                "origen": o,
                "destino": d,
                "id_ruta": ir,
            }
        )
    return out


def render_flota_rutas_mapa() -> None:
    """Multi-camión: origen/destino configurables, mapa y alertas por correo."""
    st.subheader("Flota: rutas por camión y mapa operativo")
    st.caption(
        "Configura **varias** rutas (camión + origen + destino) sobre la red de nodos; no está limitado a un solo hub. "
        "Al pulsar **Añadir ruta** se guarda en **`asignaciones_ruta_cuadro`** (día + camión + `id_ruta`) y se actualiza **`tracking_camiones`**."
    )
    with st.expander("Por qué en «Tracking» veo más filas que camiones simulados", expanded=False):
        st.markdown(
            "En Cassandra, `tracking_camiones` tiene **una fila por `id_camion`** (clave primaria). "
            "Los camiones de **pruebas antiguas** (`camion_prueba_*`, etc.) **siguen** hasta que los borres con CQL. "
            "La ingesta periódica **vuelve a escribir** los 5 camiones de simulación; no elimina ids antiguos. "
            "Tiempo real = **último valor por camión**, no «solo 5 filas en el mundo»."
        )
    st.caption(
        "Marcadores: verde (OK), naranja (aviso / sin telemetría), rojo (retraso o motivo informado)."
    )
    if "cm_flota_asignaciones" not in st.session_state:
        st.session_state.cm_flota_asignaciones = []
    if "cm_sim_trucks" not in st.session_state:
        st.session_state.cm_sim_trucks = {}
    if "cm_sim_live" not in st.session_state:
        st.session_state.cm_sim_live = False

    nodos = get_nodos()
    nombres_nodos = sorted(nodos.keys())
    tracking = cargar_tracking_cassandra()
    ids_existentes = sorted({str(t.get("id_camion")) for t in tracking if t.get("id_camion")})

    col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
    with col1:
        st.selectbox(
            "Camión (lista en Cassandra)",
            options=[""] + ids_existentes,
            format_func=lambda x: "— elegir o escribir abajo —" if x == "" else x,
            key="cm_fl_camion_sel",
        )
        camion_txt = st.text_input(
            "Id de camión (manual)",
            key="cm_fl_camion_txt",
            placeholder="ej. CAMION_01",
            help="Si rellenas esto, tiene prioridad sobre la lista.",
        )
    idx_m = nombres_nodos.index("Madrid") if "Madrid" in nombres_nodos else 0
    idx_b = nombres_nodos.index("Barcelona") if "Barcelona" in nombres_nodos else min(1, len(nombres_nodos) - 1)
    with col2:
        st.selectbox("Origen", options=nombres_nodos, index=idx_m, key="cm_fl_origen")
    with col3:
        st.selectbox("Destino", options=nombres_nodos, index=idx_b, key="cm_fl_destino")
    with col4:
        st.write("")
        st.write("")
        add_clicked = st.button("Añadir ruta", type="primary", key="cm_fl_add")

    if add_clicked:
        manual = (st.session_state.get("cm_fl_camion_txt") or "").strip()
        sel = (st.session_state.get("cm_fl_camion_sel") or "").strip()
        cid = manual or sel
        origen = st.session_state.get("cm_fl_origen", nombres_nodos[0])
        destino = st.session_state.get("cm_fl_destino", nombres_nodos[0])
        if not cid:
            st.warning("Indica un id de camión (lista o campo manual).")
        elif origen == destino:
            st.warning("Origen y destino deben ser distintos.")
        else:
            ok_db, err_db, id_ruta = persistir_asignacion_y_tracking(cid, origen, destino)
            if not ok_db:
                st.error(err_db)
                notificar_cuadro_mando("Flota: añadir ruta", False, str(err_db)[:500])
            else:
                st.session_state.cm_flota_asignaciones.append(
                    {
                        "id": uuid.uuid4().hex[:10],
                        "camion": cid,
                        "origen": origen,
                        "destino": destino,
                        "id_ruta": id_ruta,
                    }
                )
                st.success(f"Persistido en Cassandra · `id_ruta`={id_ruta}")
                notificar_cuadro_mando(
                    "Flota: añadir ruta",
                    True,
                    f"{cid} {origen}→{destino} · id_ruta={id_ruta}",
                )
                st.rerun()

    ok_asg, err_asg, filas_asg = listar_asignaciones_dia()
    if ok_asg and filas_asg:
        st.markdown("**Asignaciones guardadas hoy** (`asignaciones_ruta_cuadro`)")
        st.dataframe(pd.DataFrame(filas_asg), width="stretch", hide_index=True)
    elif ok_asg and not filas_asg:
        st.caption("Aún no hay filas en `asignaciones_ruta_cuadro` para hoy (UTC).")
    elif err_asg:
        st.warning(f"No se pudo leer `asignaciones_ruta_cuadro`: {err_asg[:200]}")

    ses_list: List[Dict[str, Any]] = st.session_state.cm_flota_asignaciones
    if ses_list:
        asignaciones_para_mapa = ses_list
    elif ok_asg and filas_asg:
        asignaciones_para_mapa = _asignaciones_desde_tabla_cuadro(filas_asg)
    else:
        asignaciones_para_mapa = []

    if ses_list:
        st.markdown("**Rutas en la sesión (mapa y correo)**")
        st.caption("«Quitar» solo elimina la entrada de esta vista; los datos ya escritos en Cassandra no se borran.")
        for a in list(ses_list):
            c1, c2 = st.columns([5, 1])
            rid = a.get("id_ruta")
            extra = f" · `id_ruta={rid}`" if rid else ""
            c1.markdown(f"**{a['camion']}** · {a['origen']} → **{a['destino']}**{extra}")
            if c2.button("Quitar", key=f"cm_fl_rm_{a['id']}"):
                st.session_state.cm_flota_asignaciones = [
                    x for x in st.session_state.cm_flota_asignaciones if x["id"] != a["id"]
                ]
                st.rerun()

    if not ses_list and asignaciones_para_mapa and ok_asg and filas_asg:
        st.markdown("**Mapa operativo**")
        st.caption(
            "Hay rutas en la tabla de **hoy** (Cassandra) pero la lista de sesión está vacía: "
            "el mapa se muestra igualmente. Usa el botón para copiar la tabla a la sesión y poder **Quitar** filas de la vista."
        )
        if st.button("Traer asignaciones de hoy a la sesión editable", key="cm_fl_pull_today"):
            st.session_state.cm_flota_asignaciones = _asignaciones_desde_tabla_cuadro(filas_asg)
            notificar_cuadro_mando(
                "Flota: traer asignaciones de hoy",
                True,
                f"{len(filas_asg)} fila(s) en tabla",
            )
            st.rerun()

    cimp1, cimp2 = st.columns(2)
    with cimp1:
        if st.button("Rellenar desde tracking Cassandra", key="cm_fl_fill_track"):
            nuevas = []
            seen = {x["camion"] for x in ses_list}
            for t in tracking:
                cid = str(t.get("id_camion") or "").strip()
                ro = (t.get("ruta_origen") or "").strip()
                rd = (t.get("ruta_destino") or "").strip()
                if not cid or not ro or not rd:
                    continue
                if ro not in nodos or rd not in nodos:
                    continue
                if cid in seen:
                    continue
                nuevas.append(
                    {"id": uuid.uuid4().hex[:10], "camion": cid, "origen": ro, "destino": rd}
                )
                seen.add(cid)
            if nuevas:
                notificar_cuadro_mando(
                    "Flota: rellenar desde tracking",
                    True,
                    f"{len(nuevas)} ruta(s) nueva(s)",
                )
            else:
                notificar_cuadro_mando(
                    "Flota: rellenar desde tracking",
                    True,
                    "Sin rutas nuevas que añadir",
                )
            st.session_state.cm_flota_asignaciones.extend(nuevas)
            st.rerun()
    with cimp2:
        if st.button("Vaciar lista", key="cm_fl_clear"):
            st.session_state.cm_flota_asignaciones = []
            notificar_cuadro_mando("Flota: vaciar lista sesión", True, "Lista de rutas en sesión vaciada.")
            st.rerun()

    if asignaciones_para_mapa:
        st.caption(
            "Las posiciones vienen de **`tracking_camiones`** (última escritura). Si no hay simulación activa, "
            "suelen quedarse en el **origen** y varios camiones pueden coincidir en el mismo punto: el mapa **separa los pins** "
            "un poco para que los distingas. Para ver **movimiento** a lo largo de la ruta, usa el expander siguiente y "
            "**Iniciar simulación** (intervalo p. ej. 5 s)."
        )
        with st.expander("Simulación de movimiento en el mapa (actualización periódica)", expanded=True):
            st.caption(
                "Camino mínimo (BFS) entre origen y destino; la posición en `tracking_camiones` se interpola "
                "según el tiempo para ver el avance en el plano. Al llegar al destino: alerta en pantalla y, "
                "opcionalmente, correo con ruta, camión, horas e incidencias."
            )
            r1, r2, r3 = st.columns(3)
            with r1:
                st.number_input("Intervalo de refresco del mapa (s)", 1, 60, 5, key="cm_sim_refresh_sec")
            with r2:
                st.number_input("Duración total del viaje (s)", 10, 7200, 120, key="cm_sim_dur_sec")
            with r3:
                st.checkbox("Correo al finalizar cada ruta", value=False, key="cm_sim_mail_fin")
            b1, b2 = st.columns(2)
            with b1:
                if st.button("Iniciar simulación", key="cm_sim_start", width="stretch"):
                    dur = float(st.session_state.get("cm_sim_dur_sec", 120))
                    state, warns = iniciar_estado_simulacion_para_asignaciones(
                        asignaciones_para_mapa, dur
                    )
                    st.session_state.cm_sim_trucks = state
                    st.session_state.cm_sim_live = bool(state)
                    for w in warns:
                        st.warning(w)
                    if state:
                        sec = int(st.session_state.get("cm_sim_refresh_sec", 5))
                        st.success(
                            f"Simulación activa: {len(state)} camión(es). Mapa cada {sec} s · viaje ~{int(dur)} s."
                        )
                        notificar_cuadro_mando(
                            "Flota: iniciar simulación",
                            True,
                            f"{len(state)} camión(es) · intervalo {sec}s · duración ~{int(dur)}s",
                        )
                    elif not warns:
                        st.warning("No hay rutas válidas para simular.")
                        notificar_cuadro_mando("Flota: iniciar simulación", False, "Sin rutas válidas")
                    st.rerun()
            with b2:
                if st.button("Detener simulación", key="cm_sim_stop", width="stretch"):
                    st.session_state.cm_sim_trucks = {}
                    st.session_state.cm_sim_live = False
                    notificar_cuadro_mando("Flota: detener simulación", True, "Simulación de mapa detenida.")
                    st.rerun()
            if st.session_state.get("cm_sim_live") and st.session_state.get("cm_sim_trucks"):
                sim_d = st.session_state.cm_sim_trucks or {}
                pend = sum(1 for s in sim_d.values() if not s.get("notificado_fin"))
                if pend == 0:
                    st.caption(
                        "Todas las rutas de esta simulación han **finalizado**. Pulsa **Detener simulación** "
                        "para dejar de refrescar el mapa automáticamente."
                    )

        iv = max(1, int(st.session_state.get("cm_sim_refresh_sec", 5)))

        def _pintar_mapa_e_incidencias(tracking_list: List[Dict[str, Any]]) -> None:
            mapa = construir_mapa_flota_asignaciones(asignaciones_para_mapa, tracking_list)
            st_folium(mapa, width=None, height=520, returned_objects=[], key="folium_cm_flota")
            df_inc = pd.DataFrame(tabla_incidencias_rows(asignaciones_para_mapa, tracking_list))
            st.markdown("**Incidencias y retrasos**")
            st.dataframe(df_inc, width="stretch", hide_index=True)

        if st.session_state.get("cm_sim_live") and st.session_state.get("cm_sim_trucks"):
            @st.fragment(run_every=timedelta(seconds=iv))
            def _mapa_vivo() -> None:
                sim = dict(st.session_state.get("cm_sim_trucks") or {})
                sim, events = sincronizar_posiciones_desde_reloj(sim)
                st.session_state.cm_sim_trucks = sim
                tracking_live = cargar_tracking_cassandra()
                for ev in events:
                    st.toast(texto_toast_ruta_finalizada(ev), icon="✅")
                    notificar_cuadro_mando(
                        "Flota: ruta finalizada (simulación mapa)",
                        True,
                        texto_toast_ruta_finalizada(ev)[:800],
                    )
                    if st.session_state.get("cm_sim_mail_fin"):
                        dests = [
                            x.strip()
                            for x in (st.session_state.get("cm_fl_emails") or "").replace(";", ",").split(",")
                            if x.strip()
                        ]
                        if dests and smtp_configurado():
                            ok_m, msg_m = enviar_correo_texto(
                                dests,
                                f"[SIMLOG] Ruta finalizada · {ev.get('id_camion')}",
                                texto_alerta_ruta_finalizada(ev),
                            )
                            if not ok_m:
                                st.caption(f"Correo no enviado: {msg_m[:160]}")
                _pintar_mapa_e_incidencias(tracking_live)

            _mapa_vivo()
        else:
            tracking_static = cargar_tracking_cassandra()
            _pintar_mapa_e_incidencias(tracking_static)

        st.markdown("**Correo electrónico**")
        st.caption(
            "Varias direcciones separadas por coma. Requiere `SIMLOG_SMTP_HOST` (y credenciales si el servidor las exige)."
        )
        emails = st.text_input(
            "Destinatarios",
            key="cm_fl_emails",
            placeholder="logistica@empresa.com, control@empresa.com",
        )
        solo_ret = st.checkbox("Enviar solo si hay retraso o aviso (incl. sin telemetría)", value=False, key="cm_fl_mail_delay")
        if st.button("Enviar resumen por correo", key="cm_fl_send_mail"):
            dests = [x.strip() for x in emails.replace(";", ",").split(",") if x.strip()]
            tracking_mail = cargar_tracking_cassandra()
            rows = tabla_incidencias_rows(st.session_state.cm_flota_asignaciones, tracking_mail)
            if solo_ret and not any(r.get("nivel") in ("retraso", "aviso") for r in rows):
                st.info("Nada que enviar según el filtro (sin retrasos ni avisos).")
                notificar_cuadro_mando(
                    "Flota: enviar resumen por correo",
                    True,
                    "Sin contenido según filtro (solo retrasos/avisos)",
                )
            elif not dests:
                st.warning("Indica al menos un correo.")
                notificar_cuadro_mando("Flota: enviar resumen por correo", False, "Sin destinatarios")
            else:
                body = texto_resumen_correo(st.session_state.cm_flota_asignaciones, tracking_mail)
                ok, msg = enviar_correo_texto(dests, "[SIMLOG] Resumen flota y rutas", body)
                if ok:
                    st.success(msg)
                    notificar_cuadro_mando(
                        "Flota: enviar resumen por correo",
                        True,
                        f"{len(dests)} destinatario(s)",
                    )
                else:
                    st.error(msg)
                    notificar_cuadro_mando("Flota: enviar resumen por correo", False, msg[:500])
        if smtp_configurado() and (
            "office365" in (SMTP_HOST or "").lower()
            or "outlook.office365" in (SMTP_HOST or "").lower()
        ):
            st.caption(
                "**Microsoft 365:** si aparece error 535 / «basic authentication is disabled», un administrador "
                "debe habilitar **SMTP autenticado** para el buzón, o configura **smtp.gmail.com** con "
                "**contraseña de aplicación** (2FA activado en Google)."
            )
        if not smtp_configurado():
            st.info("SMTP no configurado: exporta `SIMLOG_SMTP_HOST` (p. ej. smtp.gmail.com:587 con TLS).")
    else:
        st.info(
            "No hay rutas para el mapa: añade alguna con **Añadir ruta**, o espera a que existan filas en "
            "`asignaciones_ruta_cuadro` para hoy (UTC), o usa **Rellenar desde tracking**."
        )


def render_diagnostico_notificaciones() -> None:
    """Comprueba Telegram (y recuerda requisitos de Gmail) sin exponer secretos."""
    with st.expander("Notificaciones: probar Telegram y recordatorio Gmail", expanded=False):
        st.caption(
            "Tras editar `.env`, **reinicia Streamlit** para recargar variables. "
            "En Telegram, abre el bot y envía **/start** antes de esperar mensajes."
        )
        if st.button("Enviar mensaje de prueba a Telegram", key="cm_test_telegram"):
            ok, msg = probar_telegram_desde_ui()
            if ok:
                st.success(msg)
            else:
                st.error(msg)
        st.markdown(
            "**Gmail con 2FA:** rellena en `.env` `SIMLOG_SMTP_USER`, `SIMLOG_SMTP_PASSWORD` "
            "(contraseña de **aplicación** de 16 caracteres, no la de la cuenta) y `SIMLOG_SMTP_FROM`. "
            "[Crear contraseña de aplicación](https://myaccount.google.com/apppasswords)"
        )


def render_cuadro_mando_tab() -> None:
    """Contenido completo de la pestaña Cuadro de mando."""
    st.header("Cuadro de mando")
    st.markdown(
        "Vista operativa del ciclo KDD: **tareas por fase**, consultas a **Cassandra** (datos en vivo) "
        "y **Hive** (histórico), y **slides** de anticipación de retrasos por clima e incidencias."
    )
    render_operaciones_por_fase()
    render_acciones_rapidas()
    render_diagnostico_notificaciones()
    render_verificacion_kafka_cuadro()
    st.divider()
    render_consultas_cassandra()
    st.divider()
    render_consultas_hive()
    st.divider()
    render_analisis_hive_ia()
    st.divider()
    render_informes_a_medida()
    st.divider()
    render_flota_rutas_mapa()
    st.divider()
    render_slides_clima_retrasos()
