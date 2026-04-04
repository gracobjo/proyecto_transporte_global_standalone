"""
Pestaña Streamlit: trazabilidad y registro de pruebas de ingesta.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from servicios.kdd_informes_lectura import (
    catalogo_fases_kdd,
    informes_por_run_id,
    listar_run_ids_con_informes,
)
from servicios.pruebas_ingesta import (
    REGISTRO_PATH,
    capturar_snapshot_pruebas,
    describir_canales_ingesta,
    leer_registro_pruebas,
    listar_cadenas_kdd_airflow_recientes,
    registrar_prueba_airflow_cadena_kdd_completa,
    registrar_prueba_ingesta,
    resumen_ejecutivo_registro,
    resumen_tabular_pruebas,
    tipos_prueba_kdd_resumen,
)


def _valor(snapshot: dict, *keys):
    cur = snapshot
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _fmt_ts(iso: str | None) -> str:
    if not iso:
        return "—"
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return iso[:19] + "…"


def render_pruebas_ingesta_tab() -> None:
    st.subheader("Pruebas de ingesta y trazabilidad")
    st.caption(
        "El **JSON** solo se rellena solo con **botones del sidebar** (Streamlit) o al **registrar** manualmente. "
        "Las ejecuciones **Airflow** dejan informes en `reports/kdd/`; abajo se **detectan** automáticamente "
        "(fase 99 = cadena completa) y puedes **añadirlas al historial** sin repetir el trabajo."
    )

    # --- Resumen dinámico (registro + snapshot) ---
    ej = resumen_ejecutivo_registro()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Entradas en registro (JSON)", ej["total_registros"])
    c2.metric("Desde sidebar Streamlit", ej["registros_desde_streamlit"])
    ult = ej.get("ultima_prueba") or {}
    c3.metric(
        "Última entrada JSON",
        _fmt_ts(ult.get("timestamp")) if ult else "—",
    )
    uf = ej.get("ultima_desde_streamlit") or {}
    c4.metric(
        "Última desde sidebar",
        uf.get("resultado", "—") if uf else "—",
        help="Última ejecución registrada con canal Frontend Streamlit (ingesta/Spark/pipeline).",
    )
    c5.metric(
        "Cadenas KDD (Airflow) en disco",
        ej.get("cadenas_kdd_airflow_detectadas", 0),
        help="Número de informes informe_99*.md encontrados bajo reports/kdd/ (cada uno = una cadena llegó a fase 99).",
    )

    st.markdown("#### Ejecuciones Airflow — cadenas KDD completas (informe fase 99)")
    st.info(
        "Si ejecutaste **simlog_kdd_00 → … → 99** en Airflow, cada fase escribe su carpeta bajo `reports/kdd/`. "
        "Cuando existe **`informe_99_*` (consulta final)** se considera que **esa cadena terminó**. "
        "Eso **no** escribe solo en el JSON: aquí lo ves; usa **Registrar** para dejar constancia en el historial descargable."
    )
    air_rows = listar_cadenas_kdd_airflow_recientes(20)
    if air_rows:
        st.dataframe(pd.DataFrame(air_rows), width="stretch", hide_index=True)
        opciones = {f"{r['modificado_utc']} — {r['carpeta_run_id'][:56]}": r for r in air_rows}
        clave = st.selectbox(
            "Elegir ejecución para añadir al registro JSON",
            options=list(opciones.keys()),
            key="sel_airflow_cadena",
        )
        if st.button("Registrar ejecución seleccionada en el historial", key="btn_reg_airflow"):
            ruta = opciones[clave]["ruta"]
            reg = leer_registro_pruebas()
            if any(ruta in (x.get("detalle") or "") for x in reg):
                st.warning("Esta ejecución ya consta en el registro (misma ruta de informe).")
            else:
                registrar_prueba_airflow_cadena_kdd_completa(ruta_informe_f99=ruta)
                st.session_state["snapshot_pruebas_ingesta"] = capturar_snapshot_pruebas()
                st.success("Añadida al JSON. El contador «Entradas en registro» debería subir.")
                st.rerun()
    else:
        st.warning(
            "No se encontró ningún `informe_99*.md` bajo `reports/kdd/` (excl. `work`). "
            "Ejecuta la cadena hasta `simlog_kdd_99_consulta_final` o comprueba la ruta del proyecto."
        )

    st.markdown("#### Informes KDD por ejecución (HTML / Markdown)")
    st.caption(
        "Cada DAG `simlog_kdd_*` escribe `informe_<fase>_<timestamp>.html` y `.md` bajo "
        "`reports/kdd/<run_id>/`. Documentación: `docs/KDD_INFORMES_Y_FASES.md`."
    )
    runs_inf = listar_run_ids_con_informes()
    if not runs_inf:
        st.info("Aún no hay carpetas con informes en `reports/kdd/` (salvo vacío o solo `work`).")
    else:
        run_sel = st.selectbox(
            "Run (carpeta Airflow / manual)",
            options=runs_inf,
            index=0,
            key="kdd_informes_run_id",
            help="Orden: más reciente primero.",
        )
        blob = informes_por_run_id(run_sel)
        if blob.get("ok"):
            st.caption(f"**Directorio:** `{blob['base_dir']}`")
            rows_link = []
            html_list = blob.get("html") or []
            if html_list and any(Path(x["ruta"]).is_file() for x in html_list):
                st.caption(
                    "Enlaces HTML: si el navegador bloquea `file://`, copia la ruta de la tabla o abre el fichero desde el IDE."
                )
            for i, h in enumerate(html_list):
                p = Path(h["ruta"])
                uri = p.as_uri() if p.is_file() else ""
                rows_link.append(
                    {
                        "tipo": "HTML",
                        "archivo": h["nombre"],
                        "ruta": h["ruta"],
                        "uri": uri,
                    }
                )
                if uri:
                    # `st.link_button(..., key=..., help=...)` no existe en Streamlit 1.28 (solo label + url).
                    st.link_button(f"Abrir en navegador · [{i + 1}] {h['nombre']}", uri)
            for j, m in enumerate(blob.get("markdown") or []):
                p = Path(m["ruta"])
                rows_link.append({"tipo": "Markdown", "archivo": m["nombre"], "ruta": m["ruta"], "uri": ""})
                try:
                    txt = p.read_text(encoding="utf-8")
                    st.download_button(
                        f"Descargar {m['nombre']}",
                        data=txt,
                        file_name=m["nombre"],
                        mime="text/markdown",
                        key=f"kdd_dl_md_{run_sel}_{j}",
                    )
                except OSError:
                    st.caption(f"No se pudo leer: `{m['ruta']}`")
            if rows_link:
                df_links = pd.DataFrame([{k: v for k, v in r.items() if k != "uri"} for r in rows_link])
                with st.expander("Tabla de rutas (copiar)", expanded=False):
                    st.dataframe(df_links, width="stretch", hide_index=True)
        else:
            st.warning(blob.get("error", "Error al listar informes."))

    with st.expander("Catálogo de fases KDD (criterios y artefactos work/)", expanded=False):
        st.dataframe(pd.DataFrame(catalogo_fases_kdd()), width="stretch", hide_index=True)

    st.markdown("#### Pipeline ejecutado desde este front (Streamlit)")
    st.info(
        "En la **barra lateral** (pestaña principal, no aquí) están los botones que lanzan el código real: "
        "**Ejecutar ingesta (fases 1–2 KDD)** → módulo de ingesta; "
        "**Ejecutar procesamiento Spark (fases 3–5 KDD)** → `procesamiento_grafos`; "
        "**Avanzar paso + ingesta + procesamiento** → ambos en secuencia. "
        "Cada finalización **OK** o **FAIL** puede dejar una fila en el registro JSON (ingesta, Spark y pipeline completo ya registran automáticamente)."
    )

    st.markdown("#### Tipos de prueba (qué cubre el proyecto)")
    df_tipos = pd.DataFrame(tipos_prueba_kdd_resumen())
    st.dataframe(df_tipos, width="stretch", hide_index=True)

    st.caption("Plan detallado y criterios: `docs/PLAN_PRUEBAS_KDD.md`.")

    st.markdown("#### Canales de ingesta (dónde se dispara fuera de esta web)")
    cols = st.columns(4)
    for col, canal in zip(cols, describir_canales_ingesta()):
        with col:
            with st.container(border=True):
                st.markdown(f"**{canal['canal']}**")
                st.caption(canal["ejecucion"])
                st.write(canal["detalle"])
                st.caption(f"Evidencia: {canal['evidencia']}")

    c_snap, c_reg = st.columns([1, 2])
    with c_snap:
        if st.button("Actualizar snapshot de pruebas", type="primary", key="btn_snapshot_pruebas"):
            st.session_state["snapshot_pruebas_ingesta"] = capturar_snapshot_pruebas()
    with c_reg:
        st.caption(
            f"Registro persistente: `{REGISTRO_PATH}` · "
            "El snapshot consulta HDFS, Cassandra, Hive, NiFi y último informe Airflow en disco."
        )

    snapshot = st.session_state.get("snapshot_pruebas_ingesta")
    if snapshot is None:
        snapshot = capturar_snapshot_pruebas()
        st.session_state["snapshot_pruebas_ingesta"] = snapshot

    st.markdown("#### Snapshot actual (datos en vivo)")
    ts_snap = snapshot.get("timestamp") or "—"
    st.caption(f"Generado: `{ts_snap}`")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("HDFS JSON (listados)", _valor(snapshot, "hdfs", "total_json") or 0)
    m2.metric("Eventos Cassandra", _valor(snapshot, "cassandra", "eventos_historico") or 0)
    m3.metric("Hive histórico nodos", _valor(snapshot, "hive", "historico_nodos") or "—")
    m4.metric("Hive transporte", _valor(snapshot, "hive", "transporte_ingesta_completa") or "—")

    c_ev1, c_ev2, c_ev3 = st.columns(3)
    with c_ev1:
        with st.container(border=True):
            st.markdown("**Última ingesta local (evidencia)**")
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
            st.markdown("**Último informe Airflow en disco**")
            air = snapshot.get("airflow", {})
            if air.get("disponible"):
                st.caption(f"Run: `{air.get('run_id', '—')}`")
                st.caption(f"Informe: `{air.get('archivo', '—')}`")
                st.caption(f"Modificado UTC: `{air.get('modificado', '—')}`")
            else:
                st.caption(air.get("mensaje", "Sin informes Airflow todavía."))
    with c_ev3:
        with st.container(border=True):
            st.markdown("**NiFi (comprobación)**")
            nifi = snapshot.get("nifi", {})
            st.caption(f"Activo: **{'sí' if nifi.get('activo') else 'no'}**")
            st.caption(nifi.get("detalle", "Sin detalle"))

    ae = snapshot.get("airflow_estado", {})
    if isinstance(ae, dict) and ae:
        with st.expander("Estado HTTP Airflow (opcional)", expanded=False):
            st.json(ae)

    st.markdown("#### Registrar una prueba manual")
    st.caption(
        "Usa esto para anotar pruebas hechas **fuera** del sidebar (NiFi, Airflow a mano, script), "
        "o para añadir observaciones."
    )
    with st.expander("Formulario de registro", expanded=False):
        canal = st.selectbox(
            "Canal probado",
            options=["NiFi", "Airflow", "Script / terminal", "Frontend Streamlit"],
            key="prueba_canal_sel",
        )
        ejecutor_default = {
            "NiFi": "PG_SIMLOG_KDD",
            "Airflow": "simlog_maestro / simlog_kdd_*",
            "Script / terminal": "venv_transporte/bin/python -m ingesta.ingesta_kdd",
            "Frontend Streamlit": "Sidebar Streamlit",
        }[canal]
        ejecutor = st.text_input("Medio / ejecutor", value=ejecutor_default, key="prueba_ejecutor_txt")
        resultado = st.selectbox("Resultado", options=["OK", "WARN", "FAIL"], key="prueba_resultado_sel")
        detalle = st.text_input(
            "Resumen del resultado",
            value="Evidencia revisada y contadores coherentes.",
            key="prueba_detalle_txt",
        )
        observaciones = st.text_area(
            "Observaciones",
            value="",
            placeholder="Ej.: DAG simlog_kdd_02 OK; eventos en Cassandra +3.",
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
            st.rerun()

    st.markdown("#### Histórico de pruebas registradas (tabla)")
    filas = resumen_tabular_pruebas()
    if filas:
        df_hist = pd.DataFrame(filas)
        ren = {
            "timestamp": "Fecha UTC",
            "canal": "Canal",
            "ejecutor": "Ejecutor",
            "resultado": "Resultado",
            "hdfs_json": "HDFS JSON",
            "delta_hdfs": "Δ HDFS",
            "eventos_cassandra": "Eventos C*",
            "delta_eventos": "Δ eventos",
            "historico_nodos_hive": "Hive hist. nodos",
            "delta_hist_hive": "Δ Hive nodos",
            "transporte_hive": "Hive transporte",
            "delta_transporte_hive": "Δ transporte",
            "detalle": "Detalle",
        }
        df_hist = df_hist.rename(columns={k: v for k, v in ren.items() if k in df_hist.columns})
        st.dataframe(df_hist, width="stretch", hide_index=True)
    else:
        st.info(
            "Todavía no hay pruebas en el registro. "
            "Ejecuta ingesta o Spark desde el **sidebar** o pulsa **Guardar** en el formulario anterior."
        )

    registro = leer_registro_pruebas()
    if registro:
        st.markdown("#### Detalle reciente (últimas 8)")
        for item in reversed(registro[-8:]):
            canal = item.get("canal", "")
            badge = "🖥️ " if canal == "Frontend Streamlit" else ""
            st.markdown(
                f"**{badge}{item.get('timestamp', '')}** · `{canal}` · **{item.get('resultado', '')}**"
            )
            st.caption(f"Ejecutor: `{item.get('ejecutor', '')}` · {item.get('detalle', '')}")
            if item.get("observaciones"):
                st.caption(f"Obs.: {item['observaciones'][:500]}{'…' if len(item.get('observaciones', '')) > 500 else ''}")
            deltas = item.get("deltas", {}) or {}
            st.caption(
                "Δ respecto al snapshot anterior — "
                f"HDFS: `{deltas.get('hdfs_json')}` · "
                f"eventos C*: `{deltas.get('cassandra_eventos')}` · "
                f"Hive hist. nodos: `{deltas.get('hive_historico_nodos')}` · "
                f"Hive transporte: `{deltas.get('hive_transporte')}`"
            )
            st.divider()
