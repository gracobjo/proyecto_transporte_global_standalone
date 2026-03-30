"""Formulario Streamlit: catálogo de red, origen→destino, pasos, mapa y alternativas."""
from __future__ import annotations

import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

from config import COSTE_EURO_MINUTO_RETASO
from servicios.carga_rutas_red import listar_conexiones_catalogo, resumen_conexiones_por_tipo
from servicios.mapa_rutas_hibridas import crear_mapa_planificacion_rutas
from servicios.red_hibrida_rutas import analizar_ruta_completa, listar_nodos_ui, nodo_a_hub


def render_rutas_hibridas_tab() -> None:
    st.subheader("Planificación de transporte en red híbrida")
    st.markdown(
        "**Catálogo de conexiones** (fichero `datos/rutas_red_simlog.yaml`): malla entre **hubs**, "
        "enlaces **hub ↔ ciudad satélite** y refuerzos **subnodo ↔ subnodo**. "
        "Elige **origen** y **destino**; el sistema calcula el camino con **menor número de saltos** (BFS) "
        "y, si aplicas incidencias (clima, obras), **rutas alternativas** en otro color en el mapa."
    )

    # --- Catálogo completo de conexiones ---
    filas, msg_origen = listar_conexiones_catalogo()
    resumen = resumen_conexiones_por_tipo(filas)

    with st.expander("**Catálogo de conexiones de la red** (hubs ↔ hubs, hub ↔ subnodo, subnodo ↔ subnodo)", expanded=True):
        st.caption(msg_origen)
        csum1, csum2, csum3 = st.columns(3)
        csum1.metric("Hub ↔ Hub", resumen.get("malla_hubs", 0))
        csum2.metric("Hub ↔ Subnodo", resumen.get("hub_secundario", 0))
        csum3.metric("Subnodo ↔ Subnodo", resumen.get("secundario_secundario", 0))

        tipos = ["Todas", "malla_hubs", "hub_secundario", "secundario_secundario"]
        etiquetas = {
            "Todas": "Todas",
            "malla_hubs": "Solo hub ↔ hub",
            "hub_secundario": "Solo hub ↔ subnodo",
            "secundario_secundario": "Solo subnodo ↔ subnodo",
        }
        filtro = st.selectbox("Filtrar por tipo de conexión", tipos, format_func=lambda x: etiquetas[x], key="rh_filtro_tipo")

        df = pd.DataFrame(filas)
        if filtro != "Todas" and not df.empty:
            df = df[df["tipo_codigo"] == filtro]

        if not df.empty:
            df_show = df[["origen", "destino", "tipo_etiqueta", "distancia_km"]].copy()
            df_show.columns = ["Origen", "Destino", "Tipo de conexión", "Distancia (km)"]
            st.dataframe(df_show, width="stretch", hide_index=True)
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Descargar tramos (CSV)",
                data=csv,
                file_name="simlog_conexiones_red.csv",
                mime="text/csv",
                key="rh_dl_csv",
            )
        else:
            st.warning("No hay filas para mostrar con este filtro.")

    st.divider()

    # --- Formulario origen / destino ---
    nodos = listar_nodos_ui()
    st.markdown("##### Transporte: origen → destino")
    c1, c2 = st.columns(2)
    with c1:
        origen = st.selectbox(
            "Origen",
            nodos,
            index=nodos.index("Madrid") if "Madrid" in nodos else 0,
            key="rh_origen",
            help="Ciudad hub o satélite donde inicia el transporte.",
        )
    with c2:
        destino = st.selectbox(
            "Destino",
            nodos,
            index=nodos.index("Barcelona") if "Barcelona" in nodos else 0,
            key="rh_destino",
            help="Ciudad hub o satélite de llegada.",
        )

    st.markdown("##### Incidencias (red híbrida: nodos o tramos no disponibles)")
    cc1, cc2, cc3, cc4 = st.columns(4)
    with cc1:
        usar_clima = st.checkbox("Usar API clima en retrasos (Open-Meteo / heurísticas)", value=True, key="rh_clima_ret")
    with cc2:
        clima_bloquea = st.checkbox(
            "Clima severo bloquea nodos del hub (tormenta, nieve, granizo…)",
            value=False,
            key="rh_clima_blk",
        )
    with cc3:
        modo_obras = st.checkbox("Bloquear nodos con ‘obras’ en Cassandra", value=False, key="rh_obras")
    with cc4:
        mostrar_red = st.checkbox(
            "En el mapa: mostrar toda la red de fondo (malla; puede verse «telaraña»)",
            value=False,
            key="rh_mapa_red",
        )

    colb1, colb2 = st.columns([1, 2])
    with colb1:
        calcular = st.button("Calcular ruta y mostrar en mapa", type="primary", width="stretch")
    with colb2:
        st.caption(
            "Tras **Calcular**, verás los **pasos** (saltos), tiempos estimados si hay datos en Cassandra, "
            "y **rutas alternativas** si un tramo o nodo intermedio queda fuera de servicio."
        )

    if calcular:
        with st.spinner("Consultando Cassandra y API de clima…"):
            res = analizar_ruta_completa(
                origen,
                destino,
                aplicar_clima_api=usar_clima,
                aplicar_clima_bloqueo=clima_bloquea,
                aplicar_obras=modo_obras,
            )
        st.session_state["rh_resultado"] = res

    if st.session_state.get("rh_resultado"):
        res = st.session_state["rh_resultado"]
        if not res.get("ok"):
            st.error(res.get("error", "Error desconocido"))
            for line in res.get("log_escenario", []) or []:
                st.warning(line)
        else:
            ruta = res["ruta"]
            saltos = res["num_saltos"]
            st.success(
                f"**Ruta principal** — **{saltos}** salto(s) en la red: "
                f"`{' → '.join(ruta)}`"
            )
            st.caption(
                f"Coste de referencia: **{res.get('coste_eur_minuto_config', COSTE_EURO_MINUTO_RETASO)} €/min** "
                "de retraso estimado (según configuración)."
            )

            # Pasos numerados (lo que pedía la fase de presentación)
            st.markdown("##### Pasos hasta el destino")
            pasos = res.get("pasos") or []
            hay_bloqueo_inf = any((p.get("minutos") or 0) >= 1000 for p in pasos)
            hay_incidencia_grave = any(p.get("incidencia_grave") for p in pasos)
            if pasos:
                for p in pasos:
                    mins = p["minutos"]
                    mins_txt = "∞ (bloqueo)" if mins >= 1000 else f"{mins:.0f} min"
                    st.markdown(
                        f"{p['paso']}. **{p['desde']}** → **{p['hasta']}** · "
                        f"Retraso est.: {mins_txt} · "
                        f"{'; '.join(p.get('motivos') or ['—'])}"
                    )
            else:
                st.caption("Sin desglose de tramos (mismo nodo origen y destino).")

            if res.get("log_escenario"):
                with st.expander("Detalle del escenario (clima / obras)", expanded=False):
                    for line in res["log_escenario"]:
                        st.markdown(f"- {line}")

            if pasos:
                df = pd.DataFrame(
                    [
                        {
                            "Paso": p["paso"],
                            "Tramo": f"{p['desde']} → {p['hasta']}",
                            "Saltos acum.": p["saltos_acum"],
                            "Min. retraso est.": p["minutos"] if p["minutos"] < 1000 else "∞ (bloqueo)",
                            "Coste € (tramo)": (
                                f"{p['coste_eur']:.2f}" if p.get("coste_eur") is not None else "—"
                            ),
                            "Motivos": "; ".join(p.get("motivos") or []),
                        }
                        for p in pasos
                    ]
                )
                st.dataframe(df, width="stretch", hide_index=True)
                if hay_bloqueo_inf:
                    st.error(
                        "Hay al menos un **tramo bloqueado** (retraso infinito). "
                        "Revisa alternativas o el estado en Cassandra."
                    )
                elif hay_incidencia_grave:
                    st.warning(
                        "Hay **incidencia grave** (p. ej. bloqueo/incendio en Cassandra). "
                        "Los minutos y € por tramo son una **penalización acotada** para planificación; "
                        "el BFS ya encontró la ruta (no es cierre de malla)."
                    )

            ctot = res.get("minutos_totales_estimados")
            eur = res.get("coste_total_eur")
            m1, m2 = st.columns(2)
            with m1:
                if hay_bloqueo_inf:
                    st.metric(
                        "Minutos totales estimados (suma tramos)",
                        "—",
                        help="Si hay tramo bloqueado, el backend no suma minutos (no es acumulable).",
                    )
                else:
                    st.metric("Minutos totales estimados (suma tramos)", f"{ctot}")
            with m2:
                if hay_bloqueo_inf:
                    st.metric(
                        "Coste total estimado (€)",
                        "—",
                        help="Sin minutos finitos por tramo no se calcula € total.",
                    )
                else:
                    st.metric("Coste total estimado (€)", f"{eur}" if eur is not None else "—")

            ve = res.get("vehiculos_afectados") or []
            st.markdown("##### Vehículos afectados (origen→destino seleccionado)")
            if ve:
                st.dataframe(pd.DataFrame(ve), width="stretch", hide_index=True)
            else:
                st.caption("Ningún camión del cruce origen→destino elegido con datos en `tracking_camiones` que se pueda asociar a esta ruta.")

            alts = res.get("alternativas") or []
            st.markdown("##### Rutas alternativas (ante caída de tramo o nodo intermedio)")
            st.caption(
                "Se simulan **cortes** en cada arista o **nodo intermedio** de la ruta principal; "
                "se listan otros caminos distintos (ordenados por menor número de saltos). "
                "En el mapa se dibujan en **naranja** (discontinuo), frente a la principal en **azul**."
            )
            if alts:
                for i, (ruta_alt, motivo) in enumerate(alts, 1):
                    ns = len(ruta_alt) - 1
                    st.markdown(f"**{i}.** ({ns} saltos) `{ ' → '.join(ruta_alt) }` — _{motivo}_")
            else:
                st.caption("No hay rutas alternativas distintas con los mismos bloqueos de escenario.")

            # Mapa
            st.markdown("##### Mapa: red, ruta principal (azul) y alternativas (naranja)")
            st.caption(
                "Vista **sin telaraña** por defecto: fondo claro y **malla completa desactivada** "
                "(actívala en el control de capas del mapa o con la casilla superior). "
                "El zoom se centra en la ruta calculada."
            )
            mapa = crear_mapa_planificacion_rutas(
                ruta,
                alts,
                mostrar_red_completa=mostrar_red,
                max_alternativas=6,
            )
            st_folium(mapa, width=None, height=480, returned_objects=[], key="rh_folium_map")

    st.divider()
    st.caption(
        f"Referencia de hubs para clima: origen **{nodo_a_hub(origen)}** · destino **{nodo_a_hub(destino)}**."
    )
