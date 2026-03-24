"""Formulario Streamlit: red híbrida, ruta origen→destino, retrasos y alternativas."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from config import COSTE_EURO_MINUTO_RETASO
from servicios.red_hibrida_rutas import analizar_ruta_completa, listar_nodos_ui, nodo_a_hub


def render_rutas_hibridas_tab() -> None:
    st.subheader("Red híbrida: rutas, retrasos y alternativas")
    st.markdown(
        "Define **origen** y **destino** (hub o ciudad secundaria). El camino mínimo se calcula en "
        "**número de saltos** (mismo peso entre nodos adyacentes). Se cruzan datos de **Cassandra** "
        "(nodos, aristas, tracking) con **OpenWeather** (hubs) para retrasos, motivos, coste y vehículos afectados."
    )

    nodos = listar_nodos_ui()
    c1, c2 = st.columns(2)
    with c1:
        origen = st.selectbox("Origen", nodos, index=nodos.index("Madrid") if "Madrid" in nodos else 0, key="rh_origen")
    with c2:
        destino = st.selectbox("Destino", nodos, index=nodos.index("Barcelona") if "Barcelona" in nodos else 0, key="rh_destino")

    st.markdown("##### Escenarios (clima OpenWeather / obras)")
    cc1, cc2, cc3 = st.columns(3)
    with cc1:
        usar_clima = st.checkbox("Usar API clima en retrasos (OWM)", value=True, key="rh_clima_ret")
    with cc2:
        clima_bloquea = st.checkbox(
            "Clima severo bloquea nodos del hub (tormenta, nieve, granizo…)",
            value=False,
            key="rh_clima_blk",
        )
    with cc3:
        modo_obras = st.checkbox("Bloquear nodos con ‘obras’ en Cassandra", value=False, key="rh_obras")

    if st.button("Calcular ruta y análisis", type="primary", use_container_width=True):
        with st.spinner("Consultando Cassandra y OpenWeather…"):
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
            return

        st.success(
            f"**Ruta principal** ({res['num_saltos']} saltos): `{' → '.join(res['ruta'])}`"
        )
        st.caption(
            f"Coste configurado: **{res.get('coste_eur_minuto_config', COSTE_EURO_MINUTO_RETASO)} €/min** de retraso estimado."
        )

        if res.get("log_escenario"):
            with st.expander("Escenario (clima / obras)", expanded=False):
                for line in res["log_escenario"]:
                    st.markdown(f"- {line}")

        pasos = res.get("pasos") or []
        if pasos:
            df = pd.DataFrame(
                [
                    {
                        "Paso": p["paso"],
                        "Tramo": f"{p['desde']} → {p['hasta']}",
                        "Saltos acum.": p["saltos_acum"],
                        "Min. retraso est.": p["minutos"] if p["minutos"] < 1000 else "∞ (bloqueo)",
                        "Coste € (tramo)": p.get("coste_eur") if p["minutos"] < 1000 else "—",
                        "Motivos": "; ".join(p.get("motivos") or []),
                    }
                    for p in pasos
                ]
            )
            st.dataframe(df, use_container_width=True, hide_index=True)
            if any((p.get("minutos") or 0) >= 1000 for p in pasos):
                st.error(
                    "Hay al menos un **tramo bloqueado** (estimación de retraso infinito). "
                    "Revisa alternativas o el estado en Cassandra."
                )

        ctot = res.get("minutos_totales_estimados")
        eur = res.get("coste_total_eur")
        m1, m2 = st.columns(2)
        with m1:
            st.metric("Minutos totales estimados (suma tramos)", f"{ctot}")
        with m2:
            st.metric("Coste total estimado (€)", f"{eur}" if eur is not None else "—")

        ve = res.get("vehiculos_afectados") or []
        st.markdown("##### Vehículos afectados (cruce con tracking)")
        if ve:
            st.dataframe(pd.DataFrame(ve), use_container_width=True, hide_index=True)
        else:
            st.caption("Ningún camión con ruta que intersecte los nodos de esta ruta (o sin datos en `tracking_camiones`).")

        alts = res.get("alternativas") or []
        st.markdown("##### Rutas alternativas (menor número de saltos primero)")
        st.caption(
            "Se simula la caída de cada **arista** o **nodo intermedio** de la ruta principal; "
            "peso uniforme entre nodos (BFS)."
        )
        if alts:
            for i, (ruta_alt, motivo) in enumerate(alts, 1):
                saltos = len(ruta_alt) - 1
                st.markdown(f"**{i}.** {motivo} — **{saltos} saltos:** `{' → '.join(ruta_alt)}`")
        else:
            st.caption("No hay rutas alternativas distintas con los mismos bloqueos de escenario.")

    st.divider()
    st.caption(
        f"Tipo de nodo: origen **{nodo_a_hub(origen)}** / destino **{nodo_a_hub(destino)}** (hub de referencia para clima)."
    )
