"""
UI Streamlit para consumir el microservicio FAQ IA.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
import streamlit as st


def _faq_base_url() -> str:
    port = int(os.environ.get("SIMLOG_PORT_FAQ_IA", "8091"))
    return os.environ.get("SIMLOG_FAQ_IA_URL", f"http://127.0.0.1:{port}").rstrip("/")


def _faq_health(base_url: str, timeout: float = 2.0) -> bool:
    try:
        r = requests.get(f"{base_url}/health", timeout=timeout)
        return r.status_code == 200
    except requests.RequestException:
        return False


def _faq_ask(base_url: str, question: str, top_k: int = 3, timeout: float = 12.0) -> Dict[str, Any]:
    payload = {"question": question, "top_k": top_k}
    r = requests.post(f"{base_url}/api/v1/faq/ask", json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()


def render_faq_ia_panel() -> None:
    st.subheader("FAQ IA")
    st.caption("Preguntas frecuentes con recuperación semántica para resolver dudas de operación y uso.")

    base_url = _faq_base_url()
    ok = _faq_health(base_url)
    if ok:
        st.success(f"FAQ IA API disponible en `{base_url}`")
    else:
        st.warning(
            f"No se detecta FAQ IA API en `{base_url}`. "
            "Puedes arrancarla desde esta pestaña (servicio `faq_ia`) o con uvicorn."
        )

    q = st.text_input(
        "Pregunta",
        value="",
        placeholder="Ej.: ¿cómo genero un informe PDF? ¿por qué NiFi no aparece activo?",
        key="faq_ia_q",
    ).strip()
    top_k = st.slider("Sugerencias", min_value=1, max_value=6, value=3, key="faq_ia_topk")
    if "faq_ia_historial" not in st.session_state:
        st.session_state["faq_ia_historial"] = []
    hist = st.session_state.get("faq_ia_historial", [])

    c_hist1, c_hist2 = st.columns([1, 1])
    with c_hist1:
        st.caption(f"Interacciones en sesión: {len(hist)}")
    with c_hist2:
        if st.button("Limpiar historial", key="faq_ia_clear_hist"):
            st.session_state["faq_ia_historial"] = []
            st.rerun()

    if st.button("Preguntar al FAQ IA", key="faq_ia_ask_btn", type="primary"):
        if not q:
            st.warning("Escribe una pregunta.")
            return
        if not ok:
            st.error("El microservicio FAQ IA no está disponible.")
            return
        with st.spinner("Consultando FAQ IA…"):
            try:
                ans = _faq_ask(base_url, q, top_k=top_k)
            except requests.RequestException as e:
                st.error(f"Error consultando FAQ IA: {e}")
                return
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        hist.append(
            {
                "ts": ts,
                "q": q,
                "a": ans.get("answer", "Sin respuesta."),
                "confidence": float(ans.get("confidence", 0.0)),
                "matched": str(ans.get("matched_question", "")),
            }
        )
        st.session_state["faq_ia_historial"] = hist[-20:]
        st.markdown("**Respuesta**")
        st.write(ans.get("answer", "Sin respuesta."))
        c1, c2 = st.columns([1, 2])
        with c1:
            st.metric("Confianza", f"{float(ans.get('confidence', 0.0)):.2f}")
        with c2:
            st.caption(f"Coincidencia principal: `{ans.get('matched_question', '')}`")

        sugg: List[str] = [str(x) for x in (ans.get("suggestions") or []) if str(x).strip()]
        if sugg:
            st.markdown("**Sugerencias relacionadas**")
            for s in sugg:
                st.markdown(f"- {s}")

        src: List[str] = [str(x) for x in (ans.get("sources") or []) if str(x).strip()]
        if src:
            with st.expander("Fuentes", expanded=False):
                for s in src:
                    st.code(s, language="text")

    hist = st.session_state.get("faq_ia_historial", [])
    if hist:
        with st.expander("Historial de la sesión", expanded=False):
            for idx, item in enumerate(reversed(hist)):
                st.markdown(f"**[{item.get('ts','')}] Pregunta**")
                st.write(item.get("q", ""))
                c1, c2 = st.columns([1, 3])
                with c1:
                    if st.button("Reutilizar pregunta", key=f"faq_reuse_{idx}"):
                        st.session_state["faq_ia_q"] = str(item.get("q", ""))
                        st.rerun()
                with c2:
                    st.caption(
                        f"Confianza: {float(item.get('confidence', 0.0)):.2f} · "
                        f"Coincidencia: {item.get('matched','')}"
                    )
                st.markdown("**Respuesta**")
                st.write(item.get("a", ""))
                st.markdown("---")
