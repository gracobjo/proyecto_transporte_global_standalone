#!/usr/bin/env python3
"""
Sistema de Gemelo Digital Logístico - Fase IV: Dashboard Interactivo
- Mapa España (Folium): nodos, aristas coloreados por estado, camiones en tiempo real
- Botón "Paso Siguiente (15 min)": ejecuta ingesta + procesamiento
- Leyenda: motivo retraso, Ruta Alternativa (línea azul)
- Métricas: PageRank nodos críticos
- st.session_state para línea de tiempo de simulación
"""
import os
import sys
import subprocess
from pathlib import Path

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

import streamlit as st
import folium
from folium import PolyLine, CircleMarker
from streamlit_folium import st_folium
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from config_nodos import RED, get_aristas, get_nodos
from config import CASSANDRA_HOST, KEYSPACE

# Colores por estado
COLORES_ESTADO = {"OK": "green", "Congestionado": "orange", "Bloqueado": "red"}
COLOR_RUTA_ALTERNATIVA = "blue"


def get_cassandra_session():
    """Conexión a Cassandra."""
    try:
        cluster = Cluster([CASSANDRA_HOST])
        return cluster.connect(KEYSPACE)
    except Exception as e:
        st.error(f"No se pudo conectar a Cassandra: {e}")
        return None


def cargar_nodos_estado(session):
    """Cargar estado actual de nodos desde Cassandra."""
    if not session:
        return {}
    try:
        rows = session.execute("SELECT id_nodo, lat, lon, estado, motivo_retraso, clima_actual FROM nodos_estado")
        return {r.id_nodo: {"lat": r.lat, "lon": r.lon, "estado": r.estado or "OK", "motivo": r.motivo_retraso, "clima": r.clima_actual} for r in rows}
    except Exception as e:
        st.warning(f"nodos_estado no disponible: {e}")
        return {}


def cargar_tracking_camiones(session):
    """Cargar posición GPS de camiones desde Cassandra."""
    if not session:
        return []
    try:
        rows = session.execute(
            "SELECT id_camion, lat, lon, ruta_origen, ruta_destino, ruta_sugerida, estado_ruta, motivo_retraso FROM tracking_camiones"
        )
        return [
            {
                "id": r.id_camion,
                "lat": r.lat,
                "lon": r.lon,
                "origen": r.ruta_origen,
                "destino": r.ruta_destino,
                "ruta_sugerida": list(r.ruta_sugerida) if r.ruta_sugerida else [],
                "estado": r.estado_ruta or "En ruta",
                "motivo": r.motivo_retraso,
            }
            for r in rows
        ]
    except Exception as e:
        st.warning(f"tracking_camiones no disponible: {e}")
        return []


def cargar_pagerank(session):
    """Cargar PageRank de nodos."""
    if not session:
        return {}
    try:
        rows = session.execute("SELECT id_nodo, pagerank FROM pagerank_nodos")
        return {r.id_nodo: r.pagerank for r in rows}
    except Exception:
        return {}


def cargar_aristas_estado(session):
    """Cargar estado de aristas desde Cassandra."""
    if not session:
        return {}
    try:
        rows = session.execute("SELECT src, dst, estado FROM aristas_estado")
        return {f"{r.src}|{r.dst}": {"estado": r.estado or "OK"} for r in rows}
    except Exception:
        return {}


def crear_mapa(nodos_estado, camiones, aristas_estado, pagerank):
    """Crear mapa Folium de España con nodos, aristas y camiones."""
    m = folium.Map(location=[40.4, -3.7], zoom_start=6, tiles="OpenStreetMap")

    nodos = get_nodos()
    aristas = get_aristas()

    # Dibujar aristas
    for src, dst, dist in aristas:
        est_src = (nodos_estado.get(src) or {}).get("estado", "OK")
        est_dst = (nodos_estado.get(dst) or {}).get("estado", "OK")
        key = f"{src}|{dst}"
        est_arista = (aristas_estado or {}).get(key, {}).get("estado", "OK")
        color = COLORES_ESTADO.get(est_arista, "gray")
        es_alternativa = False
        if src in nodos and dst in nodos:
            points = [[nodos[src]["lat"], nodos[src]["lon"]], [nodos[dst]["lat"], nodos[dst]["lon"]]]
            folium.PolyLine(points, color=color, weight=3, opacity=0.7, tooltip=f"{src}-{dst} ({est_arista})").add_to(m)

    # Nodos
    for nid, datos in nodos.items():
        est = (nodos_estado.get(nid) or {}).get("estado", "OK")
        motivo = (nodos_estado.get(nid) or {}).get("motivo", "")
        pr = pagerank.get(nid, 0)
        color = COLORES_ESTADO.get(est, "gray")
        CircleMarker(
            location=[datos["lat"], datos["lon"]],
            radius=10 if datos["tipo"] == "hub" else 6,
            color=color,
            fill=True,
            fill_opacity=0.8,
            popup=f"<b>{nid}</b><br>Estado: {est}<br>Motivo: {motivo or '-'}<br>PageRank: {round(pr,4)}",
        ).add_to(m)

    # Camiones
    for c in camiones:
        folium.Marker(
            [c["lat"], c["lon"]],
            icon=folium.Icon(color="blue", icon="info-sign"),
            popup=f"<b>{c['id']}</b><br>Ruta: {c.get('origen','')} → {c.get('destino','')}<br>Motivo retraso: {c.get('motivo','-') or '-'}",
        ).add_to(m)

        # Ruta alternativa (línea azul)
        ruta_sug = c.get("ruta_sugerida") or []
        if len(ruta_sug) >= 2:
            pts = [[nodos.get(n, {}).get("lat", 40), nodos.get(n, {}).get("lon", -3)] for n in ruta_sug if n in nodos]
            if len(pts) >= 2:
                folium.PolyLine(pts, color=COLOR_RUTA_ALTERNATIVA, weight=2, dash_array="5,5").add_to(m)

    return m


def ejecutar_paso_siguiente():
    """Ejecutar ingesta + procesamiento para avanzar 15 min."""
    paso = st.session_state.get("paso_15min", 0)
    env = os.environ.copy()
    env["PASO_15MIN"] = str(paso)

    with st.spinner("Ejecutando ingesta..."):
        r1 = subprocess.run(
            [sys.executable, str(BASE / "ingesta" / "ingesta_kdd.py")],
            env=env,
            capture_output=True,
            text=True,
            cwd=str(BASE),
        )
    if r1.returncode != 0:
        st.error(f"Ingesta: {r1.stderr or r1.stdout}")
        return False

    with st.spinner("Procesando grafos (Spark)..."):
        r2 = subprocess.run(
            [sys.executable, str(BASE / "procesamiento" / "procesamiento_grafos.py")],
            env=env,
            capture_output=True,
            text=True,
            cwd=str(BASE),
            timeout=120,
        )
    if r2.returncode != 0:
        st.error(f"Procesamiento: {r2.stderr or r2.stdout}")
        return False

    st.session_state["paso_15min"] = paso + 1
    st.success("Paso completado. Actualizando vista...")
    try:
        st.rerun()
    except Exception:
        st.experimental_rerun()
    return True


def main():
    st.set_page_config(page_title="Gemelo Digital Logístico España", layout="wide")
    st.title("Sistema de Gemelo Digital Logístico - España")

    if "paso_15min" not in st.session_state:
        st.session_state["paso_15min"] = 0

    session = get_cassandra_session()

    # Botón Paso Siguiente
    col1, col2, _ = st.columns([1, 1, 4])
    with col1:
        if st.button("Paso Siguiente (15 min)", use_container_width=True):
            ejecutar_paso_siguiente()
    with col2:
        st.metric("Paso simulación", st.session_state["paso_15min"])

    nodos_estado = cargar_nodos_estado(session)
    camiones = cargar_tracking_camiones(session)
    pagerank = cargar_pagerank(session)
    aristas_estado = cargar_aristas_estado(session)

    # Si no hay datos en Cassandra, usar datos por defecto del config
    if not nodos_estado:
        nodos = get_nodos()
        nodos_estado = {n: {"lat": d["lat"], "lon": d["lon"], "estado": "OK", "motivo": None, "clima": ""} for n, d in nodos.items()}
    if not camiones:
        nodos = get_nodos()
        hubs = list(nodos.keys())[:5]
        camiones = [
            {"id": f"camion_{i}", "lat": 40.4 + i * 0.2, "lon": -3.7 + i * 0.1, "origen": hubs[0], "destino": hubs[-1], "ruta_sugerida": hubs[:3], "estado": "En ruta", "motivo": None}
            for i in range(1, 6)
        ]

    m = crear_mapa(nodos_estado, camiones, aristas_estado, pagerank)
    st_folium(m, width=None, height=500)

    # Leyenda y análisis de negocio
    st.subheader("Leyenda")
    c1, c2, c3 = st.columns(3)
    c1.markdown("- **Verde**: OK")
    c2.markdown("- **Amarillo/Naranja**: Congestionado (Niebla/Tráfico/Lluvia)")
    c3.markdown("- **Rojo**: Bloqueado (Incendio/Nieve/Accidente)")
    st.markdown("- **Línea azul discontinua**: Ruta alternativa cuando el camino original está cortado")

    st.subheader("PageRank - Nodos más críticos")
    if pagerank:
        sorted_pr = sorted(pagerank.items(), key=lambda x: -x[1])[:10]
        st.dataframe([{"Nodo": n, "PageRank": round(v, 4)} for n, v in sorted_pr], use_container_width=True)
    else:
        st.info("Ejecuta al menos un paso para ver PageRank desde Cassandra.")


if __name__ == "__main__":
    main()
