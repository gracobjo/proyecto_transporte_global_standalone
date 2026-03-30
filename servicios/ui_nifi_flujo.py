from __future__ import annotations

import os
from typing import Any, Dict, List

import requests
import streamlit as st
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NIFI_API = os.environ.get("SIMLOG_NIFI_API", "https://localhost:8443/nifi-api").rstrip("/")
NIFI_USER = os.environ.get("SIMLOG_NIFI_USER", "nifi")
NIFI_PASSWORD = os.environ.get("SIMLOG_NIFI_PASSWORD", "nifinifinifi")
NIFI_GROUP_NAME = "PG_SIMLOG_KDD"

PROCESSORS: List[Dict[str, Any]] = [
    {
        "name": "Timer_Ingesta_15min",
        "type": "GenerateFlowFile",
        "responsibility": "Disparador temporal del snapshot de ingesta.",
        "inputs": ["Temporizador interno de NiFi"],
        "outputs": ["FlowFile vacío inicial por `success`"],
    },
    {
        "name": "Set_Parametros_Ingesta",
        "type": "UpdateAttribute",
        "responsibility": "Inyecta parámetros operativos en atributos.",
        "inputs": ["FlowFile disparado", "URL DATEX2 DGT"],
        "outputs": ["Atributos `dgt.url`, `filename`, `paso_15min`"],
    },
    {
        "name": "Build_GPS_Sintetico",
        "type": "ExecuteScript",
        "responsibility": "Genera el payload sintético base del KDD.",
        "inputs": ["Atributos de ingesta", "script `GenerateSyntheticGpsForPractice.groovy`"],
        "outputs": ["JSON base", "atributos `simlog.provenance.*` iniciales"],
    },
    {
        "name": "OpenMeteo_InvokeHTTP",
        "type": "InvokeHTTP",
        "responsibility": "Obtiene clima (Open-Meteo, multi-hub) para enriquecer el snapshot.",
        "inputs": ["Payload sintético", "URL Open-Meteo (sin API key)"],
        "outputs": ["Respuesta HTTP (JSON Open-Meteo) en `owm.response`", "estado HTTP y métricas `invokehttp.*`"],
    },
    {
        "name": "Merge_Weather_Into_Payload",
        "type": "ExecuteScript",
        "responsibility": "Fusiona clima con el payload base y actualiza provenance.",
        "inputs": ["Payload sintético", "atributo `owm.response` (cuerpo JSON Open-Meteo)"],
        "outputs": ["Payload enriquecido con clima", "`simlog.provenance.sources=simulacion,openmeteo` (u openweather si flujo legacy)"],
    },
    {
        "name": "DGT_DATEX2_InvokeHTTP",
        "type": "InvokeHTTP",
        "responsibility": "Descarga XML DATEX2 de la DGT.",
        "inputs": ["Payload enriquecido con clima", "atributo `dgt.url`"],
        "outputs": ["XML en `dgt.response.xml`", "estado HTTP `invokehttp.*`"],
    },
    {
        "name": "Merge_DGT_Into_Payload",
        "type": "ExecuteScript",
        "responsibility": "Prioriza incidencias DGT y construye el snapshot final operativo.",
        "inputs": ["Payload con clima", "atributo `dgt.response.xml`"],
        "outputs": [
            "Payload final con `incidencias_dgt` y `resumen_dgt`",
            "atributos finales de provenance DGT",
        ],
    },
    {
        "name": "Kafka_Publish_DGT_RAW",
        "type": "PublishKafka",
        "responsibility": "Publica evidencia del snapshot final en el topic de auditoría DGT.",
        "inputs": ["Payload final"],
        "outputs": ["Mensajes Kafka en `transporte_dgt_raw`"],
    },
    {
        "name": "Kafka_Publish_RAW",
        "type": "PublishKafka",
        "responsibility": "Publica el snapshot completo en el canal raw principal.",
        "inputs": ["Payload final"],
        "outputs": ["Mensajes Kafka en `transporte_raw`"],
    },
    {
        "name": "Kafka_Publish_FILTERED",
        "type": "PublishKafka",
        "responsibility": "Publica el snapshot enriquecido listo para consumidores downstream.",
        "inputs": ["Payload final"],
        "outputs": ["Mensajes Kafka en `transporte_filtered`"],
    },
    {
        "name": "HDFS_Backup_JSON",
        "type": "ExecuteStreamCommand",
        "responsibility": "Persistencia de respaldo del JSON final en HDFS.",
        "inputs": ["Payload final por STDIN"],
        "outputs": ["Escritura HDFS", "atributo `hdfs.command.output`"],
    },
    {
        "name": "Spark_Submit_Procesamiento",
        "type": "ExecuteProcess",
        "responsibility": "Procesador opcional para lanzar Spark desde NiFi.",
        "inputs": ["Disparo manual u otra integración futura"],
        "outputs": ["Proceso Spark batch"],
    },
    {
        "name": "Log_Fallos",
        "type": "LogAttribute",
        "responsibility": "Recoge fallos y deja trazabilidad de atributos y provenance.",
        "inputs": ["Relaciones `failure`/`Retry`/`No Retry`"],
        "outputs": ["Trazas de error en NiFi"],
    },
]

CONNECTIONS: List[Dict[str, str]] = [
    {"src": "Timer_Ingesta_15min", "dst": "Set_Parametros_Ingesta", "rel": "success"},
    {"src": "Set_Parametros_Ingesta", "dst": "Build_GPS_Sintetico", "rel": "success"},
    {"src": "Build_GPS_Sintetico", "dst": "OpenMeteo_InvokeHTTP", "rel": "success"},
    {"src": "OpenMeteo_InvokeHTTP", "dst": "Merge_Weather_Into_Payload", "rel": "Original"},
    {"src": "Merge_Weather_Into_Payload", "dst": "DGT_DATEX2_InvokeHTTP", "rel": "success"},
    {"src": "DGT_DATEX2_InvokeHTTP", "dst": "Merge_DGT_Into_Payload", "rel": "Original"},
    {"src": "Merge_DGT_Into_Payload", "dst": "Kafka_Publish_DGT_RAW", "rel": "success"},
    {"src": "Merge_DGT_Into_Payload", "dst": "Kafka_Publish_RAW", "rel": "success"},
    {"src": "Merge_DGT_Into_Payload", "dst": "Kafka_Publish_FILTERED", "rel": "success"},
    {"src": "Merge_DGT_Into_Payload", "dst": "HDFS_Backup_JSON", "rel": "success"},
]

ERROR_CONNECTIONS: List[Dict[str, str]] = [
    {"src": "Build_GPS_Sintetico", "dst": "Log_Fallos", "rel": "failure"},
    {"src": "OpenMeteo_InvokeHTTP", "dst": "Log_Fallos", "rel": "Failure/Retry/No Retry"},
    {"src": "Merge_Weather_Into_Payload", "dst": "Log_Fallos", "rel": "failure"},
    {"src": "DGT_DATEX2_InvokeHTTP", "dst": "Log_Fallos", "rel": "Failure/Retry/No Retry"},
    {"src": "Merge_DGT_Into_Payload", "dst": "Log_Fallos", "rel": "failure"},
    {"src": "Kafka_Publish_DGT_RAW", "dst": "Log_Fallos", "rel": "failure"},
    {"src": "Kafka_Publish_RAW", "dst": "Log_Fallos", "rel": "failure"},
    {"src": "Kafka_Publish_FILTERED", "dst": "Log_Fallos", "rel": "failure"},
]


def _fetch_nifi_runtime() -> Dict[str, Any]:
    session = requests.Session()
    session.verify = False
    token = session.post(
        f"{NIFI_API}/access/token",
        data={"username": NIFI_USER, "password": NIFI_PASSWORD},
        timeout=15,
    )
    token.raise_for_status()
    headers = {"Authorization": f"Bearer {token.text}"}

    root = session.get(f"{NIFI_API}/flow/process-groups/root", headers=headers, timeout=20).json()
    groups = root["processGroupFlow"]["flow"].get("processGroups", [])
    target = next((g for g in groups if g["component"]["name"] == NIFI_GROUP_NAME), None)
    if target is None:
        return {"ok": False, "error": f"No se encontró `{NIFI_GROUP_NAME}` en NiFi."}

    pg_id = target["component"]["id"]
    flow = session.get(f"{NIFI_API}/flow/process-groups/{pg_id}", headers=headers, timeout=20).json()
    pg_flow = flow["processGroupFlow"]["flow"]
    processors = {}
    total_queued = 0
    for p in pg_flow.get("processors", []):
        comp = p["component"]
        snap = p.get("status", {}).get("aggregateSnapshot", {})
        processors[comp["name"]] = {
            "state": comp.get("state", "UNKNOWN"),
            "validationErrors": comp.get("validationErrors") or [],
            "bulletins": p.get("bulletins", []),
            "flowFilesIn": snap.get("flowFilesIn"),
            "flowFilesOut": snap.get("flowFilesOut"),
            "tasks": snap.get("tasks"),
        }

    queues = []
    for c in pg_flow.get("connections", []):
        comp = c["component"]
        snap = c.get("status", {}).get("aggregateSnapshot", {})
        queued = snap.get("queuedCount", "0")
        try:
            total_queued += int(queued)
        except Exception:
            pass
        queues.append(
            {
                "src": comp["source"]["name"],
                "dst": comp["destination"]["name"],
                "rels": comp.get("selectedRelationships", []),
                "queued": queued,
                "queuedSize": snap.get("queuedSize", "0 bytes"),
            }
        )

    running = sum(1 for meta in processors.values() if meta["state"] == "RUNNING")
    invalid = sum(1 for meta in processors.values() if meta["validationErrors"])
    return {
        "ok": True,
        "group_name": NIFI_GROUP_NAME,
        "processors": processors,
        "queues": queues,
        "running": running,
        "invalid": invalid,
        "total_processors": len(processors),
        "total_queued": total_queued,
    }


def _node_color(runtime: Dict[str, Any], name: str) -> str:
    if not runtime.get("ok"):
        return "#D8E2F1"
    proc = runtime.get("processors", {}).get(name)
    if not proc:
        return "#E5E7EB"
    if proc["validationErrors"]:
        return "#FCA5A5"
    if proc["state"] == "RUNNING":
        return "#86EFAC"
    if proc["state"] == "STOPPED":
        return "#BFDBFE"
    return "#E5E7EB"


def _build_dot(runtime: Dict[str, Any]) -> str:
    lines = [
        "digraph nifi_flow {",
        "rankdir=LR;",
        'graph [pad="0.2", nodesep="0.6", ranksep="1.0"];',
        'node [shape=box, style="rounded,filled", color="#4B5563", fontname="Helvetica"];',
        'edge [fontname="Helvetica", color="#6B7280"];',
    ]

    for proc in PROCESSORS:
        state = ""
        if runtime.get("ok"):
            meta = runtime.get("processors", {}).get(proc["name"], {})
            state = meta.get("state", "")
        label = f"{proc['name']}\\n{proc['type']}"
        if state:
            label += f"\\n[{state}]"
        lines.append(f'"{proc["name"]}" [label="{label}", fillcolor="{_node_color(runtime, proc["name"])}"];')

    for conn in CONNECTIONS:
        lines.append(f'"{conn["src"]}" -> "{conn["dst"]}" [label="{conn["rel"]}"];')
    for conn in ERROR_CONNECTIONS:
        lines.append(
            f'"{conn["src"]}" -> "{conn["dst"]}" [label="{conn["rel"]}", color="#DC2626", fontcolor="#DC2626", style="dashed"];'
        )
    lines.append("}")
    return "\n".join(lines)


def _render_processor_card(proc: Dict[str, Any], runtime: Dict[str, Any]) -> None:
    meta = runtime.get("processors", {}).get(proc["name"], {}) if runtime.get("ok") else {}
    with st.container(border=True):
        c1, c2 = st.columns([3, 1])
        with c1:
            st.markdown(f"### {proc['name']}")
            st.caption(f"`{proc['type']}`")
        with c2:
            st.metric("Estado", meta.get("state", "N/D"))

        st.write(proc["responsibility"])

        ci, co = st.columns(2)
        with ci:
            st.markdown("**Entradas**")
            for item in proc["inputs"]:
                st.markdown(f"- {item}")
        with co:
            st.markdown("**Salidas**")
            for item in proc["outputs"]:
                st.markdown(f"- {item}")

        if meta.get("validationErrors"):
            st.warning("Errores de validación detectados en NiFi.")
            for err in meta["validationErrors"][:3]:
                st.caption(err)

        if meta.get("bulletins"):
            with st.expander("Últimos bulletins", expanded=False):
                for bulletin in meta["bulletins"][:3]:
                    msg = bulletin.get("bulletin", {}).get("message", "")
                    if msg:
                        st.caption(msg)


def render_nifi_flujo_tab() -> None:
    st.subheader("Flujo NiFi y responsabilidades")
    st.caption(
        "Vista funcional del `PG_SIMLOG_KDD`: qué hace cada procesador, qué recibe, qué entrega y el estado actual si NiFi está disponible."
    )

    runtime: Dict[str, Any]
    try:
        runtime = _fetch_nifi_runtime()
    except Exception as exc:
        runtime = {"ok": False, "error": f"No se pudo consultar NiFi: {exc}"}

    if runtime.get("ok"):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Procesadores", str(runtime.get("total_processors", 0)))
        c2.metric("En ejecución", str(runtime.get("running", 0)))
        c3.metric("Inválidos", str(runtime.get("invalid", 0)))
        c4.metric("Colas pendientes", str(runtime.get("total_queued", 0)))
        if runtime.get("invalid", 0) > 0:
            st.warning("NiFi responde, pero hay procesadores con errores de validación.")
    else:
        st.info(runtime.get("error", "NiFi no está accesible ahora mismo."))

    st.markdown("**Diagrama del flujo operativo**")
    st.graphviz_chart(_build_dot(runtime), width="stretch")

    if runtime.get("ok"):
        with st.expander("Relaciones y colas actuales", expanded=False):
            for q in runtime.get("queues", []):
                st.caption(
                    f"`{q['src']}` -> `{q['dst']}` · rel `{', '.join(q['rels'])}` · cola `{q['queued']}` · tamaño `{q['queuedSize']}`"
                )

    st.markdown("**Procesadores del grupo**")
    cols = st.columns(2)
    for idx, proc in enumerate(PROCESSORS):
        with cols[idx % 2]:
            _render_processor_card(proc, runtime)
