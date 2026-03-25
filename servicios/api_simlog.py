#!/usr/bin/env python3
"""
API REST SIMLOG España — FastAPI + OpenAPI (Swagger UI / ReDoc).

Documentación interactiva:
  - Swagger UI:  GET /docs
  - ReDoc:       GET /redoc
  - Esquema:     GET /openapi.json

Arranque:
  cd ~/proyecto_transporte_global
  uvicorn servicios.api_simlog:app --host 0.0.0.0 --port 8090
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

from pydantic import BaseModel, Field

# Raíz del proyecto en PYTHONPATH
_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import (
    HDFS_BACKUP_PATH,
    KEYSPACE,
    PROJECT_DESCRIPTION,
    PROJECT_DISPLAY_NAME,
    PROJECT_SLUG,
    PROJECT_TAGLINE,
    TOPIC_TRANSPORTE,
)
from config_nodos import get_aristas, get_nodos
from ingesta.ingesta_kdd import consulta_clima_hubs

from servicios.estado_y_datos import (
    cargar_aristas_cassandra,
    cargar_nodos_cassandra,
    cargar_pagerank_cassandra,
    cargar_tracking_cassandra,
    estado_servicios,
    verificacion_tecnica_completa,
)
from servicios.kdd_fases import FASES_KDD, FaseKDD


# --- Modelos OpenAPI ---


class ProyectoInfo(BaseModel):
    nombre: str
    slug: str
    tagline: str
    descripcion: str
    keyspace_cassandra: str
    topic_kafka_principal: str
    hdfs_backup_path: str


class ServicioEstadoItem(BaseModel):
    nombre: str = Field(..., description="Nombre del servicio (HDFS, Kafka, Cassandra)")
    activo: bool
    detalle: str = Field(..., description="Texto legible con icono de estado")


class ServicioEstadoResponse(BaseModel):
    servicios: List[ServicioEstadoItem]
    timestamp_utc: str


class VerificacionResponse(BaseModel):
    hdfs_backup: str
    kafka_topic: str
    cassandra_nodos: str
    cassandra_tracking: str


class FaseKDDOut(BaseModel):
    orden: int
    codigo: str
    titulo: str
    resumen: str
    actividades: List[str]
    datos_entrada: List[str]
    datos_salida: List[str]
    stack: List[str]
    script: str


def _fase_to_schema(f: FaseKDD) -> FaseKDDOut:
    return FaseKDDOut(
        orden=f.orden,
        codigo=f.codigo,
        titulo=f.titulo,
        resumen=f.resumen,
        actividades=list(f.actividades),
        datos_entrada=list(f.datos_entrada),
        datos_salida=list(f.datos_salida),
        stack=list(f.stack),
        script=f.script,
    )


# --- App ---

app = FastAPI(
    title=f"{PROJECT_DISPLAY_NAME} — API",
    description=(
        f"{PROJECT_DESCRIPTION}\n\n"
        "Endpoints de **solo lectura** para integración con otras aplicaciones: "
        "topología, estado de servicios, datos en Cassandra (tras procesamiento Spark) "
        "y clima por hubs (OpenWeather)."
    ),
    version="1.0.0",
    openapi_tags=[
        {"name": "sistema", "description": "Salud y metadatos del proyecto"},
        {"name": "infraestructura", "description": "HDFS, Kafka, Cassandra (puertos y comprobaciones)"},
        {"name": "topologia", "description": "Red estática (config_nodos)"},
        {"name": "datos", "description": "Lecturas desde Cassandra (operativo)"},
        {"name": "clima", "description": "Consulta en tiempo real a OpenWeather (hubs)"},
        {"name": "kdd", "description": "Fases del ciclo KDD documentadas en el proyecto"},
    ],
    contact={
        "name": PROJECT_DISPLAY_NAME,
    },
    license_info={"name": "Uso interno / proyecto académico", "identifier": "Proprietary"},
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("SIMLOG_CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", tags=["sistema"], summary="Comprobación de vida (liveness)")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": PROJECT_SLUG}


@app.get(
    "/api/v1/info",
    response_model=ProyectoInfo,
    tags=["sistema"],
    summary="Metadatos del proyecto SIMLOG",
)
def api_info() -> ProyectoInfo:
    return ProyectoInfo(
        nombre=PROJECT_DISPLAY_NAME,
        slug=PROJECT_SLUG,
        tagline=PROJECT_TAGLINE,
        descripcion=PROJECT_DESCRIPTION,
        keyspace_cassandra=KEYSPACE,
        topic_kafka_principal=TOPIC_TRANSPORTE,
        hdfs_backup_path=HDFS_BACKUP_PATH,
    )


@app.get(
    "/api/v1/servicios/estado",
    response_model=ServicioEstadoResponse,
    tags=["infraestructura"],
    summary="Estado del stack completo (HDFS, Kafka, Cassandra, Spark, Hive, Airflow, NiFi)",
)
def api_estado_servicios() -> ServicioEstadoResponse:
    raw = estado_servicios()
    items: List[ServicioEstadoItem] = []
    for nombre, detalle in raw.items():
        activo = "Activo" in detalle or "✅" in detalle
        items.append(ServicioEstadoItem(nombre=nombre, activo=activo, detalle=detalle))
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return ServicioEstadoResponse(servicios=items, timestamp_utc=ts)


@app.get(
    "/api/v1/servicios/verificacion",
    response_model=VerificacionResponse,
    tags=["infraestructura"],
    summary="Comprobaciones técnicas (HDFS ls, Kafka topics, muestras CQL)",
)
def api_verificacion() -> VerificacionResponse:
    v = verificacion_tecnica_completa()
    return VerificacionResponse(**v)


@app.get(
    "/api/v1/topologia/nodos",
    tags=["topologia"],
    summary="Nodos de la red (hubs + secundarios)",
    response_model=Dict[str, Dict[str, Any]],
)
def api_topologia_nodos() -> Dict[str, Dict[str, Any]]:
    return get_nodos()


@app.get(
    "/api/v1/topologia/aristas",
    tags=["topologia"],
    summary="Aristas (origen, destino, distancia km)",
    response_model=List[Dict[str, Any]],
)
def api_topologia_aristas() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for src, dst, dist in get_aristas():
        out.append({"src": src, "dst": dst, "distancia_km": dist})
    return out


@app.get(
    "/api/v1/datos/nodos",
    tags=["datos"],
    summary="Estado de nodos en Cassandra (tabla nodos_estado)",
)
def api_datos_nodos() -> List[Dict[str, Any]]:
    return cargar_nodos_cassandra()


@app.get(
    "/api/v1/datos/aristas",
    tags=["datos"],
    summary="Estado de aristas en Cassandra (tabla aristas_estado)",
)
def api_datos_aristas() -> List[Dict[str, Any]]:
    return cargar_aristas_cassandra()


@app.get(
    "/api/v1/datos/tracking",
    tags=["datos"],
    summary="Tracking de camiones (tabla tracking_camiones)",
)
def api_datos_tracking() -> List[Dict[str, Any]]:
    return cargar_tracking_cassandra()


@app.get(
    "/api/v1/datos/pagerank",
    tags=["datos"],
    summary="PageRank por nodo (tabla pagerank_nodos)",
)
def api_datos_pagerank() -> List[Dict[str, Any]]:
    return cargar_pagerank_cassandra()


@app.get(
    "/api/v1/clima/hubs",
    tags=["clima"],
    summary="Clima actual por hub (OpenWeather)",
    response_model=Dict[str, Dict[str, Any]],
)
def api_clima_hubs() -> Dict[str, Dict[str, Any]]:
    return consulta_clima_hubs()


@app.get(
    "/api/v1/kdd/fases",
    tags=["kdd"],
    summary="Fases KDD del proyecto (documentación estructurada)",
    response_model=List[FaseKDDOut],
)
def api_kdd_fases() -> List[FaseKDDOut]:
    return [_fase_to_schema(f) for f in FASES_KDD]
