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
from functools import lru_cache
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
from servicios.kdd_fases import FaseKDD, etiqueta_proveedor_clima_ui, get_fases_kdd

from servicios.gemelo_digital_datos import (
    cargar_red_gemelo,
    cargar_tracking_gemelo_cassandra,
)
from servicios.gemelo_digital_rutas import (
    construir_grafo_ponderado,
    comparar_original_vs_bloqueo,
    nodo_mas_cercano,
    tiempo_estimado_horas,
)


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


class GemeloNodo(BaseModel):
    id_nodo: str
    tipo: str
    lat: float
    lon: float
    id_capital_ref: str | None = None
    nombre: str | None = None


class GemeloArista(BaseModel):
    src: str
    dst: str
    distancia_km: float


class GemeloRutaRequest(BaseModel):
    """
    Calcula shortest path con pesos dinámicos:
      peso_arista = distancia_km * factor_atasco * factor_clima
    y opcionalmente recálcula si `bloqueo_nodo` aplica.
    """

    id_camion: str | None = Field(default=None, description="ID de camión en Cassandra tracking_camiones")
    usar_origen_cercano_a_camion: bool = Field(
        default=True,
        description="Si true: setea `origen` como el nodo más cercano al GPS del camión.",
    )
    origen: str | None = Field(default=None, description="ID del nodo origen (opcional si hay id_camion)")
    destino: str = Field(..., description="ID del nodo destino (idealmente capital)")

    factor_atasco: float = Field(1.0, ge=1.0, le=5.0, description="Multiplica coste por atasco (1.0x a 5.0x)")
    factor_clima: float = Field(1.0, ge=1.0, le=3.0, description="Multiplica coste por clima (1.0x a 3.0x)")
    bloqueo_nodo: str | None = Field(
        default=None,
        description="Nodo capital bloqueado (si cae en la ruta se recalcula ruta alternativa).",
    )

    velocidad_kmh: float = Field(75.0, gt=0, description="Velocidad base para convertir km a tiempo estimado")


class GemeloRutaPoint(BaseModel):
    id_nodo: str
    lat: float
    lon: float


class GemeloRutaResponse(BaseModel):
    ruta_original: List[str]
    ruta_original_puntos: List[GemeloRutaPoint]
    km_original: float
    tiempo_min_original: float

    recalculado: bool
    ruta_alternativa: List[str] | None
    ruta_alternativa_puntos: List[GemeloRutaPoint] | None
    km_alternativa: float | None
    tiempo_min_alternativa: float | None

    delta_km: float | None
    delta_tiempo_min: float | None
    factor_atasco: float
    factor_clima: float


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

_CLIMA_UI = etiqueta_proveedor_clima_ui()

app = FastAPI(
    title=f"{PROJECT_DISPLAY_NAME} — API",
    description=(
        f"{PROJECT_DESCRIPTION}\n\n"
        "Endpoints de **solo lectura** para integración con otras aplicaciones: "
        "topología, estado de servicios, datos en Cassandra (tras procesamiento Spark) "
        f"y clima por hubs ({_CLIMA_UI}; configurable con `SIMLOG_WEATHER_PROVIDER`)."
    ),
    version="1.0.0",
    openapi_tags=[
        {"name": "sistema", "description": "Salud y metadatos del proyecto"},
        {"name": "infraestructura", "description": "HDFS, Kafka, Cassandra (puertos y comprobaciones)"},
        {"name": "topologia", "description": "Red estática (config_nodos)"},
        {"name": "datos", "description": "Lecturas desde Cassandra (operativo)"},
        {"name": "clima", "description": f"Consulta en tiempo real a {_CLIMA_UI} (hubs)"},
        {"name": "kdd", "description": "Fases del ciclo KDD documentadas en el proyecto"},
        {"name": "gemelo", "description": "Gemelo Digital Logístico (red, tracking y rutas)"},
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
    summary=f"Clima actual por hub ({_CLIMA_UI})",
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
    return [_fase_to_schema(f) for f in get_fases_kdd()]


@lru_cache(maxsize=1)
def _gemelo_red_cache() -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], str]:
    """
    La red gemelo es estática; cacheamos la carga para que los endpoints de rutas respondan rápido.
    """
    nodos, aristas, fuente = cargar_red_gemelo()
    return nodos, aristas, fuente


def _gemelo_red_cached(force_refresh: bool = False) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], str]:
    if force_refresh:
        _gemelo_red_cache.cache_clear()
    return _gemelo_red_cache()


@app.get(
    "/api/v1/gemelo/red/nodos",
    tags=["gemelo"],
    summary="Gemelo digital — nodos (red_gemelo_nodos)",
    response_model=List[GemeloNodo],
)
def api_gemelo_red_nodos() -> List[GemeloNodo]:
    nodos, _, _ = _gemelo_red_cached(force_refresh=False)
    return [GemeloNodo(**n) for n in nodos]


@app.get(
    "/api/v1/gemelo/red/aristas",
    tags=["gemelo"],
    summary="Gemelo digital — aristas (red_gemelo_aristas)",
    response_model=List[GemeloArista],
)
def api_gemelo_red_aristas() -> List[GemeloArista]:
    _, aristas, _ = _gemelo_red_cached(force_refresh=False)
    return [GemeloArista(**a) for a in aristas]


@app.get(
    "/api/v1/gemelo/datos/tracking",
    tags=["gemelo"],
    summary="Gemelo digital — tracking (Cassandra: id_camion, lat, lon, ultima_posicion)",
)
def api_gemelo_tracking() -> List[Dict[str, Any]]:
    return cargar_tracking_gemelo_cassandra()


@app.post(
    "/api/v1/gemelo/ruta/shortest-path",
    tags=["gemelo"],
    summary="Gemelo digital — Dijkstra shortest path con pesos dinámicos + recálculo por bloqueo",
    response_model=GemeloRutaResponse,
)
def api_gemelo_ruta_shortest_path(req: GemeloRutaRequest) -> GemeloRutaResponse:
    nodos, aristas, _fuente = _gemelo_red_cached(force_refresh=False)
    if not nodos or not aristas:
        raise ValueError("Red gemelo no disponible (nodos/aristas vacíos).")

    nodo_origen = req.origen

    if req.id_camion and (nodo_origen is None or not str(nodo_origen).strip()):
        tracking = cargar_tracking_gemelo_cassandra()
        camion = next((t for t in tracking if str(t.get("id_camion")) == str(req.id_camion)), None)
        if not camion:
            raise ValueError(f"Camión '{req.id_camion}' no encontrado en Cassandra.")
        if not req.usar_origen_cercano_a_camion:
            raise ValueError("Si envías id_camion, activa usar_origen_cercano_a_camion o envía origen.")
        if camion.get("lat") is None or camion.get("lon") is None:
            raise ValueError("El camión encontrado no tiene lat/lon válidos.")
        nodo_origen = nodo_mas_cercano(float(camion["lat"]), float(camion["lon"]), nodos)

    if not nodo_origen:
        raise ValueError("Debes indicar `origen` o enviar `id_camion` con origen cercano.")

    # Shortest path con pesos dinámicos:
    #   peso_arista = distancia_km * factor_atasco * factor_clima
    G = construir_grafo_ponderado(aristas, req.factor_atasco, req.factor_clima)

    cmp_out = comparar_original_vs_bloqueo(
        G,
        origen=str(nodo_origen),
        destino=str(req.destino),
        nodo_bloqueado=req.bloqueo_nodo,
    )

    ruta_original = cmp_out.get("ruta_original") or []
    km_original = float(cmp_out.get("km_original") or 0.0)
    coste_original = float(cmp_out.get("coste_original") or 0.0)
    tiempo_min_original = tiempo_estimado_horas(
        km_original,
        coste_original,
        velocidad_kmh=req.velocidad_kmh,
    ) * 60.0

    recalculado = bool(cmp_out.get("recalculado"))
    ruta_alternativa = cmp_out.get("ruta_alternativa") or None
    km_alternativa_val: float | None = None if not recalculado else (float(cmp_out.get("km_alternativa") or 0.0) if ruta_alternativa else None)
    tiempo_min_alternativa: float | None = None
    if recalculado and ruta_alternativa:
        coste_alt = float(cmp_out.get("coste_alternativa") or 0.0)
        km_alt = float(cmp_out.get("km_alternativa") or 0.0)
        tiempo_min_alternativa = tiempo_estimado_horas(km_alt, coste_alt, velocidad_kmh=req.velocidad_kmh) * 60.0

    delta_km = None
    delta_tiempo_min = None
    if recalculado and km_alternativa_val is not None and tiempo_min_alternativa is not None:
        delta_km = km_alternativa_val - km_original
        delta_tiempo_min = tiempo_min_alternativa - tiempo_min_original

    # Coordenadas lat/lon de cada nodo en la ruta (útil para dibujar polilíneas en el cliente).
    pos = {str(n.get("id_nodo")): (float(n.get("lat")), float(n.get("lon"))) for n in nodos if n.get("id_nodo") is not None}

    def _ruta_a_puntos(ruta: List[str]) -> List[GemeloRutaPoint]:
        pts: List[GemeloRutaPoint] = []
        for nid in ruta:
            key = str(nid)
            if key in pos:
                lat, lon = pos[key]
                pts.append(GemeloRutaPoint(id_nodo=key, lat=lat, lon=lon))
        return pts

    ruta_original_puntos = _ruta_a_puntos([str(x) for x in ruta_original])
    ruta_alternativa_puntos = _ruta_a_puntos([str(x) for x in ruta_alternativa]) if ruta_alternativa else None

    return GemeloRutaResponse(
        ruta_original=[str(x) for x in ruta_original],
        ruta_original_puntos=ruta_original_puntos,
        km_original=km_original,
        tiempo_min_original=tiempo_min_original,
        recalculado=recalculado,
        ruta_alternativa=[str(x) for x in ruta_alternativa] if ruta_alternativa else None,
        ruta_alternativa_puntos=ruta_alternativa_puntos,
        km_alternativa=km_alternativa_val,
        tiempo_min_alternativa=tiempo_min_alternativa,
        delta_km=delta_km,
        delta_tiempo_min=delta_tiempo_min,
        factor_atasco=req.factor_atasco,
        factor_clima=req.factor_clima,
    )
