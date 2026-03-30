"""
Fases KDD — definición central (usada por Streamlit y API REST).
Los textos que mencionan el proveedor de clima se generan con `get_fases_kdd()` según `SIMLOG_WEATHER_PROVIDER`.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from config import WEATHER_PROVIDER


@dataclass(frozen=True)
class FaseKDD:
    """Una fase del ciclo KDD con vínculo a lo que hace el proyecto."""

    orden: int
    codigo: str
    titulo: str
    resumen: str
    actividades: Tuple[str, ...]
    datos_entrada: Tuple[str, ...]
    datos_salida: Tuple[str, ...]
    stack: Tuple[str, ...]
    script: str = ""


def _norm_weather() -> str:
    return (WEATHER_PROVIDER or "openmeteo").strip().lower()


def etiqueta_proveedor_clima_ui() -> str:
    """Nombre corto para textos de usuario (KDD, cuadro de mando, OpenAPI)."""
    if _norm_weather() in ("openweather", "open-weather"):
        return "OpenWeatherMap"
    return "Open-Meteo"


def linea_actividad_clima_por_hub() -> str:
    """Actividad de la fase 1 según proveedor configurado."""
    if _norm_weather() in ("openweather", "open-weather"):
        return "Clima en tiempo real por API OpenWeatherMap para cada hub."
    return "Clima en tiempo real vía Open-Meteo (API pública, sin clave) para cada hub."


def descripcion_operacion_clima_fase1() -> str:
    """Descripción de la operación «Datos meteorológicos por hub» (fase 1)."""
    base = "selección de fuentes y variables para anticipación de retrasos."
    if _norm_weather() in ("openweather", "open-weather"):
        return f"OpenWeatherMap: {base}"
    return f"Open-Meteo: {base}"


def get_fases_kdd() -> Tuple[FaseKDD, ...]:
    """Fases KDD; la fase 1 refleja el proveedor de clima actual (`config.WEATHER_PROVIDER`)."""
    return (
        FaseKDD(
            orden=1,
            codigo="seleccion",
            titulo="Selección de datos",
            resumen="Elegir fuentes relevantes para el dominio logístico (España).",
            actividades=(
                "Red de 5 hubs + 25 nodos secundarios (coordenadas reales).",
                linea_actividad_clima_por_hub(),
                "Definición de aristas (malla hubs + estrella + redundancia secundarios).",
            ),
            datos_entrada=("coordenadas nodos", "API clima", "topología de red"),
            datos_salida=("payload JSON enriquecido con contexto temporal",),
            stack=("config_nodos.py", "ingesta/ingesta_kdd.py"),
            script="ingesta/ingesta_kdd.py",
        ),
        FaseKDD(
            orden=2,
            codigo="preprocesamiento",
            titulo="Preprocesamiento",
            resumen="Normalizar, simular y validar antes de publicar.",
            actividades=(
                "Simulación de incidentes en nodos y aristas (OK / Congestionado / Bloqueado).",
                "Interpolación GPS de camiones cada 15 minutos.",
                "Serialización JSON y publicación a Kafka; backup en HDFS.",
            ),
            datos_entrada=("estados simulados", "rutas camiones"),
            datos_salida=("topic Kafka", "ficheros JSON en HDFS"),
            stack=("Kafka", "HDFS", "ingesta/ingesta_kdd.py"),
            script="ingesta/ingesta_kdd.py",
        ),
        FaseKDD(
            orden=3,
            codigo="transformacion",
            titulo="Transformación",
            resumen="Modelar la red como grafo y aplicar reglas de negocio.",
            actividades=(
                "Construcción GraphFrame (vértices + aristas con distancia km).",
                "Autosanación: aristas Bloqueado fuera del grafo; Congestionado o motivo Niebla/Lluvia → peso ×1,5.",
                "En el dashboard, expande «Reglas de negocio y evolución del grafo (fases 3–5)».",
            ),
            datos_entrada=("JSON HDFS / Kafka", "topología estática"),
            datos_salida=("grafo filtrado y ponderado",),
            stack=(
                "Spark 3.5",
                "GraphFrames",
                "config_nodos.py",
                "datos/rutas_red_simlog.yaml",
                "procesamiento/procesamiento_grafos.py",
            ),
            script="procesamiento/procesamiento_grafos.py",
        ),
        FaseKDD(
            orden=4,
            codigo="mineria",
            titulo="Minería de datos",
            resumen="Extraer conocimiento sobre criticidad y rutas alternativas.",
            actividades=(
                "PageRank para identificar nodos críticos.",
                "Cálculo de rutas / caminos alternativos según pesos dinámicos.",
            ),
            datos_entrada=("GraphFrame saneado",),
            datos_salida=("scores PageRank", "rutas sugeridas por camión"),
            stack=("Spark", "GraphFrames"),
            script="procesamiento/procesamiento_grafos.py",
        ),
        FaseKDD(
            orden=5,
            codigo="interpretacion",
            titulo="Interpretación y despliegue",
            resumen="Persistir resultados y visualizar para decisión.",
            actividades=(
                "Escritura en Cassandra (estado actual y tracking).",
                "Histórico en Hive (si el metastore está disponible; opcional en 4GB RAM).",
                "Dashboard Streamlit (mapa, métricas, verificación).",
            ),
            datos_entrada=("DataFrames de Spark",),
            datos_salida=("tablas Cassandra", "tablas Hive opcionales"),
            stack=("Cassandra", "Hive", "Streamlit", "API REST /servicios"),
            script="procesamiento/procesamiento_grafos.py",
        ),
    )
