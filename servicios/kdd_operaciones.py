"""
Operaciones de supervisión y control alineadas con cada fase KDD (cuadro de mando).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

from servicios.kdd_fases import descripcion_operacion_clima_fase1


@dataclass(frozen=True)
class OperacionFase:
    codigo: str
    titulo: str
    descripcion: str
    tipo: str  # ingesta | spark | consulta_cassandra | consulta_hive | clima | verificacion


def get_operaciones_por_fase() -> Tuple[Tuple[int, List[OperacionFase]], ...]:
    """Operaciones por fase; la fase 1 usa el proveedor de clima actual (`SIMLOG_WEATHER_PROVIDER`)."""
    return (
        (
            1,
            [
                OperacionFase(
                    "sel_clima",
                    "Datos meteorológicos por hub",
                    descripcion_operacion_clima_fase1(),
                    "clima",
                ),
                OperacionFase(
                    "sel_topologia",
                    "Topología de red (España)",
                    "50 capitales provinciales; grafo completo (aristas haversine entre cada par).",
                    "verificacion",
                ),
            ],
        ),
        (
            2,
            [
                OperacionFase(
                    "pre_ingesta",
                    "Ingesta simulada (GPS, Kafka, HDFS)",
                    "Normalización, incidentes simulados y publicación a cola y backup.",
                    "ingesta",
                ),
                OperacionFase(
                    "pre_kafka",
                    "Supervisión de topic Kafka",
                    "Verificación de `transporte_filtered` / raw para auditoría.",
                    "verificacion",
                ),
            ],
        ),
        (
            3,
            [
                OperacionFase(
                    "tra_spark",
                    "Transformación GraphFrames",
                    "Grafo dinámico, autosanación de aristas bloqueadas, penalización por clima.",
                    "spark",
                ),
                OperacionFase(
                    "tra_cassandra_nodos",
                    "Vista nodos (Cassandra)",
                    "Consulta supervisada: estado y clima por nodo.",
                    "consulta_cassandra",
                ),
            ],
        ),
        (
            4,
            [
                OperacionFase(
                    "min_spark",
                    "Minería: PageRank y rutas",
                    "Identificación de nodos críticos y alternativas ponderadas.",
                    "spark",
                ),
                OperacionFase(
                    "min_pagerank",
                    "PageRank almacenado",
                    "Consulta supervisada en tabla `pagerank_nodos`.",
                    "consulta_cassandra",
                ),
            ],
        ),
        (
            5,
            [
                OperacionFase(
                    "int_spark",
                    "Persistencia Cassandra + Hive",
                    "Interpretación: tablas operativas e histórico en almacén.",
                    "spark",
                ),
                OperacionFase(
                    "int_hive_hist",
                    "Histórico en Hive",
                    "Consultas de series sobre histórico Hive (`SIMLOG_HIVE_TABLE_HISTORICO_NODOS`) y maestro.",
                    "consulta_hive",
                ),
                OperacionFase(
                    "int_tracking",
                    "Tracking de flota",
                    "Consulta supervisada `tracking_camiones` (retrasos y rutas).",
                    "consulta_cassandra",
                ),
            ],
        ),
    )
