"""
Operaciones de supervisión y control alineadas con cada fase KDD (cuadro de mando).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple


@dataclass(frozen=True)
class OperacionFase:
    codigo: str
    titulo: str
    descripcion: str
    tipo: str  # ingesta | spark | consulta_cassandra | consulta_hive | clima | verificacion


# Fase ordinal -> lista de operaciones mostradas en el dashboard
OPERACIONES_POR_FASE: Tuple[Tuple[int, List[OperacionFase]], ...] = (
    (
        1,
        [
            OperacionFase(
                "sel_clima",
                "Datos meteorológicos por hub",
                "OpenWeatherMap: selección de fuentes y variables para anticipación de retrasos.",
                "clima",
            ),
            OperacionFase(
                "sel_topologia",
                "Topología de red (España)",
                "5 hubs + 25 secundarios; definición de aristas para el modelo.",
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
                "Consultas de series sobre `historico_nodos` y maestros.",
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
