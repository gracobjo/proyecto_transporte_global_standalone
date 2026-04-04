"""
SIMLOG España — topología de red (capitales provinciales, grafo completo)

Modelo híbrido operativo:
- **Nodos:** una capital por provincia (lista canónica en `servicios/capitales_provincia_es.py`).
- **Aristas:** **todas las capitales están interconectadas** (grafo completo K_n): cada par tiene
  tramo directo con distancia haversine. Así, al simular caída de un nodo o incidencias por
  arista, Dijkstra / GraphFrames pueden encontrar **alternativas reales** sin depender de un
  esqueleto de solo 5 hubs.

El gemelo digital en Hive (`generar_red_gemelo_digital.py`) añade además **subnodos locales**
por capital; aquí el pipeline (ingesta, Spark) usa solo las capitales para reducir coste en
jobs frecuentes. Para la misma topología extendida en Hive, ejecuta el generador y registra
las tablas `red_gemelo_*`.

Ruta base: ~/proyecto_transporte_global/
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Tuple

from servicios.capitales_provincia_es import lista_capitales


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return round(2 * r * math.asin(math.sqrt(min(1.0, a))), 2)


def get_nodos() -> dict:
    """
    Retorna diccionario id -> {lat, lon, tipo, id_ref?, provincia?}.
    Clave id = nombre legible de la capital (único en la lista canónica).
    Todas las entradas usan tipo ``hub`` para compatibilidad con ingesta/clima/DGT
    (cada capital se trata como nodo principal de su provincia).
    """
    nodos: Dict[str, Dict[str, Any]] = {}
    for c in lista_capitales():
        nombre = str(c["nombre"])
        nodos[nombre] = {
            "lat": float(c["lat"]),
            "lon": float(c["lon"]),
            "tipo": "hub",
            "id_ref": str(c.get("id", "")),
            "provincia": str(c.get("provincia", "")),
        }
    return nodos


def get_aristas() -> list:
    """
    Lista de aristas (src, dst, distancia_km): **grafo completo** entre capitales
    (cada par una vez, sin bucles).
    """
    nodos = get_nodos()
    nombres = sorted(nodos.keys())
    aristas: List[Tuple[str, str, float]] = []
    for i in range(len(nombres)):
        for j in range(i + 1, len(nombres)):
            a, b = nombres[i], nombres[j]
            la, lo = nodos[a]["lat"], nodos[a]["lon"]
            lb, ob = nodos[b]["lat"], nodos[b]["lon"]
            d = _haversine_km(la, lo, lb, ob)
            aristas.append((a, b, d))
    return aristas


_nodos_red = get_nodos()
RED = {
    "nodos": _nodos_red,
    "aristas": get_aristas(),
    "hubs": sorted(_nodos_red.keys()),
    "secundarios": [],
}
