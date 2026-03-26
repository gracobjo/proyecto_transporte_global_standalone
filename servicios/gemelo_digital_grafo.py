"""
Generación de la red del gemelo digital (50 capitales + subnodos) y utilidades de distancia.
Compartido entre `procesamiento/generar_red_gemelo_digital.py` y el dashboard.
"""
from __future__ import annotations

import math
import random
from typing import Any, Dict, List, Set, Tuple

from servicios.capitales_provincia_es import CAPITALES_PROVINCIA_ES

RNG_SEED = 42
SUBNODOS_POR_CAPITAL = 5
RADIO_SUBNODO_KM = 20.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(min(1.0, a)))


def punto_aleatorio_radio(lat: float, lon: float, max_km: float, rng: random.Random) -> Tuple[float, float]:
    dist_km = rng.uniform(0.0, max_km)
    brng = rng.uniform(0.0, 2 * math.pi)
    dx = dist_km * math.sin(brng) / (111.32 * math.cos(math.radians(lat)))
    dy = dist_km * math.cos(brng) / 110.574
    return lat + dy, lon + dx


def generar_grafo(
    rng_seed: int = RNG_SEED,
    subnodos: int = SUBNODOS_POR_CAPITAL,
    radio_km: float = RADIO_SUBNODO_KM,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rng = random.Random(rng_seed)
    capitals = CAPITALES_PROVINCIA_ES
    if len(capitals) != 50:
        raise ValueError(f"Se esperaban 50 capitales, hay {len(capitals)}")

    nodos: List[Dict[str, Any]] = []
    cap_ids: List[str] = []

    for c in capitals:
        cid = c["id"]
        cap_ids.append(cid)
        nodos.append(
            {
                "id_nodo": cid,
                "tipo": "capital",
                "lat": c["lat"],
                "lon": c["lon"],
                "id_capital_ref": cid,
                "nombre": c["nombre"],
            }
        )
        for k in range(subnodos):
            sid = f"{cid}_SUB{k}"
            slat, slon = punto_aleatorio_radio(c["lat"], c["lon"], radio_km, rng)
            nodos.append(
                {
                    "id_nodo": sid,
                    "tipo": "subnodo",
                    "lat": slat,
                    "lon": slon,
                    "id_capital_ref": cid,
                    "nombre": sid,
                }
            )

    cap_pos = {c["id"]: (c["lat"], c["lon"]) for c in capitals}

    aristas: List[Dict[str, Any]] = []
    seen: Set[Tuple[str, str]] = set()

    def add_edge(a: str, b: str) -> None:
        if a == b:
            return
        x, y = (a, b) if a < b else (b, a)
        if (x, y) in seen:
            return
        seen.add((x, y))
        la, lo = next(n["lat"] for n in nodos if n["id_nodo"] == a), next(n["lon"] for n in nodos if n["id_nodo"] == a)
        lb, ob = next(n["lat"] for n in nodos if n["id_nodo"] == b), next(n["lon"] for n in nodos if n["id_nodo"] == b)
        d = haversine_km(la, lo, lb, ob)
        aristas.append({"src": a, "dst": b, "distancia_km": round(d, 4)})

    for i in range(len(cap_ids)):
        for j in range(i + 1, len(cap_ids)):
            add_edge(cap_ids[i], cap_ids[j])

    for n in nodos:
        if n["tipo"] != "subnodo":
            continue
        own_cap = n["id_capital_ref"]
        ox, oy = cap_pos[own_cap]
        dists: List[Tuple[float, str]] = []
        for cid in cap_ids:
            if cid == own_cap:
                continue
            cx, cy = cap_pos[cid]
            dists.append((haversine_km(ox, oy, cx, cy), cid))
        dists.sort(key=lambda t: t[0])
        vecinas = [dists[0][1], dists[1][1]] if len(dists) >= 2 else [dists[0][1]]
        add_edge(n["id_nodo"], own_cap)
        add_edge(n["id_nodo"], vecinas[0])
        if len(vecinas) > 1 and vecinas[1] != own_cap:
            add_edge(n["id_nodo"], vecinas[1])

    return nodos, aristas
