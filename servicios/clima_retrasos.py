"""
Anticipación de retrasos logísticos a partir de variables meteorológicas + contexto operativo.

Proveedor por defecto: Open-Meteo (códigos WMO → heurísticas compatibles con rangos OWM).
Alternativa: OpenWeather (`SIMLOG_WEATHER_PROVIDER=openweather` + API_WEATHER_KEY). Reglas de negocio: congestión, obras, incidentes simulados en Cassandra.
"""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import API_WEATHER_BASE, API_WEATHER_KEY, WEATHER_PROVIDER
from config_nodos import get_nodos


@lru_cache(maxsize=1)
def _secundarios_por_hub_cached() -> Dict[str, List[str]]:
    """Mapea hub -> lista de nodos secundarios (cacheado por proceso)."""
    nodos_cfg = get_nodos()
    secundarios_por_hub: Dict[str, List[str]] = {}
    for nid, d in nodos_cfg.items():
        if d.get("tipo") == "hub":
            continue
        h = d.get("hub")
        if h:
            secundarios_por_hub.setdefault(h, []).append(nid)
    return secundarios_por_hub


def obtener_clima_completo_hub(hub: str, lat: float, lon: float) -> Dict[str, Any]:
    """Clima actual por hub: Open-Meteo u OpenWeather según `SIMLOG_WEATHER_PROVIDER`."""
    if WEATHER_PROVIDER in ("openmeteo", "open-meteo"):
        from servicios.open_meteo_clima import fetch_openmeteo_current

        return fetch_openmeteo_current(hub, lat, lon)

    if not (API_WEATHER_KEY or "").strip():
        return {
            "hub": hub,
            "error": "API_WEATHER_KEY no configurada (define API_WEATHER_KEY u OWM_API_KEY/OPENWEATHER_API_KEY).",
            "lat": lat,
            "lon": lon,
            "weather_source": "openweather",
        }
    try:
        timeout = float(os.environ.get("OWM_REQUEST_TIMEOUT_SEC", "8"))
        r = requests.get(
            API_WEATHER_BASE,
            params={
                "lat": lat,
                "lon": lon,
                "appid": API_WEATHER_KEY,
                "units": "metric",
                "lang": "es",
            },
            timeout=timeout,
        )
        if r.status_code != 200:
            return {
                "hub": hub,
                "error": f"HTTP {r.status_code}",
                "lat": lat,
                "lon": lon,
                "weather_source": "openweather",
            }
        d = r.json()
        w0 = (d.get("weather") or [{}])[0]
        main = d.get("main") or {}
        wind = d.get("wind") or {}
        rain = d.get("rain") or {}
        snow = d.get("snow") or {}
        clouds = d.get("clouds") or {}
        return {
            "hub": hub,
            "lat": lat,
            "lon": lon,
            "weather_id": w0.get("id"),
            "weather_main": w0.get("main"),
            "descripcion": w0.get("description", ""),
            "icon": w0.get("icon"),
            "temp": main.get("temp"),
            "sensacion": main.get("feels_like"),
            "humedad": main.get("humidity"),
            "presion": main.get("pressure"),
            "viento_vel": wind.get("speed"),
            "viento_racha": wind.get("gust"),
            "viento_grados": wind.get("deg"),
            "visibilidad_m": d.get("visibility"),
            "nubosidad_pct": clouds.get("all"),
            "lluvia_1h_mm": rain.get("1h"),
            "nieve_1h_mm": snow.get("1h"),
            "dt": d.get("dt"),
            "timezone": d.get("timezone"),
            "weather_source": "openweather",
        }
    except Exception as e:
        return {"hub": hub, "error": str(e), "lat": lat, "lon": lon, "weather_source": "openweather"}


def obtener_clima_todos_hubs_completo() -> Dict[str, Dict[str, Any]]:
    """Clima detallado para todos los nodos ``hub`` de ``config_nodos`` (capitales provinciales)."""
    nodos = get_nodos()
    out: Dict[str, Dict[str, Any]] = {}
    for nid, meta in nodos.items():
        if str(meta.get("tipo", "")).lower() != "hub":
            continue
        out[nid] = obtener_clima_completo_hub(nid, float(meta["lat"]), float(meta["lon"]))
    return out


def _categoria_owm(weather_id: Optional[int]) -> str:
    if weather_id is None:
        return "desconocido"
    if 200 <= weather_id <= 232:
        return "tormenta"
    if 300 <= weather_id <= 321:
        return "llovizna"
    if 500 <= weather_id <= 531:
        return "lluvia"
    if 600 <= weather_id <= 622:
        return "nieve"
    if 701 <= weather_id <= 781:
        return "atmosfera"  # niebla, polvo, humo…
    if weather_id == 800:
        return "despejado"
    if 801 <= weather_id <= 804:
        return "nubes"
    return "otro"


def _minutos_base_por_clima(weather_id: Optional[int], descripcion: str) -> Tuple[int, List[str]]:
    """
    Estimación de minutos de retraso adicional (margen operativo) por fenómeno meteorológico.
    Valores orientativos para supervisión, no predicción contractual.
    """
    desc = (descripcion or "").lower()
    factores: List[str] = []
    minutos = 0
    wid = weather_id or 0

    if 200 <= wid <= 232:
        minutos += 45
        factores.append("Tormenta eléctrica: riesgo de paradas y reducción de velocidad")
    elif 503 <= wid <= 531 or wid == 502:
        minutos += 35
        factores.append("Lluvia fuerte o muy fuerte: adherencia y visibilidad")
    elif 500 <= wid <= 501:
        minutos += 18
        factores.append("Lluvia moderada: posible congestión en accesos urbanos")
    elif 300 <= wid <= 321:
        minutos += 5
        factores.append("Llovizna: impacto leve en tiempo de tránsito")

    if 600 <= wid <= 622:
        minutos += 60
        factores.append("Nieve o hielo: cadenas, desvíos y limitación de velocidad")
    elif wid == 611 or wid == 612:
        minutos += 40
        factores.append("Aguanieve / hielo granulado")

    if wid in (701, 721, 741) or "niebla" in desc or "fog" in desc:
        minutos += 25
        factores.append("Niebla / humo: visibilidad reducida en corredores")

    if "granizo" in desc or "hail" in desc:
        minutos += 25
        factores.append("Granizo: riesgo de daños y paradas temporales")

    if "tormenta" in desc or "thunderstorm" in desc:
        if not any("Tormenta" in f for f in factores):
            minutos += 30
            factores.append("Condición de tormenta en descripción")

    return minutos, factores


def _ajustes_fisicos(
    viento_ms: Optional[float],
    racha_ms: Optional[float],
    visibilidad_m: Optional[int],
    humedad: Optional[float],
) -> Tuple[int, List[str]]:
    extra = 0
    f: List[str] = []
    if viento_ms is not None and viento_ms > 15:
        extra += 12
        f.append(f"Viento fuerte ({viento_ms:.1f} m/s): inestabilidad de carga y carril")
    if racha_ms is not None and racha_ms > 20:
        extra += 5
        f.append(f"Rachas de viento ({racha_ms:.1f} m/s)")
    if visibilidad_m is not None and visibilidad_m < 5000:
        extra += 10
        f.append(f"Visibilidad reducida ({visibilidad_m} m)")
    if visibilidad_m is not None and visibilidad_m < 1000:
        extra += 15
        f.append("Visibilidad muy baja (<1 km): riesgo alto de retraso")
    if humedad is not None and humedad > 90:
        extra += 0
        f.append(f"Humedad alta ({humedad}%): puede agravar condensación en parabrisas")
    return extra, f


def _impacto_operativo_hub(
    hub: str,
    nodos_cassandra: List[Dict[str, Any]],
) -> Tuple[int, List[str]]:
    """
    Cruza nodos en Cassandra con configuración de red para estimar congestión/obras en el área del hub.
    """
    secundarios_por_hub = _secundarios_por_hub_cached()
    afectados = [hub] + secundarios_por_hub.get(hub, [])
    minutos = 0
    factores: List[str] = []
    for row in nodos_cassandra:
        nid = row.get("id_nodo")
        if nid not in afectados:
            continue
        est = (row.get("estado") or "OK").strip()
        motivo = (row.get("motivo_retraso") or "").lower()
        if est == "Congestionado":
            minutos += 12
            if "tráfico" in motivo or "trafico" in motivo:
                factores.append(f"Atascos / tráfico intenso en {nid} (modelo)")
            elif "lluvia" in motivo or "niebla" in motivo:
                factores.append(f"Congestión por condiciones en {nid}")
            else:
                factores.append(f"Congestión operativa en {nid}")
        elif est == "Bloqueado":
            minutos += 55
            if "obra" in motivo or "obras" in motivo:
                factores.append(f"Obras o corte en {nid}")
            elif "accidente" in motivo:
                factores.append(f"Incidente tipo accidente en {nid}")
            else:
                factores.append(f"Vía o nodo bloqueado en {nid}")
    return minutos, factores


def evaluar_retraso_integrado(
    clima_hub: Dict[str, Any],
    nodos_cassandra: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Combina clima (API) + estado operativo (Cassandra) para un hub.
    """
    hub = clima_hub.get("hub", "?")
    if clima_hub.get("error"):
        return {
            "hub": hub,
            "error": clima_hub["error"],
            "nivel_riesgo": "desconocido",
            "minutos_estimados": 0,
            "factores": [clima_hub["error"]],
        }

    wid = clima_hub.get("weather_id")
    desc = clima_hub.get("descripcion", "")
    m_base, factores_clima = _minutos_base_por_clima(wid, desc)

    m_fis, factores_fis = _ajustes_fisicos(
        clima_hub.get("viento_vel"),
        clima_hub.get("viento_racha"),
        clima_hub.get("visibilidad_m"),
        clima_hub.get("humedad"),
    )

    factores_op: List[str] = []
    m_op = 0
    if nodos_cassandra:
        m_op, factores_op = _impacto_operativo_hub(hub, nodos_cassandra)

    total = min(240, m_base + m_fis + m_op + (0 if clima_hub.get("lluvia_1h_mm") is None else int(clima_hub["lluvia_1h_mm"] or 0)))

    if total < 15:
        nivel = "bajo"
    elif total < 45:
        nivel = "medio"
    else:
        nivel = "alto"

    todos = factores_clima + factores_fis + factores_op
    if not todos:
        todos.append("Condiciones favorables para operación estándar")

    return {
        "hub": hub,
        "categoria": _categoria_owm(wid),
        "weather_id": wid,
        "nivel_riesgo": nivel,
        "minutos_estimados": total,
        "factores": todos,
        "desglose": {
            "clima_base_min": m_base,
            "fisicos_min": m_fis,
            "operativo_min": m_op,
        },
    }
