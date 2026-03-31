"""
Cliente Open-Meteo (forecast API) — clima actual por lat/lon.

Documentación: https://open-meteo.com/en/docs
Sin API key en uso no comercial (revisar términos en open-meteo.com).

Los códigos meteorológicos siguen WMO; se traducen a un `weather_id` sintético compatible con las heurísticas existentes
para reutilizar las heurísticas de `clima_retrasos.py`.
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import OPEN_METEO_FORECAST_URL

# WMO Weather interpretation codes (subset) → descripción corta en español
# Referencia: https://open-meteo.com/en/docs (tabla weather codes)
_WMO_DESC_ES: Dict[int, str] = {
    0: "cielo despejado",
    1: "mayormente despejado",
    2: "parcialmente nublado",
    3: "nublado",
    45: "niebla",
    48: "niebla con escarcha",
    51: "llovizna ligera",
    53: "llovizna moderada",
    55: "llovizna densa",
    56: "llovizna helada ligera",
    57: "llovizna helada densa",
    61: "lluvia ligera",
    63: "lluvia moderada",
    65: "lluvia fuerte",
    66: "lluvia helada ligera",
    67: "lluvia helada fuerte",
    71: "nieve ligera",
    73: "nieve moderada",
    75: "nieve fuerte",
    77: "granizo / nieve",
    80: "chubascos ligeros",
    81: "chubascos moderados",
    82: "chubascos violentos",
    85: "chubascos de nieve ligeros",
    86: "chubascos de nieve fuertes",
    95: "tormenta",
    96: "tormenta con granizo ligero",
    99: "tormenta con granizo fuerte",
}


def _wmo_a_weather_id_sintetico(wmo: Optional[int]) -> Optional[int]:
    """
    Aproxima un id numérico compatible con rangos tipo «OWM» usados en
    `_minutos_base_por_clima` / `construir_bloqueos_clima_obras`.
    """
    if wmo is None:
        return None
    c = int(wmo)
    if c in (95, 96, 97, 99):
        return 200 + min(max(c - 95, 0), 32)
    if c in (71, 73, 75, 77):
        return 600 + (c - 71) * 3
    if c in (85, 86):
        return 620
    if 61 <= c <= 67:
        return 500 + min(c - 61, 31)
    if 80 <= c <= 82:
        return 520 + (c - 80)
    if 51 <= c <= 57:
        return 300 + min(c - 51, 21)
    if c in (45, 48):
        return 741
    if c == 0:
        return 800
    if c in (1, 2, 3):
        return 801 + c - 1
    return 804


def _desc_wmo(wmo: Optional[int]) -> str:
    if wmo is None:
        return "N/A"
    return _WMO_DESC_ES.get(int(wmo), f"condición WMO {wmo}")


def fetch_openmeteo_current(hub: str, lat: float, lon: float) -> Dict[str, Any]:
    """
    Devuelve el mismo esquema que la rama OpenWeather de `obtener_clima_completo_hub`,
    con `weather_source=openmeteo` y `weather_id` sintético derivado de WMO.
    """
    try:
        timeout = float(os.environ.get("OPEN_METEO_REQUEST_TIMEOUT_SEC", "12"))
        r = requests.get(
            OPEN_METEO_FORECAST_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "current": ",".join(
                    [
                        "temperature_2m",
                        "relative_humidity_2m",
                        "weather_code",
                        "wind_speed_10m",
                        "wind_gusts_10m",
                        "visibility",
                        "rain",
                        "snowfall",
                        "cloud_cover",
                    ]
                ),
                "timezone": "auto",
                "wind_speed_unit": "ms",
            },
            timeout=timeout,
        )
        if r.status_code != 200:
            return {
                "hub": hub,
                "error": f"HTTP {r.status_code}",
                "lat": lat,
                "lon": lon,
                "weather_source": "openmeteo",
            }
        data = r.json()
        cur = data.get("current") or {}
        wmo = cur.get("weather_code")
        wid = _wmo_a_weather_id_sintetico(wmo)
        desc = _desc_wmo(wmo)
        # precipitación última hora (Open-Meteo); nieve en cm → aprox. mm para UI
        rain_mm = cur.get("rain")
        snow_cm = cur.get("snowfall")
        nieve_mm = float(snow_cm) * 10.0 if snow_cm is not None else None
        vis = cur.get("visibility")
        if vis is not None:
            try:
                vis = int(round(float(vis)))
            except (TypeError, ValueError):
                vis = None
        return {
            "hub": hub,
            "lat": lat,
            "lon": lon,
            "weather_id": wid,
            "weather_main": None,
            "descripcion": desc,
            "icon": None,
            "temp": cur.get("temperature_2m"),
            "sensacion": None,
            "humedad": cur.get("relative_humidity_2m"),
            "presion": None,
            "viento_vel": cur.get("wind_speed_10m"),
            "viento_racha": cur.get("wind_gusts_10m"),
            "viento_grados": None,
            "visibilidad_m": vis,
            "nubosidad_pct": cur.get("cloud_cover"),
            "lluvia_1h_mm": rain_mm,
            "nieve_1h_mm": nieve_mm,
            "dt": None,
            "timezone": None,
            "weather_code_wmo": wmo,
            "weather_source": "openmeteo",
        }
    except Exception as e:
        return {
            "hub": hub,
            "error": str(e),
            "lat": lat,
            "lon": lon,
            "weather_source": "openmeteo",
        }


def openmeteo_full_to_ingesta_slim(full: Dict[str, Any]) -> Dict[str, Any]:
    """Convierte la salida de `fetch_openmeteo_current` al mapa `clima_hubs` de la ingesta."""
    if full.get("error"):
        return {
            "descripcion": str(full["error"]),
            "temp": None,
            "humedad": None,
            "viento": None,
            "source": "openmeteo",
        }
    return {
        "descripcion": full.get("descripcion", "N/A"),
        "temp": full.get("temp"),
        "humedad": full.get("humedad"),
        "viento": full.get("viento_vel"),
        "source": "openmeteo",
    }


def _slim_from_om_block(block: Dict[str, Any]) -> Dict[str, Any]:
    """Un elemento de la respuesta multi-ubicación de Open-Meteo → mapa ingesta."""
    cur = (block or {}).get("current") or {}
    if not cur:
        return {
            "descripcion": "N/A",
            "temp": None,
            "humedad": None,
            "viento": None,
            "source": "openmeteo",
        }
    wmo = cur.get("weather_code")
    desc = _desc_wmo(wmo)
    return {
        "descripcion": desc,
        "temp": cur.get("temperature_2m"),
        "humedad": cur.get("relative_humidity_2m"),
        "viento": cur.get("wind_speed_10m"),
        "source": "openmeteo",
    }


def fetch_openmeteo_bulk_slim(nodos: List[Tuple[str, float, float]]) -> Dict[str, Dict[str, Any]]:
    """
    Clima actual para varios nodos en una sola petición HTTP (Open-Meteo devuelve un array).
    `nodos`: lista de (id_nodo, lat, lon) en el orden deseado.
    Si la petición agrupada falla o el formato no coincide, hace fallback a una petición por nodo.
    """
    if not nodos:
        return {}
    if len(nodos) == 1:
        nid, la, lo = nodos[0]
        return {nid: openmeteo_full_to_ingesta_slim(fetch_openmeteo_current(nid, la, lo))}

    def _individual() -> Dict[str, Dict[str, Any]]:
        """
        Fallback: una petición por nodo. Antes era secuencial (p. ej. 25 nodos × 12 s ≈ 5 min)
        y Streamlit parecía «colgado». Se paraleliza con un pool acotado.
        """
        out: Dict[str, Dict[str, Any]] = {}
        try:
            workers = int(os.environ.get("OPEN_METEO_INDIVIDUAL_MAX_WORKERS", "8"))
        except Exception:
            workers = 8
        workers = max(1, min(workers, len(nodos)))

        def _one(item: Tuple[str, float, float]) -> Tuple[str, Dict[str, Any]]:
            nid, la, lo = item
            return nid, openmeteo_full_to_ingesta_slim(fetch_openmeteo_current(nid, la, lo))

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_one, t) for t in nodos]
            for fut in as_completed(futs):
                nid, slim = fut.result()
                out[nid] = slim
        return out

    try:
        timeout = float(os.environ.get("OPEN_METEO_REQUEST_TIMEOUT_SEC", "20"))
        params: List[Tuple[str, Any]] = []
        for _nid, la, lo in nodos:
            params.append(("latitude", la))
            params.append(("longitude", lo))
        params.extend(
            [
                (
                    "current",
                    ",".join(
                        [
                            "temperature_2m",
                            "relative_humidity_2m",
                            "weather_code",
                            "wind_speed_10m",
                            "wind_gusts_10m",
                            "visibility",
                            "rain",
                            "snowfall",
                            "cloud_cover",
                        ]
                    ),
                ),
                ("timezone", "auto"),
                ("wind_speed_unit", "ms"),
            ]
        )
        r = requests.get(OPEN_METEO_FORECAST_URL, params=params, timeout=timeout)
        if r.status_code != 200:
            return _individual()
        data = r.json()
        if not isinstance(data, list) or len(data) != len(nodos):
            return _individual()
        return {nid: _slim_from_om_block(block) for (nid, _la, _lo), block in zip(nodos, data)}
    except Exception:
        return _individual()
