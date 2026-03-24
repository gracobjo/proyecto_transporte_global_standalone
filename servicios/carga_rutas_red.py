"""
Carga del catálogo de conexiones desde `datos/rutas_red_simlog.yaml`
y utilidades de clasificación (hub↔hub, hub↔subnodo, subnodo↔subnodo).

Si falta PyYAML o el fichero, se deriva el catálogo desde `config_nodos.get_aristas()`.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BASE = Path(__file__).resolve().parent.parent
RUTAS_YAML = BASE / "datos" / "rutas_red_simlog.yaml"

_LABEL_TIPO = {
    "malla_hubs": "Hub principal ↔ Hub principal",
    "hub_secundario": "Hub ↔ Ciudad satélite (subnodo)",
    "secundario_secundario": "Subnodo ↔ Subnodo (refuerzo local)",
}


def _haversine_km(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return round(2 * r * math.asin(math.sqrt(a)), 2)


def _clasificar_por_nodos(origen: str, destino: str, nodos: Dict[str, Any]) -> Tuple[str, str]:
    """Devuelve (código interno, etiqueta UI)."""
    no = nodos.get(origen) or {}
    nd = nodos.get(destino) or {}
    to = no.get("tipo", "")
    td = nd.get("tipo", "")
    if to == "hub" and td == "hub":
        return "malla_hubs", _LABEL_TIPO["malla_hubs"]
    if to == "secundario" and td == "secundario":
        return "secundario_secundario", _LABEL_TIPO["secundario_secundario"]
    return "hub_secundario", _LABEL_TIPO["hub_secundario"]


def _catalogo_desde_config_nodos() -> List[Dict[str, Any]]:
    from config_nodos import get_aristas, get_nodos

    nodos = get_nodos()
    filas: List[Dict[str, Any]] = []
    for src, dst, dist in get_aristas():
        codigo, etiqueta = _clasificar_por_nodos(src, dst, nodos)
        filas.append(
            {
                "origen": src,
                "destino": dst,
                "tipo_codigo": codigo,
                "tipo_etiqueta": etiqueta,
                "distancia_km": dist,
                "fuente": "config_nodos",
            }
        )
    filas.sort(key=lambda x: (x["tipo_codigo"], x["origen"], x["destino"]))
    return filas


def cargar_yaml_rutas() -> Optional[Dict[str, Any]]:
    if not RUTAS_YAML.is_file():
        return None
    try:
        import yaml  # type: ignore
    except ImportError:
        return None
    try:
        return yaml.safe_load(RUTAS_YAML.read_text(encoding="utf-8"))
    except Exception:
        return None


def listar_conexiones_catalogo() -> Tuple[List[Dict[str, Any]], str]:
    """
    Lista todas las conexiones con distancia y tipo.
    Devuelve (filas, mensaje_origen_datos).
    """
    from config_nodos import get_nodos

    nodos = get_nodos()
    raw = cargar_yaml_rutas()

    if raw and isinstance(raw, dict):
        filas: List[Dict[str, Any]] = []
        secciones = (
            ("malla_hubs", "malla_hubs", _LABEL_TIPO["malla_hubs"]),
            ("hub_a_secundario", "hub_secundario", _LABEL_TIPO["hub_secundario"]),
            ("secundario_secundario", "secundario_secundario", _LABEL_TIPO["secundario_secundario"]),
        )
        for key_yaml, codigo, etiqueta in secciones:
            pares = raw.get(key_yaml) or []
            for par in pares:
                if not isinstance(par, (list, tuple)) or len(par) < 2:
                    continue
                a, b = str(par[0]).strip(), str(par[1]).strip()
                if a not in nodos or b not in nodos:
                    continue
                la, loa = nodos[a]["lat"], nodos[a]["lon"]
                lb, lob = nodos[b]["lat"], nodos[b]["lon"]
                dist = _haversine_km(la, loa, lb, lob)
                filas.append(
                    {
                        "origen": a,
                        "destino": b,
                        "tipo_codigo": codigo,
                        "tipo_etiqueta": etiqueta,
                        "distancia_km": dist,
                        "fuente": "rutas_red_simlog.yaml",
                    }
                )
        filas.sort(key=lambda x: (x["tipo_codigo"], x["origen"], x["destino"]))
        return filas, f"Catálogo cargado desde `{RUTAS_YAML.name}`."

    filas = _catalogo_desde_config_nodos()
    if not RUTAS_YAML.is_file():
        return filas, (
            "Fichero YAML no encontrado; usando `config_nodos` (misma topología). "
            f"Crea `{RUTAS_YAML.relative_to(BASE)}` para edición explícita."
        )
    try:
        import yaml  # noqa: F401
    except ImportError:
        return filas, (
            "PyYAML no instalado; usando `config_nodos`. "
            "Instala `pyyaml` para leer `rutas_red_simlog.yaml`."
        )
    return filas, "Usando `config_nodos` (error al leer YAML)."


def resumen_conexiones_por_tipo(filas: List[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for f in filas:
        k = f.get("tipo_codigo") or "?"
        out[k] = out.get(k, 0) + 1
    return out
