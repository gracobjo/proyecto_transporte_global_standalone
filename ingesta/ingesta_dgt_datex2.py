#!/usr/bin/env python3
"""
SIMLOG Espana - Integracion DATEX2 DGT.

Descarga el feed DATEX2, extrae incidencias relevantes, las normaliza y las
proyecta sobre la topologia logistica del proyecto para poder fusionarlas con la
simulacion existente.
"""

from __future__ import annotations

import json
import math
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests

from config import DGT_DATEX2_URL, DGT_MAX_NODE_DISTANCE_KM, DGT_XML_CACHE_PATH, DGT_XML_META_PATH
from config_nodos import get_nodos


SEVERITY_ORDER = {"low": 1, "medium": 2, "high": 3, "highest": 4}
ESTADO_BY_SEVERITY = {
    "low": "OK",
    "medium": "Congestionado",
    "high": "Bloqueado",
    "highest": "Bloqueado",
}
PESO_BY_SEVERITY = {
    "low": 1.0,
    "medium": 1.5,
    "high": 3.0,
    "highest": 3.0,
}


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _iter_local(root: ET.Element, names: Iterable[str]) -> Iterable[ET.Element]:
    wanted = set(names)
    for elem in root.iter():
        if _local_name(elem.tag) in wanted:
            yield elem


def _first_text(root: ET.Element, names: Iterable[str]) -> Optional[str]:
    for elem in _iter_local(root, names):
        text = (elem.text or "").strip()
        if text:
            return text
    return None


def _all_texts(root: ET.Element, names: Iterable[str]) -> List[str]:
    out: List[str] = []
    for elem in _iter_local(root, names):
        text = (elem.text or "").strip()
        if text:
            out.append(text)
    return out


def _to_float(value) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except ValueError:
        return value


def descargar_xml_datex2(url: str = DGT_DATEX2_URL, timeout: int = 30) -> str:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def _cache_paths() -> tuple[Path, Path]:
    xml_path = Path(DGT_XML_CACHE_PATH)
    meta_path = Path(DGT_XML_META_PATH)
    xml_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    return xml_path, meta_path


def guardar_cache_xml(xml_text: str, *, source: str = "live", url: str = DGT_DATEX2_URL) -> None:
    xml_path, meta_path = _cache_paths()
    xml_path.write_text(xml_text, encoding="utf-8")
    meta_path.write_text(
        json.dumps(
            {
                "source": source,
                "url": url,
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "size_bytes": len(xml_text.encode("utf-8")),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def cargar_cache_xml() -> Optional[str]:
    xml_path, _ = _cache_paths()
    if not xml_path.exists():
        return None
    return xml_path.read_text(encoding="utf-8")


def _pick_description(record: ET.Element) -> str:
    texts = _all_texts(record, ["value", "comment", "generalPublicComment", "description"])
    return texts[0] if texts else "Incidencia DGT"


def _pick_type(record: ET.Element) -> str:
    local = _local_name(record.tag)
    if local != "situationRecord":
        return local
    return _first_text(record, ["situationRecordType", "operatorActionStatus"]) or "situationRecord"


def _extract_location(record: ET.Element) -> Dict[str, Optional[str]]:
    road_name = _first_text(record, ["roadName", "roadNumber", "roadIdentifier"])
    municipality = _first_text(record, ["municipality", "town", "city", "localityName"])
    province = _first_text(record, ["province", "administrativeArea", "county"])
    direction = _first_text(record, ["direction", "carriageway"])
    lat = _to_float(_first_text(record, ["latitude", "latitudeDegrees"]))
    lon = _to_float(_first_text(record, ["longitude", "longitudeDegrees"]))
    return {
        "carretera": road_name,
        "municipio": municipality,
        "provincia": province,
        "sentido": direction,
        "lat": lat,
        "lon": lon,
    }


def _parse_incident(record: ET.Element) -> Dict:
    severity = (_first_text(record, ["overallSeverity", "severity"]) or "low").strip().lower()
    if severity not in SEVERITY_ORDER:
        severity = "medium"
    location = _extract_location(record)
    record_id = record.attrib.get("id") or _first_text(record, ["id"]) or ""
    return {
        "id_incidencia": record_id or f"dgt-{abs(hash(_pick_description(record)))}",
        "tipo": _pick_type(record),
        "severity": severity,
        "estado": ESTADO_BY_SEVERITY.get(severity, "Congestionado"),
        "peso_pagerank": PESO_BY_SEVERITY.get(severity, 1.0),
        "descripcion": _pick_description(record),
        "fecha_inicio": _parse_datetime(_first_text(record, ["overallStartTime", "startTime"])),
        "fecha_fin": _parse_datetime(_first_text(record, ["overallEndTime", "endTime"])),
        "probabilidad": _first_text(record, ["probabilityOfOccurrence"]),
        "source": "dgt",
        **location,
    }


def parsear_xml_datex2(xml_text: str) -> List[Dict]:
    root = ET.fromstring(xml_text)
    incidents: List[Dict] = []
    for elem in root.iter():
        local = _local_name(elem.tag)
        if local == "situationRecord" or local.endswith("SituationRecord"):
            incidents.append(_parse_incident(elem))
    return incidents


def normalizar_incidencia_dgt(raw: Dict) -> Dict:
    severity = str(raw.get("severity") or "medium").lower()
    if severity not in SEVERITY_ORDER:
        severity = "medium"
    return {
        "id_incidencia": raw.get("id_incidencia") or raw.get("id") or "dgt-unknown",
        "source": "dgt",
        "severity": severity,
        "estado": raw.get("estado") or ESTADO_BY_SEVERITY.get(severity, "Congestionado"),
        "peso_pagerank": float(raw.get("peso_pagerank") or PESO_BY_SEVERITY.get(severity, 1.0)),
        "tipo": raw.get("tipo") or "situationRecord",
        "carretera": raw.get("carretera"),
        "municipio": raw.get("municipio"),
        "provincia": raw.get("provincia"),
        "sentido": raw.get("sentido"),
        "lat": _to_float(raw.get("lat")),
        "lon": _to_float(raw.get("lon")),
        "descripcion": raw.get("descripcion") or "Incidencia DGT",
        "fecha_inicio": raw.get("fecha_inicio"),
        "fecha_fin": raw.get("fecha_fin"),
        "probabilidad": raw.get("probabilidad"),
    }


def _prefer_incident(current: Optional[Dict], candidate: Dict) -> Dict:
    if current is None:
        return candidate
    cur_key = (
        SEVERITY_ORDER.get(current.get("severity", "low"), 0),
        -(float(current.get("distancia_nodo_km", 999999.0))),
    )
    cand_key = (
        SEVERITY_ORDER.get(candidate.get("severity", "low"), 0),
        -(float(candidate.get("distancia_nodo_km", 999999.0))),
    )
    return candidate if cand_key > cur_key else current


def mapear_incidencias_a_nodos(
    incidencias: List[Dict],
    nodos: Optional[Dict[str, Dict]] = None,
    max_km: float = DGT_MAX_NODE_DISTANCE_KM,
) -> Dict[str, Dict]:
    nodos = nodos or get_nodos()
    out: Dict[str, Dict] = {}
    for raw in incidencias:
        inc = normalizar_incidencia_dgt(raw)
        if inc.get("lat") is None or inc.get("lon") is None:
            continue
        best_node = None
        best_distance = None
        for node_id, meta in nodos.items():
            distance = haversine_km(float(inc["lat"]), float(inc["lon"]), float(meta["lat"]), float(meta["lon"]))
            if best_distance is None or distance < best_distance:
                best_node = node_id
                best_distance = distance
        if best_node is None or best_distance is None or best_distance > max_km:
            continue
        candidate = {**inc, "nodo_cercano": best_node, "distancia_nodo_km": round(best_distance, 2)}
        out[best_node] = _prefer_incident(out.get(best_node), candidate)
    return out


def fusionar_estados(estados_simulados: Dict[str, Dict], estados_dgt: Dict[str, Dict]) -> Dict[str, Dict]:
    merged: Dict[str, Dict] = {}
    for node_id, info in (estados_simulados or {}).items():
        merged[node_id] = {
            **info,
            "source": info.get("source") or "simulacion",
            "severity": info.get("severity") or "low",
            "peso_pagerank": float(info.get("peso_pagerank") or 1.0),
        }
    for node_id, info in (estados_dgt or {}).items():
        base = merged.get(node_id, {})
        merged[node_id] = {
            **base,
            "estado": info.get("estado", base.get("estado", "OK")),
            "motivo": info.get("descripcion") or info.get("motivo") or base.get("motivo"),
            "source": "dgt",
            "severity": info.get("severity", "medium"),
            "peso_pagerank": float(info.get("peso_pagerank") or 1.0),
            "id_incidencia": info.get("id_incidencia"),
            "descripcion": info.get("descripcion"),
            "carretera": info.get("carretera"),
            "municipio": info.get("municipio"),
            "provincia": info.get("provincia"),
            "distancia_nodo_km": info.get("distancia_nodo_km"),
            "fecha_inicio": info.get("fecha_inicio"),
            "fecha_fin": info.get("fecha_fin"),
        }
    return merged


def obtener_incidencias_dgt(
    *,
    use_cache_only: bool = False,
    allow_cache_fallback: bool = True,
    timeout: int = 30,
    max_km: float = DGT_MAX_NODE_DISTANCE_KM,
) -> Dict:
    xml_text = None
    source_mode = "disabled"
    error = None

    if use_cache_only:
        xml_text = cargar_cache_xml()
        source_mode = "cache" if xml_text else "disabled"
    else:
        try:
            xml_text = descargar_xml_datex2(timeout=timeout)
            guardar_cache_xml(xml_text, source="live")
            source_mode = "live"
        except Exception as exc:
            error = str(exc)
            if allow_cache_fallback:
                xml_text = cargar_cache_xml()
                source_mode = "cache" if xml_text else "disabled"

    if not xml_text:
        return {
            "source_mode": source_mode,
            "error": error or "Feed DGT no disponible",
            "incidencias": [],
            "mapeo_nodos": {},
        }

    raw = parsear_xml_datex2(xml_text)
    incidencias = [normalizar_incidencia_dgt(item) for item in raw]
    mapeo_nodos = mapear_incidencias_a_nodos(incidencias, max_km=max_km)
    return {
        "source_mode": source_mode,
        "error": error,
        "incidencias": incidencias,
        "mapeo_nodos": mapeo_nodos,
    }
