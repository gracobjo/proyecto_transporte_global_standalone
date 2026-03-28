from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from graphframes import GraphFrame

STATUS_ACTIVE = "ACTIVE"
STATUS_DOWN = "DOWN"

EVENT_NODE_DOWN = "NODE_DOWN"
EVENT_NODE_UP = "NODE_UP"
EVENT_ROUTE_DOWN = "ROUTE_DOWN"
EVENT_ROUTE_UP = "ROUTE_UP"
EVENT_ROUTE_DISABLED = "ROUTE_DISABLED"
EVENT_ROUTE_RESTORED = "ROUTE_RESTORED"
EVENT_ROUTE_REROUTED = "ROUTE_REROUTED"

SUPPORTED_EVENTS = {
    EVENT_NODE_DOWN,
    EVENT_NODE_UP,
    EVENT_ROUTE_DOWN,
    EVENT_ROUTE_UP,
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        raw = value.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(raw)
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return _utc_now()
    return _utc_now()


def _iso_ts(value: Any) -> str:
    return _parse_ts(value).isoformat()


def _route_lookup(aristas: Iterable[Tuple[str, str, float]]) -> Dict[str, Tuple[str, str, float]]:
    lookup: Dict[str, Tuple[str, str, float]] = {}
    for src, dst, dist in aristas:
        lookup[f"{src}|{dst}"] = (src, dst, float(dist))
        lookup[f"{dst}|{src}"] = (src, dst, float(dist))
    return lookup


def normalizar_eventos_grafo(
    eventos: Optional[List[Dict[str, Any]]],
    aristas: Iterable[Tuple[str, str, float]],
) -> List[Dict[str, Any]]:
    lookup = _route_lookup(aristas)
    normalizados: List[Dict[str, Any]] = []
    for ev in eventos or []:
        if not isinstance(ev, dict):
            continue
        event_type = str(ev.get("event") or "").strip().upper()
        if event_type not in SUPPORTED_EVENTS:
            continue
        timestamp = _parse_ts(ev.get("timestamp"))
        if event_type.startswith("NODE_"):
            node_id = ev.get("node_id") or ev.get("id_nodo") or ev.get("entity_id")
            if not node_id:
                continue
            normalizados.append(
                {
                    "event": event_type,
                    "entity_type": "node",
                    "entity_id": str(node_id),
                    "timestamp": timestamp,
                    "cause": ev.get("cause") or ev.get("motivo") or "",
                    "details": ev.get("details") or {},
                }
            )
            continue
        src = ev.get("src") or ev.get("route_src")
        dst = ev.get("dst") or ev.get("route_dst")
        edge_id = ev.get("edge_id") or (f"{src}|{dst}" if src and dst else None)
        if not edge_id or edge_id not in lookup:
            continue
        canon_src, canon_dst, _ = lookup[edge_id]
        normalizados.append(
            {
                "event": event_type,
                "entity_type": "route",
                "entity_id": f"{canon_src}|{canon_dst}",
                "src": canon_src,
                "dst": canon_dst,
                "timestamp": timestamp,
                "cause": ev.get("cause") or ev.get("motivo") or "",
                "details": ev.get("details") or {},
            }
        )
    normalizados.sort(key=lambda item: item["timestamp"])
    return normalizados


def _estado_nodos_inicial(
    nodos: Dict[str, Dict[str, Any]],
    previo: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for nid in nodos:
        prev = (previo or {}).get(nid, {})
        status = str(prev.get("status") or STATUS_ACTIVE).upper()
        out[nid] = {
            "status": STATUS_DOWN if status == STATUS_DOWN else STATUS_ACTIVE,
            "cause": prev.get("cause") or "",
            "updated_at": _iso_ts(prev.get("updated_at")),
            "last_event": prev.get("last_event") or "",
        }
    return out


def _estado_rutas_inicial(
    aristas: Iterable[Tuple[str, str, float]],
    previo: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for src, dst, dist in aristas:
        key = f"{src}|{dst}"
        prev = (previo or {}).get(key, {})
        status = str(prev.get("status") or STATUS_ACTIVE).upper()
        out[key] = {
            "src": src,
            "dst": dst,
            "distancia_km": float(dist),
            "status": STATUS_DOWN if status == STATUS_DOWN else STATUS_ACTIVE,
            "cause": prev.get("cause") or "",
            "updated_at": _iso_ts(prev.get("updated_at")),
            "last_event": prev.get("last_event") or "",
            "manual_down": bool(prev.get("manual_down", status == STATUS_DOWN)),
        }
    return out


def _registrar_evento(
    eventos: List[Dict[str, Any]],
    *,
    timestamp: datetime,
    event_type: str,
    entity_type: str,
    entity_id: str,
    previous_status: str,
    new_status: str,
    cause: str,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    eventos.append(
        {
            "timestamp": timestamp.isoformat(),
            "tipo_evento": event_type,
            "entidad_tipo": entity_type,
            "entidad_id": entity_id,
            "estado_anterior": previous_status,
            "estado_nuevo": new_status,
            "causa": cause,
            "detalles": details or {},
        }
    )


def _aplicar_eventos_reconfiguracion(
    estados_nodos: Dict[str, Dict[str, Any]],
    estados_rutas: Dict[str, Dict[str, Any]],
    eventos: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    eventos_registrados: List[Dict[str, Any]] = []
    for ev in eventos:
        event_type = ev["event"]
        ts = ev["timestamp"]
        cause = ev.get("cause") or ""
        if ev["entity_type"] == "node":
            entity_id = ev["entity_id"]
            state = estados_nodos.get(entity_id)
            if not state:
                continue
            prev = state["status"]
            nuevo = STATUS_DOWN if event_type == EVENT_NODE_DOWN else STATUS_ACTIVE
            state["status"] = nuevo
            state["cause"] = cause if nuevo == STATUS_DOWN else ""
            state["updated_at"] = ts.isoformat()
            state["last_event"] = event_type
            if prev != nuevo:
                _registrar_evento(
                    eventos_registrados,
                    timestamp=ts,
                    event_type=event_type,
                    entity_type="node",
                    entity_id=entity_id,
                    previous_status=prev,
                    new_status=nuevo,
                    cause=cause,
                    details=ev.get("details"),
                )
            continue
        entity_id = ev["entity_id"]
        state = estados_rutas.get(entity_id)
        if not state:
            continue
        prev = state["status"]
        nuevo = STATUS_DOWN if event_type == EVENT_ROUTE_DOWN else STATUS_ACTIVE
        state["manual_down"] = event_type == EVENT_ROUTE_DOWN
        state["status"] = nuevo
        state["cause"] = cause if nuevo == STATUS_DOWN else ""
        state["updated_at"] = ts.isoformat()
        state["last_event"] = event_type
        if prev != nuevo:
            _registrar_evento(
                eventos_registrados,
                timestamp=ts,
                event_type=event_type,
                entity_type="route",
                entity_id=entity_id,
                previous_status=prev,
                new_status=nuevo,
                cause=cause,
                details=ev.get("details"),
            )
    return eventos_registrados


def _recalcular_estado_rutas(
    estados_nodos: Dict[str, Dict[str, Any]],
    estados_rutas: Dict[str, Dict[str, Any]],
    *,
    timestamp: str,
) -> List[Dict[str, Any]]:
    eventos_derivados: List[Dict[str, Any]] = []
    ts = _parse_ts(timestamp)
    for route_id, route in estados_rutas.items():
        prev = route["status"]
        endpoint_down = (
            estados_nodos.get(route["src"], {}).get("status") == STATUS_DOWN
            or estados_nodos.get(route["dst"], {}).get("status") == STATUS_DOWN
        )
        if endpoint_down:
            route["status"] = STATUS_DOWN
            route["cause"] = route["cause"] or "Ruta desactivada por nodo caído"
            route["last_event"] = route["last_event"] or EVENT_ROUTE_DISABLED
        elif route.get("manual_down"):
            route["status"] = STATUS_DOWN
        else:
            route["status"] = STATUS_ACTIVE
            route["cause"] = ""
            route["last_event"] = route["last_event"] if route["last_event"] == EVENT_ROUTE_UP else EVENT_ROUTE_RESTORED
        route["updated_at"] = ts.isoformat()
        nuevo = route["status"]
        if prev == nuevo:
            continue
        _registrar_evento(
            eventos_derivados,
            timestamp=ts,
            event_type=EVENT_ROUTE_DISABLED if nuevo == STATUS_DOWN else EVENT_ROUTE_RESTORED,
            entity_type="route",
            entity_id=route_id,
            previous_status=prev,
            new_status=nuevo,
            cause=route["cause"],
        )
    return eventos_derivados


def construir_graphframe_reconfiguracion(
    spark,
    nodos: Dict[str, Dict[str, Any]],
    aristas: Iterable[Tuple[str, str, float]],
    estados_nodos_rt: Dict[str, Dict[str, Any]],
    estados_rutas_rt: Dict[str, Dict[str, Any]],
) -> Tuple[Optional[GraphFrame], Optional[GraphFrame]]:
    vertices_all = []
    vertices_active = []
    for nid, meta in nodos.items():
        row = (
            nid,
            float(meta["lat"]),
            float(meta["lon"]),
            meta.get("tipo", "secundario"),
            estados_nodos_rt.get(nid, {}).get("status", STATUS_ACTIVE),
        )
        vertices_all.append(row)
        if row[-1] == STATUS_ACTIVE:
            vertices_active.append(row)

    edges_all = []
    edges_active = []
    for src, dst, dist in aristas:
        route_id = f"{src}|{dst}"
        route = estados_rutas_rt.get(route_id, {})
        row = (
            src,
            dst,
            float(dist),
            route.get("status", STATUS_ACTIVE),
        )
        edges_all.append(row)
        if row[-1] == STATUS_ACTIVE and src in estados_nodos_rt and dst in estados_nodos_rt:
            if estados_nodos_rt[src]["status"] == STATUS_ACTIVE and estados_nodos_rt[dst]["status"] == STATUS_ACTIVE:
                edges_active.append(row)

    vertices_schema = ["id", "lat", "lon", "tipo", "status"]
    edges_schema = ["src", "dst", "distancia_km", "status"]
    try:
        g_all = GraphFrame(
            spark.createDataFrame(vertices_all, vertices_schema),
            spark.createDataFrame(edges_all, edges_schema),
        )
        g_active = GraphFrame(
            spark.createDataFrame(vertices_active, vertices_schema),
            spark.createDataFrame(edges_active, edges_schema),
        )
        return g_all, g_active
    except Exception:
        return None, None


def _extraer_ruta_bfs(df) -> Optional[List[str]]:
    rows = df.limit(1).collect()
    if not rows:
        return None
    row = rows[0].asDict(recursive=True)
    ruta: List[str] = []
    from_node = row.get("from")
    if isinstance(from_node, dict) and from_node.get("id"):
        ruta.append(from_node["id"])
    intermedios = sorted(
        [key for key in row.keys() if key.startswith("v")],
        key=lambda key: int(key[1:]),
    )
    for key in intermedios:
        node = row.get(key)
        if isinstance(node, dict) and node.get("id"):
            ruta.append(node["id"])
    to_node = row.get("to")
    if isinstance(to_node, dict) and to_node.get("id"):
        ruta.append(to_node["id"])
    return ruta or None


def _ruta_afectada(
    ruta: List[str],
    estados_nodos_rt: Dict[str, Dict[str, Any]],
    estados_rutas_rt: Dict[str, Dict[str, Any]],
) -> bool:
    if not ruta:
        return False
    if any(estados_nodos_rt.get(nodo, {}).get("status") == STATUS_DOWN for nodo in ruta):
        return True
    for src, dst in zip(ruta, ruta[1:]):
        route_id = f"{src}|{dst}"
        inv_id = f"{dst}|{src}"
        if estados_rutas_rt.get(route_id, {}).get("status") == STATUS_DOWN:
            return True
        if estados_rutas_rt.get(inv_id, {}).get("status") == STATUS_DOWN:
            return True
    return False


def _distancias_shortest_paths(g_active: Optional[GraphFrame], landmarks: List[str]) -> Dict[str, Dict[str, int]]:
    if not landmarks or g_active is None:
        return {}
    try:
        rows = g_active.shortestPaths(landmarks=landmarks).collect()
    except Exception:
        return {}
    out: Dict[str, Dict[str, int]] = {}
    for row in rows:
        row_dict = row.asDict(recursive=True)
        out[row_dict["id"]] = row_dict.get("distances") or {}
    return out


def _bfs_python_path(
    aristas: Iterable[Tuple[str, str, float]],
    estados_nodos_rt: Dict[str, Dict[str, Any]],
    estados_rutas_rt: Dict[str, Dict[str, Any]],
    origen: str,
    destino: str,
) -> Optional[List[str]]:
    if origen == destino:
        return [origen]
    if estados_nodos_rt.get(origen, {}).get("status") == STATUS_DOWN:
        return None
    if estados_nodos_rt.get(destino, {}).get("status") == STATUS_DOWN:
        return None
    vecinos: Dict[str, List[str]] = {}
    for src, dst, _ in aristas:
        route = estados_rutas_rt.get(f"{src}|{dst}") or estados_rutas_rt.get(f"{dst}|{src}") or {}
        if route.get("status") == STATUS_DOWN:
            continue
        if estados_nodos_rt.get(src, {}).get("status") == STATUS_DOWN:
            continue
        if estados_nodos_rt.get(dst, {}).get("status") == STATUS_DOWN:
            continue
        vecinos.setdefault(src, []).append(dst)
        vecinos.setdefault(dst, []).append(src)
    queue: List[List[str]] = [[origen]]
    vistos = {origen}
    while queue:
        path = queue.pop(0)
        last = path[-1]
        for nxt in vecinos.get(last, []):
            if nxt in vistos:
                continue
            new_path = path + [nxt]
            if nxt == destino:
                return new_path
            vistos.add(nxt)
            queue.append(new_path)
    return None


def recalcular_rutas_automaticas(
    spark,
    g_active: Optional[GraphFrame],
    aristas: Iterable[Tuple[str, str, float]],
    camiones: List[Dict[str, Any]],
    estados_nodos_rt: Dict[str, Dict[str, Any]],
    estados_rutas_rt: Dict[str, Dict[str, Any]],
    *,
    timestamp: Any = None,
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    rutas_alternativas: Dict[str, Dict[str, Any]] = {}
    eventos_ruta: List[Dict[str, Any]] = []
    destinos = sorted(
        {
            (cam.get("ruta_destino") or ((cam.get("ruta") or [])[-1] if (cam.get("ruta") or []) else ""))
            for cam in camiones
            if cam.get("ruta_destino") or cam.get("ruta")
        }
    )
    distancias = _distancias_shortest_paths(g_active, [d for d in destinos if d])
    ts = _parse_ts(timestamp)

    for camion in camiones:
        ruta_original = camion.get("ruta") or []
        if not _ruta_afectada(ruta_original, estados_nodos_rt, estados_rutas_rt):
            continue
        cid = camion.get("id_camion") or camion.get("id")
        origen = camion.get("nodo_actual") or camion.get("ruta_origen") or (ruta_original[0] if ruta_original else "")
        destino = camion.get("ruta_destino") or (ruta_original[-1] if ruta_original else "")
        if not cid or not origen or not destino or origen == destino:
            continue
        if g_active is not None:
            try:
                bfs_df = g_active.bfs(
                    fromExpr=f"id = '{origen}'",
                    toExpr=f"id = '{destino}'",
                    maxPathLength=max(4, len(ruta_original) + 2),
                )
                ruta_alt = _extraer_ruta_bfs(bfs_df)
            except Exception:
                ruta_alt = None
        else:
            ruta_alt = _bfs_python_path(aristas, estados_nodos_rt, estados_rutas_rt, origen, destino)
        if not ruta_alt:
            continue
        saltos = (distancias.get(origen) or {}).get(destino)
        rutas_alternativas[cid] = {
            "ruta": ruta_alt,
            "saltos_estimados": int(saltos) if saltos is not None else max(len(ruta_alt) - 1, 0),
            "motivo": "Ruta recalculada automáticamente por fallo en grafo",
            "ruta_original": ruta_original,
            "ruta_origen_recalculo": origen,
            "ruta_destino_recalculo": destino,
        }
        _registrar_evento(
            eventos_ruta,
            timestamp=ts,
            event_type=EVENT_ROUTE_REROUTED,
            entity_type="truck",
            entity_id=str(cid),
            previous_status="ORIGINAL",
            new_status="REROUTED",
            cause="Ruta recalculada automáticamente",
            details={
                "ruta_original": ruta_original,
                "ruta_alternativa": ruta_alt,
            },
        )
    return rutas_alternativas, eventos_ruta


def construir_alertas_activas(
    estados_nodos_rt: Dict[str, Dict[str, Any]],
    estados_rutas_rt: Dict[str, Dict[str, Any]],
    *,
    timestamp: Any = None,
) -> Dict[str, Dict[str, Any]]:
    ts = _iso_ts(timestamp)
    alertas: Dict[str, Dict[str, Any]] = {}
    for nid, state in estados_nodos_rt.items():
        if state.get("status") != STATUS_DOWN:
            continue
        alert_id = f"node_down::{nid}"
        alertas[alert_id] = {
            "alerta_id": alert_id,
            "tipo_alerta": "NODE_DOWN",
            "entidad_id": nid,
            "severidad": "critica",
            "mensaje": f"Nodo {nid} caído. Reconfiguración logística activada.",
            "causa": state.get("cause") or "Caída de nodo",
            "timestamp_inicio": state.get("updated_at") or ts,
            "timestamp_ultima_actualizacion": ts,
            "estado": "ACTIVA",
            "ruta_original": "",
            "ruta_alternativa": "",
        }
    for route_id, state in estados_rutas_rt.items():
        if state.get("status") != STATUS_DOWN:
            continue
        alert_id = f"route_down::{route_id}"
        alertas[alert_id] = {
            "alerta_id": alert_id,
            "tipo_alerta": "ROUTE_DOWN",
            "entidad_id": route_id,
            "severidad": "alta",
            "mensaje": f"Ruta {route_id} desactivada en el grafo activo.",
            "causa": state.get("cause") or "Ruta desactivada",
            "timestamp_inicio": state.get("updated_at") or ts,
            "timestamp_ultima_actualizacion": ts,
            "estado": "ACTIVA",
            "ruta_original": route_id.replace("|", "->"),
            "ruta_alternativa": "",
        }
    return alertas


def alertas_resueltas_para_historico(
    alertas_previas: Dict[str, Dict[str, Any]],
    alertas_actuales: Dict[str, Dict[str, Any]],
    *,
    resolved_at: Any = None,
) -> List[Dict[str, Any]]:
    ts = _parse_ts(resolved_at)
    registros: List[Dict[str, Any]] = []
    for alert_id, alerta in (alertas_previas or {}).items():
        if alert_id in (alertas_actuales or {}):
            continue
        started_at = _parse_ts(alerta.get("timestamp_inicio"))
        duracion = max(int((ts - started_at).total_seconds()), 0)
        registros.append(
            {
                "alerta_id": alert_id,
                "tipo_alerta": alerta.get("tipo_alerta", "UNKNOWN"),
                "entidad_id": alerta.get("entidad_id", ""),
                "severidad": alerta.get("severidad", "media"),
                "causa": alerta.get("causa", ""),
                "mensaje": alerta.get("mensaje", ""),
                "timestamp_inicio": started_at.isoformat(),
                "timestamp_fin": ts.isoformat(),
                "estado_resolucion": "RESUELTA",
                "duracion_segundos": duracion,
                "ruta_original": alerta.get("ruta_original", ""),
                "ruta_alternativa": alerta.get("ruta_alternativa", ""),
            }
        )
    return registros


def aplicar_reconfiguracion_logistica(
    spark,
    nodos: Dict[str, Dict[str, Any]],
    aristas: Iterable[Tuple[str, str, float]],
    camiones: List[Dict[str, Any]],
    eventos: Optional[List[Dict[str, Any]]] = None,
    *,
    estado_nodos_previo: Optional[Dict[str, Dict[str, Any]]] = None,
    estado_rutas_previo: Optional[Dict[str, Dict[str, Any]]] = None,
    alertas_previas: Optional[Dict[str, Dict[str, Any]]] = None,
    timestamp: Any = None,
) -> Dict[str, Any]:
    ts = _iso_ts(timestamp)
    estados_nodos_rt = _estado_nodos_inicial(nodos, estado_nodos_previo)
    estados_rutas_rt = _estado_rutas_inicial(aristas, estado_rutas_previo)
    eventos_norm = normalizar_eventos_grafo(eventos, aristas)
    eventos_grafo = _aplicar_eventos_reconfiguracion(estados_nodos_rt, estados_rutas_rt, eventos_norm)
    eventos_grafo.extend(_recalcular_estado_rutas(estados_nodos_rt, estados_rutas_rt, timestamp=ts))
    _, g_active = construir_graphframe_reconfiguracion(spark, nodos, aristas, estados_nodos_rt, estados_rutas_rt)
    rutas_alternativas, eventos_ruta = recalcular_rutas_automaticas(
        spark,
        g_active,
        aristas,
        camiones,
        estados_nodos_rt,
        estados_rutas_rt,
        timestamp=ts,
    )
    eventos_grafo.extend(eventos_ruta)
    alertas_activas = construir_alertas_activas(estados_nodos_rt, estados_rutas_rt, timestamp=ts)
    alertas_historicas = alertas_resueltas_para_historico(alertas_previas or {}, alertas_activas, resolved_at=ts)
    return {
        "estado_nodos": estados_nodos_rt,
        "estado_rutas": estados_rutas_rt,
        "alertas_activas": alertas_activas,
        "alertas_historicas": alertas_historicas,
        "eventos_grafo": eventos_grafo,
        "rutas_alternativas": rutas_alternativas,
        "timestamp": ts,
    }
