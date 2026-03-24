"""
Red híbrida (topología estática + estado Cassandra + clima OpenWeather).

- Caminos mínimos en **número de saltos** (peso 1 por arista) vía BFS.
- Retrasos, motivos, coste y vehículos afectados a partir de datos operativos.
- Rutas alternativas ante caída simulada de nodo/arista (clima severo, obras, etc.).
"""
from __future__ import annotations

from collections import deque
from typing import Any, Dict, List, Optional, Set, Tuple

from config import COSTE_EURO_MINUTO_RETASO
from config_nodos import get_aristas, get_nodos
from servicios.clima_retrasos import evaluar_retraso_integrado, obtener_clima_todos_hubs_completo
from servicios.estado_y_datos import cargar_aristas_cassandra, cargar_nodos_cassandra, cargar_tracking_cassandra


def normalizar_arista(u: str, v: str) -> Tuple[str, str]:
    return tuple(sorted((u, v)))


def construir_adyacencia() -> Dict[str, List[str]]:
    """Grafo no dirigido: una arista = peso 1 (mismo coste entre nodos adyacentes)."""
    adj: Dict[str, List[str]] = {}
    for src, dst, _ in get_aristas():
        adj.setdefault(src, []).append(dst)
        adj.setdefault(dst, []).append(src)
    return adj


def nodo_a_hub(nodo_id: str) -> str:
    """Hub propio si es hub; si es secundario, su hub."""
    nodos = get_nodos()
    info = nodos.get(nodo_id) or {}
    if info.get("tipo") == "hub":
        return nodo_id
    return info.get("hub") or nodo_id


def _dict_nodos_cassandra(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {r.get("id_nodo"): r for r in rows if r.get("id_nodo")}


def _dict_aristas_cassandra(rows: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in rows:
        s, d = r.get("src"), r.get("dst")
        if not s or not d:
            continue
        out[normalizar_arista(str(s), str(d))] = r
    return out


def _minutos_por_estado(estado: Optional[str], motivo: Optional[str]) -> Tuple[int, str]:
    e = (estado or "OK").strip()
    m = (motivo or "").lower()
    if "bloque" in e.lower():
        return 999, f"Bloqueo ({motivo or 'sin detalle'})"
    if "congest" in e.lower():
        base = 20
        if "obra" in m or "obras" in m:
            return 35, "Congestión / obras"
        if "niebla" in m or "lluvia" in m or "nieve" in m:
            return 25, "Congestión / clima (Cassandra)"
        return base, "Congestión"
    if "obra" in m or "obras" in m:
        return 25, "Riesgo obras (motivo)"
    return 0, "Sin retraso operativo"


def estimar_retraso_tramo(
    u: str,
    v: str,
    nodos_map: Dict[str, Dict[str, Any]],
    aristas_map: Dict[Tuple[str, str], Dict[str, Any]],
    clima_por_hub: Optional[Dict[str, Dict[str, Any]]],
    nodos_c_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Retraso agregado en el tramo u→v (arista + extremos)."""
    key = normalizar_arista(u, v)
    arow = aristas_map.get(key, {})
    nu, nv = nodos_map.get(u, {}), nodos_map.get(v, {})

    m_arista, mot_arista = _minutos_por_estado(arow.get("estado"), None)
    m_nu, mot_nu = _minutos_por_estado(nu.get("estado"), nu.get("motivo_retraso"))
    m_nv, mot_nv = _minutos_por_estado(nv.get("estado"), nv.get("motivo_retraso"))

    # Tomar el máximo razonable (no sumar todo para no inflar)
    candidatos = [
        (m_arista, mot_arista or "Arista"),
        (m_nu, mot_nu or f"Nodo {u}"),
        (m_nv, mot_nv or f"Nodo {v}"),
    ]
    m_max, _motivo_max = max(candidatos, key=lambda x: x[0])

    extra_cli = 0
    cli_txt = ""
    if clima_por_hub:
        hu, hv = nodo_a_hub(u), nodo_a_hub(v)
        raw_u = {**(clima_por_hub.get(hu) or {}), "hub": hu}
        raw_v = {**(clima_por_hub.get(hv) or {}), "hub": hv}
        e1 = evaluar_retraso_integrado(raw_u, nodos_c_rows)
        e2 = evaluar_retraso_integrado(raw_v, nodos_c_rows)
        extra_cli = max(int(e1.get("minutos_estimados", 0)), int(e2.get("minutos_estimados", 0))) // 3
        if extra_cli > 0:
            cli_txt = f"Clima (OWM) hubs {hu}/{hv}: +{extra_cli} min orient."

    total_min = min(500, m_max + extra_cli)
    if m_max >= 999:
        total_min = 9999

    motivos = [mot for _, mot in candidatos if mot and "Sin retraso" not in mot]
    if cli_txt:
        motivos.append(cli_txt)

    return {
        "minutos": total_min,
        "motivos": motivos,
        "coste_eur": round(total_min * COSTE_EURO_MINUTO_RETASO, 2) if total_min < 1000 else None,
    }


def bfs_ruta(
    adj: Dict[str, List[str]],
    origen: str,
    destino: str,
    nodos_bloqueados: Set[str],
    aristas_bloqueadas: Set[Tuple[str, str]],
) -> Optional[List[str]]:
    """Camino mínimo en saltos (BFS)."""
    if origen not in adj or destino not in adj:
        return None
    if origen in nodos_bloqueados or destino in nodos_bloqueados:
        return None

    q: deque[Tuple[str, List[str]]] = deque([(origen, [origen])])
    visitados = {origen}

    while q:
        u, camino = q.popleft()
        if u == destino:
            return camino
        for v in adj.get(u, []):
            if v in visitados or v in nodos_bloqueados:
                continue
            if normalizar_arista(u, v) in aristas_bloqueadas:
                continue
            visitados.add(v)
            q.append((v, camino + [v]))
    return None


def enumerar_alternativas(
    adj: Dict[str, List[str]],
    origen: str,
    destino: str,
    ruta_principal: List[str],
    nodos_bloqueados: Set[str],
    aristas_bloqueadas: Set[Tuple[str, str]],
    max_alternativas: int = 8,
) -> List[Tuple[List[str], str]]:
    """
    Para cada tramo de la ruta principal, simula caída del tramo (arista) y busca otra ruta.
    También prueba quitar cada nodo intermedio (excepto origen/destino).
    Devuelve lista de (ruta, motivo_simulacion), ordenada por **menor número de saltos**.
    """
    vistos: Set[Tuple[str, ...]] = set()
    out: List[Tuple[List[str], str]] = []
    principal_t = tuple(ruta_principal)

    def add(p: Optional[List[str]], motivo: str) -> None:
        if not p:
            return
        t = tuple(p)
        if t == principal_t:
            return
        if t in vistos or len(p) > len(ruta_principal) + 8:
            return
        vistos.add(t)
        out.append((p, motivo))

    # Caída por arista en la ruta principal
    for i in range(len(ruta_principal) - 1):
        u, v = ruta_principal[i], ruta_principal[i + 1]
        ab = set(aristas_bloqueadas)
        ab.add(normalizar_arista(u, v))
        alt = bfs_ruta(adj, origen, destino, nodos_bloqueados, ab)
        add(alt, f"Arista caída: {u} — {v} (clima/obras/incidencia)")

    # Caída por nodo intermedio
    for nodo in ruta_principal[1:-1]:
        nb = set(nodos_bloqueados)
        nb.add(nodo)
        alt = bfs_ruta(adj, origen, destino, nb, aristas_bloqueadas)
        add(alt, f"Nodo caído: {nodo}")

    # Ordenar por número de saltos (peso uniforme)
    out.sort(key=lambda x: len(x[0]))
    return out[:max_alternativas]


def construir_bloqueos_clima_obras(
    aplicar_clima_severo: bool,
    aplicar_obras: bool,
    clima_por_hub: Dict[str, Dict[str, Any]],
    nodos_map: Dict[str, Dict[str, Any]],
) -> Tuple[Set[str], Set[Tuple[str, str]], List[str]]:
    """
    Marca nodos/aristas afectados por umbrales OWM (tormenta/nieve fuerte) y por palabra 'obras' en descripción hub.
    """
    nodos_b: Set[str] = set()
    aristas_b: Set[Tuple[str, str]] = set()
    log: List[str] = []
    nodos_cfg = get_nodos()

    if aplicar_clima_severo:
        for hub, raw in clima_por_hub.items():
            wid = raw.get("weather_id")
            desc = (raw.get("descripcion") or "").lower()
            severo = False
            if wid is not None and (200 <= wid <= 232 or 600 <= wid <= 622 or wid in (502, 503, 504)):
                severo = True
            if "tormenta" in desc or "nieve" in desc or "granizo" in desc:
                severo = True
            if severo:
                for nid, info in nodos_cfg.items():
                    if info.get("tipo") == "hub" and nid == hub:
                        nodos_b.add(nid)
                    elif info.get("hub") == hub:
                        nodos_b.add(nid)
                log.append(f"Clima severo en hub **{hub}**: nodos del área marcados como no transitable.")

    if aplicar_obras:
        # Simulación: aristas hacia nodos con nombre que sugieren obras en motivo (si no hay datos, marcar 1 arista de ejemplo Madrid–Toledo)
        for nid, info in nodos_cfg.items():
            if "obras" in (str(info), "").lower():
                nodos_b.add(nid)
        log.append("Modo obras: revisa motivos `obras` en Cassandra; si no hay datos, las alternativas siguen la topología.")

    return nodos_b, aristas_b, log


def analizar_ruta_completa(
    origen: str,
    destino: str,
    aplicar_clima_api: bool,
    aplicar_clima_bloqueo: bool,
    aplicar_obras: bool,
) -> Dict[str, Any]:
    """
    Orquesta carga de datos, BFS, métricas y alternativas.
    """
    adj = construir_adyacencia()
    nodos_rows = cargar_nodos_cassandra()
    aristas_rows = cargar_aristas_cassandra()
    tracking_rows = cargar_tracking_cassandra()
    nodos_map = _dict_nodos_cassandra(nodos_rows)
    aristas_map = _dict_aristas_cassandra(aristas_rows)

    clima_por_hub: Dict[str, Dict[str, Any]] = {}
    if aplicar_clima_api:
        clima_por_hub = obtener_clima_todos_hubs_completo()

    nodos_b, aristas_b, log_escenario = construir_bloqueos_clima_obras(
        aplicar_clima_bloqueo, aplicar_obras, clima_por_hub, nodos_map
    )

    ruta = bfs_ruta(adj, origen, destino, nodos_b, aristas_b)
    if not ruta:
        return {
            "ok": False,
            "error": "No existe ruta con los bloqueos actuales (clima/obras o red desconectada).",
            "log_escenario": log_escenario,
            "alternativas": [],
        }

    pasos: List[Dict[str, Any]] = []
    total_min = 0.0
    total_coste = 0.0
    for i in range(len(ruta) - 1):
        u, v = ruta[i], ruta[i + 1]
        tr = estimar_retraso_tramo(u, v, nodos_map, aristas_map, clima_por_hub if aplicar_clima_api else None, nodos_rows)
        total_min += tr["minutos"] if tr["minutos"] < 1000 else 0
        if tr.get("coste_eur"):
            total_coste += tr["coste_eur"]
        pasos.append(
            {
                "paso": i + 1,
                "desde": u,
                "hasta": v,
                "saltos_acum": i + 1,
                "minutos": tr["minutos"],
                "motivos": tr["motivos"],
                "coste_eur": tr.get("coste_eur"),
            }
        )

    afectados = vehiculos_afectados_por_ruta(ruta, tracking_rows)

    alts = enumerar_alternativas(adj, origen, destino, ruta, nodos_b, aristas_b)

    return {
        "ok": True,
        "ruta": ruta,
        "num_saltos": len(ruta) - 1,
        "pasos": pasos,
        "minutos_totales_estimados": round(total_min, 1),
        "coste_total_eur": round(total_coste, 2),
        "vehiculos_afectados": afectados,
        "alternativas": alts,
        "log_escenario": log_escenario,
        "coste_eur_minuto_config": COSTE_EURO_MINUTO_RETASO,
    }


def vehiculos_afectados_por_ruta(ruta: List[str], tracking_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Camiones cuya ruta sugerida o extremos intersectan nodos de la ruta analizada."""
    en_ruta = set(ruta)
    out: List[Dict[str, Any]] = []
    vistos: Set[str] = set()
    for t in tracking_rows:
        cid = str(t.get("id_camion", ""))
        if cid in vistos:
            continue
        ro, rd = t.get("ruta_origen"), t.get("ruta_destino")
        sug = t.get("ruta_sugerida") or []
        if not isinstance(sug, list):
            sug = []
        hit = False
        if ro in en_ruta or rd in en_ruta:
            hit = True
        for s in sug:
            if s in en_ruta:
                hit = True
                break
        if hit:
            vistos.add(cid)
            out.append(
                {
                    "id_camion": cid,
                    "estado_ruta": t.get("estado_ruta"),
                    "motivo_retraso": t.get("motivo_retraso"),
                    "ruta_origen": ro,
                    "ruta_destino": rd,
                }
            )
    return out


def listar_nodos_ui() -> List[str]:
    """Todos los nodos (hubs + secundarios) para selectores."""
    return sorted(get_nodos().keys())
