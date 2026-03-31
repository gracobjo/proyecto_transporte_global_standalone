#!/usr/bin/env python3
"""
SIMLOG España — Fase I: Ingesta KDD
- Consulta climática Open-Meteo por defecto (5 hubs o todos los nodos según `SIMLOG_CLIMA_TODOS_NODOS`); OpenWeather opcional
- Simulación de incidentes (OK/Congestionado/Bloqueado)
- Interpolación GPS cada 15 min para 5 camiones
- Publicación a Kafka (transporte_status) y backup HDFS
"""
import json
import random
import math
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

# Ruta base del proyecto
BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

import requests
from config_nodos import RED, get_aristas, get_nodos
from config import (
    API_WEATHER_KEY,
    API_WEATHER_BASE,
    WEATHER_PROVIDER,
    KAFKA_BOOTSTRAP,
    TOPIC_DGT_RAW,
    TOPIC_RAW,
    TOPIC_TRANSPORTE,
    HDFS_BACKUP_PATH,
    HDFS_NAMENODE,
    usar_clima_todos_los_nodos,
)
from ingesta.ingesta_dgt_datex2 import fusionar_estados, obtener_incidencias_dgt
from ingesta.trigger_paso import resolver_paso_ingesta_detalle, semilla_simulacion

# Estados posibles
ESTADOS = ["OK", "Congestionado", "Bloqueado"]
MOTIVOS = {
    "OK": None,
    "Congestionado": ["Niebla", "Tráfico", "Lluvia"],
    "Bloqueado": ["Incendio", "Nieve", "Accidente"],
}


def _severity_for_state(estado: str) -> str:
    estado = (estado or "OK").strip().lower()
    if estado == "bloqueado":
        return "high"
    if estado == "congestionado":
        return "medium"
    return "low"


def _env_flag(key: str, default: bool = True) -> bool:
    """
    Interpreta un booleano desde entorno.
    Valores aceptados: 1/0, true/false, yes/no, on/off (case-insensitive).
    """
    v = (os.environ.get(key, "") or "").strip().lower()
    if not v:
        return default
    if v in ("1", "true", "yes", "si", "sí", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return default


def _targets_clima_nodos() -> list[tuple[str, float, float]]:
    """(id_nodo, lat, lon) para hubs solamente o para toda la red según configuración."""
    nodos = get_nodos()
    if not usar_clima_todos_los_nodos():
        return [(h, nodos[h]["lat"], nodos[h]["lon"]) for h in RED["hubs"]]
    return [(nid, d["lat"], d["lon"]) for nid, d in sorted(nodos.items())]


def _consulta_clima_hubs_openmeteo() -> dict:
    """Clima vía Open-Meteo (sin API key): una petición agrupada si hay varios nodos."""
    from servicios.open_meteo_clima import fetch_openmeteo_bulk_slim

    return fetch_openmeteo_bulk_slim(_targets_clima_nodos())


def consulta_clima_hubs(api_key: str | None = None) -> dict:
    """
    Clima actual por coordenadas de nodo (hubs o red completa según `usar_clima_todos_los_nodos()` / `SIMLOG_CLIMA_TODOS_NODOS`).
    Open-Meteo por defecto; OpenWeather si `SIMLOG_WEATHER_PROVIDER=openweather` + clave.
    Si `api_key` viene rellena (p. ej. desde el dashboard), se usa como `appid` en OpenWeather;
    si no, `API_WEATHER_KEY` de config / entorno (solo rama OpenWeather).
    """
    if WEATHER_PROVIDER in ("openmeteo", "open-meteo"):
        return _consulta_clima_hubs_openmeteo()

    appid = (api_key or "").strip() or (API_WEATHER_KEY or "").strip()
    targets = _targets_clima_nodos()
    clima: dict = {}
    if not appid:
        for nid, _, _ in targets:
            clima[nid] = {
                "descripcion": "Sin API key: define API_WEATHER_KEY o pasa api_key a consulta_clima_hubs().",
                "temp": None,
                "humedad": None,
                "viento": None,
                "source": "openweather",
            }
        return clima
    for nid, lat, lon in targets:
        try:
            r = requests.get(
                API_WEATHER_BASE,
                params={
                    "lat": lat,
                    "lon": lon,
                    "appid": appid,
                    "units": "metric",
                    "lang": "es",
                },
                timeout=10,
            )
            if r.status_code == 200:
                d = r.json()
                clima[nid] = {
                    "descripcion": d.get("weather", [{}])[0].get("description", "N/A"),
                    "temp": d.get("main", {}).get("temp"),
                    "humedad": d.get("main", {}).get("humidity"),
                    "viento": d.get("wind", {}).get("speed"),
                    "source": "openweather",
                }
            else:
                msg = "HTTP " + str(r.status_code)
                try:
                    err_j = r.json()
                    if isinstance(err_j, dict) and err_j.get("message"):
                        msg += f": {err_j['message']}"
                except Exception:
                    pass
                clima[nid] = {
                    "descripcion": msg,
                    "temp": None,
                    "humedad": None,
                    "viento": None,
                    "source": "openweather",
                }
        except Exception as e:
            clima[nid] = {
                "descripcion": f"Error: {e}",
                "temp": None,
                "humedad": None,
                "viento": None,
                "source": "openweather",
            }
    return clima


def _clima_primario_valido(item: dict | None) -> bool:
    if not isinstance(item, dict):
        return False
    desc = str(item.get("descripcion") or "").strip().lower()
    if desc.startswith("http ") or desc.startswith("error:") or "sin api key" in desc:
        return False
    return any(item.get(k) is not None for k in ("temp", "humedad", "viento", "visibilidad"))


def combinar_clima_hubs(clima_primario: dict, clima_dgt: dict) -> dict:
    clima_primario = clima_primario or {}
    clima_dgt = clima_dgt or {}
    combined: dict = {}
    hub_ids = sorted(set(clima_primario.keys()) | set(clima_dgt.keys()))

    for hub in hub_ids:
        prim = dict(clima_primario.get(hub) or {})
        dgt = dict(clima_dgt.get(hub) or {})
        if _clima_primario_valido(prim):
            src = prim.get("source") or "openmeteo"
            if src not in ("openweather", "openmeteo"):
                src = "openmeteo"
            merged = {
                **prim,
                "source": src,
                "fallback_activo": False,
            }
            if dgt:
                merged["estado_carretera"] = dgt.get("estado_carretera") or merged.get("estado_carretera")
                merged["visibilidad"] = dgt.get("visibilidad", merged.get("visibilidad"))
                merged["condiciones_meteorologicas"] = list(dgt.get("condiciones_meteorologicas") or [])
                merged["dgt_weather_backup"] = True
            combined[hub] = merged
            continue

        if dgt:
            combined[hub] = {
                **dgt,
                "source": "dgt",
                "fallback_activo": True,
                "clima_api_error": prim.get("descripcion") if prim else None,
            }
            continue

        combined[hub] = {
            **prim,
            "source": "openmeteo_unavailable",
            "fallback_activo": False,
        }
    return combined


def simular_incidentes_nodos() -> dict:
    """Estados aleatorios para nodos: OK, Congestionado, Bloqueado."""
    nodos = get_nodos()
    estados_nodos = {}
    for nid, datos in nodos.items():
        estado = random.choices(ESTADOS, weights=[0.7, 0.2, 0.1])[0]
        motivo = random.choice(MOTIVOS[estado]) if MOTIVOS[estado] else None
        estados_nodos[nid] = {
            "estado": estado,
            "motivo": motivo,
            "source": "simulacion",
            "severity": _severity_for_state(estado),
            "peso_pagerank": 1.0,
        }
    return estados_nodos


def simular_incidentes_aristas() -> dict:
    """Estados aleatorios para aristas."""
    aristas = get_aristas()
    estados_aristas = {}
    for src, dst, dist in aristas:
        key = f"{src}|{dst}"
        estado = random.choices(ESTADOS, weights=[0.75, 0.15, 0.1])[0]
        motivo = random.choice(MOTIVOS[estado]) if MOTIVOS[estado] else None
        estados_aristas[key] = {
            "estado": estado,
            "motivo": motivo,
            "distancia_km": dist,
            "source": "simulacion",
            "severity": _severity_for_state(estado),
        }
    return estados_aristas


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def interpolar_gps(lat1, lon1, lat2, lon2, factor):
    """Interpolar posición entre dos puntos. factor in [0,1]."""
    lat = lat1 + factor * (lat2 - lat1)
    lon = lon1 + factor * (lon2 - lon1)
    return round(lat, 6), round(lon, 6)


def generar_rutas_camiones(n=5):
    """Generar 5 rutas aleatorias para camiones (secuencia de nodos)."""
    aristas = get_aristas()
    nodos = get_nodos()
    rutas = []
    # Crear grafo simplificado para encontrar rutas
    vecinos = {}
    for src, dst, _ in aristas:
        vecinos.setdefault(src, []).append(dst)
        vecinos.setdefault(dst, []).append(src)

    todos = list(nodos.keys())
    for _ in range(n):
        orig = random.choice(todos)
        dest = random.choice([x for x in todos if x != orig])
        # BFS simple para ruta (máx 6 saltos)
        from collections import deque
        seen = {orig}
        q = deque([(orig, [orig])])
        ruta = [orig]
        while q:
            u, path = q.popleft()
            if u == dest:
                ruta = path
                break
            for v in vecinos.get(u, []):
                if v not in seen and len(path) < 6:
                    seen.add(v)
                    q.append((v, path + [v]))
        rutas.append(ruta)
    return rutas


def distancia_total_ruta_km(ruta):
    """Suma la distancia total de una ruta usando las aristas conocidas."""
    if not ruta or len(ruta) < 2:
        return 0.0
    nodos = get_nodos()
    distancias = {}
    for src, dst, dist in get_aristas():
        distancias[(src, dst)] = float(dist)
        distancias[(dst, src)] = float(dist)
    total = 0.0
    for src, dst in zip(ruta, ruta[1:]):
        tramo = distancias.get((src, dst))
        if tramo is None:
            n1 = nodos.get(src, {"lat": 40.4, "lon": -3.7})
            n2 = nodos.get(dst, {"lat": 40.4, "lon": -3.7})
            tramo = haversine_km(n1["lat"], n1["lon"], n2["lat"], n2["lon"])
        total += float(tramo)
    return round(total, 2)


def clima_hubs_a_lista(clima_hubs: dict, timestamp: str) -> list[dict]:
    """Convierte el mapa por hub en una lista homogénea para persistencia Hive."""
    out = []
    for hub, datos in (clima_hubs or {}).items():
        if not isinstance(datos, dict):
            continue
        out.append(
            {
                "ciudad": hub,
                "temperatura": datos.get("temp"),
                "humedad": datos.get("humedad"),
                "descripcion": datos.get("descripcion", ""),
                "visibilidad": datos.get("visibilidad"),
                "viento": datos.get("viento"),
                "source": datos.get("source"),
                "fallback_activo": datos.get("fallback_activo", False),
                "estado_carretera": datos.get("estado_carretera"),
                "timestamp": timestamp,
            }
        )
    return out


def interpolacion_gps_15min(rutas, paso_15min=0):
    """
    Calcular posición exacta (lat, lon) para cada camión cada 15 min.
    paso_15min: 0-3 indica el cuarto de hora (0=inicio, 3=fin del tramo).
    """
    nodos = get_nodos()
    posiciones = []
    for i, ruta in enumerate(rutas):
        if len(ruta) < 2:
            nodo = ruta[0] if ruta else list(nodos.keys())[0]
            d = nodos.get(nodo, {"lat": 40.4, "lon": -3.7})
            distancia_total = distancia_total_ruta_km(ruta)
            posiciones.append({
                "id_camion": f"camion_{i+1}",
                "id": f"camion_{i+1}",
                "lat": d["lat"],
                "lon": d["lon"],
                "posicion_actual": {"lat": d["lat"], "lon": d["lon"]},
                "ruta": ruta,
                "ruta_sugerida": ruta,
                "ruta_origen": ruta[0] if ruta else nodo,
                "ruta_destino": ruta[-1] if ruta else nodo,
                "indice_tramo": 0,
                "origen_tramo": nodo,
                "destino_tramo": nodo,
                "progreso": 0.0,
                "progreso_pct": 0.0,
                "distancia_total_km": distancia_total,
                "nodo_actual": nodo,
                "estado_ruta": "Sin movimiento",
                "motivo_retraso": "",
            })
            continue
        # Asumimos 4 pasos de 15 min por tramo (1 hora por arista)
        total_tramos = len(ruta) - 1
        paso_global = paso_15min % (total_tramos * 4)
        tramo = min(paso_global // 4, total_tramos - 1)
        progreso_tramo = (paso_global % 4) / 4.0
        n1, n2 = ruta[tramo], ruta[tramo + 1]
        d1 = nodos.get(n1, {"lat": 40.4, "lon": -3.7})
        d2 = nodos.get(n2, {"lat": 41.4, "lon": 2.17})
        lat, lon = interpolar_gps(d1["lat"], d1["lon"], d2["lat"], d2["lon"], progreso_tramo)
        progreso_global = ((tramo + progreso_tramo) / max(total_tramos, 1)) * 100.0
        distancia_total = distancia_total_ruta_km(ruta)
        nodo_actual = n1 if progreso_tramo < 0.5 else n2
        posiciones.append({
            "id_camion": f"camion_{i+1}",
            "id": f"camion_{i+1}",
            "lat": lat,
            "lon": lon,
            "posicion_actual": {"lat": lat, "lon": lon},
            "ruta": ruta,
            "ruta_sugerida": ruta,
            "ruta_origen": ruta[0],
            "ruta_destino": ruta[-1],
            "indice_tramo": tramo,
            "origen_tramo": n1,
            "destino_tramo": n2,
            "progreso": progreso_tramo,
            "progreso_pct": round(progreso_global, 2),
            "distancia_total_km": distancia_total,
            "nodo_actual": nodo_actual,
            "estado_ruta": "En ruta",
            "motivo_retraso": "",
        })
    return posiciones


def publicar_kafka(payload: dict) -> bool:
    """Enviar JSON del snapshot a topics raw/filtered y, si aplica, al raw DGT."""
    try:
        from kafka import KafkaProducer

        # Importante: sin timeouts explícitos, `flush()` puede bloquear indefinidamente
        # si el broker no responde (y Streamlit/Airflow "parecen colgados").
        producer = KafkaProducer(
            bootstrap_servers=[s.strip() for s in KAFKA_BOOTSTRAP.split(",") if s.strip()],
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            request_timeout_ms=10_000,
            api_version_auto_timeout_ms=10_000,
            max_block_ms=10_000,
            retries=0,
            acks=1,
        )

        producer.send(TOPIC_RAW, value=payload).get(timeout=10)
        producer.send(TOPIC_TRANSPORTE, value=payload).get(timeout=10)
        if payload.get("incidencias_dgt"):
            producer.send(
                TOPIC_DGT_RAW,
                value={
                    "timestamp": payload.get("timestamp"),
                    "origen_dgt": payload.get("resumen_dgt", {}).get("source_mode"),
                    "incidencias_dgt": payload.get("incidencias_dgt", []),
                },
            ).get(timeout=10)

        producer.flush(timeout=10)
        producer.close(timeout=5)
        return True
    except Exception as e:
        print(f"[KAFKA] Error: {e}")
        return False


def _hdfs_cmd():
    """Ruta al ejecutable hdfs (HADOOP_HOME/bin/hdfs o 'hdfs' del PATH)."""
    hadoop_home = os.environ.get("HADOOP_HOME")
    if hadoop_home:
        path = os.path.join(hadoop_home, "bin", "hdfs")
        if os.path.isfile(path) or os.path.isfile(path + ".cmd"):
            return path
    return "hdfs"


def _webhdfs_base_url() -> str:
    """
    Base URL de WebHDFS para el NameNode.
    - Por defecto: http://<host>:9870 (Hadoop 3)
    - Override: SIMLOG_WEBHDFS_URL (p.ej. http://namenode:9870)
    """
    override = (os.environ.get("SIMLOG_WEBHDFS_URL", "") or "").strip()
    if override:
        return override.rstrip("/")
    # HDFS_NAMENODE suele venir como host:9000 (RPC). Reutilizamos host y asumimos 9870 (WebHDFS).
    host = str(HDFS_NAMENODE or "127.0.0.1:9000").split(":", 1)[0].strip() or "127.0.0.1"
    port = int(os.environ.get("SIMLOG_WEBHDFS_PORT", "9870"))
    return f"http://{host}:{port}"


def _guardar_hdfs_via_webhdfs(payload: dict, path_hdfs: str) -> bool:
    """
    Fallback para entornos donde `hdfs dfs` se queda colgado por PATH/HADOOP_CONF_DIR.
    Usa WebHDFS (HTTP) contra el NameNode. Requiere que el NameNode exponga WebHDFS.
    """
    try:
        base_url = _webhdfs_base_url()
        user = (os.environ.get("HADOOP_USER_NAME") or os.environ.get("USER") or "hadoop").strip() or "hadoop"
        timeout = float(os.environ.get("SIMLOG_WEBHDFS_TIMEOUT_SEC", "20"))
        # `requests` respeta proxies del entorno; en setups locales puede colgarse intentando proxy.
        # Para WebHDFS en localhost/namenode interno, desactivamos proxies explícitamente.
        no_proxies = {"http": "", "https": ""}

        # MKDIRS del directorio destino
        dir_path = "/".join(path_hdfs.split("/")[:-1]) or "/"
        r1 = requests.put(
            f"{base_url}/webhdfs/v1{dir_path}",
            params={"op": "MKDIRS", "user.name": user},
            proxies=no_proxies,
            timeout=(5, timeout),
        )
        if r1.status_code not in (200, 201):
            msg = (r1.text or "").strip()
            print(f"[HDFS] WebHDFS MKDIRS HTTP {r1.status_code}: {msg[:280]}")
            return False

        data = json.dumps(payload, ensure_ascii=False, default=str, indent=2).encode("utf-8")
        # CREATE con redirects (NameNode responde 307 a DataNode).
        r2 = requests.put(
            f"{base_url}/webhdfs/v1{path_hdfs}",
            params={"op": "CREATE", "overwrite": "true", "user.name": user},
            data=data,
            headers={"Content-Type": "application/json; charset=utf-8"},
            allow_redirects=True,
            proxies=no_proxies,
            timeout=(5, max(timeout, 30.0)),
        )
        if r2.status_code not in (200, 201):
            msg = (r2.text or "").strip()
            print(f"[HDFS] WebHDFS CREATE HTTP {r2.status_code}: {msg[:280]}")
            return False
        return True
    except Exception as e:
        print(f"[HDFS] WebHDFS Error: {e}")
        return False


def guardar_hdfs(payload: dict) -> bool:
    """Guardar backup en HDFS."""
    try:
        import subprocess
        hdfs_bin = _hdfs_cmd()
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        archivo = f"/tmp/transporte_{ts}.json"
        with open(archivo, "w") as f:
            json.dump(payload, f, default=str, indent=2)
        path_hdfs = f"{HDFS_BACKUP_PATH}/transporte_{ts}.json"
        r1 = subprocess.run(
            [hdfs_bin, "dfs", "-mkdir", "-p", HDFS_BACKUP_PATH],
            capture_output=True,
            text=True,
            timeout=20,
        )
        if r1.returncode != 0:
            err = (r1.stderr or r1.stdout or "").strip()
            print(f"[HDFS] Error mkdir: {err[:280]}")
            try:
                os.remove(archivo)
            except OSError:
                pass
            # Fallback WebHDFS (si está habilitado)
            if _env_flag("SIMLOG_WEBHDFS_FALLBACK", True):
                return _guardar_hdfs_via_webhdfs(payload, path_hdfs)
            return False
        r2 = subprocess.run(
            [hdfs_bin, "dfs", "-put", "-f", archivo, path_hdfs],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if r2.returncode != 0:
            err = (r2.stderr or r2.stdout or "").strip()
            print(f"[HDFS] Error put: {err[:280]}")
            try:
                os.remove(archivo)
            except OSError:
                pass
            if _env_flag("SIMLOG_WEBHDFS_FALLBACK", True):
                return _guardar_hdfs_via_webhdfs(payload, path_hdfs)
            return False
        os.remove(archivo)
        return True
    except Exception as e:
        # Timeout/errores del CLI: intentar WebHDFS si procede.
        print(f"[HDFS] Error: {e}")
        try:
            if _env_flag("SIMLOG_WEBHDFS_FALLBACK", True):
                ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                path_hdfs = f"{HDFS_BACKUP_PATH}/transporte_{ts}.json"
                return _guardar_hdfs_via_webhdfs(payload, path_hdfs)
        except Exception:
            pass
        return False


def evaluar_alerta_bloqueos(estados_nodos: dict) -> dict:
    bloqueados = [nid for nid, info in (estados_nodos or {}).items() if (info or {}).get("estado") == "Bloqueado"]
    total = len(estados_nodos or {})
    ratio = round((len(bloqueados) / total), 3) if total else 0.0
    if ratio >= 0.20 or len(bloqueados) >= 5:
        nivel = "critica"
    elif len(bloqueados) >= 3:
        nivel = "alta"
    else:
        nivel = "normal"
    return {
        "tipo_alerta": "bloqueo_red",
        "nivel": nivel,
        "bloqueados": len(bloqueados),
        "ratio_bloqueados": ratio,
        "nodos_bloqueados": bloqueados,
    }


def _resolver_paso_15min_modo(paso_15min: int, paso_15min_modo: str | None) -> str:
    """Si no viene explícito, infiere desde PASO_15MIN en el entorno (subprocesos Streamlit/Airflow)."""
    if paso_15min_modo is not None:
        return paso_15min_modo
    em = os.environ.get("PASO_15MIN")
    if em is not None and str(em).strip() != "":
        return "manual"
    return "auto"


def main(paso_15min=0, paso_15min_modo: str | None = None):
    # Misma ventana temporal → misma semilla (reproducible); ventana distinta → incidentes/rutas distintos
    paso_15min_modo = _resolver_paso_15min_modo(paso_15min, paso_15min_modo)
    random.seed(semilla_simulacion(paso_15min))
    canal_ingesta = os.environ.get("SIMLOG_INGESTA_CANAL", "script_python").strip() or "script_python"
    origen_ingesta = os.environ.get("SIMLOG_INGESTA_ORIGEN", "cli_script").strip() or "cli_script"
    ejecutor_ingesta = os.environ.get("SIMLOG_INGESTA_EJECUTOR", "python -m ingesta.ingesta_kdd").strip()

    clima_primario = consulta_clima_hubs()
    sim_incid = _env_flag("SIMLOG_SIMULAR_INCIDENCIAS", True)
    use_dgt = _env_flag("SIMLOG_USE_DGT", True)
    dgt_cache_only = _env_flag("SIMLOG_DGT_ONLY_CACHE", False)
    if sim_incid:
        estados_nodos = simular_incidentes_nodos()
        estados_aristas = simular_incidentes_aristas()
    else:
        # Incidencias desactivadas: estado operativo estable (todo OK).
        nodos = get_nodos()
        estados_nodos = {nid: {"estado": "OK", "motivo": None} for nid in nodos.keys()}
        estados_aristas = {
            f"{src}|{dst}": {"estado": "OK", "motivo": None, "distancia_km": dist}
            for (src, dst, dist) in get_aristas()
        }

    info_dgt = {"source_mode": "disabled", "error": None, "incidencias": [], "mapeo_nodos": {}}
    if use_dgt:
        info_dgt = obtener_incidencias_dgt(use_cache_only=dgt_cache_only, allow_cache_fallback=True)
        if info_dgt.get("mapeo_nodos"):
            estados_nodos = fusionar_estados(estados_nodos, info_dgt["mapeo_nodos"])

    rutas = generar_rutas_camiones(5)
    posiciones = interpolacion_gps_15min(rutas, paso_15min)
    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    clima = combinar_clima_hubs(clima_primario, info_dgt.get("clima_hubs", {}))
    clima_lista = clima_hubs_a_lista(clima, timestamp)
    alerta_bloqueos = evaluar_alerta_bloqueos(estados_nodos)

    payload = {
        "origen": origen_ingesta,
        "canal_ingesta": canal_ingesta,
        "ejecutor_ingesta": ejecutor_ingesta,
        "timestamp": timestamp,
        "paso_15min": paso_15min,
        "paso_15min_modo": paso_15min_modo,
        "simulacion_incidencias": sim_incid,
        "dgt_habilitado": use_dgt,
        "clima_hubs": clima,
        "clima": clima_lista,
        "incidencias_dgt": info_dgt.get("incidencias", []),
        "resumen_dgt": {
            "source_mode": info_dgt.get("source_mode", "disabled"),
            "error": info_dgt.get("error"),
            "incidencias_totales": len(info_dgt.get("incidencias", [])),
            "nodos_afectados": len(info_dgt.get("mapeo_nodos", {})),
            "hubs_clima_respaldo": len(info_dgt.get("clima_hubs", {})),
        },
        "eventos_grafo": [],
        "alertas_operativas": [alerta_bloqueos],
        "nodos_estado": {
            n: {
                "estado": v["estado"],
                "motivo": v.get("motivo"),
                "source": v.get("source", "simulacion"),
                "severity": v.get("severity", _severity_for_state(v["estado"])),
                "peso_pagerank": v.get("peso_pagerank", 1.0),
                "id_incidencia": v.get("id_incidencia"),
                "carretera": v.get("carretera"),
                "municipio": v.get("municipio"),
                "provincia": v.get("provincia"),
                "descripcion": v.get("descripcion"),
            }
            for n, v in estados_nodos.items()
        },
        "estados_nodos": {
            n: {
                "estado": v["estado"],
                "motivo": v.get("motivo"),
                "source": v.get("source", "simulacion"),
                "severity": v.get("severity", _severity_for_state(v["estado"])),
                "peso_pagerank": v.get("peso_pagerank", 1.0),
                "id_incidencia": v.get("id_incidencia"),
                "carretera": v.get("carretera"),
                "municipio": v.get("municipio"),
                "provincia": v.get("provincia"),
                "descripcion": v.get("descripcion"),
            }
            for n, v in estados_nodos.items()
        },
        "aristas_estado": estados_aristas,
        "estados_aristas": estados_aristas,
        "camiones": posiciones,
    }

    ok_kafka = publicar_kafka(payload)
    ok_hdfs = guardar_hdfs(payload)

    # Copia local para el dashboard (Spark / fase_kdd_spark leen `ultimo_payload.json`)
    work = BASE / "reports" / "kdd" / "work"
    work.mkdir(parents=True, exist_ok=True)
    try:
        (work / "ultimo_payload.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        meta = {
            "origen": origen_ingesta,
            "canal_ingesta": canal_ingesta,
            "ejecutor_ingesta": ejecutor_ingesta,
            "timestamp": payload["timestamp"],
            "paso_15min": paso_15min,
            "paso_15min_modo": paso_15min_modo,
            "dgt_source_mode": payload["resumen_dgt"]["source_mode"],
            "dgt_incidencias_totales": payload["resumen_dgt"]["incidencias_totales"],
            "dgt_nodos_afectados": payload["resumen_dgt"]["nodos_afectados"],
            "ok_kafka": ok_kafka,
            "ok_hdfs": ok_hdfs,
        }
        (work / "ultima_ingesta_meta.json").write_text(
            json.dumps(meta, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except OSError as e:
        print(f"[INGESTA] Aviso: no se pudo escribir reports/kdd/work: {e}")

    print(f"[INGESTA] Paso {paso_15min} | Kafka: {ok_kafka} | HDFS: {ok_hdfs}")
    return payload


if __name__ == "__main__":
    # Sin PASO_15MIN en el entorno → índice automático según reloj e intervalo (ver trigger_paso.py)
    _paso, _modo = resolver_paso_ingesta_detalle()
    main(paso_15min=_paso, paso_15min_modo=_modo)
