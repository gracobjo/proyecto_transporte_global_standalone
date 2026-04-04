#!/usr/bin/env python3
"""
Recrea o actualiza el grupo NiFi «PG_SIMLOG_KDD».

Los tres **ExecuteScript** (Groovy) quedan con **Script File** apuntando a
``<raíz-del-repo>/nifi/groovy/*.groovy`` y **Script Body** vacío para que NiFi
cargue siempre el fichero del disco (un cuerpo pegado en la UI tiene prioridad
y deja de aplicar los cambios del repo).

Para solo volcar rutas/scripts tras editar un ``.groovy`` sin tocar el resto del
canvas: ``python scripts/sync_nifi_groovy_scripts.py``.
"""
from __future__ import annotations

import copy
import json
import os
import socket
import time
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE = Path(__file__).resolve().parent.parent
import sys

sys.path.insert(0, str(BASE))
from config import OPEN_METEO_MULTI_HUBS_URL
NIFI_API = os.environ.get("SIMLOG_NIFI_API", "https://localhost:8443/nifi-api").rstrip("/")
NIFI_USER = os.environ.get("SIMLOG_NIFI_USER", "nifi")
NIFI_PASSWORD = os.environ.get("SIMLOG_NIFI_PASSWORD", "nifinifinifi")
# `wait_for_nifi_api_ready()` asigna la base que responde (p. ej. HTTP:8080 si HTTPS está desactivado).
_NIFI_API_RESOLVED: str | None = None


def effective_nifi_api() -> str:
    return (_NIFI_API_RESOLVED or NIFI_API).rstrip("/")


def cargar_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


ENV = cargar_env(BASE / ".env")

# Procesadores ExecuteScript (Groovy) del flujo SIMLOG: (nombre en canvas, fichero, x, y).
NIFI_PG_EXECUTE_SCRIPTS: tuple[tuple[str, str, float, float], ...] = (
    ("Build_GPS_Sintetico", "GenerateSyntheticGpsForPractice.groovy", 700.0, 180.0),
    ("Merge_Weather_Into_Payload", "MergeOpenWeatherIntoPayload.groovy", 1380.0, 180.0),
    ("Merge_DGT_Into_Payload", "MergeDgtDatex2IntoPayload.groovy", 2060.0, 180.0),
)


def assert_nifi_groovy_sources_exist(scripts_root: Path) -> None:
    """Falla con mensaje claro si falta algún .groovy que usa el flujo."""
    missing: list[str] = []
    for _name, filename, _x, _y in NIFI_PG_EXECUTE_SCRIPTS:
        p = scripts_root / "groovy" / filename
        if not p.is_file():
            missing.append(str(p.resolve()))
    if missing:
        raise FileNotFoundError(
            "Faltan scripts Groovy para NiFi (¿rama o checkout incompleto?):\n  " + "\n  ".join(missing)
        )


def get_processor_entity_by_name(client: NifiClient, pg_id: str, name: str) -> dict[str, Any] | None:
    flow = client.get(f"/flow/process-groups/{pg_id}")
    for p in flow["processGroupFlow"]["flow"].get("processors", []):
        if p["component"]["name"] == name:
            return p
    return None


def create_or_update_execute_script_processors(
    client: NifiClient, pg_id: str, scripts_root: Path
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Crea si no existen y actualiza Script File + Script Body vacío para los tres Groovy."""
    out: list[dict[str, Any]] = []
    for proc_name, filename, x, y in NIFI_PG_EXECUTE_SCRIPTS:
        p = create_processor(
            client,
            pg_id,
            proc_name,
            "org.apache.nifi.processors.script.ExecuteScript",
            x,
            y,
        )
        groovy_path = (scripts_root / "groovy" / filename).resolve()
        p = update_processor(
            client,
            p,
            properties={
                "Script Engine": "Groovy",
                "Script File": str(groovy_path),
                "Script Body": "",
            },
            concurrent_tasks="2",
            auto_terminated=[],
            position=(x, y),
        )
        out.append(p)
    return out[0], out[1], out[2]


def _nifi_properties_api_bases() -> list[str]:
    """Lee host/puerto HTTP y HTTPS desde conf/nifi.properties (prioridad)."""
    nh = (os.environ.get("SIMLOG_NIFI_HOME") or os.environ.get("NIFI_HOME") or "").strip()
    if not nh:
        return []
    props_path = Path(nh).expanduser() / "conf" / "nifi.properties"
    if not props_path.is_file():
        return []
    props: dict[str, str] = {}
    try:
        for line in props_path.read_text(encoding="utf-8", errors="replace").splitlines():
            ls = line.strip()
            if not ls or ls.startswith("#") or "=" not in ls:
                continue
            k, v = ls.split("=", 1)
            props[k.strip()] = v.strip()
    except OSError:
        return []
    out: list[str] = []
    https_port = props.get("nifi.web.https.port", "").strip()
    http_port = props.get("nifi.web.http.port", "").strip()
    https_h = (props.get("nifi.web.https.host") or "127.0.0.1").strip() or "127.0.0.1"
    http_h = (props.get("nifi.web.http.host") or "127.0.0.1").strip() or "127.0.0.1"
    if https_port and https_port not in ("0", ""):
        out.append(f"https://{https_h}:{https_port}/nifi-api")
    if http_port and http_port not in ("0", ""):
        out.append(f"http://{http_h}:{http_port}/nifi-api")
    return out


def _nifi_api_candidate_bases() -> list[str]:
    seen: set[str] = set()
    out: list[str] = []

    def add(u: str) -> None:
        u = u.strip().rstrip("/")
        if u and u not in seen:
            seen.add(u)
            out.append(u)

    for u in _nifi_properties_api_bases():
        add(u)
    add(os.environ.get("SIMLOG_NIFI_API", "https://localhost:8443/nifi-api").rstrip("/"))
    for piece in (os.environ.get("SIMLOG_NIFI_API_ALT", "") or "").replace(",", ";").split(";"):
        if piece.strip():
            add(piece.strip())
    primary = NIFI_API
    if "localhost" in primary:
        add(primary.replace("localhost", "127.0.0.1"))
    if "127.0.0.1" in primary:
        add(primary.replace("127.0.0.1", "localhost"))
    # Tarballs con solo HTTP en 8080 (HTTPS deshabilitado o otro puerto)
    if (os.environ.get("SIMLOG_NIFI_TRY_HTTP", "1") or "").strip().lower() not in ("0", "false", "no", "off"):
        add("http://127.0.0.1:8080/nifi-api")
        add("http://localhost:8080/nifi-api")
    return out


def _nifi_failure_diagnostics() -> str:
    nh = (os.environ.get("SIMLOG_NIFI_HOME") or os.environ.get("NIFI_HOME") or "").strip()
    lines = [
        "Diagnóstico sugerido:",
        f"  Bases probadas: {', '.join(_nifi_api_candidate_bases()[:6])}{'…' if len(_nifi_api_candidate_bases()) > 6 else ''}",
    ]
    if nh:
        lines.append(f"  {nh}/bin/nifi.sh status")
        lines.append(f"  tail -80 {nh}/logs/nifi-app.log")
    lines.append("  pgrep -af org.apache.nifi.NiFi || true")
    lines.append(
        "  Si `restart` solo paró el JVM: ejecuta manualmente "
        f"`{nh or '$NIFI_HOME'}/bin/nifi.sh start` y espera a ver 'Started' en el log."
    )
    return "\n".join(lines)


def _probe_nifi_base(api_base: str) -> tuple[bool, str]:
    """TCP + GET /flow/status. Devuelve (ok, mensaje_error)."""
    api_base = api_base.rstrip("/")
    parsed = urlparse(api_base)
    host = parsed.hostname or "127.0.0.1"
    scheme = (parsed.scheme or "https").lower()
    port = parsed.port
    if port is None:
        port = 443 if scheme == "https" else 80
    try:
        with socket.create_connection((host, port), timeout=5):
            pass
    except OSError as e:
        return False, str(e)
    try:
        r = requests.get(f"{api_base}/flow/status", verify=False, timeout=15)
        if r.status_code in (200, 401, 403, 404):
            return True, ""
        return False, f"HTTP {r.status_code}"
    except requests.exceptions.RequestException as e:
        return False, str(e)


def wait_for_nifi_api_ready() -> None:
    """
    Tras `nifi.sh restart`, el JVM puede tardar mucho; además la API puede estar solo en HTTP:8080
    u otro host según `nifi.properties`.

    - `SIMLOG_NIFI_WAIT_SEC` (default 180); `0` omite la espera.
    - `SIMLOG_NIFI_HOME` / `NIFI_HOME`: lee `conf/nifi.properties` para puertos reales.
    - `SIMLOG_NIFI_TRY_HTTP=0` para no probar http://127.0.0.1:8080/nifi-api
    """
    global _NIFI_API_RESOLVED
    flag = (os.environ.get("SIMLOG_NIFI_WAIT_SEC", "") or "180").strip().lower()
    if flag in ("0", "false", "no", "off"):
        return
    try:
        timeout = float(flag)
    except ValueError:
        timeout = 180.0
    deadline = time.time() + max(5.0, timeout)
    interval = float(os.environ.get("SIMLOG_NIFI_WAIT_INTERVAL_SEC", "2") or "2")
    bases = _nifi_api_candidate_bases()
    last_err = ""
    attempt = 0
    print(
        f"[NiFi] Esperando hasta {int(timeout)}s; candidatos: {', '.join(bases[:4])}"
        f"{'…' if len(bases) > 4 else ''}",
        flush=True,
    )
    while time.time() < deadline:
        attempt += 1
        for api in bases:
            ok, err = _probe_nifi_base(api)
            if ok:
                _NIFI_API_RESOLVED = api
                print(f"[NiFi] API en {api} — obteniendo token…", flush=True)
                return
            last_err = f"{api}: {err}"
        if attempt == 1 or attempt % 6 == 0:
            print(f"[NiFi]   …reintentando ({last_err})", flush=True)
        time.sleep(interval)
    raise RuntimeError(
        f"NiFi no respondió en {int(timeout)}s. Último intento: {last_err}\n{_nifi_failure_diagnostics()}"
    )


class NifiClient:
    def __init__(self) -> None:
        self.s = requests.Session()
        self.s.verify = False
        base = effective_nifi_api()
        r = self.s.post(
            f"{base}/access/token",
            data={"username": NIFI_USER, "password": NIFI_PASSWORD},
            timeout=20,
        )
        r.raise_for_status()
        self.headers = {"Authorization": f"Bearer {r.text}"}

    def get(self, path: str) -> dict[str, Any]:
        r = self.s.get(f"{effective_nifi_api()}{path}", headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        r = self.s.post(f"{effective_nifi_api()}{path}", headers=self.headers, json=payload, timeout=30)
        r.raise_for_status()
        return r.json()

    def put(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        r = self.s.put(f"{effective_nifi_api()}{path}", headers=self.headers, json=payload, timeout=30)
        if not r.ok:
            body = (r.text or "")[:4000]
            raise requests.HTTPError(
                f"{r.status_code} Client Error for {path}: {body}",
                response=r,
            )
        return r.json()

    def delete(self, path: str, *, params: dict[str, Any] | None = None) -> None:
        r = self.s.delete(f"{effective_nifi_api()}{path}", headers=self.headers, params=params, timeout=30)
        r.raise_for_status()


def component_revision(entity: dict[str, Any]) -> dict[str, Any]:
    """
    NiFi rechaza a menudo `clientId: null` en JSON; si no hay clientId, se omite la clave.
    """
    rev = entity["revision"]
    out: dict[str, Any] = {"version": rev["version"]}
    cid = rev.get("clientId")
    if cid is not None and str(cid).strip() != "":
        out["clientId"] = str(cid)
    return out


def create_pg(client: NifiClient, root_id: str, name: str) -> dict[str, Any]:
    existing = client.get("/process-groups/root/process-groups").get("processGroups", [])
    for pg in existing:
        comp = pg["component"]
        if comp["name"] == name:
            return pg
    payload = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "position": {"x": 200.0, "y": 200.0},
        },
    }
    return client.post(f"/process-groups/{root_id}/process-groups", payload)


def create_processor(client: NifiClient, pg_id: str, name: str, proc_type: str, x: float, y: float) -> dict[str, Any]:
    flow = client.get(f"/flow/process-groups/{pg_id}")
    for p in flow["processGroupFlow"]["flow"].get("processors", []):
        if p["component"]["name"] == name:
            return p
    payload = {
        "revision": {"version": 0},
        "component": {
            "type": proc_type,
            "name": name,
            "position": {"x": x, "y": y},
        },
    }
    return client.post(f"/process-groups/{pg_id}/processors", payload)


def create_controller_service(
    client: NifiClient,
    pg_id: str,
    name: str,
    service_type: str,
    x: float,
    y: float,
) -> dict[str, Any]:
    services = client.get(f"/flow/process-groups/{pg_id}/controller-services").get("controllerServices", [])
    for svc in services:
        comp = svc["component"]
        if comp["name"] == name:
            return svc
    payload = {
        "revision": {"version": 0},
        "component": {
            "type": service_type,
            "name": name,
            "position": {"x": x, "y": y},
        },
    }
    return client.post(f"/process-groups/{pg_id}/controller-services", payload)


def get_controller_service(client: NifiClient, service_id: str) -> dict[str, Any]:
    return client.get(f"/controller-services/{service_id}")


def set_processor_run_state(client: NifiClient, entity: dict[str, Any], state: str) -> dict[str, Any]:
    """RUNNING, STOPPED, DISABLED, … Ver API `PUT /processors/{{id}}/run-status`."""
    comp_id = entity["component"]["id"]
    payload = {
        "revision": component_revision(entity),
        "state": state,
    }
    return client.put(f"/processors/{comp_id}/run-status", payload)


def list_processor_ids_in_group(client: NifiClient, pg_id: str) -> list[str]:
    flow = client.get(f"/flow/process-groups/{pg_id}")
    return [p["component"]["id"] for p in flow["processGroupFlow"]["flow"].get("processors", [])]


def halt_process_group_processors(client: NifiClient, pg_id: str) -> None:
    """
    Detiene y deshabilita todos los procesadores del grupo antes de tocar propiedades.
    Si solo se pone STOPPED, el timer puede re-disparar el flujo y dejar ExecuteScript
    RUNNING entre un update y el siguiente; DISABLED evita esa condición de carrera.
    """
    ids = list_processor_ids_in_group(client, pg_id)
    if not ids:
        return
    active = {"RUNNING", "STARTING", "VALIDATING", "LOADING"}
    for _ in range(8):
        saw_active = False
        for pid in ids:
            ent = client.get(f"/processors/{pid}")
            st = processor_effective_state(ent)
            if st in active or (st and st not in ("STOPPED", "DISABLED")):
                saw_active = True
                try:
                    set_processor_run_state(client, ent, "STOPPED")
                except requests.HTTPError:
                    pass
        time.sleep(0.6)
        if not saw_active:
            break
    for pid in ids:
        ent = client.get(f"/processors/{pid}")
        if processor_effective_state(ent) != "DISABLED":
            try:
                set_processor_run_state(client, ent, "DISABLED")
            except requests.HTTPError:
                pass
    time.sleep(0.4)


def restore_processors_disabled_to_stopped(client: NifiClient, pg_id: str) -> None:
    """DISABLED → STOPPED (habilitado pero sin ejecutar); el operador inicia el grupo en la UI."""
    for pid in list_processor_ids_in_group(client, pg_id):
        ent = client.get(f"/processors/{pid}")
        if processor_effective_state(ent) == "DISABLED":
            try:
                set_processor_run_state(client, ent, "STOPPED")
            except requests.HTTPError:
                pass


_PROCESSOR_KNOWN_STATES = frozenset(
    {"RUNNING", "STOPPED", "STARTING", "STOPPING", "DISABLED", "VALIDATING", "LOADING"}
)


def _collect_known_states_from_status(status: Any, depth: int = 0) -> list[str]:
    """NiFi 2.x a veces pone runStatus/scheduledState en nodos anidados bajo `status`, no solo en aggregateSnapshot."""
    if depth > 10 or status is None:
        return []
    found: list[str] = []
    if isinstance(status, dict):
        for v in status.values():
            found.extend(_collect_known_states_from_status(v, depth + 1))
    elif isinstance(status, list):
        for it in status:
            found.extend(_collect_known_states_from_status(it, depth + 1))
    elif isinstance(status, str):
        u = status.strip().upper()
        if u in _PROCESSOR_KNOWN_STATES:
            found.append(u)
    return found


def processor_effective_state(entity: dict[str, Any]) -> str:
    """
    Estado operativo para saber si hay que parar antes de editar config.
    NiFi 2.x puede dejar `component.state` en STOPPED mientras el snapshot ya indica STARTING;
    priorizar estados de transición/ejecución del aggregateSnapshot evita 409 al mandar RUNNING otra vez.
    """
    comp = entity.get("component") or {}
    snap = (entity.get("status") or {}).get("aggregateSnapshot") or {}
    candidates: list[str] = []
    for key in ("runStatus", "state", "scheduledState"):
        rs = snap.get(key)
        if rs is not None and str(rs).strip():
            candidates.append(str(rs).strip().upper())
    candidates.extend(_collect_known_states_from_status(entity.get("status")))
    cs = comp.get("state")
    if cs is not None and str(cs).strip():
        candidates.append(str(cs).strip().upper())
    if not candidates:
        return ""
    cset = set(candidates)
    # Orden: el estado “más vivo” gana frente a STOPPED obsoleto en component.
    priority = (
        "RUNNING",
        "VALIDATING",
        "LOADING",
        "STARTING",
        "STOPPING",
        "DISABLED",
        "STOPPED",
    )
    for p in priority:
        if p in cset:
            return p
    return candidates[-1]


def update_processor(
    client: NifiClient,
    entity: dict[str, Any],
    *,
    properties: dict[str, Any] | None = None,
    properties_remove: list[str] | None = None,
    scheduling_period: str | None = None,
    scheduling_strategy: str | None = None,
    concurrent_tasks: str | None = None,
    auto_terminated: list[str] | None = None,
    position: tuple[float, float] | None = None,
) -> dict[str, Any]:
    comp_id = entity["component"]["id"]
    # Releer el procesador: la revisión del listado del flujo suele estar desactualizada y provoca 400.
    entity = client.get(f"/processors/{comp_id}")
    comp = entity["component"]
    # No reactivar RUNNING aquí: el timer dispararía el flujo a mitad del recreate y rompería
    # los PUT siguientes. El script llama a `halt_process_group_processors` al inicio y
    # `restore_processors_disabled_to_stopped` al final.
    raw_cfg = comp.get("config")
    if not isinstance(raw_cfg, dict):
        raw_cfg = {}
    cfg = copy.deepcopy(raw_cfg)
    props = dict(cfg.get("properties") or {})
    if properties:
        props.update(properties)
    if properties_remove:
        for key in properties_remove:
            props.pop(key, None)
    # JSON `null` en propiedades opcionales (p. ej. Remote URL) provoca 400 en varias versiones.
    cfg["properties"] = {k: v for k, v in props.items() if v is not None}
    if scheduling_period:
        cfg["schedulingPeriod"] = scheduling_period
    if scheduling_strategy:
        cfg["schedulingStrategy"] = scheduling_strategy
    if concurrent_tasks is not None:
        cfg["concurrentTasks"] = concurrent_tasks
    if auto_terminated is not None:
        cfg["autoTerminatedRelationships"] = auto_terminated
    pos = {"x": float(position[0]), "y": float(position[1])} if position is not None else comp.get(
        "position", {"x": 0.0, "y": 0.0}
    )
    payload = {
        "revision": component_revision(entity),
        "component": {
            "id": comp["id"],
            "name": comp["name"],
            "position": pos,
            "config": cfg,
        },
    }
    return client.put(f"/processors/{comp_id}", payload)


def update_controller_service(
    client: NifiClient,
    entity: dict[str, Any],
    *,
    properties: dict[str, Any] | None = None,
) -> dict[str, Any]:
    comp = entity["component"]
    payload = {
        "revision": component_revision(entity),
        "component": {
            "id": comp["id"],
            "name": comp["name"],
            "properties": dict(comp.get("properties", {})),
        },
    }
    if properties:
        payload["component"]["properties"].update(properties)
    return client.put(f"/controller-services/{comp['id']}", payload)


def enable_controller_service(client: NifiClient, entity: dict[str, Any]) -> dict[str, Any]:
    comp = entity["component"]
    payload = {
        "revision": component_revision(entity),
        "state": "ENABLED",
    }
    return client.put(f"/controller-services/{comp['id']}/run-status", payload)


def disable_controller_service(client: NifiClient, entity: dict[str, Any]) -> dict[str, Any]:
    comp = entity["component"]
    payload = {
        "revision": component_revision(entity),
        "state": "DISABLED",
    }
    return client.put(f"/controller-services/{comp['id']}/run-status", payload)


def wait_for_controller_service_state(
    client: NifiClient,
    service_id: str,
    target_state: str,
    timeout_s: float = 15.0,
) -> dict[str, Any]:
    deadline = time.time() + timeout_s
    latest = get_controller_service(client, service_id)
    while time.time() < deadline:
        latest = get_controller_service(client, service_id)
        if latest["component"].get("state") == target_state:
            return latest
        time.sleep(0.5)
    return latest


def connect(client: NifiClient, pg_id: str, source: dict[str, Any], dest: dict[str, Any], relationship: str) -> None:
    flow = client.get(f"/flow/process-groups/{pg_id}")
    src_id = source["component"]["id"]
    dst_id = dest["component"]["id"]
    for c in flow["processGroupFlow"]["flow"].get("connections", []):
        comp = c["component"]
        if comp["source"]["id"] == src_id and comp["destination"]["id"] == dst_id and relationship in comp.get("selectedRelationships", []):
            return
    payload = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src_id, "groupId": pg_id, "type": "PROCESSOR"},
            "destination": {"id": dst_id, "groupId": pg_id, "type": "PROCESSOR"},
            "selectedRelationships": [relationship],
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB",
        },
    }
    client.post(f"/process-groups/{pg_id}/connections", payload)


def drop_queue(client: NifiClient, connection_id: str) -> None:
    req = client.post(f"/flowfile-queues/{connection_id}/drop-requests", {})
    drop = req["dropRequest"]
    drop_id = drop["id"]
    while not drop.get("finished", False):
        drop = client.get(f"/flowfile-queues/{connection_id}/drop-requests/{drop_id}")["dropRequest"]
    client.delete(f"/flowfile-queues/{connection_id}/drop-requests/{drop_id}")


def remove_connection(client: NifiClient, pg_id: str, src_name: str, dst_name: str, relationship: str) -> None:
    flow = client.get(f"/flow/process-groups/{pg_id}")
    for conn in flow["processGroupFlow"]["flow"].get("connections", []):
        comp = conn["component"]
        if (
            comp["source"]["name"] == src_name
            and comp["destination"]["name"] == dst_name
            and relationship in comp.get("selectedRelationships", [])
        ):
            queued = conn.get("status", {}).get("aggregateSnapshot", {}).get("queuedCount")
            if queued not in (None, "0", 0):
                drop_queue(client, comp["id"])
            client.delete(f"/connections/{comp['id']}", params=component_revision(conn))


def remove_processor(client: NifiClient, pg_id: str, name: str) -> None:
    flow = client.get(f"/flow/process-groups/{pg_id}")
    for proc in flow["processGroupFlow"]["flow"].get("processors", []):
        comp = proc["component"]
        if comp["name"] != name:
            continue
        for conn in list(flow["processGroupFlow"]["flow"].get("connections", [])):
            c = conn["component"]
            if c["source"]["id"] == comp["id"] or c["destination"]["id"] == comp["id"]:
                queued = conn.get("status", {}).get("aggregateSnapshot", {}).get("queuedCount")
                if queued not in (None, "0", 0):
                    drop_queue(client, c["id"])
                client.delete(f"/connections/{c['id']}", params=component_revision(conn))
        client.delete(f"/processors/{comp['id']}", params=component_revision(proc))
        return


def main() -> None:
    wait_for_nifi_api_ready()
    client = NifiClient()
    root_id = client.get("/flow/process-groups/root")["processGroupFlow"]["id"]
    pg = create_pg(client, root_id, "PG_SIMLOG_KDD")
    pg_id = pg["component"]["id"]

    scripts_root = BASE / "nifi"
    assert_nifi_groovy_sources_exist(scripts_root)

    halt_process_group_processors(client, pg_id)

    p1 = create_processor(client, pg_id, "Timer_Ingesta_15min", "org.apache.nifi.processors.standard.GenerateFlowFile", 80, 180)
    p1 = update_processor(client, p1, properties={"Batch Size": "1"}, scheduling_period="15 min",
                          scheduling_strategy="TIMER_DRIVEN", auto_terminated=[],
                          position=(80, 180))

    p2 = create_processor(client, pg_id, "Set_Parametros_Ingesta", "org.apache.nifi.processors.attributes.UpdateAttribute", 380, 180)
    p2 = update_processor(
        client,
        p2,
        properties={
            "dgt.url": "https://nap.dgt.es/datex2/v3/dgt/SituationPublication/datex2_v36.xml",
            "paso_15min": "0",
            "filename": "simlog_practice_${now():format(\"yyyyMMdd_HHmmss\")}.json",
        },
        auto_terminated=[],
        position=(380, 180),
    )

    p3, p5, p7 = create_or_update_execute_script_processors(client, pg_id, scripts_root)

    p4 = create_processor(client, pg_id, "OpenMeteo_InvokeHTTP", "org.apache.nifi.processors.standard.InvokeHTTP", 1040, 180)
    p4 = update_processor(
        client,
        p4,
        properties={
            "HTTP URL": OPEN_METEO_MULTI_HUBS_URL,
            "Remote URL": None,
            "HTTP Method": "GET",
            "Response Body Attribute Name": "owm.response",
            "Always Output Response": None,
        },
        auto_terminated=["Response"],
        position=(1040, 180),
    )

    p6 = create_processor(client, pg_id, "DGT_DATEX2_InvokeHTTP", "org.apache.nifi.processors.standard.InvokeHTTP", 1720, 180)
    p6 = update_processor(
        client,
        p6,
        properties={
            "HTTP URL": "${dgt.url}",
            "Remote URL": None,
            "HTTP Method": "GET",
            "Response Body Attribute Name": "dgt.response.xml",
            "Always Output Response": None,
        },
        auto_terminated=["Response"],
        position=(1720, 180),
    )

    kafka_service = create_controller_service(
        client,
        pg_id,
        "Kafka_SIMLOG_Local",
        "org.apache.nifi.kafka.service.Kafka3ConnectionService",
        1700,
        -260,
    )
    kafka_service = wait_for_controller_service_state(client, kafka_service["component"]["id"], "DISABLED")
    if kafka_service["component"].get("state") != "DISABLED":
        kafka_service = disable_controller_service(client, kafka_service)
        kafka_service = wait_for_controller_service_state(client, kafka_service["component"]["id"], "DISABLED")
    kafka_service = update_controller_service(
        client,
        kafka_service,
        properties={
            "bootstrap.servers": "localhost:9092",
        },
    )
    kafka_service = enable_controller_service(client, kafka_service)

    p8 = create_processor(client, pg_id, "Kafka_Publish_DGT_RAW", "org.apache.nifi.kafka.processors.PublishKafka", 2440, 20)
    p8 = update_processor(
        client,
        p8,
        properties={
            "Bootstrap Servers": None,
            "Kafka Connection Service": kafka_service["component"]["id"],
            "Topic Name": "transporte_dgt_raw",
            "Transactions Enabled": "false",
        },
        auto_terminated=["success"],
        position=(2440, 20),
    )

    p9 = create_processor(client, pg_id, "Kafka_Publish_RAW", "org.apache.nifi.kafka.processors.PublishKafka", 2440, 160)
    p9 = update_processor(
        client,
        p9,
        properties={
            "Bootstrap Servers": None,
            "Kafka Connection Service": kafka_service["component"]["id"],
            "Topic Name": "transporte_raw",
            "Transactions Enabled": "false",
        },
        auto_terminated=["success"],
        position=(2440, 160),
    )

    p10 = create_processor(client, pg_id, "Kafka_Publish_FILTERED", "org.apache.nifi.kafka.processors.PublishKafka", 2440, 300)
    p10 = update_processor(
        client,
        p10,
        properties={
            "Bootstrap Servers": None,
            "Kafka Connection Service": kafka_service["component"]["id"],
            "Topic Name": "transporte_filtered",
            "Transactions Enabled": "false",
        },
        auto_terminated=[],
        position=(2440, 300),
    )

    p11 = create_processor(client, pg_id, "HDFS_Backup_JSON", "org.apache.nifi.processors.standard.ExecuteStreamCommand", 2440, 440)
    p11 = update_processor(
        client,
        p11,
        properties={
            "Command Path": "/bin/bash",
            "Command Arguments": str(scripts_root / "scripts" / "put_hdfs_from_stdin.sh"),
            "Ignore STDIN": "false",
            "Output Destination Attribute": "hdfs.command.output",
        },
        auto_terminated=[],
        position=(2440, 440),
    )

    p14 = create_processor(client, pg_id, "Log_Fallos", "org.apache.nifi.processors.standard.LogAttribute", 2060, 600)
    p14 = update_processor(
        client,
        p14,
        properties={"Log Level": "error"},
        auto_terminated=["success"],
        position=(2060, 600),
    )

    for legacy_name in (
        "Spark_Submit_Procesamiento",
        "Notify_Pipeline_OK",
        "Notify_Pipeline_FALLO",
        "Route_Spark_Exito_Fallo",
        "Kafka_Consume_Filtered_for_Spark",
    ):
        try:
            remove_processor(client, pg_id, legacy_name)
        except Exception:
            pass

    # Esta instalación de NiFi rechaza EVENT_DRIVEN en PUT /processors (solo TIMER_DRIVEN | CRON_DRIVEN).
    spark_sh = str(scripts_root / "scripts" / "run_spark_from_nifi_flow.sh")
    p13 = create_processor(
        client,
        pg_id,
        "Spark_Submit_Procesamiento",
        "org.apache.nifi.processors.standard.ExecuteStreamCommand",
        2800,
        300,
    )
    # ExecuteStreamCommand: relaciones = original | output stream | nonzero status (no success/failure).
    # Quitar Output Destination Attribute del config (valor "" provoca IllegalArgumentException: Invalid attribute key).
    p13 = update_processor(
        client,
        p13,
        properties={
            "Command Path": "/bin/bash",
            "Command Arguments": spark_sh,
            "Ignore STDIN": "true",
        },
        properties_remove=["Output Destination Attribute"],
        auto_terminated=["output stream", "nonzero status"],
        position=(2800, 300),
    )

    p_route = create_processor(
        client,
        pg_id,
        "Route_Spark_Exito_Fallo",
        "org.apache.nifi.processors.standard.RouteOnAttribute",
        2990,
        300,
    )
    p_route = update_processor(
        client,
        p_route,
        properties={
            "Routing Strategy": "Route to Property name",
            "exito": "${execution.status:equals('0')}",
            "fallo": "${execution.status:equals('0'):not()}",
        },
        auto_terminated=[],
        position=(2990, 300),
    )

    notify_ok_sh = str(scripts_root / "scripts" / "notify_pipeline_nifi_exito.sh")
    p15 = create_processor(
        client,
        pg_id,
        "Notify_Pipeline_OK",
        "org.apache.nifi.processors.standard.ExecuteStreamCommand",
        3180,
        180,
    )
    p15 = update_processor(
        client,
        p15,
        properties={
            "Command Path": "/bin/bash",
            "Command Arguments": notify_ok_sh,
            "Ignore STDIN": "true",
        },
        auto_terminated=["original", "output stream"],
        position=(3180, 180),
    )

    notify_fail_sh = str(scripts_root / "scripts" / "notify_pipeline_nifi_fallo.sh")
    p16 = create_processor(
        client,
        pg_id,
        "Notify_Pipeline_FALLO",
        "org.apache.nifi.processors.standard.ExecuteStreamCommand",
        3180,
        420,
    )
    p16 = update_processor(
        client,
        p16,
        properties={
            "Command Path": "/bin/bash",
            "Command Arguments": notify_fail_sh,
            "Ignore STDIN": "true",
        },
        auto_terminated=["original", "output stream"],
        position=(3180, 420),
    )

    remove_connection(client, pg_id, "Merge_Weather_Into_Payload", "Kafka_Publish_RAW", "success")
    remove_connection(client, pg_id, "Merge_Weather_Into_Payload", "Kafka_Publish_FILTERED", "success")
    remove_connection(client, pg_id, "Merge_Weather_Into_Payload", "HDFS_Backup_JSON", "success")
    remove_connection(client, pg_id, "Kafka_Publish_FILTERED", "Kafka_Consume_Filtered_for_Spark", "success")
    remove_connection(client, pg_id, "Kafka_Consume_Filtered_for_Spark", "Spark_Submit_Procesamiento", "success")
    remove_connection(client, pg_id, "Kafka_Publish_FILTERED", "Spark_Submit_Procesamiento", "success")
    remove_connection(client, pg_id, "HDFS_Backup_JSON", "Log_Fallos", "failure")
    remove_connection(client, pg_id, "Spark_Submit_Procesamiento", "Log_Fallos", "failure")
    remove_connection(client, pg_id, "Spark_Submit_Procesamiento", "Notify_Pipeline_OK", "success")
    remove_connection(client, pg_id, "Spark_Submit_Procesamiento", "Notify_Pipeline_FALLO", "failure")
    remove_connection(client, pg_id, "Spark_Submit_Procesamiento", "Route_Spark_Exito_Fallo", "original")
    remove_connection(client, pg_id, "Route_Spark_Exito_Fallo", "Notify_Pipeline_OK", "exito")
    remove_connection(client, pg_id, "Route_Spark_Exito_Fallo", "Notify_Pipeline_FALLO", "fallo")
    remove_connection(client, pg_id, "Route_Spark_Exito_Fallo", "Log_Fallos", "unmatched")
    remove_connection(client, pg_id, "HDFS_Backup_JSON", "Spark_Submit_Procesamiento", "success")
    remove_connection(client, pg_id, "HDFS_Backup_JSON", "Spark_Submit_Procesamiento", "original")
    remove_connection(client, pg_id, "Notify_Pipeline_OK", "Log_Fallos", "failure")
    remove_connection(client, pg_id, "Notify_Pipeline_OK", "Log_Fallos", "nonzero status")
    remove_connection(client, pg_id, "Notify_Pipeline_FALLO", "Log_Fallos", "failure")
    remove_connection(client, pg_id, "Notify_Pipeline_FALLO", "Log_Fallos", "nonzero status")
    p10 = update_processor(client, p10, auto_terminated=["success"], position=(2440, 300))

    connect(client, pg_id, p1, p2, "success")
    connect(client, pg_id, p2, p3, "success")
    connect(client, pg_id, p3, p4, "success")
    connect(client, pg_id, p4, p5, "Original")
    connect(client, pg_id, p4, p5, "Failure")
    connect(client, pg_id, p4, p5, "No Retry")
    connect(client, pg_id, p5, p6, "success")
    connect(client, pg_id, p6, p7, "Original")
    connect(client, pg_id, p7, p8, "success")
    connect(client, pg_id, p7, p9, "success")
    connect(client, pg_id, p7, p10, "success")
    connect(client, pg_id, p7, p11, "success")

    def _connect_if_possible(src_proc, dst_proc, relationship: str) -> None:
        try:
            connect(client, pg_id, src_proc, dst_proc, relationship)
        except Exception:
            pass

    # Pipeline: HDFS ExecuteStreamCommand solo emite `original` (tiene Output Destination Attribute).
    # Spark ExecuteStreamCommand: `original` → RouteOnAttribute por execution.status → notificaciones.
    connect(client, pg_id, p11, p13, "original")
    connect(client, pg_id, p13, p_route, "original")
    connect(client, pg_id, p_route, p15, "exito")
    connect(client, pg_id, p_route, p16, "fallo")
    connect(client, pg_id, p_route, p14, "unmatched")

    for src, rel in [
        (p3, "failure"),
        (p4, "Failure"),
        (p4, "Retry"),
        (p4, "No Retry"),
        (p5, "failure"),
        (p6, "Failure"),
        (p6, "Retry"),
        (p6, "No Retry"),
        (p7, "failure"),
        (p8, "failure"),
        (p9, "failure"),
        (p10, "failure"),
        (p11, "failure"),
    ]:
        try:
            connect(client, pg_id, src, p14, rel)
        except Exception:
            pass

    for src_nf in (p15, p16):
        try:
            connect(client, pg_id, src_nf, p14, "nonzero status")
        except Exception:
            pass

    halt_process_group_processors(client, pg_id)
    restore_processors_disabled_to_stopped(client, pg_id)

    print(
        json.dumps(
            {
                "status": "ok",
                "process_group": "PG_SIMLOG_KDD",
                "process_group_id": pg_id,
                "note": "Procesadores en STOPPED; inicia PG_SIMLOG_KDD en la UI de NiFi para ejecutar el pipeline.",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
