#!/usr/bin/env python3
"""
Arranca o detiene el pipeline completo en NiFi (PG_SIMLOG_KDD) solo por terminal, vía API REST.

Requisitos: NiFi en marcha, credenciales SIMLOG_NIFI_* (como recreate_nifi_practice_flow.py).
Tras `recreate_nifi_practice_flow.py` los procesadores suelen quedar en STOPPED; este script los pone en RUNNING.

En NiFi 2.x no hay ``PUT /process-groups/{id}/run-status`` para grupos locales; el script usa
el arranque/parada **procesador a procesador** (comportamiento normal, no es un error).

Uso:
  export SIMLOG_NIFI_HOME=~/proyecto_transporte_global/nifi-2.0.0   # opcional, para espera/propiedades
  source venv_transporte/bin/activate
  python scripts/nifi_iniciar_pipeline_simlog.py              # iniciar
  python scripts/nifi_iniciar_pipeline_simlog.py --detener    # parar (STOPPED)
  python scripts/nifi_iniciar_pipeline_simlog.py --omitir-spark  # sin Spark_Submit_Procesamiento
"""
from __future__ import annotations

import argparse
import importlib.util
import re
import sys
import time
from pathlib import Path
from typing import Any

import requests

BASE = Path(__file__).resolve().parent.parent
PG_NAME = "PG_SIMLOG_KDD"

# Orden de arranque: fuente → transformación → publicación → HDFS → Spark → notificaciones.
# No usar coordenadas del canvas: Kafka/Spark quedaban antes que el timer y rompían el flujo.
_PIPELINE_START_ORDER: tuple[str, ...] = (
    "Timer_Ingesta_15min",
    "Set_Parametros_Ingesta",
    "Build_GPS_Sintetico",
    "OpenMeteo_InvokeHTTP",
    "OpenWeather_InvokeHTTP",
    "Merge_Weather_Into_Payload",
    "DGT_DATEX2_InvokeHTTP",
    "Merge_DGT_Into_Payload",
    "Kafka_Publish_DGT_RAW",
    "Kafka_Publish_RAW",
    "Kafka_Publish_FILTERED",
    "HDFS_Backup_JSON",
    "Spark_Submit_Procesamiento",
    "Route_Spark_Exito_Fallo",
    "Notify_Pipeline_OK",
    "Notify_Pipeline_FALLO",
    "Log_Fallos",
)

# ExecuteStreamCommand (p. ej. spark-submit) puede tardar minutos en salir de STOPPING tras una ejecución previa.
_PROCESSOR_START_MAX_WAIT: dict[str, float] = {
    "Spark_Submit_Procesamiento": 600.0,
    # Mismo patrón ExecuteStreamCommand / 409 que Spark si NiFi queda incoherente.
    "Notify_Pipeline_OK": 600.0,
    "Notify_Pipeline_FALLO": 600.0,
}
_DEFAULT_START_MAX_WAIT = 120.0
_STOPPING_NUDGE_SEC = 35.0
# GET=STOPPED + 409 hint STARTING sin alinearse: forzar PUT STOPPED y reintentar RUNNING.
_RECONCILE_STOPPED_AFTER_MISMATCH_SEC = 90.0
_MAX_RECONCILE_STOPPED_ATTEMPTS = 3
# Si NiFi devuelve 409 en bucle con el procesador ya en STOPPED, no tiene sentido esperar 10 min en silencio.
_MAX_CONSECUTIVE_RUN_409 = 35
# GET /processors a veces devuelve STOPPED mientras el 409 de run-status dice STARTING/RUNNING.
_STATE_PRIORITY: tuple[str, ...] = (
    "RUNNING",
    "VALIDATING",
    "LOADING",
    "STARTING",
    "STOPPING",
    "DISABLED",
    "STOPPED",
)


def _state_from_run_status_409_body(body: str) -> str | None:
    if not body:
        return None
    for pat in (
        r"Current state is\s+(\w+)",
        r"currentState\"\s*:\s*\"(\w+)\"",
        r"current_state\"\s*:\s*\"(\w+)\"",
    ):
        m = re.search(pat, body, re.I)
        if m:
            return m.group(1).strip().upper()
    return None


def _merge_effective_state(api_st: str, run_status_hint: str | None) -> str:
    parts = {p for p in (api_st, run_status_hint or "") if p and str(p).strip()}
    if not parts:
        return api_st or ""
    for p in _STATE_PRIORITY:
        if p in parts:
            return p
    return api_st or ""


def _409_implies_transitional_state(body: str) -> bool:
    """
    NiFi responde 409 si mandamos RUNNING y el procesador no está en STOPPED (STARTING, RUNNING, etc.).
    El texto exacto varía por versión; no subir el contador de fallos: esperar y releer estado.
    """
    b = (body or "").lower()
    if "cannot be started because it is not stopped" in b:
        return True
    return any(
        x in b
        for x in (
            "current state is starting",
            "current state is running",
            "current state is stopping",
            "current state is validating",
        )
    )


def _http_body_snippet(resp: requests.Response | None, limit: int = 900) -> str:
    if resp is None or not resp.text:
        return "(sin cuerpo)"
    t = resp.text.strip().replace("\n", " ")
    return (t[:limit] + "…") if len(t) > limit else t


def _load_recreate_mod():
    spec = importlib.util.spec_from_file_location(
        "recreate_nifi",
        BASE / "scripts" / "recreate_nifi_practice_flow.py",
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def _find_process_group_id(client, mod) -> str:
    root = client.get("/flow/process-groups/root")
    for g in root["processGroupFlow"]["flow"].get("processGroups", []):
        if g["component"]["name"] == PG_NAME:
            return str(g["component"]["id"])
    raise RuntimeError(f"No existe el process group `{PG_NAME}`. Ejecuta: python scripts/recreate_nifi_practice_flow.py")


def _try_process_group_run_status(client, mod, pg_id: str, state: str) -> bool:
    """
    Intenta arrancar/parar todo el grupo en un solo PUT. En NiFi 2.x la API pública no expone
    ``/process-groups/{id}/run-status`` para grupos locales (responde 404); solo aparece en
    remote process groups. Se siguen probando dos rutas por compatibilidad; los 404 no se
    imprimen para no confundir con un fallo real.
    """
    pg_entity = client.get(f"/process-groups/{pg_id}")
    payload = {"revision": mod.component_revision(pg_entity), "state": state}
    for path in (
        f"/process-groups/{pg_id}/run-status",
        f"/flow/process-groups/{pg_id}/run-status",
    ):
        try:
            client.put(path, payload)
            print(f"[NiFi] PUT {path} state={state} OK", flush=True)
            return True
        except requests.HTTPError as e:
            sc = getattr(e.response, "status_code", None)
            if sc == 404:
                continue
            body = (e.response.text if e.response else "") or str(e)
            print(f"[NiFi] {path} falló ({sc}): {body[:400]}", flush=True)
    return False


def _put_processor_run_status_refresh_on_stale_revision(
    client: Any,
    mod: Any,
    *,
    name: str,
    pid: str,
    entity: dict[str, Any],
    state: str,
    op_label: str = "",
    max_stale_retries: int = 3,
) -> None:
    """
    PUT /processors/{{id}}/run-status. Si NiFi devuelve 400 por revisión obsoleta, releer y reintentar
    (hasta max_stale_retries). Encadenar DISABLED → STOPPED puede requerir más de un releído.
    """
    ent_work = entity
    for attempt in range(max_stale_retries):
        try:
            mod.set_processor_run_state(client, ent_work, state)
            return
        except requests.HTTPError as e:
            sc = getattr(e.response, "status_code", None)
            body = ((e.response.text or "") if e.response else "").lower()
            stale = sc == 400 and (
                "revision" in body or "up-to-date" in body or "most up-to-date" in body or "modified" in body
            )
            if not stale or attempt == max_stale_retries - 1:
                raise
            suffix = f" {op_label}" if op_label else ""
            print(
                f"[NiFi] {name}: PUT {state}{suffix} revisión obsoleta (intento {attempt + 1}/{max_stale_retries}); releeyendo…",
                flush=True,
            )
            ent_work = client.get(f"/processors/{pid}")


def _start_processor_run(client, mod, name: str, pid: str, max_wait: float = _DEFAULT_START_MAX_WAIT) -> None:
    """
    ExecuteStreamCommand y otros pueden quedar en STOPPING unos segundos; RUNNING antes → 409.
    Si STOPPING se alarga, un PUT STOPPED extra a veces desbloquea la transición en NiFi 2.x.
    Con el procesador en STOPPED, 409 repetido suele ser validación/configuración: se muestra y se aborta.
    """
    deadline = time.time() + max_wait
    t0 = time.time()
    last_log = 0.0
    last_stop_nudge = 0.0
    consecutive_run_409 = 0
    last_409_snippet = ""
    run_status_hint: str | None = None
    mismatch_since: float | None = None
    reconcile_stopped_attempts = 0
    last_post_reconcile_stop_nudge = 0.0
    while time.time() < deadline:
        ent = client.get(f"/processors/{pid}")
        api_st = mod.processor_effective_state(ent)
        if api_st and api_st != "STOPPED":
            run_status_hint = None
        # Tras N reconciliaciones PUT STOPPED, el hint 409 deja de ser fiable: GET queda en STOPPED y NiFi sigue en STARTING.
        if reconcile_stopped_attempts >= _MAX_RECONCILE_STOPPED_ATTEMPTS:
            run_status_hint = None
        st = _merge_effective_state(api_st, run_status_hint)
        now = time.time()

        if run_status_hint and api_st == "STOPPED" and st in ("STARTING", "STOPPING"):
            if mismatch_since is None:
                mismatch_since = now
            elif (
                reconcile_stopped_attempts < _MAX_RECONCILE_STOPPED_ATTEMPTS
                and (now - mismatch_since) >= _RECONCILE_STOPPED_AFTER_MISMATCH_SEC
            ):
                print(
                    f"[NiFi] {name}: GET=STOPPED sin alinearse con hint_409={run_status_hint!r} "
                    f"({int(now - mismatch_since)}s); PUT STOPPED para reconciliar "
                    f"({reconcile_stopped_attempts + 1}/{_MAX_RECONCILE_STOPPED_ATTEMPTS})…",
                    flush=True,
                )
                if reconcile_stopped_attempts == _MAX_RECONCILE_STOPPED_ATTEMPTS - 1:
                    print(
                        f"[NiFi] {name}: última reconciliación → DISABLED y luego STOPPED (ExecuteStreamCommand atascado)…",
                        flush=True,
                    )
                    try:
                        _put_processor_run_status_refresh_on_stale_revision(
                            client,
                            mod,
                            name=name,
                            pid=pid,
                            entity=ent,
                            state="DISABLED",
                            op_label="(reconcile)",
                        )
                    except requests.HTTPError:
                        pass
                    time.sleep(0.7)
                    ent = client.get(f"/processors/{pid}")
                try:
                    _put_processor_run_status_refresh_on_stale_revision(
                        client,
                        mod,
                        name=name,
                        pid=pid,
                        entity=ent,
                        state="STOPPED",
                        op_label="(reconcile)",
                    )
                except requests.HTTPError as e:
                    print(f"[NiFi] {name}: PUT STOPPED reconcile falló: {e!s}"[:400], flush=True)
                run_status_hint = None
                mismatch_since = None
                reconcile_stopped_attempts += 1
                time.sleep(1.5)
                continue
        else:
            mismatch_since = None

        if st != "STOPPED":
            consecutive_run_409 = 0
        if now - last_log >= 45.0:
            if run_status_hint and api_st == "STOPPED":
                extra = ", NiFi run-status desincronizado con GET (no reenviar RUNNING hasta que coincida)"
            elif last_409_snippet and _409_implies_transitional_state(last_409_snippet):
                extra = ", 409 transitorio (esperando)"
            elif last_409_snippet:
                extra = f", último 409: {last_409_snippet[:120]}…"
            else:
                extra = ""
            st_show = (
                f"{st!r} [GET={api_st!r}, hint_409={run_status_hint!r}]"
                if run_status_hint and api_st == "STOPPED" and st != api_st
                else repr(st or "(vacío)")
            )
            print(f"[NiFi] … esperando {name} (estado={st_show}, {int(now - t0)}s{extra})", flush=True)
            last_log = now
        if st == "RUNNING":
            print(f"[NiFi] ya RUNNING → {name}", flush=True)
            return
        if st == "DISABLED":
            mod.set_processor_run_state(client, ent, "STOPPED")
            time.sleep(0.35)
            continue
        if st in ("STOPPING", "STARTING"):
            if st == "STOPPING" and (now - last_stop_nudge) >= _STOPPING_NUDGE_SEC:
                try:
                    mod.set_processor_run_state(client, ent, "STOPPED")
                except requests.HTTPError:
                    pass
                last_stop_nudge = now
            time.sleep(0.5)
            continue
        if st != "STOPPED":
            time.sleep(0.35)
            continue
        try:
            _put_processor_run_status_refresh_on_stale_revision(
                client, mod, name=name, pid=pid, entity=ent, state="RUNNING", op_label="(RUNNING)"
            )
            print(f"[NiFi] RUNNING → {name}", flush=True)
            return
        except requests.HTTPError as e:
            if getattr(e.response, "status_code", None) == 409:
                raw_409 = (e.response.text or "") if e.response is not None else ""
                last_409_snippet = _http_body_snippet(e.response)
                if _409_implies_transitional_state(raw_409):
                    consecutive_run_409 = 0
                    if reconcile_stopped_attempts < _MAX_RECONCILE_STOPPED_ATTEMPTS:
                        parsed = _state_from_run_status_409_body(raw_409)
                        run_status_hint = parsed if parsed in _STATE_PRIORITY else "STARTING"
                        time.sleep(1.0)
                    else:
                        # Sin hint: reintentar RUNNING; NiFi a veces queda en STARTING interno hasta un STOPPED fresco.
                        nnow = time.time()
                        if nnow - last_post_reconcile_stop_nudge >= 45.0:
                            last_post_reconcile_stop_nudge = nnow
                            try:
                                ent_n = client.get(f"/processors/{pid}")
                                _put_processor_run_status_refresh_on_stale_revision(
                                    client,
                                    mod,
                                    name=name,
                                    pid=pid,
                                    entity=ent_n,
                                    state="STOPPED",
                                    op_label="(tras reconciliar, 409)",
                                )
                                print(
                                    f"[NiFi] {name}: 409 transitorio persistente; PUT STOPPED de apoyo cada ~45s…",
                                    flush=True,
                                )
                            except requests.HTTPError:
                                pass
                        time.sleep(2.5)
                    continue
                consecutive_run_409 += 1
                if consecutive_run_409 == 1:
                    print(
                        f"[NiFi] {name}: NiFi rechazó PUT run-status RUNNING (409). Detalle: {last_409_snippet}",
                        flush=True,
                    )
                if consecutive_run_409 >= _MAX_CONSECUTIVE_RUN_409:
                    raise RuntimeError(
                        f"NiFi devolvió 409 {_MAX_CONSECUTIVE_RUN_409} veces seguidas al poner RUNNING a `{name}` "
                        f"con el procesador en STOPPED. Respuesta típica: {last_409_snippet}\n"
                        "Revisa en la UI validación del procesador, relaciones obligatorias, controller services, "
                        "y bulletins; a veces hace falta corregir la configuración o la revisión del flujo."
                    ) from None
                time.sleep(0.7)
                continue
            raise
    ent = client.get(f"/processors/{pid}")
    api_final = mod.processor_effective_state(ent)
    st_final = _merge_effective_state(api_final, run_status_hint)
    hint = (
        f"hint desde 409={run_status_hint!r}, GET={api_final!r}" if run_status_hint else
        (f"Último rechazo RUNNING (409): {last_409_snippet}" if last_409_snippet else
         "Si estaba en STOPPING, puede haber un spark-submit colgado.")
    )
    raise RuntimeError(
        f"Timeout arrancando {name} tras {int(max_wait)}s (estado efectivo {st_final!r}). {hint} "
        "Revisa bulletins del procesador y la configuración en NiFi."
    )


def _stop_processor_run(client, mod, name: str, pid: str, max_wait: float = 120.0) -> None:
    deadline = time.time() + max_wait
    printed = False
    while time.time() < deadline:
        ent = client.get(f"/processors/{pid}")
        st = mod.processor_effective_state(ent)
        if st in ("STOPPED", "DISABLED"):
            if printed:
                print(f"[NiFi] STOPPED → {name}", flush=True)
            return
        if st == "STOPPING":
            time.sleep(0.5)
            continue
        if st in ("RUNNING", "STARTING"):
            try:
                mod.set_processor_run_state(client, ent, "STOPPED")
                printed = True
            except requests.HTTPError as e:
                if getattr(e.response, "status_code", None) == 409:
                    time.sleep(0.7)
            time.sleep(0.35)
            continue
        time.sleep(0.35)
    raise RuntimeError(f"Timeout deteniendo {name}.")


def _per_processor_run_state(
    client,
    mod,
    pg_id: str,
    state: str,
    *,
    omit_processor_names: frozenset[str] | None = None,
) -> None:
    flow = client.get(f"/flow/process-groups/{pg_id}")
    procs = flow["processGroupFlow"]["flow"].get("processors", [])
    by_name: dict[str, dict] = {}
    for p in procs:
        name = (p.get("component") or {}).get("name") or ""
        if name:
            by_name[name] = p

    omit = omit_processor_names or frozenset()

    if state == "RUNNING":
        for p in procs:
            ent = client.get(f"/processors/{p['component']['id']}")
            if mod.processor_effective_state(ent) == "DISABLED":
                mod.set_processor_run_state(client, ent, "STOPPED")

        ordered_names: list[str] = list(_PIPELINE_START_ORDER)
        known = set(_PIPELINE_START_ORDER)
        extras = sorted(n for n in by_name if n not in known)
        ordered_names.extend(extras)

        for name in ordered_names:
            if name in omit:
                print(f"[NiFi] omitido (--omitir-spark) → {name}", flush=True)
                continue
            p = by_name.get(name)
            if not p:
                continue
            pid = p["component"]["id"]
            mw = _PROCESSOR_START_MAX_WAIT.get(name, _DEFAULT_START_MAX_WAIT)
            _start_processor_run(client, mod, name, pid, max_wait=mw)
        return

    # STOPPED: primero aguas abajo (notify, spark, hdfs, kafka…) luego ingesta.
    stop_order = list(reversed(_PIPELINE_START_ORDER))
    known = set(_PIPELINE_START_ORDER)
    extras = sorted((n for n in by_name if n not in known), reverse=True)
    stop_order.extend(extras)

    for name in stop_order:
        p = by_name.get(name)
        if not p:
            continue
        pid = p["component"]["id"]
        ent = client.get(f"/processors/{pid}")
        st = mod.processor_effective_state(ent)
        if st in ("RUNNING", "STARTING", "STOPPING"):
            _stop_processor_run(client, mod, name, pid)


def main() -> int:
    ap = argparse.ArgumentParser(description="Iniciar o detener PG_SIMLOG_KDD en NiFi (API REST).")
    ap.add_argument("--detener", action="store_true", help="Poner procesadores en STOPPED (no DISABLED).")
    ap.add_argument(
        "--omitir-spark",
        action="store_true",
        help="No arrancar Spark_Submit_Procesamiento (el resto del pipeline sí). Útil si NiFi deja el ExecuteStreamCommand incoherente.",
    )
    args = ap.parse_args()

    mod = _load_recreate_mod()
    try:
        mod.wait_for_nifi_api_ready()
    except Exception as e:
        print(f"[NiFi] ERROR esperando API: {e}", file=sys.stderr)
        return 1

    client = mod.NifiClient()
    try:
        pg_id = _find_process_group_id(client, mod)
    except RuntimeError as e:
        print(f"[NiFi] ERROR: {e}", file=sys.stderr)
        return 1

    state = "STOPPED" if args.detener else "RUNNING"
    print(f"[NiFi] Process group `{PG_NAME}` id={pg_id} → {state}", flush=True)

    omit = frozenset({"Spark_Submit_Procesamiento"}) if args.omitir_spark else frozenset()
    if not _try_process_group_run_status(client, mod, pg_id, state):
        print(
            "[NiFi] Modo por procesador: NiFi 2.x no implementa run-status del grupo local (404 esperado en la API).",
            flush=True,
        )
        _per_processor_run_state(client, mod, pg_id, state, omit_processor_names=omit)

    if args.detener:
        print("[NiFi] Hecho. Procesadores en STOPPED.", flush=True)
    else:
        print(
            "[NiFi] Hecho. Timer en RUNNING: ingesta → Kafka/HDFS → Spark → notificaciones "
            "(Kafka, HDFS, Cassandra y scripts deben estar operativos).",
            flush=True,
        )
        print(
            "[NiFi] Si en log aparece error XML (DGT DATEX2): la DGT a veces devuelve HTML o XML truncado; "
            "revisa bulletins de InvokeHTTP/Merge_DGT o desactiva temporalmente la rama DGT.",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
