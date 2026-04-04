#!/usr/bin/env python3
"""
Comprueba vía API REST que PG_SIMLOG_KDD existe y el pipeline completo está cableado
(HDFS → Spark → notificaciones). Escribe reports/nifi_flow_verify.log y código de salida 0/1.

Requiere NiFi en marcha y credenciales (SIMLOG_NIFI_API, SIMLOG_NIFI_USER, SIMLOG_NIFI_PASSWORD).

  python scripts/verificar_flujo_nifi_api.py
  SIMLOG_SKIP_NIFI_VERIFY=1 python scripts/verificar_flujo_nifi_api.py   # solo log "omitido"
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

NIFI_GROUP = "PG_SIMLOG_KDD"
REQUIRED_PROCESSORS = {
    "Timer_Ingesta_15min",
    "Merge_DGT_Into_Payload",
    "HDFS_Backup_JSON",
    "Spark_Submit_Procesamiento",
    "Route_Spark_Exito_Fallo",
    "Notify_Pipeline_OK",
    "Notify_Pipeline_FALLO",
    "Log_Fallos",
}
REQUIRED_EDGES = [
    ("HDFS_Backup_JSON", "Spark_Submit_Procesamiento"),
    ("Spark_Submit_Procesamiento", "Route_Spark_Exito_Fallo"),
    ("Route_Spark_Exito_Fallo", "Notify_Pipeline_OK"),
    ("Route_Spark_Exito_Fallo", "Notify_Pipeline_FALLO"),
]


def main() -> int:
    log_dir = BASE / "reports"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "nifi_flow_verify.log"

    if os.environ.get("SIMLOG_SKIP_NIFI_VERIFY", "").strip() in ("1", "true", "yes"):
        msg = "SKIP: SIMLOG_SKIP_NIFI_VERIFY activo.\n"
        log_path.write_text(msg, encoding="utf-8")
        print(msg.strip())
        return 0

    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "recreate_nifi",
        BASE / "scripts" / "recreate_nifi_practice_flow.py",
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    NifiClient = mod.NifiClient

    try:
        mod.wait_for_nifi_api_ready()
    except Exception as e:
        err = f"FAIL: NiFi no disponible tras espera: {e}\n"
        log_path.write_text(err, encoding="utf-8")
        print(err.strip(), file=sys.stderr)
        return 1

    try:
        client = NifiClient()
    except Exception as e:
        err = f"FAIL: no se pudo autenticar en NiFi API: {e}\n"
        log_path.write_text(err, encoding="utf-8")
        print(err.strip(), file=sys.stderr)
        return 1

    root = client.get("/flow/process-groups/root")
    pg_id = None
    for g in root["processGroupFlow"]["flow"].get("processGroups", []):
        if g["component"]["name"] == NIFI_GROUP:
            pg_id = g["component"]["id"]
            break

    if not pg_id:
        err = f"FAIL: no existe process group `{NIFI_GROUP}`.\n"
        log_path.write_text(err, encoding="utf-8")
        print(err.strip(), file=sys.stderr)
        return 1

    flow = client.get(f"/flow/process-groups/{pg_id}")
    pg_flow = flow["processGroupFlow"]["flow"]
    by_name = {p["component"]["name"]: p for p in pg_flow.get("processors", [])}
    missing = sorted(REQUIRED_PROCESSORS - set(by_name))
    if missing:
        err = f"FAIL: faltan procesadores: {missing}\n"
        log_path.write_text(err, encoding="utf-8")
        print(err.strip(), file=sys.stderr)
        return 1

    spark = by_name.get("Spark_Submit_Procesamiento", {})
    stype = (spark.get("component") or {}).get("type", "")
    lines: list[str] = []
    if "ExecuteStreamCommand" not in stype:
        w = f"WARN: Spark_Submit_Procesamiento no es ExecuteStreamCommand (tipo={stype})."
        lines.append(w)
        print(w, file=sys.stderr)

    edges_ok = []
    for src_n, dst_n in REQUIRED_EDGES:
        sid = by_name[src_n]["component"]["id"]
        did = by_name[dst_n]["component"]["id"]
        found = False
        for c in pg_flow.get("connections", []):
            comp = c["component"]
            if comp["source"]["id"] == sid and comp["destination"]["id"] == did:
                found = True
                break
        edges_ok.append((src_n, dst_n, found))

    bad = [e for e in edges_ok if not e[2]]
    if bad:
        err = "FAIL: faltan conexiones: " + json.dumps(bad, ensure_ascii=False) + "\n"
        log_path.write_text("\n".join(lines) + err, encoding="utf-8")
        print(err.strip(), file=sys.stderr)
        return 1

    ok = (
        f"OK: `{NIFI_GROUP}` verificado ({len(by_name)} procesadores). "
        f"Edges HDFS→Spark→notify presentes.\n"
    )
    log_path.write_text("\n".join(lines + [ok.strip()]) + "\n", encoding="utf-8")
    print(ok.strip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
