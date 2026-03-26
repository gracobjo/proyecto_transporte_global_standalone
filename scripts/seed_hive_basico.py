#!/usr/bin/env python3
"""
Sembrado básico de Hive para que la UI funcione sin Spark+Hive metastore.

- Rellena `logistica_espana.nodos_maestro` desde `config_nodos.get_nodos()`.
- Inserta una "foto" en `logistica_espana.historico_nodos` a partir del último payload
  (`reports/kdd/work/ultimo_payload.json`), si existe.

Motivo: en algunos entornos Hive 4.x + Spark 3.5.x no son compatibles en Thrift metastore
(errores tipo `Invalid method name: 'get_table'`). Este script evita el metastore de Spark y usa beeline.
"""

from __future__ import annotations

import sys
import json
import os
import csv
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


BASE = Path(__file__).resolve().parents[1]
# Permite importar módulos del proyecto aunque ejecutes desde otro cwd.
sys.path.insert(0, str(BASE))
DEFAULT_PAYLOAD = BASE / "reports" / "kdd" / "work" / "ultimo_payload.json"


def _sql_quote(v: Optional[Any]) -> str:
    if v is None:
        return "NULL"
    s = str(v)
    # escape simple quote for Hive
    s = s.replace("\\", "\\\\").replace("'", "''")
    return f"'{s}'"


def _beeline_bin() -> str:
    # Preferir SPARK beeline (ya existe en el entorno del proyecto)
    cand = os.environ.get("HIVE_BEELINE_BIN", "").strip()
    if cand:
        return cand
    for p in ("/opt/spark/bin/beeline",):
        if Path(p).exists():
            return p
    return "beeline"


def _jdbc() -> str:
    # Mantener consistencia con config.py por defecto
    return os.environ.get("HIVE_JDBC_URL", "jdbc:hive2://127.0.0.1:10000/logistica_espana")


def _user() -> str:
    return os.environ.get("SIMLOG_HIVE_BEELINE_USER") or os.environ.get("HIVE_USER") or os.environ.get("USER") or "hadoop"


def _run_beeline(sql: str) -> Tuple[int, str]:
    beeline = _beeline_bin()
    jdbc = _jdbc()
    user = _user()
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".hql", delete=False) as f:
        f.write(sql)
        tmp = f.name
    try:
        r = subprocess.run(
            [beeline, "-u", jdbc, "-n", user, "-f", tmp],
            capture_output=True,
            text=True,
        )
        out = (r.stdout or "") + ("\n" + r.stderr if r.stderr else "")
        return r.returncode, out
    finally:
        try:
            Path(tmp).unlink(missing_ok=True)
        except OSError:
            pass


def _hdfs_put(local_path: Path, hdfs_dir: str, *, filename: str) -> None:
    """
    Sube un fichero local a un directorio HDFS.
    Asume que el comando `hdfs` está en PATH.
    """
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=False, capture_output=True, text=True)
    subprocess.run(
        ["hdfs", "dfs", "-put", "-f", str(local_path), f"{hdfs_dir.rstrip('/')}/{filename}"],
        check=True,
        capture_output=True,
        text=True,
    )


def _write_csv(path: Path, headers: List[str], rows: List[Dict[str, Any]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in headers})


def seed_external_tables(payload_path: Path) -> Tuple[str, str]:
    """
    Genera CSVs locales, los sube a HDFS y devuelve (loc_nodos_maestro, loc_historico_nodos).

    Esto evita `INSERT` en Hive (que puede disparar MapReduce/YARN).
    """
    from config_nodos import get_nodos

    nodos: Dict[str, Dict[str, Any]] = get_nodos()
    maestro_rows: List[Dict[str, Any]] = []
    for nid, d in nodos.items():
        hub = d.get("hub") or (nid if d.get("tipo") == "hub" else "")
        maestro_rows.append(
            {
                "id_nodo": nid,
                "lat": float(d.get("lat", 0.0)),
                "lon": float(d.get("lon", 0.0)),
                "tipo": d.get("tipo", ""),
                "hub": hub,
            }
        )

    payload: Optional[Dict[str, Any]] = None
    if payload_path.exists():
        try:
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
        except Exception:
            payload = None

    historico_rows: List[Dict[str, Any]] = []
    now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if payload:
        estados = payload.get("nodos_estado") or payload.get("estados_nodos") or {}
        clima_hubs = payload.get("clima_hubs") or {}
        for nid, est in (estados or {}).items():
            d = nodos.get(nid) or {}
            hub = d.get("hub") or (nid if d.get("tipo") == "hub" else "")
            clima = (clima_hubs or {}).get(hub) if hub else None
            clima_desc = None
            if isinstance(clima, dict):
                clima_desc = clima.get("descripcion") or clima.get("clima_desc") or clima.get("desc")
            historico_rows.append(
                {
                    "id_nodo": nid,
                    "lat": float(d.get("lat", 0.0)),
                    "lon": float(d.get("lon", 0.0)),
                    "tipo": d.get("tipo", ""),
                    "estado": (est or {}).get("estado") if isinstance(est, dict) else "",
                    "motivo_retraso": (est or {}).get("motivo") if isinstance(est, dict) else "",
                    "clima_actual": clima_desc or "N/A",
                    "temperatura": "",
                    "humedad": "",
                    "viento_velocidad": "",
                    # timestamps en formato parseable por Hive TIMESTAMP (yyyy-MM-dd HH:mm:ss)
                    "ultima_actualizacion": now_ts,
                    "fecha_proceso": now_ts,
                }
            )

    with tempfile.TemporaryDirectory() as td:
        tdir = Path(td)
        p_maestro = tdir / "nodos_maestro.csv"
        p_hist = tdir / "historico_nodos.csv"
        _write_csv(p_maestro, ["id_nodo", "lat", "lon", "tipo", "hub"], maestro_rows)
        _write_csv(
            p_hist,
            [
                "id_nodo",
                "lat",
                "lon",
                "tipo",
                "estado",
                "motivo_retraso",
                "clima_actual",
                "temperatura",
                "humedad",
                "viento_velocidad",
                "ultima_actualizacion",
                "fecha_proceso",
            ],
            historico_rows,
        )

        base = os.environ.get("SIMLOG_HDFS_SEED_BASE", "/user/hadoop/simlog_seed").rstrip("/")
        loc_maestro = f"{base}/nodos_maestro"
        loc_hist = f"{base}/historico_nodos"
        _hdfs_put(p_maestro, loc_maestro, filename="nodos_maestro.csv")
        _hdfs_put(p_hist, loc_hist, filename="historico_nodos.csv")
        return loc_maestro, loc_hist


def build_seed_sql_external(loc_maestro: str, loc_hist: str) -> str:
    # Re-crear como EXTERNAL para evitar INSERT.
    # Nota: si existían como managed Parquet, DROP TABLE elimina solo metadata (no borra HDFS EXTERNAL).
    return (
        "USE logistica_espana;\n"
        "DROP TABLE IF EXISTS nodos_maestro;\n"
        "DROP TABLE IF EXISTS historico_nodos;\n"
        "\n"
        "CREATE EXTERNAL TABLE IF NOT EXISTS logistica_espana.nodos_maestro (\n"
        "  id_nodo STRING,\n"
        "  lat DOUBLE,\n"
        "  lon DOUBLE,\n"
        "  tipo STRING,\n"
        "  hub STRING\n"
        ")\n"
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n"
        "WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='\"','escapeChar'='\\\\')\n"
        "STORED AS TEXTFILE\n"
        f"LOCATION '{loc_maestro}'\n"
        "TBLPROPERTIES ('skip.header.line.count'='1','serialization.null'='');\n"
        "\n"
        "CREATE EXTERNAL TABLE IF NOT EXISTS logistica_espana.historico_nodos (\n"
        "  id_nodo STRING,\n"
        "  lat DOUBLE,\n"
        "  lon DOUBLE,\n"
        "  tipo STRING,\n"
        "  estado STRING,\n"
        "  motivo_retraso STRING,\n"
        "  clima_actual STRING,\n"
        "  temperatura DOUBLE,\n"
        "  humedad DOUBLE,\n"
        "  viento_velocidad DOUBLE,\n"
        "  ultima_actualizacion TIMESTAMP,\n"
        "  fecha_proceso TIMESTAMP\n"
        ")\n"
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n"
        "WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='\"','escapeChar'='\\\\')\n"
        "STORED AS TEXTFILE\n"
        f"LOCATION '{loc_hist}'\n"
        "TBLPROPERTIES ('skip.header.line.count'='1','serialization.null'='');\n"
        "\n"
        # Evitar COUNT(*) en TEXTFILE: puede lanzar MapReduce y tardar/colgarse en entornos sin YARN.
        "SELECT * FROM nodos_maestro LIMIT 3;\n"
        "SELECT * FROM historico_nodos LIMIT 3;\n"
    )


def main() -> None:
    payload = Path(os.environ.get("SIMLOG_PAYLOAD_PATH", str(DEFAULT_PAYLOAD)))
    loc_maestro, loc_hist = seed_external_tables(payload)
    sql = build_seed_sql_external(loc_maestro, loc_hist)
    code, out = _run_beeline(sql)
    print(out)
    raise SystemExit(code)


if __name__ == "__main__":
    main()

