#!/usr/bin/env python3
"""
Genera la red estática del gemelo digital (50 capitales + 5 subnodos/capital):
capitales en **grafo completo** entre sí; subnodos enlazados a su capital y capitales
cercanas. Volcado a HDFS + instrucciones Hive (logistica_espana). El pipeline batch usa
solo capitales en `config_nodos.py` (mismo K_n haversine, sin subnodos).

Ejecución (desde la raíz del proyecto):
  python3 procesamiento/generar_red_gemelo_digital.py

Requiere: HADOOP/HDFS en PATH; opcionalmente beeline para crear tablas (ver sql/hive_gemelo_digital.hql).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Tuple

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

from config import HDFS_BACKUP_PATH, HIVE_BEELINE_USER, HIVE_DB, HIVE_JDBC_URL
from servicios.gemelo_digital_grafo import generar_grafo

HDFS_GEMELO_BASE = os.environ.get("SIMLOG_HDFS_GEMELO", "/user/hadoop/gemelo_digital")


def _hdfs_bin() -> str:
    hadoop_home = os.environ.get("HADOOP_HOME")
    if hadoop_home:
        p = os.path.join(hadoop_home, "bin", "hdfs")
        if os.path.isfile(p):
            return p
    return "hdfs"


def escribir_csv_local(nodos: List[Dict], aristas: List[Dict], dir_tmp: Path) -> Tuple[Path, Path]:
    fn = dir_tmp / "red_gemelo_nodos.csv"
    fa = dir_tmp / "red_gemelo_aristas.csv"
    with open(fn, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["id_nodo", "tipo", "lat", "lon", "id_capital_ref", "nombre"],
        )
        w.writeheader()
        for row in nodos:
            w.writerow(row)
    with open(fa, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["src", "dst", "distancia_km"])
        w.writeheader()
        for row in aristas:
            w.writerow(row)
    return fn, fa


def hdfs_put(local: Path, remote: str) -> bool:
    hb = _hdfs_bin()
    subprocess.run([hb, "dfs", "-mkdir", "-p", os.path.dirname(remote)], capture_output=True, text=True)
    r2 = subprocess.run([hb, "dfs", "-put", "-f", str(local), remote], capture_output=True, text=True)
    if r2.returncode != 0:
        print(f"[HDFS] put {local} -> {remote}: {r2.stderr}", file=sys.stderr)
        return False
    return True


def main() -> None:
    ap = argparse.ArgumentParser(description="Generar red gemelo digital y subir a HDFS")
    ap.add_argument("--solo-local", action="store_true", help="Solo CSV en /tmp, sin HDFS")
    ap.add_argument("--hdfs-base", default=HDFS_GEMELO_BASE, help="Ruta base en HDFS")
    args = ap.parse_args()

    nodos, aristas = generar_grafo()
    meta = {
        "n_nodos": len(nodos),
        "n_aristas": len(aristas),
        "subnodos_por_capital": 5,
        "radio_subnodo_km": 20.0,
    }
    print(json.dumps(meta, indent=2))

    with tempfile.TemporaryDirectory() as td:
        tdir = Path(td)
        p_nodos, p_aristas = escribir_csv_local(nodos, aristas, tdir)
        (tdir / "red_gemelo_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

        if args.solo_local:
            out = Path("/tmp/gemelo_digital_export")
            out.mkdir(parents=True, exist_ok=True)
            for p in (p_nodos, p_aristas, tdir / "red_gemelo_meta.json"):
                dest = out / p.name
                dest.write_bytes(p.read_bytes())
            print(f"CSV escritos en {out}")
            return

        base = args.hdfs_base.rstrip("/")
        loc_n = f"{base}/nodos"
        loc_a = f"{base}/aristas"
        ok1 = hdfs_put(p_nodos, f"{loc_n}/red_gemelo_nodos.csv")
        ok2 = hdfs_put(p_aristas, f"{loc_a}/red_gemelo_aristas.csv")
        ok3 = hdfs_put(tdir / "red_gemelo_meta.json", f"{base}/red_gemelo_meta.json")
        if not (ok1 and ok2 and ok3):
            print("Fallo subiendo a HDFS. Copia local:", file=sys.stderr)
            print(p_nodos, p_aristas, file=sys.stderr)
            sys.exit(1)

        hdfs_put(p_nodos, f"{HDFS_BACKUP_PATH.rstrip('/')}/gemelo/nodos/red_gemelo_nodos.csv")
        hdfs_put(p_aristas, f"{HDFS_BACKUP_PATH.rstrip('/')}/gemelo/aristas/red_gemelo_aristas.csv")

    hive_db = HIVE_DB or "logistica_espana"
    jdbc = HIVE_JDBC_URL
    user = HIVE_BEELINE_USER
    base = args.hdfs_base.rstrip("/")
    loc_n = f"{base}/nodos"
    loc_a = f"{base}/aristas"
    print("\n-- Ejecutar en Hive (beeline) para registrar tablas externas:\n")
    print(f"!connect {jdbc}")
    print(f"USE {hive_db};")
    print(
        f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {hive_db}.red_gemelo_nodos (
  id_nodo STRING,
  tipo STRING,
  lat DOUBLE,
  lon DOUBLE,
  id_capital_ref STRING,
  nombre STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\\\'
)
STORED AS TEXTFILE
LOCATION '{loc_n}'
TBLPROPERTIES ('skip.header.line.count'='1', 'serialization.null'='');
"""
    )
    print(
        f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {hive_db}.red_gemelo_aristas (
  src STRING,
  dst STRING,
  distancia_km DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\\\'
)
STORED AS TEXTFILE
LOCATION '{loc_a}'
TBLPROPERTIES ('skip.header.line.count'='1', 'serialization.null'='');
"""
    )
    print(f"-- Usuario JDBC sugerido: {user}")


if __name__ == "__main__":
    main()
