"""
Consultas Hive vía beeline con salida CSV (csv2), optimizada para parseo en Python.
Alternativa a PyHive cuando se desea alineación exacta con la CLI de Hive.
"""
from __future__ import annotations

import csv
import io
import os
import re
import shlex
import subprocess
from typing import Any, Dict, List, Tuple

from config import HIVE_BEELINE_USER, HIVE_DB, HIVE_JDBC_URL


def _jdbc_url() -> str:
    return (HIVE_JDBC_URL or "jdbc:hive2://localhost:10000").strip()


def _hive_db() -> str:
    return (HIVE_DB or "logistica_espana").strip()


def _sql_readonly_ok(sql: str) -> bool:
    u = sql.strip().upper()
    return bool(re.match(r"^(SHOW|SELECT|DESC|DESCRIBE|WITH)\b", u))


def ejecutar_beeline_csv2(sql: str, timeout_sec: int = 120) -> Tuple[bool, str, str]:
    """
    Ejecuta una sentencia Hive de solo lectura (SHOW/SELECT/WITH/DESC) y devuelve salida csv2.
    """
    sql = sql.strip()
    if not _sql_readonly_ok(sql):
        return False, "Solo SHOW, SELECT, WITH o DESCRIBE", ""

    beeline = os.environ.get("BEELINE_CMD", "beeline")
    jdbc = _jdbc_url()
    user = HIVE_BEELINE_USER or "hadoop"
    db = _hive_db()

    cmd = [
        beeline,
        "-u",
        jdbc,
        "-n",
        user,
        "--silent=true",
        "--outputformat=csv2",
        "-e",
        f"USE {db}; {sql}",
    ]
    try:
        r = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            env={**os.environ, "HADOOP_USER_NAME": os.environ.get("HADOOP_USER_NAME") or user},
        )
    except FileNotFoundError:
        return False, f"Comando no encontrado: {beeline}", ""
    except subprocess.TimeoutExpired:
        return False, f"Timeout beeline ({timeout_sec}s)", ""

    out = (r.stdout or "").strip()
    if r.returncode != 0:
        err = (r.stderr or r.stdout or "").strip()
        return False, err[:2000], ""

    return True, "", out


def parse_csv2(text: str) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Parsea cabecera + filas desde salida beeline csv2."""
    if not text:
        return [], []
    f = io.StringIO(text)
    reader = csv.reader(f)
    rows = list(reader)
    if not rows:
        return [], []
    header = [h.strip() for h in rows[0]]
    data: List[Dict[str, Any]] = []
    for parts in rows[1:]:
        if not parts or all(not p.strip() for p in parts):
            continue
        row: Dict[str, Any] = {}
        for i, h in enumerate(header):
            if i < len(parts):
                v = parts[i].strip()
                row[h] = None if v == "" or v.upper() == "NULL" else v
        data.append(row)
    return header, data


def ejecutar_beeline_csv2_parseado(sql: str) -> Tuple[bool, str, List[str], List[Dict[str, Any]]]:
    ok, err, out = ejecutar_beeline_csv2(sql)
    if not ok:
        return False, err, [], []
    h, rows = parse_csv2(out)
    return True, "", h, rows
