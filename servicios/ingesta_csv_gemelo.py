"""
Ingesta desde CSV (explorador) hacia Kafka y HDFS para simulaciones posteriores.
Columnas esperadas: id_camion, lat, lon, origen, destino (opcional: progreso_pct).
"""
from __future__ import annotations

import csv
import io
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from config import HDFS_BACKUP_PATH, KAFKA_BOOTSTRAP, TOPIC_TRANSPORTE


def _hdfs_bin() -> str:
    hadoop_home = os.environ.get("HADOOP_HOME")
    if hadoop_home:
        p = os.path.join(hadoop_home, "bin", "hdfs")
        if os.path.isfile(p):
            return p
    return "hdfs"


def parsear_csv_gemelo(contenido: str) -> Tuple[List[Dict[str, Any]], str]:
    """Parsea CSV (coma); primera fila cabecera."""
    f = io.StringIO(contenido.strip())
    r = csv.reader(f)
    rows = list(r)
    if len(rows) < 2:
        return [], "El CSV debe tener cabecera y al menos una fila de datos."
    cab = [c.strip().lower() for c in rows[0]]
    req = {"id_camion", "lat", "lon", "origen", "destino"}
    if not req.issubset(set(cab)):
        return [], f"Cabecera debe incluir: {sorted(req)}. Obtenido: {cab}"

    idx = {c: i for i, c in enumerate(cab)}
    filas: List[Dict[str, Any]] = []
    for partes in rows[1:]:
        if not partes or all(not p.strip() for p in partes):
            continue
        try:
            filas.append(
                {
                    "id_camion": partes[idx["id_camion"]].strip(),
                    "lat": float(partes[idx["lat"]].strip()),
                    "lon": float(partes[idx["lon"]].strip()),
                    "origen": partes[idx["origen"]].strip(),
                    "destino": partes[idx["destino"]].strip(),
                    "progreso_pct": float(partes[idx["progreso_pct"]].strip())
                    if "progreso_pct" in idx and idx["progreso_pct"] < len(partes)
                    else None,
                }
            )
        except (ValueError, IndexError) as e:
            return [], f"Fila inválida: {partes[:5]}… ({e})"
    return filas, ""


def publicar_kafka_filas(filas: List[Dict[str, Any]], etiqueta: str = "gemelo_csv") -> bool:
    try:
        from kafka import KafkaProducer
    except ImportError:
        return False

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        for f in filas:
            payload = {
                "tipo": "ingesta_explorador_gemelo",
                "etiqueta": etiqueta,
                "timestamp": ts,
                "registro": f,
            }
            producer.send(TOPIC_TRANSPORTE, value=payload)
        producer.flush()
        return True
    except Exception:
        return False
    finally:
        producer.close()


def guardar_hdfs_jsonl(filas: List[Dict[str, Any]], etiqueta: str = "gemelo_csv") -> Tuple[bool, str]:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    nombre = f"gemelo_ingesta_{etiqueta}_{ts}.jsonl"
    local = Path("/tmp") / nombre
    hdfs_dir = f"{HDFS_BACKUP_PATH.rstrip('/')}/gemelo/ingesta_csv"
    hdfs_path = f"{hdfs_dir}/{nombre}"

    try:
        with open(local, "w", encoding="utf-8") as out:
            for f in filas:
                out.write(json.dumps(f, ensure_ascii=False) + "\n")
        subprocess.run([_hdfs_bin(), "dfs", "-mkdir", "-p", hdfs_dir], capture_output=True, text=True)
        r = subprocess.run(
            [_hdfs_bin(), "dfs", "-put", "-f", str(local), hdfs_path],
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            return False, r.stderr or r.stdout or "hdfs put error"
        try:
            local.unlink()
        except OSError:
            pass
        return True, hdfs_path
    except Exception as e:
        return False, str(e)


def ingerir_csv_gemelo(contenido_csv: str, etiqueta: str = "gemelo_csv") -> Dict[str, Any]:
    filas, err = parsear_csv_gemelo(contenido_csv)
    if err:
        return {"ok": False, "error": err, "n": 0}

    ok_k = publicar_kafka_filas(filas, etiqueta=etiqueta)
    ok_h, path_or_err = guardar_hdfs_jsonl(filas, etiqueta=etiqueta)
    return {
        "ok": True,
        "n": len(filas),
        "kafka": ok_k,
        "hdfs": ok_h,
        "hdfs_path": path_or_err if ok_h else path_or_err,
    }
