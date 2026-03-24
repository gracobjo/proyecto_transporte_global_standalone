#!/usr/bin/env python3
"""
Ejecución por subfases KDD (3–5) sobre Spark, reutilizando lógica de procesamiento_grafos.
Uso: python -m procesamiento.fase_kdd_spark --fase transformacion|mineria|interpretacion
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

WORK = BASE / "reports" / "kdd" / "work"
WORK.mkdir(parents=True, exist_ok=True)


def _load_payload():
    p = WORK / "ultimo_payload.json"
    if not p.exists():
        raise FileNotFoundError(f"No hay payload de ingesta: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def _normalize_from_payload(payload):
    from procesamiento.procesamiento_grafos import limpiar_datos_antes_cassandra

    estados_nodos = payload.get("nodos_estado", payload.get("estados_nodos", {}))
    estados_aristas = payload.get("aristas_estado", payload.get("estados_aristas", {}))
    camiones = payload.get("camiones", [])
    camiones = [
        {
            "id_camion": c.get("id") or c.get("id_camion"),
            "lat": (c.get("posicion_actual") or {}).get("lat") if isinstance(c.get("posicion_actual"), dict) else c.get("lat"),
            "lon": (c.get("posicion_actual") or {}).get("lon") if isinstance(c.get("posicion_actual"), dict) else c.get("lon"),
            "ruta": c.get("ruta", []),
            "ruta_sugerida": c.get("ruta_sugerida", c.get("ruta", [])),
            "ruta_origen": (c.get("ruta") or [None])[0],
            "ruta_destino": (c.get("ruta") or [None])[-1],
            "estado_ruta": c.get("estado_ruta", "En ruta"),
            "motivo_retraso": c.get("motivo_retraso"),
        }
        for c in camiones
    ]
    clima = payload.get("clima_hubs", {})
    for hub, c in (clima or {}).items():
        if hub in estados_nodos and isinstance(estados_nodos[hub], dict):
            estados_nodos[hub]["clima_desc"] = c.get("descripcion", c.get("descripcion", "N/A"))
            estados_nodos[hub]["temp"] = c.get("temp")
            estados_nodos[hub]["humedad"] = c.get("humedad")
            estados_nodos[hub]["viento"] = c.get("viento")

    estados_nodos, estados_aristas, camiones = limpiar_datos_antes_cassandra(
        estados_nodos, estados_aristas, camiones
    )
    return estados_nodos, estados_aristas, camiones


def run_transformacion():
    from config_nodos import get_nodos, get_aristas
    from procesamiento.procesamiento_grafos import crear_spark, construir_grafo_base, aplicar_autosanacion

    payload = _load_payload()
    estados_nodos, estados_aristas, camiones = _normalize_from_payload(payload)
    spark = crear_spark()
    try:
        nodos = get_nodos()
        aristas = get_aristas()
        g0 = construir_grafo_base(spark, nodos, aristas)
        g = aplicar_autosanacion(g0, estados_aristas, estados_nodos)
        nv, ne = g.vertices.count(), g.edges.count()
        out = {"vertices": nv, "edges": ne, "mensaje": "Grafo transformado (autosanación aplicada)"}
        (WORK / "fase3_metricas.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
        print(json.dumps(out))
        return 0
    finally:
        spark.stop()


def run_mineria():
    from config_nodos import get_nodos, get_aristas
    from procesamiento.procesamiento_grafos import crear_spark, construir_grafo_base, aplicar_autosanacion
    from pyspark.sql.functions import col

    payload = _load_payload()
    estados_nodos, estados_aristas, camiones = _normalize_from_payload(payload)
    spark = crear_spark()
    try:
        nodos = get_nodos()
        aristas = get_aristas()
        g0 = construir_grafo_base(spark, nodos, aristas)
        g = aplicar_autosanacion(g0, estados_aristas, estados_nodos)
        pr = g.pageRank(resetProbability=0.15, maxIter=10)
        top = pr.vertices.select("id", col("pagerank").alias("pr")).orderBy(col("pr").desc()).limit(10).collect()
        out = {"top_pagerank": [{"id": r["id"], "pagerank": float(r["pr"])} for r in top]}
        (WORK / "fase4_pagerank.json").write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(out, ensure_ascii=False))
        return 0
    finally:
        spark.stop()


def run_interpretacion():
    """Persistencia Cassandra + Hive: misma lógica que `procesamiento_grafos.main()` (lee HDFS / simula)."""
    from procesamiento.procesamiento_grafos import main as pg_main

    pg_main()
    r = {"ok": True, "nota": "Ver stdout de procesamiento; datos persistidos en Cassandra/Hive si aplica."}
    (WORK / "fase5_resumen.json").write_text(json.dumps(r, indent=2), encoding="utf-8")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fase", required=True, choices=["transformacion", "mineria", "interpretacion"])
    args = ap.parse_args()
    if args.fase == "transformacion":
        sys.exit(run_transformacion())
    if args.fase == "mineria":
        sys.exit(run_mineria())
    sys.exit(run_interpretacion())


if __name__ == "__main__":
    main()
