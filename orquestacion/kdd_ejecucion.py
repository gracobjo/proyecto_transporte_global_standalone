#!/usr/bin/env python3
"""
Tareas Python reutilizables para DAGs KDD (SIMLOG).
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict

_ORQ = Path(__file__).resolve().parent
if str(_ORQ) not in sys.path:
    sys.path.insert(0, str(_ORQ))

BASE = _ORQ.parent
sys.path.insert(0, str(BASE))

from kdd_informe import contexto_airflow_datos, generar_informe_fase
from servicios_arranque import arrancar_cassandra, arrancar_hdfs, arrancar_kafka

WORK_KDD = BASE / "reports" / "kdd" / "work"
WORK_KDD.mkdir(parents=True, exist_ok=True)

F1_CLIMA = WORK_KDD / "fase1_clima.json"
F2_PAYLOAD = WORK_KDD / "ultimo_payload.json"


def _paso_desde_context(**context) -> int:
    ds = context.get("data_interval_start") or context.get("logical_date")
    if ds is not None:
        try:
            h, m = ds.hour, ds.minute
            return h * 4 + m // 15
        except Exception:
            pass
    return int(os.environ.get("PASO_15MIN", "0"))


def verificar_servicios_core() -> Dict[str, bool]:
    import socket

    def ok(h, p):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((h, p))
            s.close()
            return True
        except OSError:
            return False

    return {
        "hdfs_namenode_9870": ok("127.0.0.1", 9870),
        "kafka_9092": ok("127.0.0.1", 9092),
        "cassandra_9042": ok("127.0.0.1", 9042),
    }


def con_informe(codigo: str, titulo: str, fn: Callable[..., Dict[str, Any]], **context) -> str:
    """Ejecuta `fn` (debe devolver dict con inicio/resultado o un dict plano) y genera Markdown/HTML."""
    datos_ctx = contexto_airflow_datos(**context)
    dr = context.get("dag_run")
    run_id = getattr(dr, "run_id", None) if dr else None
    salida = fn(**context)
    if isinstance(salida, dict) and "inicio" in salida and "resultado" in salida:
        inicio = salida["inicio"]
        resultado = salida["resultado"]
    else:
        inicio = datos_ctx
        resultado = salida
    path = generar_informe_fase(
        codigo,
        titulo,
        datos_inicio=inicio,
        datos_resultado=resultado,
        run_id=run_id,
        notas="Generado automáticamente por Airflow (SIMLOG).",
    )
    return str(path)


# --- Fase 0 ---

def ejecutar_fase00_infra(**context) -> Dict[str, Any]:
    r1 = arrancar_hdfs(**context)
    r2 = arrancar_cassandra(**context)
    r3 = arrancar_kafka(**context)
    v = verificar_servicios_core()
    inicio = {"accion": "arranque HDFS, Cassandra, Kafka + verificación de puertos"}
    resultado = {
        "mensajes_arranque": {"hdfs": r1, "cassandra": r2, "kafka": r3},
        "puertos_ok": v,
        "todos_servicios_ok": all(v.values()),
    }
    return {"inicio": inicio, "resultado": resultado}


def tarea_fase00(**context):
    return con_informe(
        "00_infra",
        "SIMLOG — Fase 0: Infraestructura (arranque y comprobación)",
        ejecutar_fase00_infra,
        **context,
    )


# --- Fase 1 ---

def ejecutar_fase_seleccion(**context) -> Dict[str, Any]:
    from config_nodos import RED, get_nodos
    from ingesta.ingesta_kdd import consulta_clima_hubs

    inicio = {
        "hubs": list(RED.get("hubs", [])),
        "nodos_totales": len(get_nodos()),
    }
    clima = consulta_clima_hubs()
    out = {"clima_hubs": clima, "timestamp": datetime.now(timezone.utc).isoformat()}
    F1_CLIMA.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    resultado = {"archivo_clima": str(F1_CLIMA), "hubs_con_clima": list(clima.keys())}
    return {"inicio": inicio, "resultado": resultado}


def tarea_fase01(**context):
    return con_informe(
        "01_seleccion",
        "SIMLOG — Fase 1 KDD: Selección de datos (clima + contexto)",
        ejecutar_fase_seleccion,
        **context,
    )


# --- Fase 2 ---

def ejecutar_fase_preprocesamiento(**context) -> Dict[str, Any]:
    from ingesta.ingesta_kdd import (
        clima_hubs_a_lista,
        generar_rutas_camiones,
        guardar_hdfs,
        interpolacion_gps_15min,
        publicar_kafka,
        simular_incidentes_aristas,
        simular_incidentes_nodos,
    )

    paso = _paso_desde_context(**context)
    if not F1_CLIMA.exists():
        raise FileNotFoundError(f"Falta {F1_CLIMA}. Ejecuta la fase 1 antes.")
    blob = json.loads(F1_CLIMA.read_text(encoding="utf-8"))
    clima = blob.get("clima_hubs", {})

    inicio = {"paso_15min": paso, "clima_fuente": str(F1_CLIMA)}

    estados_nodos = simular_incidentes_nodos()
    estados_aristas = simular_incidentes_aristas()
    rutas = generar_rutas_camiones(5)
    posiciones = interpolacion_gps_15min(rutas, paso)

    payload = {
        "origen": "airflow_kdd_fase02",
        "canal_ingesta": "airflow",
        "ejecutor_ingesta": "simlog_kdd_02_preprocesamiento",
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "paso_15min": paso,
        "clima_hubs": clima,
        "clima": clima_hubs_a_lista(clima, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")),
        "nodos_estado": {n: {"estado": v["estado"], "motivo": v["motivo"]} for n, v in estados_nodos.items()},
        "estados_nodos": {n: {"estado": v["estado"], "motivo": v["motivo"]} for n, v in estados_nodos.items()},
        "aristas_estado": estados_aristas,
        "estados_aristas": estados_aristas,
        "camiones": posiciones,
    }

    ok_k = publicar_kafka(payload)
    ok_h = guardar_hdfs(payload)
    F2_PAYLOAD.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    resultado = {
        "kafka_ok": ok_k,
        "hdfs_ok": ok_h,
        "payload_json": str(F2_PAYLOAD),
        "camiones": len(posiciones),
    }
    return {"inicio": inicio, "resultado": resultado}


def tarea_fase02(**context):
    return con_informe(
        "02_preprocesamiento",
        "SIMLOG — Fase 2 KDD: Preprocesamiento (simulación, GPS, Kafka, HDFS)",
        ejecutar_fase_preprocesamiento,
        **context,
    )


# --- Fases Spark 3–5 ---

def ejecutar_fase_spark(subfase: str, **context) -> Dict[str, Any]:
    import subprocess

    script = BASE / "procesamiento" / "fase_kdd_spark.py"
    paso = _paso_desde_context(**context)
    # Hive es opcional en el proyecto; para la subfase de interpretación (fase 5)
    # necesitamos que Spark habilite soporte Hive para que se creen/actualicen
    # tablas en `logistica_espana` y el frontend pueda consultarlas con beeline.
    env_extra: Dict[str, str] = {}
    detener_hive = False
    if subfase == "interpretacion":
        env_extra["SIMLOG_ENABLE_HIVE"] = "1"
        detener_hive = True
        # En modo local, HiveServer2 usa metastore Derby embebido. Si Spark intenta
        # habilitar Hive al mismo tiempo, falla con ERROR XSDB6 (lock de Derby).
        # Por eso, se para HS2 antes de ejecutar Spark y se reinicia al final.
        try:
            from servicios.gestion_servicios import parar_hive

            parar_hive()
        except Exception:
            # Si no se puede parar, Spark volverá a fallar con el lock; lo dejamos
            # continuar para que el error sea visible en los logs del task.
            pass
    r = subprocess.run(
        [sys.executable, str(script), "--fase", subfase],
        cwd=str(BASE),
        capture_output=True,
        text=True,
        timeout=900,
        env={**os.environ, "PASO_15MIN": str(paso), **env_extra},
    )
    if detener_hive:
        try:
            from servicios.gestion_servicios import iniciar_hive

            iniciar_hive()
        except Exception:
            pass
    inicio = {"subfase": subfase, "paso_15min": paso}
    resultado: Dict[str, Any] = {
        "returncode": r.returncode,
        "stdout_tail": (r.stdout or "")[-4000:],
        "stderr_tail": (r.stderr or "")[-4000:],
    }
    if r.returncode != 0:
        raise RuntimeError(f"fase_kdd_spark {subfase} falló: {r.stderr or r.stdout}")
    extra = {
        "transformacion": "fase3_metricas.json",
        "mineria": "fase4_pagerank.json",
        "interpretacion": "fase5_resumen.json",
    }
    jname = extra.get(subfase)
    if jname:
        jp = WORK_KDD / jname
        if jp.exists():
            resultado[jname] = json.loads(jp.read_text(encoding="utf-8"))
    return {"inicio": inicio, "resultado": resultado}


def ejecutar_fase_transformacion(**context) -> Dict[str, Any]:
    return ejecutar_fase_spark("transformacion", **context)


def ejecutar_fase_mineria(**context) -> Dict[str, Any]:
    return ejecutar_fase_spark("mineria", **context)


def ejecutar_fase_interpretacion(**context) -> Dict[str, Any]:
    return ejecutar_fase_spark("interpretacion", **context)


def tarea_fase03(**context):
    return con_informe(
        "03_transformacion",
        "SIMLOG — Fase 3 KDD: Transformación (grafo GraphFrames)",
        ejecutar_fase_transformacion,
        **context,
    )


def tarea_fase04(**context):
    return con_informe(
        "04_mineria",
        "SIMLOG — Fase 4 KDD: Minería (PageRank)",
        ejecutar_fase_mineria,
        **context,
    )


def tarea_fase05(**context):
    return con_informe(
        "05_interpretacion",
        "SIMLOG — Fase 5 KDD: Interpretación (persistencia Cassandra/Hive)",
        ejecutar_fase_interpretacion,
        **context,
    )


# --- Fase 99: consulta final ---

def ejecutar_fase99_consulta(**context) -> Dict[str, Any]:
    inicio = {"accion": "consulta de estado en Cassandra tras el pipeline"}
    res = _consulta_cassandra_counts()
    return {"inicio": inicio, "resultado": res}


def _consulta_cassandra_counts() -> Dict[str, Any]:
    try:
        from cassandra.cluster import Cluster

        from config import CASSANDRA_HOST, KEYSPACE

        cluster = Cluster([CASSANDRA_HOST])
        s = cluster.connect(KEYSPACE)
        counts = {}
        for t in ("nodos_estado", "aristas_estado", "tracking_camiones", "pagerank_nodos"):
            try:
                rows = s.execute(f"SELECT COUNT(*) FROM {t}")
                counts[t] = rows.one()[0] if rows else 0
            except Exception as e:
                counts[t] = f"error: {e}"
        cluster.shutdown()
        return {"cassandra_conteos": counts, "ts": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        return {"error": str(e)}


def tarea_fase99(**context):
    return con_informe(
        "99_consulta_final",
        "SIMLOG — Consulta final y cierre documental (sin parar servicios)",
        ejecutar_fase99_consulta,
        **context,
    )
