"""
Airflow DAG de testing end-to-end para SIMLOG.

Objetivo:
- ejecutar ingesta
- ejecutar procesamiento Spark
- validar HDFS, Cassandra y Hive
- dejar un reporte persistente
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

from config import CASSANDRA_HOST, HDFS_BACKUP_PATH, HIVE_DB, HIVE_TABLE_TRANSPORTE_HIST, KEYSPACE
from servicios.consultas_cuadro_mando import ejecutar_hive_consulta, ejecutar_hive_sql_seguro
from servicios.gestion_servicios import comprobar_airflow, comprobar_cassandra, comprobar_hdfs, comprobar_kafka
from servicios.pipeline_verificacion import hdfs_listado_json

REPORTS_DIR = BASE / "reports" / "tests"
HIVE_VALIDATION_TIMEOUT_SEC = int(os.environ.get("SIMLOG_TEST_HIVE_TIMEOUT_SEC", "30"))


def _force_fail(task_name: str) -> None:
    forced = (os.environ.get("SIMLOG_TEST_FORCE_FAIL_TASK", "") or "").strip()
    if forced and forced == task_name:
        raise RuntimeError(f"Fallo forzado de prueba para la tarea {task_name}")


def _python_bin() -> str:
    venv = BASE / "venv_transporte" / "bin" / "python"
    return str(venv) if venv.exists() else sys.executable


def _run(cmd: list[str], *, env: dict[str, str], timeout: int) -> Dict[str, Any]:
    p = subprocess.run(
        cmd,
        cwd=str(BASE),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return {
        "returncode": p.returncode,
        "stdout_tail": (p.stdout or "")[-4000:],
        "stderr_tail": (p.stderr or "")[-4000:],
    }


def _cassandra_counts() -> Dict[str, Any]:
    from cassandra.cluster import Cluster

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)
    out: Dict[str, Any] = {}
    for table in ("nodos_estado", "aristas_estado", "tracking_camiones", "pagerank_nodos", "eventos_historico"):
        rows = session.execute(f"SELECT COUNT(*) FROM {table}")
        row = rows.one() if rows else None
        out[table] = int(row[0]) if row is not None else 0
    cluster.shutdown()
    return out


def _parse_hive_count(text: str) -> int | None:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if len(lines) < 2:
        return None
    for token in reversed(lines[1].split("\t")):
        try:
            return int(token)
        except ValueError:
            continue
    return None


def _hive_has_rows(text: str) -> bool:
    lineas = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    return len(lineas) >= 2


def task_verificar_servicios(**context) -> Dict[str, Any]:
    _force_fail("verificar_servicios")
    estado = {
        "hdfs": comprobar_hdfs(),
        "kafka": comprobar_kafka(),
        "cassandra": comprobar_cassandra(),
        "airflow": comprobar_airflow(),
    }
    if not all(v.get("activo") for v in estado.values() if isinstance(v, dict)):
        raise RuntimeError(f"Servicios base no disponibles: {estado}")
    return estado


def task_ejecutar_ingesta(**context) -> Dict[str, Any]:
    _force_fail("ejecutar_ingesta")
    env = {
        **os.environ,
        "SIMLOG_INGESTA_CANAL": "airflow",
        "SIMLOG_INGESTA_ORIGEN": "airflow_test_pipeline",
        "SIMLOG_INGESTA_EJECUTOR": "test_pipeline_transporte_global",
    }
    result = _run([_python_bin(), "-m", "ingesta.ingesta_kdd"], env=env, timeout=300)
    if result["returncode"] != 0:
        raise RuntimeError(result["stderr_tail"] or result["stdout_tail"])
    return result


def task_ejecutar_spark(**context) -> Dict[str, Any]:
    _force_fail("ejecutar_spark")
    env = {**os.environ, "SIMLOG_ENABLE_HIVE": "1"}
    result = _run([_python_bin(), "-m", "procesamiento.procesamiento_grafos"], env=env, timeout=1200)
    if result["returncode"] != 0:
        raise RuntimeError(result["stderr_tail"] or result["stdout_tail"])
    return result


def task_validar_hdfs(**context) -> Dict[str, Any]:
    _force_fail("validar_hdfs")
    res = hdfs_listado_json(HDFS_BACKUP_PATH, max_items=5, timeout=25)
    if not res.get("ok") or int(res.get("total_json_listados") or 0) <= 0:
        raise RuntimeError(f"Sin evidencias HDFS válidas: {res}")
    return res


def task_validar_cassandra(**context) -> Dict[str, Any]:
    _force_fail("validar_cassandra")
    counts = _cassandra_counts()
    if counts["tracking_camiones"] <= 0 or counts["nodos_estado"] <= 0:
        raise RuntimeError(f"Cassandra sin datos esperados: {counts}")
    return counts


def task_validar_hive(**context) -> Dict[str, Any]:
    _force_fail("validar_hive")
    os.environ.setdefault("HIVE_QUERY_TIMEOUT_SEC", str(HIVE_VALIDATION_TIMEOUT_SEC))
    ok_hist, err_hist, txt_hist = ejecutar_hive_consulta("historico_nodos_muestra")
    ok_trans, err_trans, txt_trans = ejecutar_hive_consulta("transporte_ingesta_real_muestra")
    out = {
        "historico_nodos": "ok" if ok_hist and _hive_has_rows(txt_hist) else f"error: {err_hist or 'sin filas'}",
        "transporte_ingesta_completa": (
            "ok" if ok_trans and _hive_has_rows(txt_trans) else f"error: {err_trans or 'sin filas'}"
        ),
    }
    if out["historico_nodos"] != "ok":
        raise RuntimeError(f"Hive historico_nodos no accesible: {out}")
    if out["transporte_ingesta_completa"] != "ok":
        raise RuntimeError(f"Hive transporte_ingesta_completa no accesible: {out}")
    return out


def task_generar_reporte(**context) -> str:
    _force_fail("generar_reporte")
    run_id = getattr(context.get("dag_run"), "run_id", "manual")
    out_dir = REPORTS_DIR / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    ti = context["ti"]
    hive_xcom = ti.xcom_pull(task_ids="validar_hive")
    hive_report = (
        hive_xcom
        if isinstance(hive_xcom, dict)
        else {"estado_tarea": "sin_xcom", "detalle": "La tarea validar_hive no devolvió XCom utilizable."}
    )
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dag_id": context["dag"].dag_id,
        "run_id": run_id,
        "verificacion_servicios": ti.xcom_pull(task_ids="verificar_servicios"),
        "ingesta": ti.xcom_pull(task_ids="ejecutar_ingesta"),
        "spark": ti.xcom_pull(task_ids="ejecutar_spark"),
        "hdfs": ti.xcom_pull(task_ids="validar_hdfs"),
        "cassandra": ti.xcom_pull(task_ids="validar_cassandra"),
        "hive": hive_report,
    }
    path_json = out_dir / "test_pipeline_transporte_global.json"
    path_md = out_dir / "test_pipeline_transporte_global.md"
    path_json.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    body = (
        "# Test pipeline transporte global\n\n"
        f"- Run: `{run_id}`\n"
        f"- Generado (UTC): `{report['generated_at']}`\n\n"
        "## Resumen\n\n"
        f"- HDFS JSON: `{report['hdfs'].get('total_json_listados')}`\n"
        f"- Cassandra tracking: `{report['cassandra'].get('tracking_camiones')}`\n"
        f"- Estado validar_hive: `{report['hive'].get('estado_tarea', 'desconocido')}`\n"
        f"- Hive histórico nodos: `{report['hive'].get('historico_nodos')}`\n"
        f"- Hive transporte: `{report['hive'].get('transporte_ingesta_completa')}`\n\n"
        "## Detalle JSON\n\n"
        "```json\n"
        + json.dumps(report, indent=2, ensure_ascii=False, default=str)
        + "\n```\n"
    )
    path_md.write_text(body, encoding="utf-8")
    return str(path_json)


default_args = {
    "owner": "simlog",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="test_pipeline_transporte_global",
    default_args=default_args,
    description="SIMLOG — DAG de pruebas end-to-end (ingesta, Spark, HDFS, Cassandra, Hive)",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["simlog", "testing", "kdd", "airflow"],
) as dag:
    verificar_servicios = PythonOperator(task_id="verificar_servicios", python_callable=task_verificar_servicios)
    ejecutar_ingesta = PythonOperator(task_id="ejecutar_ingesta", python_callable=task_ejecutar_ingesta)
    ejecutar_spark = PythonOperator(task_id="ejecutar_spark", python_callable=task_ejecutar_spark)
    validar_hdfs = PythonOperator(task_id="validar_hdfs", python_callable=task_validar_hdfs)
    validar_cassandra = PythonOperator(task_id="validar_cassandra", python_callable=task_validar_cassandra)
    validar_hive = PythonOperator(task_id="validar_hive", python_callable=task_validar_hive)
    generar_reporte = PythonOperator(
        task_id="generar_reporte",
        python_callable=task_generar_reporte,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    verificar_servicios >> ejecutar_ingesta >> ejecutar_spark
    ejecutar_spark >> [validar_hdfs, validar_cassandra, validar_hive] >> generar_reporte
