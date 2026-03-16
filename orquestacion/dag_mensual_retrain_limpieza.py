"""
DAG mensual: re-entrenamiento del modelo de grafos y limpieza de tablas temporales en HDFS (PDF Fase IV).
- Ejecuta una vez al mes (día 1 a las 00:00).
- Limpia en HDFS: archivos en transporte_backup más antiguos que DIAS_RETENCION_BACKUP.
- Re-ejecuta procesamiento de grafos (batch completo) como re-entrenamiento.

Configuración: DIAS_RETENCION_BACKUP por defecto 30; override con variable de Airflow
"dag_mensual_dias_retencion" (número).
"""
from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

BASE = Path(__file__).resolve().parent.parent

# Ruta HDFS donde la ingesta escribe (y Spark lee); se limpian aquí los JSON antiguos
HDFS_BACKUP_PATH = os.environ.get("HDFS_BACKUP_PATH", "/user/hadoop/transporte_backup")
# Conservar últimos N días en backup (borrar más antiguos). Configurable por Airflow.
DIAS_RETENCION_BACKUP = int(os.environ.get("DIAS_RETENCION_BACKUP", "30"))


def limpiar_hdfs_temporales(**context):
    """Elimina en HDFS los JSON de backup más viejos que DIAS_RETENCION_BACKUP."""
    import subprocess
    from datetime import datetime, timedelta
    cutoff = (datetime.utcnow() - timedelta(days=DIAS_RETENCION_BACKUP)).strftime("%Y%m%d")
    r = subprocess.run(
        ["hdfs", "dfs", "-ls", HDFS_BACKUP_PATH],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if r.returncode != 0:
        return
    for line in (r.stdout or "").strip().split("\n"):
        if "transporte_" not in line:
            continue
        parts = line.split()
        if len(parts) < 8:
            continue
        path = parts[-1]
        try:
            name = path.split("/")[-1]
            if name.startswith("transporte_") and ".json" in name:
                fecha_str = name.replace("transporte_", "").split("_")[0][:8]
                if fecha_str < cutoff:
                    subprocess.run(["hdfs", "dfs", "-rm", "-skipTrash", path], capture_output=True, timeout=10)
        except Exception:
            pass


def reentrenar_grafos(**context):
    """Ejecuta el procesamiento de grafos (batch completo) como re-entrenamiento mensual."""
    import subprocess
    r = subprocess.run(
        [f"{BASE}/venv_transporte/bin/python", str(BASE / "procesamiento" / "procesamiento_grafos.py")],
        cwd=str(BASE),
        capture_output=True,
        text=True,
        timeout=300,
        env={"REENTRENAMIENTO_MENSUAL": "1"},
    )
    if r.returncode != 0:
        raise RuntimeError(f"Re-entrenamiento falló: {r.stderr or r.stdout}")


default_args = {
    "owner": "logistica",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_mensual_retrain_limpieza",
    default_args=default_args,
    description="Re-entrenamiento mensual del modelo de grafos y limpieza HDFS",
    schedule="0 0 1 * *",  # Día 1 de cada mes a las 00:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["logistica", "mensual", "limpieza", "retrain"],
) as dag:

    limpieza_hdfs = PythonOperator(
        task_id="limpiar_hdfs_temporales",
        python_callable=limpiar_hdfs_temporales,
    )

    reentrenar = PythonOperator(
        task_id="reentrenar_modelo_grafos",
        python_callable=reentrenar_grafos,
    )

    # Primero limpiar, luego re-entrenar (para no borrar datos recientes que use el batch)
    limpieza_hdfs >> reentrenar
