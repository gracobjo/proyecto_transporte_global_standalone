"""
DAG de arranque de servicios: HDFS, Cassandra, Kafka.
Ejecutar manualmente (Trigger DAG) para levantar los servicios
antes del pipeline KDD o simlog_pipeline_maestro.
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from servicios_arranque import arrancar_hdfs, arrancar_cassandra, arrancar_kafka

BASE = Path(__file__).resolve().parent.parent

default_args = {
    "owner": "logistica",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}

with DAG(
    dag_id="dag_arranque_servicios",
    default_args=default_args,
    description="Arrancar HDFS, Cassandra y Kafka antes del pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["simlog", "servicios", "arranque"],
) as dag:

    start_hdfs = PythonOperator(
        task_id="arrancar_hdfs",
        python_callable=arrancar_hdfs,
    )
    start_cassandra = PythonOperator(
        task_id="arrancar_cassandra",
        python_callable=arrancar_cassandra,
    )
    start_kafka = PythonOperator(
        task_id="arrancar_kafka",
        python_callable=arrancar_kafka,
    )

    [start_hdfs, start_cassandra, start_kafka]
