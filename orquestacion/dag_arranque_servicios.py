"""
DAG de arranque de servicios: HDFS, Cassandra, Kafka.
Ejecutar manualmente (Trigger DAG) para levantar los servicios
antes del pipeline de ingesta y procesamiento (dag_maestro_transporte).
"""
from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

BASE = Path(__file__).resolve().parent.parent
CASSANDRA_BIN = BASE / "cassandra" / "bin" / "cassandra"


def _puerto_activo(host: str, port: int) -> bool:
    """True si el puerto está en escucha."""
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        sock.connect((host, port))
        sock.close()
        return True
    except Exception:
        return False


def arrancar_hdfs(**context):
    """Arranca HDFS si no está activo (NameNode 9870)."""
    if _puerto_activo("127.0.0.1", 9870):
        return "HDFS ya estaba activo"
    import subprocess
    hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
    start_script = Path(hadoop_home) / "sbin" / "start-dfs.sh"
    if start_script.exists():
        subprocess.run([str(start_script)], cwd=str(BASE), timeout=60, capture_output=True)
        return "HDFS arrancado"
    r = subprocess.run(["start-dfs.sh"], shell=True, cwd=str(BASE), timeout=60, capture_output=True)
    if r.returncode != 0:
        raise RuntimeError("No se pudo arrancar HDFS. Ejecuta manualmente: start-dfs.sh")
    return "HDFS arrancado"


def arrancar_cassandra(**context):
    """Arranca Cassandra en background si no está activa (9042)."""
    if _puerto_activo("127.0.0.1", 9042):
        return "Cassandra ya estaba activa"
    import subprocess
    if not CASSANDRA_BIN.exists():
        raise RuntimeError(f"No encontrado: {CASSANDRA_BIN}")
    subprocess.Popen(
        [str(CASSANDRA_BIN)],
        cwd=str(BASE),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    return "Cassandra arrancando (esperar ~30-60 s)"


def arrancar_kafka(**context):
    """Arranca Kafka si no está activo (9092)."""
    if _puerto_activo("127.0.0.1", 9092):
        return "Kafka ya estaba activo"
    import subprocess
    kafka_home = Path(os.environ.get("KAFKA_HOME", "/opt/kafka"))
    start_script = kafka_home / "bin" / "kafka-server-start.sh"
    config = kafka_home / "config" / "server.properties"
    if not start_script.exists():
        config_kraft = kafka_home / "config" / "kraft" / "server.properties"
        if config_kraft.exists():
            config = config_kraft
            start_script = kafka_home / "bin" / "kafka-server-start.sh"
        else:
            raise RuntimeError(f"Kafka no encontrado en {kafka_home}. Configura KAFKA_HOME o arranca manualmente.")
    subprocess.Popen(
        [str(start_script), str(config)],
        cwd=str(kafka_home),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    return "Kafka arrancando (esperar unos segundos)"


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
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["logistica", "servicios", "arranque"],
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
