"""Arranque de HDFS, Cassandra y Kafka (reutilizable por DAGs y tareas KDD)."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
CASSANDRA_BIN = BASE / "cassandra" / "bin" / "cassandra"


def puerto_activo(host: str, port: int) -> bool:
    import socket

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        sock.connect((host, port))
        sock.close()
        return True
    except OSError:
        return False


def arrancar_hdfs(**context):
    if puerto_activo("127.0.0.1", 9870):
        return "HDFS ya estaba activo"
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
    if puerto_activo("127.0.0.1", 9042):
        return "Cassandra ya estaba activa"
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
    if puerto_activo("127.0.0.1", 9092):
        return "Kafka ya estaba activo"
    kafka_home = Path(os.environ.get("KAFKA_HOME", "/opt/kafka"))
    start_script = kafka_home / "bin" / "kafka-server-start.sh"
    config = kafka_home / "config" / "server.properties"
    if not start_script.exists():
        config_kraft = kafka_home / "config" / "kraft" / "server.properties"
        if config_kraft.exists():
            config = config_kraft
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
