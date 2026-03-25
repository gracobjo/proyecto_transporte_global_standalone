"""
Gestión de servicios del stack (iniciar, comprobar, parar): HDFS, Kafka, Cassandra,
Spark (standalone opcional), HiveServer2, Airflow, NiFi.

Rutas configurables vía variables de entorno: HADOOP_HOME, KAFKA_HOME, SPARK_HOME,
HIVE_HOME, NIFI_HOME, AIRFLOW_HOME, etc.

Las paradas pueden ser disruptivas: usar solo en entornos de desarrollo o con precaución.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

BASE = Path(__file__).resolve().parent.parent

# Puertos por defecto (sobrescribibles con SIMLOG_PORT_*)
PORT_HDFS = int(os.environ.get("SIMLOG_PORT_HDFS", "9870"))
PORT_KAFKA = int(os.environ.get("SIMLOG_PORT_KAFKA", "9092"))
PORT_CASSANDRA = int(os.environ.get("SIMLOG_PORT_CASSANDRA", "9042"))
PORT_HIVE = int(os.environ.get("SIMLOG_PORT_HIVE", "10000"))
PORT_SPARK_MASTER = int(os.environ.get("SIMLOG_PORT_SPARK_MASTER", "7077"))
PORT_AIRFLOW = int(os.environ.get("SIMLOG_PORT_AIRFLOW", "8088"))
PORT_NIFI_HTTPS = int(os.environ.get("SIMLOG_PORT_NIFI_HTTPS", "8443"))
PORT_NIFI_HTTP = int(os.environ.get("SIMLOG_PORT_NIFI_HTTP", "8080"))


def puerto_activo(host: str, port: int, timeout: float = 2.0) -> bool:
    import socket

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
        s.close()
        return True
    except OSError:
        return False


def puerto_activo_en_hosts(hosts: List[str], port: int, timeout: float = 2.0) -> bool:
    """Devuelve True si el puerto responde en cualquiera de los hosts dados."""
    return any(puerto_activo(h, port, timeout=timeout) for h in hosts)


def _popen_bg(cmd: List[str], cwd: Optional[Path] = None) -> None:
    subprocess.Popen(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def _run(cmd: List[str], cwd: Optional[Path] = None, timeout: int = 120) -> Tuple[int, str, str]:
    try:
        r = subprocess.run(cmd, cwd=str(cwd) if cwd else None, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout or "", r.stderr or ""
    except FileNotFoundError as e:
        return 127, "", str(e)
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"


def _hadoop_home() -> Path:
    return Path(os.environ.get("HADOOP_HOME", "/opt/hadoop"))


def _kafka_home() -> Path:
    return Path(os.environ.get("KAFKA_HOME", "/opt/kafka"))


def _spark_home() -> Path:
    return Path(os.environ.get("SPARK_HOME", "/opt/spark"))


def _hive_home() -> Path:
    env_home = os.environ.get("HIVE_HOME")
    if env_home:
        return Path(env_home)
    candidates = [
        Path("/home/hadoop/apache-hive-4.2.0-bin"),
        Path("/home/hadoop/apache-hive-3.1.3-bin"),
        Path("/opt/hive"),
    ]
    for p in candidates:
        if p.exists():
            return p
    return Path("/opt/hive")


def _hive_conf_dir(hh: Path) -> Path:
    """
    Directorio de configuración Hive (hive-site.xml).
    Por defecto HIVE_HOME/conf; se puede forzar con SIMLOG_HIVE_CONF_DIR o HIVE_CONF_DIR.
    """
    for key in ("SIMLOG_HIVE_CONF_DIR", "HIVE_CONF_DIR"):
        raw = os.environ.get(key)
        if raw:
            return Path(raw).expanduser()
    return hh / "conf"


def _hive_tail_log(log_path: Path, max_chars: int = 2500) -> str:
    if not log_path.exists():
        return "(sin fichero de log)"
    try:
        data = log_path.read_text(encoding="utf-8", errors="replace")
        return data[-max_chars:] if len(data) > max_chars else data
    except OSError as e:
        return f"(no se pudo leer el log: {e})"


def _cassandra_logs_dir() -> Path:
    return BASE / "cassandra" / "logs"


def _cassandra_tail_system_log(max_chars: int = 2500) -> str:
    return _hive_tail_log(_cassandra_logs_dir() / "system.log", max_chars=max_chars)


def _nifi_home() -> Path:
    return Path(os.environ.get("NIFI_HOME", "/opt/nifi"))


def _cassandra_bin() -> Path:
    return BASE / "cassandra" / "bin" / "cassandra"


def _cassandra_daemon_corriendo() -> bool:
    """True si hay un proceso del nodo Cassandra (evita lanzar un segundo JVM)."""
    r = subprocess.run(
        ["pgrep", "-f", "org.apache.cassandra.service.CassandraDaemon"],
        capture_output=True,
    )
    return r.returncode == 0


def _kill_port(port: int, host: str = "127.0.0.1") -> str:
    """Intenta terminar proceso que escucha en puerto (fuser/lsof)."""
    if not puerto_activo(host, port):
        return f"Puerto {port} ya estaba libre."
    for cmd in (
        ["fuser", "-k", f"{port}/tcp"],
        ["bash", "-c", f"lsof -ti:{port} | xargs -r kill -15"],
        ["bash", "-c", f"fuser -k {port}/tcp"],
    ):
        code, out, err = _run(cmd)
        if code == 0:
            return f"Señal enviada a proceso en puerto {port}."
    return f"No se pudo liberar el puerto {port} automáticamente. Revisa permisos o mata el proceso a mano."


def _pkill_pattern(pattern: str) -> str:
    r = subprocess.run(["pkill", "-f", pattern], capture_output=True, text=True)
    if r.returncode in (0, 1):
        return f"Procesos coincidentes con `{pattern}` terminados o no encontrados."
    return r.stderr or "pkill falló"


# --- Comprobar ---


def comprobar_hdfs() -> Dict[str, Any]:
    ok = puerto_activo("127.0.0.1", PORT_HDFS)
    return {
        "id": "hdfs",
        "nombre": "HDFS (NameNode)",
        "activo": ok,
        "detalle": f"Puerto {PORT_HDFS} {'abierto' if ok else 'cerrado'}",
        "puerto": PORT_HDFS,
    }


def comprobar_kafka() -> Dict[str, Any]:
    ok = puerto_activo("127.0.0.1", PORT_KAFKA)
    return {"id": "kafka", "nombre": "Kafka", "activo": ok, "detalle": f"Puerto {PORT_KAFKA}", "puerto": PORT_KAFKA}


def comprobar_cassandra() -> Dict[str, Any]:
    ok = puerto_activo("127.0.0.1", PORT_CASSANDRA)
    return {
        "id": "cassandra",
        "nombre": "Cassandra",
        "activo": ok,
        "detalle": f"Puerto {PORT_CASSANDRA}",
        "puerto": PORT_CASSANDRA,
    }


def comprobar_spark() -> Dict[str, Any]:
    """Spark standalone master (7077) o nota si no aplica."""
    # En algunas máquinas Spark master liga a 127.0.1.1 (hostname local) y no a 127.0.0.1.
    spark_hosts = ["127.0.0.1", "127.0.1.1", "localhost"]
    ok_master = puerto_activo_en_hosts(spark_hosts, PORT_SPARK_MASTER)
    sh = _spark_home()
    has_scripts = (sh / "sbin" / "start-master.sh").exists()
    return {
        "id": "spark",
        "nombre": "Spark (master standalone)",
        "activo": ok_master,
        "detalle": (
            f"Puerto master {PORT_SPARK_MASTER}: {'activo' if ok_master else 'inactivo'}. "
            f"Jobs `local[*]` no requieren daemon. Scripts en {sh}: {'sí' if has_scripts else 'no'}"
        ),
        "puerto": PORT_SPARK_MASTER,
    }


def comprobar_hive() -> Dict[str, Any]:
    ok = puerto_activo_en_hosts(["127.0.0.1", "127.0.1.1", "localhost"], PORT_HIVE)
    return {
        "id": "hive",
        "nombre": "HiveServer2",
        "activo": ok,
        "detalle": f"Puerto JDBC {PORT_HIVE}",
        "puerto": PORT_HIVE,
    }


def comprobar_airflow() -> Dict[str, Any]:
    ok = puerto_activo("127.0.0.1", PORT_AIRFLOW)
    # Scheduler no expone puerto fijo; comprobamos API/web
    return {
        "id": "airflow",
        "nombre": "Airflow (api-server / web)",
        "activo": ok,
        "detalle": f"Puerto {PORT_AIRFLOW} (api-server). El scheduler es otro proceso.",
        "puerto": PORT_AIRFLOW,
    }


def comprobar_nifi() -> Dict[str, Any]:
    ok_https = puerto_activo("127.0.0.1", PORT_NIFI_HTTPS)
    ok_http = puerto_activo("127.0.0.1", PORT_NIFI_HTTP)
    ok = ok_https or ok_http
    return {
        "id": "nifi",
        "nombre": "Apache NiFi",
        "activo": ok,
        "detalle": f"HTTPS {PORT_NIFI_HTTPS}: {'sí' if ok_https else 'no'} · HTTP {PORT_NIFI_HTTP}: {'sí' if ok_http else 'no'}",
        "puerto": PORT_NIFI_HTTPS,
    }


COMPRUEBA: Dict[str, Callable[[], Dict[str, Any]]] = {
    "hdfs": comprobar_hdfs,
    "kafka": comprobar_kafka,
    "cassandra": comprobar_cassandra,
    "spark": comprobar_spark,
    "hive": comprobar_hive,
    "airflow": comprobar_airflow,
    "nifi": comprobar_nifi,
}

ORDEN_SERVICIOS: List[str] = ["hdfs", "kafka", "cassandra", "spark", "hive", "airflow", "nifi"]

# Orden recomendado al arrancar todo el stack (HDFS y Cassandra antes que Kafka)
ORDEN_ARRANQUE_TODOS: List[str] = ["hdfs", "cassandra", "kafka", "spark", "hive", "airflow", "nifi"]

# Parada inversa: clientes/orquestación antes que almacenamiento
ORDEN_PARADA_TODOS: List[str] = ["nifi", "airflow", "hive", "spark", "kafka", "cassandra", "hdfs"]


def comprobar_todos() -> List[Dict[str, Any]]:
    return [COMPRUEBA[sid]() for sid in ORDEN_SERVICIOS]


# --- Iniciar ---


def iniciar_hdfs() -> str:
    if puerto_activo("127.0.0.1", PORT_HDFS):
        return "HDFS ya estaba activo (NameNode)."
    hh = _hadoop_home()
    script = hh / "sbin" / "start-dfs.sh"
    if script.exists():
        code, out, err = _run([str(script)], cwd=hh, timeout=90)
        if code != 0:
            return f"start-dfs.sh código {code}: {err[-500:]}"
        return "HDFS: start-dfs.sh ejecutado. Espera unos segundos y comprueba."
    code, out, err = _run(["bash", "-c", "start-dfs.sh"], cwd=str(BASE), timeout=90)
    if code != 0:
        return f"No se encontró start-dfs.sh en HADOOP_HOME={hh}. Configura HADOOP_HOME."
    return "HDFS: arranque lanzado."


def iniciar_kafka() -> str:
    if puerto_activo("127.0.0.1", PORT_KAFKA):
        return "Kafka ya estaba activo."
    kh = _kafka_home()
    start = kh / "bin" / "kafka-server-start.sh"
    config = kh / "config" / "server.properties"
    if not config.exists():
        alt = kh / "config" / "kraft" / "server.properties"
        if alt.exists():
            config = alt
    if not start.exists():
        return f"No se encontró {start}. Configura KAFKA_HOME."
    _popen_bg([str(start), str(config)], cwd=kh)
    return "Kafka arrancando en segundo plano (espera 10–30 s y comprueba)."


def iniciar_cassandra() -> str:
    if puerto_activo("127.0.0.1", PORT_CASSANDRA):
        return "Cassandra ya estaba activa."
    cb = _cassandra_bin()
    if not cb.exists():
        return f"No se encontró {cb}. Usa la instalación embebida del proyecto o arranca Cassandra manualmente."
    if not _cassandra_daemon_corriendo():
        _popen_bg([str(cb)], cwd=BASE)
    # Si ya hay un CassandraDaemon (arranque lento), no lanzar otro JVM sobre el mismo data/.
    max_wait = max(45, int(os.environ.get("SIMLOG_CASSANDRA_MAX_WAIT_SEC", "180")))
    paso = 3
    t0 = time.monotonic()
    while time.monotonic() - t0 < max_wait:
        if puerto_activo("127.0.0.1", PORT_CASSANDRA):
            return "Cassandra activa (puerto CQL 9042)."
        time.sleep(paso)
    tail = _cassandra_tail_system_log()
    return (
        "Cassandra se lanzó pero no abrió el puerto CQL a tiempo. "
        f"Revisa {_cassandra_logs_dir()}. Últimas líneas de system.log:\n---\n{tail}\n---"
    )


def iniciar_spark() -> str:
    spark_hosts = ["127.0.0.1", "127.0.1.1", "localhost"]
    if puerto_activo_en_hosts(spark_hosts, PORT_SPARK_MASTER):
        return "Spark Master ya responde en el puerto configurado."
    sh = _spark_home()
    sm = sh / "sbin" / "start-master.sh"
    if not sm.exists():
        return (
            "No hay Spark standalone en SPARK_HOME (o no usa start-master.sh). "
            "Los jobs del proyecto usan `local[*]` sin daemon obligatorio."
        )
    code, _, err = _run(["bash", str(sm)], cwd=sh, timeout=60)
    if code != 0 and "already running" not in (err or "").lower():
        return f"Spark master: {err[-400:] if err else 'error'}"
    return "Spark Master: arranque lanzado (comprueba puerto 7077)."


def iniciar_hive() -> str:
    hive_hosts = ["127.0.0.1", "127.0.1.1", "localhost"]
    if puerto_activo_en_hosts(hive_hosts, PORT_HIVE):
        return "HiveServer2 ya parece activo (puerto JDBC)."
    hh = _hive_home()
    conf_dir = _hive_conf_dir(hh)
    hive_bin = hh / "bin" / "hive"
    if not hive_bin.exists():
        hive_bin = Path("/usr/bin/hive")
    if not hive_bin.exists():
        return f"No se encontró `hive` en HIVE_HOME={hh}."

    log_path = Path(os.environ.get("SIMLOG_HIVE_LOG", "/tmp/hadoop/hiveserver2-daemon.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)

    pid_path = hh / "conf" / "hiveserver2.pid" if hh.exists() else Path("/tmp/hiveserver2.pid")
    if pid_path.exists():
        try:
            old_pid = int(pid_path.read_text(encoding="utf-8").strip())
            # Si el PID no existe, limpiamos archivo stale para que hiveserver2 no se bloquee.
            if subprocess.run(["kill", "-0", str(old_pid)], capture_output=True).returncode != 0:
                pid_path.unlink(missing_ok=True)
        except Exception:
            pid_path.unlink(missing_ok=True)

    # Entorno aislado: HADOOP_CLASSPATH vacío evita NoClassDefFoundError / conflictos con JARs de Hadoop.
    env = os.environ.copy()
    env["HADOOP_CLASSPATH"] = ""
    env["HIVE_CONF_DIR"] = str(conf_dir)
    if hh.exists():
        env.setdefault("HIVE_HOME", str(hh))

    try:
        log_f = open(log_path, "ab", buffering=0)
    except OSError as e:
        return f"No se pudo abrir el log de HiveServer2 ({log_path}): {e}"

    try:
        subprocess.Popen(
            [str(hive_bin), "--service", "hiveserver2"],
            cwd=str(hh) if hh.exists() else str(BASE),
            env=env,
            stdout=log_f,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception as e:
        log_f.close()
        return f"No se pudo lanzar HiveServer2: {e}"
    log_f.close()

    # Comprobación activa (Hive + Derby/metastore pueden tardar varios minutos en máquinas lentas).
    max_wait = max(90, int(os.environ.get("SIMLOG_HIVE_MAX_WAIT_SEC", "420")))
    paso = 5
    t0 = time.monotonic()
    while time.monotonic() - t0 < max_wait:
        if puerto_activo_en_hosts(hive_hosts, PORT_HIVE):
            return "HiveServer2 activo y escuchando en el puerto JDBC."
        time.sleep(paso)

    tail = _hive_tail_log(log_path)
    extra = ""
    if "Another instance of Derby" in tail or "Failed to start database" in tail or "db.lck" in tail.lower():
        extra = (
            " Posible base Derby bloqueada: cierra otros Hive/metastore, o revisa locks en "
            "`metastore_db/` o la ruta de `javax.jdo.option.ConnectionURL` en hive-site.xml."
        )
    return (
        "HiveServer2 se lanzó pero no abrió el puerto JDBC a tiempo. "
        f"Revisa {log_path}.{extra} Últimas líneas del log:\n---\n{tail}\n---"
    )


def esperar_cassandra(timeout_sec: int = 120, paso: int = 3) -> Tuple[bool, str]:
    """
    Espera a que el puerto CQL (9042) de Cassandra responda.
    Útil tras arranque en segundo plano o para comprobar el stack completo.
    """
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_sec:
        if puerto_activo("127.0.0.1", PORT_CASSANDRA):
            return True, "Cassandra responde en el puerto CQL."
        time.sleep(paso)
    return False, (
        f"No se detectó el puerto CQL {PORT_CASSANDRA} tras {timeout_sec}s. "
        f"Revisa {_cassandra_logs_dir() / 'system.log'}"
    )


def esperar_hiveserver2(timeout_sec: int = 240, paso: int = 5) -> Tuple[bool, str]:
    """
    Solo espera a que el puerto JDBC de HiveServer2 responda (sin relanzar procesos).
    Útil tras `arrancar_todos_servicios` si Hive sigue arrancando en segundo plano.
    """
    hive_hosts = ["127.0.0.1", "127.0.1.1", "localhost"]
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_sec:
        if puerto_activo_en_hosts(hive_hosts, PORT_HIVE):
            return True, "HiveServer2 responde en el puerto JDBC."
        time.sleep(paso)
    return False, (
        f"No se detectó el puerto JDBC {PORT_HIVE} tras {timeout_sec}s. "
        "Revisa /tmp/hadoop/hiveserver2-daemon.log"
    )


def _airflow_scheduler_activo() -> bool:
    r = subprocess.run(["pgrep", "-f", "airflow scheduler"], capture_output=True)
    return r.returncode == 0


def _env_airflow_api_alineado() -> dict:
    """
    Alinea el Execution API con el puerto real del api-server (SIMLOG_PORT_AIRFLOW).
    Si no, LocalExecutor usa http://localhost:8080/execution/ y las tareas quedan en cola.
    """
    env = os.environ.copy()
    env.setdefault("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
    base = f"http://127.0.0.1:{PORT_AIRFLOW}"
    env.setdefault("AIRFLOW__API__BASE_URL", base)
    env.setdefault("AIRFLOW__API__PORT", str(PORT_AIRFLOW))
    return env


def _iniciar_airflow_scheduler_si_hace_falta() -> str:
    """Arranca `airflow scheduler` en segundo plano si no hay uno en marcha."""
    if _airflow_scheduler_activo():
        return "Airflow scheduler ya estaba en ejecución."
    env = _env_airflow_api_alineado()
    airflow_exe = shutil.which("airflow")
    if airflow_exe:
        cmd = [airflow_exe, "scheduler"]
    else:
        cmd = [sys.executable, "-m", "airflow", "scheduler"]
    try:
        subprocess.Popen(
            cmd,
            env=env,
            cwd=str(BASE),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return "Airflow scheduler lanzado en segundo plano."
    except Exception as e:
        return f"No se pudo lanzar el scheduler: {e}"


def iniciar_airflow() -> str:
    if puerto_activo("127.0.0.1", PORT_AIRFLOW):
        sch = _iniciar_airflow_scheduler_si_hace_falta()
        return f"Airflow api-server ya escucha (puerto {PORT_AIRFLOW}). {sch}"
    env = _env_airflow_api_alineado()
    airflow_exe = shutil.which("airflow")
    if airflow_exe:
        cmd = [airflow_exe, "api-server", "-p", str(PORT_AIRFLOW), "-H", "0.0.0.0"]
    else:
        cmd = [sys.executable, "-m", "airflow", "api-server", "-p", str(PORT_AIRFLOW), "-H", "0.0.0.0"]
    try:
        subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        for _ in range(24):
            if puerto_activo("127.0.0.1", PORT_AIRFLOW):
                sch = _iniciar_airflow_scheduler_si_hace_falta()
                return f"Airflow api-server activo (puerto {PORT_AIRFLOW}). {sch}"
            time.sleep(1)
        return (
            f"Airflow se lanzó pero no abrió el puerto {PORT_AIRFLOW} a tiempo. "
            "Revisa configuración de Airflow/AIRFLOW_HOME."
        )
    except Exception as e:
        return f"No se pudo lanzar Airflow: {e}"


def iniciar_nifi() -> str:
    if puerto_activo("127.0.0.1", PORT_NIFI_HTTPS) or puerto_activo("127.0.0.1", PORT_NIFI_HTTP):
        return "NiFi ya tiene un puerto en escucha."
    nh = _nifi_home()
    script = nh / "bin" / "nifi.sh"
    if not script.exists():
        return f"No se encontró {script}. Configura NIFI_HOME."
    code, _, err = _run([str(script), "start"], cwd=nh, timeout=120)
    if code != 0:
        return f"nifi.sh start: {err[-500:] if err else 'error'}"
    return "NiFi: arranque solicitado (puede tardar 1–2 min)."


INICIA: Dict[str, Callable[[], str]] = {
    "hdfs": iniciar_hdfs,
    "kafka": iniciar_kafka,
    "cassandra": iniciar_cassandra,
    "spark": iniciar_spark,
    "hive": iniciar_hive,
    "airflow": iniciar_airflow,
    "nifi": iniciar_nifi,
}


def arrancar_stack_basico() -> List[str]:
    """
    Arranque encadenado para la ingesta SIMLOG: **HDFS → Cassandra → Kafka**.
    Devuelve una línea de resultado por paso (mostrar en el dashboard).
    """
    out: List[str] = []
    out.append(f"HDFS: {iniciar_hdfs()}")
    time.sleep(2)
    out.append(f"Cassandra: {iniciar_cassandra()}")
    time.sleep(1)
    out.append(f"Kafka: {iniciar_kafka()}")
    return out


def arrancar_todos_servicios(*, verbose: bool = False) -> List[str]:
    """
    Intenta iniciar **todos** los servicios (orden: HDFS → Cassandra → Kafka → …).
    Puede tardar varios minutos; algunos pueden no aplicar en tu máquina (mensaje informativo).
    """
    out: List[str] = []
    for sid in ORDEN_ARRANQUE_TODOS:
        fn = INICIA.get(sid)
        if not fn:
            continue
        if verbose:
            print(f"→ Iniciando {sid}… (espera; Hive/Spark pueden tardar 1–2 min)", flush=True)
        try:
            out.append(f"{sid}: {fn()}")
        except Exception as e:
            out.append(f"{sid}: Error — {e}")
        time.sleep(1)
    return out


def parar_todos_servicios(*, verbose: bool = False) -> List[str]:
    """
    Detiene el stack en orden inverso a las dependencias (NiFi → … → HDFS).
    """
    out: List[str] = []
    for sid in ORDEN_PARADA_TODOS:
        fn = PARA.get(sid)
        if not fn:
            continue
        if verbose:
            print(f"→ Deteniendo {sid}…", flush=True)
        try:
            out.append(f"{sid}: {fn()}")
        except Exception as e:
            out.append(f"{sid}: Error — {e}")
        time.sleep(1)
    return out


# --- Parar ---


def parar_hdfs() -> str:
    hh = _hadoop_home()
    stop = hh / "sbin" / "stop-dfs.sh"
    if stop.exists():
        code, _, err = _run([str(stop)], cwd=hh, timeout=90)
        return f"HDFS stop-dfs.sh: código {code}. {err[-300:] if err else ''}"
    return "No se encontró stop-dfs.sh. Para el cluster manualmente."


def parar_kafka() -> str:
    kh = _kafka_home()
    stop = kh / "bin" / "kafka-server-stop.sh"
    if stop.exists():
        code, _, err = _run([str(stop)], cwd=kh, timeout=60)
        return f"Kafka stop: código {code}. {err[-200:] if err else ''}"
    return _pkill_pattern("kafka.Kafka")


def parar_cassandra() -> str:
    # nodetool stopdaemon si hay cluster
    nodetool = BASE / "cassandra" / "bin" / "nodetool"
    if nodetool.exists():
        code, out, err = _run([str(nodetool), "stopdaemon"], cwd=BASE, timeout=60)
        if code == 0:
            return "Cassandra: nodetool stopdaemon ejecutado."
    return _pkill_pattern("org.apache.cassandra.service.CassandraDaemon")


def parar_spark() -> str:
    sh = _spark_home()
    stp = sh / "sbin" / "stop-master.sh"
    if stp.exists():
        _run([str(stp)], cwd=sh, timeout=30)
    _run([str(sh / "sbin" / "stop-workers.sh")], cwd=sh, timeout=30)
    return "Spark: stop-master/workers ejecutado (si existían)." if stp.exists() else "No hay scripts stop en SPARK_HOME."


def parar_hive() -> str:
    msg = _kill_port(PORT_HIVE)
    for _ in range(12):
        if not puerto_activo_en_hosts(["127.0.0.1", "127.0.1.1", "localhost"], PORT_HIVE):
            return msg
        time.sleep(1)
    return f"{msg} (el puerto {PORT_HIVE} sigue activo; puede tardar en cerrar)."


def parar_airflow() -> str:
    msg_api = _kill_port(PORT_AIRFLOW)
    pk = _pkill_pattern("airflow scheduler")
    return f"{msg_api} · {pk}"


def parar_nifi() -> str:
    nh = _nifi_home()
    script = nh / "bin" / "nifi.sh"
    if script.exists():
        code, _, err = _run([str(script), "stop"], cwd=nh, timeout=120)
        return f"NiFi stop: código {code}. {err[-300:] if err else ''}"
    return _pkill_pattern("nifi")


PARA: Dict[str, Callable[[], str]] = {
    "hdfs": parar_hdfs,
    "kafka": parar_kafka,
    "cassandra": parar_cassandra,
    "spark": parar_spark,
    "hive": parar_hive,
    "airflow": parar_airflow,
    "nifi": parar_nifi,
}


def ejecutar_comprobar(svc_id: str) -> Dict[str, Any]:
    fn = COMPRUEBA.get(svc_id)
    if not fn:
        return {"error": f"Servicio desconocido: {svc_id}"}
    return fn()


def ejecutar_iniciar(svc_id: str) -> str:
    fn = INICIA.get(svc_id)
    if not fn:
        return f"Servicio desconocido: {svc_id}"
    try:
        return fn()
    except Exception as e:
        return f"Error: {e}"


def ejecutar_parar(svc_id: str) -> str:
    fn = PARA.get(svc_id)
    if not fn:
        return f"Servicio desconocido: {svc_id}"
    try:
        return fn()
    except Exception as e:
        return f"Error: {e}"
