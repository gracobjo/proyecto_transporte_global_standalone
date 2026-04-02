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
import urllib.error
import urllib.request
import ssl
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

BASE = Path(__file__).resolve().parent.parent

# Puertos por defecto (sobrescribibles con SIMLOG_PORT_*)
PORT_HDFS = int(os.environ.get("SIMLOG_PORT_HDFS", "9870"))
PORT_KAFKA = int(os.environ.get("SIMLOG_PORT_KAFKA", "9092"))
PORT_CASSANDRA = int(os.environ.get("SIMLOG_PORT_CASSANDRA", "9042"))
PORT_HIVE = int(os.environ.get("SIMLOG_PORT_HIVE", "10000"))
PORT_HIVE_METASTORE = int(os.environ.get("SIMLOG_PORT_HIVE_METASTORE", "9083"))
PORT_SPARK_MASTER = int(os.environ.get("SIMLOG_PORT_SPARK_MASTER", "7077"))
# UI HTTP del Spark Standalone Master (no confundir con 7077, que es el RPC del master).
PORT_SPARK_MASTER_UI = int(os.environ.get("SIMLOG_PORT_SPARK_MASTER_UI", "8080"))
PORT_AIRFLOW = int(os.environ.get("SIMLOG_PORT_AIRFLOW", "8088"))
PORT_API = int(os.environ.get("SIMLOG_PORT_API", "8090"))
PORT_FAQ_IA = int(os.environ.get("SIMLOG_PORT_FAQ_IA", "8091"))
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
    project_conf = BASE / "hive" / "conf"
    if (project_conf / "hive-site.xml").exists():
        return project_conf
    return hh / "conf"


def _hive_hosts() -> List[str]:
    return ["127.0.0.1", "127.0.1.1", "localhost"]


def _hive_env(hh: Path, conf_dir: Path) -> Dict[str, str]:
    env = os.environ.copy()
    env["HADOOP_CLASSPATH"] = ""
    env["HIVE_CONF_DIR"] = str(conf_dir)
    env.setdefault("SIMLOG_HIVE_CONF_DIR", str(conf_dir))
    env.setdefault("HIVE_METASTORE_URIS", f"thrift://127.0.0.1:{PORT_HIVE_METASTORE}")
    if hh.exists():
        env.setdefault("HIVE_HOME", str(hh))
    return env


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
    # Este proyecto debe priorizar siempre su instalación local de NiFi.
    # `nifi/` contiene documentación y flujos; el runtime arrancable está en
    # directorios tipo `nifi-<version>` dentro del repo.
    candidates: List[Path] = [BASE / "nifi-2.0.0"]
    candidates.extend(sorted((p for p in BASE.glob("nifi-*") if p.is_dir()), reverse=True))

    explicit = os.environ.get("SIMLOG_NIFI_HOME", "").strip() or os.environ.get("NIFI_HOME", "").strip()
    if explicit:
        explicit_path = Path(explicit).expanduser().resolve()
        try:
            explicit_path.relative_to(BASE.resolve())
        except ValueError:
            explicit_path = None  # type: ignore[assignment]
        if explicit_path is not None:
            candidates.insert(0, explicit_path)

    seen: set[str] = set()
    for p in candidates:
        key = str(p)
        if key in seen:
            continue
        seen.add(key)
        if (p / "bin" / "nifi.sh").exists():
            return p

    # Fallback conservador: no salir del repo.
    return BASE / "nifi-2.0.0"


def _leer_properties(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            out[key.strip()] = value.strip()
    except OSError:
        return {}
    return out


def _int_or_none(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return None


def _nifi_web_config(nh: Optional[Path] = None) -> Dict[str, Any]:
    nh = nh or _nifi_home()
    props = _leer_properties(nh / "conf" / "nifi.properties")
    has_props = bool(props)
    http_port = _int_or_none(props.get("nifi.web.http.port"))
    https_port = _int_or_none(props.get("nifi.web.https.port"))
    return {
        "http_host": props.get("nifi.web.http.host") or "127.0.0.1",
        "http_port": http_port,
        "https_host": props.get("nifi.web.https.host") or "localhost",
        "https_port": https_port if has_props else PORT_NIFI_HTTPS,
    }


def _nifi_probe_targets(nh: Optional[Path] = None) -> List[Tuple[str, str, int]]:
    cfg = _nifi_web_config(nh)
    out: List[Tuple[str, str, int]] = []
    seen: set[Tuple[str, str, int]] = set()

    def add(scheme: str, host: str, port: Optional[int], extra_hosts: List[str]) -> None:
        if not port:
            return
        for candidate in [host, *extra_hosts]:
            item = (scheme, candidate, port)
            if item not in seen:
                seen.add(item)
                out.append(item)

    add("https", str(cfg["https_host"]), cfg["https_port"], ["localhost", "127.0.0.1"])
    add("http", str(cfg["http_host"]), cfg["http_port"], ["127.0.0.1", "localhost"])
    return out


def _nifi_conflictos_puertos(nh: Optional[Path] = None) -> List[str]:
    conflictos: List[str] = []
    for scheme, host, port in _nifi_probe_targets(nh):
        if not puerto_activo(host, port):
            continue
        url = f"{scheme}://{host}:{port}/nifi"
        if not _endpoint_parece_nifi(url):
            conflictos.append(f"{scheme.upper()} {host}:{port} ocupado por otro servicio")
    return conflictos


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


def _proceso_nifi_corriendo() -> bool:
    """True si existe una JVM de NiFi en ejecución."""
    r = subprocess.run(
        ["pgrep", "-f", "org.apache.nifi.NiFi"],
        capture_output=True,
    )
    return r.returncode == 0


def _endpoint_parece_nifi(url: str, timeout: float = 2.0) -> bool:
    """
    Detecta firma básica de NiFi en la respuesta HTTP.
    Evita falsos positivos cuando otro servicio ocupa el puerto (ej. Spark en 8080).
    """
    try:
        req = urllib.request.Request(url, method="GET")
        context = None
        if url.startswith("https://"):
            context = ssl._create_unverified_context()
        with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
            body = resp.read(2048).decode("utf-8", errors="ignore").lower()
            headers = str(resp.headers).lower()
            markers = ("nifi", "apache nifi", "x-frame-options")
            return any(m in body for m in markers) or "nifi" in headers
    except (urllib.error.URLError, TimeoutError, ValueError, OSError):
        return False


def _endpoint_http_ok(url: str, timeout: float = 2.0) -> bool:
    """True si un endpoint HTTP(S) responde con código < 400."""
    try:
        req = urllib.request.Request(url, method="GET")
        context = None
        if url.startswith("https://"):
            context = ssl._create_unverified_context()
        with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
            return int(getattr(resp, "status", 200)) < 400
    except (urllib.error.URLError, TimeoutError, ValueError, OSError):
        return False


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
            f"RPC {PORT_SPARK_MASTER}: {'activo' if ok_master else 'inactivo'} · UI HTTP :{PORT_SPARK_MASTER_UI}. "
            f"Master «local» por defecto en código → 0 apps/workers en el master. "
            f"SPARK_HOME scripts: {'sí' if has_scripts else 'no'}"
        ),
        "puerto": PORT_SPARK_MASTER,
    }


def comprobar_hive() -> Dict[str, Any]:
    hosts = _hive_hosts()
    ok_hs2 = puerto_activo_en_hosts(hosts, PORT_HIVE)
    ok_meta = puerto_activo_en_hosts(hosts, PORT_HIVE_METASTORE)
    return {
        "id": "hive",
        "nombre": "HiveServer2",
        "activo": ok_hs2 and ok_meta,
        "activo_jdbc": ok_hs2,
        "activo_metastore": ok_meta,
        "necesita_solo_hiveserver2": ok_meta and not ok_hs2,
        "detalle": (
            f"JDBC {PORT_HIVE}: {'activo' if ok_hs2 else 'inactivo'} · "
            f"Metastore {PORT_HIVE_METASTORE}: {'activo' if ok_meta else 'inactivo'}"
        ),
        "puerto": PORT_HIVE,
    }


def comprobar_airflow() -> Dict[str, Any]:
    port_ok = puerto_activo("127.0.0.1", PORT_AIRFLOW)
    api_ok = _endpoint_http_ok(f"http://127.0.0.1:{PORT_AIRFLOW}/health")
    ui_ok = _endpoint_http_ok(f"http://127.0.0.1:{PORT_AIRFLOW}/")
    ok = port_ok and (api_ok or ui_ok)
    detalle = (
        f"Puerto {PORT_AIRFLOW}: {'abierto' if port_ok else 'cerrado'} · "
        f"endpoint /health: {'ok' if api_ok else 'no'} · "
        f"raíz /: {'ok' if ui_ok else 'no'}. "
        "El scheduler es un proceso separado."
    )
    return {
        "id": "airflow",
        "nombre": "Airflow (api-server / web)",
        "activo": ok,
        "detalle": detalle,
        "puerto": PORT_AIRFLOW,
    }


def comprobar_api() -> Dict[str, Any]:
    port_ok = puerto_activo("127.0.0.1", PORT_API)
    docs_ok = _endpoint_http_ok(f"http://127.0.0.1:{PORT_API}/docs")
    health_ok = _endpoint_http_ok(f"http://127.0.0.1:{PORT_API}/health")
    ok = port_ok and (docs_ok or health_ok)
    return {
        "id": "api",
        "nombre": "Swagger API (FastAPI)",
        "activo": ok,
        "detalle": (
            f"Puerto {PORT_API}: {'abierto' if port_ok else 'cerrado'} · "
            f"/docs: {'ok' if docs_ok else 'no'} · /health: {'ok' if health_ok else 'no'}"
        ),
        "puerto": PORT_API,
    }


def comprobar_faq_ia() -> Dict[str, Any]:
    port_ok = puerto_activo("127.0.0.1", PORT_FAQ_IA)
    docs_ok = _endpoint_http_ok(f"http://127.0.0.1:{PORT_FAQ_IA}/docs")
    health_ok = _endpoint_http_ok(f"http://127.0.0.1:{PORT_FAQ_IA}/health")
    ok = port_ok and (docs_ok or health_ok)
    return {
        "id": "faq_ia",
        "nombre": "FAQ IA API (FastAPI)",
        "activo": ok,
        "detalle": (
            f"Puerto {PORT_FAQ_IA}: {'abierto' if port_ok else 'cerrado'} · "
            f"/docs: {'ok' if docs_ok else 'no'} · /health: {'ok' if health_ok else 'no'}"
        ),
        "puerto": PORT_FAQ_IA,
    }


def comprobar_nifi() -> Dict[str, Any]:
    nh = _nifi_home()
    cfg = _nifi_web_config(nh)
    proceso = _proceso_nifi_corriendo()

    encontrados: List[str] = []
    ocupacion: List[str] = []
    for scheme, host, port in _nifi_probe_targets(nh):
        up = puerto_activo(host, port)
        if not up:
            continue
        url = f"{scheme}://{host}:{port}/nifi"
        if _endpoint_parece_nifi(url):
            encontrados.append(f"{scheme.upper()} {host}:{port}")
        else:
            ocupacion.append(f"{scheme.upper()} {host}:{port} abierto sin firma NiFi")

    ok = bool(encontrados)
    if ok:
        detalle = (
            f"NiFi detectado en {', '.join(encontrados)} · proceso JVM: {'sí' if proceso else 'no'} · "
            f"NIFI_HOME={nh}"
        )
    else:
        cfg_txt = (
            f"config HTTPS={cfg.get('https_host')}:{cfg.get('https_port') or 'off'} · "
            f"HTTP={cfg.get('http_host')}:{cfg.get('http_port') or 'off'}"
        )
        extra = ("; ".join(ocupacion)) if ocupacion else "sin puertos NiFi en escucha"
        detalle = (
            f"No se detecta NiFi ({extra}). Proceso JVM NiFi: {'sí' if proceso else 'no'} · "
            f"{cfg_txt} · NIFI_HOME={nh}"
        )

    return {
        "id": "nifi",
        "nombre": "Apache NiFi",
        "activo": ok,
        "detalle": detalle,
        "puerto": cfg.get("https_port") or cfg.get("http_port") or PORT_NIFI_HTTPS,
    }


COMPRUEBA: Dict[str, Callable[[], Dict[str, Any]]] = {
    "hdfs": comprobar_hdfs,
    "kafka": comprobar_kafka,
    "cassandra": comprobar_cassandra,
    "spark": comprobar_spark,
    "hive": comprobar_hive,
    "airflow": comprobar_airflow,
    "api": comprobar_api,
    "faq_ia": comprobar_faq_ia,
    "nifi": comprobar_nifi,
}

ORDEN_SERVICIOS: List[str] = ["hdfs", "kafka", "cassandra", "spark", "hive", "airflow", "api", "faq_ia", "nifi"]

# Orden recomendado al arrancar todo el stack (HDFS y Cassandra antes que Kafka)
ORDEN_ARRANQUE_TODOS: List[str] = ["hdfs", "cassandra", "kafka", "spark", "hive", "airflow", "api", "faq_ia", "nifi"]

# Parada inversa: clientes/orquestación antes que almacenamiento
ORDEN_PARADA_TODOS: List[str] = ["nifi", "faq_ia", "api", "airflow", "hive", "spark", "kafka", "cassandra", "hdfs"]


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
    return (
        f"Spark Master: arranque lanzado (RPC {PORT_SPARK_MASTER}, UI ~{PORT_SPARK_MASTER_UI}). "
        "Sin `start-worker.sh` la UI muestra 0 workers. "
        "Los jobs con SPARK_MASTER=local no se listan en el master."
    )


def iniciar_hive() -> str:
    hive_hosts = _hive_hosts()
    hh = _hive_home()
    conf_dir = _hive_conf_dir(hh)
    hive_bin = hh / "bin" / "hive"
    if not hive_bin.exists():
        hive_bin = Path("/usr/bin/hive")
    if not hive_bin.exists():
        return f"No se encontró `hive` en HIVE_HOME={hh}."

    log_path = Path(os.environ.get("SIMLOG_HIVE_LOG", "/tmp/hadoop/hiveserver2-daemon.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Hive usa `HIVE_CONF_DIR/hiveserver2.pid` (ver `bin/ext/hiveserver2.sh`).
    # Si limpiamos otro fichero, el launcher puede seguir creyendo que hay una instancia viva.
    pid_path = (conf_dir / "hiveserver2.pid") if conf_dir else (hh / "conf" / "hiveserver2.pid")
    if pid_path.exists():
        try:
            old_pid = int(pid_path.read_text(encoding="utf-8").strip())
            # Si el PID no existe, limpiamos archivo stale para que hiveserver2 no se bloquee.
            if subprocess.run(["kill", "-0", str(old_pid)], capture_output=True).returncode != 0:
                pid_path.unlink(missing_ok=True)
        except Exception:
            pid_path.unlink(missing_ok=True)

    env = _hive_env(hh, conf_dir)

    meta_msg = iniciar_hive_metastore()
    if not puerto_activo_en_hosts(hive_hosts, PORT_HIVE_METASTORE):
        return (
            "No se pudo dejar operativo el metastore Hive. "
            f"{meta_msg}"
        )
    if puerto_activo_en_hosts(hive_hosts, PORT_HIVE):
        return f"HiveServer2 ya parece activo (puerto JDBC). {meta_msg}"

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


def ensure_hiveserver2() -> str:
    """
    Asegura HiveServer2 (puerto JDBC) y el metastore. Idempotente: si ya responden, no hace nada.

    Caso habitual tras reinicios o Spark/Airflow: **metastore (9083) arriba** y **HiveServer2 (10000) abajo**.
    Equivale a «Iniciar Hive» en la UI pero con nombre explícito para scripts y cron.

    Para dejar HS2 **persistente** tras reboot, instala la unidad systemd
    `orquestacion/systemd/simlog-hiveserver2.service` (ver comentarios en el fichero).
    """
    return iniciar_hive()


def iniciar_hive_metastore() -> str:
    hosts = _hive_hosts()
    if puerto_activo_en_hosts(hosts, PORT_HIVE_METASTORE):
        return "Metastore Hive ya estaba activo."
    hh = _hive_home()
    conf_dir = _hive_conf_dir(hh)
    hive_bin = hh / "bin" / "hive"
    if not hive_bin.exists():
        hive_bin = Path("/usr/bin/hive")
    if not hive_bin.exists():
        return f"No se encontró `hive` en HIVE_HOME={hh}."

    log_path = Path(os.environ.get("SIMLOG_HIVE_METASTORE_LOG", "/tmp/hadoop/hive-metastore.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = _hive_env(hh, conf_dir)

    try:
        log_f = open(log_path, "ab", buffering=0)
    except OSError as e:
        return f"No se pudo abrir el log del metastore Hive ({log_path}): {e}"

    try:
        subprocess.Popen(
            [str(hive_bin), "--service", "metastore"],
            cwd=str(hh) if hh.exists() else str(BASE),
            env=env,
            stdout=log_f,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception as e:
        log_f.close()
        return f"No se pudo lanzar el metastore Hive: {e}"
    log_f.close()

    ok, msg = esperar_hive_metastore(
        timeout_sec=max(45, int(os.environ.get("SIMLOG_HIVE_METASTORE_MAX_WAIT_SEC", "180"))),
        paso=3,
    )
    if ok:
        return msg
    tail = _hive_tail_log(log_path)
    extra = ""
    if "Another instance of Derby" in tail or "XSDB6" in tail:
        extra = (
            " Derby sigue bloqueado por otro proceso Hive. "
            "Debes parar HiveServer2 antes de levantar el metastore compartido."
        )
    return f"{msg}{extra} Últimas líneas del log:\n---\n{tail}\n---"


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


def esperar_hive_metastore(timeout_sec: int = 120, paso: int = 3) -> Tuple[bool, str]:
    hosts = _hive_hosts()
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_sec:
        if puerto_activo_en_hosts(hosts, PORT_HIVE_METASTORE):
            return True, f"Metastore Hive responde en el puerto {PORT_HIVE_METASTORE}."
        time.sleep(paso)
    return False, (
        f"No se detectó el metastore Hive en el puerto {PORT_HIVE_METASTORE} tras {timeout_sec}s. "
        "Revisa /tmp/hadoop/hive-metastore.log"
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
    nh = _nifi_home()
    script = nh / "bin" / "nifi.sh"
    if not script.exists():
        return f"No se encontró {script}. Configura NIFI_HOME."

    estado = comprobar_nifi()
    if estado.get("activo"):
        return "NiFi ya estaba activo."

    conflictos = _nifi_conflictos_puertos(nh)
    if conflictos:
        return (
            "NiFi no se puede iniciar porque sus puertos configurados están ocupados por otro servicio: "
            + "; ".join(conflictos)
        )

    code, _, err = _run([str(script), "start"], cwd=nh, timeout=45)
    if code not in (0, -1):
        return f"nifi.sh start: {err[-500:] if err else 'error'}"

    max_wait = max(90, int(os.environ.get("SIMLOG_NIFI_MAX_WAIT_SEC", "240")))
    t0 = time.monotonic()
    while time.monotonic() - t0 < max_wait:
        estado = comprobar_nifi()
        if estado.get("activo"):
            return str(estado.get("detalle") or "NiFi activo.")
        time.sleep(3)

    code_st, out_st, err_st = _run([str(script), "status"], cwd=nh, timeout=30)
    status_txt = (out_st or err_st or f"código {code_st}").strip()
    if "Status: UP" in status_txt or "Command Status [SUCCESS]" in status_txt:
        return f"NiFi activo según bootstrap. {status_txt[-400:]}"
    if _proceso_nifi_corriendo():
        return (
            "NiFi sigue arrancando en segundo plano; el proceso JVM ya existe pero la UI aún no responde. "
            f"Estado bootstrap: {status_txt[-300:]}"
        )
    return (
        "NiFi se lanzó pero no respondió a tiempo. "
        f"Estado bootstrap: {status_txt[-400:]}"
    )


def iniciar_api() -> str:
    if puerto_activo("127.0.0.1", PORT_API):
        return f"Swagger API ya estaba activa en puerto {PORT_API}."
    try:
        cmd = [sys.executable, "-m", "uvicorn", "servicios.api_simlog:app", "--host", "0.0.0.0", "--port", str(PORT_API)]
        subprocess.Popen(cmd, cwd=str(BASE), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        for _ in range(20):
            if puerto_activo("127.0.0.1", PORT_API):
                return f"Swagger API activa en puerto {PORT_API}."
            time.sleep(1)
        return f"Swagger API lanzada pero no abrió el puerto {PORT_API} a tiempo."
    except Exception as e:
        return f"No se pudo lanzar Swagger API: {e}"


def iniciar_faq_ia() -> str:
    if puerto_activo("127.0.0.1", PORT_FAQ_IA):
        return f"FAQ IA API ya estaba activa en puerto {PORT_FAQ_IA}."
    try:
        cmd = [sys.executable, "-m", "uvicorn", "servicios.api_faq_ia:app", "--host", "0.0.0.0", "--port", str(PORT_FAQ_IA)]
        subprocess.Popen(cmd, cwd=str(BASE), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        for _ in range(20):
            if puerto_activo("127.0.0.1", PORT_FAQ_IA):
                return f"FAQ IA API activa en puerto {PORT_FAQ_IA}."
            time.sleep(1)
        return f"FAQ IA API lanzada pero no abrió el puerto {PORT_FAQ_IA} a tiempo."
    except Exception as e:
        return f"No se pudo lanzar FAQ IA API: {e}"


INICIA: Dict[str, Callable[[], str]] = {
    "hdfs": iniciar_hdfs,
    "kafka": iniciar_kafka,
    "cassandra": iniciar_cassandra,
    "spark": iniciar_spark,
    "hive": iniciar_hive,
    "airflow": iniciar_airflow,
    "api": iniciar_api,
    "faq_ia": iniciar_faq_ia,
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
    msg_meta = _kill_port(PORT_HIVE_METASTORE)
    msg_hs2 = _kill_port(PORT_HIVE)
    pk_hs2 = _pkill_pattern("org.apache.hive.service.server.HiveServer2")
    pk_meta = _pkill_pattern("org.apache.hadoop.hive.metastore.HiveMetaStore")
    for _ in range(12):
        if not puerto_activo_en_hosts(_hive_hosts(), PORT_HIVE) and not puerto_activo_en_hosts(_hive_hosts(), PORT_HIVE_METASTORE):
            return f"{msg_meta} · {msg_hs2} · {pk_meta} · {pk_hs2}"
        time.sleep(1)
    return (
        f"{msg_meta} · {msg_hs2} · {pk_meta} · {pk_hs2} "
        f"(los puertos {PORT_HIVE_METASTORE}/{PORT_HIVE} siguen activos; puede tardar en cerrar)."
    )


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


def parar_api() -> str:
    msg = _kill_port(PORT_API)
    pk = _pkill_pattern("uvicorn servicios.api_simlog:app")
    return f"{msg} · {pk}"


def parar_faq_ia() -> str:
    msg = _kill_port(PORT_FAQ_IA)
    pk = _pkill_pattern("uvicorn servicios.api_faq_ia:app")
    return f"{msg} · {pk}"


PARA: Dict[str, Callable[[], str]] = {
    "hdfs": parar_hdfs,
    "kafka": parar_kafka,
    "cassandra": parar_cassandra,
    "spark": parar_spark,
    "hive": parar_hive,
    "airflow": parar_airflow,
    "api": parar_api,
    "faq_ia": parar_faq_ia,
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
