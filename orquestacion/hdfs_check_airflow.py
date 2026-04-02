"""
Comprobación ligera de HDFS para tareas Airflow (sin `hdfs dfs`).

El CLI de Hadoop puede colgarse (DNS, proxy, core-site) y provocar TimeoutExpired.
Se usa WebHDFS (puerto 9870 por defecto) y, si falla, un intento TCP al RPC del NameNode.

Variables de entorno:
- SIMLOG_AIRFLOW_LOCAL=1 — modo desarrollo local: omite esta comprobación (equivale a skip HDFS).
- SIMLOG_SKIP_HDFS_CHECK=1 — omite la comprobación (desarrollo local sin HDFS).
- HDFS_NAMENODE — host:puerto RPC (por defecto 127.0.0.1:9000).
- SIMLOG_WEBHDFS_URL — URL base WebHDFS (opcional; si no, http://<host>:SIMLOG_WEBHDFS_PORT).
- SIMLOG_WEBHDFS_PORT — por defecto 9870.
- SIMLOG_AIRFLOW_HDFS_CHECK_TIMEOUT_SEC — timeout HTTP/socket (por defecto 5).
"""

from __future__ import annotations

import os
import socket
import urllib.request


def verificar_hdfs_airflow(**_context) -> str:
    local = (os.environ.get("SIMLOG_AIRFLOW_LOCAL") or "").strip().lower()
    if local in ("1", "true", "yes", "on"):
        return "SKIP (SIMLOG_AIRFLOW_LOCAL)"
    skip = (os.environ.get("SIMLOG_SKIP_HDFS_CHECK") or "").strip().lower()
    if skip in ("1", "true", "yes", "on"):
        return "SKIP (SIMLOG_SKIP_HDFS_CHECK)"

    nn = (os.environ.get("HDFS_NAMENODE", "127.0.0.1:9000") or "127.0.0.1:9000").strip()
    parts = nn.split(":", 1)
    host = (parts[0] or "127.0.0.1").strip() or "127.0.0.1"
    rpc_port = int(parts[1]) if len(parts) > 1 else 9000

    override = (os.environ.get("SIMLOG_WEBHDFS_URL", "") or "").strip()
    if override:
        base = override.rstrip("/")
    else:
        web_port = int(os.environ.get("SIMLOG_WEBHDFS_PORT", "9870"))
        base = f"http://{host}:{web_port}"
    user = (os.environ.get("HADOOP_USER_NAME") or os.environ.get("USER") or "hadoop").strip() or "hadoop"
    url = f"{base}/webhdfs/v1/?op=LISTSTATUS&user.name={user}"
    timeout_sec = float(os.environ.get("SIMLOG_AIRFLOW_HDFS_CHECK_TIMEOUT_SEC", "5"))

    no_proxy_handler = urllib.request.ProxyHandler({})
    opener = urllib.request.build_opener(no_proxy_handler)
    web_err: Exception | None = None
    try:
        with opener.open(url, timeout=timeout_sec) as resp:
            if getattr(resp, "status", 200) != 200:
                raise RuntimeError(f"WebHDFS HTTP {getattr(resp, 'status', '?')}")
        return "OK"
    except Exception as e:
        web_err = e

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(min(3.0, timeout_sec))
        sock.connect((host, rpc_port))
        sock.close()
        return "OK"
    except Exception as e2:
        w = f"{web_err!s}"[:240] if web_err else ""
        raise RuntimeError(
            f"HDFS no disponible (WebHDFS {base}: {w}; RPC {host}:{rpc_port}: {e2!s})"
        ) from (web_err or e2)
