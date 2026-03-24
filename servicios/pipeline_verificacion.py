"""
Comprobaciones estructuradas del pipeline SIMLOG (ingesta → Kafka/HDFS → Spark → Cassandra/Hive).
Usado por el dashboard Streamlit (pestaña «Resultados pipeline»).
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from shutil import which
from typing import Any, Dict, List, Optional, Tuple

BASE = Path(__file__).resolve().parent.parent
WORK_KDD = BASE / "reports" / "kdd" / "work"


def _run(cmd: List[str], timeout: int = 25) -> Tuple[int, str, str]:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout or "", r.stderr or ""
    except FileNotFoundError:
        return -1, "", "comando no encontrado"
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"
    except Exception as e:
        return -1, "", str(e)[:200]


def leer_ultima_ingesta() -> Dict[str, Any]:
    """Último payload y metadatos escritos por `ingesta.ingesta_kdd` (si existen)."""
    out: Dict[str, Any] = {
        "disponible": False,
        "ruta_payload": str(WORK_KDD / "ultimo_payload.json"),
        "ruta_meta": str(WORK_KDD / "ultima_ingesta_meta.json"),
    }
    meta_p = WORK_KDD / "ultima_ingesta_meta.json"
    pay_p = WORK_KDD / "ultimo_payload.json"
    if meta_p.exists():
        try:
            out["meta"] = json.loads(meta_p.read_text(encoding="utf-8"))
            out["disponible"] = True
        except Exception as e:
            out["error_meta"] = str(e)
    if pay_p.exists():
        try:
            blob = json.loads(pay_p.read_text(encoding="utf-8"))
            out["timestamp"] = blob.get("timestamp")
            out["paso_15min"] = blob.get("paso_15min")
            out["hubs_clima"] = len(blob.get("clima_hubs") or {})
            out["camiones"] = len(blob.get("camiones") or [])
            out["nodos_en_payload"] = len(blob.get("nodos_estado") or {})
            out["disponible"] = True
        except Exception as e:
            out["error_payload"] = str(e)
    if not out.get("disponible"):
        out["mensaje"] = (
            "Aún no hay `ultimo_payload.json` en esta máquina. "
            "Ejecuta **ingesta** desde el sidebar o la pestaña Ciclo KDD."
        )
    return out


def hdfs_listado_json(ruta: str, max_items: int = 8) -> Dict[str, Any]:
    """Lista JSON en HDFS (backup ingesta) con nombre y tamaño aproximado."""
    code, stdout, stderr = _run(["hdfs", "dfs", "-ls", ruta], timeout=20)
    if code != 0:
        return {
            "ok": False,
            "ruta": ruta,
            "detalle": (stderr or stdout or "error hdfs")[:500],
        }
    lineas = [l.strip() for l in stdout.splitlines() if l.strip() and not l.startswith("Found")]
    filas: List[Dict[str, Any]] = []
    for linea in lineas:
        if ".json" not in linea:
            continue
        partes = linea.split()
        if len(partes) >= 8:
            try:
                tam = int(partes[4])
                fecha = f"{partes[5]} {partes[6]}"
                nombre = partes[-1]
                filas.append({"archivo": nombre.split("/")[-1], "ruta_hdfs": nombre, "tam_bytes": tam, "fecha": fecha})
            except (ValueError, IndexError):
                filas.append({"linea": linea[:120]})
    filas.sort(key=lambda x: x.get("fecha") or "", reverse=True)
    return {
        "ok": True,
        "ruta": ruta,
        "total_json_listados": len(filas),
        "ultimos": filas[:max_items],
    }


def _kafka_ejecutable_offsets() -> List[str]:
    """Rutas a kafka-get-offsets (la distribución Apache usa *.sh en bin/)."""
    nombres = ("kafka-get-offsets.sh", "kafka-get-offsets")
    encontrados: List[str] = []
    kh = os.environ.get("KAFKA_HOME", "").strip()
    if kh:
        b = Path(kh) / "bin"
        for n in nombres:
            p = b / n
            if p.is_file():
                encontrados.append(str(p))
    for n in nombres:
        w = which(n)
        if w:
            encontrados.append(w)
    # Sin duplicados conservando orden
    vistos = set()
    out: List[str] = []
    for e in encontrados:
        if e not in vistos:
            vistos.add(e)
            out.append(e)
    return out


def _kafka_ejecutable_run_class() -> List[str]:
    """kafka-run-class.sh para GetOffsetShell."""
    nombres = ("kafka-run-class.sh", "kafka-run-class")
    encontrados: List[str] = []
    kh = os.environ.get("KAFKA_HOME", "").strip()
    if kh:
        b = Path(kh) / "bin"
        for n in nombres:
            p = b / n
            if p.is_file():
                encontrados.append(str(p))
    for n in nombres:
        w = which(n)
        if w:
            encontrados.append(w)
    vistos = set()
    out: List[str] = []
    for e in encontrados:
        if e not in vistos:
            vistos.add(e)
            out.append(e)
    return out


def _obtener_offsets_kafka(bootstrap: str, topic: str) -> Tuple[Optional[str], str]:
    """
    Obtiene líneas de offsets (último offset por partición si es posible).
    Devuelve (texto o None, mensaje de diagnóstico si falla todo).
    """
    brokers = bootstrap.replace("localhost", "127.0.0.1")

    for exe in _kafka_ejecutable_offsets():
        code, off_out, err = _run(
            [exe, "--bootstrap-server", bootstrap, "--topic", topic],
            timeout=25,
        )
        if code == 0 and (off_out or "").strip():
            return off_out.strip()[:2000], ""

    # GetOffsetShell: --time -1 = último offset (Kafka 2.x/3.x)
    clases = (
        "kafka.tools.GetOffsetShell",
        "org.apache.kafka.tools.GetOffsetShell",
    )
    for exe in _kafka_ejecutable_run_class():
        for clase in clases:
            code, off2, err2 = _run(
                [
                    exe,
                    clase,
                    "--broker-list",
                    brokers,
                    "--topic",
                    topic,
                    "--time",
                    "-1",
                ],
                timeout=25,
            )
            if code == 0 and (off2 or "").strip():
                return off2.strip()[:2000], ""
            # Algunas instalaciones esperan bootstrap en lugar de broker-list
            code3, off3, _ = _run(
                [
                    exe,
                    clase,
                    "--bootstrap-server",
                    bootstrap,
                    "--topic",
                    topic,
                    "--time",
                    "-1",
                ],
                timeout=25,
            )
            if code3 == 0 and (off3 or "").strip():
                return off3.strip()[:2000], ""

    diag = []
    if not _kafka_ejecutable_offsets() and not _kafka_ejecutable_run_class():
        diag.append(
            "No está en PATH `kafka-get-offsets.sh` ni `kafka-run-class.sh`. "
            "Exporta `KAFKA_HOME` apuntando a la instalación de Kafka (p. ej. `export KAFKA_HOME=/opt/kafka`)."
        )
    else:
        diag.append(
            "Los comandos de offsets fallaron (revisa broker, ACL o versión de Kafka). "
            "Si el topic tiene mensajes, prueba: "
            f"`$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server {bootstrap} "
            f"--topic {topic} --from-beginning --max-messages 1`"
        )
    return None, " ".join(diag)


def kafka_resumen_topic(bootstrap: str, topic: str) -> Dict[str, Any]:
    """Describe el topic y, si es posible, offsets finales por partición."""
    out: Dict[str, Any] = {"bootstrap": bootstrap, "topic": topic, "topic_existe": False}
    code, stdout, stderr = _run(
        ["kafka-topics.sh", "--describe", "--topic", topic, "--bootstrap-server", bootstrap],
        timeout=20,
    )
    out["describe_ok"] = code == 0
    out["describe_salida"] = (stdout or stderr or "")[:1200]

    code2, out2, err2 = _run(
        ["kafka-topics.sh", "--list", "--bootstrap-server", bootstrap],
        timeout=15,
    )
    if code2 == 0 and topic in out2:
        out["topic_existe"] = True

    off_text, diag = _obtener_offsets_kafka(bootstrap, topic)
    if off_text:
        out["offsets"] = off_text
    else:
        out["nota_offsets"] = (
            diag
            or (
                "Offsets no disponibles (`kafka-get-offsets.sh` / GetOffsetShell). "
                "Con topic creado y ingesta OK, los mensajes deberían estar en el log del productor."
            )
        )
    return out


def cassandra_resumen_tablas(host: str, keyspace: str) -> Dict[str, Any]:
    """Conteos aproximados en tablas operativas del gemelo (Cassandra)."""
    tablas = [
        ("nodos_estado", "SELECT COUNT(*) FROM nodos_estado"),
        ("aristas_estado", "SELECT COUNT(*) FROM aristas_estado"),
        ("tracking_camiones", "SELECT COUNT(*) FROM tracking_camiones"),
        ("pagerank_nodos", "SELECT COUNT(*) FROM pagerank_nodos"),
    ]
    out: Dict[str, Any] = {"host": host, "keyspace": keyspace, "tablas": {}}
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([host])
        session = cluster.connect(keyspace)
        for nombre, cql in tablas:
            try:
                row = session.execute(cql).one()
                # cassandra-driver: Row no tiene .values(); usar índice o nombre de columna
                if row is None:
                    n = 0
                else:
                    n = row[0]
                out["tablas"][nombre] = {"filas": int(n) if n is not None else 0, "ok": True}
            except Exception as e:
                out["tablas"][nombre] = {"ok": False, "error": str(e)[:120]}
        cluster.shutdown()
        out["ok"] = True
    except Exception as e:
        out["ok"] = False
        out["error"] = str(e)[:300]
    return out


def hive_resumen(beeline_sql_show_tables: bool = True) -> Dict[str, Any]:
    """SHOW TABLES + muestra de conteos vía consultas whitelist (beeline)."""
    from servicios.consultas_cuadro_mando import ejecutar_hive_consulta

    out: Dict[str, Any] = {"tablas": None, "conteos": {}}
    ok, err, salida = ejecutar_hive_consulta("tablas_bd")
    out["show_tables_ok"] = ok
    if ok:
        out["tablas"] = salida[:4000]
    else:
        out["error_tablas"] = err or salida

    for codigo in ("historico_nodos_conteo", "nodos_maestro_conteo"):
        ok2, err2, sal2 = ejecutar_hive_consulta(codigo)
        out["conteos"][codigo] = {
            "ok": ok2,
            "error": err2 if not ok2 else "",
            "salida": (sal2 or "")[:800],
        }

    blob_err = (out.get("error_tablas") or "") + str(out.get("conteos"))
    if "rehusada" in blob_err or "refused" in blob_err.lower():
        out["hint"] = (
            "HiveServer2 no acepta conexiones en el JDBC actual (`HIVE_JDBC_URL`, por defecto "
            "`jdbc:hive2://localhost:10000`). Arranca HiveServer2 (p. ej. servicio `hive-server` "
            "en docker-compose del proyecto) o apunta el JDBC al host donde escucha HS2."
        )
    return out


def obtener_snapshot_pipeline(
    hdfs_path: str,
    kafka_bootstrap: str,
    topic: str,
    cassandra_host: str,
    keyspace: str,
) -> Dict[str, Any]:
    """
    Un solo dict con todo lo necesario para la pestaña de resultados.
    """
    return {
        "ingesta_local": leer_ultima_ingesta(),
        "hdfs_backup": hdfs_listado_json(hdfs_path),
        "kafka": kafka_resumen_topic(kafka_bootstrap, topic),
        "cassandra": cassandra_resumen_tablas(cassandra_host, keyspace),
        "hive": hive_resumen(),
    }
