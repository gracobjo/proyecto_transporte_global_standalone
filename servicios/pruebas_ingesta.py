"""
Registro persistente y evidencias de pruebas de ingesta.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import CASSANDRA_HOST, HDFS_BACKUP_PATH, HIVE_DB, HIVE_TABLE_TRANSPORTE_HIST, KEYSPACE
from servicios.consultas_cuadro_mando import ejecutar_hive_consulta, ejecutar_hive_sql_seguro
from servicios.gestion_servicios import comprobar_airflow, comprobar_nifi
from servicios.pipeline_verificacion import WORK_KDD, hdfs_listado_json, leer_ultima_ingesta

BASE = Path(__file__).resolve().parent.parent
REPORTS_KDD = BASE / "reports" / "kdd"
PRUEBAS_DIR = BASE / "reports" / "pruebas"
REGISTRO_PATH = PRUEBAS_DIR / "registro_pruebas_ingesta.json"


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_json(path: Path, data: Any) -> None:
    _ensure_dir(path.parent)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def _parse_tsv_count(txt: str) -> Optional[int]:
    lineas = [ln.strip() for ln in (txt or "").splitlines() if ln.strip()]
    if len(lineas) < 2:
        return None
    for token in reversed(lineas[1].split("\t")):
        try:
            return int(token.strip())
        except ValueError:
            continue
    return None


def _cassandra_counts() -> Dict[str, Any]:
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(KEYSPACE)
        out: Dict[str, Any] = {}
        for tabla in ("nodos_estado", "aristas_estado", "tracking_camiones", "pagerank_nodos", "eventos_historico"):
            try:
                rows = session.execute(f"SELECT COUNT(*) FROM {tabla}")
                row = rows.one() if rows else None
                out[tabla] = int(row[0]) if row is not None else 0
            except Exception as e:
                out[tabla] = f"error: {e}"
        cluster.shutdown()
        return out
    except Exception as e:
        return {"error": str(e)}


def _hive_counts() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    ok_hist, err_hist, txt_hist = ejecutar_hive_consulta("historico_nodos_conteo")
    out["historico_nodos"] = _parse_tsv_count(txt_hist) if ok_hist else f"error: {err_hist}"

    ok_mae, err_mae, txt_mae = ejecutar_hive_consulta("nodos_maestro_conteo")
    out["nodos_maestro"] = _parse_tsv_count(txt_mae) if ok_mae else f"error: {err_mae}"

    ok_tr, err_tr, txt_tr = ejecutar_hive_sql_seguro(
        f"SELECT COUNT(*) AS total FROM {HIVE_DB}.{HIVE_TABLE_TRANSPORTE_HIST}"
    )
    out["transporte_ingesta_completa"] = _parse_tsv_count(txt_tr) if ok_tr else f"error: {err_tr}"
    return out


def _latest_airflow_report() -> Dict[str, Any]:
    if not REPORTS_KDD.exists():
        return {"disponible": False, "mensaje": "Sin carpeta de informes KDD todavía."}
    informes = sorted(
        (
            p
            for p in REPORTS_KDD.glob("*/informe_*.md")
            if p.is_file() and p.parent.name != "work"
        ),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not informes:
        return {"disponible": False, "mensaje": "Airflow aún no ha generado informes en reports/kdd/."}
    ultimo = informes[0]
    return {
        "disponible": True,
        "ruta": str(ultimo),
        "run_id": ultimo.parent.name,
        "archivo": ultimo.name,
        "modificado": datetime.fromtimestamp(ultimo.stat().st_mtime, tz=timezone.utc).isoformat(),
    }


def describir_canales_ingesta() -> List[Dict[str, str]]:
    """Canales de ingesta; el primero es el que usa la propia app Streamlit (barra lateral)."""
    return [
        {
            "canal": "Frontend Streamlit",
            "ejecucion": "Barra lateral → «Ejecutar ingesta (fases 1–2 KDD)», «Spark (3–5)» o «Avanzar paso + …»",
            "detalle": "Llama a `ejecutar_ingesta` / `ejecutar_procesamiento` en `app_visualizacion.py` (módulos `ingesta.ingesta_kdd` y `procesamiento_grafos`). Cada ejecución puede registrarse en el JSON de pruebas.",
            "evidencia": "Registro `registro_pruebas_ingesta.json` con `canal=Frontend Streamlit`, timeline en sidebar y `ultima_ingesta_meta.json`.",
        },
        {
            "canal": "NiFi",
            "ejecucion": "Process Group `PG_SIMLOG_KDD`",
            "detalle": "Genera payload enriquecido con Groovy, publica en Kafka, guarda backup en HDFS y puede disparar Spark.",
            "evidencia": "Estado NiFi + payload con `canal_ingesta=nifi` / `origen=simlog_nifi_invokehttp`.",
        },
        {
            "canal": "Airflow",
            "ejecucion": "DAGs `simlog_maestro`, `simlog_pipeline_maestro`, cadena `simlog_kdd_*`",
            "detalle": "Lanza ingesta y/o Spark programados o manuales; informes bajo `reports/kdd/<run_id>/`.",
            "evidencia": "Informes Markdown/HTML en esa carpeta y ejecución visible en la UI de Airflow.",
        },
        {
            "canal": "Script / terminal",
            "ejecucion": "`venv_transporte/bin/python -m ingesta.ingesta_kdd` (o `ingesta_kdd.py` en raíz según despliegue)",
            "detalle": "Ingesta local sin pasar por esta web; trazas en `reports/kdd/work/`.",
            "evidencia": "`ultimo_payload.json` y `ultima_ingesta_meta.json`.",
        },
    ]


def tipos_prueba_kdd_resumen() -> List[Dict[str, str]]:
    """Filas para tabla «qué se prueba» en la UI (estático, alineado al plan KDD)."""
    return [
        {
            "tipo": "Ingesta fases 1–2",
            "objetivo": "Clima hubs, simulación, GPS, Kafka, backup HDFS",
            "desde_esta_web": "Sí — botón «Ejecutar ingesta (fases 1–2 KDD)» (sidebar)",
            "resultado_esperado": "Código 0, JSON en HDFS, metadatos locales; registro OK en pruebas",
        },
        {
            "tipo": "Procesamiento Spark 3–5",
            "objetivo": "Grafo, métricas, PageRank, persistencia Cassandra/Hive",
            "desde_esta_web": "Sí — «Ejecutar procesamiento Spark (fases 3–5 KDD)»",
            "resultado_esperado": "Código 0; tablas Hive/Cassandra actualizadas según pipeline",
        },
        {
            "tipo": "Pipeline completo (paso + ingesta + Spark)",
            "objetivo": "Un solo clic avanza paso y ejecuta ingesta + Spark",
            "desde_esta_web": "Sí — «Avanzar paso + ingesta + procesamiento»",
            "resultado_esperado": "Ambos códigos 0; entrada en registro de pruebas con detalle de paso",
        },
        {
            "tipo": "Flujo NiFi",
            "objetivo": "Ingesta vía Kafka con trazabilidad NiFi",
            "desde_esta_web": "No (se opera en NiFi); aquí solo comprobación y snapshot",
            "resultado_esperado": "PG activo; contadores y HDFS coherentes tras el flujo",
        },
        {
            "tipo": "Airflow (maestro o KDD)",
            "objetivo": "Ingesta/Spark programados o cadena manual de fases",
            "desde_esta_web": "Disparo en UI Airflow; aquí se **listan** cadenas con informe fase 99 y puedes **añadir al JSON**",
            "resultado_esperado": "DAG en verde; tabla «Corridas Airflow» con `informe_99_*.md` en `reports/kdd/`",
        },
    ]


def resumen_ejecutivo_registro() -> Dict[str, Any]:
    """Contadores y últimas entradas para el dashboard de pruebas."""
    reg = leer_registro_pruebas()
    front = [x for x in reg if x.get("canal") == "Frontend Streamlit"]
    ultima = reg[-1] if reg else None
    ultima_front: Optional[Dict[str, Any]] = None
    for x in reversed(reg):
        if x.get("canal") == "Frontend Streamlit":
            ultima_front = x
            break
    cadenas_af = listar_cadenas_kdd_airflow_recientes(limit=99)
    return {
        "total_registros": len(reg),
        "registros_desde_streamlit": len(front),
        "ultima_prueba": ultima,
        "ultima_desde_streamlit": ultima_front,
        "cadenas_kdd_airflow_detectadas": len(cadenas_af),
    }


def listar_cadenas_kdd_airflow_recientes(limit: int = 20) -> List[Dict[str, Any]]:
    """
    Lista corridas Airflow que completaron la fase 99 (informe `informe_99_*` en `reports/kdd/`).
    Cada DAG `simlog_kdd_*` escribe su carpeta `manual__...`; la fase 99 confirma que la cadena llegó al final.
    """
    if not REPORTS_KDD.is_dir():
        return []
    paths = [
        p
        for p in REPORTS_KDD.glob("*/informe_99*.md")
        if p.is_file() and p.parent.name != "work"
    ]
    paths.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    out: List[Dict[str, Any]] = []
    for p in paths[:limit]:
        stt = p.stat()
        out.append(
            {
                "carpeta_run_id": p.parent.name,
                "archivo": p.name,
                "modificado_utc": datetime.fromtimestamp(stt.st_mtime, tz=timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S UTC"
                ),
                "ruta": str(p.resolve()),
            }
        )
    return out


def registrar_prueba_airflow_cadena_kdd_completa(*, ruta_informe_f99: str) -> Dict[str, Any]:
    """
    Añade al JSON una prueba con canal Airflow a partir del informe de fase 99 (cadena KDD completa).
    """
    ruta = ruta_informe_f99.strip()
    return registrar_prueba_ingesta(
        canal="Airflow",
        ejecutor="DAGs simlog_kdd_00…99 (informe fase 99 en disco)",
        resultado="OK",
        detalle=f"Cadena KDD completada; informe consulta final: {ruta}",
        observaciones="Registrado desde la pestaña Pruebas al detectar informe_99 en reports/kdd/.",
    )


def capturar_snapshot_pruebas() -> Dict[str, Any]:
    ing = leer_ultima_ingesta()
    hdfs = hdfs_listado_json(HDFS_BACKUP_PATH, max_items=5, timeout=20)
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ingesta_local": ing,
        "airflow": _latest_airflow_report(),
        "nifi": comprobar_nifi(),
        "airflow_estado": comprobar_airflow(),
        "hdfs": {
            "ruta": HDFS_BACKUP_PATH,
            "total_json": hdfs.get("total_json_listados") if hdfs.get("ok") else None,
            "detalle": hdfs,
        },
        "cassandra": _cassandra_counts(),
        "hive": _hive_counts(),
    }


def leer_registro_pruebas() -> List[Dict[str, Any]]:
    data = _read_json(REGISTRO_PATH)
    return data if isinstance(data, list) else []


def _snapshot_metric(snapshot: Dict[str, Any], ruta: List[str]) -> Optional[int]:
    cur: Any = snapshot
    for key in ruta:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur if isinstance(cur, int) else None


def _delta(snapshot: Dict[str, Any], prev: Optional[Dict[str, Any]], ruta: List[str]) -> Optional[int]:
    if not prev:
        return None
    actual = _snapshot_metric(snapshot, ruta)
    anterior = _snapshot_metric(prev, ruta)
    if actual is None or anterior is None:
        return None
    return actual - anterior


def registrar_prueba_ingesta(
    *,
    canal: str,
    ejecutor: str,
    resultado: str,
    detalle: str,
    observaciones: str = "",
) -> Dict[str, Any]:
    registro = leer_registro_pruebas()
    anterior = registro[-1]["snapshot"] if registro else None
    snapshot = capturar_snapshot_pruebas()
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "canal": canal,
        "ejecutor": ejecutor,
        "resultado": resultado,
        "detalle": detalle,
        "observaciones": observaciones.strip(),
        "snapshot": snapshot,
        "deltas": {
            "hdfs_json": _delta(snapshot, anterior, ["hdfs", "total_json"]),
            "cassandra_eventos": _delta(snapshot, anterior, ["cassandra", "eventos_historico"]),
            "hive_historico_nodos": _delta(snapshot, anterior, ["hive", "historico_nodos"]),
            "hive_transporte": _delta(snapshot, anterior, ["hive", "transporte_ingesta_completa"]),
        },
    }
    registro.append(entry)
    _write_json(REGISTRO_PATH, registro)
    return entry


def resumen_tabular_pruebas() -> List[Dict[str, Any]]:
    filas: List[Dict[str, Any]] = []
    for item in reversed(leer_registro_pruebas()):
        snap = item.get("snapshot", {})
        filas.append(
            {
                "timestamp": item.get("timestamp", ""),
                "canal": item.get("canal", ""),
                "ejecutor": item.get("ejecutor", ""),
                "resultado": item.get("resultado", ""),
                "hdfs_json": _snapshot_metric(snap, ["hdfs", "total_json"]),
                "delta_hdfs": item.get("deltas", {}).get("hdfs_json"),
                "eventos_cassandra": _snapshot_metric(snap, ["cassandra", "eventos_historico"]),
                "delta_eventos": item.get("deltas", {}).get("cassandra_eventos"),
                "historico_nodos_hive": _snapshot_metric(snap, ["hive", "historico_nodos"]),
                "delta_hist_hive": item.get("deltas", {}).get("hive_historico_nodos"),
                "transporte_hive": _snapshot_metric(snap, ["hive", "transporte_ingesta_completa"]),
                "delta_transporte_hive": item.get("deltas", {}).get("hive_transporte"),
                "detalle": item.get("detalle", ""),
            }
        )
    return filas
