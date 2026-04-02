"""
Lectura de informes KDD generados por Airflow (`reports/kdd/<run_id>/`).

Uso típico en API o Streamlit: listar HTML/Markdown por ejecución y enlazarlos en el front.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE = Path(__file__).resolve().parent.parent
REPORTS_KDD = BASE / "reports" / "kdd"
WORK_KDD = REPORTS_KDD / "work"

# Metadatos estáticos alineados con orquestacion/kdd_ejecucion.py + kdd_informe.py
FASES_KDD: List[Dict[str, Any]] = [
    {
        "codigo": "00_infra",
        "dag_id": "simlog_kdd_00_infra",
        "titulo": "Infraestructura (HDFS, Cassandra, Kafka)",
        "criterio_exito": "Puertos OK y arranque sin error en el JSON del informe.",
        "work_rel": None,
    },
    {
        "codigo": "01_seleccion",
        "dag_id": "simlog_kdd_01_seleccion",
        "titulo": "Selección — clima hubs",
        "criterio_exito": "fase1_clima.json escrito y hubs con datos en resultado.",
        "work_rel": "fase1_clima.json",
    },
    {
        "codigo": "02_preprocesamiento",
        "dag_id": "simlog_kdd_02_preprocesamiento",
        "titulo": "Preprocesamiento — simulación, Kafka, HDFS",
        "criterio_exito": "kafka_ok y hdfs_ok; ultimo_payload.json actualizado.",
        "work_rel": "ultimo_payload.json",
    },
    {
        "codigo": "03_transformacion",
        "dag_id": "simlog_kdd_03_transformacion",
        "titulo": "Transformación — GraphFrames",
        "criterio_exito": "returncode 0 y métricas de grafo en informe / fase3_metricas.json.",
        "work_rel": "fase3_metricas.json",
    },
    {
        "codigo": "04_mineria",
        "dag_id": "simlog_kdd_04_mineria",
        "titulo": "Minería — PageRank",
        "criterio_exito": "returncode 0 y top_pagerank en informe / fase4_pagerank.json.",
        "work_rel": "fase4_pagerank.json",
    },
    {
        "codigo": "05_interpretacion",
        "dag_id": "simlog_kdd_05_interpretacion",
        "titulo": "Interpretación — Cassandra / Hive",
        "criterio_exito": "returncode 0; fase5_resumen.json; datos visibles en pipeline.",
        "work_rel": "fase5_resumen.json",
    },
    {
        "codigo": "99_consulta_final",
        "dag_id": "simlog_kdd_99_consulta_final",
        "titulo": "Consulta final Cassandra",
        "criterio_exito": "cassandra_conteos con enteros (sin error de conexión).",
        "work_rel": None,
    },
]


def catalogo_fases_kdd() -> List[Dict[str, Any]]:
    """Filas para tablas de ayuda o tooltips en el front."""
    return list(FASES_KDD)


def listar_run_ids_con_informes() -> List[str]:
    """Nombres de carpeta (`run_id`) bajo `reports/kdd/` que contienen al menos un informe."""
    if not REPORTS_KDD.is_dir():
        return []
    out: List[str] = []
    for d in sorted(REPORTS_KDD.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if not d.is_dir() or d.name == "work":
            continue
        if any(d.glob("informe_*.md")):
            out.append(d.name)
    return out


def informes_por_run_id(run_id: str) -> Dict[str, Any]:
    """
    Lista ficheros de informe para un run concreto.

    Devuelve claves `markdown`, `html`, `run_id`, `base_dir`.
    """
    base = REPORTS_KDD / run_id
    if not base.is_dir():
        return {"ok": False, "error": f"No existe carpeta: {base}", "run_id": run_id}
    md = sorted(base.glob("informe_*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
    html = sorted(base.glob("informe_*.html"), key=lambda p: p.stat().st_mtime, reverse=True)
    return {
        "ok": True,
        "run_id": run_id,
        "base_dir": str(base.resolve()),
        "markdown": [{"nombre": p.name, "ruta": str(p.resolve())} for p in md],
        "html": [{"nombre": p.name, "ruta": str(p.resolve())} for p in html],
    }


def extraer_json_bloques_desde_markdown(path_md: Path, max_bloques: int = 2) -> List[Any]:
    """
    Extrae JSON de los bloques ``` del informe (secciones 1 y 2).
    No usa eval; falla silenciosamente en bloques inválidos.
    """
    import json

    raw = path_md.read_text(encoding="utf-8")
    bloques = re.findall(r"```(?:json)?\s*([\s\S]*?)```", raw)
    out: List[Any] = []
    for b in bloques[:max_bloques]:
        b = b.strip()
        if not b:
            continue
        try:
            out.append(json.loads(b))
        except json.JSONDecodeError:
            out.append(None)
    return out


def work_json_si_existe(nombre: str) -> Optional[Path]:
    """Ruta absoluta a un JSON en work/ si existe."""
    p = WORK_KDD / nombre
    return p if p.is_file() else None
