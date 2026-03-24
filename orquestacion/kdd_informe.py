#!/usr/bin/env python3
"""
Generación de informes Markdown/HTML por fase KDD (exportables a PDF).
Uso: Airflow PythonOperator o ejecución manual.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# Raíz del proyecto (orquestacion -> parent)
BASE = Path(__file__).resolve().parent.parent
REPORTS_KDD = BASE / "reports" / "kdd"


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def generar_informe_fase(
    fase_id: str,
    titulo: str,
    datos_inicio: Dict[str, Any],
    datos_resultado: Dict[str, Any],
    run_id: Optional[str] = None,
    notas: Optional[str] = None,
) -> Path:
    """
    Escribe un informe en Markdown bajo reports/kdd/<run_id o 'manual'>/
    Devuelve la ruta del fichero .md (pandoc/weasyprint pueden convertir a PDF).
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    rid = run_id or "manual"
    out_dir = REPORTS_KDD / rid
    _ensure_dir(out_dir)
    path_md = out_dir / f"informe_{fase_id}_{ts}.md"
    path_html = out_dir / f"informe_{fase_id}_{ts}.html"

    body_md = f"""# {titulo}

**Fase:** `{fase_id}`  
**Ejecución (run):** `{rid}`  
**Generado (UTC):** {datetime.now(timezone.utc).isoformat()}

## 1. Datos de inicio / contexto

```json
{json.dumps(datos_inicio, indent=2, ensure_ascii=False, default=str)}
```

## 2. Resultado

```json
{json.dumps(datos_resultado, indent=2, ensure_ascii=False, default=str)}
```

"""

    if notas:
        body_md += f"\n## 3. Notas\n\n{notas}\n"

    body_md += """

---

## Exportar a PDF

```bash
# Con pandoc (recomendado)
pandoc """ + path_md.name + """ -o """ + path_md.with_suffix(".pdf").name + """ --pdf-engine=xelatex

# O imprimir el HTML desde navegador (Ctrl+P → Guardar como PDF)
```

"""

    path_md.write_text(body_md, encoding="utf-8")

    # HTML mínimo imprimible
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8"/>
  <title>{titulo}</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem; max-width: 900px; }}
    pre {{ background: #f4f4f4; padding: 1rem; overflow: auto; }}
    h1 {{ color: #1a5f7a; }}
  </style>
</head>
<body>
<h1>{titulo}</h1>
<p><strong>Fase</strong>: {fase_id} | <strong>Run</strong>: {rid} | <strong>UTC</strong>: {datetime.now(timezone.utc).isoformat()}</p>
<h2>1. Datos de inicio</h2>
<pre>{json.dumps(datos_inicio, indent=2, ensure_ascii=False, default=str)}</pre>
<h2>2. Resultado</h2>
<pre>{json.dumps(datos_resultado, indent=2, ensure_ascii=False, default=str)}</pre>
"""
    if notas:
        html += f"<h2>3. Notas</h2><p>{notas}</p>"
    html += "</body></html>"
    path_html.write_text(html, encoding="utf-8")

    return path_md


def contexto_airflow_datos(**context) -> Dict[str, Any]:
    """Extrae metadatos útiles del contexto Airflow para el informe."""
    dr = context.get("dag_run")
    ti = context.get("ti")
    out: Dict[str, Any] = {
        "dag_id": context.get("dag").dag_id if context.get("dag") else None,
        "logical_date": str(context.get("logical_date") or context.get("data_interval_start") or ""),
        "run_id": getattr(dr, "run_id", None) if dr else None,
    }
    if ti:
        out["task_id"] = ti.task_id
    return out
