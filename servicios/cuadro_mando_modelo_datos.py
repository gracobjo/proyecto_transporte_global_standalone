"""
Descripciones del modelo de datos (Cassandra operativo vs Hive histórico) para el cuadro de mando.

Complementa las consultas supervisadas con contexto de esquema sin ejecutar DDL en caliente.
"""
from __future__ import annotations

from typing import Dict, Iterable, List

# --- Rol global (markdown corto) ---
RESUMEN_CASSANDRA_MD = """
**Cassandra (`logistica_espana`) — estado operativo en tiempo real**

- **Propósito**: fotografía actual del gemelo (nodos, aristas, flota, PageRank) y eventos recientes con TTL.
- **Clave de diseño**: tablas optimizadas por **clave primaria** (`id_nodo`, `(src,dst)`, `id_camion`, …);  
  filtros flexibles suelen requerir `ALLOW FILTERING` (uso moderado en tablas pequeñas).
- **Origen de datos**: ingesta + `procesamiento_grafos` / pipeline Spark.
"""

RESUMEN_HIVE_MD = """
**Hive (`logistica_espana` u otra BD vía `HIVE_DB`) — histórico analítico**

- **Propósito**: series temporales y agregados para informes, cuadro de mando y análisis (particiones `anio`/`mes`/`dia` o `anio_part`/`mes_part` según tabla).
- **Clave de diseño**: consultas **batch** vía HiveServer2; conviene poda por partición y `LIMIT`.
- **Origen de datos**: misma línea de negocio persistida con `SIMLOG_ENABLE_HIVE=1` y jobs Spark.
"""

# Descripción por tabla lógica (nombre en metastore / CQL)
DESCRIPCION_TABLA_CASSANDRA: Dict[str, str] = {
    "nodos_estado": (
        "Hubs y nodos de carretera: coordenadas, **estado** operativo, retrasos, **clima** en sitio, "
        "metadatos DGT (municipio, provincia, carretera) e incidencias."
    ),
    "aristas_estado": (
        "Conexiones **src→dst**: distancia, estado (OK / congestión / bloqueo) y peso penalizado para routing."
    ),
    "tracking_camiones": (
        "Posición actual de camiones (**lat/lon**), ruta origen/destino, estado de ruta y última marca temporal."
    ),
    "pagerank_nodos": (
        "Métrica **PageRank** y peso derivado por nodo; sirve para priorizar criticidad en la red."
    ),
    "eventos_historico": (
        "Historial corto en Cassandra de cambios de estado (**TTL**); no confundir con `eventos_historico` de Hive."
    ),
}

COLUMNAS_REFERENCIA_CASSANDRA: Dict[str, List[str]] = {
    "nodos_estado": [
        "id_nodo",
        "lat",
        "lon",
        "tipo",
        "estado",
        "motivo_retraso",
        "clima_actual",
        "temperatura",
        "humedad",
        "viento_velocidad",
        "provincia",
        "municipio",
        "ultima_actualizacion",
    ],
    "aristas_estado": ["src", "dst", "distancia_km", "estado", "peso_penalizado"],
    "tracking_camiones": [
        "id_camion",
        "lat",
        "lon",
        "ruta_origen",
        "ruta_destino",
        "estado_ruta",
        "motivo_retraso",
        "ultima_posicion",
    ],
    "pagerank_nodos": [
        "id_nodo",
        "pagerank",
        "peso_pagerank",
        "source",
        "estado",
        "ultima_actualizacion",
    ],
    "eventos_historico": [
        "tipo_entidad",
        "id_entidad",
        "estado_anterior",
        "estado_nuevo",
        "motivo",
        "timestamp_evento",
    ],
}

DESCRIPCION_TABLA_HIVE: Dict[str, str] = {
    "eventos_historico": (
        "Línea de tiempo de eventos de nodos/aristas: tipo, estado, motivo, hub, PageRank asociado, partición temporal."
    ),
    "clima_historico": (
        "Series por ciudad: temperatura, humedad, descripción meteorológica, visibilidad y estado de carretera."
    ),
    "tracking_camiones_historico": (
        "Evolución de posiciones y progreso de ruta de la flota (histórico largo vs Cassandra)."
    ),
    "transporte_ingesta_completa": (
        "Registros de ingesta del pipeline: origen/destino, estado de ruta, motivos de retraso, geometría aproximada. "
        "El nombre físico lo define `SIMLOG_HIVE_TABLA_TRANSPORTE`."
    ),
    "rutas_alternativas_historico": (
        "Comparativa ruta original vs alternativa, distancias y motivo de bloqueo que disparó el replan."
    ),
    "agg_estadisticas_diarias": (
        "Agregados diarios por tipo de evento y estado; base ideal para tendencias y cuadros de mando."
    ),
    "historico_nodos": ("Snapshots históricos de nodos (si existen en tu despliegue)."),
    "nodos_maestro": ("Catálogo maestro de nodos cargado desde HDFS/CSV."),
    "red_gemelo_nodos": ("Topología estática del gemelo: nodos y referencias a capitales."),
    "red_gemelo_aristas": ("Aristas estáticas del gemelo con distancias."),
}

# Nota: la clave `transporte_ingesta_completa` es el valor por defecto de HIVE_TABLE_TRANSPORTE_HIST;
# la UI sustituye por el nombre real de config al mostrar.


def markdown_contexto_cassandra(tablas: Iterable[str]) -> str:
    """Texto markdown: descripción de negocio + columnas de referencia por tabla."""
    lines: List[str] = [RESUMEN_CASSANDRA_MD.strip(), ""]
    for t in sorted(set(tablas)):
        desc = DESCRIPCION_TABLA_CASSANDRA.get(t, "_Tabla operativa Cassandra._")
        lines.append(f"**`{t}`** — {desc}")
        cols = COLUMNAS_REFERENCIA_CASSANDRA.get(t)
        if cols:
            lines.append(f"- *Columnas habituales*: `{', '.join(cols)}`")
        lines.append("")
    return "\n".join(lines).strip()


def markdown_contexto_hive(tablas: Iterable[str], nombre_transporte_real: str) -> str:
    """Igual para Hive; enlaza la tabla de transporte con la descripción genérica."""
    lines: List[str] = [RESUMEN_HIVE_MD.strip(), ""]
    for t in sorted(set(tablas)):
        if t == nombre_transporte_real or t == "transporte_ingesta_completa":
            desc = DESCRIPCION_TABLA_HIVE.get("transporte_ingesta_completa", "_Tabla de transporte._")
            suf = "" if t == "transporte_ingesta_completa" else " *(config: `SIMLOG_HIVE_TABLA_TRANSPORTE`)*"
        else:
            desc = DESCRIPCION_TABLA_HIVE.get(t, "_Tabla analítica Hive._")
            suf = ""
        lines.append(f"**`{t}`**{suf} — {desc}")
        lines.append("")
    return "\n".join(lines).strip()
