"""
Texto de UI: reglas de negocio del grafo y su evolución por fase KDD.
Debe mantenerse alineado con `procesamiento/procesamiento_grafos.py` y `fase_kdd_spark.py`.
"""
from __future__ import annotations

# Constantes de negocio (sincronizar con procesamiento_grafos.py)
PESO_CONGESTION_CLIMA = 1.5
PR_RESET_PROBABILITY = 0.15
PR_MAX_ITER = 10


def _bloque_comun_grafo_base() -> str:
    return f"""
**Grafo base (antes de reglas)** — `construir_grafo_base`:

- **Vértices:** todos los nodos de `config_nodos` (hubs + secundarios).
- **Aristas:** topología estática (`get_aristas()`), peso inicial = **distancia en km** (haversine entre nodos).
"""


def _bloque_autosanacion() -> str:
    return f"""
**Autosanación** — `aplicar_autosanacion` (misma lógica en fases 3, 4 y en el `main` de persistencia):

| Condición (por arista, según payload / simulación) | Efecto en el grafo |
|---------------------------------------------------|-------------------|
| `estado == "Bloqueado"` | La arista **no entra** en el grafo (se descarta). |
| `estado == "Congestionado"` **o** `motivo` es **"Niebla"** o **"Lluvia"** | El peso pasa a **distancia × {PESO_CONGESTION_CLIMA}** (penalización; columna `peso_penalizado`). |
| Sin datos de estado para esa arista | Se conserva la **distancia original** como peso. |

**Cómo “cambia” el grafo:** menos aristas si hay tramos bloqueados; en caminos mínimos / PageRank, las aristas penalizadas cuentan como **más largas**, desviando flujo hacia alternativas.
"""


def markdown_panel_reglas(orden_fase: int) -> str:
    """Compatibilidad: delega en el panel unificado (misma red en 3–5, texto no duplicado)."""
    return markdown_panel_reglas_unificado(orden_fase)


def markdown_panel_reglas_unificado(orden_fase: int) -> str:
    """
    Un solo bloque común (grafo base + autosanación) y tres apartados de fase,
    resaltando la fase actual. Evita repetir el mismo texto al cambiar 3 ↔ 4 ↔ 5.
    """
    base = _bloque_comun_grafo_base()
    auto = _bloque_autosanacion()
    mark = lambda n: "👉 **Estás aquí** — " if orden_fase == n else ""

    f3 = f"""
#### {mark(3)}Fase 3 (transformación) — `fase_kdd_spark --fase transformacion`
- Construye **G₀** → **autosanación** → **G** transformado.
- Métricas: `reports/kdd/work/fase3_metricas.json`.
"""
    f4 = f"""
#### {mark(4)}Fase 4 (minería) — `fase_kdd_spark --fase mineria`
- Mismo **G** ponderado que en fase 3 (reconstrucción + autosanación).
- **PageRank** sobre **G**: `resetProbability={PR_RESET_PROBABILITY}`, `maxIter={PR_MAX_ITER}`.
- `reports/kdd/work/fase4_pagerank.json`.
"""
    f5 = f"""
#### {mark(5)}Fase 5 (interpretación) — `interpretacion` / `procesamiento_grafos.main()`
- Persistencia **Cassandra** (nodos, aristas, tracking, PageRank) y **Hive** opcional (`SIMLOG_ENABLE_HIVE=1`).
- Mapa geográfico y métricas en otras pestañas; **abajo** solo topología lógica (misma red).
"""
    return (
        f"_Fase seleccionada en el selector: **{orden_fase}**._\n\n"
        "### Común a las fases 3, 4 y 5\n"
        + base
        + auto
        + "### Qué añade cada fase\n"
        + f3
        + f4
        + f5
    )


def render_panel_reglas_grafo(st: object, orden_fase: int) -> None:
    """Un expander con reglas sin repetir el bloque del grafo base entre fases."""
    if orden_fase not in (3, 4, 5):
        return
    titulo = "Reglas de negocio y evolución del grafo (fases 3–5)"
    with st.expander(titulo, expanded=False):
        st.markdown(markdown_panel_reglas_unificado(orden_fase))
