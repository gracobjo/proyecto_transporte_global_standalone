"""
Análisis descriptivo y proyecciones simples sobre datos históricos Hive (cuadro de mando).

Sin dependencia de servicios externos de IA: estadísticas en pandas y tendencia lineal básica.
"""
from __future__ import annotations

import io
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from servicios.consultas_cuadro_mando import ejecutar_hive_consulta


def _tsv_a_dataframe(tsv: str) -> pd.DataFrame:
    if not (tsv or "").strip():
        return pd.DataFrame()
    df = pd.read_csv(io.StringIO(tsv), sep="\t")
    df.columns = [str(c).split(".")[-1].strip() for c in df.columns]
    return df


def _regresion_lineal(xs: List[float], ys: List[float]) -> Tuple[float, float]:
    """Pendiente e intercepto (y = a + b*x)."""
    n = len(xs)
    if n < 2 or len(ys) != n:
        return 0.0, float(ys[-1]) if ys else 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return my, 0.0
    b = num / den
    a = my - b * mx
    return a, b


def ejecutar_paquete_analisis_hive() -> Dict[str, Any]:
    """
    Lanza varias consultas Hive aprobadas y devuelve DataFrames + textos de síntesis.

    Claves: ok_general, errores, dfs, predicciones_md, tablas_resumen_md.
    """
    paquete: Dict[str, Any] = {
        "ok_general": True,
        "errores": [],
        "dfs": {},
        "predicciones_md": "",
        "tablas_resumen_md": "",
    }

    consultas = (
        ("agg_ultima_semana", "Agregaciones última semana"),
        ("eventos_evolucion_dia", "Eventos por día (7 días)"),
        ("gestor_incidencias_resumen", "Resumen incidencias 24h"),
    )

    for codigo, _etiq in consultas:
        ok, err, out = ejecutar_hive_consulta(codigo)
        if not ok:
            paquete["ok_general"] = False
            paquete["errores"].append(f"**{codigo}**: {err or 'error'}")
            paquete["dfs"][codigo] = pd.DataFrame()
            continue
        paquete["dfs"][codigo] = _tsv_a_dataframe(out or "")

    df_agg = paquete["dfs"].get("agg_ultima_semana", pd.DataFrame())
    df_evo = paquete["dfs"].get("eventos_evolucion_dia", pd.DataFrame())
    df_inc = paquete["dfs"].get("gestor_incidencias_resumen", pd.DataFrame())

    lineas_pred: List[str] = []

    # --- Serie diaria desde agregaciones (suma de contador por día calendario) ---
    if not df_agg.empty and {"anio", "mes", "dia", "contador"}.issubset(df_agg.columns):
        dfa = df_agg.copy()
        dfa["contador"] = pd.to_numeric(dfa["contador"], errors="coerce").fillna(0)
        for c in ("anio", "mes", "dia"):
            dfa[c] = pd.to_numeric(dfa[c], errors="coerce")
        dfa = dfa.dropna(subset=["anio", "mes", "dia"])
        if not dfa.empty:
            g = dfa.groupby(["anio", "mes", "dia"], as_index=False)["contador"].sum()

            def _fila_a_fecha(r: pd.Series) -> Optional[date]:
                try:
                    return date(int(r["anio"]), int(r["mes"]), int(r["dia"]))
                except (TypeError, ValueError, OverflowError):
                    return None

            g["fecha"] = g.apply(_fila_a_fecha, axis=1)
            g = g.dropna(subset=["fecha"]).sort_values("fecha")
            if len(g) >= 2:
                xs = list(range(len(g)))
                ys = g["contador"].astype(float).tolist()
                a, b = _regresion_lineal([float(x) for x in xs], ys)
                siguiente = float(a + b * float(len(g)))
                ultimo = float(ys[-1])
                media = float(sum(ys) / len(ys))
                lineas_pred.append(
                    f"- **Actividad agregada (suma `contador` por día)**: últimos **{len(g)}** días con datos. "
                    f"Media diaria ≈ **{media:.1f}**; último día **{ultimo:.1f}**."
                )
                lineas_pred.append(
                    f"- **Proyección naive (tendencia lineal sobre índice de día)**: siguiente punto ≈ **{max(0.0, siguiente):.1f}** "
                    f"unidades de contador agregado (*solo orientativa*, no intervalo de confianza)."
                )
            paquete["dfs"]["_serie_agg_diaria"] = g

    # --- Eventos evolución por día ---
    if not df_evo.empty:
        if "total" in df_evo.columns and "dia" in df_evo.columns:
            de = df_evo.copy()
            de["total"] = pd.to_numeric(de["total"], errors="coerce").fillna(0)
            tot_por_dia = de.groupby("dia", as_index=False)["total"].sum().sort_values("dia")
            if len(tot_por_dia) >= 2:
                xs = list(range(len(tot_por_dia)))
                ys = tot_por_dia["total"].astype(float).tolist()
                a, b = _regresion_lineal([float(x) for x in xs], ys)
                siguiente_e = float(a + b * float(len(tot_por_dia)))
                lineas_pred.append(
                    f"- **Eventos por `dia` (consulta evolución)**: tendencia lineal sugiere **~{max(0.0, siguiente_e):.1f}** "
                    f"eventos agregados en el siguiente bucket de la serie mostrada."
                )
            paquete["dfs"]["_eventos_total_por_dia"] = tot_por_dia

    # --- Resumen incidencias ---
    if not df_inc.empty and "total" in df_inc.columns:
        di = df_inc.copy()
        di["total"] = pd.to_numeric(di["total"], errors="coerce").fillna(0)
        top = di.sort_values("total", ascending=False).head(5)
        if not top.empty:
            filas = []
            for _, r in top.iterrows():
                est = r.get("estado", "")
                filas.append(f"  - `{est}` → **{float(r['total']):.0f}** registros")
            lineas_pred.append("- **Top estados (24h, resumen gestor)**:\n" + "\n".join(filas))

    paquete["predicciones_md"] = "\n".join(lineas_pred) if lineas_pred else "_No hubo datos suficientes para proyecciones._"

    # Tablas markdown cortas (describe numéricas)
    bloques: List[str] = []
    for codigo, etiq in consultas:
        df = paquete["dfs"].get(codigo)
        if df is None or df.empty:
            continue
        num = df.select_dtypes(include=["number"])
        if not num.empty:
            bloques.append(
                f"**{etiq}** (`{codigo}`)\n\n```text\n{num.describe().to_string()}\n```"
            )
    paquete["tablas_resumen_md"] = "\n\n".join(bloques) if bloques else ""

    return paquete
