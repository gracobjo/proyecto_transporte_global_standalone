"""
Regresión: el ExecuteScript NiFi `MergeDgtDatex2IntoPayload.groovy` no debe comparar
listas con `>` (provoca IllegalArgumentException en Groovy: Cannot compare ArrayList...).

Antes de copiar el script al canvas de NiFi, ejecutar: pytest tests/test_merge_dgt_nifi_groovy.py
"""
from __future__ import annotations

from pathlib import Path

import pytest

GROOVY_PATH = Path(__file__).resolve().parents[1] / "nifi" / "groovy" / "MergeDgtDatex2IntoPayload.groovy"


@pytest.fixture(scope="module")
def groovy_src() -> str:
    assert GROOVY_PATH.is_file(), f"Falta el script: {GROOVY_PATH}"
    return GROOVY_PATH.read_text(encoding="utf-8")


def test_groovy_existe() -> None:
    assert GROOVY_PATH.stat().st_size > 500


def test_no_comparacion_listas_score_legacy(groovy_src: str) -> None:
    """Evita la regresión del bulletin ArrayList vs ArrayList."""
    lowered = groovy_src.lower()
    assert "candidatescore" not in lowered
    assert "currentscore" not in lowered
    assert "candidatescore > currentscore" not in groovy_src.replace(" ", "")


def test_fallback_clima_usa_rangos_explicitos(groovy_src: str) -> None:
    """El fallback DGT para clima_hubs debe usar rank/distancia explícitos."""
    assert "candRank" in groovy_src
    assert "curRank" in groovy_src
    assert "boolean replace" in groovy_src or "boolean replace =" in groovy_src


def test_distancia_referencia_consistente(groovy_src: str) -> None:
    assert "distancia_referencia_km: candDist" in groovy_src
