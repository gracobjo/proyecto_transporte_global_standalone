from __future__ import annotations

import json
from pathlib import Path

from ingesta import ingesta_kdd


def test_interpolacion_gps_generates_contract_fields():
    rutas = [["Madrid", "Barcelona"]]

    posiciones = ingesta_kdd.interpolacion_gps_15min(rutas, paso_15min=2)

    assert len(posiciones) == 1
    camion = posiciones[0]
    assert camion["id_camion"] == "camion_1"
    assert camion["id"] == "camion_1"
    assert camion["ruta_origen"] == "Madrid"
    assert camion["ruta_destino"] == "Barcelona"
    assert camion["distancia_total_km"] > 0
    assert 0 <= camion["progreso_pct"] <= 100
    assert "posicion_actual" in camion


def test_main_generates_payload_and_meta(monkeypatch, tmp_path):
    monkeypatch.setattr(ingesta_kdd, "BASE", tmp_path)
    monkeypatch.setattr(
        ingesta_kdd,
        "consulta_clima_hubs",
        lambda api_key=None: {"Madrid": {"descripcion": "despejado", "temp": 20, "humedad": 40, "viento": 3}},
    )
    monkeypatch.setattr(
        ingesta_kdd,
        "obtener_incidencias_dgt",
        lambda **kwargs: {"source_mode": "disabled", "error": None, "incidencias": [], "mapeo_nodos": {}},
    )
    monkeypatch.setattr(ingesta_kdd, "publicar_kafka", lambda payload: True)
    monkeypatch.setattr(ingesta_kdd, "guardar_hdfs", lambda payload: True)
    monkeypatch.setenv("SIMLOG_INGESTA_CANAL", "script_python")
    monkeypatch.setenv("SIMLOG_INGESTA_ORIGEN", "pytest_cli")
    monkeypatch.setenv("SIMLOG_INGESTA_EJECUTOR", "pytest")

    payload = ingesta_kdd.main(5)

    assert payload["canal_ingesta"] == "script_python"
    assert payload["origen"] == "pytest_cli"
    assert payload["ejecutor_ingesta"] == "pytest"
    assert "clima" in payload and payload["clima"]
    assert "resumen_dgt" in payload
    assert "alertas_operativas" in payload
    assert "nodos_estado" in payload and "estados_nodos" in payload
    assert "aristas_estado" in payload and "estados_aristas" in payload
    assert "camiones" in payload and payload["camiones"]

    meta_path = tmp_path / "reports" / "kdd" / "work" / "ultima_ingesta_meta.json"
    payload_path = tmp_path / "reports" / "kdd" / "work" / "ultimo_payload.json"
    assert meta_path.exists()
    assert payload_path.exists()

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    saved_payload = json.loads(payload_path.read_text(encoding="utf-8"))
    assert meta["ok_kafka"] is True
    assert meta["ok_hdfs"] is True
    assert meta["canal_ingesta"] == "script_python"
    assert saved_payload["origen"] == "pytest_cli"


def test_fixture_payload_respects_expected_contract(load_json_fixture):
    payload = load_json_fixture("ingesta_normal.json")

    assert payload["canal_ingesta"] == "script_python"
    assert payload["camiones"][0]["posicion_actual"]["lat"] == payload["camiones"][0]["lat"]
    assert payload["camiones"][0]["ruta_sugerida"] != []
    assert len(payload["clima"]) == len(payload["clima_hubs"])
