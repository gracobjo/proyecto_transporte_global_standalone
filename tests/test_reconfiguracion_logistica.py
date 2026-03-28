from __future__ import annotations

from procesamiento.reconfiguracion_grafo import (
    STATUS_ACTIVE,
    STATUS_DOWN,
    aplicar_reconfiguracion_logistica,
)


def _topologia_base():
    nodos = {
        "A": {"lat": 40.0, "lon": -3.0, "tipo": "hub"},
        "B": {"lat": 40.2, "lon": -3.1, "tipo": "secundario"},
        "C": {"lat": 40.4, "lon": -3.2, "tipo": "hub"},
        "D": {"lat": 40.1, "lon": -3.4, "tipo": "secundario"},
    }
    aristas = [
        ("A", "B", 10.0),
        ("B", "C", 10.0),
        ("A", "D", 12.0),
        ("D", "C", 12.0),
    ]
    return nodos, aristas


def _camion_base():
    return [
        {
            "id_camion": "camion_1",
            "ruta": ["A", "B", "C"],
            "ruta_origen": "A",
            "ruta_destino": "C",
            "nodo_actual": "A",
            "estado_ruta": "En ruta",
        }
    ]


def test_caida_nodo_desactiva_rutas_y_recalcula_alternativa(spark):
    nodos, aristas = _topologia_base()
    resultado = aplicar_reconfiguracion_logistica(
        spark,
        nodos,
        aristas,
        _camion_base(),
        eventos=[
            {
                "node_id": "B",
                "event": "NODE_DOWN",
                "timestamp": "2026-03-27T10:00:00Z",
                "cause": "corte eléctrico",
            }
        ],
    )

    assert resultado["estado_nodos"]["B"]["status"] == STATUS_DOWN
    assert resultado["estado_rutas"]["A|B"]["status"] == STATUS_DOWN
    assert resultado["estado_rutas"]["B|C"]["status"] == STATUS_DOWN
    assert "node_down::B" in resultado["alertas_activas"]
    assert resultado["rutas_alternativas"]["camion_1"]["ruta"] == ["A", "D", "C"]


def test_recuperacion_resuelve_alerta_y_prepara_paso_a_hive(spark):
    nodos, aristas = _topologia_base()
    previo = aplicar_reconfiguracion_logistica(
        spark,
        nodos,
        aristas,
        _camion_base(),
        eventos=[
            {
                "node_id": "B",
                "event": "NODE_DOWN",
                "timestamp": "2026-03-27T10:00:00Z",
                "cause": "corte eléctrico",
            }
        ],
    )

    actual = aplicar_reconfiguracion_logistica(
        spark,
        nodos,
        aristas,
        _camion_base(),
        eventos=[
            {
                "node_id": "B",
                "event": "NODE_UP",
                "timestamp": "2026-03-27T10:20:00Z",
                "cause": "recuperado",
            }
        ],
        estado_nodos_previo=previo["estado_nodos"],
        estado_rutas_previo=previo["estado_rutas"],
        alertas_previas=previo["alertas_activas"],
    )

    assert actual["estado_nodos"]["B"]["status"] == STATUS_ACTIVE
    assert any(row["alerta_id"] == "node_down::B" for row in actual["alertas_historicas"])
    assert "node_down::B" not in actual["alertas_activas"]


def test_evento_route_down_mantiene_estado_actual_para_persistencia_cassandra(spark):
    nodos, aristas = _topologia_base()
    resultado = aplicar_reconfiguracion_logistica(
        spark,
        nodos,
        aristas,
        _camion_base(),
        eventos=[
            {
                "src": "A",
                "dst": "B",
                "event": "ROUTE_DOWN",
                "timestamp": "2026-03-27T11:00:00Z",
                "cause": "obras críticas",
            }
        ],
    )

    ruta = resultado["estado_rutas"]["A|B"]
    assert ruta["status"] == STATUS_DOWN
    assert ruta["cause"] == "obras críticas"
    assert resultado["alertas_activas"]["route_down::A|B"]["tipo_alerta"] == "ROUTE_DOWN"


def test_fallo_en_cascada_genera_multiples_alertas_y_sin_ruta_directa(spark):
    nodos, aristas = _topologia_base()
    resultado = aplicar_reconfiguracion_logistica(
        spark,
        nodos,
        aristas,
        _camion_base(),
        eventos=[
            {"node_id": "B", "event": "NODE_DOWN", "timestamp": "2026-03-27T12:00:00Z", "cause": "caída 1"},
            {"node_id": "D", "event": "NODE_DOWN", "timestamp": "2026-03-27T12:01:00Z", "cause": "caída 2"},
        ],
    )

    assert resultado["estado_nodos"]["B"]["status"] == STATUS_DOWN
    assert resultado["estado_nodos"]["D"]["status"] == STATUS_DOWN
    assert len(resultado["alertas_activas"]) >= 4
    assert "camion_1" not in resultado["rutas_alternativas"]
