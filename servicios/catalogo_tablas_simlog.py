"""
Catálogo de tablas y columnas (Cassandra + Hive) para la UI y documentación del gestor.

No importa persistencia_hive (evita cargar PySpark al abrir el asistente).
"""
from __future__ import annotations

from typing import Dict, List, Tuple

Col = Tuple[str, str]

# Cassandra — esquema_logistica.cql
CASSANDRA_TABLAS: Dict[str, List[Col]] = {
    "nodos_estado": [
        ("id_nodo", "TEXT (PK)"),
        ("lat, lon", "FLOAT"),
        ("tipo, estado, motivo_retraso, clima_actual", "TEXT"),
        ("temperatura, humedad, viento_velocidad", "FLOAT"),
        ("source, severity, carretera, municipio, provincia", "TEXT"),
        ("ultima_actualizacion", "TIMESTAMP"),
    ],
    "aristas_estado": [
        ("src, dst", "TEXT (PK compuesta)"),
        ("distancia_km, peso_penalizado", "FLOAT"),
        ("estado", "TEXT"),
    ],
    "asignaciones_ruta_cuadro": [
        ("dia", "DATE (partition)"),
        ("id_camion", "TEXT (clustering)"),
        ("id_ruta", "TEXT"),
        ("origen, destino", "TEXT"),
        ("creado_en", "TIMESTAMP"),
    ],
    "tracking_camiones": [
        ("id_camion", "TEXT (PK)"),
        ("lat, lon", "FLOAT"),
        ("ruta_origen, ruta_destino", "TEXT"),
        ("ruta_sugerida", "LIST<TEXT>"),
        ("estado_ruta, motivo_retraso", "TEXT"),
        ("ultima_posicion", "TIMESTAMP"),
    ],
    "eventos_historico": [
        ("id_evento", "UUID"),
        ("tipo_entidad, id_entidad", "TEXT"),
        ("estado_anterior, estado_nuevo, motivo", "TEXT"),
        ("lat, lon", "FLOAT"),
        ("timestamp_evento", "TIMESTAMP"),
    ],
    "pagerank_nodos": [
        ("id_nodo", "TEXT (PK)"),
        ("pagerank, peso_pagerank", "FLOAT"),
        ("source, estado", "TEXT"),
        ("ultima_actualizacion", "TIMESTAMP"),
    ],
}

# Hive — alineado con persistencia_hive.TABLE_SCHEMAS y seed_hive_basico
HIVE_TABLAS: Dict[str, List[Col]] = {
    "transporte_ingesta_completa": [
        ("timestamp", "STRING"),
        ("anio, mes, dia, hora, minuto", "INT"),
        ("id_camion", "STRING"),
        ("origen, destino, nodo_actual", "STRING"),
        ("lat, lon, progreso_pct, distancia_total_km", "DOUBLE"),
        ("estado_ruta, motivo_retraso", "STRING"),
        ("ruta, ruta_sugerida, hub_actual", "STRING"),
        ("anio_part, mes_part", "INT"),
    ],
    "eventos_historico": [
        ("timestamp", "STRING"),
        ("tipo_evento, id_elemento, tipo_elemento", "STRING"),
        ("estado, motivo, hub_asociado", "STRING"),
        ("pagerank, distancia_km", "DOUBLE"),
        ("anio_part, mes_part", "INT"),
    ],
    "clima_historico": [
        ("timestamp", "STRING"),
        ("ciudad", "STRING"),
        ("temperatura, humedad", "DOUBLE/INT"),
        ("descripcion, estado_carretera", "STRING"),
        ("anio_part, mes_part", "INT"),
    ],
    "tracking_camiones_historico": [
        ("timestamp", "STRING"),
        ("id_camion, origen, destino, nodo_actual", "STRING"),
        ("lat_actual, lon_actual, progreso_pct", "DOUBLE"),
        ("tiene_ruta_alternativa", "BOOLEAN"),
        ("anio_part, mes_part", "INT"),
    ],
    "rutas_alternativas_historico": [
        ("timestamp", "STRING"),
        ("origen, destino", "STRING"),
        ("ruta_original, ruta_alternativa", "STRING"),
        ("distancia_original_km, distancia_alternativa_km, ahorro_km", "DOUBLE"),
        ("motivo_bloqueo", "STRING"),
        ("anio_part, mes_part", "INT"),
    ],
    "agg_estadisticas_diarias": [
        ("anio, mes, dia", "INT"),
        ("tipo_evento, estado, motivo", "STRING"),
        ("contador", "INT"),
        ("pct_total", "DOUBLE"),
        ("anio_part, mes_part", "INT"),
    ],
    "nodos_maestro": [
        ("id_nodo", "STRING"),
        ("lat, lon", "DOUBLE"),
        ("tipo, hub", "STRING"),
    ],
    "historico_nodos": [
        ("id_nodo", "STRING"),
        ("lat, lon", "DOUBLE"),
        ("tipo, estado, motivo_retraso, clima_actual", "STRING"),
        ("temperatura, humedad, viento_velocidad", "DOUBLE"),
        ("ultima_actualizacion, fecha_proceso", "TIMESTAMP"),
    ],
    "red_gemelo_nodos": [
        ("id_nodo, tipo, id_capital_ref, nombre", "STRING"),
        ("lat, lon", "DOUBLE"),
    ],
    "red_gemelo_aristas": [
        ("src, dst", "STRING"),
        ("distancia_km", "DOUBLE"),
    ],
}


def listar_tablas_cassandra() -> List[str]:
    return sorted(CASSANDRA_TABLAS.keys())


def listar_tablas_hive() -> List[str]:
    return sorted(HIVE_TABLAS.keys())


def columnas_cassandra(tabla: str) -> List[Col]:
    return list(CASSANDRA_TABLAS.get(tabla, []))


def columnas_hive(tabla: str) -> List[Col]:
    return list(HIVE_TABLAS.get(tabla, []))


def dataframe_esquema(columnas: List[Col]):
    import pandas as pd

    if not columnas:
        return pd.DataFrame(columns=["campo", "tipo"])
    rows = [{"campo": c[0], "tipo": c[1]} for c in columnas if isinstance(c, tuple) and len(c) >= 2]
    return pd.DataFrame(rows)
