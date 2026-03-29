-- =============================================================================
-- SIMLOG España — esquema Cassandra
-- Esquema Cassandra para nodos_estado, tracking_camiones e histórico Hive-equivalent
-- Ejecutar: cqlsh -f esquema_logistica.cql
-- =============================================================================

-- Keyspace con estrategia NetworkTopologyStrategy (replicación)
CREATE KEYSPACE IF NOT EXISTS logistica_espana
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
AND durable_writes = true;

USE logistica_espana;

-- =============================================================================
-- Tabla: nodos_estado - Estado actual de cada nodo (hub/secundario)
-- Tipos: float para coordenadas, timestamp para actualización
-- =============================================================================
CREATE TABLE IF NOT EXISTS nodos_estado (
    id_nodo TEXT PRIMARY KEY,
    lat FLOAT,
    lon FLOAT,
    tipo TEXT,
    estado TEXT,
    motivo_retraso TEXT,
    clima_actual TEXT,
    temperatura FLOAT,
    humedad FLOAT,
    viento_velocidad FLOAT,
    source TEXT,
    severity TEXT,
    id_incidencia TEXT,
    carretera TEXT,
    municipio TEXT,
    provincia TEXT,
    descripcion_incidencia TEXT,
    ultima_actualizacion TIMESTAMP
);

-- =============================================================================
-- Tabla: aristas_estado - Estado de cada arista (ruta entre nodos)
-- =============================================================================
CREATE TABLE IF NOT EXISTS aristas_estado (
    src TEXT,
    dst TEXT,
    distancia_km FLOAT,
    estado TEXT,
    peso_penalizado FLOAT,
    PRIMARY KEY (src, dst)
);

-- =============================================================================
-- Tabla: tracking_camiones - Posición GPS actual y rutas sugeridas
-- Tipos: float para lat/lon, timestamp para hora
-- =============================================================================
CREATE TABLE IF NOT EXISTS tracking_camiones (
    id_camion TEXT,
    lat FLOAT,
    lon FLOAT,
    ruta_origen TEXT,
    ruta_destino TEXT,
    ruta_sugerida LIST<TEXT>,
    estado_ruta TEXT,
    motivo_retraso TEXT,
    ultima_posicion TIMESTAMP,
    PRIMARY KEY (id_camion)
);

-- =============================================================================
-- Tabla: eventos_historico - Para análisis temporal (eventos de estado)
-- =============================================================================
CREATE TABLE IF NOT EXISTS eventos_historico (
    id_evento UUID,
    tipo_entidad TEXT,
    id_entidad TEXT,
    estado_anterior TEXT,
    estado_nuevo TEXT,
    motivo TEXT,
    lat FLOAT,
    lon FLOAT,
    timestamp_evento TIMESTAMP,
    PRIMARY KEY ((id_entidad), timestamp_evento)
) WITH CLUSTERING ORDER BY (timestamp_evento DESC)
  AND default_time_to_live = 2592000;

-- =============================================================================
-- Tabla: pagerank_nodos - PageRank para identificar nodos críticos
-- =============================================================================
CREATE TABLE IF NOT EXISTS pagerank_nodos (
    id_nodo TEXT PRIMARY KEY,
    pagerank FLOAT,
    peso_pagerank FLOAT,
    source TEXT,
    estado TEXT,
    ultima_actualizacion TIMESTAMP
);

-- =============================================================================
-- Tablas de reconfiguración logística en tiempo real
-- estado_nodos / estado_rutas = fotografía actual del grafo operativo
-- alertas_activas = alertas abiertas que aún no se han resuelto
-- =============================================================================
CREATE TABLE IF NOT EXISTS estado_nodos (
    id_nodo TEXT PRIMARY KEY,
    status TEXT,
    cause TEXT,
    updated_at TIMESTAMP,
    last_event TEXT
);

CREATE TABLE IF NOT EXISTS estado_rutas (
    src TEXT,
    dst TEXT,
    status TEXT,
    cause TEXT,
    updated_at TIMESTAMP,
    last_event TEXT,
    manual_down BOOLEAN,
    PRIMARY KEY (src, dst)
);

CREATE TABLE IF NOT EXISTS alertas_activas (
    alerta_id TEXT PRIMARY KEY,
    tipo_alerta TEXT,
    entidad_id TEXT,
    severidad TEXT,
    mensaje TEXT,
    causa TEXT,
    timestamp_inicio TIMESTAMP,
    timestamp_ultima_actualizacion TIMESTAMP,
    estado TEXT,
    ruta_original TEXT,
    ruta_alternativa TEXT
);

-- =============================================================================
-- Tabla: graph_anomalies - Anomalías detectadas por Graph AI (snapshots periódicos)
-- Se particiona por bucket temporal (15 minutos) + metric_type para consultas por ventana.
-- =============================================================================
CREATE TABLE IF NOT EXISTS graph_anomalies (
    id UUID,
    timestamp TIMESTAMP,
    node_id TEXT,
    anomaly_score DOUBLE,
    metric_type TEXT,
    metric_value DOUBLE,
    ts_bucket BIGINT,
    PRIMARY KEY ((ts_bucket, metric_type), timestamp, node_id, id)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- =============================================================================
-- Asignaciones de ruta definidas en el cuadro de mando (día + camión + id_ruta)
-- Spark no escribe aquí; solo la UI al pulsar «Añadir ruta».
-- =============================================================================
CREATE TABLE IF NOT EXISTS asignaciones_ruta_cuadro (
    dia DATE,
    id_camion TEXT,
    id_ruta TEXT,
    origen TEXT,
    destino TEXT,
    creado_en TIMESTAMP,
    PRIMARY KEY ((dia), id_camion)
) WITH CLUSTERING ORDER BY (id_camion ASC);

-- Índices secundarios (ejecutar solo si no existen)
-- CREATE INDEX idx_nodos_estado ON nodos_estado (estado);
-- CREATE INDEX idx_tracking_estado ON tracking_camiones (estado_ruta);
