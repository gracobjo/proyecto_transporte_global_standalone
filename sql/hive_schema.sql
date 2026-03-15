-- =============================================================================
-- Capa de Persistencia Relacional Histórica - Hive
-- Sistema de Gemelo Digital Logístico - España
-- Base de datos: logistica_analytics
-- =============================================================================

CREATE DATABASE IF NOT EXISTS logistica_analytics;
USE logistica_analytics;

-- =============================================================================
-- 1. Tabla de Histórico de Clima (Contexto)
-- Almacena condiciones climáticas de los hubs cada 15 minutos
-- =============================================================================
CREATE TABLE IF NOT EXISTS clima_hist (
    hub_nombre STRING,
    temperatura FLOAT,
    descripcion STRING,
    humedad INT,
    visibilidad INT,
    fecha_captura TIMESTAMP
)
PARTITIONED BY (dia STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =============================================================================
-- 2. Tabla de Eventos de Red (Estado de las Carreteras)
-- Estado de nodos y aristas cada 15 minutos
-- =============================================================================
CREATE TABLE IF NOT EXISTS red_transporte_hist (
    elemento_tipo STRING,          -- 'nodo' o 'arista'
    elemento_id STRING,             -- nombre del nodo o "origen|destino"
    origen STRING,
    destino STRING,
    distancia FLOAT,
    estado STRING,                  -- 'OK', 'CONGESTIONADO', 'BLOQUEADO'
    motivo_fallo STRING,            -- 'Nieve', 'Incendio', 'Tráfico', 'Niebla'
    es_alternativa BOOLEAN,
    pagerank_score FLOAT,
    timestamp_evento TIMESTAMP
)
PARTITIONED BY (fecha STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =============================================================================
-- 3. Tabla de Tracking de Camiones (Posiciones GPS)
-- Historial de posiciones de cada camión
-- =============================================================================
CREATE TABLE IF NOT EXISTS tracking_camiones_hist (
    camion_id STRING,
    origen STRING,
    destino STRING,
    nodo_actual STRING,
    lat_actual FLOAT,
    lon_actual FLOAT,
    progreso_pct FLOAT,
    distancia_total_km FLOAT,
    distancia_recorrida_km FLOAT,
    ruta_actual STRING,             -- формат: "nodo1->nodo2->nodo3"
    ruta_alternativa STRING,
    tiene_alternativa BOOLEAN,
    timestamp_posicion TIMESTAMP
)
PARTITIONED BY (fecha STRING)
STORED AS PARQUET;

-- =============================================================================
-- 4. Tabla de Métricas de Grafo (KDD: Minería)
-- PageRank y métricas de conectividad
-- =============================================================================
CREATE TABLE IF NOT EXISTS metricas_nodos_hist (
    id_nodo STRING,
    tipo_nodo STRING,               -- 'hub' o 'secundario'
    pagerank_score FLOAT,
    conectividad_grado INT,
    betweenness FLOAT,
    clustering_coef FLOAT,
    fecha_proceso TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =============================================================================
-- 5. Tabla de Rutas Calculadas (Histórico de shortestPath)
-- Registro de rutas originales vs alternativas
-- =============================================================================
CREATE TABLE IF NOT EXISTS rutas_calculadas_hist (
    camion_id STRING,
    origen STRING,
    destino STRING,
    ruta_original STRING,
    distancia_original_km FLOAT,
    ruta_alternativa STRING,
    distancia_alternativa_km FLOAT,
    motivo_bloqueo STRING,
    ahorro_km FLOAT,
    timestamp_calculo TIMESTAMP
)
PARTITIONED BY (fecha STRING)
STORED AS PARQUET;

-- =============================================================================
-- 6. Vista para Análisis de Tendencias
-- Consulta rápida de incidentes por fecha
-- =============================================================================
CREATE OR REPLACE VIEW vista_incidentes_dia AS
SELECT 
    fecha,
    elemento_tipo,
    estado,
    motivo_fallo,
    COUNT(*) as total_incidentes
FROM red_transporte_hist
WHERE estado != 'OK'
GROUP BY fecha, elemento_tipo, estado, motivo_fallo;

-- =============================================================================
-- 7. Vista para Rutas Críticas
-- Identifica rutas con más bloqueos
-- =============================================================================
CREATE OR REPLACE VIEW vista_rutas_criticas AS
SELECT 
    origen,
    destino,
    COUNT(*) as bloqueos,
    SUM(CASE WHEN motivo_fallo = 'Incendio' THEN 1 ELSE 0 END) as incendios,
    SUM(CASE WHEN motivo_fallo = 'Nieve' THEN 1 ELSE 0 END) as nevadas,
    SUM(CASE WHEN motivo_fallo = 'Tráfico' THEN 1 ELSE 0 END) as_atascos
FROM red_transporte_hist
WHERE estado = 'BLOQUEADO'
GROUP BY origen, destino;

-- =============================================================================
-- 8. Querys de Análisis de Tendencias (ejemplos)
-- =============================================================================

-- ¿En qué meses hay más incidentes?
SELECT 
    SUBSTR(fecha, 1, 7) as mes,
    estado,
    COUNT(*) as total
FROM red_transporte_hist
WHERE estado != 'OK'
GROUP BY SUBSTR(fecha, 1, 7), estado
ORDER BY mes, total DESC;

-- ¿Qué rutas tienen más bloqueos por nieve o incendio?
SELECT 
    origen,
    destino,
    motivo_fallo,
    COUNT(*) as veces
FROM red_transporte_hist
WHERE motivo_fallo IN ('Nieve', 'Incendio')
GROUP BY origen, destino, motivo_fallo
ORDER BY veces DESC
LIMIT 20;

-- Evolución del PageRank en el tiempo
SELECT 
    id_nodo,
    fecha_proceso,
    pagerank_score
FROM metricas_nodos_hist
WHERE id_nodo IN ('Madrid', 'Barcelona', 'Sevilla')
ORDER BY fecha_proceso;

-- Impacto del clima en el estado de las carreteras
SELECT 
    c.dia,
    c.hub_nombre,
    c.descripcion as clima,
    COUNT(r.estado) as incidentes
FROM clima_hist c
LEFT JOIN red_transporte_hist r ON SUBSTR(c.fecha_captura, 1, 16) = SUBSTR(r.timestamp_evento, 1, 16)
GROUP BY c.dia, c.hub_nombre, c.descripcion
ORDER BY c.dia, incidentes DESC;
