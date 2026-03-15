-- Crear la base de datos para el proyecto
CREATE DATABASE IF NOT EXISTS logistica_analytics;
USE logistica_analytics;

-- 1. Tabla de Histórico de Clima (Contexto)
CREATE TABLE IF NOT EXISTS clima_hist (
    hub_nombre STRING,
    temperatura FLOAT,
    descripcion STRING,
    humedad INT,
    fecha_captura TIMESTAMP
)
PARTITIONED BY (dia STRING)
STORED AS ORC;

-- 2. Tabla de Eventos de Red (Estado de las Carreteras)
CREATE TABLE IF NOT EXISTS red_transporte_hist (
    origen STRING,
    destino STRING,
    distancia FLOAT,
    estado STRING,        -- 'OK', 'CONGESTIONADO', 'BLOQUEADO'
    motivo_fallo STRING,  -- 'Nieve', 'Incendio', 'Tráfico'
    es_alternativa BOOLEAN,
    timestamp_evento TIMESTAMP
)
PARTITIONED BY (fecha STRING)
STORED AS PARQUET;

-- 3. Tabla de Métricas de Grafo (KDD: Minería)
CREATE TABLE IF NOT EXISTS metricas_nodos_hist (
    id_nodo STRING,
    pagerank_score FLOAT,
    conectividad_grado INT,
    fecha_proceso TIMESTAMP
)
STORED AS PARQUET;
