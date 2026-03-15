-- Datos maestros de nodos (almacenes/hubs) para enriquecimiento desde Hive (PDF Fase II)
CREATE DATABASE IF NOT EXISTS logistica_espana;
USE logistica_espana;

CREATE TABLE IF NOT EXISTS nodos_maestro (
    id_nodo STRING,
    lat DOUBLE,
    lon DOUBLE,
    tipo STRING,
    hub STRING
)
STORED AS PARQUET;

-- Los datos se cargan desde Spark (procesamiento) usando config_nodos o un job inicial.
