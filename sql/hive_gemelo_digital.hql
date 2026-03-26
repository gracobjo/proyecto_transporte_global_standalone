-- Gemelo digital — tablas Hive (base logistica_espana)
-- Tras subir CSV a HDFS con procesamiento/generar_red_gemelo_digital.py, ejecutar el DDL de nodos/aristas
-- que imprime ese script (LOCATION .../nodos y .../aristas).

USE logistica_espana;

-- Histórico / ingesta por explorador: struct anidado según modelo de datos
CREATE TABLE IF NOT EXISTS transporte_ingesta_real (
  camiones STRUCT<
    id: STRING,
    progreso_pct: DOUBLE,
    posicion_actual: STRUCT<
      lat: DOUBLE,
      lon: DOUBLE
    >
  >
)
STORED AS PARQUET;

-- Ejemplo (Hive 3+):
-- INSERT INTO transporte_ingesta_real VALUES (
--   named_struct(
--     'id', 'CAM001',
--     'progreso_pct', 42.5,
--     'posicion_actual', named_struct('lat', 40.4168, 'lon', -3.7038)
--   )
-- );
