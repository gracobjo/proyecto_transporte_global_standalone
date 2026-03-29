-- Asignaciones de ruta desde el cuadro de mando (Streamlit).
--
-- Si cqlsh devuelve OperationTimedOut al crear la tabla:
--   1) Reintenta con timeout mayor:
--        cqlsh --request-timeout=180 -f cassandra/migracion_asignaciones_ruta_cuadro.cql
--      (si tu cqlsh no admite --request-timeout, usa el script Python del repo.)
--   2) python3 scripts/aplicar_migracion_asignaciones_cuadro.py
--   3) Comprueba cluster: nodetool status · que el keyspace exista · carga en el nodo.
--
USE logistica_espana;

CREATE TABLE IF NOT EXISTS asignaciones_ruta_cuadro (
    dia DATE,
    id_camion TEXT,
    id_ruta TEXT,
    origen TEXT,
    destino TEXT,
    creado_en TIMESTAMP,
    PRIMARY KEY ((dia), id_camion)
) WITH CLUSTERING ORDER BY (id_camion ASC);
