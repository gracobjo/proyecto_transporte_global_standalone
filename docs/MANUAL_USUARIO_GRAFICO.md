# Manual de Usuario — Guía Rápida (ejemplos guiados)

Este documento es una guía corta con ejemplos concretos (para usuarios finales) y lo que verán como resultado.

## A. “Ciclo KDD”

1. En **Paso temporal**, elige un valor (0–96).
2. Ve a una fase:
   - Fase 1 o 2: pulsa **Ejecutar fase**.
   - Fase 3–5: pulsa **Ejecutar fase** (Spark).

Resultado: datos estructurados en Cassandra y (si aplica) Hive.

## B. “Resultados pipeline”

1. Pulsa **Actualizar comprobaciones**.

Resultado: una tabla/estado por:
- HDFS (backup JSON)
- Kafka (topic)
- Cassandra (tablas y filas)
- Hive (si el metastore responde)

## C. “Asistente flota”

Pruebas:

- Pregunta: “¿Dónde está el camión 1?”
  - Resultado: `st.dataframe` con `id_camion`, `lat`, `lon`, ruta y estado de ruta.
  - Abre “Ver consulta SQL” para inspeccionar CQL.

- Pregunta: “¿Qué carreteras están cortadas?”
  - Resultado: lista de aristas con `estado = 'Bloqueado'` (con `src`, `dst`, `estado`).

- Pregunta: “Historial del camión 1”
  - Resultado: DataFrame con filas de la tabla histórica configurada en Hive (`SIMLOG_HIVE_TABLA_TRANSPORTE`).

