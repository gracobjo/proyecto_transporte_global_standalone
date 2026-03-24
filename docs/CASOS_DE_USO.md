# Casos de uso (SIMLOG España)

Documento de casos de uso funcionales para la plataforma en modo standalone.

## Actores

- Operador de plataforma
- Analista logístico
- Orquestador (Airflow/NiFi)
- Sistema externo (API OpenWeather)

## Catálogo de casos de uso

| ID | Caso de uso | Actor principal | Resultado |
|----|-------------|-----------------|-----------|
| CU-01 | Ejecutar ingesta KDD | Operador / Orquestador | Snapshot con clima, estados y camiones publicado en Kafka y guardado en HDFS |
| CU-02 | Procesar grafo de transporte | Operador / Orquestador | Grafo autosanado, PageRank y persistencia en Cassandra/Hive |
| CU-03 | Consultar estado operativo | Analista | Visualización actual de nodos/aristas/camiones en dashboard |
| CU-04 | Planificar ruta alternativa | Analista | Ruta principal y alternativas con estimación de retraso/coste |
| CU-05 | Ejecutar pipeline por fases | Operador | Ejecución controlada de fases KDD 1..5 con trazabilidad |
| CU-06 | Revisar calidad de datos | Operador | Detección y tratamiento de nulos/duplicados/estados no canónicos |
| CU-07 | Orquestar operación periódica | Orquestador | Ejecución programada cada 15 min y tareas mensuales de mantenimiento |
| CU-08 | Gestionar servicios del stack | Operador | Inicio/parada/comprobación de HDFS, Kafka, Cassandra, Spark, Hive, NiFi |

## Detalle resumido por caso

### CU-01 Ejecutar ingesta KDD

- **Entrada**: API key OpenWeather, topología de nodos.
- **Flujo**: consulta clima + simulación de incidentes + generación GPS camiones.
- **Salida**: JSON enriquecido en `transporte_raw`/`transporte_filtered` y backup HDFS.

### CU-02 Procesar grafo de transporte

- **Entrada**: JSON desde HDFS/Kafka.
- **Flujo**: limpieza, autosanación de aristas, métricas de centralidad (PageRank).
- **Salida**: tablas operativas en Cassandra y tablas históricas en Hive.

### CU-03 Consultar estado operativo

- **Entrada**: datos de Cassandra.
- **Flujo**: renderizado en mapa y paneles de métricas.
- **Salida**: visión en tiempo casi real de la red.

### CU-04 Planificar ruta alternativa

- **Entrada**: origen/destino y estado de red.
- **Flujo**: cálculo de ruta principal + alternativas ante bloqueos/incidencias.
- **Salida**: pasos de ruta, coste estimado y visualización.

### CU-05 Ejecutar pipeline por fases

- **Entrada**: fase seleccionada (1..5), paso temporal.
- **Flujo**: ejecución controlada por script/pestaña KDD.
- **Salida**: artefactos por fase y trazabilidad en `reports/kdd/work`.

### CU-06 Revisar calidad de datos

- **Entrada**: payloads de ingesta.
- **Flujo**: normalización de estados, tratamiento de nulos, deduplicación por `id_camion`.
- **Salida**: datos válidos para persistencia.

### CU-07 Orquestar operación periódica

- **Entrada**: planificación Airflow (15 min/mensual).
- **Flujo**: tareas encadenadas y control de dependencias.
- **Salida**: pipeline autónomo y mantenido.

### CU-08 Gestionar servicios del stack

- **Entrada**: orden de operación (iniciar/comprobar/parar).
- **Flujo**: comandos de servicio y chequeo por puertos.
- **Salida**: estado técnico actualizado por componente.
