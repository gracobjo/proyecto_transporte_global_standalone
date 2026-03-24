# Diseño de arquitectura (SIMLOG España)

## Objetivo

Definir la arquitectura técnica del sistema para ingesta, procesamiento, persistencia y visualización de la red logística.

## Estilo de arquitectura

- Arquitectura de datos por etapas (KDD)
- Procesamiento batch/near-real-time
- Despliegue preferente standalone (una máquina) con opción de evolución a YARN

## Componentes principales

| Componente | Responsabilidad | Tecnología |
|------------|------------------|------------|
| Ingesta | Construir snapshot de 15 min (clima + incidentes + GPS) | Python / NiFi (opcional) |
| Cola | Buffer y desacoplo de eventos | Kafka |
| Data lake raw | Backup de snapshots y fuente de reproceso | HDFS |
| Procesamiento | Limpieza, autosanación de grafo, analítica | Spark 3.5 + GraphFrames |
| Estado operativo | Consulta rápida en tiempo casi real | Cassandra |
| Histórico analítico | Consultas de tendencia y reporting | Hive |
| Orquestación | Programación y ejecución encadenada | Airflow |
| Visualización | Operación y análisis de rutas/métricas | Streamlit + Folium |

## Flujo de extremo a extremo

1. Ingesta produce payload con contrato canónico (`camiones`, `nodos_estado`, `aristas_estado`, `clima_hubs`).
2. Publicación en Kafka (`transporte_raw`, `transporte_filtered`) y backup en HDFS.
3. Spark lee desde HDFS/Kafka, normaliza datos y aplica lógica de grafo.
4. Persistencia en Cassandra (estado actual) y Hive (histórico).
5. Dashboard y API consumen Cassandra para operación diaria.

## Decisiones de diseño

- **Cassandra como estado operativo**: lectura rápida para UI/API.
- **Hive como histórico**: análisis temporal y agregados.
- **HDFS como origen auditable**: replay/reproceso de snapshots.
- **Limpieza previa a persistencia**: nulos, duplicados y estados no canónicos.
- **Standalone-first**: menos fricción en desarrollo; compatible con salto a YARN.

## Contratos de datos clave

- `camiones`: `id_camion`, `lat`, `lon`, `ruta`, `ruta_origen`, `ruta_destino`, `ruta_sugerida`, `estado_ruta`, `motivo_retraso`.
- `nodos_estado`: estado operativo y variables de clima por nodo.
- `aristas_estado`: estado por tramo, motivo y distancia.

## Operación y resiliencia

- Back-pressure y DLQ en flujos NiFi/Kafka cuando aplique.
- Reintentos y timeouts en ingesta y orquestación.
- Separación `raw`/`filtered` para auditoría y consumo.
- Tareas periódicas de mantenimiento (limpieza HDFS, verificación de servicios).

## Evolución prevista

- Activar NiFi 2.6 en runtime de forma estándar.
- Endurecer validaciones de contrato JSON (esquemas explícitos).
- Añadir evidencias de pruebas E2E para rúbrica académica.
