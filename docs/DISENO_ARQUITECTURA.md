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
| Histórico analítico | Consultas de tendencia, reporting y analítica de incidencias (riesgo por hub 24h, top causas 24h) | Hive |
| Orquestación | Programación y ejecución encadenada | Airflow |
| Visualización | Operación, rutas/métricas y **guía del ciclo KDD** (fases, payload, topología vs mapa) + asistente de flota | Streamlit + Folium + Altair (topología en pestaña KDD) |
| FAQ IA | Resolución de preguntas frecuentes sobre operación, servicios e informes | FastAPI + KB JSON local |
| Asistente de Flota | Traducción lenguaje natural → consultas supervisadas (Cassandra/Hive) | Streamlit + cassandra-driver + PyHive |
| Graph AI microservicio | Detección de anomalías y scoring por snapshot de grafo | FastAPI + NetworkX |
| Persistencia de anomalías | Almacenamiento de resultados Graph AI para reporting | Cassandra (`graph_anomalies`) |

## Flujo de extremo a extremo

1. Ingesta produce payload con contrato canónico (`camiones`, `nodos_estado`, `aristas_estado`, `clima_hubs`).
2. Publicación en Kafka (`transporte_raw`, `transporte_filtered`) y backup en HDFS.
3. Spark lee desde HDFS/Kafka, normaliza datos y aplica lógica de grafo.
4. Persistencia en Cassandra (estado actual) y Hive (histórico).
5. Dashboard y API consumen Cassandra para operación diaria.
6. La pestaña **Ciclo KDD** del dashboard enlaza fases con scripts y ficheros del repo, permite probar **OpenWeather** con clave opcional en sesión y muestra **una** vista topológica de la red para fases 3–5 (detalle en `docs/DASHBOARD_KDD_UI.md`).
7. La pestaña **Servicios** integra un panel **FAQ IA** que consulta un microservicio local (`servicios/api_faq_ia.py`) y devuelve respuesta, nivel de confianza, sugerencias y fuentes.
8. El **Asistente de Flota** (Streamlit) traduce lenguaje natural a consultas supervisadas (Cassandra/Hive) y muestra resultados como tablas; y un pipeline periódico ejecuta **Graph AI** para registrar anomalías en `graph_anomalies`.

## Decisiones de diseño

- **Cassandra como estado operativo**: lectura rápida para UI/API.
- **Hive como histórico**: análisis temporal, agregados y derivación de incidencias para reporting 24h.
- **HDFS como origen auditable**: replay/reproceso de snapshots.
- **Limpieza previa a persistencia**: nulos, duplicados y estados no canónicos.
- **Standalone-first**: menos fricción en desarrollo; compatible con salto a YARN.
- **FAQ IA local**: soporte contextual sin dependencia de proveedores externos; base de conocimiento versionable.

## Contratos de datos clave

- `camiones`: `id_camion`, `lat`, `lon`, `ruta`, `ruta_origen`, `ruta_destino`, `ruta_sugerida`, `estado_ruta`, `motivo_retraso`.
- `nodos_estado`: estado operativo y variables de clima por nodo.
- `aristas_estado`: estado por tramo, motivo y distancia.

## Operación y resiliencia

- Back-pressure y DLQ en flujos NiFi/Kafka cuando aplique.
- Reintentos y timeouts en ingesta y orquestación.
- Separación `raw`/`filtered` para auditoría y consumo.
- Tareas periódicas de mantenimiento (limpieza HDFS, verificación de servicios).
- Transparencia operativa: FAQ IA devuelve fuentes y pregunta emparejada para evitar respuestas opacas.

## Evolución prevista

- Seguir ampliando la base de conocimiento del FAQ con incidencias y procedimientos recurrentes.
- Endurecer validaciones de contrato JSON (esquemas explícitos).
- Añadir evidencias de pruebas E2E para rúbrica académica.

## Diagramas

Los diagramas visuales actualizados (Mermaid) están en [DIAGRAMAS_MERMAID.md](DIAGRAMAS_MERMAID.md) y en [DISENO_SISTEMA.md](DISENO_SISTEMA.md).
