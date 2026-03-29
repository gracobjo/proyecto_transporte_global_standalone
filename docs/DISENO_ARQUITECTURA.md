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
| Ingesta | Construir snapshot de 15 min (clima + incidentes + GPS), usando OpenWeather si está disponible o respaldo DGT si no lo está, y fusionar DATEX2 DGT | Python / NiFi (opcional) |
| Cola | Buffer y desacoplo de eventos | Kafka |
| Data lake raw | Backup de snapshots y fuente de reproceso | HDFS |
| Procesamiento | Limpieza, autosanación de grafo, analítica | Spark 3.5 + GraphFrames |
| Estado operativo | Consulta rápida en tiempo casi real | Cassandra |
| Histórico analítico | Consultas de tendencia, reporting y analítica de incidencias (riesgo por hub 24h, top causas 24h) | Hive |
| Orquestación | Programación y ejecución encadenada | Airflow |
| Visualización | Operación, rutas/métricas, **guía del ciclo KDD** (fases, payload, topología vs mapa), **cuadro de mando** (consultas, informes, flota multi-camión, simulación de movimiento en mapa, correo SMTP opcional) + asistente de flota | Streamlit + Folium + Altair (topología en pestaña KDD) |
| FAQ IA | Resolución de preguntas frecuentes sobre operación, servicios e informes | FastAPI + KB JSON local |
| Asistente de Flota | Traducción lenguaje natural → consultas supervisadas (Cassandra/Hive) | Streamlit + cassandra-driver + PyHive |
| Graph AI microservicio | Detección de anomalías y scoring por snapshot de grafo | FastAPI + NetworkX |
| Persistencia de anomalías | Almacenamiento de resultados Graph AI para reporting | Cassandra (`graph_anomalies`) |

## Flujo de extremo a extremo

1. Ingesta produce payload con contrato canónico (`camiones`, `nodos_estado`, `aristas_estado`, `clima_hubs`) y añade `incidencias_dgt` / `resumen_dgt` cuando el feed DATEX2 está disponible.
2. Si OpenWeather entrega una respuesta válida, `clima_hubs` se publica con `source=openweather`; si no, la ingesta reutiliza el contexto meteorológico inferido desde DGT y marca `fallback_activo=true`.
3. El merge aplica prioridad de `source=dgt` sobre `source=simulacion` y conserva `severity`, `peso_pagerank`, `id_incidencia` y localización.
4. Publicación en Kafka (`transporte_dgt_raw`, `transporte_raw`, `transporte_filtered`) y backup en HDFS.
5. Spark lee desde HDFS/Kafka, normaliza datos y aplica lógica de grafo.
6. Persistencia en Cassandra (estado actual) y Hive (histórico).
7. Dashboard y API consumen Cassandra para operación diaria.
8. La pestaña **Ciclo KDD** del dashboard enlaza fases con scripts y ficheros del repo, permite probar **OpenWeather** con clave opcional en sesión y muestra **una** vista topológica de la red para fases 3–5 (detalle en `docs/DASHBOARD_KDD_UI.md`).
9. La pestaña **Servicios** integra un panel **FAQ IA** que consulta un microservicio local (`servicios/api_faq_ia.py`) y devuelve respuesta, nivel de confianza, sugerencias y fuentes.
10. El **Asistente de Flota** (Streamlit) traduce lenguaje natural a consultas supervisadas (Cassandra/Hive) y muestra resultados como tablas; y un pipeline periódico ejecuta **Graph AI** para registrar anomalías en `graph_anomalies`.

## Decisiones de diseño

- **Cassandra como estado operativo**: lectura rápida para UI/API.
- **Hive como histórico**: análisis temporal, agregados y derivación de incidencias para reporting 24h.
- **HDFS como origen auditable**: replay/reproceso de snapshots.
- **Limpieza previa a persistencia**: nulos, duplicados y estados no canónicos.
- **Señal real prioritaria**: la DGT pisa la simulación solo en nodos afectados, manteniendo continuidad operativa si el feed falla.
- **Clima degradado controlado**: OpenWeather es opcional; cuando no hay clave válida o la respuesta no es usable, el sistema deriva clima operativo desde DATEX2 y conserva el mismo contrato JSON.
- **Modo degradado controlado**: el DAG y el script standalone pueden continuar con caché local o solo simulación.
- **Reconfiguración logística desacoplada**: los eventos de caída/recuperación se resuelven en un módulo específico (`procesamiento/reconfiguracion_grafo.py`) y persisten en Cassandra/Hive sin romper el pipeline KDD principal.
- **Standalone-first**: menos fricción en desarrollo; compatible con salto a YARN.
- **FAQ IA local**: soporte contextual sin dependencia de proveedores externos; base de conocimiento versionable.

## Contratos de datos clave

- `camiones`: `id_camion`, `lat`, `lon`, `ruta`, `ruta_origen`, `ruta_destino`, `ruta_sugerida`, `estado_ruta`, `motivo_retraso`.
- `nodos_estado`: estado operativo y variables de clima por nodo, con `source`, `severity`, `peso_pagerank`, `id_incidencia`, `carretera`, `municipio`, `provincia`.
- `aristas_estado`: estado por tramo, motivo y distancia.
- `incidencias_dgt`: lista normalizada de incidencias reales DATEX2.
- `resumen_dgt`: modo de operación (`live`, `cache`, `disabled`), error y alcance del merge.
- `clima_hubs` / `clima`: incorpora `source` (`openweather` o `dgt`) y `fallback_activo` para distinguir dato principal frente a dato alternativo.

## Operación y resiliencia

- Back-pressure y DLQ en flujos NiFi/Kafka cuando aplique.
- Reintentos y timeouts en ingesta y orquestación.
- Separación `raw`/`filtered` para auditoría y consumo.
- Separación adicional `dgt_raw` para auditar la señal real sin mezclarla con el snapshot completo.
- Tareas periódicas de mantenimiento (limpieza HDFS, verificación de servicios).
- Transparencia operativa: FAQ IA devuelve fuentes y pregunta emparejada para evitar respuestas opacas.
- NiFi conserva atributos `simlog.provenance.*` para inspeccionar en `Data Provenance` qué parte del snapshot procede de simulación, OpenWeather y DGT.
- El modo actual documentado prioriza la continuidad del pipeline con información alternativa DGT mientras OpenWeather no disponga de una API key válida.

## Configuración del respaldo a OpenWeather

- `ingesta/ingesta_kdd.py` combina `consulta_clima_hubs()` con `obtener_incidencias_dgt()` mediante `combinar_clima_hubs(...)`.
- Si OpenWeather devuelve `401`, timeout o JSON inválido, la función usa `info_dgt["clima_hubs"]` y expone `fallback_activo=true`.
- En CLI y Airflow, el comportamiento es automático; no requiere intervención adicional si `SIMLOG_USE_DGT` está habilitado.
- En NiFi, `OpenWeather_InvokeHTTP` intenta enriquecer el payload y `MergeOpenWeatherIntoPayload.groovy` solo incorpora clima cuando la respuesta es válida. Después `MergeDgtDatex2IntoPayload.groovy` reconstruye `clima_hubs` desde incidencias DATEX2 si `simlog.weather.available=false` o el clima está vacío.
- La clave de OpenWeather se inyecta por `.env` / variables (`OWM_API_KEY` o `API_WEATHER_KEY`) en Python y por atributo `owm.api.key` en `Set_Parametros_Ingesta` dentro de NiFi.

## Evolución prevista

- Seguir ampliando la base de conocimiento del FAQ con incidencias y procedimientos recurrentes.
- Endurecer validaciones de contrato JSON (esquemas explícitos).
- Añadir evidencias de pruebas E2E para rúbrica académica.

## Diagramas

Los diagramas visuales actualizados (Mermaid) están en [DIAGRAMAS_MERMAID.md](DIAGRAMAS_MERMAID.md) y en [DISENO_SISTEMA.md](DISENO_SISTEMA.md).
