# Plan De Pruebas KDD

## 1. Objetivo

Definir, ejecutar y documentar un plan de pruebas profesional para `SIMLOG`, cubriendo las fases operativas del ciclo KDD sobre el stack real del proyecto:

- Fase I: ingesta (`NiFi`, `Kafka`, `HDFS`, `ingesta/ingesta_kdd.py`)
- Fase II: preprocesamiento y transformación (`Spark`, `Hive`, `Cassandra`)
- Fase III: minería, streaming y eventos críticos (`Structured Streaming`, `graph_ai`, `Cassandra`)
- Fase IV: orquestación (`Airflow`)

En `SIMLOG` el estado actual se valida contra `Cassandra`; no se usa `MongoDB`.

## 2. Alcance Y Componentes

Componentes bajo prueba:

- `ingesta/ingesta_kdd.py`
- `nifi/` y `scripts/recreate_nifi_practice_flow.py`
- `procesamiento/procesamiento_grafos.py`
- `procesamiento/streaming_ventanas_15min.py`
- `graph_ai/graph_processing.py`
- `persistencia_hive.py`
- `orquestacion/dag_maestro.py`
- `orquestacion/dag_simlog_kdd_fases.py`
- `orquestacion/test_pipeline_transporte_global.py`

Persistencia validada:

- `HDFS`: backup raw de ingesta
- `Kafka`: `transporte_raw`, `transporte_filtered`
- `Cassandra`: `nodos_estado`, `aristas_estado`, `tracking_camiones`, `pagerank_nodos`, `eventos_historico`, `graph_anomalies`
- `Hive`: `historico_nodos`, `transporte_ingesta_completa`, `tracking_camiones_historico`, `clima_historico`, `eventos_historico`

## 3. Entornos De Prueba

### Entorno 1 — CLI

Validación por terminal, sin interfaces gráficas.

- Arranque de componentes por comandos.
- Verificación por `hdfs dfs`, `cqlsh`, `beeline`, consumidores Kafka y logs.
- Scripts de apoyo en `scripts/testing/`.

### Entorno 2 — GUI

Validación visual de flujos y ejecución.

- `NiFi UI`: estado del `Process Group`, colas, back-pressure, rutas `success/failure`.
- `Airflow UI`: DAGs, retries, tiempos, estados `success/failed/up_for_retry`.
- `Streamlit`: pestaña `Pruebas` para registrar evidencias y crecimiento de históricos.

### Entorno 3 — Orquestado

Validación end-to-end mediante Airflow.

- DAG existente `simlog_pipeline_maestro`
- DAG por fases `simlog_kdd_*`
- DAG de pruebas `test_pipeline_transporte_global`

## 4. Estrategia De Pruebas

### 4.1 Tipos De Prueba

| Tipo | Propósito | Artefactos |
|---|---|---|
| Unitarias | Validar transformaciones y reglas de negocio aisladas | `tests/test_cleaning.py`, `tests/test_enrichment.py`, `tests/test_streaming.py`, `tests/test_events.py` |
| Integración | Validar paso de datos entre componentes | scripts CLI, DAG de testing, consultas HDFS/Cassandra/Hive |
| End-to-end | Validar el ciclo completo desde ingesta a persistencia | `orquestacion/test_pipeline_transporte_global.py` |
| Calidad de datos | Validar nulos, duplicados, rangos y consistencia | datasets `tests/data/`, tests Spark |
| Resiliencia / fallos | Validar retries, fallos forzados, retrasos, pérdida de señal y cascada | DAG de testing, checklist GUI, `test_events.py` |

### 4.2 Objetivos Por Fase KDD

| Fase | Objetivo de validación |
|---|---|
| Fase I — Ingesta | Garantizar que el snapshot se genera, publica y persiste de forma trazable |
| Fase II — Transformación | Garantizar limpieza, normalización y enriquecimiento correctos |
| Fase III — Minería / acción | Garantizar agregación por ventanas, detección de retrasos, congestión y fallo en cascada |
| Fase IV — Orquestación | Garantizar ejecución automática fiable, dependencias, retries y reporte |

## 5. Datos De Prueba

Datasets reproducibles incluidos en `tests/data/`:

| Dataset | Archivo | Uso |
|---|---|---|
| Normal | `tests/data/ingesta_normal.json` | Contrato canónico de ingesta |
| Errores | `tests/data/ingesta_errores.json` | Nulos, duplicados, coordenadas inválidas, velocidad no válida |
| Extremos | `tests/data/streaming_extremos.json` | Retrasos elevados y congestión |
| Crítico | `tests/data/fallo_cascada.json` | Pérdida simultánea de vehículos + cambio estructural del grafo |

Escenarios cubiertos:

- datos normales
- datos con nulos
- duplicados
- velocidades anómalas
- retrasos altos
- pérdida de señal
- evento en cascada

## 6. Casos De Prueba

### 6.1 Fase I — Ingesta

| ID | Caso | Entrada | Resultado esperado | Medio |
|---|---|---|---|---|
| FI-01 | Generación de payload sintético | `ingesta/ingesta_kdd.py` | JSON con `clima`, `camiones`, `nodos_estado`, `aristas_estado`, alias y metadatos de origen | `tests/test_ingestion.py` |
| FI-02 | Publicación Kafka | ejecución de ingesta | Mensajes visibles en `transporte_filtered` | `scripts/testing/consume_kafka_messages.sh` |
| FI-03 | Persistencia raw en HDFS | ejecución de ingesta | aparece al menos un JSON nuevo en `HDFS_BACKUP_PATH` | `scripts/testing/check_hdfs_ingesta.sh` |
| FI-04 | Trazabilidad del canal | ejecución por Streamlit / Airflow / script / NiFi | payload/meta etiquetados con `canal_ingesta`, `origen`, `ejecutor_ingesta` | pestaña `Pruebas`, tests de ingesta |
| FI-05 | Back-pressure / error visible | cola o processor bloqueado en NiFi | la UI muestra cola detenida o processor en failure, sin pérdida silenciosa | checklist GUI |

### 6.2 Fase II — Preprocesamiento Y Transformación

| ID | Caso | Entrada | Resultado esperado | Medio |
|---|---|---|---|---|
| FII-01 | Eliminación de nulos | dataset con `id_vehiculo` nulo | registros inválidos descartados | `tests/test_cleaning.py` |
| FII-02 | Eliminación de duplicados | dos registros idénticos vehículo-timestamp | un único registro persiste | `tests/test_cleaning.py` |
| FII-03 | Validación geográfica | lat/lon fuera de España | registro descartado | `tests/test_cleaning.py` |
| FII-04 | Velocidad válida | `velocidad <= 0` | registro descartado | `tests/test_cleaning.py` |
| FII-05 | JOIN con maestros | GPS + maestro de almacenes | origen, destino, ruta y hub quedan enriquecidos | `tests/test_enrichment.py` |
| FII-06 | Persistencia en Cassandra | procesamiento Spark completo | tablas operativas con conteos > 0 | `query_cassandra_estado.sh`, DAG testing |
| FII-07 | Persistencia en Hive | procesamiento Spark con Hive habilitado | `historico_nodos` y `transporte_ingesta_completa` accesibles | `query_hive_history.sh`, DAG testing |

### 6.3 Fase III — Streaming Y Eventos Críticos

| ID | Caso | Entrada | Resultado esperado | Medio |
|---|---|---|---|---|
| FIII-01 | Ventanas de 15 min | stream con timestamps dentro de la misma ventana | agregación correcta por ruta y ventana | `tests/test_streaming.py` |
| FIII-02 | Retraso medio por ruta | retrasos altos en una ruta | `retraso_medio_min` > umbral | `tests/test_streaming.py`, `tests/test_events.py` |
| FIII-03 | Detección de congestión | varios vehículos en `Congestionado` | evento `congestion` | `tests/test_events.py` |
| FIII-04 | Pérdida de vehículo | uno o más vehículos `Perdido` / `Sin señal` | evento `perdida_vehiculo` | `tests/test_events.py` |
| FIII-05 | Fallo en cascada | ≥ 3 vehículos perdidos en la misma ventana | evento `fallo_cascada` con severidad crítica | `tests/test_events.py` |
| FIII-06 | Cambio estructural del grafo | snapshot anterior vs actual degradado | anomalías en `graph_ai` | `tests/test_events.py` |
| FIII-07 | Persistencia de eventos/anomalías | ejecución de streaming / graph ai | datos en `eventos_historico` y `graph_anomalies` | CLI + DAGs |

### 6.4 Fase IV — Orquestación Airflow

| ID | Caso | Entrada | Resultado esperado | Medio |
|---|---|---|---|---|
| FIV-01 | DAG maestro completo | trigger `simlog_pipeline_maestro` | tareas `success`, outputs visibles | Airflow UI / CLI |
| FIV-02 | DAG por fases | trigger `simlog_kdd_00_infra` | secuencia 00→99 completa con informes | Airflow UI |
| FIV-03 | Retry automático | fallo transitorio o forzado | tarea entra en retry y luego recupera | Airflow UI / `SIMLOG_TEST_FORCE_FAIL_TASK` |
| FIV-04 | Fallo controlado | forzar fallo en DAG de testing | ejecución queda documentada como `failed` | `test_pipeline_transporte_global` |
| FIV-05 | Reporte de testing | fin del DAG de testing | JSON/Markdown en `reports/tests/<run_id>/` | DAG testing |

## 7. Matriz De Trazabilidad

| Requisito | Componente | Casos |
|---|---|---|
| RF-KDD-ING-01: ingestar snapshot logístico completo | `ingesta/ingesta_kdd.py`, `nifi/` | FI-01, FI-02, FI-03, FI-04 |
| RF-KDD-ING-02: garantizar persistencia raw | `Kafka`, `HDFS` | FI-02, FI-03 |
| RF-KDD-TR-01: limpieza de nulos y duplicados | `procesamiento/testing_kdd_utils.py`, `procesamiento/procesamiento_grafos.py` | FII-01, FII-02 |
| RF-KDD-TR-02: validación geográfica y velocidad | transformaciones Spark | FII-03, FII-04 |
| RF-KDD-TR-03: enriquecimiento con maestros | `Hive`, joins Spark | FII-05 |
| RF-KDD-MIN-01: retraso medio por ruta | `procesamiento/streaming_ventanas_15min.py` | FIII-01, FIII-02 |
| RF-KDD-MIN-02: detección de congestión y pérdida | `procesamiento/testing_kdd_utils.py` | FIII-03, FIII-04 |
| RF-KDD-MIN-03: detección de fallo en cascada | `graph_ai/graph_processing.py`, `dag_graph_ai_anomalias.py` | FIII-05, FIII-06 |
| RF-KDD-PER-01: persistencia operacional actual | `Cassandra` | FII-06, FIII-07 |
| RF-KDD-PER-02: persistencia histórica | `Hive` | FII-07, FIII-07 |
| RF-KDD-ORQ-01: ejecución automática programada | `dag_maestro.py`, `dag_simlog_kdd_fases.py` | FIV-01, FIV-02 |
| RF-KDD-ORQ-02: retries y fallo controlado | `test_pipeline_transporte_global.py` | FIV-03, FIV-04, FIV-05 |

## 8. Checklist GUI

### 8.1 NiFi UI

- `PG_SIMLOG_KDD` presente y arrancado
- processors `running` o `stopped` según la prueba, nunca `invalid`
- colas sin crecimiento indefinido
- relaciones `failure` conectadas a logging
- back-pressure visible y controlado
- FlowFiles procesados con timestamps recientes

### 8.2 Airflow UI

- DAG `simlog_pipeline_maestro` visible
- DAG `test_pipeline_transporte_global` visible
- tasks en `success` tras la ejecución nominal
- retries visibles cuando se fuerza fallo
- logs accesibles desde cada task
- duración razonable sin bloqueo de dependencias

## 9. Scripts CLI Del Plan

Ubicación: `scripts/testing/`

| Script | Objetivo |
|---|---|
| `check_kafka_topics.sh` | listar topics |
| `consume_kafka_messages.sh` | consumir mensajes del topic |
| `check_hdfs_ingesta.sh` | listar JSON de HDFS |
| `query_hive_history.sh` | consultar Hive |
| `query_cassandra_estado.sh` | consultar Cassandra |
| `show_spark_logs.sh` | revisar logs Spark |

## 10. Tests Automatizados

Ubicación: `tests/`

| Fichero | Cobertura |
|---|---|
| `tests/test_ingestion.py` | contrato de payload y metadatos de ingesta |
| `tests/test_cleaning.py` | nulos, duplicados, rangos, velocidad |
| `tests/test_enrichment.py` | JOIN con maestros |
| `tests/test_streaming.py` | ventanas de 15 minutos |
| `tests/test_events.py` | retrasos, congestión, pérdida, cascada, anomalías |

Ejecución recomendada:

```bash
source venv_transporte/bin/activate
pip install -r requirements-dev.txt
pytest tests -q
```

## 11. DAG De Testing

`orquestacion/test_pipeline_transporte_global.py`

Cobertura:

- verifica servicios base
- ejecuta la ingesta Python con etiquetado de testing
- ejecuta el procesamiento Spark
- valida salidas en HDFS, Cassandra y Hive
- genera un reporte en `reports/tests/<run_id>/`
- soporta fallo forzado con `SIMLOG_TEST_FORCE_FAIL_TASK`

## 12. Resultados Esperados Y Criterios De Éxito

### Éxito

- se genera al menos un payload de ingesta válido
- hay mensajes en Kafka y JSON en HDFS
- `tracking_camiones` y `nodos_estado` quedan pobladas
- `historico_nodos` y `transporte_ingesta_completa` se pueden consultar
- los eventos críticos esperados se detectan en datasets extremos
- Airflow completa el DAG de testing y produce reporte

### Fallo

- payload incompleto o sin trazabilidad de origen
- no aparecen nuevos JSON en HDFS tras la ingesta
- Cassandra no recibe estado actual
- Hive no expone histórico esperado
- no se detectan retrasos extremos o cascadas en escenarios sintéticos
- Airflow no respeta dependencias o retries

## 13. Justificación Técnica

Este plan sigue criterios de sistemas distribuidos industriales:

- pruebas unitarias para reglas deterministas de transformación
- pruebas de integración para confirmar contratos entre componentes asíncronos
- pruebas end-to-end para validar la cadena completa
- datasets extremos para validar resiliencia ante fallos y degradación operativa
- evidencias persistentes para auditoría y repetibilidad

En streaming, la validación no debe limitarse al valor puntual de una tabla; debe comprobar:

- crecimiento controlado del histórico
- estabilidad del estado actual
- consistencia temporal por ventanas
- comportamiento ante datos tardíos o ausentes

## 14. Artefactos Entregados

- Documento: `docs/PLAN_PRUEBAS_KDD.md`
- Utilidades Spark de testing: `procesamiento/testing_kdd_utils.py`
- Datasets: `tests/data/*.json`
- Tests `pytest`: `tests/test_*.py`
- Dependencias de desarrollo: `requirements-dev.txt`
- Scripts CLI: `scripts/testing/*.sh`
- DAG de testing: `orquestacion/test_pipeline_transporte_global.py`
