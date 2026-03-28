# Casos de uso — SIMLOG España

Documento funcional para la plataforma en modo standalone.

## Actores

| Actor | Descripción |
|-------|-------------|
| **Operador de plataforma** | Arranca/para servicios, lanza DAGs, ejecuta scripts. |
| **Analista logístico** | Consulta mapa, métricas, histórico. |
| **Planificador** | Evalúa rutas híbridas y alternativas. |
| **Sistema programador** | Airflow, NiFi, cron — ejecución automática. |

## Catálogo

| ID | Caso de uso | Actor principal | Resultado |
|----|-------------|-----------------|-----------|
| CU-01 | Ejecutar ciclo KDD (ingesta → procesamiento) | Operador / Programador | Snapshot en Kafka/HDFS y datos en Cassandra/Hive |
| CU-02 | Supervisar y gobernar el stack | Operador | Servicios coherentes (puertos / estado) |
| CU-03 | Gestionar stack vía script CLI | Operador | `simlog_stack.py start/status/stop` |
| CU-04 | Visualizar estado de red y camiones | Analista | Dashboard Streamlit / mapa |
| CU-05 | Consultar histórico analítico | Analista | Hive / SQL supervisado (+ analítica Hive 24h: riesgo por hub y top causas) |
| CU-06 | Evaluar rutas híbridas | Planificador | Rutas y métricas en UI de planificación |
| CU-07 | Orquestar con Airflow (fases o maestro) | Operador / Programador | DAG runs e informes bajo `reports/kdd/` |
| CU-08 | Ingestar vía NiFi con trigger periódico | Programador | Flujo hacia Kafka/HDFS según `nifi/` |
| CU-09 | Explorar y validar el ciclo KDD en el dashboard | Analista / Operador | Fases enlazadas a código y datos; prueba OpenWeather si hay clave válida; respaldo DGT visible cuando falla; simulación por paso; topología sin duplicar mapas |
| CU-10 | Consultar operativamente con “Asistente de Flota” | Analista / Operador | Traducción lenguaje natural → CQL/HiveSQL supervisado + `st.dataframe` |
| CU-11 | Detectar anomalías en el grafo con Graph AI | Operador / Analista | NetworkX metrics + scoring + persistencia en `graph_anomalies` |
| CU-12 | Desplegar clúster didáctico en GitHub Codespaces | Operador / Docente | Hadoop+Spark+Kafka+Jupyter en perfil aislado `*.codespaces.*` |
| CU-13 | Generar informes a medida (plantillas + PDF) | Analista / Operador | Informe personalizado por tabla/campos/filtros y export PDF |
| CU-14 | Navegar por buscador semántico del dashboard | Analista / Operador | Hallazgos rápidos y salto directo a secciones/pestañas |
| CU-15 | Resolver dudas operativas con FAQ IA | Analista / Operador | Respuesta semántica local con sugerencias y fuentes |
| CU-16 | Integrar incidencias reales DATEX2 DGT | Operador / Programador | Snapshot enriquecido con señal real y prioridad sobre simulación |
| CU-17 | Auditar procedencia de la ingesta en NiFi | Operador | Trazabilidad por relaciones y atributos de provenance |
| CU-18 | Reconfigurar la red logística ante fallo crítico | Operador / Analista | Nodos/rutas desactivados, rutas alternativas recalculadas y alertas activas |

## Detalle breve

### CU-01 — Ejecutar ciclo KDD

- **Precondiciones:** HDFS, Kafka, Cassandra (y opcionalmente Hive) activos.
- **Flujo:** ingesta genera JSON → Kafka + HDFS → Spark procesa → Cassandra + Hive.
- **Variante real:** si DATEX2 DGT está disponible, la ingesta añade incidencias reales; si OpenWeather falla, el clima por hub se reconstruye desde DGT; si la DGT también falla, usa caché o sigue solo con simulación.
- **Disparadores:** Streamlit “Paso siguiente”, Airflow `dag_maestro_smart_grid`, DAGs `simlog_kdd_*`, NiFi.

### CU-02 — Supervisar el stack

- **Flujo:** panel Streamlit “Servicios” o `scripts/comprobar_stack.sh` / API si está expuesta.

### CU-03 — Gestionar stack por CLI

- **Comandos:** `python -u scripts/simlog_stack.py start|status|stop` desde la raíz del proyecto (venv activado).
- **Nota:** no sustituye la configuración de `AIRFLOW_HOME` ni de `[api] base_url` para Airflow.

### CU-04 — Visualizar estado operativo

- **Entrada:** lecturas desde Cassandra en `app_visualizacion.py`.

### CU-05 — Histórico

- **Entrada:** Hive (`logistica_db` u otra base definida en el proyecto).

- **Enfoque analítico 24h:** consultar incidencias derivadas en Hive sobre `logistica_espana.historico_nodos`, clasificando por `estado`, `motivo_retraso` y `clima_actual`.
- **Resultados para el gestor:** informes (1) **Riesgo por hub (últimas 24h)** y (2) **Top causas (últimas 24h)**.

### CU-06 — Rutas híbridas

- **UI:** vistas de planificación / mapa híbrido en el proyecto.

### CU-07 — Airflow

- **Entrada:** UI `http://localhost:8088` (puerto típico SIMLOG) con api-server + scheduler activos.
- **DAGs:** fases `simlog_kdd_00_infra` … `simlog_kdd_99_consulta_final`; maestro `dag_maestro_smart_grid`.

### CU-08 — NiFi

- **Documentación:** `nifi/README_NIFI.md`, especificación de flujo en `nifi/flow/`.
- **Relaciones clave:** `Build_GPS_Sintetico -> OpenWeather_InvokeHTTP -> Merge_Weather_Into_Payload -> DGT_DATEX2_InvokeHTTP -> Merge_DGT_Into_Payload`.
- **Comportamiento actual:** si `OpenWeather_InvokeHTTP` no aporta clima válido, `Merge_Weather_Into_Payload` deja pasar el payload y `Merge_DGT_Into_Payload` genera `clima_hubs` alternativo desde DATEX2.
- **Provenance:** atributos `simlog.provenance.stage`, `simlog.provenance.sources`, `simlog.provenance.dgt_mode`, `simlog.provenance.dgt_incidents`.

### CU-09 — Explorar ciclo KDD en Streamlit

- **Precondiciones:** proyecto clonado; opcionalmente ingesta previa para ver `ultimo_payload.json`.
- **Flujo principal:** abrir pestaña **Ciclo KDD** → elegir fase (◀ ▶ o desplegable) → leer reglas / vistas previas / grafo topológico según fase → en 1–2 ajustar paso, ejecutar ingesta o guardar instantánea y comparar payload → en 1–2 opcionalmente introducir API key y consultar OpenWeather; si no responde, revisar en el snapshot `source=dgt` y `fallback_activo=true`.
- **Postcondiciones:** comprensión del alineamiento fase–script–datos sin exigir lectura directa de todo el código.
- **Diseño:** `docs/DASHBOARD_KDD_UI.md`.

### CU-10 — Asistente de Flota (lenguaje natural → SQL supervisado)

- **Entrada:** usuario pregunta en lenguaje natural (ej. “¿Dónde está el camión 1?”).
- **Flujo:** `resolver_intencion_gestor()` traduce keywords a consultas preaprobadas (whitelist) y ejecuta contra Cassandra (tiempo real) o Hive (histórico).
- **Salida:** tabla `st.dataframe` + opción “Ver consulta SQL”.
- **Restricción clave:** no se ejecuta SQL arbitrario del usuario; solo plantillas alineadas al esquema real.

### CU-11 — Graph AI anomalías (microservicio desacoplado)

- **Entrada:** snapshot del grafo (nodos/aristas) materializado en Cassandra.
- **Flujo:** Airflow llama al microservicio FastAPI `/analyze-graph` (NetworkX) y persiste los resultados en Cassandra (`graph_anomalies`).
- **Salida:** anomalías por nodo con `anomaly_score` y métricas asociadas; opcional Kafka `graph_anomalies`.

### CU-12 — Desplegar clúster en GitHub Codespaces

- **Precondiciones:** repositorio actualizado, Codespace activo, Docker operativo.
- **Flujo principal:** ejecutar `docker compose -f docker-compose.codespaces.yml up -d --build` -> publicar puertos (`9870`, `8080`, `8888`) en modo Public -> validar UIs y logs.
- **Postcondición:** clúster docente disponible sin alterar el `docker-compose.yml` principal.
- **Documentación:** `docs/CODESPACES_CLUSTER.md`.

### CU-13 — Informes a medida (Cassandra/Hive)

- **Entrada:** selección de motor, tabla y campos (o `SELECT *`), filtros, orden y límite.
- **Flujo:** el constructor genera consulta segura de lectura, muestra vista previa y permite guardar plantilla.
- **Salida:** descarga de informe PDF y reutilización por plantilla.

### CU-14 — Búsqueda semántica en cabecera

- **Entrada:** texto libre (ej. “swagger”, “historico hive”, “rutas alternativas”).
- **Flujo:** matching semántico sobre catálogo funcional y botón `Ir a ...`.
- **Salida:** apertura directa de la sección objetivo en la navegación principal.

### CU-15 — FAQ IA (preguntas frecuentes de operación)

- **Entrada:** pregunta libre (ej. “¿cómo genero un informe PDF?” o “¿por qué NiFi no aparece activo?”).
- **Flujo:** la pestaña **Servicios** consulta `servicios/api_faq_ia.py`, que busca coincidencias en `servicios/faq_knowledge_base.json` y devuelve respuesta, confianza, coincidencia principal, sugerencias y fuentes.
- **Salida:** respuesta operativa inmediata sin salir del dashboard; posibilidad de reutilizar preguntas del historial.

### CU-16 — Integrar DATEX2 DGT

- **Entrada:** feed XML DATEX2 v3.6 de la DGT.
- **Flujo:** `ingesta/ingesta_dgt_datex2.py` descarga/parsing -> mapeo a nodos -> merge con simulación -> respaldo climático para hubs cuando OpenWeather falla -> publicación en Kafka/HDFS -> Spark recalcula criticidad.
- **Salida:** nodos afectados con `source=dgt`, `severity`, `peso_pagerank`, `fallback_activo` en clima cuando aplica y evidencia en `transporte_dgt_raw`.

### CU-17 — Auditar procedencia de la ingesta en NiFi

- **Entrada:** evento generado por `PG_SIMLOG_KDD`.
- **Flujo:** revisar `Data Provenance` y atributos `simlog.provenance.*` en los procesadores de merge.
- **Salida:** trazabilidad sobre qué datos proceden de simulación, OpenWeather y DGT, y sobre si el clima final se obtuvo por fuente principal o por respaldo.

### CU-18 — Reconfigurar la red logística ante fallo crítico

- **Entrada:** evento `NODE_DOWN`, `NODE_UP`, `ROUTE_DOWN` o `ROUTE_UP`.
- **Flujo:** `procesamiento/reconfiguracion_grafo.py` actualiza el grafo activo, desactiva rutas afectadas, recalcula alternativas y mantiene alertas activas en Cassandra.
- **Salida:** estado actual en `estado_nodos` / `estado_rutas`, alertas activas y cierre histórico en Hive.

---

## Diagrama de casos de uso (Mermaid)

> Reproducible en GitHub, GitLab, VS Code (Mermaid) y editores compatibles.

```mermaid
flowchart LR
  subgraph Actores
    OP[Operador]
    AN[Analista]
    PL[Planificador]
    PR[Sistema programador]
  end
  subgraph SIMLOG
    CU1[CU-01 Ciclo KDD]
    CU2[CU-02 Supervisar stack]
    CU3[CU-03 Script CLI]
    CU4[CU-04 Visualizar]
    CU5[CU-05 Histórico Hive]
    CU6[CU-06 Rutas híbridas]
    CU7[CU-07 Airflow]
    CU8[CU-08 NiFi]
    CU9[CU-09 Explorar KDD en UI]
    CU10[CU-10 Asistente de Flota]
    CU11[CU-11 Graph AI anomalías]
    CU12[CU-12 Cluster Codespaces]
    CU13[CU-13 Informes a medida]
    CU14[CU-14 Buscador semántico]
    CU15[CU-15 FAQ IA]
    CU16[CU-16 Integrar DATEX2 DGT]
    CU17[CU-17 Auditar provenance NiFi]
  end
  OP --> CU1
  OP --> CU2
  OP --> CU3
  OP --> CU7
  OP --> CU9
  AN --> CU4
  AN --> CU5
  AN --> CU9
  AN --> CU10
  PL --> CU6
  PR --> CU1
  PR --> CU7
  PR --> CU8
  OP --> CU11
  AN --> CU11
  OP --> CU12
  PR --> CU12
  OP --> CU13
  AN --> CU13
  OP --> CU14
  AN --> CU14
  OP --> CU15
  AN --> CU15
  OP --> CU16
  PR --> CU16
  OP --> CU17
```
