# Manual de Desarrollador — SIMLOG España

Guía para extender y operar la plataforma SIMLOG, incluyendo:

- **FAQ IA** (microservicio local + base de conocimiento JSON)
- **Asistente de Flota** (lenguaje natural → consultas supervisadas)
- **Graph AI** (FastAPI + NetworkX) para detección de anomalías
- Integración con **Cassandra**, **Hive**, **Airflow**

## 1. Estructura general (repo)

Componentes principales:

- `ingesta/`: genera snapshots (clima OpenWeather o respaldo DGT + incidentes + GPS simulado) y publica en Kafka + backup JSON en HDFS. Incluye `ingesta_dgt_datex2.py` para integrar incidencias reales DATEX2.
- `procesamiento/`: Spark (GraphFrames) → escribe en Cassandra y (opcionalmente) Hive.
- `orquestacion/`: DAGs Airflow.
- `servicios/`: módulos para Streamlit (UI) y consultas supervisadas (Cassandra/Hive).
- `servicios/api_faq_ia.py`, `servicios/ui_faq_ia.py`, `servicios/faq_knowledge_base.json`: FAQ IA operativa.
- `graph_ai/`: microservicio de análisis de grafos (FastAPI + NetworkX).
- `cassandra/esquema_logistica.cql`: DDL de tablas.

## 2. FAQ IA (microservicio local + KB JSON)

### 2.1 Qué hace

El FAQ IA resuelve preguntas frecuentes sobre operación y uso del proyecto sin salir del dashboard:

- arranque del stack,
- informes PDF,
- estado de NiFi,
- ubicaciones Swagger/OpenAPI,
- dudas recurrentes de consultas y servicios.

No utiliza un LLM externo. La recuperación se basa en:

- tokenización local,
- similitud léxica (`Jaccard`),
- similitud difusa (`SequenceMatcher`),
- base de conocimiento editable en JSON.

### 2.2 Módulos y responsables

- `servicios/api_faq_ia.py`
  - API FastAPI del FAQ.
  - Endpoints: `/health`, `/api/v1/faq/questions`, `/api/v1/faq/ask`.
  - Respuesta estructurada: `answer`, `confidence`, `matched_question`, `suggestions`, `sources`, `engine`.
- `servicios/ui_faq_ia.py`
  - Panel Streamlit embebido en la pestaña **Servicios**.
  - Consulta el microservicio, muestra respuesta y mantiene historial en `st.session_state`.
- `servicios/faq_knowledge_base.json`
  - Base versionada con `question`, `keywords`, `answer`, `sources`.

### 2.3 Arranque y documentación interactiva

```bash
cd ~/proyecto_transporte_global
source venv_transporte/bin/activate
uvicorn servicios.api_faq_ia:app --host 0.0.0.0 --port 8091
```

Documentación:

- Swagger UI: `http://<host>:8091/docs`
- ReDoc: `http://<host>:8091/redoc`
- OpenAPI JSON: `http://<host>:8091/openapi.json`

### 2.4 Cómo ampliar la base de conocimiento

1. Edita `servicios/faq_knowledge_base.json`.
2. Añade un objeto con:
   - `question`
   - `keywords`
   - `answer`
   - `sources`
3. Mantén respuestas cortas, operativas y trazables a ficheros reales.
4. Reinicia el servicio FAQ IA si quieres garantizar recarga limpia.

## 3. Asistente de Flota (lenguaje natural → CQL/HiveQL supervisado)

### 2.1 Qué hace

El asistente traduce una pregunta en lenguaje natural a:

- **CQL Cassandra** (tiempo real)
- **HiveQL** (histórico) usando PyHive

No ejecuta SQL arbitrario: usa **whitelist**/plantillas y heurísticas.

### 2.2 Módulos y responsables

- `servicios/ui_asistente_flota.py`
  - Interfaz Streamlit: `st.chat_input`, muestra `st.dataframe` y el toggle “Ver consulta SQL”.
  - Mantiene historial en `st.session_state`.
- `servicios/gestor_consultas_sql.py`
  - Mapeo (diccionario + heurísticas) de intención → SQL/CQL supervisado.
  - Extracción segura del identificador del camión (ej. `camion_1`, `CAM-001`).
- `servicios/consultas_cuadro_mando.py`
  - Expone `ejecutar_hive_sql_seguro(sql)` con ejecución segura vía PyHive.
  - Mantiene whitelist de consultas Cassandra/Hive para otras partes de la UI.

### 2.3 Integración Hive sin beeline de consola

`ejecutar_hive_sql_seguro()` usa PyHive contra HiveServer2:

- Host/puerto se obtiene de `HIVE_SERVER` o `HIVE_JDBC_URL`.
- Ajusta timeout con `HIVE_QUERY_TIMEOUT_SEC`.
- Intenta modos de auth compatibles (NOSASL/NONE) según entorno.

### 2.3.1 YARN, conflictos de puerto y Beeline (verificación operativa)

Las consultas Hive que lanzan trabajos en cluster (p. ej. motor MapReduce o configuración que delega en YARN) necesitan que el **ResourceManager** esté en marcha. Si desde Beeline o el cliente ves errores de conexión al **puerto 8032** (`Connection refused` hacia `nodo1` o el host del RM), suele faltar **YARN**:

```bash
# Tras tener HDFS operativo (típicamente ya levantado)
/opt/hadoop/sbin/start-yarn.sh
```

Comprueba con `jps` que existen procesos `ResourceManager` y `NodeManager`, y que el RM escucha en **8032** (RPC).

**Conflicto con la UI web del ResourceManager (puerto 8088):** por defecto, YARN enlaza la consola del RM en **8088**. En un nodo donde también corre **Airflow** (u otro servicio que ya use 8088), el ResourceManager **falla al arrancar** (`BindException` / “La dirección ya se está usando”) y no quedará proceso escuchando en 8032. En ese caso hay que cambiar la dirección de la web del RM en `yarn-site.xml`, por ejemplo:

```xml
<property>
  <name>yarn.resourcemanager.webapp.address</name>
  <value>0.0.0.0:18088</value>
</property>
```

(El valor exacto debe ser un puerto libre en la máquina; **18088** es una convención habitual para no chocar con 8088.) Tras editar, reinicia YARN: `stop-yarn.sh` y `start-yarn.sh`. La UI del cluster quedará en `http://<host>:18088/` (ajusta firewall y DNS si aplica).

**Base de datos JDBC:** el nombre del esquema Hive del proyecto viene de `HIVE_DB` en `config.py` (por defecto **`logistica_espana`**). Las URLs `jdbc:hive2://.../logistica_db` u otros nombres solo funcionan si ese esquema existe; si Beeline responde que la base no existe, revisa la variable de entorno `HIVE_DB` o usa el nombre por defecto.

Ejemplo de comprobación rápida con Beeline (ajusta host y usuario):

```bash
export HIVE_HOME=/ruta/a/apache-hive-*-bin
beeline -u 'jdbc:hive2://127.0.0.1:10000/logistica_espana' -n hadoop \
  -e 'SELECT COUNT(*) FROM alertas_historicas;'
```

La primera consulta tras arrancar servicios puede tardar varios minutos (arranque en frío de sesión y cola YARN).

### 2.4 Extender el asistente (nuevo intent / nueva plantilla)

1. Añade una nueva intención en `resolver_intencion_gestor()` en `servicios/gestor_consultas_sql.py`.
2. Define la consulta supervisada:
   - Para Cassandra: asegúrate de que el esquema coincide con `cassandra/esquema_logistica.cql`.
   - Para Hive: define tabla/DDL existente (y ajusta `SIMLOG_HIVE_TABLA_TRANSPORTE` si el nombre difiere).
3. Si necesitas post-procesado (p.ej. ordenar top por `pagerank`), implementa el ajuste en el cliente antes del render (el asistente ya lo soporta en `aplicar_postproceso_gestor`).

## 3.b Integración DATEX2 DGT

### Qué hace

El módulo `ingesta/ingesta_dgt_datex2.py`:

1. Descarga el feed XML DATEX2 v3.6 de la DGT.
2. Lo normaliza a un contrato interno con `id_incidencia`, `severity`, `estado`, `carretera`, `municipio`, `provincia`, `lat`, `lon`, `descripcion`.
3. Proyecta cada incidencia al nodo logístico más cercano.
4. Fusiona la señal DGT con la simulación manteniendo prioridad de `source=dgt`.

### Puntos de extensión

- `parsear_xml_datex2(xml_text)`: ampliar extractores XML.
- `mapear_incidencias_a_nodos(...)`: mejorar matching geográfico o pasar a aristas.
- `fusionar_estados(...)`: cambiar política de desempate.
- `obtener_incidencias_dgt(...)`: endurecer caché, timeout y modo degradado.

### Contrato de salida

- `incidencias_dgt`
- `resumen_dgt`
- `nodos_estado` / `estados_nodos` con:
  - `source`
  - `severity`
  - `peso_pagerank`
  - `id_incidencia`
  - `carretera`, `municipio`, `provincia`
- `clima_hubs` / `clima` con:
  - `source` (`openweather` o `dgt`)
  - `fallback_activo`
  - `estado_carretera`
  - `visibilidad`

### Script standalone

```bash
venv_transporte/bin/python scripts/ejecutar_ingesta_dgt.py --skip-processing
```

### Tests

- `tests/test_ingesta_dgt_datex2.py`
- `tests/test_ingestion.py`

Los tests cubren parseo mínimo de DATEX2, mapeo a nodos y prioridad del merge frente a la simulación.

### Cómo está configurado el uso de información alternativa a OpenWeather

La configuración actual deja a OpenWeather como fuente **preferente pero opcional**. El respaldo operativo se implementa así:

1. `ingesta/ingesta_kdd.py` llama a `consulta_clima_hubs()` para OpenWeather.
2. La respuesta se valida con `_clima_openweather_valido(...)`.
3. En paralelo, `obtener_incidencias_dgt()` devuelve incidencias y `clima_hubs` inferido desde DATEX2.
4. `combinar_clima_hubs(clima_owm, info_dgt["clima_hubs"])` prioriza OpenWeather si es válido; en caso contrario rellena los hubs desde DGT y marca `fallback_activo=true`.
5. `clima_hubs_a_lista(...)` expone el resultado final en el payload canónico que consumen Kafka, HDFS, Spark y UI.

Variables y banderas relevantes:

- `OWM_API_KEY` / `API_WEATHER_KEY`: clave OpenWeather para el camino principal.
- `SIMLOG_USE_DGT`: habilita la integración DATEX2.
- `SIMLOG_DGT_ONLY_CACHE`: fuerza uso de caché DGT si hace falta degradar aún más.

Consecuencia práctica: si OpenWeather devuelve `401`, timeout o JSON inválido, el pipeline sigue siendo consistente y publica clima operativo derivado de DGT.

## 4. Graph AI (FastAPI + NetworkX)

### 3.1 Qué hace

Microservicio desacoplado que:

1. Construye un grafo NetworkX desde JSON (`nodes[]`, `edges[]`).
2. Calcula métricas:
   - degree centrality
   - betweenness centrality (aproximada si el grafo crece)
   - pagerank
3. Detecta anomalías:
   - aislados
   - outliers por grado (z-score)
   - outliers por peso de aristas (z-score)
   - (opcional) cambios estructurales si aportas `previous_graph`
4. Asigna `anomaly_score` por nodo y devuelve lista de anomalías.

### 3.2 Estructura del código

- `graph_ai/models.py`: schemas Pydantic para requests/responses.
- `graph_ai/graph_processing.py`:
  - `build_nx_graph()`
  - `compute_centralities()`
  - `detect_anomalies()`
  - `compare_graphs()`
- `graph_ai/api.py`:
  - `POST /analyze-graph`
  - `POST /compare-graphs`
  - `GET /health`

### 3.3 Endpoints (ejemplos)

`POST /analyze-graph`:

Request:
```json
{
  "graph": {
    "directed": true,
    "nodes": [{"id": "A"}, {"id": "B"}],
    "edges": [{"source": "A", "target": "B", "weight": 10}]
  }
}
```

Response:
 - `centrality_metrics`
 - `anomaly_scores`
 - `anomalous_nodes`

### 3.4 Cassandra: persistencia de anomalías

Tabla:
- `cassandra/esquema_logistica.cql`: `graph_anomalies`

Columnas:
- `id` (UUID)
- `timestamp` (TIMESTAMP)
- `node_id` (TEXT)
- `anomaly_score` (DOUBLE)
- `metric_type` (TEXT)
- `metric_value` (DOUBLE)
- `ts_bucket` (BIGINT, bucket temporal de 15 min)

### 3.5 Elección de clave de partición

Se usa `ts_bucket` + `metric_type` en la PRIMARY KEY (y por tanto como parte de la partición efectiva):

- Facilita consultas típicas: “dame anomalías del último bucket” o “ventana reciente”.
- Limita el tamaño de particiones y reduce el scan por rango.
- Permite añadir tipos de métricas (metric_type) sin mezclar todo en una partición gigante.

### 3.6 Orquestación Airflow

El DAG está en:
- `orquestacion/dag_graph_ai_anomalias.py`

Ejecución:

1. Cada **15 minutos**, fetch del grafo desde Cassandra (`nodos_estado`, `aristas_estado`).
2. Llamada al microservicio `POST /analyze-graph`.
3. Inserción de anomalías en Cassandra (`graph_anomalies`).
4. (Opcional) publicación en Kafka.

## 6. NiFi: relaciones y provenance para DGT

La rama NiFi enriquecida queda así:

`Build_GPS_Sintetico -> OpenWeather_InvokeHTTP -> Merge_Weather_Into_Payload -> DGT_DATEX2_InvokeHTTP -> Merge_DGT_Into_Payload -> Kafka/HDFS/Spark`

Archivos relevantes:

- `nifi/groovy/GenerateSyntheticGpsForPractice.groovy`
- `nifi/groovy/MergeOpenWeatherIntoPayload.groovy`
- `nifi/groovy/MergeDgtDatex2IntoPayload.groovy`
- `scripts/recreate_nifi_practice_flow.py`
- `nifi/flow/simlog_kdd_flow_spec.yaml`

### Atributos de provenance

El merge NiFi deja atributos consultables en `Data Provenance`:

- `simlog.provenance.stage`
- `simlog.provenance.sources`
- `simlog.provenance.dgt_mode`
- `simlog.provenance.dgt_incidents`
- `simlog.provenance.dgt_nodes_affected`

Esto permite diferenciar snapshots puramente simulados de snapshots enriquecidos con señal real DGT, y distinguir cuándo el clima procede de OpenWeather o cuándo se ha tenido que reconstruir desde la fuente alternativa.

### Configuración concreta en NiFi

El fallback a OpenWeather está repartido en estos componentes:

- `Set_Parametros_Ingesta`: inyecta `owm.api.key`, `owm.city.ids` y `dgt.url`.
- `OpenWeather_InvokeHTTP`: intenta la llamada HTTP a OpenWeather.
- `MergeOpenWeatherIntoPayload.groovy`: solo mezcla clima si el estado HTTP es 2xx y el JSON es válido; si no, deja `clima_hubs` vacío o intacto para que el flujo continúe.
- `DGT_DATEX2_InvokeHTTP`: descarga el XML DATEX2.
- `MergeDgtDatex2IntoPayload.groovy`: fusiona incidencias DGT y, si no hay clima OpenWeather disponible (`simlog.weather.available=false` o `clima_hubs` vacío), crea `clima_hubs` alternativo a partir de `condiciones_meteorologicas`, `estado_carretera` y `visibilidad`.

En otras palabras, el flujo se ha configurado para que **OpenWeather no bloquee la ingesta**: la meteorología alternativa entra en el último merge y mantiene intactos Kafka, HDFS y el contrato del payload.

## 6.b Reconfiguración logística en tiempo real

La lógica de resiliencia del grafo se ha separado en:

- `procesamiento/reconfiguracion_grafo.py`: eventos `NODE_DOWN` / `NODE_UP` / `ROUTE_DOWN` / `ROUTE_UP`, cálculo de estado activo del grafo, rutas alternativas y alertas.
- `procesamiento/procesamiento_grafos.py`: integración con Spark, Cassandra e Hive.
- `persistencia_hive.py`: histórico de `alertas_historicas` y `eventos_grafo`.

Estado actual:

- Cassandra conserva `estado_nodos`, `estado_rutas` y `alertas_activas`.
- Hive conserva `alertas_historicas` y `eventos_grafo`.
- Streamlit pinta el overlay de reconfiguración sobre el mapa operativo.

Documento de referencia: `docs/RECONFIGURACION_LOGISTICA_CRITICA.md`.

## 5. Requisitos para correr Graph AI (FastAPI) en tu entorno

1. Instala dependencias del repo:

   ```bash
   pip install -r requirements.txt
   ```

2. Levanta el servicio:

   ```bash
   uvicorn graph_ai.api:app --host 0.0.0.0 --port 8001
   ```

3. Verifica:
   - `GET /health`
   - `POST /analyze-graph` con un ejemplo.

## 5.1 Swagger / OpenAPI (documentación interactiva)

Este proyecto usa **FastAPI**, por lo que incluye documentación interactiva **Swagger UI** y **ReDoc**.

### API principal SIMLOG (`servicios/api_simlog.py`)

Arranque típico:

```bash
uvicorn servicios.api_simlog:app --host 0.0.0.0 --port 8090
```

Documentación:
- Swagger UI: `http://<host>:8090/docs`
- ReDoc: `http://<host>:8090/redoc`
- Esquema OpenAPI JSON: `http://<host>:8090/openapi.json`

### Graph AI (`graph_ai/api.py`)

Arranque típico:

```bash
uvicorn graph_ai.api:app --host 0.0.0.0 --port 8001
```

Documentación:
- Swagger UI: `http://<host>:8001/docs`
- ReDoc: `http://<host>:8001/redoc`
- Esquema OpenAPI JSON: `http://<host>:8001/openapi.json`

### FAQ IA (`servicios/api_faq_ia.py`)

Arranque típico:

```bash
uvicorn servicios.api_faq_ia:app --host 0.0.0.0 --port 8091
```

Documentación:
- Swagger UI: `http://<host>:8091/docs`
- ReDoc: `http://<host>:8091/redoc`
- Esquema OpenAPI JSON: `http://<host>:8091/openapi.json`

Sugerencia: prueba endpoints directamente desde Swagger (botón “Try it out”) para validar payloads y respuestas.

## 5.2 Cluster Big Data en GitHub Codespaces (perfil aislado)

Para evitar conflictos con el stack principal del proyecto, el repositorio incorpora un perfil dedicado a Codespaces:

- `docker-compose.codespaces.yml`
- `Dockerfile.codespaces`
- `hadoop.codespaces.env`
- guía operativa: `docs/CODESPACES_CLUSTER.md`

### Flujo recomendado

1. Crear Codespace sobre `main`.
2. Ejecutar:

   ```bash
   docker compose -f docker-compose.codespaces.yml up -d --build
   docker compose -f docker-compose.codespaces.yml ps
   ```

3. En la pestaña **Ports**, marcar como `Public`:
   - `9870` (Hadoop NameNode UI)
   - `8080` (Spark Master UI)
   - `8888` (Jupyter)

4. Validar logs:

   ```bash
   docker compose -f docker-compose.codespaces.yml logs --tail=120 namenode
   docker compose -f docker-compose.codespaces.yml logs --tail=120 spark-master
   docker compose -f docker-compose.codespaces.yml logs --tail=120 kafka
   ```

### Notas de mantenimiento

- Este perfil no reemplaza `docker-compose.yml` del stack completo.
- Si hay presión de recursos en Codespaces, parar `python-env` (Jupyter) primero.
- No levantar simultaneamente perfil Codespaces y stack completo en el mismo entorno.

## 6. UML / Diagramas (PlantUML) para documentación

### 5.1 Secuencia — Asistente de Flota

```plantuml
@startuml
actor Usuario
participant "Streamlit" as ST
participant "Gestor SQL" as GSQL
participant "Cassandra / Hive" as DB

Usuario -> ST: Pregunta (chat)\n"¿Dónde está el camión 1?"
ST -> GSQL: resolver_intencion_gestor()
GSQL -> DB: Ejecutar CQL/HiveQL supervisado
DB --> ST: Filas (DataFrame/TSV)
ST -> Usuario: st.dataframe + toggle "Ver consulta SQL"
@enduml
```

### 5.2 Secuencia — Graph AI

```plantuml
@startuml
actor Airflow as AF
participant "Cassandra" as C
participant "FastAPI Graph AI" as API
participant "NetworkX" as NX
participant "Cassandra graph_anomalies" as STORE

AF -> C: fetch_graph\n(nodos_estado, aristas_estado)
AF -> API: POST /analyze-graph
API -> NX: build + centralities + anomalies
NX --> API: anomaly_score por nodo
API --> AF: respuesta anomalías
AF -> STORE: INSERT graph_anomalies
@enduml
```

## 7. Checklist de extensiones (para mantener coherencia)

## 7.1 Novedades UI (consultas, informes y navegacion)

Nuevos bloques relevantes:

- `servicios/cuadro_mando_ui.py`
  - **Informes a medida (plantillas + PDF)**:
    - descubrimiento de tablas/columnas por motor,
    - modo `SELECT *` y modo por campos,
    - filtros (`WHERE`), orden (`ORDER BY`), limite,
    - export PDF (`reportlab`),
    - plantillas personalizadas persistidas en `servicios/report_templates.json`.
  - **Consultas libres seguras**:
    - Cassandra: `SELECT` via `ejecutar_cassandra_cql_seguro()`.
    - Hive: `SHOW|SELECT|WITH|DESCRIBE` via `ejecutar_hive_sql_seguro()`.

- `servicios/consultas_cuadro_mando.py`
  - Descubrimiento de metadata:
    - `listar_keyspaces_cassandra()`
    - `listar_tablas_cassandra()`
    - `listar_columnas_cassandra()`
    - `listar_tablas_hive()`
    - `listar_columnas_hive()`

- `app_visualizacion.py`
  - **Buscador semantico rapido** en cabecera.
  - Navegacion determinista por `active_tab` al pulsar hallazgos.
  - Mejora de presentacion (branding en sidebar + cabecera).

- `servicios/ui_faq_ia.py`
  - Panel FAQ IA integrado en **Servicios**.
  - Historial de preguntas por sesión.
  - Respuesta con confianza, coincidencia principal, sugerencias y fuentes.

## 7.2 Servicios del stack: Swagger API incluido

Se integra `api` como servicio gestionable:

- Archivo: `servicios/gestion_servicios.py`
- Operaciones:
  - `comprobar_api()`
  - `iniciar_api()` (lanza `uvicorn servicios.api_simlog:app`)
  - `parar_api()`
- Puerto configurable:
  - `SIMLOG_PORT_API` (default `8090`)

Enlaces de interfaz:

- Archivo: `servicios/ui_servicios_web.py`
- Entrada nueva: `api` con URL por defecto `http://127.0.0.1:8090/docs`.

FAQ IA como servicio gestionable:

- Archivo: `servicios/gestion_servicios.py`
- Operaciones:
  - `comprobar_faq_ia()`
  - `iniciar_faq_ia()`
  - `parar_faq_ia()`
- Puerto configurable:
  - `SIMLOG_PORT_FAQ_IA` (default `8091`)

Antes de añadir nuevas funcionalidades:

- Validar que las columnas reales coinciden con `cassandra/esquema_logistica.cql`.
- Para Hive: validar el DDL de la tabla histórica y rutas/estructuras anidadas del JSON.
- Evitar SQL dinámico del usuario (siempre whitelist/plantillas).
- No acoplar Graph AI con Spark: Graph AI solo consume grafo materializado.

