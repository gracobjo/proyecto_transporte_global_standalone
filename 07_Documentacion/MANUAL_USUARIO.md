# Manual de Usuario — SIMLOG España

Este documento explica **qué hace la aplicación**, cómo usar cada pestaña/menú y **qué resultados** vas a obtener.

## 1. Vista general de la aplicación

La app es un dashboard **Streamlit** para supervisión y análisis del ciclo **KDD** en logística:

- **Cassandra**: estado operativo y datos “tiempo real”.
- **Hive**: histórico y analítica SQL (si HiveServer2 responde).
- **Spark**: procesamiento/analítica que rellena Cassandra (y opcionalmente Hive).
- **Airflow**: orquestación periódica (DAGs).
- **NiFi + Kafka + HDFS**: ingesta, buffer y backup de snapshots.

En la configuración operativa actual, el proyecto sigue funcionando aunque OpenWeather no responda: el contexto meteorológico se reconstruye con información alternativa procedente de incidencias DATEX2 DGT y se marca en el payload con `source="dgt"` y `fallback_activo=true`.

## 2. Arranque rápido (local)

1. Ejecuta el dashboard:

   ```bash
   streamlit run app_visualizacion.py
   ```

2. Si servicios no están activos, usa el menú lateral (sidebar) o la pestaña **Servicios**.
3. Si tienes dudas de uso, en la pestaña **Servicios** encontrarás el bloque **FAQ IA** para preguntas frecuentes del proyecto.

## 3. Sidebar global (qué controles hay y qué hace)

En la **barra lateral izquierda** verás:

### 3.1 Estado de servicios

- Botón **“🔄 Actualizar estado”**: revisa si HDFS/Kafka/Cassandra/Spark/Hive/Airflow/API(Swagger)/NiFi responden por puerto.
- Si todo está OK: puedes usar **“Reiniciar arranque”** (opcional).
- Si falta algo: puedes usar:
  - **“▶ Arrancar base (HDFS + Cassandra + Kafka)”**
  - **“Ejecutar arranque completo (8 servicios)”**

**Resultado esperado**: servicios levantados y listos para ingesta/procesamiento.

### 3.2 Línea temporal (simulación 15 min)

Controles:

- Checkbox **“Paso automático …”**: si está activo, la ingesta usa la ventana de tiempo actual.
- **Número “Paso actual (ventana)”**: el valor se envía como `PASO_15MIN` al ejecutar ingesta.
- Botones:
  - **“Ejecutar ingesta (fases 1–2 KDD)”**
  - **“Ejecutar procesamiento Spark (fases 3–5 KDD)”**
  - **“Avanzar paso + ingesta + procesamiento”**

**Resultado esperado**:

- Tras ingesta: se genera un snapshot (clima + incidentes + GPS simulado). Si OpenWeather responde, el clima sale con `source=openweather`; si no, el sistema usa información alternativa desde la DGT. Si la DGT DATEX2 responde, además se enriquece con incidencias reales; si falla, puede usar caché o continuar solo con simulación. Después se publica en Kafka y se guarda JSON en HDFS.
- Tras Spark: se estructura y persiste el “estado operativo” en Cassandra, y (si está configurado) se actualiza histórico Hive.

### 3.3 Enlace a “Gemelo digital — incidencias”

La sidebar también incluye sliders/checkboxes para la pestaña **Gemelo digital** (atasco, clima, bloqueo de capital).

## 4. Pestañas principales (qué hacer y qué obtienes)

> Nota: el orden de trabajo recomendado suele ser:
> 1) **Ciclo KDD** (disparar ingesta y procesamiento) → 2) **Resultados pipeline** → 3) Consultas/Mapa.

---

## 4.1 Pestaña “Ciclo KDD”

Objetivo: ejecutar **fases 1–5** (ingesta y Spark) y ver la documentación/código asociado.

Qué encontrarás:

- **Selector de fase** (Fase 1 a 5) con navegación (◀/▶ o lista).
- El sistema muestra qué script se usa en esa fase y una vista previa del contenido.
- Te informa que:
  - Fases **1–2** se concentran en `ingesta/ingesta_kdd.py`.
  - Fases **3–5** se concentran en `procesamiento/procesamiento_grafos.py` (Spark).

Acciones:

1. Ajusta el **Paso temporal** (desde la sidebar).
2. En la fase adecuada pulsa:
   - **“Ejecutar fase X”**
   - o usa los botones rápidos desde la sidebar (**ingesta**, **Spark**, **pipeline completo**).

**Resultados esperados**:

- Fase 1–2: se crea el snapshot, se publica a Kafka y se guarda en HDFS.
- Fase 3–5: se calcula el grafo, se aplica autosanación y se actualiza Cassandra (y Hive si procede).

---

## 4.2 Pestaña “Resultados pipeline”

Objetivo: comprobar **fase a fase** si hay datos en:

- HDFS (backup JSON)
- Kafka (topics)
- Cassandra (tablas operativas)
- Hive histórico (opcional)

Controles:

- **“Actualizar comprobaciones”**: consulta HDFS/Kafka/Cassandra/Hive (si se activa).
- **“Crear temas Kafka (raw + filtered) si faltan”**: ejecuta creación de topics.

Secciones:

- **Ingesta (clima + GPS simulado)**: vista del último snapshot local (compatible con el flujo de Spark).
- **Señal DGT**: el snapshot indica si la fuente operó en modo `live`, `cache` o `disabled`, cuántos nodos quedaron afectados y si el clima se está sirviendo como respaldo de OpenWeather.
- **Kafka + HDFS**: confirma accesibilidad y muestra ficheros `.json` en HDFS.
- **Spark → Cassandra**: lista de tablas y número de filas.
- **Hive (histórico opcional)**: muestra `SHOW TABLES` y conteos (si HiveServer2 está OK).

**Resultado esperado**: una “foto” diagnóstica con OK/⚠️ para cada capa.

---

## 4.3 Pestaña “Cuadro de mando”

Objetivo: supervisión operativa + consultas supervisadas.

Qué incluye:

1. **Operaciones por fase**: explicación de qué hace cada fase.
2. **Acciones rápidas**:
   - Ejecutar ingesta (1–2)
   - Ejecutar Spark (3–5)
   - Refrescar clima (si aplica para las slides)
3. **Verificación Kafka**: estado del topic del proyecto.
4. **Consultas supervisadas — Cassandra**:
   - Las consultas están organizadas por **categorías** (📍 Estado de Nodos, 🟤 Estado de Rutas, 🚛 Tracking Camiones, 📊 PageRank, 📋 Eventos, 👤 Gestor).
   - Selecciona una categoría del primer desplegable y luego una consulta específica.
   - Pulsas **“Ejecutar consulta Cassandra”**.
   - Se muestra un `st.dataframe`.
   - (Siempre puedes ver la CQL exacta en el expander “CQL ejecutado”.)
   - También dispones de un bloque **“CQL (copiar / pegar / ejecutar)”** para consultas `SELECT` directas.
5. **Consultas supervisadas — Hive (histórico)**:
   - Las consultas están organizadas por **categorías** (🔧 Diagnóstico, 📋 Eventos Histórico, 🌤 Clima Histórico, 🚛 Tracking Camiones, 📦 Transporte Ingestado, 🟤 Rutas Alternativas, 📊 Agregaciones Diarias, 👤 Gestor).
   - Selecciona una categoría del primer desplegable y luego una consulta específica.
   - Igual que Cassandra, pero usando HiveServer2 con PyHive.
   - Se limita para evitar bloqueos del UI.
   - También dispones de **“SQL (copiar / pegar / ejecutar)”** para `SHOW`, `SELECT`, `WITH`, `DESCRIBE`.
6. **Informes a medida (Cassandra/Hive)**:
   - Selección por motor, tabla y campos (o modo `SELECT *`).
   - Configuración de `WHERE`, `ORDER BY`, `LIMIT`.
   - Guardado/carga de plantillas personalizadas.
   - Exportación a PDF para visualización o impresión.
7. **Slides — Clima y anticipación de retrasos**:
   - Genera una estimación orientativa por hub combinando variables meteorológicas y estados.

**Resultado esperado**: tablas y métricas listas para usuario final, con opción de consulta libre segura y generación de informes PDF.

---

## 4.4 Pestaña “Asistente flota”

Objetivo: preguntar en **lenguaje natural** y ejecutar consultas supervisadas en Cassandra/Hive.

Cómo usarlo:

1. Escribe en el chat (ejemplos):
   - “¿Dónde está el camión 1?”
   - “¿Qué nodos tienen más incidencia?”
   - “¿Cuál es el nodo logístico más crítico?”
   - “Historial de rutas del camión 1” (Hive)
2. El asistente traducirá la intención a una consulta preaprobada.
3. La respuesta se muestra en un `st.dataframe`.
4. Debajo del dataframe podrás activar:
   - **“Ver consulta SQL”** para ver la consulta CQL/HiveQL exacta.

**Resultados esperados**:

- Tiempo real (Cassandra): coordenadas (lat/lon), rutas y estados.
- Histórico (Hive): muestras o conteos sobre tablas históricas.

---

## 4.5 Pestaña “Rutas híbridas”

Objetivo: planificación de transporte en una red híbrida (hub ↔ ciudad, etc.), con incidencias y alternativas.

Flujo guiado:

1. Abre el expander **“Catálogo de conexiones…”** (solo para inspeccionar la red).
2. En el formulario “Transporte: origen → destino”:
   - Elige **origen** y **destino**.
3. Define incidencias:
   - Checkbox de clima (OWM) si quieres aplicar clima a retrasos.
   - Checkbox de bloqueo por obras/clima severo.
4. Pulsa **“Calcular ruta y mostrar en mapa”**.

**Resultados esperados**:

- Ruta principal: secuencia de nodos + saltos.
- Estimación: minutos totales y coste.
- Si aplica: rutas alternativas (dibujo naranja discontinuo) y lista con motivos.
- Mapa interactivo con la red y trazado.

---

## 4.6 Pestaña “Gemelo digital”

Objetivo: simulación de planificación sobre red “modelo” (estática) con recálculo Dijkstra/NetworkX.

Incluye:

- Sidebar de incidencias (atasco, clima, bloqueo de capital).
- Selección:
  - Camión de referencia (por datos en Cassandra)
  - Origen/destino (capitales)
- Visualización:
  - Ruta de referencia (y alternativa si bloqueo activo)
  - Mapa Folium con red, ruta y marcadores de camiones
- Histórico Hive:
  - Muestra una vista de `transporte_ingesta_real` si la tabla Hive está creada.
- Ingesta explorador (CSV):
  - Subes un CSV de camiones, y el sistema lo integra hacia Kafka/HDFS.

**Resultado esperado**: una simulación “qué pasa si…” con rutas y tabla de apoyo desde Hive.

---

## 4.7 Pestaña “Servicios”

Objetivo: iniciar, comprobar y parar servicios del stack.

Cómo usarla:

1. Pulsa **“Comprobar todos los servicios”**.
2. Por cada servicio podrás:
   - **Iniciar**
   - **Comprobar**
   - **Parar** (requiere confirmar en checkbox, porque puede cortar trabajos).
3. En la parte inferior encontrarás **FAQ IA** para resolver dudas rápidas sobre operación, informes, NiFi, Swagger o uso general del dashboard.

**Resultado esperado**: estados por servicio (OK/❌), enlaces a consolas web si existen y un asistente de FAQ local para incidencias frecuentes.

Si NiFi está activo, el linaje de la ingesta enriquecida se puede revisar en la UI de NiFi:

- abrir `Data Provenance`
- filtrar por `Merge_DGT_Into_Payload`
- inspeccionar atributos `simlog.provenance.stage`, `simlog.provenance.sources`, `simlog.provenance.dgt_mode` y `simlog.provenance.dgt_incidents`

## 4.7.b Cómo interpretar el clima alternativo a OpenWeather

Cuando OpenWeather no esté disponible o la clave no sea válida, la aplicación no se detiene. En ese caso:

- el payload mostrará entradas de `clima` o `clima_hubs` con `source = dgt`,
- el campo `fallback_activo` aparecerá a `true`,
- `resumen_dgt.hubs_clima_respaldo` indicará cuántos hubs se han cubierto con la fuente alternativa,
- la pestaña **Resultados pipeline** seguirá mostrando la ingesta como válida aunque el enriquecimiento meteorológico principal no esté activo.

Qué debes revisar:

1. En **Resultados pipeline**, comprueba el modo DGT (`live`, `cache`, `disabled`).
2. En el último snapshot, comprueba si el clima viene con `source=openweather` o `source=dgt`.
3. Si estás usando NiFi, revisa `Data Provenance` para ver en qué etapa se mezclaron las fuentes.

## 5. Ejecución de la ingesta DGT

### 5.1 Script standalone

```bash
venv_transporte/bin/python scripts/ejecutar_ingesta_dgt.py
```

Opciones útiles:

- `--cache-only`: reutiliza la caché XML local de la DGT.
- `--no-dgt`: desactiva la fuente DGT y usa solo simulación.
- `--skip-processing`: ejecuta solo la ingesta y no lanza Spark.

### 5.2 Qué cambia al usar DGT

- El payload añade `incidencias_dgt` y `resumen_dgt`.
- Los nodos afectados pasan a `source=dgt`.
- La severidad real ajusta `peso_pagerank`, por lo que el ranking de criticidad puede cambiar.
- Si hay muchos nodos bloqueados, aparece una alerta operativa en el snapshot.
- Si OpenWeather falla, la información meteorológica útil se toma de DGT y queda visible con `fallback_activo=true`.

### 5.3 Cómo está configurado el respaldo a OpenWeather

- El sistema intenta primero consultar OpenWeather.
- Si la respuesta no es válida, se conservan las incidencias DGT y se reutilizan sus campos meteorológicos (`condiciones_meteorologicas`, `estado_carretera`, `visibilidad`) para completar los hubs.
- Este comportamiento aplica tanto al script de ingesta como a Airflow y al flujo NiFi del grupo `PG_SIMLOG_KDD`.
- Para el usuario final no cambia el uso del dashboard: cambia únicamente la fuente mostrada en el payload y en los indicadores.

### FAQ IA dentro de “Servicios”

Qué hace:

- Consulta una base de conocimiento local del proyecto.
- Devuelve una respuesta, una puntuación de confianza, la coincidencia principal y sugerencias relacionadas.
- Guarda un historial corto de la sesión.

Cómo usarlo:

1. Asegúrate de que el servicio **FAQ IA API** está activo.
2. Escribe una pregunta como:
   - “¿Cómo genero un informe PDF?”
   - “¿Por qué NiFi no aparece activo?”
   - “¿Dónde veo Swagger?”
3. Pulsa **“Preguntar al FAQ IA”**.

Resultados esperados:

- Respuesta operativa inmediata.
- Métrica de confianza.
- Sugerencias de preguntas cercanas.
- Fuentes del repositorio utilizadas como referencia.

---

## 4.8 Pestaña “Mapa y métricas”

Objetivo: visualizar estado operativo (Cassandra) o planificación (último cálculo).

Modo:

- **operativo**:
  - Mapa Folium con nodos y aristas desde Cassandra
  - PageRank (muestra top)
  - Leyenda: verde OK, naranja congestión, rojo bloqueo
- **planificación**:
  - Reutiliza el resultado de “Rutas híbridas” para mostrar la última ruta y alternativas.

**Resultado esperado**: visualización coherente con lo que se ve en Cassandra o en el cálculo de rutas.

---

## 4.9 Pestaña “Verificación técnica”

Objetivo: checks rápidos.

Muestra:

- HDFS backup (`HDFS_BACKUP_PATH`)
- Kafka topic (`TOPIC_TRANSPORTE`)
- Cassandra: nodos (`nodos_estado`) y tracking (`tracking_camiones`)
- Nota: Hive es opcional si HiveServer2 falla.

---

## 5. Qué hacer si “no aparece” el histórico Hive

1. Confirma que Spark ha corrido (fase 5 / procesamiento).
2. Verifica HiveServer2 (pestaña **Resultados pipeline** o **Verificación técnica**).
3. Si Hive no responde, el núcleo del proyecto igual valida con Cassandra (tiempo casi real).


---

## 📚 Obsidian Vault - Documentación Extendida

Este proyecto incluye un **vault de Obsidian** con documentación interactiva adicional.

### Cómo Acceder

1. Abre **Obsidian** en tu ordenador
2. File → Open Vault → Selecciona la carpeta del proyecto
3. El archivo `Bienvenido.md` es el punto de entrada

### Contenido Adicional

- **Glosario técnico**: `01_Conceptos/` - Kafka, Spark, Cassandra, GraphFrames...
- **Snippets de código**: `05_Snippets_Code/` - Código reutilizable
- **Modelos IA**: `06_Modelos_IA/` - PageRank, Detección de Anomalías
- **Manuales completos**: `07_Documentacion/`

### Navegación

- `Ctrl+O` - Buscar notas
- `Ctrl+G` - Ver grafo de conocimientos
- `Ctrl+P` - Paleta de comandos

Ver [`OBSIDIAN_VAULT.md`](OBSIDIAN_VAULT.md) para guía completa.
