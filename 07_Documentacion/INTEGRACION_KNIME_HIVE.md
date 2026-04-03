# Integración KNIME en la arquitectura Big Data SIMLOG

Guía práctica para conectar **KNIME Analytics Platform** al **HiveServer2** del proyecto, construir un dataset de entrenamiento y desplegar modelos con impacto mínimo en RAM del servidor (KNIME corre **fuera** del nodo Hadoop por defecto).

### Versión de KNIME (política del proyecto)

| Pregunta | Respuesta |
|----------|-----------|
| ¿Qué versión instala este repo? | **Ninguna.** KNIME no es dependencia Python ni artefacto del cluster; no aparece en `requirements.txt` ni se despliega en el nodo Hadoop. |
| ¿Qué producto usar? | **KNIME Analytics Platform** (cliente de escritorio) en el **PC del analista**. |
| ¿Por qué no fijamos un número en el código? | La integración es **JDBC genérico** a HiveServer2 (`jdbc:hive2://…`). Cualquier Analytics Platform reciente con nodos *Database Connector* / *DB Reader* sirve; lo crítico es alinear el **driver Hive JDBC** con la versión de Hive del servidor (véase §2.1). |
| ¿Cómo homogeneizar el equipo? | Acordar **una misma versión** de Analytics Platform entre analistas (mismos nodos, mismas extensiones, workflows reproducibles). Comprobar en *Help → About KNIME Analytics Platform*. Documentar internamente la versión acordada (wiki o README de equipo). |
| Criterio práctico | Elegir la **última release estable** soportada por el SO del puesto; si usáis extensiones (p. ej. Python, PMML), verificar compatibilidad en la documentación de KNIME antes de fijar versión. |

---

## 1. Contexto y principio de bajo consumo

| Componente | Rol en SIMLOG | ¿Debe estar encendido para KNIME? |
|--------------|-----------------|-----------------------------------|
| **HiveServer2** | JDBC (`jdbc:hive2://…:10000`) | **Sí** (único imprescindible para leer tablas) |
| **Metastore** | Metadatos Hive | Sí (HS2 lo necesita) |
| **HDFS / NameNode** | Warehouse de tablas externas | Sí si los datos están en HDFS |
| **NiFi, Spark, Airflow** | Ingesta y procesamiento | **No** para solo entrenar en KNIME |
| **KNIME** | Cliente analítico | **No** en el servidor: instálalo en **PC del analista** (típ. 8–16 GB RAM para workflows medianos) |

**Modo mínimo recomendado (equipo con poca RAM en el cluster):** levantar solo **HDFS + Metastore + HiveServer2** cuando vayas a extraer datos; el resto del stack puede permanecer parado.

En el dashboard Streamlit, pestaña **KNIME / IA avanzada**, puedes usar **«Arrancar stack mínimo (HDFS + Hive)»** y **«Parar Hive + HDFS»**, que llaman a `arrancar_stack_minimo_knime()` / `parar_stack_minimo_knime()` en `servicios/gestion_servicios.py` (misma lógica que la pestaña Servicios).

---

## 2. Conexión KNIME → Hive (JDBC)

### 2.1 Driver

KNIME usa el conector **Hive** basado en JDBC tipo 4:

1. Descarga el **Hive JDBC standalone JAR** acorde a tu versión de Hive (p. ej. del tarball `apache-hive-*-jdbc.jar` o del directorio `jdbc` de la distribución Hive).
2. En KNIME: *File → Preferences → KNIME → Databases → Register new driver* y registra el JAR.
3. URL de conexión (misma lógica que `HIVE_JDBC_URL` en `config.py`):

```text
jdbc:hive2://<HOST>:10000/<DATABASE>
```

Ejemplos:

```text
jdbc:hive2://127.0.0.1:10000/logistica_espana
jdbc:hive2://hiveserver.host:10000/logistica_espana
```

- **Puerto por defecto HiveServer2:** `10000`
- **Base de datos:** `HIVE_DB` (por defecto `logistica_espana`)

### 2.2 Autenticación

- Entornos locales suelen usar **sin Kerberos** (usuario Hadoop): en el nodo *Database Connector* indica usuario (`hadoop` o el valor de `SIMLOG_HIVE_BEELINE_USER` / `HADOOP_USER_NAME`).
- Si el cluster exige Kerberos, configura el *Advanced JDBC* según la política del cluster (principal, keytab, `hive.server2.authentication`).

### 2.3 Dependencias (resumen)

| Artefacto | Uso |
|-----------|-----|
| `hive-jdbc-standalone-*.jar` (o equivalente) | Driver JDBC en KNIME |
| HiveServer2 en marcha | Servidor |
| Metastore (Derby/MySQL/Postgres) | Ya configurado en tu instalación Hive |

### 2.4 Plantilla en `docs/knime/jdbc_connection_template.properties.example`

Copia y ajusta host, puerto y base; no commitees credenciales reales.

---

## 3. Consulta HiveQL — dataset de entrenamiento

El esquema SIMLOG no incluye sensores físicos de **velocidad** ni **densidad**; la consulta de ejemplo **deriva proxies** a partir de:

- `progreso_pct`, `estado_ruta` (transporte),
- `temperatura`, `descripcion` (clima),
- `tipo` (red gemelo),
- agregados de eventos como **densidad** de actividad.

Archivo versionado: **`sql/hive_dataset_entrenamiento_knime.hql`**.

Variables objetivo y features (alineadas al enunciado académico):

| Campo | Origen |
|-------|--------|
| `velocidad_media` | Proxy: `100 - LEAST(progreso_pct, 99)` (índice inverso de “atasco” en la simulación) |
| `densidad_trafico` | Proxy: `contador` de `agg_estadisticas_diarias` o `COUNT` de eventos por hub/día |
| `temperatura` | `clima_historico.temperatura` |
| `lluvia` | Binaria desde `descripcion` (lluvia/rain/chubasco) |
| `hora` | `hora` de `transporte` o `0` si falta |
| `tipo_via` | `red_gemelo_nodos.tipo` (hub / secundario / …) |
| `congestion` | **1** si retraso/bloqueo/congestión en texto de estado o carretera no óptima; **0** en caso contrario |

Ajusta `JOIN` y `WHERE` si tus tablas están vacías o tienen otros nombres (`SIMLOG_HIVE_TABLA_TRANSPORTE`, etc.).

---

## 4. Pipeline KNIME (nodos recomendados)

Orden lógico (modo **supervised learning** binario):

| # | Nodo KNIME | Propósito | Configuración recomendada |
|---|------------|-----------|---------------------------|
| 1 | **DB Connector** | Conexión JDBC a Hive | URL, driver registrado, usuario, *Test connection* |
| 2 | **DB SQL Executor** *(opcional)* | Validar `SELECT` | Pegar SQL de prueba con `LIMIT 100` |
| 3 | **DB Reader** | Traer dataset a memoria | Query desde fichero `.sql` o portapapeles; **no** cargar millones de filas en equipos débiles: usa `LIMIT`/`PARTITION` en Hive |
| 4 | **Missing Value** | Imputar nulos | Media/mediana numéricas; moda en categóricas; o filtrar filas |
| 5 | **String Manipulation** / **Rule Engine** | Homogeneizar `tipo_via`, `hora` | Reglas simples |
| 6 | **Column Filter** | Quitar IDs/timestamps crudos | Mantener solo features + target |
| 7 | **Normalizer** (Min-Max o Z) | Escalar numéricas | Necesario para SVM/NN; Random Forest tolera sin escalar |
| 8 | **Partitioning** | Train/test | 70/30 o 80/20, **stratified** sobre `congestion` |
| 9 | **Random Forest Learner** | Modelo tabular | 200–500 árboles, profundidad limitada si overfitting |
| 10 | **Random Forest Predictor** | Inferencia en test | — |
| 11 | **Scorer** | Accuracy, Kappa | Marca columna real = `congestion` |

**Alternativa ligera en RAM:** **Decision Tree** o **Logistic Regression** en lugar de Random Forest profundo.

---

## 5. Evaluación del modelo

En el nodo **Scorer** (o **Binary Classification Inspector**):

| Métrica | Interpretación breve |
|---------|----------------------|
| **Accuracy** | % aciertos globales; engañoso si clases desbalanceadas |
| **Precision** | De los predichos positivos, cuántos son realmente congestión |
| **Recall** | De los reales positivos, cuántos detecta el modelo |
| **F1** | Media armónica precision/recall; **referencia principal** en binario |

**Mejoras:** rebalanceo (SMOTE en KNIME, **Row Sampling**), más datos históricos, tuning de hiperparámetros (**Parameter Optimization** loop), features con ventana temporal (lags de `hora`).

---

## 6. Exportación del modelo

| Formato | Descripción |
|---------|-------------|
| **PMML** | *Random Forest PMML* (extensión) o árbol → PMML; consumible en Java, Spark, algunos servicios |
| **Model Writer (KNIME)** | Fichero `.model` para reabrir en KNIME |
| **Python** | *KNIME Python extension* para serializar con `pickle`/`joblib` (árbol/sklearn) si conviertes a sklearn vía scripting |

**Ejemplo posterior en Python (conceptual):** cargar PMML con `pypmml` o deserializar sklearn y exponer `/predict` en FastAPI.

---

## 7. Integración futura (tiempo real)

| Opción | Descripción |
|--------|-------------|
| **1 — NiFi + REST** | NiFi ejecuta `InvokeHTTP` contra un microservicio que hace scoring; payload con features actuales. |
| **2 — Microservicio Python (FastAPI)** | Mismo patrón que `servicios/api_simlog.py`: carga modelo (PMML/sklearn), recibe JSON, devuelve `congestion_prob`. Baja latencia, fácil de versionar. |
| **3 — Scoring en streaming** | Spark Structured Streaming o Flink leyendo Kafka; modelo embebido o broadcast; solo para volúmenes altos. |

En SIMLOG, la vía más alineada con el repo es **FastAPI** + opcional cola Kafka.

---

## 8. Bonus: mejoras de arquitectura

| Idea | Beneficio |
|------|-----------|
| **Vista Hive** `v_knime_features_train` | Fija la query de entrenamiento; KNIME solo hace `SELECT * FROM v_knime_features_train` |
| **Particionar export por `anio_part/mes_part`** | Extracciones incrementales sin full scan |
| **Feature store ligero** (CSV en HDFS + catálogo) | Trazabilidad para trabajo en equipo |
| **Dashboard** | Añadir métricas del modelo en Streamlit leyendo resultados exportados (CSV) desde `reports/knime/` |

---

## 9. Referencias en el repo

| Ruta | Contenido |
|------|-----------|
| `sql/hive_dataset_entrenamiento_knime.hql` | Query ejemplo |
| `docs/knime/jdbc_connection_template.properties.example` | Plantilla JDBC |
| Pestaña Streamlit **KNIME / IA avanzada** | Checklist + JDBC + enlaces |

---

*Documento orientado a proyecto académico / especialización Big Data. Ajustar nombres de tabla con las variables de entorno del despliegue real.*
