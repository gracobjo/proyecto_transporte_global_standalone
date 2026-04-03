# Correcciones del cuadro de mando Hive (SIMLOG)

Documento de referencia para los cambios aplicados a consultas supervisadas, configuración y UI relacionada con **Hive** en el proyecto. La fuente de verdad del esquema tabular sigue siendo `persistencia_hive.py` (`TABLE_SCHEMAS`).

---

## 1. Configuración unificada (`config.py`)

| Variable | Propósito | Valor por defecto |
|----------|-----------|-------------------|
| `HIVE_TABLE_TRANSPORTE_HIST` (`SIMLOG_HIVE_TABLA_TRANSPORTE`) | Tabla plana de transporte para UI y gestor | `transporte_ingesta_completa` |
| `HIVE_TABLE_TRACKING_HIST` (`SIMLOG_HIVE_TABLE_TRACKING_HIST`) | Histórico de tracking en Hive | `tracking_camiones_historico` |
| `HIVE_TABLE_HISTORICO_NODOS` | Conteos/muestras de histórico de nodos | `historico_nodos` |
| `HIVE_TABLE_NODOS_MAESTRO` | Maestro de nodos (HDFS/CSV) | `nodos_maestro` |
| `HIVE_TABLE_RED_GEMELO_NODOS` / `HIVE_TABLE_RED_GEMELO_ARISTAS` | Red estática del gemelo (DDL en `generar_red_gemelo_digital.py`) | `red_gemelo_nodos`, `red_gemelo_aristas` |

`persistencia_hive.TABLA_CAMIONES` y `TABLA_TRANSPORTE` usan estos nombres para que **Spark y el cuadro de mando** apunten a la **misma** tabla.

---

## 2. Whitelist `HIVE_CONSULTAS` (`servicios/consultas_cuadro_mando.py`)

- **Imports**: Se importan desde `config` los nombres de tabla anteriores; evita `NameError` al construir SQL con `historico_nodos` / `nodos_maestro` / gemelo.
- **Transporte**: `_T_TRANSPORTE = HIVE_TABLE_TRANSPORTE_HIST` (antes fijo a `transporte_ingesta_completa`).
- **Tracking**: `_T_TRACKING = HIVE_TABLE_TRACKING_HIST`.
- **Gemelo digital**: Entradas `gemelo_red_nodos` y `gemelo_red_aristas` (columnas alineadas con el DDL de la red gemelo). Consumidas por `cargar_red_gemelo()` en `servicios/gemelo_digital_datos.py`.
- **Columna temporal**: Uso de `` `timestamp` `` (`_HIVE_TS`) donde el identificador es palabra reservada en Hive 3+.
- **Particiones / ventanas**: Fragmentos `_PM`, `_P7`, **`_P24H`**, `_F24`, `_F7D` para poda por `anio_part`/`mes_part` y filtros por `anio`/`mes`/`dia` sin comparar `timestamp` STRING con funciones de fecha de forma frágil. **Regla**: las consultas con ventana calendario **hoy/ayer** (`_F24`) deben usar **`_P24H`** (mes de hoy ∪ mes de ayer), no **`_P7`**, para no incluir el mes de `date_sub(current_date(), 6)` (en días 1–6 del mes eso abría **todo el mes anterior** y provocaba timeouts en `GROUP BY`).
- **Sesión PyHive**: SET por defecto (p. ej. `hive.auto.convert.join=false`) para reducir errores tipo `MapredLocalTask` en JOINs; caché SQL con TTL renovable (`SIMLOG_HIVE_CACHE_*`).

---

## 3. Consulta «Gestor — clima adverso que afecta transporte»

- **Problema**: El `JOIN` igualaba `timestamp` entre tablas de clima y transporte; en la práctica casi nunca coincidía.
- **Corrección**: `JOIN` por **ciudad = hub_actual** y **misma fecha** (`anio`, `mes`, `dia`), con subconsultas que proyectan esas columnas.

---

## 4. Comparaciones de texto (`_hive_txt_norm`)

- **Función**: Expresión Hive generada en Python: `trim` → `lower` → `translate` (quita tildes habituales) para comparar estados sin depender de mayúsculas ni de variantes como `Óptimo` / `Optimo`.
- **Aplicado a**: Filtros sobre `tipo_evento`, `estado` (bloqueos), `estado_ruta` (retrasos), `estado_carretera` (clima no óptimo en subconsulta del gestor), etc.
- **Rutas**: `rutas_alternativas_bloqueos` usa `length(trim(...)) > 0` en lugar de `!= ''` para excluir solo espacios.

---

## 5. Otras correcciones relacionadas

| Área | Cambio |
|------|--------|
| `tracking_ultima_posicion` | `ORDER BY \`timestamp\` DESC` antes de `LIMIT` para muestra reciente coherente. |
| `servicios/cuadro_mando_ui.py` | Criterio «Verificar tablas» usa `HIVE_TABLE_TRANSPORTE_HIST` para la tabla de transporte. Caption actualizado. |
| `servicios/gemelo_digital_datos.py` | Histórico de tracking vía `ejecutar_hive_sql_seguro` y columnas de `tracking_camiones_historico` (`id_camion`, `` `timestamp` ``); eliminada consulta inexistente `custom`. |
| `servicios/gestor_consultas_sql.py` | SQL Hive alineado con columnas reales (sin `camiones` en transporte). |
| `servicios/catalogo_tablas_simlog.py` | Catálogo Hive ampliado (p. ej. `red_gemelo_*`, esquemas consultables en UI). |

---

## 6. Variables útiles si «no hay error pero no hay filas»

| Variable | Efecto |
|----------|--------|
| `SIMLOG_HIVE_PARTITION_PRUNING=0` | Desactiva filtros `anio_part`/`mes_part` en consultas de la UI (útil si los datos no están particionados como asume el código). |
| `SIMLOG_HIVE_DAY_FILTER_24H=0` | Desactiva el filtro de «hoy/ayer» en ventanas 24h. |
| `HIVE_QUERY_TIMEOUT_SEC` | Aumenta el tiempo de espera a HiveServer2 (por defecto 600 s en código). |

---

## 7. DAG y orquestación

- DAG maestro documentado con prefijo `simlog_` (p. ej. `dag_simlog_maestro.py`); detalles en `docs/AIRFLOW.md` y `orquestacion/README_DAGS_KDD.md`.

---

## 8. Poda `_P24H` (ventana 24h, 2026-04)

Consultas afectadas (sustituyen `_P7`+`_F24` por `_P24H`+`_F24`): entre otras, `gestor_eventos_por_hub`, `gestor_incidencias_resumen`, `eventos_nodos_24h`, `eventos_bloqueos_24h`, `clima_estado_carretera`, `tracking_ultima_posicion`, subconsultas de `gestor_clima_afecta_transporte`.

Las consultas con ventana de **7 días** (`_F7D`) **siguen** usando `_P7` donde corresponda.

---

## 9. Cassandra — plantillas que requieren post-procesado en cliente

- **Clima adverso (varios `LIKE`)**: CQL no admite `OR` entre varios `LIKE` en columnas no clave; se lee un subconjunto de filas y se filtra en `ejecutar_cassandra_consulta()`.
- **Incidencias por provincia**: `GROUP BY` debe respetar la PK de Cassandra; el agregado `COUNT` por `(provincia, estado, motivo_retraso)` se calcula en cliente.
- **Tracking**: la tabla no tiene `temperatura`; no incluir en `SELECT`.

---

*Última actualización: abril 2026 — `_P24H`, modelo de datos en UI, análisis Hive asistido.*
