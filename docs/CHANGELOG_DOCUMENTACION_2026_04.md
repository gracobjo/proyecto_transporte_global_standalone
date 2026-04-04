# Changelog de documentación (abril 2026)

Resumen de cambios funcionales reflejados en manuales, diseño, requisitos y diagramas.

## Cuadro de mando (Streamlit)

- **Contexto de modelo de datos**: al seleccionar una consulta supervisada Cassandra o Hive, la UI muestra la plantilla CQL/SQL, el número de tablas implicadas y texto de negocio (`servicios/cuadro_mando_modelo_datos.py`). Botones opcionales para `DESCRIBE` / metadatos en vivo.
- **Análisis asistido (histórico Hive)**: sección que ejecuta plantillas aprobadas (`agg_ultima_semana`, `eventos_evolucion_dia`, `gestor_incidencias_resumen`), estadísticas numéricas y proyecciones heurísticas (regresión lineal simple sobre la serie de la muestra). No sustituye modelos ML desplegados (`servicios/cuadro_mando_analisis_hive.py`).
- **Mapas de dependencias Hive**: `HIVE_CONSULTA_DEP_TABLAS` y `hive_tablas_de_consulta()` en `servicios/consultas_cuadro_mando.py` centralizan qué tabla física exige cada consulta (sincronizado con «Verificar tablas» en la UI).

## Consultas Cassandra (whitelist)

- **CQL válido**: consultas que usaban `OR` entre varios `LIKE` o `GROUP BY` incompatible con la PK se sustituyen por lectura + agregación/filtrado en cliente donde hace falta (p. ej. clima adverso, incidencias por provincia). Ver `docs/HIVE_CUADRO_MANDO_CORRECCIONES.md` §9.

## Consultas Hive — rendimiento ventana 24h

- **Poda de particiones `_P24H`**: las consultas que combinan ventana «hoy/ayer» (`_F24`) ya no usan `_P7` (mes de `date_sub(..., 6)`), que en los primeros días del mes abría **todo el mes anterior** y provocaba timeouts en `GROUP BY`. Ahora se usa `_P24H` (solo mes de hoy y mes de ayer). Detalle: `docs/HIVE_CUADRO_MANDO_CORRECCIONES.md` §8.

## Requisitos Python

- `pandas>=2.0.0` añadido explícitamente en `requirements.txt` para el módulo de análisis del cuadro de mando.

## KNIME ↔ Hive

- **`docs/INTEGRACION_KNIME_HIVE.md`**: tabla de política de versión (Analytics Platform no versionada en el repo; JDBC; recomendación de homogeneizar versión entre analistas; criterio release estable y extensiones).

## Documentos actualizados en esta entrega

| Documento | Ajuste principal |
|-----------|------------------|
| `docs/MANUAL_USUARIO.md` | Cuadro de mando: modelo de datos, análisis Hive, notas Hive |
| `docs/MANUAL_DESARROLLADOR.md` | Módulos nuevos, `_P24H`, `CASSANDRA_CONSULTA_TABLAS` |
| `docs/DISENO_SISTEMA.md` | Capa presentación / decisiones cuadro de mando |
| `docs/CASOS_DE_USO.md` | CU-20 análisis asistido |
| `docs/DIAGRAMAS_MERMAID.md` | UC-20 y secuencia consulta + modelo |
| `docs/REQUIREMENTS_CHECKLIST.md` | Filas cuadro de mando / RNF Hive |
| `docs/HIVE_CUADRO_MANDO_CORRECCIONES.md` | §8 `_P24H`, §9 Cassandra |
| `docs/FLUJO_DATOS_Y_REQUISITOS.md` | Hive UI / poda particiones |
| `README.md` / `README_SIMLOG.md` | Estructura y punteros |

La carpeta `07_Documentacion/` se sincroniza con copias de los mismos ficheros bajo `docs/` cuando procede el mantenimiento paralelo del vault.

## NiFi PG_SIMLOG_KDD, verificación por terminal y Streamlit (2026-04-04)

### Scripts y API NiFi

| Script | Uso |
|--------|-----|
| `scripts/recreate_nifi_practice_flow.py` | Crea/actualiza `PG_SIMLOG_KDD`: ExecuteScript con `Script File` absoluto bajo `nifi/groovy/`, **`Script Body` vacío** (evita que un cuerpo pegado en la UI anule el fichero), **`concurrentTasks` = 2** en esos Groovy, flujo HDFS → Spark → `RouteOnAttribute` → notificaciones. Constante `NIFI_PG_EXECUTE_SCRIPTS` y `assert_nifi_groovy_sources_exist`. NiFi 2.x aquí no admite `EVENT_DRIVEN` en la API (solo `TIMER_DRIVEN` / `CRON_DRIVEN`). |
| `scripts/sync_nifi_groovy_scripts.py` | Solo vuelca rutas y propiedades de los tres ExecuteScript Groovy (tras `git pull` o editar `.groovy`). |
| `scripts/verificar_flujo_nifi_api.py` | Comprueba procesadores y aristas HDFS → Spark → notify; escribe `reports/nifi_flow_verify.log`. |
| `scripts/nifi_iniciar_pipeline_simlog.py` | RUNNING/STOPPED por procesador; intento de `run-status` del grupo silenciado en **404** (NiFi 2 no expone ese endpoint para grupos locales). |

### Groovy DGT

- `nifi/groovy/MergeDgtDatex2IntoPayload.groovy`: durante `XmlSlurper.parseText`, redirección temporal de `System.err` (con bloqueo) para evitar líneas `[Fatal Error]` de Xerces en XML truncado/ inválido aunque el flujo siga en modo degradado.

### Dashboard (Streamlit)

- **Resultados pipeline**: si Cassandra CQL falla con *connection refused*, mensaje de ayuda y caption con `CASSANDRA_HOST` (`servicios/pipeline_verificacion.py`, `servicios/ui_pipeline_resultados.py`).
- **`st.link_button`**: eliminado el argumento `key` (no soportado en Streamlit ~1.28 del venv) en `servicios/ui_pruebas_ingesta.py` y `servicios/ui_servicios_web.py`.

### Otros ficheros en el mismo commit

Incluye además cambios en `nifi/flow/simlog_kdd_flow_spec.yaml`, scripts bajo `nifi/scripts/`, `ingesta/`, `procesamiento/`, `servicios/` (API, gemelo digital, KDD UI), `config_nodos.py` y punteros en `README_SIMLOG.md`; revisar `git show` para el detalle por archivo.
