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
