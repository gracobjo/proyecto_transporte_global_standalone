# Documentación — SIMLOG España

Índice de documentos técnicos del proyecto (modo **standalone**).

## Visión general

| Documento | Descripción |
|-----------|-------------|
| [ALINEACION_SENTINEL360_STANDALONE.md](ALINEACION_SENTINEL360_STANDALONE.md) | **Equivalencias** con el proyecto de referencia [Sentinel360](https://github.com/gracobjo/Proyecto-Big-Data-Sentinel-360) y principios standalone |
| [FLUJO_DATOS_Y_REQUISITOS.md](FLUJO_DATOS_Y_REQUISITOS.md) | Flujo de datos, ejemplos JSON, Hive/Cassandra |
| [REQUIREMENTS_CHECKLIST.md](REQUIREMENTS_CHECKLIST.md) | Checklist frente al PDF del enunciado |
| [AIRFLOW.md](AIRFLOW.md) | Arranque de Airflow (api-server + scheduler) |
| [YARN_Y_SPARK.md](YARN_Y_SPARK.md) | Notas sobre YARN vs Spark local |

## Referencia externa

- **Sentinel360** (Big Data, KDD, stack Apache, dashboards): [gracobjo/Proyecto-Big-Data-Sentinel-360](https://github.com/gracobjo/Proyecto-Big-Data-Sentinel-360)  
  SIMLOG toma ideas de funcionalidad; el despliegue **no** requiere clúster multi-nodo (ver `ALINEACION_SENTINEL360_STANDALONE.md`).
- **Persistencia operativa en SIMLOG:** solo **Apache Cassandra** (no MongoDB).
